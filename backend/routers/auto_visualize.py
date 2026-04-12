"""
Auto-Visualize Router — POST /auto-visualize
Generates chart-ready data directly from the dataset schema without needing a user query.

Strategy:
1. Find the best numeric + categorical columns from the schema
2. Generate meaningful aggregation queries:
   - Trend: numeric column AVG grouped by a categorical column (top 15)
   - Composition: COUNT per category (top 10 for pie)
   - Comparison: AVG of top 2–3 numeric columns grouped by category
3. Return all three datasets with chart metadata
"""
import math
import datetime
import duckdb
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple

import state

router = APIRouter()

DATE_KEYWORDS = {"date", "month", "year", "quarter", "week", "day", "time",
                 "period", "created", "updated", "timestamp"}


class AutoVisualizeRequest(BaseModel):
    dataset_id: str
    mode: str = "raw"


class ChartDataset(BaseModel):
    chart_type: str
    chart_x: str
    chart_y: List[str]
    result: List[Dict[str, Any]]
    title: str
    sql: str


class AutoVisualizeResponse(BaseModel):
    trend: Optional[ChartDataset] = None
    composition: Optional[ChartDataset] = None
    comparison: Optional[ChartDataset] = None
    summary_stats: Dict[str, Any] = {}


# ── helpers ─────────────────────────────────────────────────────────────────

def _escape(path: str) -> str:
    return path.replace("\\", "/")


def _classify_columns(columns: List[Dict]) -> Tuple[List[str], List[str], List[str]]:
    """Return (numeric_cols, categorical_cols, date_cols)."""
    numeric, categorical, date = [], [], []
    for col in columns:
        name = col["name"]
        col_type = col.get("type", "").upper()
        name_lower = name.lower()

        is_numeric = any(t in col_type for t in
                         ["INT", "FLOAT", "DOUBLE", "DECIMAL", "BIGINT", "HUGEINT", "REAL", "NUMERIC"])
        is_date = (
            any(t in col_type for t in ["DATE", "TIMESTAMP", "TIME"]) or
            any(kw in name_lower for kw in DATE_KEYWORDS)
        )
        unique_count = col.get("unique_count", 9999)

        if is_date:
            date.append(name)
        elif is_numeric:
            numeric.append(name)
        else:
            # Treat low-cardinality as categorical (good for grouping)
            categorical.append(name)

    return numeric, categorical, date


def _safe_rows(rows: List[Dict]) -> List[Dict]:
    """Replace NaN/non-serialisable values."""
    clean = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, float) and math.isnan(v):
                new_row[k] = None
            elif isinstance(v, (datetime.date, datetime.datetime)):
                new_row[k] = str(v)
            else:
                new_row[k] = v
        clean.append(new_row)
    return clean


def _run_sql(conn: duckdb.DuckDBPyConnection, sql: str) -> List[Dict]:
    try:
        df = conn.execute(sql).fetchdf()
        df = df.where(df.notna(), other=None)
        return _safe_rows(df.to_dict(orient="records"))
    except Exception:
        return []


def _pick_group_col(categorical: List[str], date: List[str],
                    columns: List[Dict]) -> Optional[str]:
    """Pick the best column to GROUP BY (low-cardinality categorical preferred)."""
    # Sort categoricals by unique_count ascending so we get meaningful groups
    cat_with_card = []
    for c in categorical:
        for col in columns:
            if col["name"] == c:
                cat_with_card.append((c, col.get("unique_count", 9999)))
                break
    cat_with_card.sort(key=lambda x: x[1])
    # Prefer cardinality 2-30 for grouping
    for name, uc in cat_with_card:
        if 1 < uc <= 30:
            return name
    # Fall back to first date or first categorical
    if date:
        return date[0]
    if cat_with_card:
        return cat_with_card[0][0]
    return None


# ── main endpoint ────────────────────────────────────────────────────────────

@router.post("/auto-visualize", response_model=AutoVisualizeResponse)
async def auto_visualize(req: AutoVisualizeRequest):
    dataset = state.datasets.get(req.dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    file_path: str = dataset["file_path"]
    schema: List[Dict] = dataset["columns"]
    row_count: int = dataset["row_count"]

    numeric, categorical, date = _classify_columns(schema)

    if not numeric:
        raise HTTPException(status_code=422, detail="No numeric columns found for visualization.")

    conn = duckdb.connect()
    try:
        conn.execute(
            f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{_escape(file_path)}')"
        )

        group_col = _pick_group_col(categorical, date, schema)
        num_col = numeric[0]          # Primary numeric column
        num_cols_3 = numeric[:3]      # Up to 3 numeric cols for comparison

        trend: Optional[ChartDataset] = None
        composition: Optional[ChartDataset] = None
        comparison: Optional[ChartDataset] = None

        # ── Trend: AVG(numeric) grouped by best group column ────────────────
        if group_col:
            sql_trend = f"""
                SELECT "{group_col}", AVG(CAST("{num_col}" AS DOUBLE)) AS avg_{num_col}
                FROM data
                WHERE "{group_col}" IS NOT NULL AND "{num_col}" IS NOT NULL
                GROUP BY "{group_col}"
                ORDER BY avg_{num_col} DESC
                LIMIT 20
            """
            rows_trend = _run_sql(conn, sql_trend)
            if rows_trend:
                chart_type = "line" if group_col in date else "area"
                trend = ChartDataset(
                    chart_type=chart_type,
                    chart_x=group_col,
                    chart_y=[f"avg_{num_col}"],
                    result=rows_trend,
                    title=f"Avg {num_col} by {group_col}",
                    sql=sql_trend.strip(),
                )

        # If no group col at all, fall back to histogram-style bucketing
        if not trend and numeric:
            sql_hist = f"""
                SELECT
                    FLOOR(CAST("{num_col}" AS DOUBLE) / (
                        (MAX(CAST("{num_col}" AS DOUBLE)) - MIN(CAST("{num_col}" AS DOUBLE))) / 10 + 0.0001
                    )) * (
                        (MAX(CAST("{num_col}" AS DOUBLE)) - MIN(CAST("{num_col}" AS DOUBLE))) / 10 + 0.0001
                    ) AS bucket,
                    COUNT(*) AS count
                FROM data
                WHERE "{num_col}" IS NOT NULL
                GROUP BY bucket
                ORDER BY bucket
            """
            rows_hist = _run_sql(conn, sql_hist)
            if rows_hist:
                trend = ChartDataset(
                    chart_type="area",
                    chart_x="bucket",
                    chart_y=["count"],
                    result=rows_hist,
                    title=f"Distribution of {num_col}",
                    sql=sql_hist.strip(),
                )

        # ── Composition: COUNT per category (pie) ───────────────────────────
        if group_col:
            sql_comp = f"""
                SELECT "{group_col}", COUNT(*) AS count
                FROM data
                WHERE "{group_col}" IS NOT NULL
                GROUP BY "{group_col}"
                ORDER BY count DESC
                LIMIT 10
            """
            rows_comp = _run_sql(conn, sql_comp)
            if rows_comp:
                composition = ChartDataset(
                    chart_type="pie",
                    chart_x=group_col,
                    chart_y=["count"],
                    result=rows_comp,
                    title=f"Count by {group_col}",
                    sql=sql_comp.strip(),
                )

        # ── Comparison: multi-metric bar chart ──────────────────────────────
        if group_col and len(num_cols_3) >= 2:
            avgs = ", ".join(
                f'AVG(CAST("{c}" AS DOUBLE)) AS "avg_{c}"' for c in num_cols_3
            )
            sql_comp2 = f"""
                SELECT "{group_col}", {avgs}
                FROM data
                WHERE "{group_col}" IS NOT NULL
                GROUP BY "{group_col}"
                ORDER BY "avg_{num_cols_3[0]}" DESC
                LIMIT 15
            """
            rows_comp2 = _run_sql(conn, sql_comp2)
            if rows_comp2:
                comparison = ChartDataset(
                    chart_type="bar",
                    chart_x=group_col,
                    chart_y=[f"avg_{c}" for c in num_cols_3],
                    result=rows_comp2,
                    title=f"Metric Comparison by {group_col}",
                    sql=sql_comp2.strip(),
                )
        elif group_col:
            # Only one numeric col — compare it with itself (still useful bar)
            sql_comp2 = f"""
                SELECT "{group_col}", AVG(CAST("{num_col}" AS DOUBLE)) AS avg_{num_col}
                FROM data
                WHERE "{group_col}" IS NOT NULL AND "{num_col}" IS NOT NULL
                GROUP BY "{group_col}"
                ORDER BY avg_{num_col} DESC
                LIMIT 15
            """
            rows_comp2 = _run_sql(conn, sql_comp2)
            if rows_comp2:
                comparison = ChartDataset(
                    chart_type="bar",
                    chart_x=group_col,
                    chart_y=[f"avg_{num_col}"],
                    result=rows_comp2,
                    title=f"Avg {num_col} by {group_col}",
                    sql=sql_comp2.strip(),
                )

        # ── Summary stats ───────────────────────────────────────────────────
        summary_stats = {
            "total_rows": row_count,
            "total_cols": len(schema),
            "numeric_cols": len(numeric),
            "categorical_cols": len(categorical),
            "group_col": group_col,
            "primary_metric": num_col,
        }

        return AutoVisualizeResponse(
            trend=trend,
            composition=composition,
            comparison=comparison,
            summary_stats=summary_stats,
        )

    finally:
        try:
            conn.close()
        except Exception:
            pass
