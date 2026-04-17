"""
Query Router — POST /query
Orchestrates: LLM SQL generation → DuckDB execution → preprocessing (Smart) →
data health → LLM explanation → chart detection → structured response.
"""
import re
from typing import List, Dict, Any, Optional, Tuple

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException

import state
from models.schemas import QueryRequest, QueryResponse, DataHealth
from services import llm_service, query_engine, preprocessing, data_health, spark_engine

router = APIRouter()

DATE_KEYWORDS = {"date", "month", "year", "quarter", "week", "day", "time", "period", "created", "updated", "timestamp"}



@router.post("/query", response_model=QueryResponse)
async def run_query(req: QueryRequest) -> QueryResponse:
    # ── 1. Validate dataset ────────────────────────────────────────────────
    dataset = state.datasets.get(req.dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=404,
            detail=(
                "Dataset not found. This usually happens because the server restarted "
                "(Render free tier sleeps after 15 minutes of inactivity and clears all "
                "in-memory data on wake-up). Please re-upload your CSV file and try again."
            ),
        )

    file_path: str      = dataset["file_path"]
    schema: list        = dataset["columns"]
    sample: list        = dataset["sample"]
    row_count: int      = dataset["row_count"]
    session_id: str     = req.session_id or "default"
    mode: str           = req.mode  # "raw" | "smart" | "scalable"

    # ── 2. Build history for context memory ───────────────────────────────
    history = state.sessions.get(session_id, [])

    # ── 3. Generate SQL ───────────────────────────────────────────────────
    try:
        sql = llm_service.generate_sql(schema, sample, req.question, history)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"LLM SQL generation failed: {exc}")

    # Basic safety guard — ensure it's a SELECT
    if not _is_select(sql):
        raise HTTPException(
            status_code=422,
            detail=f"LLM generated a non-SELECT statement. Query rejected.\nSQL: {sql}",
        )

    # ── 4. Execute query (raw, smart, or scalable) ───────────────────────
    log: List[str] = []
    outlier_count: int = 0
    conn = None
    cleaned_df_for_health = None

    try:
        if mode == "smart":
            preprocess_result = preprocessing.preprocess(file_path, schema)
            log = _preprocess_log_to_lines(preprocess_result.log)
            outlier_count = preprocess_result.outlier_count
            conn = preprocess_result.conn
            rows, columns = query_engine.execute_query(file_path, sql, conn=conn)
        elif mode == "scalable":
            raw_df = pd.read_csv(file_path)
            spark_result = spark_engine.run_spark_pipeline(raw_df)
            cleaned_df_for_health = spark_result["cleaned_df"]

            conn = duckdb.connect()
            conn.register("spark_cleaned_df", cleaned_df_for_health)
            conn.execute("CREATE TABLE data AS SELECT * FROM spark_cleaned_df")

            rows, columns = query_engine.execute_query(file_path, sql, conn=conn)
            log = [
                "Processed using PySpark (scalable mode)",
                f"Rows processed in Spark pipeline: {spark_result['rows_processed']}",
            ]
        else:
            rows, columns = query_engine.execute_query(file_path, sql)
            log = ["ℹ️ Results are based on raw data (no preprocessing applied)."]
    except Exception as exc:
        error_msg = str(exc)
        # Return partial response with error so frontend can show it
        health = data_health.compute_health(schema, 0, row_count)
        return QueryResponse(
            sql=sql,
            result=[],
            columns=[],
            explanation="",
            insights=[],
            data_health=DataHealth(**health),
            preprocessing_log=log,
            mode=mode,
            error=_format_query_error(mode, error_msg),
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # ── 5. Data health ────────────────────────────────────────────────────
    if mode == "scalable" and cleaned_df_for_health is not None:
        health = data_health.compute_health_from_dataframe(cleaned_df_for_health)
    else:
        health = data_health.compute_health(schema, outlier_count, row_count)

    # ── 6. LLM explanation ────────────────────────────────────────────────
    explanation_data = {"explanation": "Query executed successfully.", "insights": [], "why_analysis": ""}
    if rows:
        try:
            explanation_data = llm_service.explain_result(req.question, sql, rows, columns)
        except Exception:
            pass

    # ── 7. Chart detection ────────────────────────────────────────────────
    chart_type, chart_x, chart_y = _detect_chart(rows, columns)

    # ── 8. Save to session history ────────────────────────────────────────
    if session_id not in state.sessions:
        state.sessions[session_id] = []
    state.sessions[session_id].append({"role": "user", "content": req.question})
    state.sessions[session_id].append({"role": "assistant", "content": sql})
    # Keep last 10 turns
    state.sessions[session_id] = state.sessions[session_id][-10:]

    return QueryResponse(
        sql=sql,
        result=_serialise_rows(rows[:200]),   # cap at 200 rows for response size
        columns=columns,
        explanation=explanation_data.get("explanation", ""),
        insights=explanation_data.get("insights", []),
        chart_type=chart_type,
        chart_x=chart_x,
        chart_y=chart_y,
        data_health=DataHealth(**health),
        preprocessing_log=log,
        mode=mode,
        why_analysis=explanation_data.get("why_analysis", ""),
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_select(sql: str) -> bool:
    stripped = sql.strip().lstrip("(").upper()
    return stripped.startswith("SELECT") or stripped.startswith("WITH")


def _detect_chart(
    rows: List[Dict[str, Any]],
    columns: List[str],
) -> Tuple[Optional[str], Optional[str], List[str]]:
    if not rows or len(columns) < 2:
        return None, None, []

    numeric_cols: List[str] = []
    categorical_cols: List[str] = []

    for col in columns:
        sample_vals = [r.get(col) for r in rows[:5] if r.get(col) is not None]
        if sample_vals and all(isinstance(v, (int, float)) for v in sample_vals):
            numeric_cols.append(col)
        else:
            categorical_cols.append(col)

    if not numeric_cols:
        return None, None, []

    # If no categorical columns, use the first numeric column as X
    if not categorical_cols:
        x_col = numeric_cols[0]
        y_cols = numeric_cols[1:4]
    else:
        x_col = categorical_cols[0]
        y_cols = numeric_cols[:3]

    if not y_cols:
        return None, None, []

    # Line if x column name contains time-like keyword
    is_time = any(kw in x_col.lower() for kw in DATE_KEYWORDS)
    chart_type = "line" if is_time else "bar"

    return chart_type, x_col, y_cols


def _serialise_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Replace NaN / non-JSON-serialisable values."""
    import math, datetime
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


def _preprocess_log_to_lines(log_entries: List[Dict[str, Any]]) -> List[str]:
    """Convert structured preprocessing log entries to UI-friendly lines."""
    lines: List[str] = []
    for entry in log_entries or []:
        if isinstance(entry, dict):
            message = entry.get("message")
            if not message:
                continue
            level = str(entry.get("level", "")).upper()
            prefix = f"[{level}] " if level else ""
            lines.append(f"{prefix}{message}")
        elif entry is not None:
            lines.append(str(entry))
    return lines


def _format_query_error(mode: str, error_msg: str) -> str:
    if mode != "scalable":
        return f"Query execution failed: {error_msg}"

    msg = error_msg.lower()

    if "pyspark is not available" in msg or "no module named 'pyspark'" in msg:
        return (
            "Scalable mode is unavailable because PySpark is not installed on the API server. "
            "Install pyspark and retry, or use smart/raw mode."
        )

    if (
        "java gateway process exited before sending its port number" in msg
        or "java" in msg and ("not found" in msg or "could not find" in msg)
    ):
        return (
            "Scalable mode requires a local Java runtime for Spark. "
            "Install Java and retry, or use smart/raw mode."
        )

    if "unsupportedclassversionerror" in msg or "class file version" in msg:
        return (
            "Scalable mode failed because Java is too old for the installed Spark version. "
            "Use Java 17+ and retry."
        )

    return f"Scalable mode failed during Spark processing: {error_msg}"
