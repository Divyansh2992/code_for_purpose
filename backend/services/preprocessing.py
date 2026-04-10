"""
Preprocessing Service (Smart Mode only).

Steps:
1. Load CSV into DuckDB as raw_data
2. Per-column quality analysis (null%, skewness, outliers)
3. Conditional imputation:
   - null% < threshold  → numeric: mean/median (based on skewness), categorical: mode/Unknown
   - null% >= threshold → warn, skip imputation
4. Create final "data" table (preprocessed) for query_engine
5. Return transparency log + outlier count + DuckDB connection
"""
import duckdb
from typing import List, Dict, Any, Tuple

NULL_THRESHOLD = 10.0   # % above which we skip imputation


def preprocess(
    file_path: str,
    columns: List[Dict[str, Any]],
) -> Tuple[List[str], int, duckdb.DuckDBPyConnection]:
    """
    Returns:
        log            - list of human-readable preprocessing messages
        outlier_count  - total outlier rows detected across numeric columns
        conn           - DuckDB connection with "data" table ready for querying
    """
    conn = duckdb.connect()
    conn.execute(
        f"CREATE TABLE raw_data AS SELECT * FROM read_csv_auto('{_escape(file_path)}')"
    )

    row_count: int = conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
    log: List[str] = []
    outlier_count: int = 0

    # Build SELECT clause with per-column transformations
    select_parts: List[str] = []

    for col in columns:
        col_name = col["name"]
        col_type = col["type"].upper()
        null_pct = col.get("null_pct", 0.0)
        is_numeric = any(
            t in col_type
            for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "BIGINT", "HUGEINT", "REAL", "NUMERIC"]
        )

        # No nulls — pass through unchanged
        if null_pct == 0.0:
            select_parts.append(f'"{col_name}"')
            continue

        # Above threshold — warn and pass through
        if null_pct > NULL_THRESHOLD:
            log.append(
                f"⚠️ '{col_name}': {null_pct:.1f}% missing — exceeds {NULL_THRESHOLD}% "
                f"threshold; imputation skipped to avoid bias"
            )
            select_parts.append(f'"{col_name}"')
            continue

        # ── Numeric imputation ──────────────────────────────────────────────
        if is_numeric:
            try:
                stats = conn.execute(
                    f"""
                    SELECT
                        AVG(CAST("{col_name}" AS DOUBLE))                          AS mean,
                        PERCENTILE_CONT(0.5) WITHIN GROUP
                            (ORDER BY CAST("{col_name}" AS DOUBLE))                AS median,
                        STDDEV(CAST("{col_name}" AS DOUBLE))                       AS stddev
                    FROM raw_data
                    WHERE "{col_name}" IS NOT NULL
                    """
                ).fetchone()
                mean_val, median_val, std_val = stats

                if mean_val is not None and median_val is not None:
                    # Simple skewness proxy: |mean - median| / stddev
                    skew_ratio = abs(mean_val - median_val) / max(std_val or 1e-9, 1e-9)
                    if skew_ratio > 0.5:
                        fill_val = round(median_val, 6)
                        method = f"median ({fill_val}) — skewed distribution detected (ratio={skew_ratio:.2f})"
                    else:
                        fill_val = round(mean_val, 6)
                        method = f"mean ({fill_val}) — symmetric distribution"

                    select_parts.append(
                        f'COALESCE(CAST("{col_name}" AS DOUBLE), {fill_val}) AS "{col_name}"'
                    )
                    log.append(f"✅ '{col_name}': {null_pct:.1f}% nulls filled using {method}")
                else:
                    select_parts.append(f'"{col_name}"')

            except Exception as exc:
                select_parts.append(f'"{col_name}"')
                log.append(f"⚠️ '{col_name}': imputation failed — {exc}")

        # ── Categorical imputation ─────────────────────────────────────────
        else:
            try:
                mode_row = conn.execute(
                    f"""
                    SELECT "{col_name}", COUNT(*) AS cnt
                    FROM raw_data
                    WHERE "{col_name}" IS NOT NULL
                    GROUP BY "{col_name}"
                    ORDER BY cnt DESC
                    LIMIT 1
                    """
                ).fetchone()

                if mode_row:
                    fill_str = str(mode_row[0]).replace("'", "''")
                    select_parts.append(
                        f"COALESCE(\"{col_name}\", '{fill_str}') AS \"{col_name}\""
                    )
                    log.append(
                        f"✅ '{col_name}': {null_pct:.1f}% nulls filled with mode value '{mode_row[0]}'"
                    )
                else:
                    select_parts.append(
                        f"COALESCE(\"{col_name}\", 'Unknown') AS \"{col_name}\""
                    )
                    log.append(f"✅ '{col_name}': {null_pct:.1f}% nulls filled with 'Unknown'")

            except Exception as exc:
                select_parts.append(f'"{col_name}"')
                log.append(f"⚠️ '{col_name}': categorical imputation failed — {exc}")

    # Build preprocessed "data" table
    select_sql = ", ".join(select_parts) if select_parts else "*"
    conn.execute(
        f"CREATE TABLE data AS SELECT {select_sql} FROM raw_data"
    )

    # ── Outlier detection (IQR) across numeric columns ─────────────────────
    for col in columns:
        col_name = col["name"]
        col_type = col["type"].upper()
        is_numeric = any(
            t in col_type
            for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "BIGINT", "HUGEINT", "REAL", "NUMERIC"]
        )
        if not is_numeric:
            continue

        try:
            iqr_row = conn.execute(
                f"""
                SELECT
                    PERCENTILE_CONT(0.25) WITHIN GROUP
                        (ORDER BY CAST("{col_name}" AS DOUBLE)) AS q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP
                        (ORDER BY CAST("{col_name}" AS DOUBLE)) AS q3
                FROM data
                WHERE "{col_name}" IS NOT NULL
                """
            ).fetchone()
            q1, q3 = iqr_row
            if q1 is not None and q3 is not None:
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                col_outliers: int = conn.execute(
                    f"""
                    SELECT COUNT(*) FROM data
                    WHERE CAST("{col_name}" AS DOUBLE) < {lower}
                       OR CAST("{col_name}" AS DOUBLE) > {upper}
                    """
                ).fetchone()[0]
                if col_outliers > 0:
                    outlier_count += col_outliers
                    log.append(
                        f"🔍 '{col_name}': {col_outliers} outliers detected "
                        f"(IQR bounds [{lower:.2f}, {upper:.2f}])"
                    )
        except Exception:
            pass

    return log, outlier_count, conn


def _escape(path: str) -> str:
    return path.replace("\\", "/")
