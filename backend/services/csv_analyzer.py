"""
CSV Analyzer — extracts schema, statistics, and sample rows using DuckDB.
Only schema + sample (≤5 rows) are sent to the LLM; never the full dataset.
"""
import duckdb
from typing import Dict, Any, List


def analyze_csv(file_path: str) -> Dict[str, Any]:
    """
    Analyze a CSV file and return:
    - row_count
    - columns: list of {name, type, null_pct, mean, min, max, unique_count}
    - sample: first 5 rows as list of dicts
    """
    conn = duckdb.connect()
    try:
        # Load CSV into DuckDB
        conn.execute(
            f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{_escape(file_path)}')"
        )

        row_count: int = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]

        # Get schema
        raw_schema = conn.execute("DESCRIBE data").fetchall()
        # DESCRIBE returns: (column_name, column_type, null, key, default, extra)

        columns: List[Dict[str, Any]] = []
        for row in raw_schema:
            col_name: str = row[0]
            col_type: str = row[1]

            # Null percentage
            null_count: int = conn.execute(
                f'SELECT COUNT(*) FROM data WHERE "{col_name}" IS NULL'
            ).fetchone()[0]
            null_pct = round((null_count / row_count * 100), 2) if row_count > 0 else 0.0

            col_info: Dict[str, Any] = {
                "name": col_name,
                "type": col_type,
                "null_pct": null_pct,
            }

            # Numeric stats
            upper_type = col_type.upper()
            is_numeric = any(
                t in upper_type
                for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "BIGINT", "HUGEINT", "REAL", "NUMERIC"]
            )
            if is_numeric:
                try:
                    stats = conn.execute(
                        f"""
                        SELECT
                            AVG(CAST("{col_name}" AS DOUBLE)),
                            MIN(CAST("{col_name}" AS DOUBLE)),
                            MAX(CAST("{col_name}" AS DOUBLE))
                        FROM data
                        WHERE "{col_name}" IS NOT NULL
                        """
                    ).fetchone()
                    col_info["mean"] = round(stats[0], 4) if stats[0] is not None else None
                    col_info["min"] = stats[1]
                    col_info["max"] = stats[2]
                except Exception:
                    pass

            # Unique count (useful for categorical cardinality)
            try:
                unique_count = conn.execute(
                    f'SELECT COUNT(DISTINCT "{col_name}") FROM data'
                ).fetchone()[0]
                col_info["unique_count"] = unique_count
            except Exception:
                pass

            columns.append(col_info)

        # Sample rows (first 5 — sent to LLM as context)
        sample_df = conn.execute("SELECT * FROM data LIMIT 5").fetchdf()
        sample = sample_df.to_dict(orient="records")

        return {
            "row_count": row_count,
            "columns": columns,
            "sample": sample,
        }

    finally:
        conn.close()


def _escape(path: str) -> str:
    """Escape backslashes in file paths for DuckDB SQL strings."""
    return path.replace("\\", "/")
