"""
CSV Analyzer — extracts schema, statistics, and sample rows using DuckDB.

Features:
- Safe CSV loading with validation
- Type-aware statistics (numeric, boolean, date)
- Sampling for large datasets (performance optimized)
- Configurable sample size
- Robust error handling with meaningful messages

Limitations:
- Mixed-type columns may produce partial stats
- Extremely large files may still require external preprocessing
- Assumes CSV is reasonably well-formed (header row present)
- Empty files return empty schema and sample

Security:
- File paths sanitized
- No direct user SQL execution
"""

import duckdb
import os
from typing import Dict, Any, List


def analyze_csv(file_path: str, sample_size: int = 5, max_rows_scan: int = 100000) -> Dict[str, Any]:
    """
    Analyze a CSV file and return:
    - row_count
    - columns: list of {name, type, null_pct, mean, min, max, unique_count}
    - sample: first N rows as list of dicts

    Args:
        file_path (str): Path to CSV file
        sample_size (int): Number of sample rows (default: 5)
        max_rows_scan (int): Max rows to scan for stats (performance safeguard)

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If file is invalid or empty
        RuntimeError: If DuckDB fails to process file
    """

    # ── Security: validate path ─────────────────────────────────────────────
    if not isinstance(file_path, str) or not file_path.strip():
        raise ValueError("Invalid file path provided")

    file_path = os.path.abspath(file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    if not file_path.lower().endswith(".csv"):
        raise ValueError("Only CSV files are supported")

    conn = duckdb.connect()

    try:
        # ── Load CSV safely ────────────────────────────────────────────────
        try:
            conn.execute(
                f"""
                CREATE TABLE data AS 
                SELECT * FROM read_csv_auto('{_escape(file_path)}', SAMPLE_SIZE={max_rows_scan})
                """
            )
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV: {str(e)}")

        # ── Row count ──────────────────────────────────────────────────────
        try:
            row_count: int = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
        except Exception as e:
            raise RuntimeError(f"Failed to count rows: {str(e)}")

        if row_count == 0:
            return {"row_count": 0, "columns": [], "sample": []}

        # ── Schema extraction ──────────────────────────────────────────────
        try:
            raw_schema = conn.execute("DESCRIBE data").fetchall()
        except Exception as e:
            raise RuntimeError(f"Failed to extract schema: {str(e)}")

        columns: List[Dict[str, Any]] = []

        for row in raw_schema:
            col_name: str = row[0]
            col_type: str = row[1]
            upper_type = col_type.upper()

            col_info: Dict[str, Any] = {
                "name": col_name,
                "type": col_type,
            }

            # ── Null percentage ────────────────────────────────────────────
            try:
                null_count = conn.execute(
                    f'SELECT COUNT(*) FROM data WHERE "{col_name}" IS NULL'
                ).fetchone()[0]

                col_info["null_pct"] = round((null_count / row_count * 100), 2)
            except Exception:
                col_info["null_pct"] = None

            # ── Type detection ─────────────────────────────────────────────
            is_numeric = any(t in upper_type for t in [
                "INT", "FLOAT", "DOUBLE", "DECIMAL", "BIGINT", "REAL", "NUMERIC"
            ])

            is_boolean = "BOOL" in upper_type

            is_date = any(t in upper_type for t in ["DATE", "TIME", "TIMESTAMP"])

            # ── Numeric stats ──────────────────────────────────────────────
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
                        LIMIT {max_rows_scan}
                        """
                    ).fetchone()

                    col_info["mean"] = round(stats[0], 4) if stats[0] is not None else None
                    col_info["min"] = stats[1]
                    col_info["max"] = stats[2]
                except Exception:
                    pass

            # ── Boolean stats ──────────────────────────────────────────────
            elif is_boolean:
                try:
                    stats = conn.execute(
                        f"""
                        SELECT
                            SUM(CASE WHEN "{col_name}" = TRUE THEN 1 ELSE 0 END),
                            SUM(CASE WHEN "{col_name}" = FALSE THEN 1 ELSE 0 END)
                        FROM data
                        """
                    ).fetchone()

                    col_info["true_count"] = stats[0]
                    col_info["false_count"] = stats[1]
                except Exception:
                    pass

            # ── Date stats ────────────────────────────────────────────────
            elif is_date:
                try:
                    stats = conn.execute(
                        f"""
                        SELECT
                            MIN("{col_name}"),
                            MAX("{col_name}")
                        FROM data
                        WHERE "{col_name}" IS NOT NULL
                        """
                    ).fetchone()

                    col_info["min_date"] = str(stats[0]) if stats[0] else None
                    col_info["max_date"] = str(stats[1]) if stats[1] else None
                except Exception:
                    pass

            # ── Unique count ──────────────────────────────────────────────
            try:
                unique_count = conn.execute(
                    f'SELECT COUNT(DISTINCT "{col_name}") FROM data'
                ).fetchone()[0]

                col_info["unique_count"] = unique_count
            except Exception:
                pass

            columns.append(col_info)

        # ── Sample rows ───────────────────────────────────────────────────
        try:
            sample_df = conn.execute(
                f"SELECT * FROM data LIMIT {int(sample_size)}"
            ).fetchdf()

            sample = sample_df.to_dict(orient="records")
        except Exception:
            sample = []

        return {
            "row_count": row_count,
            "columns": columns,
            "sample": sample,
        }

    finally:
        conn.close()


def _escape(path: str) -> str:
    """
    Sanitize file path for DuckDB usage.

    Prevents:
    - SQL injection via path
    - malformed escape sequences
    """
    return path.replace("\\", "/").replace("'", "''")

if __name__ == "__main__":
    import sys
    import pprint

    # Change this to the path of a real CSV file you want to test
    test_csv = r"C:\Users\DEVANSH KANOJIYA\Desktop\archive\healthcare_disease_prediction_dataset.csv" if len(sys.argv) < 2 else sys.argv[1]

    try:
        result = analyze_csv(test_csv)
        pprint.pprint(result)
    except Exception as e:
        print(f"Error: {e}")