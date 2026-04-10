"""
Query Engine — executes SQL against a CSV file using DuckDB.
Accepts an optional pre-built connection (from Smart Mode preprocessing).
"""
import duckdb
from typing import List, Dict, Any, Tuple, Optional


def execute_query(
    file_path: str,
    sql: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Execute a SQL query and return (rows, column_names).

    If `conn` is provided (Smart Mode), it reuses the existing connection
    which already has the preprocessed "data" table loaded.
    The caller is responsible for closing the connection afterwards.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = duckdb.connect()
        conn.execute(
            f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{_escape(file_path)}')"
        )

    try:
        result_df = conn.execute(sql).fetchdf()
        columns = list(result_df.columns)
        # Replace NaN with None for JSON serialisation
        result_df = result_df.where(result_df.notna(), other=None)
        rows = result_df.to_dict(orient="records")
        return rows, columns
    finally:
        if owns_conn:
            conn.close()


def _escape(path: str) -> str:
    return path.replace("\\", "/")
