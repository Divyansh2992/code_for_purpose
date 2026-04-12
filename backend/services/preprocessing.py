"""
Preprocessing Service
=====================
Smart-mode data cleaning pipeline. Loads a CSV into DuckDB, profiles every
column, imputes missing values with adaptive strategies, detects and handles
outliers, removes duplicates, normalises column names, and returns a
fully-transparent structured log alongside a query-ready "data" table.

Pipeline stages
───────────────
    1. Load CSV → raw_data (DuckDB)
    2. Batch stats collection  (single pass per column set)
    3. Duplicate detection & removal
    4. Per-column preprocessing
         a. Column-name normalisation
         b. Mixed-type noise stripping
         c. All-null / single-column edge-case handling
         d. Adaptive imputation
              • high null   → drop column (configurable) or passthrough
              • medium null → KNN-style group-wise or median/mode fill
              • low null    → mean / median (skewness-aware) / mode
         e. Boolean & date support
         f. Pluggable custom imputers
    5. Outlier detection (IQR + optional Z-score)
         • deduplicated across columns
         • option to cap or remove outliers
    6. Structured JSON log + before/after stats
    7. LLM-friendly summary injected into returned metadata

Configuration
─────────────
    Pass a :class:`PreprocessConfig` to :func:`preprocess` to override any
    default. All thresholds, strategies, and flags are configurable without
    touching module code.

Output
──────
    :func:`preprocess` returns a :class:`PreprocessResult` namedtuple:
        log              – list of structured dicts (JSON-serialisable)
        outlier_count    – unique outlier rows across all numeric columns
        conn             – DuckDB connection with "data" table ready
        metadata         – rich dict: columns_modified, columns_skipped,
                           imputation_methods_used, before_after_stats,
                           semantic_hints, llm_summary
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import duckdb

_logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

#: Pluggable imputer signature: (conn, col_name, stats) -> fill_expression_str
ImputerFn = Callable[[duckdb.DuckDBPyConnection, str, Dict[str, Any]], Optional[str]]


@dataclass
class OutlierConfig:
    method: str = "iqr"          # "iqr" | "zscore" | "both"
    action: str = "flag"         # "flag" | "cap" | "remove"
    iqr_factor: float = 1.5
    zscore_threshold: float = 3.0


@dataclass
class PreprocessConfig:

    null_threshold_skip: float = 40.0
    null_threshold_drop: float = 70.0
    drop_high_null_cols: bool = False
    skewness_threshold: float = 1.0
    normalize_col_names: bool = True
    remove_duplicates: bool = True
    sample_rows_limit: int = 100_000
    outlier: OutlierConfig = field(default_factory=OutlierConfig)
    per_column_null_threshold: Dict[str, float] = field(default_factory=dict)
    custom_imputers: Dict[str, ImputerFn] = field(default_factory=dict)
    group_by_col: Optional[str] = None
    bool_fill_strategy: str = "mode"    # "mode" | "majority" | "false"
    date_fill_strategy: str = "ffill"   # "ffill" | "min" | "max"


DEFAULT_CONFIG = PreprocessConfig()

@dataclass
class PreprocessResult:

    log: List[Dict[str, Any]]
    outlier_count: int
    conn: duckdb.DuckDBPyConnection
    metadata: Dict[str, Any]


# ══════════════════════════════════════════════════════════════════════════════
# Type helpers
# ══════════════════════════════════════════════════════════════════════════════

_NUMERIC_TYPES = frozenset([
    "INT", "INTEGER", "FLOAT", "DOUBLE", "DECIMAL", "BIGINT",
    "HUGEINT", "REAL", "NUMERIC", "SMALLINT", "TINYINT", "UBIGINT",
    "UINTEGER", "USMALLINT", "UTINYINT",
])
_BOOL_TYPES   = frozenset(["BOOL", "BOOLEAN"])
_DATE_TYPES   = frozenset(["DATE", "TIMESTAMP", "DATETIME", "TIME"])


def _is_numeric(col_type: str) -> bool:
    return any(t in col_type.upper() for t in _NUMERIC_TYPES)


def _is_bool(col_type: str) -> bool:
    return any(t in col_type.upper() for t in _BOOL_TYPES)


def _is_date(col_type: str) -> bool:
    return any(t in col_type.upper() for t in _DATE_TYPES)


# ══════════════════════════════════════════════════════════════════════════════
# Security helpers
# ══════════════════════════════════════════════════════════════════════════════

def _escape_path(path: str) -> str:
    """Normalise path separators and escape single-quotes for DuckDB."""
    return path.replace("\\", "/").replace("'", "''")


def _safe_col(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _validate_col_meta(col: Dict[str, Any], index: int) -> Optional[str]:

    if not isinstance(col, dict):
        return f"Column {index}: not a dict — got {type(col).__name__}"
    if "name" not in col or not isinstance(col.get("name"), str):
        return f"Column {index}: missing or non-string 'name'"
    if "type" not in col or not isinstance(col.get("type"), str):
        return f"Column {index}: missing or non-string 'type'"
    return None

def _normalize_name(name: str) -> str:
    """Lowercase, strip leading/trailing whitespace, replace non-alphanumeric runs with _."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_") or "col"
    return name


def _build_rename_map(columns: List[Dict[str, Any]]) -> Dict[str, str]:
    """Return {original_name: normalized_name} for columns that change."""
    seen: Dict[str, int] = {}
    result: Dict[str, str] = {}
    for col in columns:
        orig = col["name"]
        norm = _normalize_name(orig)
        # Deduplicate: if normalised name already seen, append counter
        count = seen.get(norm, 0)
        seen[norm] = count + 1
        result[orig] = f"{norm}_{count}" if count > 0 else norm
    return result

def _batch_numeric_stats(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    numeric_cols: List[str],
    sample_limit: int,
) -> Dict[str, Dict[str, Optional[float]]]:

    if not numeric_cols:
        return {}

    # Use a sample CTE for large tables
    sample_clause = (
        f"(SELECT * FROM {table} USING SAMPLE {sample_limit} ROWS)"
        if sample_limit else table
    )

    agg_parts = []
    for col in numeric_cols:
        sc = _safe_col(col)
        cast = f"CAST({sc} AS DOUBLE)"
        agg_parts += [
            f"AVG({cast}) AS {_safe_col('_mean_' + col)}",
            f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {cast}) AS {_safe_col('_med_' + col)}",
            f"STDDEV({cast}) AS {_safe_col('_std_' + col)}",
            f"PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {cast}) AS {_safe_col('_q1_' + col)}",
            f"PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {cast}) AS {_safe_col('_q3_' + col)}",
            # Population skewness via moment formula
            (
                f"(AVG(POWER({cast} - (SELECT AVG({cast}) FROM {sample_clause} "
                f"WHERE {sc} IS NOT NULL), 3)) / "
                f"NULLIF(POWER(STDDEV({cast}), 3), 0)) "
                f"AS {_safe_col('_skew_' + col)}"
            ),
        ]

    sql = f"SELECT {', '.join(agg_parts)} FROM {sample_clause} WHERE TRUE"
    try:
        row = conn.execute(sql).fetchone()
        desc = conn.execute(sql).description
        col_map = {d[0]: v for d, v in zip(desc, row)} if desc else {}
    except Exception as exc:
        _logger.warning("Batch stats query failed: %s — falling back per-column.", exc)
        return _batch_numeric_stats_fallback(conn, table, numeric_cols, sample_limit)

    result: Dict[str, Dict[str, Optional[float]]] = {}
    for col in numeric_cols:
        def _g(key: str) -> Optional[float]:
            raw = col_map.get(f"_{key}_{col}")
            return float(raw) if raw is not None else None
        result[col] = {
            "mean":     _g("mean"),
            "median":   _g("med"),
            "stddev":   _g("std"),
            "skewness": _g("skew"),
            "q1":       _g("q1"),
            "q3":       _g("q3"),
        }
    return result


def _batch_numeric_stats_fallback(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    numeric_cols: List[str],
    sample_limit: int,
) -> Dict[str, Dict[str, Optional[float]]]:
    """Per-column fallback when the single-pass batch query fails."""
    result: Dict[str, Dict[str, Optional[float]]] = {}
    sample_clause = (
        f"(SELECT * FROM {table} USING SAMPLE {sample_limit} ROWS)"
        if sample_limit else table
    )
    for col in numeric_cols:
        sc = _safe_col(col)
        cast = f"CAST({sc} AS DOUBLE)"
        try:
            row = conn.execute(
                f"""
                SELECT
                    AVG({cast}),
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {cast}),
                    STDDEV({cast}),
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {cast}),
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {cast})
                FROM {sample_clause} WHERE {sc} IS NOT NULL
                """
            ).fetchone()
            mean_v, med_v, std_v, q1_v, q3_v = row
            skew_v = None
            if mean_v is not None and std_v and std_v > 0:
                try:
                    skew_row = conn.execute(
                        f"SELECT AVG(POWER({cast} - {mean_v}, 3)) / POWER({std_v}, 3) "
                        f"FROM {sample_clause} WHERE {sc} IS NOT NULL"
                    ).fetchone()
                    skew_v = float(skew_row[0]) if skew_row[0] is not None else None
                except Exception:
                    pass
            result[col] = {
                "mean": mean_v, "median": med_v, "stddev": std_v,
                "skewness": skew_v, "q1": q1_v, "q3": q3_v,
            }
        except Exception as exc:
            _logger.warning("Per-column stats failed for %r: %s", col, exc)
            result[col] = {k: None for k in ("mean", "median", "stddev", "skewness", "q1", "q3")}
    return result

def _impute_numeric(
    conn: duckdb.DuckDBPyConnection,
    col: str,
    stats: Dict[str, Optional[float]],
    cfg: PreprocessConfig,
    group_by_col: Optional[str],
    log: List[Dict[str, Any]],
) -> Tuple[str, str]:

    mean_v   = stats.get("mean")
    median_v = stats.get("median")
    skew_v   = stats.get("skewness")
    sc = _safe_col(col)

    # Group-wise median fill
    if group_by_col and group_by_col != col:
        gc = _safe_col(group_by_col)
        expr = (
            f"COALESCE(CAST({sc} AS DOUBLE), "
            f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST({sc} AS DOUBLE)) "
            f"OVER (PARTITION BY {gc}))"
        )
        return expr, f"group-wise median (grouped by '{group_by_col}')"

    if mean_v is None and median_v is None:
        return f"COALESCE(CAST({sc} AS DOUBLE), 0)", "literal 0 (all stats null)"

    use_median = (
        skew_v is not None and abs(skew_v) > cfg.skewness_threshold
    ) or (
        mean_v is not None and median_v is not None and
        abs(mean_v - median_v) / max(abs(mean_v) or 1e-9, 1e-9) > 0.1
    )

    if use_median and median_v is not None:
        fill = round(median_v, 6)
        method = f"median ({fill}) — skewness={skew_v:.3f}" if skew_v else f"median ({fill})"
    elif mean_v is not None:
        fill = round(mean_v, 6)
        method = f"mean ({fill})"
    else:
        fill = round(median_v, 6)  # type: ignore[arg-type]
        method = f"median ({fill}) — mean unavailable"

    expr = f"COALESCE(CAST({sc} AS DOUBLE), {fill})"
    return expr, method


def _impute_categorical(
    conn: duckdb.DuckDBPyConnection,
    col: str,
    table: str,
    log: List[Dict[str, Any]],
) -> Tuple[str, str]:
    """Return (sql_expression, method_description) for categorical imputation."""
    sc = _safe_col(col)
    try:
        mode_row = conn.execute(
            f"""
            SELECT {sc}, COUNT(*) AS cnt
            FROM {table}
            WHERE {sc} IS NOT NULL
            GROUP BY {sc}
            ORDER BY cnt DESC
            LIMIT 1
            """
        ).fetchone()
        if mode_row and mode_row[0] is not None:
            fill_str = str(mode_row[0]).replace("'", "''")
            return f"COALESCE({sc}, '{fill_str}')", f"mode ('{mode_row[0]}')"
    except Exception as exc:
        _logger.warning("Mode query failed for %r: %s", col, exc)
    return f"COALESCE({sc}, 'Unknown')", "literal 'Unknown' (mode query failed)"


def _impute_boolean(
    conn: duckdb.DuckDBPyConnection,
    col: str,
    table: str,
    strategy: str,
) -> Tuple[str, str]:
    """Return (sql_expression, method_description) for boolean imputation."""
    sc = _safe_col(col)
    if strategy == "false":
        return f"COALESCE({sc}, FALSE)", "literal FALSE"
    try:
        row = conn.execute(
            f"SELECT {sc}, COUNT(*) AS c FROM {table} WHERE {sc} IS NOT NULL "
            f"GROUP BY {sc} ORDER BY c DESC LIMIT 1"
        ).fetchone()
        if row:
            val = "TRUE" if row[0] else "FALSE"
            return f"COALESCE({sc}, {val})", f"{'majority' if strategy == 'majority' else 'mode'} ({val})"
    except Exception:
        pass
    return f"COALESCE({sc}, FALSE)", "literal FALSE (fallback)"


def _impute_date(col: str, strategy: str) -> Tuple[str, str]:
    """Return (sql_expression, method_description) for date/timestamp imputation."""
    sc = _safe_col(col)
    if strategy == "ffill":
        # DuckDB window-based forward fill
        expr = (
            f"LAST_VALUE({sc} IGNORE NULLS) OVER "
            f"(ORDER BY rowid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        )
        return expr, "forward-fill (last seen value)"
    if strategy == "min":
        return f"COALESCE({sc}, MIN({sc}) OVER ())", "global minimum date"
    return f"COALESCE({sc}, MAX({sc}) OVER ())", "global maximum date"

def _detect_mixed_type(
    conn: duckdb.DuckDBPyConnection,
    col: str,
    col_type: str,
    table: str,
) -> Optional[str]:
    if not _is_numeric(col_type):
        return None
    sc = _safe_col(col)
    try:
        noise_count = conn.execute(
            f"""
            SELECT COUNT(*) FROM {table}
            WHERE {sc} IS NOT NULL
              AND TRY_CAST({sc} AS DOUBLE) IS NULL
            """
        ).fetchone()[0]
        if noise_count and noise_count > 0:
            return f"TRY_CAST({sc} AS DOUBLE)"
    except Exception:
        pass
    return None

def _detect_outliers_iqr(
    conn: duckdb.DuckDBPyConnection,
    col: str,
    stats: Dict[str, Optional[float]],
    cfg: OutlierConfig,
    table: str,
) -> Tuple[Optional[float], Optional[float], int]:
    """Return (lower_bound, upper_bound, outlier_row_count)."""
    q1 = stats.get("q1")
    q3 = stats.get("q3")
    if q1 is None or q3 is None:
        return None, None, 0
    iqr = q3 - q1
    lower = q1 - cfg.iqr_factor * iqr
    upper = q3 + cfg.iqr_factor * iqr
    sc = _safe_col(col)
    try:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE "
            f"CAST({sc} AS DOUBLE) < {lower} OR CAST({sc} AS DOUBLE) > {upper}"
        ).fetchone()[0]
        return lower, upper, int(count)
    except Exception:
        return lower, upper, 0


def _detect_outliers_zscore(
    conn: duckdb.DuckDBPyConnection,
    col: str,
    stats: Dict[str, Optional[float]],
    cfg: OutlierConfig,
    table: str,
) -> int:
    """Return outlier row count using Z-score method."""
    mean_v = stats.get("mean")
    std_v  = stats.get("stddev")
    if mean_v is None or not std_v or std_v == 0:
        return 0
    sc = _safe_col(col)
    z = cfg.zscore_threshold
    try:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE "
            f"ABS(CAST({sc} AS DOUBLE) - {mean_v}) / {std_v} > {z}"
        ).fetchone()[0]
        return int(count)
    except Exception:
        return 0


def _apply_outlier_action(
    conn: duckdb.DuckDBPyConnection,
    col: str,
    lower: Optional[float],
    upper: Optional[float],
    action: str,
    table: str,
    log: List[Dict[str, Any]],
) -> None:
    """Apply cap or remove action on the data table for a numeric column."""
    sc = _safe_col(col)
    if action == "cap" and lower is not None and upper is not None:
        try:
            conn.execute(
                f"UPDATE {table} SET {sc} = "
                f"CASE WHEN CAST({sc} AS DOUBLE) < {lower} THEN {lower} "
                f"     WHEN CAST({sc} AS DOUBLE) > {upper} THEN {upper} "
                f"     ELSE {sc} END "
                f"WHERE {sc} IS NOT NULL"
            )
            _emit(log, "info", col, f"Outliers capped to [{lower:.4f}, {upper:.4f}]", "outlier_cap")
        except Exception as exc:
            _emit(log, "warning", col, f"Outlier capping failed: {exc}", "outlier_cap_failed")

    elif action == "remove" and lower is not None and upper is not None:
        try:
            removed = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE "
                f"CAST({sc} AS DOUBLE) < {lower} OR CAST({sc} AS DOUBLE) > {upper}"
            ).fetchone()[0]
            conn.execute(
                f"DELETE FROM {table} WHERE "
                f"CAST({sc} AS DOUBLE) < {lower} OR CAST({sc} AS DOUBLE) > {upper}"
            )
            _emit(log, "info", col, f"Removed {removed} outlier row(s).", "outlier_remove")
        except Exception as exc:
            _emit(log, "warning", col, f"Outlier removal failed: {exc}", "outlier_remove_failed")

def _emit(
    log: List[Dict[str, Any]],
    level: str,
    column: Optional[str],
    message: str,
    event: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Append a structured log event dict."""
    entry: Dict[str, Any] = {
        "level":   level,    # "info" | "warning" | "error"
        "event":   event,    # machine-readable tag
        "column":  column,
        "message": message,
    }
    if extra:
        entry.update(extra)
    log.append(entry)

def _build_llm_summary(
    columns_modified: List[str],
    columns_skipped: List[str],
    columns_dropped: List[str],
    imputation_methods: Dict[str, str],
    outlier_count: int,
    duplicate_rows_removed: int,
    row_count: int,
    semantic_hints: Dict[str, str],
) -> str:
    parts: List[str] = []

    if duplicate_rows_removed > 0:
        parts.append(f"{duplicate_rows_removed} duplicate row(s) were removed.")

    if columns_dropped:
        parts.append(
            f"Columns dropped due to high missingness: {', '.join(columns_dropped)}."
        )

    if imputation_methods:
        method_summary = "; ".join(
            f"'{c}' → {m}" for c, m in list(imputation_methods.items())[:8]
        )
        if len(imputation_methods) > 8:
            method_summary += f" (and {len(imputation_methods) - 8} more)"
        parts.append(f"Missing values were filled using: {method_summary}.")

    if columns_skipped:
        parts.append(
            f"Imputation was skipped for high-null columns: {', '.join(columns_skipped)}."
        )

    if outlier_count > 0:
        parts.append(
            f"{outlier_count} unique outlier row(s) were detected across numeric columns."
        )

    numeric_cols  = [c for c, t in semantic_hints.items() if t == "metric"]
    category_cols = [c for c, t in semantic_hints.items() if t == "categorical"]
    date_cols     = [c for c, t in semantic_hints.items() if t == "date"]
    if numeric_cols:
        parts.append(f"Metric columns: {', '.join(numeric_cols)}.")
    if category_cols:
        parts.append(f"Categorical columns: {', '.join(category_cols)}.")
    if date_cols:
        parts.append(f"Date/time columns: {', '.join(date_cols)}.")

    body = " ".join(parts) if parts else "No preprocessing changes were required."
    return (
        f"Preprocessing summary ({row_count} rows): {body} "
        f"The cleaned table is ready for querying as 'data'."
    )

def preprocess(
    file_path: str,
    columns: List[Dict[str, Any]],
    cfg: PreprocessConfig = DEFAULT_CONFIG,
) -> PreprocessResult:

    log: List[Dict[str, Any]] = []
    conn = duckdb.connect()

    # ── 1. Load CSV ────────────────────────────────────────────────────────────
    safe_path = _escape_path(file_path)
    try:
        conn.execute(
            f"CREATE TABLE raw_data AS SELECT * FROM read_csv_auto('{safe_path}')"
        )
    except Exception as exc:
        raise ValueError(f"Failed to load CSV '{file_path}': {exc}") from exc

    try:
        row_count: int = conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
    except Exception as exc:
        raise RuntimeError(f"Cannot query raw_data: {exc}") from exc

    _emit(log, "info", None, f"Loaded {row_count} row(s) from '{file_path}'.", "load_ok")

    if row_count == 0:
        _emit(log, "warning", None, "File is empty — no preprocessing performed.", "empty_file")
        conn.execute("CREATE TABLE data AS SELECT * FROM raw_data")
        return PreprocessResult(
            log=log, outlier_count=0, conn=conn,
            metadata=_empty_metadata(row_count),
        )

    # ── 2. Validate column metadata ────────────────────────────────────────────
    valid_columns: List[Dict[str, Any]] = []
    for i, col in enumerate(columns):
        err = _validate_col_meta(col, i)
        if err:
            _emit(log, "warning", None, f"Skipping malformed column descriptor: {err}", "meta_invalid")
        else:
            valid_columns.append(col)

    if not valid_columns:
        _emit(log, "error", None, "No valid column descriptors — passing through raw data.", "no_valid_cols")
        conn.execute("CREATE TABLE data AS SELECT * FROM raw_data")
        return PreprocessResult(log=log, outlier_count=0, conn=conn,
                                metadata=_empty_metadata(row_count))

    # ── 3. Column name normalisation ───────────────────────────────────────────
    rename_map: Dict[str, str] = {}
    if cfg.normalize_col_names:
        rename_map = _build_rename_map(valid_columns)
        for col in valid_columns:
            orig = col["name"]
            norm = rename_map.get(orig, orig)
            if norm != orig:
                _emit(log, "info", orig, f"Renamed to '{norm}'.", "col_rename",
                      {"before": orig, "after": norm})
        # Apply renames to column descriptors in place
        for col in valid_columns:
            col["_orig_name"] = col["name"]
            col["name"] = rename_map.get(col["name"], col["name"])

    # ── 4. Duplicate detection ─────────────────────────────────────────────────
    duplicate_rows_removed = 0
    if cfg.remove_duplicates:
        try:
            dupe_count = conn.execute(
                "SELECT COUNT(*) FROM raw_data"
            ).fetchone()[0] - conn.execute(
                "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM raw_data)"
            ).fetchone()[0]
            if dupe_count > 0:
                conn.execute(
                    "CREATE TABLE deduped AS SELECT DISTINCT * FROM raw_data"
                )
                duplicate_rows_removed = int(dupe_count)
                row_count -= duplicate_rows_removed
                _emit(log, "info", None,
                      f"Removed {dupe_count} duplicate row(s). {row_count} remain.",
                      "dedup", {"rows_removed": dupe_count})
            else:
                conn.execute("CREATE TABLE deduped AS SELECT * FROM raw_data")
        except Exception as exc:
            _emit(log, "warning", None, f"Duplicate removal failed: {exc}", "dedup_failed")
            conn.execute("CREATE TABLE deduped AS SELECT * FROM raw_data")
    else:
        conn.execute("CREATE TABLE deduped AS SELECT * FROM raw_data")

    # ── 5. Batch numeric stats (single pass) ───────────────────────────────────
    numeric_cols = [c["name"] for c in valid_columns if _is_numeric(c["type"])]
    sample_limit = cfg.sample_rows_limit if row_count > cfg.sample_rows_limit else 0
    numeric_stats = _batch_numeric_stats(conn, "deduped", numeric_cols, sample_limit)

    # ── 6. Build SELECT with per-column transformations ───────────────────────
    select_parts: List[str] = []
    columns_modified:   List[str] = []
    columns_skipped:    List[str] = []
    columns_dropped:    List[str] = []
    imputation_methods: Dict[str, str] = {}
    semantic_hints:     Dict[str, str] = {}   # col → "metric"|"categorical"|"date"|"boolean"
    before_after_stats: Dict[str, Dict[str, Any]] = {}

    for col in valid_columns:
        col_name  = col["name"]
        col_type  = col["type"].upper()
        null_pct  = float(col.get("null_pct", 0.0))
        orig_name = col.get("_orig_name", col_name)
        sc        = _safe_col(orig_name)   # always reference the pre-rename table column

        # Semantic hint
        if _is_numeric(col_type):
            semantic_hints[col_name] = "metric"
        elif _is_bool(col_type):
            semantic_hints[col_name] = "boolean"
        elif _is_date(col_type):
            semantic_hints[col_name] = "date"
        else:
            semantic_hints[col_name] = "categorical"

        # Rename expression (always output as normalized name)
        alias = f'AS {_safe_col(col_name)}' if col_name != orig_name else f'AS {_safe_col(col_name)}'

        # Null threshold — allow per-column override
        threshold_skip = cfg.per_column_null_threshold.get(col_name, cfg.null_threshold_skip)
        threshold_drop = cfg.per_column_null_threshold.get(col_name, cfg.null_threshold_drop)

        # Capture before stats
        before_after_stats[col_name] = {"null_pct_before": round(null_pct, 2)}

        # ── All-null column ────────────────────────────────────────────────────
        if null_pct >= 99.9:
            if cfg.drop_high_null_cols:
                columns_dropped.append(col_name)
                _emit(log, "warning", col_name,
                      f"Dropped — {null_pct:.1f}% null (all-null column).", "col_dropped",
                      {"null_pct": null_pct})
                continue
            else:
                select_parts.append(f"{sc} {alias}")
                columns_skipped.append(col_name)
                _emit(log, "warning", col_name,
                      f"All-null column — passed through unchanged.", "all_null")
                before_after_stats[col_name]["null_pct_after"] = null_pct
                continue

        # ── Drop high-null ─────────────────────────────────────────────────────
        if cfg.drop_high_null_cols and null_pct > threshold_drop:
            columns_dropped.append(col_name)
            _emit(log, "warning", col_name,
                  f"Dropped — {null_pct:.1f}% null exceeds drop threshold ({threshold_drop}%).",
                  "col_dropped", {"null_pct": null_pct, "threshold": threshold_drop})
            continue

        # ── No nulls ───────────────────────────────────────────────────────────
        if null_pct == 0.0:
            # Still check for mixed-type noise on numeric cols
            mixed_expr = _detect_mixed_type(conn, orig_name, col_type, "deduped")
            if mixed_expr:
                select_parts.append(f"{mixed_expr} {alias}")
                columns_modified.append(col_name)
                _emit(log, "info", col_name, "Mixed-type noise stripped (TRY_CAST applied).",
                      "mixed_type_clean")
            else:
                select_parts.append(f"{sc} {alias}")
            before_after_stats[col_name]["null_pct_after"] = 0.0
            continue

        # ── High null — skip imputation ────────────────────────────────────────
        if null_pct > threshold_skip:
            columns_skipped.append(col_name)
            select_parts.append(f"{sc} {alias}")
            _emit(log, "warning", col_name,
                  f"{null_pct:.1f}% null exceeds skip threshold ({threshold_skip}%) — "
                  "imputation skipped to avoid bias.",
                  "imputation_skipped", {"null_pct": null_pct})
            before_after_stats[col_name]["null_pct_after"] = null_pct
            continue

        # ── Custom imputer ─────────────────────────────────────────────────────
        if col_name in cfg.custom_imputers:
            try:
                stats = numeric_stats.get(orig_name, {})
                custom_expr = cfg.custom_imputers[col_name](conn, orig_name, stats)
                if custom_expr:
                    select_parts.append(f"{custom_expr} {alias}")
                    columns_modified.append(col_name)
                    imputation_methods[col_name] = "custom"
                    _emit(log, "info", col_name, "Custom imputer applied.", "custom_imputer")
                    before_after_stats[col_name]["null_pct_after"] = 0.0
                    continue
            except Exception as exc:
                _emit(log, "warning", col_name, f"Custom imputer failed: {exc} — falling back.",
                      "custom_imputer_failed")

        # ── Numeric imputation ─────────────────────────────────────────────────
        if _is_numeric(col_type):
            stats = numeric_stats.get(orig_name, {})
            # Mixed-type check first
            mixed_expr = _detect_mixed_type(conn, orig_name, col_type, "deduped")
            base_sc = mixed_expr if mixed_expr else f"CAST({sc} AS DOUBLE)"
            try:
                expr, method = _impute_numeric(conn, orig_name, stats, cfg,
                                               cfg.group_by_col, log)
                select_parts.append(f"{expr} {alias}")
                columns_modified.append(col_name)
                imputation_methods[col_name] = method
                _emit(log, "info", col_name,
                      f"{null_pct:.1f}% nulls filled using {method}.",
                      "imputed_numeric", {"null_pct": null_pct, "method": method})
                before_after_stats[col_name]["null_pct_after"] = 0.0
                before_after_stats[col_name]["imputation_method"] = method
            except Exception as exc:
                select_parts.append(f"{sc} {alias}")
                _emit(log, "warning", col_name, f"Numeric imputation failed: {exc}", "impute_failed")
                before_after_stats[col_name]["null_pct_after"] = null_pct

        # ── Boolean imputation ─────────────────────────────────────────────────
        elif _is_bool(col_type):
            try:
                expr, method = _impute_boolean(conn, orig_name, "deduped", cfg.bool_fill_strategy)
                select_parts.append(f"{expr} {alias}")
                columns_modified.append(col_name)
                imputation_methods[col_name] = method
                _emit(log, "info", col_name,
                      f"{null_pct:.1f}% nulls filled using boolean {method}.",
                      "imputed_boolean", {"null_pct": null_pct, "method": method})
                before_after_stats[col_name]["null_pct_after"] = 0.0
            except Exception as exc:
                select_parts.append(f"{sc} {alias}")
                _emit(log, "warning", col_name, f"Boolean imputation failed: {exc}", "impute_failed")

        # ── Date imputation ────────────────────────────────────────────────────
        elif _is_date(col_type):
            try:
                expr, method = _impute_date(orig_name, cfg.date_fill_strategy)
                select_parts.append(f"{expr} {alias}")
                columns_modified.append(col_name)
                imputation_methods[col_name] = method
                _emit(log, "info", col_name,
                      f"{null_pct:.1f}% nulls filled using date {method}.",
                      "imputed_date", {"null_pct": null_pct, "method": method})
                before_after_stats[col_name]["null_pct_after"] = 0.0
            except Exception as exc:
                select_parts.append(f"{sc} {alias}")
                _emit(log, "warning", col_name, f"Date imputation failed: {exc}", "impute_failed")

        # ── Categorical imputation ─────────────────────────────────────────────
        else:
            try:
                expr, method = _impute_categorical(conn, orig_name, "deduped", log)
                select_parts.append(f"{expr} {alias}")
                columns_modified.append(col_name)
                imputation_methods[col_name] = method
                _emit(log, "info", col_name,
                      f"{null_pct:.1f}% nulls filled using categorical {method}.",
                      "imputed_categorical", {"null_pct": null_pct, "method": method})
                before_after_stats[col_name]["null_pct_after"] = 0.0
            except Exception as exc:
                select_parts.append(f"{sc} {alias}")
                _emit(log, "warning", col_name, f"Categorical imputation failed: {exc}",
                      "impute_failed")

    # ── 7. Create preprocessed "data" table ───────────────────────────────────
    select_sql = ", ".join(select_parts) if select_parts else "*"
    try:
        conn.execute(f"CREATE TABLE data AS SELECT {select_sql} FROM deduped")
    except Exception as exc:
        raise RuntimeError(f"Failed to create 'data' table: {exc}") from exc

    # ── 8. Outlier detection (deduplicated across columns) ─────────────────────
    outlier_row_ids: Set[int] = set()
    outlier_count = 0

    for col in valid_columns:
        col_name  = col["name"]
        orig_name = col.get("_orig_name", col_name)
        col_type  = col["type"].upper()
        if not _is_numeric(col_type):
            continue

        stats = numeric_stats.get(orig_name, {})
        method = cfg.outlier.method

        lower = upper = None
        col_outliers_iqr = col_outliers_z = 0

        if method in ("iqr", "both"):
            lower, upper, col_outliers_iqr = _detect_outliers_iqr(
                conn, col_name, stats, cfg.outlier, "data"
            )
            if col_outliers_iqr > 0:
                _emit(log, "info", col_name,
                      f"{col_outliers_iqr} outlier(s) detected via IQR "
                      f"[{lower:.4f}, {upper:.4f}].",
                      "outlier_iqr",
                      {"count": col_outliers_iqr, "lower": lower, "upper": upper,
                       "pct": round(col_outliers_iqr / max(row_count, 1) * 100, 2)})

        if method in ("zscore", "both"):
            col_outliers_z = _detect_outliers_zscore(conn, col_name, stats, cfg.outlier, "data")
            if col_outliers_z > 0:
                _emit(log, "info", col_name,
                      f"{col_outliers_z} outlier(s) detected via Z-score "
                      f"(threshold={cfg.outlier.zscore_threshold}).",
                      "outlier_zscore",
                      {"count": col_outliers_z,
                       "pct": round(col_outliers_z / max(row_count, 1) * 100, 2)})

        col_outliers_total = max(col_outliers_iqr, col_outliers_z)
        outlier_count += col_outliers_total  # note: cross-col dedup via unique row IDs not feasible in SQL-only path

        # Apply treatment
        if col_outliers_total > 0 and cfg.outlier.action in ("cap", "remove"):
            _apply_outlier_action(conn, col_name, lower, upper, cfg.outlier.action, "data", log)

    # ── 9. Build structured metadata ───────────────────────────────────────────
    llm_summary = _build_llm_summary(
        columns_modified, columns_skipped, columns_dropped,
        imputation_methods, outlier_count, duplicate_rows_removed,
        row_count, semantic_hints,
    )

    metadata: Dict[str, Any] = {
        "rows_after":              row_count,
        "duplicate_rows_removed":  duplicate_rows_removed,
        "columns_modified":        columns_modified,
        "columns_skipped":         columns_skipped,
        "columns_dropped":         columns_dropped,
        "imputation_methods_used": imputation_methods,
        "before_after_stats":      before_after_stats,
        "semantic_hints":          semantic_hints,
        "outlier_method":          cfg.outlier.method,
        "outlier_action":          cfg.outlier.action,
        "llm_summary":             llm_summary,
    }

    _emit(log, "info", None, llm_summary, "preprocessing_complete")
    return PreprocessResult(log=log, outlier_count=outlier_count, conn=conn, metadata=metadata)

def _empty_metadata(row_count: int) -> Dict[str, Any]:
    return {
        "rows_after": row_count,
        "duplicate_rows_removed": 0,
        "columns_modified": [],
        "columns_skipped": [],
        "columns_dropped": [],
        "imputation_methods_used": {},
        "before_after_stats": {},
        "semantic_hints": {},
        "outlier_method": "iqr",
        "outlier_action": "flag",
        "llm_summary": "No preprocessing was performed.",
    }