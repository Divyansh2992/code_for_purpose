"""
Data Health Service — computes quality metrics and confidence score for any query result.
"""
from typing import List, Dict, Any


def compute_health(
    columns: List[Dict[str, Any]],
    outlier_count: int,
    row_count: int,
) -> Dict[str, Any]:
    """
    Compute a data health summary:
    - missing_pct: average null % across all columns
    - outliers: total outlier count passed in
    - rows_used: row_count
    - confidence: 0-100 score penalised by missingness & outliers
    """
    if columns:
        avg_missing = sum(c.get("null_pct", 0.0) for c in columns) / len(columns)
    else:
        avg_missing = 0.0

    # Confidence penalties (tunable)
    missing_penalty = min(avg_missing * 1.5, 40.0)   # max -40 pts
    outlier_pct = (outlier_count / max(row_count, 1)) * 100.0
    outlier_penalty = min(outlier_pct * 2.0, 30.0)   # max -30 pts

    confidence = round(max(100.0 - missing_penalty - outlier_penalty, 0.0), 1)

    return {
        "missing_pct": round(avg_missing, 2),
        "outliers": outlier_count,
        "rows_used": row_count,
        "confidence": confidence,
    }


def compute_health_from_dataframe(
    df: Any,
    outlier_count: int = 0,
) -> Dict[str, Any]:
    """
    Compute health directly from a cleaned dataframe-like object.
    This is used by scalable mode after Spark preprocessing.
    """
    if df is None:
        return compute_health([], outlier_count, 0)

    row_count = int(len(df)) if hasattr(df, "__len__") else 0

    avg_missing = 0.0
    try:
        if row_count > 0 and getattr(df, "shape", (0, 0))[1] > 0:
            avg_missing = float(df.isna().mean().mean() * 100.0)
    except Exception:
        avg_missing = 0.0

    missing_penalty = min(avg_missing * 1.5, 40.0)
    outlier_pct = (outlier_count / max(row_count, 1)) * 100.0
    outlier_penalty = min(outlier_pct * 2.0, 30.0)
    confidence = round(max(100.0 - missing_penalty - outlier_penalty, 0.0), 1)

    return {
        "missing_pct": round(avg_missing, 2),
        "outliers": outlier_count,
        "rows_used": row_count,
        "confidence": confidence,
    }
