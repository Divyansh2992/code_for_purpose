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
