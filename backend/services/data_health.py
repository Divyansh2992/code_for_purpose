"""
Data Health Service
===================
Computes rich quality metrics and a weighted confidence score for any dataset
or query result — both from pre-computed column statistics and live DataFrames.

Confidence Score Calculation
─────────────────────────────
The score starts at 100 and deducts weighted penalties across six dimensions:

    1. Missingness    - avg null % across columns            (max - 40 pts)
    2. Outliers       - outlier row % of total rows          (max - 30 pts)
    3. Small dataset  - row count below safe thresholds      (max - 20 pts)
    4. Cardinality    - near-constant or single-value cols   (max - 10 pts)
    5. Low variance   - numeric cols with near-zero std dev  (max - 10 pts)
    6. Skewness       - highly skewed numeric distributions  (max - 10 pts)

All penalties are capped individually and the total score is clamped to [0, 100].

Output Keys
────────────
    missing_pct        - average null % (float)
    outliers           - total outlier count (int)
    rows_used          - row count (int)
    confidence         - 0 - 100 weighted score (float)
    confidence_level   - "High" | "Medium" | "Low"
    confidence_reason  - list of human-readable issue strings
    column_health      - per-column quality scores and flags (list[dict])
    summary_text       - LLM-friendly single-paragraph narrative (str)
    penalty_breakdown  - dict of each penalty component for transparency

Edge Cases
──────────
    - None / empty DataFrame  → zero-row health report, confidence = 0
    - Single-column data      → skewness and cardinality still computed
    - Non-numeric-only data   → numeric signals skipped gracefully
    - Malformed null_pct      → logged as warning, defaults to 0.0
    - Very large DataFrames   → sampled to SAMPLE_ROWS_LIMIT rows before scan
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional, Tuple
import pandas as pd

#: Rows below this trigger the small-dataset penalty.
SMALL_DATASET_WARN_THRESHOLD: int = 30
#: Rows at or below this get the full small-dataset penalty.
SMALL_DATASET_CRITICAL_THRESHOLD: int = 10
#: DataFrames larger than this are sampled before computing health signals.
SAMPLE_ROWS_LIMIT: int = 100_000
#: Cardinality ratio above which a column is considered ID-like.
HIGH_CARDINALITY_RATIO: float = 0.95
#: Cardinality ratio below which a column is considered near-constant.
LOW_CARDINALITY_RATIO: float = 0.01
#: Absolute skewness threshold above which a column is flagged.
SKEWNESS_THRESHOLD: float = 3.0
#: Coefficient of variation below which a numeric column is "low variance".
LOW_VARIANCE_CV_THRESHOLD: float = 0.01

_logger = logging.getLogger(__name__)

# ── Custom metric plugin type ──────────────────────────────────────────────────
#  Signature: (df_sample, row_count) -> (penalty_points, reason_or_None)
CustomMetricFn = Callable[[pd.DataFrame, int], Tuple[float, Optional[str]]]

@dataclass
class PenaltyWeights:
    missing_multiplier: float = 1.5
    missing_cap: float = 40.0
    outlier_multiplier: float = 2.0
    outlier_cap: float = 30.0
    small_data_cap: float = 20.0
    cardinality_cap: float = 10.0
    low_variance_cap: float = 10.0
    skewness_cap: float = 10.0


DEFAULT_WEIGHTS = PenaltyWeights()

@dataclass
class _PenaltyBreakdown:
    missing: float = 0.0
    outlier: float = 0.0
    small_data: float = 0.0
    cardinality: float = 0.0
    low_variance: float = 0.0
    skewness: float = 0.0
    custom: float = 0.0

    def total(self) -> float:
        return (
            self.missing + self.outlier + self.small_data
            + self.cardinality + self.low_variance
            + self.skewness + self.custom
        )


@dataclass
class _ColumnHealth:
    name: str
    null_pct: float
    score: float
    unique_count: Optional[int] = None
    cardinality_ratio: Optional[float] = None
    is_high_cardinality: bool = False
    is_near_constant: bool = False
    is_low_variance: bool = False
    skewness: Optional[float] = None
    is_highly_skewed: bool = False
    dtype: str = "unknown"
    flags: List[str] = field(default_factory=list)

def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert *value* to float, returning *default* on any error."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _confidence_level(score: float) -> str:
    if score >= 80:
        return "High"
    if score >= 55:
        return "Medium"
    return "Low"


def _apply_penalty(value: float, multiplier: float, cap: float) -> float:
    return min(max(value, 0.0) * multiplier, cap)


def _small_data_penalty(row_count: int, cap: float) -> float:
    if row_count <= SMALL_DATASET_CRITICAL_THRESHOLD:
        return cap
    if row_count < SMALL_DATASET_WARN_THRESHOLD:
        span = SMALL_DATASET_WARN_THRESHOLD - SMALL_DATASET_CRITICAL_THRESHOLD
        ratio = 1.0 - (row_count - SMALL_DATASET_CRITICAL_THRESHOLD) / span
        return round(cap * ratio, 2)
    return 0.0


def _column_score(null_pct: float, flags: List[str]) -> float:
    score = 100.0 - min(null_pct * 1.5, 60.0) - len(flags) * 5.0
    return round(max(score, 0.0), 1)


def _analyse_column_from_series(
    series: pd.Series,
    row_count: int,
    null_pct: float,
) -> _ColumnHealth:

    name = str(series.name)
    dtype = str(series.dtype)
    flags: List[str] = []

    # Missingness flag
    if null_pct > 50:
        flags.append(f"high_missingness ({null_pct:.1f}%)")
    elif null_pct > 20:
        flags.append(f"moderate_missingness ({null_pct:.1f}%)")

    # Cardinality
    unique_count: Optional[int] = None
    cardinality_ratio: Optional[float] = None
    is_high_card = is_near_const = False
    try:
        unique_count = int(series.dropna().nunique())
        cardinality_ratio = round(unique_count / max(row_count, 1), 4)
        if cardinality_ratio >= HIGH_CARDINALITY_RATIO and row_count > 10:
            is_high_card = True
            flags.append("high_cardinality (possible ID column)")
        if cardinality_ratio <= LOW_CARDINALITY_RATIO and unique_count <= 1:
            is_near_const = True
            flags.append("near_constant (single unique value)")
    except Exception:
        pass

    # Numeric: low variance
    is_low_var = False
    skewness: Optional[float] = None
    is_skewed = False
    if pd.api.types.is_numeric_dtype(series):
        try:
            numeric = series.dropna()
            if len(numeric) > 1:
                std = float(numeric.std())
                mean = float(numeric.mean())
                cv = abs(std / mean) if mean != 0 else std
                if cv < LOW_VARIANCE_CV_THRESHOLD:
                    is_low_var = True
                    flags.append(f"low_variance (CV={cv:.4f})")
        except Exception:
            pass

        # Numeric: skewness
        try:
            numeric = series.dropna()
            if len(numeric) >= 3:
                skewness = round(float(numeric.skew()), 3)
                if abs(skewness) > SKEWNESS_THRESHOLD:
                    is_skewed = True
                    flags.append(f"high_skewness ({skewness:+.2f})")
        except Exception:
            pass

    return _ColumnHealth(
        name=name,
        null_pct=round(null_pct, 2),
        score=_column_score(null_pct, flags),
        unique_count=unique_count,
        cardinality_ratio=cardinality_ratio,
        is_high_cardinality=is_high_card,
        is_near_constant=is_near_const,
        is_low_variance=is_low_var,
        skewness=skewness,
        is_highly_skewed=is_skewed,
        dtype=dtype,
        flags=flags,
    )


def _build_penalties(
    avg_missing: float,
    outlier_count: int,
    row_count: int,
    col_healths: List[_ColumnHealth],
    weights: PenaltyWeights,
    plugins: List[CustomMetricFn],
    df_sample: Optional[pd.DataFrame],
) -> _PenaltyBreakdown:
    bd = _PenaltyBreakdown()
    total_cols = max(len(col_healths), 1)

    # 1. Missingness
    bd.missing = _apply_penalty(avg_missing, weights.missing_multiplier, weights.missing_cap)

    # 2. Outliers — dampen for tiny datasets to avoid double-penalising with small_data
    outlier_pct = (outlier_count / max(row_count, 1)) * 100.0
    dampen = min(row_count / 50.0, 1.0)
    bd.outlier = _apply_penalty(
        outlier_pct * dampen, weights.outlier_multiplier, weights.outlier_cap
    )

    # 3. Small dataset (graduated)
    bd.small_data = _small_data_penalty(row_count, weights.small_data_cap)

    # 4. Cardinality — near-constant columns
    near_const = sum(1 for c in col_healths if c.is_near_constant)
    bd.cardinality = min(
        (near_const / total_cols) * weights.cardinality_cap * 2, weights.cardinality_cap
    )

    # 5. Low variance
    low_var = sum(1 for c in col_healths if c.is_low_variance)
    bd.low_variance = min(
        (low_var / total_cols) * weights.low_variance_cap * 2, weights.low_variance_cap
    )

    # 6. Skewness
    skewed = sum(1 for c in col_healths if c.is_highly_skewed)
    bd.skewness = min(
        (skewed / total_cols) * weights.skewness_cap * 2, weights.skewness_cap
    )

    # 7. Custom pluggable metrics (capped at 30 total)
    if plugins and df_sample is not None:
        for fn in plugins:
            try:
                penalty, _ = fn(df_sample, row_count)
                bd.custom += max(float(penalty), 0.0)
            except Exception as exc:
                _logger.warning("Custom metric %r raised: %s", fn, exc)
        bd.custom = min(bd.custom, 30.0)

    return bd


def _build_reasons(
    bd: _PenaltyBreakdown,
    col_healths: List[_ColumnHealth],
    row_count: int,
    outlier_count: int,
    avg_missing: float,
    plugins: List[CustomMetricFn],
    df_sample: Optional[pd.DataFrame],
) -> List[str]:
    reasons: List[str] = []
    if bd.missing > 0:
        reasons.append(f"Average missingness is {avg_missing:.1f}% across columns.")
    if bd.outlier > 0:
        pct = outlier_count / max(row_count, 1) * 100
        reasons.append(f"{outlier_count} outlier row(s) detected ({pct:.1f}% of data).")
    if bd.small_data > 0:
        reasons.append(
            f"Small dataset: {row_count} row(s) — statistical conclusions may be unreliable."
        )
    for ch in col_healths:
        for flag in ch.flags:
            reasons.append(f"Column '{ch.name}': {flag}.")
    if plugins and df_sample is not None:
        for fn in plugins:
            try:
                _, reason = fn(df_sample, row_count)
                if reason:
                    reasons.append(reason)
            except Exception:
                pass
    return reasons


def _build_summary_text(
    confidence: float,
    level: str,
    row_count: int,
    col_count: int,
    avg_missing: float,
    outlier_count: int,
    reasons: List[str],
) -> str:
    """LLM-friendly paragraph summarising dataset health."""
    issue_str = (
        " Issues found: " + " ".join(reasons) if reasons else " No major issues detected."
    )
    return (
        f"Dataset health is {level} (confidence score: {confidence}/100). "
        f"The dataset has {row_count} row(s) and {col_count} column(s) "
        f"with an average missingness of {avg_missing:.1f}% and "
        f"{outlier_count} detected outlier(s).{issue_str}"
    )


def _zero_health() -> Dict[str, Any]:
    """Return a zero-confidence health report for empty / invalid inputs."""
    return {
        "missing_pct": 0.0,
        "outliers": 0,
        "rows_used": 0,
        "confidence": 0.0,
        "confidence_level": "Low",
        "confidence_reason": ["No data available."],
        "column_health": [],
        "penalty_breakdown": asdict(_PenaltyBreakdown()),
        "summary_text": (
            "Dataset health is Low (confidence score: 0/100). "
            "No data was available to analyse."
        ),
    }

def compute_health(
    columns: List[Dict[str, Any]],
    outlier_count: int,
    row_count: int,
    *,
    weights: PenaltyWeights = DEFAULT_WEIGHTS,
    verbose: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    
    log = logger or _logger

    col_healths: List[_ColumnHealth] = []
    for i, col in enumerate(columns):
        raw = col.get("null_pct")
        if raw is None:
            log.warning("Column %d (%r) missing 'null_pct'; defaulting to 0.0.",
                        i, col.get("name", "?"))
            null_pct = 0.0
        else:
            null_pct = _safe_float(raw, default=0.0)
            if null_pct == 0.0 and raw != 0 and raw != 0.0:
                log.warning("Column %d (%r) malformed 'null_pct'=%r; defaulted to 0.0.",
                            i, col.get("name", "?"), raw)

        flags: List[str] = []
        if null_pct > 50:
            flags.append(f"high_missingness ({null_pct:.1f}%)")
        elif null_pct > 20:
            flags.append(f"moderate_missingness ({null_pct:.1f}%)")

        col_healths.append(_ColumnHealth(
            name=col.get("name", f"col_{i}"),
            null_pct=round(null_pct, 2),
            score=_column_score(null_pct, flags),
            dtype=str(col.get("type", "unknown")),
            flags=flags,
        ))

    avg_missing = (
        sum(c.null_pct for c in col_healths) / len(col_healths) if col_healths else 0.0
    )

    bd = _build_penalties(
        avg_missing, outlier_count, row_count, col_healths,
        weights, [], None,
    )
    confidence = round(max(100.0 - bd.total(), 0.0), 1)
    level = _confidence_level(confidence)
    reasons = _build_reasons(bd, col_healths, row_count, outlier_count, avg_missing, [], None)
    summary = _build_summary_text(
        confidence, level, row_count, len(col_healths), avg_missing, outlier_count, reasons
    )

    if verbose:
        log.debug("compute_health — penalties=%s confidence=%.1f", asdict(bd), confidence)

    return {
        "missing_pct": round(avg_missing, 2),
        "outliers": outlier_count,
        "rows_used": row_count,
        "confidence": confidence,
        "confidence_level": level,
        "confidence_reason": reasons,
        "column_health": [asdict(c) for c in col_healths],
        "penalty_breakdown": asdict(bd),
        "summary_text": summary,
    }

def compute_health_from_dataframe(
    df: Optional[pd.DataFrame],
    outlier_count: int = 0,
    *,
    weights: PenaltyWeights = DEFAULT_WEIGHTS,
    custom_metrics: Optional[List[CustomMetricFn]] = None,
    verbose: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    log = logger or _logger
    plugins = custom_metrics or []

    if df is None:
        log.debug("Received None; returning zero health.")
        return _zero_health()

    if not isinstance(df, pd.DataFrame):
        log.error(
            "Expected pd.DataFrame, got %s — returning zero health.",
            type(df).__name__,
        )
        return _zero_health()

    try:
        row_count = int(len(df))
        col_count = int(df.shape[1]) if df.ndim > 1 else 1
    except Exception as exc:
        log.exception("Cannot determine DataFrame shape: %s", exc)
        return _zero_health()

    if row_count == 0 or col_count == 0:
        log.debug("Empty DataFrame (shape=%s); returning zero health.", df.shape)
        return _zero_health()

    try:
        df_sample: pd.DataFrame = (
            df.sample(n=SAMPLE_ROWS_LIMIT, random_state=42)
            if row_count > SAMPLE_ROWS_LIMIT else df
        )
    except Exception:
        df_sample = df

    try:
        null_pcts: pd.Series = df_sample.isna().mean() * 100.0
    except Exception as exc:
        log.exception("Failed to compute null percentages: %s", exc)
        null_pcts = pd.Series(0.0, index=df_sample.columns)

    avg_missing = float(null_pcts.mean()) if len(null_pcts) > 0 else 0.0

    col_healths: List[_ColumnHealth] = []
    for col_name in df_sample.columns:
        try:
            series = df_sample[col_name]
            null_pct = float(null_pcts.get(col_name, 0.0))
            ch = _analyse_column_from_series(series, row_count, null_pct)
        except Exception as exc:
            log.warning("Error analysing column %r: %s — using defaults.", col_name, exc)
            ch = _ColumnHealth(name=str(col_name), null_pct=0.0, score=100.0)
        col_healths.append(ch)

    bd = _build_penalties(
        avg_missing, outlier_count, row_count, col_healths,
        weights, plugins, df_sample,
    )
    confidence = round(max(100.0 - bd.total(), 0.0), 1)
    level = _confidence_level(confidence)
    reasons = _build_reasons(
        bd, col_healths, row_count, outlier_count, avg_missing, plugins, df_sample
    )
    summary = _build_summary_text(
        confidence, level, row_count, col_count, avg_missing, outlier_count, reasons
    )

    if verbose:
        log.debug(
            "compute_health_from_dataframe — shape=(%d,%d) sampled=%s "
            "penalties=%s confidence=%.1f",
            row_count, col_count, row_count > SAMPLE_ROWS_LIMIT, asdict(bd), confidence,
        )

    return {
        "missing_pct": round(avg_missing, 2),
        "outliers": outlier_count,
        "rows_used": row_count,
        "confidence": confidence,
        "confidence_level": level,
        "confidence_reason": reasons,
        "column_health": [asdict(c) for c in col_healths],
        "penalty_breakdown": asdict(bd),
        "summary_text": summary,
    }