"""
Correlation Matrix Router — POST /correlation-matrix

Uses pandas to compute a full pairwise Pearson correlation matrix on
all numeric columns in the dataset. This is the same approach used in
Jupyter notebooks (df.corr()), completely bypassing SQL.

Returns a flat list of {col_a, col_b, correlation} records so the
frontend CorrelationHeatmap component can render the N×N grid.
"""
import math
from typing import List, Dict, Any

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import state

router = APIRouter()


class CorrRequest(BaseModel):
    dataset_id: str
    method: str = "pearson"   # pearson | spearman | kendall


class CorrResponse(BaseModel):
    columns: List[str]
    data: List[Dict[str, Any]]   # [{col_a, col_b, correlation}, ...]
    method: str
    note: str = ""


@router.post("/correlation-matrix", response_model=CorrResponse)
async def correlation_matrix(req: CorrRequest) -> CorrResponse:
    # ── 1. Validate dataset ────────────────────────────────────────────────
    dataset = state.datasets.get(req.dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=404,
            detail=(
                "Dataset not found. The server may have restarted — "
                "please re-upload your CSV file and try again."
            ),
        )

    file_path: str = dataset["file_path"]

    # ── 2. Load CSV with pandas ────────────────────────────────────────────
    try:
        df = pd.read_csv(file_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read CSV: {exc}")

    # ── 3. Select numeric columns (same as Jupyter df.select_dtypes) ───────
    numeric_df = df.select_dtypes(include="number")

    if numeric_df.shape[1] < 2:
        col_info = ", ".join(
            f"{c} ({df[c].dtype})" for c in df.columns
        )
        raise HTTPException(
            status_code=422,
            detail=(
                f"Correlation matrix requires at least 2 numeric columns. "
                f"This dataset has {numeric_df.shape[1]} numeric column(s). "
                f"All columns: {col_info}"
            ),
        )

    # ── 4. Compute correlation matrix ──────────────────────────────────────
    try:
        corr_matrix = numeric_df.corr(method=req.method)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Correlation computation failed: {exc}"
        )

    # ── 5. Flatten to [{col_a, col_b, correlation}] ────────────────────────
    cols = list(corr_matrix.columns)
    flat: List[Dict[str, Any]] = []
    for col_a in cols:
        for col_b in cols:
            val = corr_matrix.loc[col_a, col_b]
            flat.append({
                "col_a": col_a,
                "col_b": col_b,
                "correlation": None if (isinstance(val, float) and math.isnan(val)) else round(float(val), 6),
            })

    note = ""
    if numeric_df.shape[1] == 1:
        note = "Only one numeric column found — a meaningful correlation matrix needs at least 2."

    return CorrResponse(
        columns=cols,
        data=flat,
        method=req.method,
        note=note,
    )
