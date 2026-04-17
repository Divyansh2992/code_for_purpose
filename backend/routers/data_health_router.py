"""
Data Health Router — POST /data-health
Returns data health metrics for a given dataset + mode WITHOUT running a full query.
- raw:      health based on raw CSV schema (null_pct from upload analysis)
- smart:    runs preprocessing to compute post-clean missing% + outliers
- scalable: same as smart (Spark pipeline omitted for perf — uses preprocessing stats)
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import state
from services import preprocessing, data_health

router = APIRouter()


class DataHealthRequest(BaseModel):
    dataset_id: str
    mode: str = "raw"   # "raw" | "smart" | "scalable"


@router.post("/data-health")
async def get_data_health(req: DataHealthRequest):
    dataset = state.datasets.get(req.dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    schema = dataset["columns"]
    row_count = dataset["row_count"]
    file_path = dataset["file_path"]

    if req.mode == "raw":
        # Raw: use the original schema null_pct (no preprocessing)
        health = data_health.compute_health(schema, 0, row_count)

    elif req.mode in ("smart", "scalable"):
        # Smart/Scalable: run preprocessing to get post-clean stats
        try:
            preprocess_result = preprocessing.preprocess(file_path, schema)
            outlier_count = preprocess_result.outlier_count
            preprocess_result.conn.close()

            # After preprocessing, missing% is reduced to only columns that exceeded threshold
            cleaned_missing = sum(
                c.get("null_pct", 0.0)
                for c in schema
                if c.get("null_pct", 0.0) > preprocessing.DEFAULT_CONFIG.null_threshold_skip
            )
            avg_missing = round(
                cleaned_missing / len(schema) if schema else 0.0, 2
            )

            # Rebuild health with cleaned stats
            missing_penalty = min(avg_missing * 1.5, 40.0)
            outlier_pct = (outlier_count / max(row_count, 1)) * 100.0
            outlier_penalty = min(outlier_pct * 2.0, 30.0)
            confidence = round(max(100.0 - missing_penalty - outlier_penalty, 0.0), 1)

            health = {
                "missing_pct": avg_missing,
                "outliers": outlier_count,
                "rows_used": row_count,
                "confidence": confidence,
            }
        except Exception as exc:
            # Fall back to raw health on error
            health = data_health.compute_health(schema, 0, row_count)

    else:
        health = data_health.compute_health(schema, 0, row_count)

    return health
