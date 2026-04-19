"""
Data Health Router — POST /data-health
Returns data health metrics for a given dataset + mode WITHOUT running a full query.
- raw:      health based on sampled raw CSV (includes estimated outliers)
- smart:    runs preprocessing to compute post-clean missing% + outliers
- scalable: same as smart (Spark pipeline omitted for perf — uses preprocessing stats)
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd

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
        # Raw: estimate health directly from sampled raw data
        try:
            raw_df_sample = pd.read_csv(file_path, nrows=data_health.SAMPLE_ROWS_LIMIT)
            health = data_health.compute_health_from_dataframe(
                raw_df_sample,
                total_row_count=row_count,
            )
        except Exception:
            health = data_health.compute_health(schema, 0, row_count)

    elif req.mode in ("smart", "scalable"):
        # Smart/Scalable: run preprocessing and compute full drill-down health
        try:
            preprocess_result = preprocessing.preprocess(file_path, schema)
            outlier_count = preprocess_result.outlier_count
            rows_after = int(preprocess_result.metadata.get("rows_after", row_count))

            try:
                cleaned_df_sample = preprocess_result.conn.execute(
                    f"SELECT * FROM data LIMIT {data_health.SAMPLE_ROWS_LIMIT}"
                ).fetchdf()
            finally:
                preprocess_result.conn.close()

            health = data_health.compute_health_from_dataframe(
                cleaned_df_sample,
                outlier_count=outlier_count,
                total_row_count=rows_after,
            )
        except Exception as exc:
            # Fall back to raw health on error
            health = data_health.compute_health(schema, 0, row_count)

    else:
        health = data_health.compute_health(schema, 0, row_count)

    return health
