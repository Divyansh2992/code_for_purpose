"""
Background Jobs Router

Adds lightweight async workers for heavy tasks so the API can return quickly
while preprocessing/correlation/auto-visualization execute in the background.
"""

from __future__ import annotations

import asyncio
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import state
from routers import auto_visualize
from routers.query import _preprocess_log_to_lines
from services import preprocessing

router = APIRouter()

JOB_TTL_SECONDS = 6 * 60 * 60


class PreprocessJobRequest(BaseModel):
    dataset_id: str


class CorrelationJobRequest(BaseModel):
    dataset_id: str
    method: str = "pearson"  # pearson | spearman | kendall


class AutoVisualizeJobRequest(BaseModel):
    dataset_id: str
    mode: str = "raw"


class JobStartResponse(BaseModel):
    job_id: str
    status: str
    job_type: str
    poll_url: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    job_type: str
    dataset_id: str
    payload: Dict[str, Any]
    created_at: str
    updated_at: str
    duration_ms: Optional[int] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _prune_jobs() -> None:
    now_ts = time.time()
    to_delete: List[str] = []

    for job_id, job in state.jobs.items():
        status = job.get("status")
        updated_ts = float(job.get("updated_ts", now_ts))
        age = now_ts - updated_ts

        if status in {"completed", "failed"} and age > JOB_TTL_SECONDS:
            to_delete.append(job_id)

    for job_id in to_delete:
        state.jobs.pop(job_id, None)


def _create_job(job_type: str, dataset_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    job_id = str(uuid4())
    now_iso = _now_iso()
    now_ts = time.time()

    job = {
        "job_id": job_id,
        "status": "queued",
        "job_type": job_type,
        "dataset_id": dataset_id,
        "payload": payload,
        "created_at": now_iso,
        "updated_at": now_iso,
        "updated_ts": now_ts,
        "duration_ms": None,
        "result": None,
        "error": None,
        "error_type": None,
    }
    state.jobs[job_id] = job
    return job


async def _run_job(job_id: str, worker_fn, *args, **kwargs) -> None:
    job = state.jobs.get(job_id)
    if not job:
        return

    start = time.perf_counter()
    job["status"] = "running"
    job["updated_at"] = _now_iso()
    job["updated_ts"] = time.time()

    try:
        result = await asyncio.to_thread(worker_fn, *args, **kwargs)
        duration_ms = int((time.perf_counter() - start) * 1000)

        job["status"] = "completed"
        job["duration_ms"] = duration_ms
        job["result"] = result
        job["updated_at"] = _now_iso()
        job["updated_ts"] = time.time()
    except Exception as exc:
        duration_ms = int((time.perf_counter() - start) * 1000)

        job["status"] = "failed"
        job["duration_ms"] = duration_ms
        job["error"] = str(exc)
        job["error_type"] = type(exc).__name__
        job["updated_at"] = _now_iso()
        job["updated_ts"] = time.time()


def _preprocess_worker(dataset: Dict[str, Any]) -> Dict[str, Any]:
    file_path = str(dataset["file_path"])
    schema = dataset["columns"]

    result = preprocessing.preprocess(file_path, schema)
    try:
        metadata = result.metadata or {}
        rows_after = int(metadata.get("rows_after", dataset.get("row_count", 0)))
        return {
            "outlier_count": int(result.outlier_count),
            "rows_after": rows_after,
            "preprocessing_log": _preprocess_log_to_lines(result.log),
            "metadata": metadata,
        }
    finally:
        try:
            result.conn.close()
        except Exception:
            pass


def _correlation_worker(dataset: Dict[str, Any], method: str) -> Dict[str, Any]:
    file_path = str(dataset["file_path"])

    if method not in {"pearson", "spearman", "kendall"}:
        raise ValueError("method must be one of: pearson, spearman, kendall")

    df = pd.read_csv(file_path)
    numeric_df = df.select_dtypes(include="number")

    if numeric_df.shape[1] < 2:
        raise ValueError("Correlation matrix requires at least 2 numeric columns.")

    corr_matrix = numeric_df.corr(method=method)
    cols = list(corr_matrix.columns)
    flat: List[Dict[str, Any]] = []

    for col_a in cols:
        for col_b in cols:
            val = corr_matrix.loc[col_a, col_b]
            if isinstance(val, float) and math.isnan(val):
                corr_val = None
            else:
                corr_val = round(float(val), 6)
            flat.append({"col_a": col_a, "col_b": col_b, "correlation": corr_val})

    return {
        "columns": cols,
        "data": flat,
        "method": method,
    }


def _auto_visualize_worker(dataset_id: str, mode: str) -> Dict[str, Any]:
    req = auto_visualize.AutoVisualizeRequest(dataset_id=dataset_id, mode=mode)
    response = asyncio.run(auto_visualize.auto_visualize(req))
    if hasattr(response, "model_dump"):
        return response.model_dump()
    return dict(response)


@router.post("/jobs/preprocess", response_model=JobStartResponse)
async def start_preprocess_job(req: PreprocessJobRequest) -> JobStartResponse:
    dataset = state.datasets.get(req.dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    _prune_jobs()
    job = _create_job("preprocess", req.dataset_id, payload={})
    asyncio.create_task(_run_job(job["job_id"], _preprocess_worker, dataset))

    return JobStartResponse(
        job_id=job["job_id"],
        status=job["status"],
        job_type=job["job_type"],
        poll_url=f"/jobs/{job['job_id']}",
    )


@router.post("/jobs/correlation", response_model=JobStartResponse)
async def start_correlation_job(req: CorrelationJobRequest) -> JobStartResponse:
    dataset = state.datasets.get(req.dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    _prune_jobs()
    job = _create_job("correlation", req.dataset_id, payload={"method": req.method})
    asyncio.create_task(_run_job(job["job_id"], _correlation_worker, dataset, req.method))

    return JobStartResponse(
        job_id=job["job_id"],
        status=job["status"],
        job_type=job["job_type"],
        poll_url=f"/jobs/{job['job_id']}",
    )


@router.post("/jobs/auto-visualize", response_model=JobStartResponse)
async def start_auto_visualize_job(req: AutoVisualizeJobRequest) -> JobStartResponse:
    dataset = state.datasets.get(req.dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    _prune_jobs()
    job = _create_job("auto-visualize", req.dataset_id, payload={"mode": req.mode})
    asyncio.create_task(_run_job(job["job_id"], _auto_visualize_worker, req.dataset_id, req.mode))

    return JobStartResponse(
        job_id=job["job_id"],
        status=job["status"],
        job_type=job["job_type"],
        poll_url=f"/jobs/{job['job_id']}",
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str) -> JobStatusResponse:
    _prune_jobs()
    job = state.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired.")

    return JobStatusResponse(**job)
