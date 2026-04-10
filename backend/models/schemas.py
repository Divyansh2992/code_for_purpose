from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class ColumnInfo(BaseModel):
    name: str
    type: str
    null_pct: float
    mean: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None
    unique_count: Optional[int] = None


class UploadResponse(BaseModel):
    dataset_id: str
    filename: str
    row_count: int
    columns: List[ColumnInfo]
    sample: List[Dict[str, Any]]
    suggested_questions: List[str] = []


class QueryRequest(BaseModel):
    dataset_id: str
    question: str
    mode: str = "raw"          # "raw" | "smart"
    session_id: Optional[str] = None


class DataHealth(BaseModel):
    missing_pct: float
    outliers: int
    rows_used: int
    confidence: float


class QueryResponse(BaseModel):
    sql: str
    result: List[Dict[str, Any]]
    columns: List[str]
    explanation: str
    insights: List[str]
    chart_type: Optional[str] = None   # "bar" | "line" | None
    chart_x: Optional[str] = None
    chart_y: List[str] = []
    data_health: DataHealth
    preprocessing_log: List[str] = []
    mode: str
    why_analysis: Optional[str] = None
    error: Optional[str] = None
