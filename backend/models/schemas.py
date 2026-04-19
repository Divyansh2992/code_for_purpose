from pydantic import BaseModel, Field
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
    mode: str = "raw"          # "raw" | "smart" | "scalable"
    session_id: Optional[str] = None
    guardian_enabled: bool = True


class DataHealth(BaseModel):
    missing_pct: float
    outliers: int
    rows_used: int
    confidence: float
    confidence_level: Optional[str] = None
    confidence_reason: List[str] = Field(default_factory=list)
    column_health: List[Dict[str, Any]] = Field(default_factory=list)
    penalty_breakdown: Dict[str, float] = Field(default_factory=dict)
    summary_text: Optional[str] = None


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
    guardian_enabled: bool = True
    guardian_passed: bool = False
    guardian_confidence: float = 0.0
    guardian_retries: int = 0
    guardian_log: List[str] = []
    guardian_steps: List[Dict[str, Any]] = Field(default_factory=list)
    why_analysis: Optional[str] = None
    error: Optional[str] = None
