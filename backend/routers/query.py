"""
Query Router — POST /query
Orchestrates: LLM SQL generation → DuckDB execution → preprocessing (Smart) →
data health → LLM explanation → chart detection → structured response.
"""
from typing import List, Dict, Any, Optional, Tuple

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException

import state
from models.schemas import QueryRequest, QueryResponse, DataHealth
from services import llm_service, query_engine, preprocessing, data_health, spark_engine

router = APIRouter()

DATE_KEYWORDS = {"date", "month", "year", "quarter", "week", "day", "time", "period", "created", "updated", "timestamp"}
MAX_GUARDIAN_RETRIES = 2



@router.post("/query", response_model=QueryResponse)
async def run_query(req: QueryRequest) -> QueryResponse:
    # ── 1. Validate dataset ────────────────────────────────────────────────
    dataset = state.datasets.get(req.dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=404,
            detail=(
                "Dataset not found. This usually happens because the server restarted "
                "(Render free tier sleeps after 15 minutes of inactivity and clears all "
                "in-memory data on wake-up). Please re-upload your CSV file and try again."
            ),
        )

    file_path: str      = dataset["file_path"]
    schema: list        = dataset["columns"]
    sample: list        = dataset["sample"]
    row_count: int      = dataset["row_count"]
    session_id: str     = req.session_id or "default"
    mode: str           = req.mode  # "raw" | "smart" | "scalable"
    guardian_enabled: bool = req.guardian_enabled

    # ── 2. Build history for context memory ───────────────────────────────
    history = state.sessions.get(session_id, [])

    # ── 3. Generate SQL ───────────────────────────────────────────────────
    try:
        sql = llm_service.generate_sql(schema, sample, req.question, history)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"LLM SQL generation failed: {exc}")

    # Basic safety guard — ensure it's a SELECT
    if not _is_select(sql):
        raise HTTPException(
            status_code=422,
            detail=f"LLM generated a non-SELECT statement. Query rejected.\nSQL: {sql}",
        )

    guardian_log: List[str] = []
    guardian_steps: List[Dict[str, Any]] = []
    guardian_passed = not guardian_enabled
    guardian_retries = 0
    guardian_confidence = 0.0

    if guardian_enabled:
        guardian_result = _run_sql_guardian(
            file_path=file_path,
            schema=schema,
            sample=sample,
            question=req.question,
            history=history,
            initial_sql=sql,
            max_retries=MAX_GUARDIAN_RETRIES,
        )
        guardian_log = guardian_result["log"]
        guardian_steps = guardian_result.get("steps", [])
        guardian_passed = guardian_result["passed"]
        guardian_retries = guardian_result["retries"]
        guardian_confidence = guardian_result["confidence"]
        sql = guardian_result["sql"]

        if not guardian_passed:
            health = data_health.compute_health(schema, 0, row_count)
            return QueryResponse(
                sql=sql,
                result=[],
                columns=[],
                explanation="",
                insights=[],
                data_health=DataHealth(**health),
                preprocessing_log=[],
                mode=mode,
                guardian_enabled=guardian_enabled,
                guardian_passed=False,
                guardian_confidence=guardian_confidence,
                guardian_retries=guardian_retries,
                guardian_log=guardian_log,
                guardian_steps=guardian_steps,
                error=guardian_result.get("error") or "SQL Guardian blocked this query.",
            )

    # ── 4. Execute query (raw, smart, or scalable) ───────────────────────
    log: List[str] = []
    outlier_count: int = 0
    conn = None
    cleaned_df_for_health = None
    total_rows_for_health = row_count

    try:
        if mode == "smart":
            preprocess_result = preprocessing.preprocess(file_path, schema)
            log = _preprocess_log_to_lines(preprocess_result.log)
            outlier_count = preprocess_result.outlier_count
            conn = preprocess_result.conn
            total_rows_for_health = int(preprocess_result.metadata.get("rows_after", row_count))
            try:
                cleaned_df_for_health = conn.execute(
                    f"SELECT * FROM data LIMIT {data_health.SAMPLE_ROWS_LIMIT}"
                ).fetchdf()
            except Exception:
                cleaned_df_for_health = None
            rows, columns = query_engine.execute_query(file_path, sql, conn=conn)
        elif mode == "scalable":
            raw_df = pd.read_csv(file_path)
            spark_result = spark_engine.run_spark_pipeline(raw_df)
            cleaned_df_for_health = spark_result["cleaned_df"]
            total_rows_for_health = int(len(cleaned_df_for_health))

            conn = duckdb.connect()
            conn.register("spark_cleaned_df", cleaned_df_for_health)
            conn.execute("CREATE TABLE data AS SELECT * FROM spark_cleaned_df")

            rows, columns = query_engine.execute_query(file_path, sql, conn=conn)
            log = [
                "Processed using PySpark (scalable mode)",
                f"Rows processed in Spark pipeline: {spark_result['rows_processed']}",
            ]
        else:
            rows, columns = query_engine.execute_query(file_path, sql)
            log = ["ℹ️ Results are based on raw data (no preprocessing applied)."]
    except Exception as exc:
        error_msg = str(exc)
        # Return partial response with error so frontend can show it
        health = data_health.compute_health(schema, 0, row_count)
        return QueryResponse(
            sql=sql,
            result=[],
            columns=[],
            explanation="",
            insights=[],
            data_health=DataHealth(**health),
            preprocessing_log=log,
            mode=mode,
            guardian_enabled=guardian_enabled,
            guardian_passed=guardian_passed,
            guardian_confidence=guardian_confidence,
            guardian_retries=guardian_retries,
            guardian_log=guardian_log,
            guardian_steps=guardian_steps,
            error=_format_query_error(mode, error_msg),
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # ── 5. Data health ────────────────────────────────────────────────────
    if mode in ("smart", "scalable") and cleaned_df_for_health is not None:
        health = data_health.compute_health_from_dataframe(
            cleaned_df_for_health,
            outlier_count=outlier_count if mode == "smart" else None,
            total_row_count=total_rows_for_health,
        )
    elif mode == "raw":
        try:
            raw_df_sample = pd.read_csv(file_path, nrows=data_health.SAMPLE_ROWS_LIMIT)
            health = data_health.compute_health_from_dataframe(
                raw_df_sample,
                total_row_count=row_count,
            )
        except Exception:
            health = data_health.compute_health(schema, outlier_count, row_count)
    else:
        health = data_health.compute_health(schema, outlier_count, row_count)

    # ── 6. LLM explanation ────────────────────────────────────────────────
    explanation_data = {"explanation": "Query executed successfully.", "insights": [], "why_analysis": ""}
    if rows:
        try:
            explanation_data = llm_service.explain_result(req.question, sql, rows, columns)
        except Exception:
            pass

    # ── 7. Chart detection ────────────────────────────────────────────────
    chart_type, chart_x, chart_y = _detect_chart(rows, columns)

    # ── 8. Save to session history ────────────────────────────────────────
    if session_id not in state.sessions:
        state.sessions[session_id] = []
    state.sessions[session_id].append({"role": "user", "content": req.question})
    state.sessions[session_id].append({"role": "assistant", "content": sql})
    # Keep last 10 turns
    state.sessions[session_id] = state.sessions[session_id][-10:]

    return QueryResponse(
        sql=sql,
        result=_serialise_rows(rows[:200]),   # cap at 200 rows for response size
        columns=columns,
        explanation=explanation_data.get("explanation", ""),
        insights=explanation_data.get("insights", []),
        chart_type=chart_type,
        chart_x=chart_x,
        chart_y=chart_y,
        data_health=DataHealth(**health),
        preprocessing_log=log,
        mode=mode,
        guardian_enabled=guardian_enabled,
        guardian_passed=guardian_passed,
        guardian_confidence=guardian_confidence,
        guardian_retries=guardian_retries,
        guardian_log=guardian_log,
        guardian_steps=guardian_steps,
        why_analysis=explanation_data.get("why_analysis", ""),
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _run_sql_guardian(
    file_path: str,
    schema: List[Dict[str, Any]],
    sample: List[Dict[str, Any]],
    question: str,
    history: List[Dict[str, str]],
    initial_sql: str,
    max_retries: int = 2,
) -> Dict[str, Any]:
    """
    Verify and auto-repair SQL before execution.

    Pipeline per attempt:
      1. Static validator
      2. LLM semantic reviewer
      3. DuckDB dry-run (EXPLAIN + LIMIT probe)
    """
    candidate_sql = initial_sql.strip().rstrip(";")
    log: List[str] = []
    steps: List[Dict[str, Any]] = []
    verifier_available = True

    def _new_attempt(attempt_no: int) -> Dict[str, Any]:
        attempt_entry: Dict[str, Any] = {"attempt": attempt_no, "stages": []}
        steps.append(attempt_entry)
        return attempt_entry

    def _add_stage(
        attempt_entry: Dict[str, Any],
        stage: str,
        status: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        stage_entry: Dict[str, Any] = {
            "stage": stage,
            "status": status,
            "message": message,
        }
        if details:
            stage_entry["details"] = details
        attempt_entry["stages"].append(stage_entry)

    for attempt in range(max_retries + 1):
        attempt_entry = _new_attempt(attempt + 1)

        try:
            llm_service.validate_sql(candidate_sql)
            log.append(f"Attempt {attempt + 1}: static SQL safety check passed.")
            _add_stage(
                attempt_entry,
                "validator",
                "pass",
                "Static SQL safety check passed.",
            )
        except Exception as exc:
            log.append(f"Attempt {attempt + 1}: static SQL safety check failed: {exc}")
            _add_stage(
                attempt_entry,
                "validator",
                "fail",
                "Static SQL safety check failed.",
                {"error": str(exc)},
            )
            if attempt == max_retries:
                return {
                    "passed": False,
                    "sql": candidate_sql,
                    "retries": attempt,
                    "confidence": _compute_guardian_confidence(attempt, verifier_available),
                    "log": log,
                    "steps": steps,
                    "error": (
                        "SQL Guardian blocked execution because the generated query "
                        f"is unsafe after {attempt + 1} attempt(s)."
                    ),
                }
            repaired_sql = _repair_with_feedback(
                schema=schema,
                sample=sample,
                question=question,
                failed_sql=candidate_sql,
                reason=str(exc),
                history=history,
            )
            if not repaired_sql:
                _add_stage(
                    attempt_entry,
                    "repair",
                    "fail",
                    "Automatic SQL repair failed after validator rejection.",
                )
                return {
                    "passed": False,
                    "sql": candidate_sql,
                    "retries": attempt,
                    "confidence": _compute_guardian_confidence(attempt, verifier_available),
                    "log": log,
                    "steps": steps,
                    "error": "SQL Guardian could not repair an unsafe query.",
                }
            candidate_sql = repaired_sql
            log.append("Guardian generated a repaired SQL candidate after safety failure.")
            _add_stage(
                attempt_entry,
                "repair",
                "pass",
                "Generated repaired SQL after validator rejection.",
            )
            continue

        review = llm_service.review_sql(schema, sample, question, candidate_sql)
        verdict = str(review.get("verdict", "PASS")).upper()
        reason = str(review.get("reason", "No issues detected.")).strip()
        fixed_sql = str(review.get("fixed_sql", "")).strip().rstrip(";")

        if "unavailable" in reason.lower():
            verifier_available = False

        log.append(f"Attempt {attempt + 1}: semantic review {verdict} - {reason}")
        _add_stage(
            attempt_entry,
            "semantic_review",
            "pass" if verdict != "FAIL" else "fail",
            f"Semantic review {verdict}: {reason}",
            {"fixed_sql": fixed_sql} if fixed_sql else None,
        )

        if verdict == "FAIL":
            if attempt == max_retries:
                return {
                    "passed": False,
                    "sql": candidate_sql,
                    "retries": attempt,
                    "confidence": _compute_guardian_confidence(attempt, verifier_available),
                    "log": log,
                    "steps": steps,
                    "error": (
                        "SQL Guardian blocked execution because semantic validation "
                        f"kept failing after {attempt + 1} attempt(s)."
                    ),
                }

            semantic_reason = f"Semantic verifier failure: {reason}"
            semantic_failed_sql = candidate_sql

            if fixed_sql:
                semantic_failed_sql = fixed_sql
                semantic_reason += f" | Verifier suggested SQL: {fixed_sql}"
                log.append("Verifier supplied SQL hint; forwarding to Groq repair.")
                _add_stage(
                    attempt_entry,
                    "repair",
                    "info",
                    "Verifier suggested SQL was forwarded to repair model.",
                )

            repaired_sql = _repair_with_feedback(
                schema=schema,
                sample=sample,
                question=question,
                failed_sql=semantic_failed_sql,
                reason=semantic_reason,
                history=history,
            )
            if not repaired_sql:
                _add_stage(
                    attempt_entry,
                    "repair",
                    "fail",
                    "Automatic SQL repair failed after semantic rejection.",
                )
                return {
                    "passed": False,
                    "sql": candidate_sql,
                    "retries": attempt,
                    "confidence": _compute_guardian_confidence(attempt, verifier_available),
                    "log": log,
                    "steps": steps,
                    "error": "SQL Guardian could not repair semantically incorrect SQL.",
                }
            candidate_sql = repaired_sql
            log.append("Guardian generated a repaired SQL candidate after semantic failure.")
            _add_stage(
                attempt_entry,
                "repair",
                "pass",
                "Generated repaired SQL after semantic rejection.",
            )
            continue

        dry_ok, dry_error = _dry_run_sql(file_path, candidate_sql)
        if dry_ok:
            log.append("Attempt {0}: dry-run passed (EXPLAIN + LIMIT probe).".format(attempt + 1))
            _add_stage(
                attempt_entry,
                "dry_run",
                "pass",
                "Dry-run passed (EXPLAIN + LIMIT probe).",
            )
            return {
                "passed": True,
                "sql": candidate_sql,
                "retries": attempt,
                "confidence": _compute_guardian_confidence(attempt, verifier_available),
                "log": log,
                "steps": steps,
                "error": None,
            }

        log.append(f"Attempt {attempt + 1}: dry-run failed: {dry_error}")
        _add_stage(
            attempt_entry,
            "dry_run",
            "fail",
            "Dry-run failed.",
            {"error": dry_error},
        )

        if attempt == max_retries:
            return {
                "passed": False,
                "sql": candidate_sql,
                "retries": attempt,
                "confidence": _compute_guardian_confidence(attempt, verifier_available),
                "log": log,
                "steps": steps,
                "error": (
                    "SQL Guardian blocked execution because query compilation/execution "
                    f"failed after {attempt + 1} attempt(s)."
                ),
            }

        repaired_sql = _repair_with_feedback(
            schema=schema,
            sample=sample,
            question=question,
            failed_sql=candidate_sql,
            reason=f"DuckDB dry-run error: {dry_error}",
            history=history,
        )
        if not repaired_sql:
            _add_stage(
                attempt_entry,
                "repair",
                "fail",
                "Automatic SQL repair failed after dry-run error.",
            )
            return {
                "passed": False,
                "sql": candidate_sql,
                "retries": attempt,
                "confidence": _compute_guardian_confidence(attempt, verifier_available),
                "log": log,
                "steps": steps,
                "error": "SQL Guardian could not repair SQL after dry-run failure.",
            }
        candidate_sql = repaired_sql
        log.append("Guardian generated a repaired SQL candidate after dry-run failure.")
        _add_stage(
            attempt_entry,
            "repair",
            "pass",
            "Generated repaired SQL after dry-run failure.",
        )

    return {
        "passed": False,
        "sql": candidate_sql,
        "retries": max_retries,
        "confidence": _compute_guardian_confidence(max_retries, verifier_available),
        "log": log,
        "steps": steps,
        "error": "SQL Guardian ended unexpectedly.",
    }


def _repair_with_feedback(
    schema: List[Dict[str, Any]],
    sample: List[Dict[str, Any]],
    question: str,
    failed_sql: str,
    reason: str,
    history: List[Dict[str, str]],
) -> Optional[str]:
    try:
        repaired = llm_service.repair_sql(
            schema=schema,
            sample=sample,
            question=question,
            failed_sql=failed_sql,
            error_reason=reason,
            history=history,
        )
        return repaired.strip().rstrip(";")
    except Exception:
        return None


def _dry_run_sql(file_path: str, sql: str) -> Tuple[bool, str]:
    conn = duckdb.connect()
    escaped_path = file_path.replace("\\", "/")
    safe_sql = sql.strip().rstrip(";")

    try:
        conn.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{escaped_path}')")
        conn.execute(f"EXPLAIN {safe_sql}")
        conn.execute(f"SELECT * FROM ({safe_sql}) AS guardian_probe LIMIT 3").fetchall()
        return True, ""
    except Exception as exc:
        return False, str(exc)
    finally:
        conn.close()


def _compute_guardian_confidence(retries: int, verifier_available: bool) -> float:
    score = 0.92 - (0.12 * retries)
    if not verifier_available:
        score -= 0.15
    score = max(0.05, min(0.99, score))
    return round(score * 100, 1)

def _is_select(sql: str) -> bool:
    stripped = sql.strip().lstrip("(").upper()
    return stripped.startswith("SELECT") or stripped.startswith("WITH")


def _detect_chart(
    rows: List[Dict[str, Any]],
    columns: List[str],
) -> Tuple[Optional[str], Optional[str], List[str]]:
    if not rows or len(columns) < 2:
        return None, None, []

    numeric_cols: List[str] = []
    categorical_cols: List[str] = []

    for col in columns:
        sample_vals = [r.get(col) for r in rows[:5] if r.get(col) is not None]
        if sample_vals and all(isinstance(v, (int, float)) for v in sample_vals):
            numeric_cols.append(col)
        else:
            categorical_cols.append(col)

    if not numeric_cols:
        return None, None, []

    # If no categorical columns, use the first numeric column as X
    if not categorical_cols:
        x_col = numeric_cols[0]
        y_cols = numeric_cols[1:4]
    else:
        x_col = categorical_cols[0]
        y_cols = numeric_cols[:3]

    if not y_cols:
        return None, None, []

    # Line if x column name contains time-like keyword
    is_time = any(kw in x_col.lower() for kw in DATE_KEYWORDS)
    chart_type = "line" if is_time else "bar"

    return chart_type, x_col, y_cols


def _serialise_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Replace NaN / non-JSON-serialisable values."""
    import math, datetime
    clean = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, float) and math.isnan(v):
                new_row[k] = None
            elif isinstance(v, (datetime.date, datetime.datetime)):
                new_row[k] = str(v)
            else:
                new_row[k] = v
        clean.append(new_row)
    return clean


def _preprocess_log_to_lines(log_entries: List[Dict[str, Any]]) -> List[str]:
    """Convert structured preprocessing log entries to UI-friendly lines."""
    lines: List[str] = []
    for entry in log_entries or []:
        if isinstance(entry, dict):
            message = entry.get("message")
            if not message:
                continue
            level = str(entry.get("level", "")).upper()
            prefix = f"[{level}] " if level else ""
            lines.append(f"{prefix}{message}")
        elif entry is not None:
            lines.append(str(entry))
    return lines


def _format_query_error(mode: str, error_msg: str) -> str:
    if mode != "scalable":
        return f"Query execution failed: {error_msg}"

    msg = error_msg.lower()

    if "pyspark is not available" in msg or "no module named 'pyspark'" in msg:
        return (
            "Scalable mode is unavailable because PySpark is not installed on the API server. "
            "Install pyspark and retry, or use smart/raw mode."
        )

    if (
        "java gateway process exited before sending its port number" in msg
        or "java" in msg and ("not found" in msg or "could not find" in msg)
    ):
        return (
            "Scalable mode requires a local Java runtime for Spark. "
            "Install Java and retry, or use smart/raw mode."
        )

    if "unsupportedclassversionerror" in msg or "class file version" in msg:
        return (
            "Scalable mode failed because Java is too old for the installed Spark version. "
            "Use Java 17+ and retry."
        )

    return f"Scalable mode failed during Spark processing: {error_msg}"
