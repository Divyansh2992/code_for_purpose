import time

from routers import query as query_router


def test_query_response_includes_column_lineage(client, seeded_dataset, monkeypatch):
    monkeypatch.setattr(
        query_router.llm_service,
        "generate_sql",
        lambda *_args, **_kwargs: 'SELECT "x", AVG("y") AS "avg_y" FROM data GROUP BY "x"',
    )
    monkeypatch.setattr(
        query_router.llm_service,
        "explain_result",
        lambda *_args, **_kwargs: {
            "explanation": "Average y for each x.",
            "insights": ["y grows as x increases"],
            "why_analysis": "x and y are directly related.",
        },
    )

    response = client.post(
        "/query",
        json={
            "dataset_id": seeded_dataset,
            "question": "average y by x",
            "mode": "raw",
            "guardian_enabled": False,
        },
    )
    body = response.json()

    assert response.status_code == 200
    assert "lineage" in body

    lineage = body["lineage"]
    assert set(lineage["source_columns"]) == {"x", "y"}
    assert set(lineage["sql_columns"]) == {"x", "y"}
    assert set(lineage["explanation_columns"]) == {"x", "y"}
    assert "x" in lineage["result_columns"]
    assert "avg_y" in lineage["derived_columns"]


def test_preprocess_background_job_completes(client, seeded_dataset):
    start_resp = client.post("/jobs/preprocess", json={"dataset_id": seeded_dataset})
    assert start_resp.status_code == 200

    start_body = start_resp.json()
    job_id = start_body["job_id"]

    final_status = None
    final_body = None

    for _ in range(60):
        poll_resp = client.get(f"/jobs/{job_id}")
        assert poll_resp.status_code == 200
        final_body = poll_resp.json()
        final_status = final_body["status"]
        if final_status in {"completed", "failed"}:
            break
        time.sleep(0.05)

    assert final_status == "completed"
    assert "result" in final_body
    assert "outlier_count" in final_body["result"]
    assert "preprocessing_log" in final_body["result"]
