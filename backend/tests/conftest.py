import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import main
import state


@pytest.fixture(autouse=True)
def clear_in_memory_state():
    state.datasets.clear()
    state.sessions.clear()
    state.jobs.clear()
    yield
    state.datasets.clear()
    state.sessions.clear()
    state.jobs.clear()


@pytest.fixture
def client():
    with TestClient(main.app) as test_client:
        yield test_client


@pytest.fixture
def seeded_dataset(tmp_path):
    csv_path = tmp_path / "guardian_fixture.csv"
    csv_path.write_text("x,y\n1,10\n2,20\n", encoding="utf-8")

    dataset_id = "guardian-test-dataset"
    state.datasets[dataset_id] = {
        "file_path": str(csv_path),
        "filename": "guardian_fixture.csv",
        "row_count": 2,
        "columns": [
            {"name": "x", "type": "INTEGER", "null_pct": 0.0},
            {"name": "y", "type": "INTEGER", "null_pct": 0.0},
        ],
        "sample": [{"x": 1, "y": 10}, {"x": 2, "y": 20}],
    }

    return dataset_id
