"""
In-memory application state.
In production, replace with Redis or a persistent database.
"""

# dataset_id -> { file_path, filename, row_count, columns, sample }
datasets: dict = {}

# session_id -> [ {"role": "user"/"assistant", "content": "..."} ]
sessions: dict = {}

# job_id -> {
#   status, job_type, dataset_id, payload, created_at, updated_at, result, error
# }
jobs: dict = {}
