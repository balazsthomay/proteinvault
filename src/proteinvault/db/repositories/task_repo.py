import uuid
from dataclasses import dataclass
from datetime import datetime

import duckdb


@dataclass
class TaskRecord:
    id: str
    task_type: str
    status: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None
    result_ref: str | None


class TaskRepo:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.cursor = cursor

    def create(self, task_type: str) -> TaskRecord:
        task_id = str(uuid.uuid4())
        self.cursor.execute(
            "INSERT INTO tasks (id, task_type, status) VALUES (?, ?, 'pending')",
            [task_id, task_type],
        )
        return self.get(task_id)  # type: ignore[return-value]

    def get(self, task_id: str) -> TaskRecord | None:
        result = self.cursor.execute(
            "SELECT id, task_type, status, created_at, started_at, completed_at, "
            "error_message, result_ref FROM tasks WHERE id = ?",
            [task_id],
        ).fetchone()
        if result is None:
            return None
        return TaskRecord(*result)

    def update_status(
        self,
        task_id: str,
        status: str,
        error_message: str | None = None,
        result_ref: str | None = None,
    ) -> None:
        if status == "running":
            self.cursor.execute(
                "UPDATE tasks SET status = ?, started_at = current_timestamp WHERE id = ?",
                [status, task_id],
            )
        elif status in ("completed", "failed"):
            self.cursor.execute(
                "UPDATE tasks SET status = ?, completed_at = current_timestamp, "
                "error_message = ?, result_ref = ? WHERE id = ?",
                [status, error_message, result_ref, task_id],
            )
        else:
            self.cursor.execute(
                "UPDATE tasks SET status = ? WHERE id = ?", [status, task_id]
            )
