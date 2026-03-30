from datetime import datetime

from pydantic import BaseModel


class TaskResponse(BaseModel):
    id: str
    task_type: str
    status: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None
    result_ref: str | None
