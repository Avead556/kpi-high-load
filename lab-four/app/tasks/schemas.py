from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel

TaskStatus = Literal["queued", "running", "done", "failed", "cancelled"]


class TaskResultOut(BaseModel):
    token: int
    worker_id: str
    result: dict[str, Any]
    created_at: datetime


class TaskOut(BaseModel):
    id: int
    record_id: int
    status: TaskStatus
    seed: int
    attempts: int
    max_attempts: int
    token: int
    lease_until: datetime | None
    worker_id: str | None
    cancel_requested_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    result: TaskResultOut | None = None


class TaskPage(BaseModel):
    items: list[TaskOut]
    total: int
    limit: int
    offset: int
