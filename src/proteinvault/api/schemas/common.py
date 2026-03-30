from datetime import datetime

from pydantic import BaseModel


class PaginatedResponse[T](BaseModel):
    items: list[T]
    total: int
    limit: int
    offset: int


class ErrorResponse(BaseModel):
    detail: str


class VersionInfo(BaseModel):
    id: str
    version_number: int
    description: str | None
    created_at: datetime
    is_active: bool
