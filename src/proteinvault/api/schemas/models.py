from datetime import datetime

from pydantic import BaseModel, Field


class ModelCreate(BaseModel):
    name: str = Field(..., min_length=1)
    version: str = Field(..., min_length=1)
    model_type: str = Field(..., min_length=1)
    config: dict = Field(default_factory=dict)


class ModelResponse(BaseModel):
    id: str
    name: str
    version: str
    model_type: str
    config: dict
    created_at: datetime
