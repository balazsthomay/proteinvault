from datetime import datetime

from pydantic import BaseModel


class PredictRequest(BaseModel):
    dataset_id: str
    data_version: int | None = None
    sequence_ids: list[str] | None = None
    wild_type_sequence: str
    scoring_method: str = "wildtype_marginal"
    async_execution: bool = False


class PredictionResponse(BaseModel):
    id: str
    model_id: str
    dataset_id: str | None
    data_version: int | None
    scoring_method: str
    created_at: datetime
    score_count: int


class PredictionScoreResponse(BaseModel):
    sequence_id: str
    score: float
    details: dict | None
