import logging

import duckdb

from proteinvault.api.schemas.predictions import (
    PredictionResponse,
    PredictionScoreResponse,
    PredictRequest,
)
from proteinvault.api.schemas.tasks import TaskResponse
from proteinvault.db.repositories.dataset_repo import DatasetRepo
from proteinvault.db.repositories.model_repo import ModelRepo
from proteinvault.db.repositories.prediction_repo import PredictionRepo
from proteinvault.db.repositories.sequence_repo import SequenceRepo
from proteinvault.db.repositories.task_repo import TaskRepo
from proteinvault.domain.exceptions import (
    DatasetNotFoundError,
    ModelNotFoundError,
    PredictionNotFoundError,
    TaskNotFoundError,
)
from proteinvault.services.scoring.base import Scorer, ScoringRequest
from proteinvault.services.scoring.esm2 import ESM2Scorer

logger = logging.getLogger(__name__)

# Module-level scorer cache (single model at a time)
_loaded_scorer: Scorer | None = None
_loaded_model_id: str | None = None


def _get_scorer(model_id: str, config: dict) -> Scorer:
    global _loaded_scorer, _loaded_model_id  # noqa: PLW0603

    if _loaded_model_id == model_id and _loaded_scorer is not None:
        return _loaded_scorer

    if _loaded_scorer is not None:
        _loaded_scorer.unload()

    hf_model_name = config.get(
        "hf_model_name", "facebook/esm2_t6_8M_UR50D"
    )
    scorer = ESM2Scorer(model_name=hf_model_name)
    scorer.load()

    _loaded_scorer = scorer
    _loaded_model_id = model_id
    return scorer


class PredictionService:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.cursor = cursor
        self.model_repo = ModelRepo(cursor)
        self.dataset_repo = DatasetRepo(cursor)
        self.sequence_repo = SequenceRepo(cursor)
        self.prediction_repo = PredictionRepo(cursor)
        self.task_repo = TaskRepo(cursor)

    def predict_sync(
        self, model_id: str, request: PredictRequest
    ) -> PredictionResponse:
        model = self.model_repo.get(model_id)
        if model is None:
            raise ModelNotFoundError(model_id)

        ds = self.dataset_repo.get(request.dataset_id)
        if ds is None:
            raise DatasetNotFoundError(request.dataset_id)

        data_version = request.data_version
        if data_version is None:
            data_version = self.dataset_repo.get_current_version(
                request.dataset_id
            )

        # Get sequences to score
        if request.sequence_ids:
            seq_ids = request.sequence_ids
        else:
            seq_ids = self.sequence_repo.get_ids_at_version(
                request.dataset_id, data_version
            )

        sequences = []
        for sid in seq_ids:
            seq = self.sequence_repo.get_by_id(sid)
            if seq is not None and seq.chains:
                sequences.append((sid, seq.chains[0].sequence))

        # Score
        scorer = _get_scorer(model_id, model.config)
        scoring_requests = [
            ScoringRequest(sequence_id=sid, mutant_sequence=seq)
            for sid, seq in sequences
        ]
        results = scorer.score(request.wild_type_sequence, scoring_requests)

        # Store prediction
        prediction = self.prediction_repo.create(
            model_id=model_id,
            scoring_method=request.scoring_method,
            dataset_id=request.dataset_id,
            data_version=data_version,
        )

        scores = [
            {
                "prediction_id": prediction.id,
                "sequence_id": r.sequence_id,
                "score": r.score,
                "details": r.details,
            }
            for r in results
        ]
        self.prediction_repo.bulk_insert_scores(scores)

        return PredictionResponse(
            id=prediction.id,
            model_id=prediction.model_id,
            dataset_id=prediction.dataset_id,
            data_version=prediction.data_version,
            scoring_method=prediction.scoring_method,
            created_at=prediction.created_at,
            score_count=len(results),
        )

    def create_task(
        self, model_id: str, request: PredictRequest
    ) -> TaskResponse:
        model = self.model_repo.get(model_id)
        if model is None:
            raise ModelNotFoundError(model_id)

        task = self.task_repo.create("prediction")
        return TaskResponse(
            id=task.id,
            task_type=task.task_type,
            status=task.status,
            created_at=task.created_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            error_message=task.error_message,
            result_ref=task.result_ref,
        )

    def execute_task(
        self, task_id: str, model_id: str, request: PredictRequest
    ) -> None:
        try:
            self.task_repo.update_status(task_id, "running")
            result = self.predict_sync(model_id, request)
            self.task_repo.update_status(
                task_id, "completed", result_ref=result.id
            )
        except Exception as e:
            logger.exception("Task %s failed", task_id)
            self.task_repo.update_status(
                task_id, "failed", error_message=str(e)
            )

    def get_task(self, task_id: str) -> TaskResponse:
        task = self.task_repo.get(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        return TaskResponse(
            id=task.id,
            task_type=task.task_type,
            status=task.status,
            created_at=task.created_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            error_message=task.error_message,
            result_ref=task.result_ref,
        )

    def get_prediction(self, prediction_id: str) -> PredictionResponse:
        pred = self.prediction_repo.get(prediction_id)
        if pred is None:
            raise PredictionNotFoundError(prediction_id)
        count = self.prediction_repo.count_scores(prediction_id)
        return PredictionResponse(
            id=pred.id,
            model_id=pred.model_id,
            dataset_id=pred.dataset_id,
            data_version=pred.data_version,
            scoring_method=pred.scoring_method,
            created_at=pred.created_at,
            score_count=count,
        )

    def get_prediction_scores(
        self,
        prediction_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PredictionScoreResponse]:
        pred = self.prediction_repo.get(prediction_id)
        if pred is None:
            raise PredictionNotFoundError(prediction_id)
        scores = self.prediction_repo.get_scores(prediction_id, limit, offset)
        return [
            PredictionScoreResponse(
                sequence_id=s.sequence_id,
                score=s.score,
                details=s.details,
            )
            for s in scores
        ]
