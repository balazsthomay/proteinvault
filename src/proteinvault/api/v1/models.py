from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, Query, status

from proteinvault.api.schemas.models import ModelCreate, ModelResponse
from proteinvault.api.schemas.predictions import (
    PredictionResponse,
    PredictionScoreResponse,
    PredictRequest,
)
from proteinvault.api.schemas.tasks import TaskResponse
from proteinvault.dependencies import CursorDep
from proteinvault.services.model_service import ModelService
from proteinvault.services.prediction_service import PredictionService

router = APIRouter(tags=["models"])


def get_model_service(cursor: CursorDep) -> ModelService:
    return ModelService(cursor)


def get_prediction_service(cursor: CursorDep) -> PredictionService:
    return PredictionService(cursor)


ModelServiceDep = Annotated[ModelService, Depends(get_model_service)]
PredictionServiceDep = Annotated[
    PredictionService, Depends(get_prediction_service)
]


@router.post("/models", status_code=status.HTTP_201_CREATED)
def register_model(
    request: ModelCreate, service: ModelServiceDep
) -> ModelResponse:
    return service.register_model(request)


@router.get("/models")
def list_models(service: ModelServiceDep) -> list[ModelResponse]:
    return service.list_models()


@router.get("/models/{model_id}")
def get_model(model_id: str, service: ModelServiceDep) -> ModelResponse:
    return service.get_model(model_id)


@router.post("/models/{model_id}/predict")
def predict(
    model_id: str,
    request: PredictRequest,
    background_tasks: BackgroundTasks,
    pred_service: PredictionServiceDep,
) -> PredictionResponse | TaskResponse:
    if request.async_execution:
        task = pred_service.create_task(model_id, request)
        background_tasks.add_task(
            pred_service.execute_task, task.id, model_id, request
        )
        return task
    return pred_service.predict_sync(model_id, request)


@router.get("/tasks/{task_id}")
def get_task(task_id: str, service: PredictionServiceDep) -> TaskResponse:
    return service.get_task(task_id)


@router.get("/predictions/{prediction_id}")
def get_prediction(
    prediction_id: str, service: PredictionServiceDep
) -> PredictionResponse:
    return service.get_prediction(prediction_id)


@router.get("/predictions/{prediction_id}/scores")
def get_prediction_scores(
    prediction_id: str,
    service: PredictionServiceDep,
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
) -> list[PredictionScoreResponse]:
    return service.get_prediction_scores(prediction_id, limit, offset)
