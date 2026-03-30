from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from proteinvault.api.v1.router import v1_router
from proteinvault.config import settings
from proteinvault.db.connection import close_db, init_db
from proteinvault.domain.exceptions import (
    DatasetNotFoundError,
    DuplicateModelVersionError,
    ModelNotFoundError,
    NothingToRedoError,
    NothingToUndoError,
    PredictionNotFoundError,
    SequenceValidationError,
    TaskNotFoundError,
)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    init_db(settings)
    yield
    close_db()


def create_app() -> FastAPI:
    app = FastAPI(
        title="ProteinVault",
        version="0.1.0",
        description="Versioned protein data service with ML model gateway",
        lifespan=lifespan,
    )

    _register_exception_handlers(app)
    app.include_router(v1_router, prefix="/api/v1")
    return app


def _register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(DatasetNotFoundError)
    async def dataset_not_found(_req: Request, exc: DatasetNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ModelNotFoundError)
    async def model_not_found(_req: Request, exc: ModelNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(TaskNotFoundError)
    async def task_not_found(_req: Request, exc: TaskNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(PredictionNotFoundError)
    async def prediction_not_found(
        _req: Request, exc: PredictionNotFoundError
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(SequenceValidationError)
    async def validation_error(_req: Request, exc: SequenceValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(DuplicateModelVersionError)
    async def duplicate_model(_req: Request, exc: DuplicateModelVersionError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(NothingToUndoError)
    async def nothing_to_undo(_req: Request, exc: NothingToUndoError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(NothingToRedoError)
    async def nothing_to_redo(_req: Request, exc: NothingToRedoError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})


app = create_app()
