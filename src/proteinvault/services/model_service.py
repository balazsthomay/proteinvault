import duckdb

from proteinvault.api.schemas.models import ModelCreate, ModelResponse
from proteinvault.db.repositories.model_repo import ModelRepo
from proteinvault.domain.exceptions import DuplicateModelVersionError, ModelNotFoundError


class ModelService:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.model_repo = ModelRepo(cursor)

    def register_model(self, request: ModelCreate) -> ModelResponse:
        existing = self.model_repo.get_by_name_version(
            request.name, request.version
        )
        if existing is not None:
            raise DuplicateModelVersionError(request.name, request.version)

        record = self.model_repo.create(
            name=request.name,
            version=request.version,
            model_type=request.model_type,
            config=request.config,
        )
        return ModelResponse(
            id=record.id,
            name=record.name,
            version=record.version,
            model_type=record.model_type,
            config=record.config,
            created_at=record.created_at,
        )

    def get_model(self, model_id: str) -> ModelResponse:
        record = self.model_repo.get(model_id)
        if record is None:
            raise ModelNotFoundError(model_id)
        return ModelResponse(
            id=record.id,
            name=record.name,
            version=record.version,
            model_type=record.model_type,
            config=record.config,
            created_at=record.created_at,
        )

    def list_models(self) -> list[ModelResponse]:
        records = self.model_repo.list_all()
        return [
            ModelResponse(
                id=r.id,
                name=r.name,
                version=r.version,
                model_type=r.model_type,
                config=r.config,
                created_at=r.created_at,
            )
            for r in records
        ]
