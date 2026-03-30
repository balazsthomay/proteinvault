from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Query, UploadFile, status

from proteinvault.api.schemas.common import PaginatedResponse, VersionInfo
from proteinvault.api.schemas.datasets import (
    DataLoadCreate,
    DataLoadResponse,
    DatasetCreate,
    DatasetResponse,
    SequenceResponse,
)
from proteinvault.dependencies import CursorDep
from proteinvault.domain.enums import MoleculeType
from proteinvault.services.dataset_service import DatasetService

router = APIRouter(prefix="/datasets", tags=["datasets"])


def get_dataset_service(cursor: CursorDep) -> DatasetService:
    return DatasetService(cursor)


ServiceDep = Annotated[DatasetService, Depends(get_dataset_service)]


@router.post("", status_code=status.HTTP_201_CREATED)
def create_dataset(request: DatasetCreate, service: ServiceDep) -> DatasetResponse:
    return service.create_dataset(request)


@router.get("")
def list_datasets(service: ServiceDep) -> list[DatasetResponse]:
    return service.list_datasets()


@router.get("/{dataset_id}")
def get_dataset(dataset_id: str, service: ServiceDep) -> DatasetResponse:
    return service.get_dataset(dataset_id)


@router.post("/{dataset_id}/loads", status_code=status.HTTP_201_CREATED)
def create_load(
    dataset_id: str, request: DataLoadCreate, service: ServiceDep
) -> DataLoadResponse:
    return service.create_load(dataset_id, request)


@router.post("/{dataset_id}/loads/csv", status_code=status.HTTP_201_CREATED)
async def create_load_from_csv(
    dataset_id: str,
    service: ServiceDep,
    file: Annotated[UploadFile, File(...)],
    description: Annotated[str | None, Form()] = None,
    sequence_column: Annotated[str, Form()] = "mutated_sequence",
    measurement_columns: Annotated[str, Form()] = "DMS_score",
    name_column: Annotated[str, Form()] = "mutant",
    molecule_type: Annotated[MoleculeType, Form()] = MoleculeType.PROTEIN,
) -> DataLoadResponse:
    content = await file.read()
    return service.create_load_from_csv(
        dataset_id=dataset_id,
        file_content=content,
        description=description,
        sequence_column=sequence_column,
        measurement_columns=measurement_columns,
        name_column=name_column,
        molecule_type=molecule_type,
    )


@router.get("/{dataset_id}/versions")
def list_versions(dataset_id: str, service: ServiceDep) -> list[VersionInfo]:
    return service.list_versions(dataset_id)


@router.get("/{dataset_id}/sequences")
def get_sequences(
    dataset_id: str,
    service: ServiceDep,
    version: int | None = Query(None),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
) -> PaginatedResponse[SequenceResponse]:
    return service.get_sequences(dataset_id, version, limit, offset)


@router.post("/{dataset_id}/undo")
def undo(dataset_id: str, service: ServiceDep) -> VersionInfo:
    return service.undo(dataset_id)


@router.post("/{dataset_id}/redo")
def redo(dataset_id: str, service: ServiceDep) -> VersionInfo:
    return service.redo(dataset_id)


@router.get("/{dataset_id}/query")
def query_data(
    dataset_id: str,
    service: ServiceDep,
    version: int | None = Query(None),
    assay: str | None = Query(None),
    min_value: float | None = Query(None),
    max_value: float | None = Query(None),
    order_by: str = Query("value"),
    order: str = Query("desc"),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
) -> list[dict]:
    return service.query(
        dataset_id, version, assay, min_value, max_value, order_by, order, limit, offset
    )
