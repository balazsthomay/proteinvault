import contextlib
import csv
import io

import duckdb

from proteinvault.api.schemas.common import PaginatedResponse, VersionInfo
from proteinvault.api.schemas.datasets import (
    ChainResponse,
    DataLoadCreate,
    DataLoadResponse,
    DatasetCreate,
    DatasetResponse,
    SequenceInput,
    SequenceResponse,
)
from proteinvault.db.repositories.dataset_repo import DatasetRepo
from proteinvault.db.repositories.measurement_repo import MeasurementRepo
from proteinvault.db.repositories.sequence_repo import SequenceRepo
from proteinvault.domain.enums import MoleculeType
from proteinvault.domain.exceptions import (
    DatasetNotFoundError,
    NothingToRedoError,
    NothingToUndoError,
    SequenceValidationError,
)
from proteinvault.domain.validation import validate_sequence


class DatasetService:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.dataset_repo = DatasetRepo(cursor)
        self.sequence_repo = SequenceRepo(cursor)
        self.measurement_repo = MeasurementRepo(cursor)

    def create_dataset(self, request: DatasetCreate) -> DatasetResponse:
        ds = self.dataset_repo.create(request.name, request.description)
        return DatasetResponse(
            id=ds.id,
            name=ds.name,
            description=ds.description,
            created_at=ds.created_at,
            current_version=0,
        )

    def get_dataset(self, dataset_id: str) -> DatasetResponse:
        ds = self.dataset_repo.get(dataset_id)
        if ds is None:
            raise DatasetNotFoundError(dataset_id)
        version = self.dataset_repo.get_current_version(dataset_id)
        return DatasetResponse(
            id=ds.id,
            name=ds.name,
            description=ds.description,
            created_at=ds.created_at,
            current_version=version,
        )

    def list_datasets(self) -> list[DatasetResponse]:
        datasets = self.dataset_repo.list_all()
        result = []
        for ds in datasets:
            version = self.dataset_repo.get_current_version(ds.id)
            result.append(DatasetResponse(
                id=ds.id,
                name=ds.name,
                description=ds.description,
                created_at=ds.created_at,
                current_version=version,
            ))
        return result

    def create_load(
        self, dataset_id: str, request: DataLoadCreate
    ) -> DataLoadResponse:
        ds = self.dataset_repo.get(dataset_id)
        if ds is None:
            raise DatasetNotFoundError(dataset_id)

        # Validate all sequences
        self._validate_sequences(request.sequences)

        # Create load
        load = self.dataset_repo.create_load(dataset_id, request.description)

        # Prepare and insert sequences
        seq_data_list = []
        for seq_input in request.sequences:
            seq_data = self._sequence_input_to_dict(seq_input)
            seq_data_list.append(seq_data)

        inserted_ids = self.sequence_repo.bulk_insert(
            dataset_id, load.id, seq_data_list
        )

        # Insert measurements using the returned sequence IDs
        for seq_id, seq_input in zip(
            inserted_ids, request.sequences, strict=True
        ):
            for assay_name, value in seq_input.measurements.items():
                self.measurement_repo.bulk_insert([{
                    "sequence_id": seq_id,
                    "load_id": load.id,
                    "assay_name": assay_name,
                    "value": value,
                }])

        return DataLoadResponse(
            id=load.id,
            version_number=load.version_number,
            description=load.description,
            created_at=load.created_at,
            sequence_count=len(inserted_ids),
        )

    def create_load_from_csv(
        self,
        dataset_id: str,
        file_content: bytes,
        description: str | None = None,
        sequence_column: str = "mutated_sequence",
        measurement_columns: str = "DMS_score",
        name_column: str = "mutant",
        molecule_type: MoleculeType = MoleculeType.PROTEIN,
    ) -> DataLoadResponse:
        ds = self.dataset_repo.get(dataset_id)
        if ds is None:
            raise DatasetNotFoundError(dataset_id)

        text = file_content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))

        m_cols = [c.strip() for c in measurement_columns.split(",")]

        sequences: list[SequenceInput] = []
        for row in reader:
            seq_str = row.get(sequence_column, "").strip()
            if not seq_str:
                continue

            name = row.get(name_column, "").strip() or None
            measurements: dict[str, float] = {}
            for mc in m_cols:
                val_str = row.get(mc, "").strip()
                if val_str:
                    with contextlib.suppress(ValueError):
                        measurements[mc] = float(val_str)

            sequences.append(SequenceInput(
                name=name,
                molecule_type=molecule_type,
                sequence=seq_str,
                measurements=measurements,
            ))

        request = DataLoadCreate(description=description, sequences=sequences)
        return self.create_load(dataset_id, request)

    def list_versions(self, dataset_id: str) -> list[VersionInfo]:
        ds = self.dataset_repo.get(dataset_id)
        if ds is None:
            raise DatasetNotFoundError(dataset_id)
        loads = self.dataset_repo.list_versions(dataset_id)
        return [
            VersionInfo(
                id=load.id,
                version_number=load.version_number,
                description=load.description,
                created_at=load.created_at,
                is_active=load.is_active,
            )
            for load in loads
        ]

    def get_sequences(
        self,
        dataset_id: str,
        version: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedResponse[SequenceResponse]:
        ds = self.dataset_repo.get(dataset_id)
        if ds is None:
            raise DatasetNotFoundError(dataset_id)

        if version is None:
            version = self.dataset_repo.get_current_version(dataset_id)

        total = self.sequence_repo.count_at_version(dataset_id, version)
        records = self.sequence_repo.get_at_version(dataset_id, version, limit, offset)

        items = [
            SequenceResponse(
                id=r.id,
                name=r.name,
                molecule_type=r.molecule_type,
                chains=[
                    ChainResponse(
                        chain_id=c.chain_id,
                        chain_index=c.chain_index,
                        sequence=c.sequence,
                    )
                    for c in r.chains
                ],
            )
            for r in records
        ]

        return PaginatedResponse(items=items, total=total, limit=limit, offset=offset)

    def undo(self, dataset_id: str) -> VersionInfo:
        ds = self.dataset_repo.get(dataset_id)
        if ds is None:
            raise DatasetNotFoundError(dataset_id)
        load = self.dataset_repo.undo_load(dataset_id)
        if load is None:
            raise NothingToUndoError(dataset_id)
        return VersionInfo(
            id=load.id,
            version_number=load.version_number,
            description=load.description,
            created_at=load.created_at,
            is_active=load.is_active,
        )

    def redo(self, dataset_id: str) -> VersionInfo:
        ds = self.dataset_repo.get(dataset_id)
        if ds is None:
            raise DatasetNotFoundError(dataset_id)
        load = self.dataset_repo.redo_load(dataset_id)
        if load is None:
            raise NothingToRedoError(dataset_id)
        return VersionInfo(
            id=load.id,
            version_number=load.version_number,
            description=load.description,
            created_at=load.created_at,
            is_active=load.is_active,
        )

    def query(
        self,
        dataset_id: str,
        version: int | None = None,
        assay: str | None = None,
        min_value: float | None = None,
        max_value: float | None = None,
        order_by: str = "value",
        order: str = "desc",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        ds = self.dataset_repo.get(dataset_id)
        if ds is None:
            raise DatasetNotFoundError(dataset_id)
        if version is None:
            version = self.dataset_repo.get_current_version(dataset_id)
        return self.measurement_repo.query(
            dataset_id, version, assay, min_value, max_value, order_by, order, limit, offset
        )

    def _validate_sequences(self, sequences: list[SequenceInput]) -> None:
        errors: list[str] = []
        for i, seq_input in enumerate(sequences):
            if seq_input.sequence:
                result = validate_sequence(
                    seq_input.sequence, seq_input.molecule_type, strict=False
                )
                if not result.is_valid:
                    errors.append(f"Sequence {i}: {'; '.join(result.errors)}")
            for chain in seq_input.chains:
                result = validate_sequence(
                    chain.sequence, seq_input.molecule_type, strict=False
                )
                if not result.is_valid:
                    errors.append(
                        f"Sequence {i}, chain {chain.chain_id}: {'; '.join(result.errors)}"
                    )
        if errors:
            raise SequenceValidationError("\n".join(errors))

    def _sequence_input_to_dict(self, seq_input: SequenceInput) -> dict:
        chains: list[dict]
        if seq_input.sequence:
            chains = [{"chain_id": "A", "chain_index": 0, "sequence": seq_input.sequence}]
        else:
            chains = [
                {"chain_id": c.chain_id, "chain_index": c.chain_index, "sequence": c.sequence}
                for c in seq_input.chains
            ]
        return {
            "molecule_type": seq_input.molecule_type.value,
            "name": seq_input.name,
            "metadata": None,
            "chains": chains,
        }
