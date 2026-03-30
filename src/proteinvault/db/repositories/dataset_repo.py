import uuid
from dataclasses import dataclass
from datetime import datetime

import duckdb


@dataclass
class DatasetRecord:
    id: str
    name: str
    description: str | None
    created_at: datetime


@dataclass
class DataLoadRecord:
    id: str
    dataset_id: str
    version_number: int
    description: str | None
    created_at: datetime
    is_active: bool


class DatasetRepo:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.cursor = cursor

    def create(self, name: str, description: str | None = None) -> DatasetRecord:
        record_id = str(uuid.uuid4())
        self.cursor.execute(
            "INSERT INTO datasets (id, name, description) VALUES (?, ?, ?)",
            [record_id, name, description],
        )
        return self.get(record_id)  # type: ignore[return-value]

    def get(self, dataset_id: str) -> DatasetRecord | None:
        result = self.cursor.execute(
            "SELECT id, name, description, created_at FROM datasets WHERE id = ?",
            [dataset_id],
        ).fetchone()
        if result is None:
            return None
        return DatasetRecord(*result)

    def list_all(self) -> list[DatasetRecord]:
        rows = self.cursor.execute(
            "SELECT id, name, description, created_at FROM datasets ORDER BY created_at DESC"
        ).fetchall()
        return [DatasetRecord(*row) for row in rows]

    def get_current_version(self, dataset_id: str) -> int:
        result = self.cursor.execute(
            "SELECT COALESCE(MAX(version_number), 0) FROM data_loads "
            "WHERE dataset_id = ? AND is_active = true",
            [dataset_id],
        ).fetchone()
        return result[0] if result else 0

    def create_load(
        self, dataset_id: str, description: str | None = None
    ) -> DataLoadRecord:
        next_version = self._next_version_number(dataset_id)
        load_id = str(uuid.uuid4())
        self.cursor.execute(
            "INSERT INTO data_loads (id, dataset_id, version_number, description) "
            "VALUES (?, ?, ?, ?)",
            [load_id, dataset_id, next_version, description],
        )
        return self.get_load(load_id)  # type: ignore[return-value]

    def get_load(self, load_id: str) -> DataLoadRecord | None:
        result = self.cursor.execute(
            "SELECT id, dataset_id, version_number, description, created_at, is_active "
            "FROM data_loads WHERE id = ?",
            [load_id],
        ).fetchone()
        if result is None:
            return None
        return DataLoadRecord(*result)

    def list_versions(self, dataset_id: str) -> list[DataLoadRecord]:
        rows = self.cursor.execute(
            "SELECT id, dataset_id, version_number, description, created_at, is_active "
            "FROM data_loads WHERE dataset_id = ? ORDER BY version_number",
            [dataset_id],
        ).fetchall()
        return [DataLoadRecord(*row) for row in rows]

    def undo_load(self, dataset_id: str) -> DataLoadRecord | None:
        result = self.cursor.execute(
            "SELECT id FROM data_loads "
            "WHERE dataset_id = ? AND is_active = true "
            "ORDER BY version_number DESC LIMIT 1",
            [dataset_id],
        ).fetchone()
        if result is None:
            return None
        load_id = result[0]
        self.cursor.execute(
            "UPDATE data_loads SET is_active = false WHERE id = ?", [load_id]
        )
        return self.get_load(load_id)

    def redo_load(self, dataset_id: str) -> DataLoadRecord | None:
        result = self.cursor.execute(
            "SELECT id FROM data_loads "
            "WHERE dataset_id = ? AND is_active = false "
            "ORDER BY version_number ASC LIMIT 1",
            [dataset_id],
        ).fetchone()
        if result is None:
            return None
        load_id = result[0]
        self.cursor.execute(
            "UPDATE data_loads SET is_active = true WHERE id = ?", [load_id]
        )
        return self.get_load(load_id)

    def _next_version_number(self, dataset_id: str) -> int:
        result = self.cursor.execute(
            "SELECT COALESCE(MAX(version_number), 0) FROM data_loads WHERE dataset_id = ?",
            [dataset_id],
        ).fetchone()
        return (result[0] if result else 0) + 1
