import json
import uuid
from dataclasses import dataclass
from datetime import datetime

import duckdb


@dataclass
class ModelRecord:
    id: str
    name: str
    version: str
    model_type: str
    config: dict
    created_at: datetime


class ModelRepo:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.cursor = cursor

    def create(
        self,
        name: str,
        version: str,
        model_type: str,
        config: dict,
    ) -> ModelRecord:
        record_id = str(uuid.uuid4())
        self.cursor.execute(
            "INSERT INTO models (id, name, version, model_type, config) VALUES (?, ?, ?, ?, ?)",
            [record_id, name, version, model_type, json.dumps(config)],
        )
        return self.get(record_id)  # type: ignore[return-value]

    def get(self, model_id: str) -> ModelRecord | None:
        result = self.cursor.execute(
            "SELECT id, name, version, model_type, config, created_at FROM models WHERE id = ?",
            [model_id],
        ).fetchone()
        if result is None:
            return None
        return ModelRecord(
            id=result[0],
            name=result[1],
            version=result[2],
            model_type=result[3],
            config=json.loads(result[4]) if isinstance(result[4], str) else result[4],
            created_at=result[5],
        )

    def get_by_name_version(self, name: str, version: str) -> ModelRecord | None:
        result = self.cursor.execute(
            "SELECT id, name, version, model_type, config, created_at "
            "FROM models WHERE name = ? AND version = ?",
            [name, version],
        ).fetchone()
        if result is None:
            return None
        return ModelRecord(
            id=result[0],
            name=result[1],
            version=result[2],
            model_type=result[3],
            config=json.loads(result[4]) if isinstance(result[4], str) else result[4],
            created_at=result[5],
        )

    def list_all(self) -> list[ModelRecord]:
        rows = self.cursor.execute(
            "SELECT id, name, version, model_type, config, created_at "
            "FROM models ORDER BY created_at DESC"
        ).fetchall()
        return [
            ModelRecord(
                id=row[0],
                name=row[1],
                version=row[2],
                model_type=row[3],
                config=json.loads(row[4]) if isinstance(row[4], str) else row[4],
                created_at=row[5],
            )
            for row in rows
        ]
