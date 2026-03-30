import json
import uuid
from dataclasses import dataclass
from datetime import datetime

import duckdb


@dataclass
class PredictionRecord:
    id: str
    model_id: str
    task_id: str | None
    dataset_id: str | None
    data_version: int | None
    scoring_method: str
    created_at: datetime


@dataclass
class PredictionScoreRecord:
    id: str
    prediction_id: str
    sequence_id: str
    score: float
    details: dict | None


class PredictionRepo:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.cursor = cursor

    def create(
        self,
        model_id: str,
        scoring_method: str,
        task_id: str | None = None,
        dataset_id: str | None = None,
        data_version: int | None = None,
    ) -> PredictionRecord:
        pred_id = str(uuid.uuid4())
        self.cursor.execute(
            "INSERT INTO predictions "
            "(id, model_id, task_id, dataset_id, data_version, scoring_method) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [pred_id, model_id, task_id, dataset_id, data_version, scoring_method],
        )
        return self.get(pred_id)  # type: ignore[return-value]

    def get(self, prediction_id: str) -> PredictionRecord | None:
        result = self.cursor.execute(
            "SELECT id, model_id, task_id, dataset_id, data_version, scoring_method, created_at "
            "FROM predictions WHERE id = ?",
            [prediction_id],
        ).fetchone()
        if result is None:
            return None
        return PredictionRecord(*result)

    def bulk_insert_scores(self, scores: list[dict]) -> int:
        count = 0
        for s in scores:
            score_id = str(uuid.uuid4())
            details_json = json.dumps(s["details"]) if s.get("details") else None
            self.cursor.execute(
                "INSERT INTO prediction_scores (id, prediction_id, sequence_id, score, details) "
                "VALUES (?, ?, ?, ?, ?)",
                [score_id, s["prediction_id"], s["sequence_id"], s["score"], details_json],
            )
            count += 1
        return count

    def get_scores(
        self,
        prediction_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PredictionScoreRecord]:
        rows = self.cursor.execute(
            "SELECT id, prediction_id, sequence_id, score, details "
            "FROM prediction_scores WHERE prediction_id = ? "
            "ORDER BY score DESC LIMIT ? OFFSET ?",
            [prediction_id, limit, offset],
        ).fetchall()
        return [
            PredictionScoreRecord(
                id=row[0],
                prediction_id=row[1],
                sequence_id=row[2],
                score=row[3],
                details=json.loads(row[4]) if isinstance(row[4], str) else row[4],
            )
            for row in rows
        ]

    def count_scores(self, prediction_id: str) -> int:
        result = self.cursor.execute(
            "SELECT COUNT(*) FROM prediction_scores WHERE prediction_id = ?",
            [prediction_id],
        ).fetchone()
        return result[0] if result else 0
