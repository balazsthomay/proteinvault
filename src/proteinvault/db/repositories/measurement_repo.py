import uuid
from dataclasses import dataclass

import duckdb


@dataclass
class MeasurementRecord:
    id: str
    sequence_id: str
    load_id: str
    assay_name: str
    value: float
    metadata: str | None


class MeasurementRepo:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.cursor = cursor

    def bulk_insert(self, measurements: list[dict]) -> int:
        count = 0
        for m in measurements:
            m_id = str(uuid.uuid4())
            self.cursor.execute(
                "INSERT INTO measurements (id, sequence_id, load_id, assay_name, value, metadata) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    m_id,
                    m["sequence_id"],
                    m["load_id"],
                    m["assay_name"],
                    m["value"],
                    m.get("metadata"),
                ],
            )
            count += 1
        return count

    def get_at_version(
        self,
        dataset_id: str,
        version: int,
        assay_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[MeasurementRecord]:
        query = """
            SELECT m.id, m.sequence_id, m.load_id, m.assay_name, m.value, m.metadata
            FROM measurements m
            JOIN data_loads dl ON dl.id = m.load_id
            WHERE dl.dataset_id = ? AND dl.version_number <= ? AND dl.is_active = true
        """
        params: list[object] = [dataset_id, version]

        if assay_name is not None:
            query += " AND m.assay_name = ?"
            params.append(assay_name)

        query += " ORDER BY m.value DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self.cursor.execute(query, params).fetchall()
        return [MeasurementRecord(*row) for row in rows]

    def query(
        self,
        dataset_id: str,
        version: int,
        assay_name: str | None = None,
        min_value: float | None = None,
        max_value: float | None = None,
        order_by: str = "value",
        order: str = "desc",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        query = """
            SELECT s.id as sequence_id, s.name, c.sequence, m.assay_name, m.value
            FROM measurements m
            JOIN sequences s ON s.id = m.sequence_id
            JOIN chains c ON c.sequence_id = s.id
            JOIN data_loads dl ON dl.id = m.load_id
            WHERE dl.dataset_id = ? AND dl.version_number <= ? AND dl.is_active = true
        """
        params: list[object] = [dataset_id, version]

        if assay_name is not None:
            query += " AND m.assay_name = ?"
            params.append(assay_name)
        if min_value is not None:
            query += " AND m.value >= ?"
            params.append(min_value)
        if max_value is not None:
            query += " AND m.value <= ?"
            params.append(max_value)

        order_dir = "DESC" if order.lower() == "desc" else "ASC"
        if order_by == "value":
            query += f" ORDER BY m.value {order_dir}"
        else:
            query += f" ORDER BY s.name {order_dir}"

        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self.cursor.execute(query, params).fetchall()
        columns = ["sequence_id", "name", "sequence", "assay_name", "value"]
        return [dict(zip(columns, row, strict=True)) for row in rows]
