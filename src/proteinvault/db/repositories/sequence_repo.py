import uuid
from dataclasses import dataclass

import duckdb


@dataclass
class ChainRecord:
    id: str
    sequence_id: str
    chain_id: str
    chain_index: int
    sequence: str


@dataclass
class SequenceRecord:
    id: str
    dataset_id: str
    load_id: str
    molecule_type: str
    name: str | None
    metadata: str | None  # JSON string
    chains: list[ChainRecord]


class SequenceRepo:
    def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
        self.cursor = cursor

    def bulk_insert(
        self,
        dataset_id: str,
        load_id: str,
        sequences: list[dict],
    ) -> list[str]:
        inserted_ids: list[str] = []
        for seq_data in sequences:
            seq_id = str(uuid.uuid4())
            self.cursor.execute(
                "INSERT INTO sequences (id, dataset_id, load_id, molecule_type, name, metadata) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    seq_id,
                    dataset_id,
                    load_id,
                    seq_data["molecule_type"],
                    seq_data.get("name"),
                    seq_data.get("metadata"),
                ],
            )
            for chain in seq_data["chains"]:
                chain_id = str(uuid.uuid4())
                self.cursor.execute(
                    "INSERT INTO chains (id, sequence_id, chain_id, chain_index, sequence) "
                    "VALUES (?, ?, ?, ?, ?)",
                    [
                        chain_id,
                        seq_id,
                        chain["chain_id"],
                        chain["chain_index"],
                        chain["sequence"],
                    ],
                )
            inserted_ids.append(seq_id)
        return inserted_ids

    def get_at_version(
        self,
        dataset_id: str,
        version: int,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SequenceRecord]:
        rows = self.cursor.execute(
            """
            SELECT s.id, s.dataset_id, s.load_id, s.molecule_type, s.name, s.metadata
            FROM sequences s
            JOIN data_loads dl ON dl.id = s.load_id
            WHERE dl.dataset_id = ? AND dl.version_number <= ? AND dl.is_active = true
            ORDER BY s.id
            LIMIT ? OFFSET ?
            """,
            [dataset_id, version, limit, offset],
        ).fetchall()

        records = []
        for row in rows:
            seq_id = row[0]
            chains = self._get_chains(seq_id)
            records.append(
                SequenceRecord(
                    id=row[0],
                    dataset_id=row[1],
                    load_id=row[2],
                    molecule_type=row[3],
                    name=row[4],
                    metadata=row[5],
                    chains=chains,
                )
            )
        return records

    def get_by_id(self, sequence_id: str) -> SequenceRecord | None:
        result = self.cursor.execute(
            "SELECT id, dataset_id, load_id, molecule_type, name, metadata "
            "FROM sequences WHERE id = ?",
            [sequence_id],
        ).fetchone()
        if result is None:
            return None
        chains = self._get_chains(result[0])
        return SequenceRecord(*result, chains=chains)

    def count_at_version(self, dataset_id: str, version: int) -> int:
        result = self.cursor.execute(
            """
            SELECT COUNT(*)
            FROM sequences s
            JOIN data_loads dl ON dl.id = s.load_id
            WHERE dl.dataset_id = ? AND dl.version_number <= ? AND dl.is_active = true
            """,
            [dataset_id, version],
        ).fetchone()
        return result[0] if result else 0

    def get_ids_at_version(
        self, dataset_id: str, version: int
    ) -> list[str]:
        rows = self.cursor.execute(
            """
            SELECT s.id
            FROM sequences s
            JOIN data_loads dl ON dl.id = s.load_id
            WHERE dl.dataset_id = ? AND dl.version_number <= ? AND dl.is_active = true
            ORDER BY s.id
            """,
            [dataset_id, version],
        ).fetchall()
        return [row[0] for row in rows]

    def _get_chains(self, sequence_id: str) -> list[ChainRecord]:
        rows = self.cursor.execute(
            "SELECT id, sequence_id, chain_id, chain_index, sequence "
            "FROM chains WHERE sequence_id = ? ORDER BY chain_index",
            [sequence_id],
        ).fetchall()
        return [ChainRecord(*row) for row in rows]
