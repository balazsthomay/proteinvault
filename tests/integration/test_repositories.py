import duckdb
import pytest

from proteinvault.db.repositories.dataset_repo import DatasetRepo
from proteinvault.db.repositories.measurement_repo import MeasurementRepo
from proteinvault.db.repositories.sequence_repo import SequenceRepo
from proteinvault.db.schema import create_schema


@pytest.fixture
def db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    create_schema(conn)
    return conn


def _make_sequence_data(
    name: str, sequence: str, measurements: dict[str, float] | None = None
) -> dict:
    data: dict = {
        "molecule_type": "protein",
        "name": name,
        "metadata": None,
        "chains": [{"chain_id": "A", "chain_index": 0, "sequence": sequence}],
    }
    if measurements:
        data["measurements"] = measurements
    return data


class TestDatasetRepo:
    def test_create_and_get(self, db: duckdb.DuckDBPyConnection) -> None:
        repo = DatasetRepo(db)
        ds = repo.create("test_dataset", "A test dataset")
        assert ds.name == "test_dataset"
        assert ds.description == "A test dataset"

        fetched = repo.get(ds.id)
        assert fetched is not None
        assert fetched.id == ds.id

    def test_list_all(self, db: duckdb.DuckDBPyConnection) -> None:
        repo = DatasetRepo(db)
        repo.create("ds1")
        repo.create("ds2")
        result = repo.list_all()
        assert len(result) == 2

    def test_version_numbers_increment(self, db: duckdb.DuckDBPyConnection) -> None:
        repo = DatasetRepo(db)
        ds = repo.create("test")
        load1 = repo.create_load(ds.id, "first load")
        load2 = repo.create_load(ds.id, "second load")
        assert load1.version_number == 1
        assert load2.version_number == 2
        assert repo.get_current_version(ds.id) == 2

    def test_undo_redo(self, db: duckdb.DuckDBPyConnection) -> None:
        repo = DatasetRepo(db)
        ds = repo.create("test")
        repo.create_load(ds.id, "v1")
        repo.create_load(ds.id, "v2")
        assert repo.get_current_version(ds.id) == 2

        # Undo
        undone = repo.undo_load(ds.id)
        assert undone is not None
        assert undone.version_number == 2
        assert not undone.is_active
        assert repo.get_current_version(ds.id) == 1

        # Redo
        redone = repo.redo_load(ds.id)
        assert redone is not None
        assert redone.version_number == 2
        assert redone.is_active
        assert repo.get_current_version(ds.id) == 2

    def test_undo_nothing(self, db: duckdb.DuckDBPyConnection) -> None:
        repo = DatasetRepo(db)
        ds = repo.create("test")
        result = repo.undo_load(ds.id)
        assert result is None

    def test_redo_nothing(self, db: duckdb.DuckDBPyConnection) -> None:
        repo = DatasetRepo(db)
        ds = repo.create("test")
        repo.create_load(ds.id)
        result = repo.redo_load(ds.id)
        assert result is None

    def test_list_versions(self, db: duckdb.DuckDBPyConnection) -> None:
        repo = DatasetRepo(db)
        ds = repo.create("test")
        repo.create_load(ds.id, "v1")
        repo.create_load(ds.id, "v2")
        versions = repo.list_versions(ds.id)
        assert len(versions) == 2
        assert versions[0].version_number == 1
        assert versions[1].version_number == 2


class TestSequenceRepo:
    def test_bulk_insert_and_versioned_query(
        self, db: duckdb.DuckDBPyConnection
    ) -> None:
        ds_repo = DatasetRepo(db)
        seq_repo = SequenceRepo(db)

        ds = ds_repo.create("test")
        load1 = ds_repo.create_load(ds.id, "v1")
        load2 = ds_repo.create_load(ds.id, "v2")

        # Insert 3 sequences in load 1
        seq_repo.bulk_insert(ds.id, load1.id, [
            _make_sequence_data("seq1", "MVLSPADK"),
            _make_sequence_data("seq2", "ACDEFGHI"),
            _make_sequence_data("seq3", "KLMNPQRS"),
        ])

        # Insert 2 sequences in load 2
        seq_repo.bulk_insert(ds.id, load2.id, [
            _make_sequence_data("seq4", "TVWYACDE"),
            _make_sequence_data("seq5", "FGHIKLMN"),
        ])

        # Query at version 1: should see 3
        at_v1 = seq_repo.get_at_version(ds.id, 1, limit=100)
        assert len(at_v1) == 3

        # Query at version 2: should see all 5
        at_v2 = seq_repo.get_at_version(ds.id, 2, limit=100)
        assert len(at_v2) == 5

    def test_undo_hides_sequences(self, db: duckdb.DuckDBPyConnection) -> None:
        ds_repo = DatasetRepo(db)
        seq_repo = SequenceRepo(db)

        ds = ds_repo.create("test")
        load1 = ds_repo.create_load(ds.id, "v1")
        load2 = ds_repo.create_load(ds.id, "v2")

        seq_repo.bulk_insert(ds.id, load1.id, [
            _make_sequence_data("seq1", "MVLSPADK"),
        ])
        seq_repo.bulk_insert(ds.id, load2.id, [
            _make_sequence_data("seq2", "ACDEFGHI"),
        ])

        # Before undo: current version sees both
        current_v = ds_repo.get_current_version(ds.id)
        assert seq_repo.count_at_version(ds.id, current_v) == 2

        # After undo: current version sees only load 1
        ds_repo.undo_load(ds.id)
        current_v = ds_repo.get_current_version(ds.id)
        assert current_v == 1
        assert seq_repo.count_at_version(ds.id, current_v) == 1

    def test_count_at_version(self, db: duckdb.DuckDBPyConnection) -> None:
        ds_repo = DatasetRepo(db)
        seq_repo = SequenceRepo(db)

        ds = ds_repo.create("test")
        load1 = ds_repo.create_load(ds.id)
        ids = seq_repo.bulk_insert(ds.id, load1.id, [
            _make_sequence_data(f"seq{i}", "MVLSPADK") for i in range(50)
        ])
        assert len(ids) == 50
        assert seq_repo.count_at_version(ds.id, 1) == 50

    def test_pagination(self, db: duckdb.DuckDBPyConnection) -> None:
        ds_repo = DatasetRepo(db)
        seq_repo = SequenceRepo(db)

        ds = ds_repo.create("test")
        load1 = ds_repo.create_load(ds.id)
        seq_repo.bulk_insert(ds.id, load1.id, [
            _make_sequence_data(f"seq{i}", "MVLSPADK") for i in range(10)
        ])

        page1 = seq_repo.get_at_version(ds.id, 1, limit=3, offset=0)
        page2 = seq_repo.get_at_version(ds.id, 1, limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        assert page1[0].id != page2[0].id

    def test_multi_chain_sequences(self, db: duckdb.DuckDBPyConnection) -> None:
        ds_repo = DatasetRepo(db)
        seq_repo = SequenceRepo(db)

        ds = ds_repo.create("antibodies")
        load = ds_repo.create_load(ds.id)
        seq_repo.bulk_insert(ds.id, load.id, [{
            "molecule_type": "protein",
            "name": "antibody_1",
            "metadata": None,
            "chains": [
                {"chain_id": "H", "chain_index": 0, "sequence": "EVQLVESGG"},
                {"chain_id": "L", "chain_index": 1, "sequence": "DIQMTQSPS"},
            ],
        }])

        seqs = seq_repo.get_at_version(ds.id, 1)
        assert len(seqs) == 1
        assert len(seqs[0].chains) == 2
        assert seqs[0].chains[0].chain_id == "H"
        assert seqs[0].chains[1].chain_id == "L"


class TestMeasurementRepo:
    def test_bulk_insert_and_query(self, db: duckdb.DuckDBPyConnection) -> None:
        ds_repo = DatasetRepo(db)
        seq_repo = SequenceRepo(db)
        m_repo = MeasurementRepo(db)

        ds = ds_repo.create("test")
        load = ds_repo.create_load(ds.id)
        seq_repo.bulk_insert(ds.id, load.id, [
            _make_sequence_data("wt", "MVLSPADK"),
            _make_sequence_data("mut1", "AVLSPADK"),
        ])

        seqs = seq_repo.get_at_version(ds.id, 1)
        m_repo.bulk_insert([
            {"sequence_id": seqs[0].id, "load_id": load.id,
             "assay_name": "DMS_score", "value": 1.0},
            {"sequence_id": seqs[1].id, "load_id": load.id,
             "assay_name": "DMS_score", "value": -0.5},
        ])

        results = m_repo.query(ds.id, 1, assay_name="DMS_score")
        assert len(results) == 2
        assert results[0]["value"] > results[1]["value"]  # Default order is desc

    def test_query_with_value_range(self, db: duckdb.DuckDBPyConnection) -> None:
        ds_repo = DatasetRepo(db)
        seq_repo = SequenceRepo(db)
        m_repo = MeasurementRepo(db)

        ds = ds_repo.create("test")
        load = ds_repo.create_load(ds.id)
        seq_repo.bulk_insert(ds.id, load.id, [
            _make_sequence_data(f"seq{i}", "MVLSPADK") for i in range(5)
        ])

        seqs = seq_repo.get_at_version(ds.id, 1)
        values = [-2.0, -0.5, 0.0, 0.5, 2.0]
        m_repo.bulk_insert([
            {"sequence_id": seqs[i].id, "load_id": load.id, "assay_name": "DMS_score", "value": v}
            for i, v in enumerate(values)
        ])

        results = m_repo.query(ds.id, 1, assay_name="DMS_score", min_value=-0.5, max_value=0.5)
        assert len(results) == 3
        assert all(-0.5 <= r["value"] <= 0.5 for r in results)

    def test_versioned_measurements(self, db: duckdb.DuckDBPyConnection) -> None:
        ds_repo = DatasetRepo(db)
        seq_repo = SequenceRepo(db)
        m_repo = MeasurementRepo(db)

        ds = ds_repo.create("test")
        load1 = ds_repo.create_load(ds.id)
        load2 = ds_repo.create_load(ds.id)

        seq_repo.bulk_insert(ds.id, load1.id, [_make_sequence_data("seq1", "MVLSPADK")])
        seq_repo.bulk_insert(ds.id, load2.id, [_make_sequence_data("seq2", "ACDEFGHI")])

        seqs_v1 = seq_repo.get_at_version(ds.id, 1)
        seqs_v2 = seq_repo.get_at_version(ds.id, 2)

        m_repo.bulk_insert([
            {"sequence_id": seqs_v1[0].id, "load_id": load1.id,
             "assay_name": "DMS_score", "value": 1.0},
        ])
        m_repo.bulk_insert([
            {"sequence_id": seqs_v2[1].id, "load_id": load2.id,
             "assay_name": "DMS_score", "value": 2.0},
        ])

        # Version 1 should have 1 measurement
        results_v1 = m_repo.get_at_version(ds.id, 1)
        assert len(results_v1) == 1

        # Version 2 should have 2 measurements
        results_v2 = m_repo.get_at_version(ds.id, 2)
        assert len(results_v2) == 2
