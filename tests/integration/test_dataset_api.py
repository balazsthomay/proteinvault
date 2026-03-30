import io

from fastapi.testclient import TestClient


class TestDatasetCRUD:
    def test_create_dataset(self, client: TestClient) -> None:
        resp = client.post("/api/v1/datasets", json={
            "name": "test_dataset", "description": "A test"
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "test_dataset"
        assert data["current_version"] == 0

    def test_list_datasets(self, client: TestClient) -> None:
        client.post("/api/v1/datasets", json={"name": "ds1"})
        client.post("/api/v1/datasets", json={"name": "ds2"})
        resp = client.get("/api/v1/datasets")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_get_dataset(self, client: TestClient) -> None:
        create_resp = client.post("/api/v1/datasets", json={"name": "ds"})
        ds_id = create_resp.json()["id"]
        resp = client.get(f"/api/v1/datasets/{ds_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == ds_id

    def test_get_nonexistent_dataset(self, client: TestClient) -> None:
        resp = client.get("/api/v1/datasets/nonexistent")
        assert resp.status_code == 404


class TestDataLoading:
    def test_load_sequences(self, client: TestClient) -> None:
        ds = client.post("/api/v1/datasets", json={"name": "ds"}).json()
        resp = client.post(f"/api/v1/datasets/{ds['id']}/loads", json={
            "description": "first load",
            "sequences": [
                {"name": "wt", "sequence": "MVLSPADK"},
                {"name": "mut1", "sequence": "AVLSPADK", "measurements": {"DMS_score": -0.5}},
            ],
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["version_number"] == 1
        assert data["sequence_count"] == 2

    def test_load_increments_version(self, client: TestClient) -> None:
        ds = client.post("/api/v1/datasets", json={"name": "ds"}).json()
        client.post(f"/api/v1/datasets/{ds['id']}/loads", json={
            "sequences": [{"name": "s1", "sequence": "MVLSPADK"}]
        })
        resp2 = client.post(f"/api/v1/datasets/{ds['id']}/loads", json={
            "sequences": [{"name": "s2", "sequence": "ACDEFGHI"}]
        })
        assert resp2.json()["version_number"] == 2

    def test_load_invalid_sequence(self, client: TestClient) -> None:
        ds = client.post("/api/v1/datasets", json={"name": "ds"}).json()
        resp = client.post(f"/api/v1/datasets/{ds['id']}/loads", json={
            "sequences": [{"name": "bad", "sequence": "MVL123"}]
        })
        assert resp.status_code == 422

    def test_load_csv(self, client: TestClient) -> None:
        ds = client.post("/api/v1/datasets", json={"name": "ds"}).json()
        csv_content = "mutant,mutated_sequence,DMS_score\nM1A,AVLSPADK,-0.5\nV2L,MLLSPADK,0.3\n"
        resp = client.post(
            f"/api/v1/datasets/{ds['id']}/loads/csv",
            files={"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")},
            data={"description": "csv load", "sequence_column": "mutated_sequence",
                  "measurement_columns": "DMS_score", "name_column": "mutant"},
        )
        assert resp.status_code == 201
        assert resp.json()["sequence_count"] == 2

    def test_load_multi_chain(self, client: TestClient) -> None:
        ds = client.post("/api/v1/datasets", json={"name": "antibodies"}).json()
        resp = client.post(f"/api/v1/datasets/{ds['id']}/loads", json={
            "sequences": [{
                "name": "ab1",
                "chains": [
                    {"chain_id": "H", "chain_index": 0, "sequence": "EVQLVESGG"},
                    {"chain_id": "L", "chain_index": 1, "sequence": "DIQMTQSPS"},
                ],
            }],
        })
        assert resp.status_code == 201


class TestVersioning:
    def _setup_versioned_data(self, client: TestClient) -> str:
        ds = client.post("/api/v1/datasets", json={"name": "ds"}).json()
        ds_id = ds["id"]
        client.post(f"/api/v1/datasets/{ds_id}/loads", json={
            "sequences": [
                {"name": f"s{i}", "sequence": "MVLSPADK", "measurements": {"DMS_score": float(i)}}
                for i in range(3)
            ],
        })
        client.post(f"/api/v1/datasets/{ds_id}/loads", json={
            "sequences": [
                {"name": f"s{i}", "sequence": "ACDEFGHI", "measurements": {"DMS_score": float(i)}}
                for i in range(3, 5)
            ],
        })
        return ds_id

    def test_query_at_version(self, client: TestClient) -> None:
        ds_id = self._setup_versioned_data(client)

        # Version 1: 3 sequences
        resp = client.get(f"/api/v1/datasets/{ds_id}/sequences?version=1")
        assert resp.json()["total"] == 3

        # Version 2: 5 sequences
        resp = client.get(f"/api/v1/datasets/{ds_id}/sequences?version=2")
        assert resp.json()["total"] == 5

    def test_undo_redo(self, client: TestClient) -> None:
        ds_id = self._setup_versioned_data(client)

        # Undo
        resp = client.post(f"/api/v1/datasets/{ds_id}/undo")
        assert resp.status_code == 200
        assert resp.json()["version_number"] == 2
        assert not resp.json()["is_active"]

        # Current version should show only v1 data
        ds = client.get(f"/api/v1/datasets/{ds_id}").json()
        assert ds["current_version"] == 1

        seqs = client.get(f"/api/v1/datasets/{ds_id}/sequences").json()
        assert seqs["total"] == 3

        # Redo
        resp = client.post(f"/api/v1/datasets/{ds_id}/redo")
        assert resp.status_code == 200
        assert resp.json()["is_active"]

        seqs = client.get(f"/api/v1/datasets/{ds_id}/sequences").json()
        assert seqs["total"] == 5

    def test_undo_nothing(self, client: TestClient) -> None:
        ds = client.post("/api/v1/datasets", json={"name": "empty"}).json()
        resp = client.post(f"/api/v1/datasets/{ds['id']}/undo")
        assert resp.status_code == 409

    def test_list_versions(self, client: TestClient) -> None:
        ds_id = self._setup_versioned_data(client)
        resp = client.get(f"/api/v1/datasets/{ds_id}/versions")
        assert resp.status_code == 200
        versions = resp.json()
        assert len(versions) == 2
        assert versions[0]["version_number"] == 1
        assert versions[1]["version_number"] == 2


class TestQuery:
    def test_query_by_assay(self, client: TestClient) -> None:
        ds = client.post("/api/v1/datasets", json={"name": "ds"}).json()
        client.post(f"/api/v1/datasets/{ds['id']}/loads", json={
            "sequences": [
                {"name": "s1", "sequence": "MVLSPADK", "measurements": {"DMS_score": 1.5}},
                {"name": "s2", "sequence": "ACDEFGHI", "measurements": {"DMS_score": -0.5}},
                {"name": "s3", "sequence": "KLMNPQRS", "measurements": {"DMS_score": 0.3}},
            ],
        })

        resp = client.get(f"/api/v1/datasets/{ds['id']}/query?assay=DMS_score&order=desc")
        assert resp.status_code == 200
        results = resp.json()
        assert len(results) == 3
        assert results[0]["value"] >= results[1]["value"]

    def test_query_with_range(self, client: TestClient) -> None:
        ds = client.post("/api/v1/datasets", json={"name": "ds"}).json()
        client.post(f"/api/v1/datasets/{ds['id']}/loads", json={
            "sequences": [
                {"name": "s1", "sequence": "MVLSPADK", "measurements": {"DMS_score": 2.0}},
                {"name": "s2", "sequence": "ACDEFGHI", "measurements": {"DMS_score": 0.5}},
                {"name": "s3", "sequence": "KLMNPQRS", "measurements": {"DMS_score": -1.0}},
            ],
        })

        resp = client.get(
            f"/api/v1/datasets/{ds['id']}/query?assay=DMS_score&min_value=0&max_value=1"
        )
        results = resp.json()
        assert len(results) == 1
        assert results[0]["value"] == 0.5
