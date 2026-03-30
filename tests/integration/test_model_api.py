from fastapi.testclient import TestClient


class TestModelRegistry:
    def test_register_model(self, client: TestClient) -> None:
        resp = client.post("/api/v1/models", json={
            "name": "esm2",
            "version": "t6_8M",
            "model_type": "esm2",
            "config": {"hf_model_name": "facebook/esm2_t6_8M_UR50D"},
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "esm2"
        assert data["version"] == "t6_8M"

    def test_list_models(self, client: TestClient) -> None:
        client.post("/api/v1/models", json={
            "name": "esm2", "version": "8M",
            "model_type": "esm2", "config": {},
        })
        client.post("/api/v1/models", json={
            "name": "esm2", "version": "150M",
            "model_type": "esm2", "config": {},
        })
        resp = client.get("/api/v1/models")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_get_model(self, client: TestClient) -> None:
        create_resp = client.post("/api/v1/models", json={
            "name": "esm2", "version": "8M",
            "model_type": "esm2", "config": {"key": "value"},
        })
        model_id = create_resp.json()["id"]
        resp = client.get(f"/api/v1/models/{model_id}")
        assert resp.status_code == 200
        assert resp.json()["config"] == {"key": "value"}

    def test_duplicate_version_rejected(self, client: TestClient) -> None:
        client.post("/api/v1/models", json={
            "name": "esm2", "version": "8M",
            "model_type": "esm2", "config": {},
        })
        resp = client.post("/api/v1/models", json={
            "name": "esm2", "version": "8M",
            "model_type": "esm2", "config": {},
        })
        assert resp.status_code == 409

    def test_get_nonexistent_model(self, client: TestClient) -> None:
        resp = client.get("/api/v1/models/nonexistent")
        assert resp.status_code == 404
