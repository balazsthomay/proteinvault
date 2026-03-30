import pytest
from fastapi.testclient import TestClient


def _setup_data_and_model(client: TestClient) -> tuple[str, str, str]:
    """Create a dataset with sequences and register a model. Returns (ds_id, model_id, wt)."""
    wt = "MVLSPADK"

    # Create dataset and load sequences
    ds = client.post("/api/v1/datasets", json={"name": "test_ds"}).json()
    ds_id = ds["id"]
    client.post(f"/api/v1/datasets/{ds_id}/loads", json={
        "sequences": [
            {"name": "wt", "sequence": wt, "measurements": {"DMS_score": 0.0}},
            {"name": "M1A", "sequence": "AVLSPADK", "measurements": {"DMS_score": -0.5}},
            {"name": "V2L", "sequence": "MLLSPADK", "measurements": {"DMS_score": 0.3}},
        ],
    })

    # Register model
    model = client.post("/api/v1/models", json={
        "name": "esm2",
        "version": "t6_8M",
        "model_type": "esm2",
        "config": {"hf_model_name": "facebook/esm2_t6_8M_UR50D"},
    }).json()

    return ds_id, model["id"], wt


@pytest.mark.slow
class TestSyncPrediction:
    def test_predict_sync(self, client: TestClient) -> None:
        ds_id, model_id, wt = _setup_data_and_model(client)

        resp = client.post(f"/api/v1/models/{model_id}/predict", json={
            "dataset_id": ds_id,
            "wild_type_sequence": wt,
            "scoring_method": "wildtype_marginal",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["model_id"] == model_id
        assert data["dataset_id"] == ds_id
        assert data["data_version"] == 1
        assert data["score_count"] == 3

    def test_prediction_provenance(self, client: TestClient) -> None:
        ds_id, model_id, wt = _setup_data_and_model(client)

        pred_resp = client.post(f"/api/v1/models/{model_id}/predict", json={
            "dataset_id": ds_id,
            "wild_type_sequence": wt,
        }).json()

        # Get prediction details
        resp = client.get(f"/api/v1/predictions/{pred_resp['id']}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["model_id"] == model_id
        assert data["dataset_id"] == ds_id
        assert data["scoring_method"] == "wildtype_marginal"

    def test_prediction_scores(self, client: TestClient) -> None:
        ds_id, model_id, wt = _setup_data_and_model(client)

        pred_resp = client.post(f"/api/v1/models/{model_id}/predict", json={
            "dataset_id": ds_id,
            "wild_type_sequence": wt,
        }).json()

        resp = client.get(
            f"/api/v1/predictions/{pred_resp['id']}/scores"
        )
        assert resp.status_code == 200
        scores = resp.json()
        assert len(scores) == 3
        # Each score should have a numeric value
        for s in scores:
            assert isinstance(s["score"], float)

    def test_predict_nonexistent_model(self, client: TestClient) -> None:
        ds = client.post(
            "/api/v1/datasets", json={"name": "ds"}
        ).json()
        resp = client.post("/api/v1/models/nonexistent/predict", json={
            "dataset_id": ds["id"],
            "wild_type_sequence": "MVLS",
        })
        assert resp.status_code == 404


@pytest.mark.slow
class TestAsyncPrediction:
    def test_async_predict(self, client: TestClient) -> None:
        ds_id, model_id, wt = _setup_data_and_model(client)

        # Submit async task
        resp = client.post(f"/api/v1/models/{model_id}/predict", json={
            "dataset_id": ds_id,
            "wild_type_sequence": wt,
            "async_execution": True,
        })
        assert resp.status_code == 200
        task_data = resp.json()
        assert task_data["task_type"] == "prediction"
        assert "id" in task_data

        # Check task status
        task_resp = client.get(f"/api/v1/tasks/{task_data['id']}")
        assert task_resp.status_code == 200

    def test_get_nonexistent_task(self, client: TestClient) -> None:
        resp = client.get("/api/v1/tasks/nonexistent")
        assert resp.status_code == 404
