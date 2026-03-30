"""E2E test exercising the full demo flow with small fixture data."""


import pytest
from fastapi.testclient import TestClient
from scipy.stats import spearmanr

# Small fixture data: 10 variants of a short protein
WILD_TYPE = "MVLSPADK"

FIXTURE_CSV = """mutant,mutated_sequence,DMS_score
M1A,AVLSPADK,-0.8
V2L,MLLSPADK,0.2
L3I,MVISPADK,-0.1
S4T,MVLTPADK,0.3
P5A,MVLSAADK,-0.6
A6G,MVLSPGDK,0.1
D7E,MVLSPAEK,0.4
K8R,MVLSPADR,0.5
M1V,VVLSPADK,-0.3
V2A,MALSPADK,-0.4
"""


@pytest.mark.slow
class TestDemoFlow:
    def test_full_e2e_flow(self, client: TestClient) -> None:
        base = "/api/v1"

        # Create dataset
        ds = client.post(f"{base}/datasets", json={
            "name": "test_e2e", "description": "E2E test dataset"
        }).json()
        ds_id = ds["id"]

        # Load CSV data
        load_resp = client.post(
            f"{base}/datasets/{ds_id}/loads/csv",
            files={"file": ("test.csv", FIXTURE_CSV.encode(), "text/csv")},
            data={
                "sequence_column": "mutated_sequence",
                "measurement_columns": "DMS_score",
                "name_column": "mutant",
            },
        )
        assert load_resp.status_code == 201
        load = load_resp.json()
        assert load["sequence_count"] == 10

        # Verify sequences loaded
        seqs = client.get(
            f"{base}/datasets/{ds_id}/sequences?limit=100"
        ).json()
        assert seqs["total"] == 10

        # Register ESM-2 8M model
        model = client.post(f"{base}/models", json={
            "name": "esm2",
            "version": "t6_8M",
            "model_type": "esm2",
            "config": {"hf_model_name": "facebook/esm2_t6_8M_UR50D"},
        }).json()

        # Score all variants
        pred = client.post(f"{base}/models/{model['id']}/predict", json={
            "dataset_id": ds_id,
            "wild_type_sequence": WILD_TYPE,
            "scoring_method": "wildtype_marginal",
        }).json()
        assert pred["score_count"] == 10
        assert pred["model_id"] == model["id"]
        assert pred["dataset_id"] == ds_id
        assert pred["data_version"] == 1

        # Get prediction scores
        scores = client.get(
            f"{base}/predictions/{pred['id']}/scores?limit=100"
        ).json()
        assert len(scores) == 10

        # Get DMS measurements
        dms = client.get(
            f"{base}/datasets/{ds_id}/query?assay=DMS_score&limit=100"
        ).json()

        # Build score maps
        dms_map = {r["sequence_id"]: r["value"] for r in dms}
        predicted = []
        measured = []
        for s in scores:
            if s["sequence_id"] in dms_map:
                predicted.append(s["score"])
                measured.append(dms_map[s["sequence_id"]])

        assert len(predicted) == 10

        # Compute Spearman correlation
        rho, pvalue = spearmanr(predicted, measured)
        assert -1.0 <= rho <= 1.0
        assert pvalue >= 0.0

        # Verify provenance
        pred_detail = client.get(
            f"{base}/predictions/{pred['id']}"
        ).json()
        assert pred_detail["model_id"] == model["id"]
        assert pred_detail["scoring_method"] == "wildtype_marginal"
