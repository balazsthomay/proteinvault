"""End-to-end integration demo: ProteinGym data + ESM-2 scoring."""

import argparse
import time
import urllib.request
import zipfile
from pathlib import Path

import httpx
from scipy.stats import spearmanr

PROTEINGYM_BASE_URL = (
    "https://marks.hms.harvard.edu/proteingym/ProteinGym_v1.3"
)
SUBSTITUTIONS_ZIP = "DMS_ProteinGym_substitutions.zip"
REFERENCE_FILE = "DMS_substitutions.csv"

DEFAULT_DATASET = "BLAT_ECOLX_Stiffler_2015"

# Wild-type sequence for TEM-1 beta-lactamase (BLAT_ECOLX)
BLAT_ECOLX_WT = (
    "MSIQHFRVALIPFFAAFCLPVFAHPETLVKVKDAEDQLGARVGYIELDLNSGKILESFRPEERFPMMSTFKVLLCGAVLSRIDAGQE"
    "QLGRRIHYSQNDLVEYSPVTEKHLTDGMTVRELCSAAITMSDNTAANLLLTTIGGPKELTAFLHNMGDHVTRLDRWEPELNEAIPND"
    "ERDTTMPVAMATTLRKLLTGELLTLASRQQLIDWMEADKVAGPLLRSALPAGWFIADKSGAGERGSRGIIAALGPDGKPSRIVVIYTTG"
    "SQATMDERNRQIAEIGASLIKHW"
)

MODEL_SIZES = {
    "8M": {
        "version": "t6_8M_UR50D",
        "hf_model_name": "facebook/esm2_t6_8M_UR50D",
    },
    "35M": {
        "version": "t12_35M_UR50D",
        "hf_model_name": "facebook/esm2_t12_35M_UR50D",
    },
    "150M": {
        "version": "t30_150M_UR50D",
        "hf_model_name": "facebook/esm2_t30_150M_UR50D",
    },
    "650M": {
        "version": "t33_650M_UR50D",
        "hf_model_name": "facebook/esm2_t33_650M_UR50D",
    },
}


def download_proteingym_dataset(
    dataset_name: str, cache_dir: Path
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    csv_path = cache_dir / f"{dataset_name}.csv"

    if csv_path.exists():
        print(f"  Using cached: {csv_path}")
        return csv_path

    zip_path = cache_dir / SUBSTITUTIONS_ZIP
    if not zip_path.exists():
        url = f"{PROTEINGYM_BASE_URL}/{SUBSTITUTIONS_ZIP}"
        print(f"  Downloading {SUBSTITUTIONS_ZIP} from ProteinGym...")
        urllib.request.urlretrieve(url, zip_path)
        print(f"  Downloaded: {zip_path}")

    # Extract the specific dataset CSV
    with zipfile.ZipFile(zip_path, "r") as zf:
        target_name = f"{dataset_name}.csv"
        for name in zf.namelist():
            if name.endswith(target_name):
                data = zf.read(name)
                csv_path.write_bytes(data)
                print(f"  Extracted: {csv_path}")
                return csv_path

    raise FileNotFoundError(
        f"Dataset {dataset_name}.csv not found in {SUBSTITUTIONS_ZIP}"
    )


def run_demo(
    base_url: str,
    model_size: str,
    dataset_name: str,
    cache_dir: Path,
    max_sequences: int | None = None,
) -> None:
    client = httpx.Client(base_url=base_url, timeout=600.0)

    print(f"\n{'='*60}")
    print("ProteinVault Integration Demo")
    print(f"{'='*60}")

    # Step 1: Download data
    print(f"\n[1/7] Downloading ProteinGym dataset: {dataset_name}")
    csv_path = download_proteingym_dataset(dataset_name, cache_dir)
    csv_data = csv_path.read_text()
    lines = csv_data.strip().split("\n")
    total_variants = len(lines) - 1  # Subtract header
    print(f"  Total variants: {total_variants}")

    # Step 2: Create dataset
    print("\n[2/7] Creating dataset via API")
    ds_resp = client.post("/datasets", json={
        "name": dataset_name,
        "description": f"ProteinGym DMS substitution dataset: {dataset_name}",
    })
    ds_resp.raise_for_status()
    ds = ds_resp.json()
    ds_id = ds["id"]
    print(f"  Dataset created: {ds_id}")

    # Step 3: Load data in two versions (demonstrate versioning)
    print("\n[3/7] Loading data in two versions")

    mid = total_variants // 2 + 1  # +1 for header
    csv_part1 = "\n".join(lines[:mid])
    csv_part2 = lines[0] + "\n" + "\n".join(lines[mid:])

    load1_resp = client.post(
        f"/datasets/{ds_id}/loads/csv",
        files={"file": ("part1.csv", csv_part1.encode(), "text/csv")},
        data={
            "description": "First half of variants",
            "sequence_column": "mutated_sequence",
            "measurement_columns": "DMS_score",
            "name_column": "mutant",
        },
    )
    load1_resp.raise_for_status()
    v1 = load1_resp.json()
    print(f"  Version 1: {v1['sequence_count']} sequences loaded")

    load2_resp = client.post(
        f"/datasets/{ds_id}/loads/csv",
        files={"file": ("part2.csv", csv_part2.encode(), "text/csv")},
        data={
            "description": "Second half of variants",
            "sequence_column": "mutated_sequence",
            "measurement_columns": "DMS_score",
            "name_column": "mutant",
        },
    )
    load2_resp.raise_for_status()
    v2 = load2_resp.json()
    print(f"  Version 2: {v2['sequence_count']} more sequences loaded")

    # Step 4: Demonstrate versioning
    print("\n[4/7] Demonstrating version queries")

    seqs_v1 = client.get(
        f"/datasets/{ds_id}/sequences?version=1&limit=1"
    ).json()
    seqs_v2 = client.get(
        f"/datasets/{ds_id}/sequences?version=2&limit=1"
    ).json()
    print(f"  Version 1: {seqs_v1['total']} sequences")
    print(f"  Version 2: {seqs_v2['total']} sequences")

    # Undo and redo
    undo_resp = client.post(f"/datasets/{ds_id}/undo")
    undo_resp.raise_for_status()
    current = client.get(f"/datasets/{ds_id}").json()
    print(f"  After undo: version {current['current_version']}")

    redo_resp = client.post(f"/datasets/{ds_id}/redo")
    redo_resp.raise_for_status()
    current = client.get(f"/datasets/{ds_id}").json()
    print(f"  After redo: version {current['current_version']}")

    # Step 5: Register ESM-2 model
    model_config = MODEL_SIZES[model_size]
    print(f"\n[5/7] Registering ESM-2 model ({model_size} params)")
    model_resp = client.post("/models", json={
        "name": "esm2",
        "version": model_config["version"],
        "model_type": "esm2",
        "config": {"hf_model_name": model_config["hf_model_name"]},
    })
    model_resp.raise_for_status()
    model = model_resp.json()
    model_id = model["id"]
    print(f"  Model registered: {model_id}")

    # Step 6: Score all variants
    print(f"\n[6/7] Scoring variants with ESM-2 {model_size}")
    start_time = time.time()

    pred_resp = client.post(f"/models/{model_id}/predict", json={
        "dataset_id": ds_id,
        "wild_type_sequence": BLAT_ECOLX_WT,
        "scoring_method": "wildtype_marginal",
        "async_execution": False,
    })
    pred_resp.raise_for_status()
    prediction = pred_resp.json()

    elapsed = time.time() - start_time
    print(f"  Scored {prediction['score_count']} sequences in {elapsed:.1f}s")
    print(f"  Prediction ID: {prediction['id']}")
    print(f"  Data version: {prediction['data_version']}")

    # Step 7: Compute Spearman correlation
    print("\n[7/7] Computing Spearman correlation")

    # Get all prediction scores
    all_scores = []
    offset = 0
    batch_size = 1000
    while True:
        scores_resp = client.get(
            f"/predictions/{prediction['id']}/scores"
            f"?limit={batch_size}&offset={offset}"
        )
        scores_resp.raise_for_status()
        batch = scores_resp.json()
        if not batch:
            break
        all_scores.extend(batch)
        offset += batch_size

    # Get DMS_score measurements
    dms_scores = {}
    offset = 0
    while True:
        query_resp = client.get(
            f"/datasets/{ds_id}/query"
            f"?assay=DMS_score&limit={batch_size}&offset={offset}"
        )
        query_resp.raise_for_status()
        batch = query_resp.json()
        if not batch:
            break
        for row in batch:
            dms_scores[row["sequence_id"]] = row["value"]
        offset += batch_size

    # Match predicted scores with DMS scores
    predicted = []
    measured = []
    for score in all_scores:
        sid = score["sequence_id"]
        if sid in dms_scores:
            predicted.append(score["score"])
            measured.append(dms_scores[sid])

    if len(predicted) < 2:
        print("  ERROR: Not enough matched scores to compute correlation")
        return

    corr_result = spearmanr(predicted, measured)
    rho = float(corr_result[0])  # type: ignore[arg-type]
    pvalue = float(corr_result[1])  # type: ignore[arg-type]

    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"  Model:              ESM-2 {model_size}")
    print(f"  Dataset:            {dataset_name}")
    print(f"  Sequences scored:   {len(predicted)}")
    print(f"  Data version:       {prediction['data_version']}")
    print(f"  Scoring method:     {prediction['scoring_method']}")
    print(f"  Spearman rho:       {rho:.4f}")
    print(f"  p-value:            {pvalue:.2e}")
    print(f"  Inference time:     {elapsed:.1f}s")
    print(f"{'='*60}")

    if 0.1 < abs(rho) < 0.8:
        print("  Status: PASS (correlation in expected range)")
    else:
        print(
            f"  Status: CHECK (rho={rho:.4f} outside typical 0.3-0.7 range)"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ProteinVault integration demo"
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000/api/v1",
        help="API base URL",
    )
    parser.add_argument(
        "--model-size",
        default="8M",
        choices=list(MODEL_SIZES.keys()),
        help="ESM-2 model size (default: 8M)",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help="ProteinGym dataset name",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(Path.home() / ".cache" / "proteinvault" / "data"),
        help="Cache directory for downloaded data",
    )
    args = parser.parse_args()

    run_demo(
        base_url=args.base_url,
        model_size=args.model_size,
        dataset_name=args.dataset,
        cache_dir=Path(args.cache_dir),
    )


if __name__ == "__main__":
    main()
