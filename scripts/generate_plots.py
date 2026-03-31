"""Generate README plots from cached ProteinGym data + ESM-2 scoring."""
# ruff: noqa: E402, I001

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import spearmanr

from proteinvault.services.scoring.base import ScoringRequest
from proteinvault.services.scoring.esm2 import ESM2Scorer

CACHE_DIR = Path.home() / ".cache" / "proteinvault" / "data"
DATASET = "BLAT_ECOLX_Stiffler_2015"
ASSETS_DIR = Path(__file__).parent.parent / "assets"

BLAT_ECOLX_WT = (
    "MSIQHFRVALIPFFAAFCLPVFAHPETLVKVKDAEDQLGARVGYIELDLNSGKILESFRPEERFPMMSTFKVLLCGAVLSRIDAGQE"
    "QLGRRIHYSQNDLVEYSPVTEKHLTDGMTVRELCSAAITMSDNTAANLLLTTIGGPKELTAFLHNMGDHVTRLDRWEPELNEAIPND"
    "ERDTTMPVAMATTLRKLLTGELLTLASRQQLIDWMEADKVAGPLLRSALPAGWFIADKSGAGERGSRGIIAALGPDGKPSRIVVIYTTG"
    "SQATMDERNRQIAEIGASLIKHW"
)


def load_dataset() -> list[dict]:
    csv_path = CACHE_DIR / f"{DATASET}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Run the demo first to download the dataset: {csv_path}"
        )
    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "mutant": row["mutant"],
                "sequence": row["mutated_sequence"],
                "dms_score": float(row["DMS_score"]),
            })
    return rows


def score_with_esm2(
    rows: list[dict], model_name: str, label: str
) -> tuple[list[float], list[float]]:
    print(f"  Loading {label}...")
    scorer = ESM2Scorer(model_name=model_name)
    scorer.load()

    requests = [
        ScoringRequest(sequence_id=str(i), mutant_sequence=row["sequence"])
        for i, row in enumerate(rows)
    ]

    print(f"  Scoring {len(requests)} variants...")
    results = scorer.score(BLAT_ECOLX_WT, requests)
    scorer.unload()

    predicted = [r.score for r in results]
    measured = [rows[int(r.sequence_id)]["dms_score"] for r in results]
    return predicted, measured


def plot_correlation(
    predicted: list[float],
    measured: list[float],
    model_label: str,
    filename: str,
) -> None:
    rho = float(spearmanr(predicted, measured)[0])  # type: ignore[arg-type]

    fig, ax = plt.subplots(figsize=(7, 6))

    ax.scatter(
        measured, predicted,
        alpha=0.15, s=8, c="#4C72B0", edgecolors="none", rasterized=True,
    )

    # Trend line
    z = np.polyfit(measured, predicted, 1)
    p = np.poly1d(z)
    x_range = np.linspace(min(measured), max(measured), 100)
    ax.plot(x_range, p(x_range), color="#C44E52", linewidth=2, linestyle="--")

    ax.set_xlabel("Measured Fitness (DMS Score)", fontsize=12)
    ax.set_ylabel("ESM-2 Predicted Score", fontsize=12)
    ax.set_title(
        f"{model_label} | BLAT_ECOLX (n={len(predicted):,})\n"
        f"Spearman $\\rho$ = {rho:.4f}",
        fontsize=13,
    )

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(labelsize=10)

    fig.tight_layout()
    out = ASSETS_DIR / filename
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def main() -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)

    rows = load_dataset()
    print(f"Loaded {len(rows)} variants from {DATASET}")

    # ESM-2 8M
    print("\n[1/2] ESM-2 8M")
    pred_8m, meas_8m = score_with_esm2(
        rows, "facebook/esm2_t6_8M_UR50D", "ESM-2 8M"
    )
    plot_correlation(pred_8m, meas_8m, "ESM-2 8M", "correlation_8m.png")

    # ESM-2 650M
    print("\n[2/2] ESM-2 650M")
    pred_650m, meas_650m = score_with_esm2(
        rows, "facebook/esm2_t33_650M_UR50D", "ESM-2 650M"
    )
    plot_correlation(
        pred_650m, meas_650m, "ESM-2 650M", "correlation_650m.png"
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
