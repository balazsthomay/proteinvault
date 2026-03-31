# ProteinVault

A versioned data service for biological sequences and assay measurements, with an integrated ML model gateway for protein fitness prediction.

Stores proteins, multi-chain complexes, and nucleotide sequences in append-only versioned tables. Registers and serves protein language models (ESM-2) with full provenance tracking -- every prediction records exactly which model version and data version produced it. Works end-to-end with real ProteinGym deep mutational scanning data.

## Architecture

```
                          FastAPI (REST API)
                               |
                    +----------+-----------+
                    |                      |
            Component A              Component B
         (Data Layer)            (Model Gateway)
                    |                      |
             +------+------+        +------+------+
             |      |      |        |      |      |
          Dataset  Seq   Meas.    Model  Predict  Task
          Repo     Repo   Repo    Repo   Repo     Repo
             |      |      |        |      |      |
             +------+------+--------+------+------+
                           |
                     DuckDB (persistent)
                    append-only versioned
```

### Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Database | DuckDB | Zero-config, Arrow-native columnar storage. Excellent for analytical queries over assay data (filter by fitness, rank, compare). Single-writer is fine for a local single-user service. Tradeoff vs PostgreSQL: no concurrent writers, but no setup friction and better analytical performance. |
| Versioning | Append-only with `is_active` flag | Each data load creates a new immutable version. Undo/redo by toggling `is_active` on loads -- no data ever deleted. Event-sourced design gives full auditability. |
| Multi-chain | `sequences` -> `chains` (1:N) | Single-chain proteins use 1 chain. Antibodies use 2 (H/L). Homo-multimers use N. Unified schema, no special-casing. |
| Sequence validation | Type-specific alphabets with strict/lenient | Handles 20 standard AAs, non-standard (U,O,X,B,Z,J), gaps for MSA data, DNA, RNA. Strict mode for clean data, lenient for real-world messiness. |
| Scoring | Wildtype marginal | Single forward pass through ESM-2 for ALL mutants of a wild-type. O(1) in number of mutants. ~1000x faster than masked marginal scoring. |
| ML framework | HuggingFace `transformers` | Actively maintained, Python 3.12 compatible, MIT license. `fair-esm` (Meta's original package) hasn't been updated since November 2022. |
| Async inference | FastAPI BackgroundTasks | No external task queue needed. Submit prediction -> get task ID -> poll for completion. Production systems would swap this for Celery/Redis. |

### Data Versioning Model

```
Dataset
  |-- DataLoad (version 1, is_active=true)
  |     |-- Sequence A (+ chains, measurements)
  |     |-- Sequence B (+ chains, measurements)
  |
  |-- DataLoad (version 2, is_active=true)
        |-- Sequence C (+ chains, measurements)

Query at version 1: returns A, B
Query at version 2: returns A, B, C
Undo: sets version 2 is_active=false -> current version is 1
Redo: sets version 2 is_active=true -> current version is 2
```

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

### Run locally

```bash
# Install dependencies
uv sync

# Start the server
uv run uvicorn proteinvault.main:app --reload

# API docs at http://localhost:8000/docs
```

### Run with Docker

```bash
docker build -t proteinvault .
docker run -p 8000:8000 proteinvault
```

### Run the integration demo

The demo downloads a ProteinGym dataset, loads it through the API, registers ESM-2, scores all variants, and computes Spearman correlation against measured fitness.

```bash
# Start the server first, then in another terminal:
uv run python -m proteinvault.cli.demo --model-size 8M

# For higher accuracy (slower, ~5K sequences):
uv run python -m proteinvault.cli.demo --model-size 650M
```

### Demo Results

Tested on BLAT_ECOLX_Stiffler_2015 (4,996 single-substitution variants of TEM-1 beta-lactamase):

| Model | Spearman rho | p-value | Inference time |
|-------|-------------|---------|----------------|
| ESM-2 8M | 0.3748 | 2.03e-166 | 6.2s |
| ESM-2 650M | **0.7247** | ~0.0 | 35.9s |

Both results fall within the expected range for ESM-2 zero-shot wildtype marginal scoring on ProteinGym substitution benchmarks.

### Run tests

```bash
# Fast tests (no ML inference)
uv run pytest -m "not slow"

# All tests including ML inference (~30s)
uv run pytest

# With coverage
uv run pytest --cov=proteinvault --cov-report=term-missing
```

## API Reference

### Data Layer

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/datasets` | Create a dataset |
| GET | `/api/v1/datasets` | List datasets |
| GET | `/api/v1/datasets/{id}` | Get dataset with current version |
| POST | `/api/v1/datasets/{id}/loads` | Upload data (JSON) |
| POST | `/api/v1/datasets/{id}/loads/csv` | Upload CSV file |
| GET | `/api/v1/datasets/{id}/versions` | List all versions |
| GET | `/api/v1/datasets/{id}/sequences?version=N` | Query sequences at version |
| POST | `/api/v1/datasets/{id}/undo` | Undo last load |
| POST | `/api/v1/datasets/{id}/redo` | Redo undone load |
| GET | `/api/v1/datasets/{id}/query?assay=X` | Query assay measurements |

### Model Gateway

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/models` | Register a model |
| GET | `/api/v1/models` | List models |
| GET | `/api/v1/models/{id}` | Get model details |
| POST | `/api/v1/models/{id}/predict` | Submit prediction (sync/async) |
| GET | `/api/v1/tasks/{id}` | Check async task status |
| GET | `/api/v1/predictions/{id}` | Get prediction with provenance |
| GET | `/api/v1/predictions/{id}/scores` | Get individual scores |

## Project Structure

```
src/proteinvault/
  domain/           # Core domain: models, enums, validation, exceptions
  db/               # DuckDB schema and repository layer
  services/         # Business logic + ESM-2 scorer
  api/              # FastAPI routes and Pydantic schemas
  cli/              # Integration demo script
```

## Tech Stack

- **Python 3.12**, **FastAPI**, **Pydantic**
- **DuckDB** -- embedded analytical database
- **HuggingFace Transformers** -- ESM-2 protein language model
- **uv** -- dependency management
- **ruff** -- linting, **pyright** -- type checking
- **pytest** -- testing (83 tests, 83% coverage)
