# AGENTS.md

## Repository Overview

`pfun-data` is a Python data-utilities package for **PFun Digital Health**. It provides tools for ingesting, converting, and querying health datasets (ECG, continuous glucose monitoring, etc.).

> **Note:** This repository is under very active development.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.12 | Language (`requires-python = "==3.12.*"`) |
| [uv](https://github.com/astral-sh/uv) | Package / environment manager (`uv.lock` present) |
| [hatchling](https://hatch.pypa.io/) | Build backend |
| DuckDB | In-process analytical SQL |
| PostgreSQL / psycopg2 | Remote relational database |
| pandas / pyarrow | DataFrame and Parquet I/O |
| pydantic-settings | Environment-based configuration |

---

## Environment Setup

```bash
# Clone (including submodule)
git clone --recurse-submodules https://github.com/pfun-health/pfun-data.git
cd pfun-data

# Install all dependencies (including dev extras) with uv
uv sync --all-extras
```

### Required environment variables

The package reads database credentials from the environment (or a `.env` file in the working directory):

| Variable | Description |
|----------|-------------|
| `POSTGRES_HOST` | Hostname of the PostgreSQL server |
| `POSTGRES_PORT` | Port of the PostgreSQL server |
| `POSTGRES_PASSWORD` | Password for the `postgres` user |

Create a `.env` file (never commit it):

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_PASSWORD=secret
```

---

## Project Layout

```
pfun_data/
  __init__.py          # Package root
  settings.py          # PostgresDBConfig (pydantic-settings)
  connect.py           # psycopg2 + DuckDB connection helpers
  tools.py             # ZIP → CSV → Parquet pipeline + CLI entry-point
  data/
    __init__.py
    valid_data.csv

scripts/
  download-d1namo.sh   # Download the D1NAMO ECG+CGM dataset from Kaggle
  unzipcsv2parquet.sh  # Shell wrapper around the CLI tool

sql/
  get_glucose_viz.sql  # Example SQL query for glucose visualisation

notebooks/
  load-data-from-csv.ipynb
  viz-data-from-postgres.ipynb

dependencies/
  zip-to-parquet/      # Git submodule (https://github.com/pfun-health/zip-to-parquet)

_raw_data/             # Local data directory (not committed)
```

---

## CLI Tools

### `pfun-data-unzipcsv2parquet`

Extracts CSV files from a ZIP archive and converts them to Parquet format using a parallel process pool.

```bash
pfun-data-unzipcsv2parquet \
    --zip-path    /path/to/archive.zip \
    --csv-path    /path/to/csv_output_dir \
    --parquet-path /path/to/parquet_output_dir

# Skip re-processing files that already exist (default: enabled)
# Use --no-skip-existing to force overwrite
```

A convenience wrapper is provided in `scripts/unzipcsv2parquet.sh` pre-configured for the D1NAMO dataset paths.

---

## Key Python API

```python
from pfun_data.tools import (
    extractFromZip,       # Extract all files from a ZIP archive
    convertCsvToParquet,  # Batch-convert CSV files to Parquet
    Csv2ParquetPipeline,  # Callable pipeline combining both steps
    unzipCsv2Parquet,     # Convenience wrapper around the pipeline
)

from pfun_data.settings import pg_config  # PostgresDBConfig instance
# pg_config.pg_conn_str  →  "postgresql://postgres:<pw>@<host>:<port>/pfun"

import pfun_data.connect  # psycopg2 + duckdb connection helpers
```

---

## Data Acquisition

Download the D1NAMO ECG + continuous glucose monitoring dataset from Kaggle:

```bash
bash scripts/download-d1namo.sh   # uses aria2c; requires Kaggle credentials in the URL or ~/.kaggle/kaggle.json
```

The downloaded ZIP is saved to `_raw_data/d1namo-ecg-glucose-data.zip`.

---

## Tests

There is currently no test suite. When adding tests, place them in a top-level `tests/` directory and run them with:

```bash
uv run pytest
```

---

## Common Tasks

| Task | Command |
|------|---------|
| Install dependencies | `uv sync --all-extras` |
| Run the CLI tool | `uv run pfun-data-unzipcsv2parquet --help` |
| Launch Jupyter | `uv run jupyter lab` |
| Build the package | `uv build` |
