# QALITA Core

<p align="center">
  <img width="250px" height="auto" src="https://cloud.platform.qalita.io/logo.svg" style="max-width:250px;"/>
</p>

QALITA Core is a lightweight helper library used by QALITA packs to load data from multiple sources, materialize them to Parquet in deterministic chunks, and share common utilities (sanitization and aggregation helpers).

## Key features

- Unified data access via a simple `DataSource` abstraction and factory
- File, database, and object storage loaders with streaming to Parquet
- Deterministic, size-bounded Parquet chunking with stable filenames
- Safe Parquet writing for pandas DataFrames (automatic sanitization)
- Shared aggregators for completeness, outliers, duplicates, and timeliness
- Minimal pack runtime with JSON config loading and simple asset persistence

## Supported sources

- Files: CSV (`.csv`), Excel (`.xlsx`), JSON, Parquet (pass-through)
- Databases: PostgreSQL, MySQL, Oracle, MS SQL Server, SQLite
- Object storage: Amazon S3, Google Cloud Storage, Azure Blob (via `abfs`), HDFS

Notes:
- Folder, MongoDB classes exist as placeholders; MongoDB is not yet implemented.
- SQLite is supported through the generic `DatabaseSource` when selected via `type: "sqlite"`.

## Installation

Prerequisites: Python 3.10–3.12 and uv.

Install dependencies and set your environment:

```bash
pip install uv
uv sync
```

Open a uv shell when developing:

```bash
uv shell
```

## Quickstart

### Use within a Pack

`Pack` loads four JSON files by default (overridable) and provides `load_data()` for `source` or `target` triggers.

```python
from qalita_core.pack import Pack

pack = Pack(configs={
    "pack_conf": "./pack_conf.json",
    "source_conf": "./source_conf.json",
    "target_conf": "./target_conf.json",
    "agent_file": "~/.qalita/.agent",
})

# Ensure chunking/output are set (can be in pack_conf["job"] too)
pack.pack_config.setdefault("job", {})
pack.pack_config["job"]["parquet_output_dir"] = "./parquet"
pack.pack_config["job"]["chunk_rows"] = 100_000

# Load source
source_paths = pack.load_data("source")
# Load target (optional)
target_paths = pack.load_data("target")

# Persist custom metrics/recommendations/schemas to JSON files
pack.metrics.data.append({"key": "score", "value": "0.95", "scope": {"perimeter": "dataset", "value": "my_dataset"}})
pack.metrics.save()       # writes metrics.json
pack.recommendations.save()  # writes recommendations.json
pack.schemas.save()          # writes schemas.json
```

## Parquet chunking and filenames

- CSV/JSON/Excel are streamed with `chunksize` into multiple parquet files.
- Databases are read with chunked SQL via SQLAlchemy/`pandas.read_sql`.
- Filenames use a stable pattern: `<source>_<object>_part_<k>.parquet` where:
  - `<source>` is a slug of the source type (e.g. `file`, `sqlite`, `postgresql`).
  - `<object>` is a slug of the table name, query label, or file stem.
  - Example: `file_testdata_part_1.parquet`, `sqlite_items_part_3.parquet`, `sqlite_query_part_2.parquet`.

Configure output and size via `pack_config`:

- `parquet_output_dir` (default: `./parquet`)
- `chunk_rows` (default: `100000`)
- Optional `job.source.skiprows` applied to CSV/Excel

## Safe Parquet writing for pandas

On import, QALITA Core installs a small monkeypatch so `DataFrame.to_parquet`:

- Ensures column names are strings
- Decodes bytes to UTF‑8 strings when present
- Normalizes mixed-type object columns and categoricals
- Defaults to `engine="pyarrow"`

You can also call the sanitizer explicitly:

```python
from qalita_core import sanitize_dataframe_for_parquet
clean_df = sanitize_dataframe_for_parquet(df)
```

## Aggregation helpers (for packs)

Helpers centralize common result/metric aggregation logic:

```python
from qalita_core import (
    detect_chunked_from_items,
    normalize_and_dedupe_recommendations,
    CompletenessAggregator,
    OutlierAggregator,
    DuplicateAggregator,
    TimelinessAggregator,
)
```

- `CompletenessAggregator`: column/dataset completeness and schema extraction
- `OutlierAggregator`: per-column and dataset outlier/normality metrics
- `DuplicateAggregator`: duplicate counts and dataset-level score using key columns
- `TimelinessAggregator`: dates/years coverage and recency scoring

## Development

- Tests: `uv run pytest`
- Formatting: `uv run black .`
- Linting: `uv run flake8` and `uv run pylint <module>`
- Editable install while debugging:

```bash
uv sync
uv pip install -e .
```

## Documentation

Additional material can be found in the online documentation: `https://doc.qalita.io/`.
