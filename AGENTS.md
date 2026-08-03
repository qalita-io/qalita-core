# AGENTS.md — Qalita Core

Instructions for AI agents working on this repository.

## Project

**Qalita Core** — Python library used by Qalita packs to load multi-source data, materialize as Parquet, and share common utilities.

- **Organization** : `qalita`
- **Package** : `qalita_core` (PyPI)
- **Python** : >= 3.10
- **Package Manager** : uv

## Tech Stack

| Component | Technologies |
|-----------|-------------|
| **Data processing** | Polars (only supported engine), pandas (optional extra, legacy packs) |
| **Formats** | pyarrow, openpyxl (Excel) |
| **Databases** | SQLAlchemy 2, psycopg2, pymysql, pymongo, oracledb, pymssql |
| **Data warehouses** | Snowflake, BigQuery, Databricks, Redshift, ClickHouse, DuckDB, Trino |
| **Enterprise DB** | Teradata, SAP HANA, Cassandra, Elasticsearch, IBM DB2, Athena |
| **Object storage** | boto3 (S3), google-cloud-storage, azure-storage-blob, hdfs, paramiko (SFTP) |
| **Build** | hatchling |
| **Linting** | Black, Pylint, Flake8 |

## Dependencies

See `pyproject.toml` for full dependency list. Key dependencies:
- `polars>=1.30`, `pyarrow>=19.0` — 1.30 is a hard floor, not a courtesy: below
  1.25 `collect(engine="streaming")` is silently ignored and 1.27-1.29 panic in
  the streaming parquet reader
- `sqlalchemy>=2.0`, `psycopg2-binary`, `pymongo>=4.0`
- `boto3`, `google-cloud-storage`, `azure-storage-blob`

**pandas is an optional extra** (`qalita_core[pandas]`), not a dependency.
Importing `qalita_core` must never import pandas — there is a test for it
(`tests/test_aggregation.py::TestPandasIsOptional`). Only reach for pandas when
a pack wraps a pandas-only third party; then import it inside the function that
needs it, never at module level.

## Computing over data — the only sanctioned way

```python
from qalita_core import analytics
from qalita_core.profiling import profile

lf = pack.scan("source")                       # LazyFrame, never a path
schema = pack.schema("source")                 # from Parquet footers, no read

stats = analytics.agg(lf, {                    # ONE pass, every metric at once
    "rows": pl.len(),
    **{f"{c}__nulls": pl.col(c).null_count()
       for c in analytics.numeric_columns(schema)},
})
n_bad, examples = analytics.failures(lf, pl.col("age") < 0, limit=10)
```

Rules that are enforced, not suggested:

1. No bare `.collect()`. Every collect goes through `analytics`, which uses
   `engine="streaming"` and RAISES (`StreamingCollectError`). Never wrap it in a
   fallback to the in-memory engine: on a large source that is an OOM kill
   dressed as resilience.
2. No `pd.read_parquet` / `pl.read_parquet` / `.to_pandas()` on source data.
3. No `for column in columns:` issuing one query per column. Batch every
   expression into a single `analytics.agg()` call — on 100 GiB, a per-column
   loop re-reads the source once per column.
4. Anything returning ROWS must be bounded: `analytics.failures`,
   `analytics.sample`, `analytics.top_k`, `analytics.value_counts`.
5. Approximate by default (`approx_n_unique`, histogram `quantiles`). Any metric
   derived from an approximate statistic emits a sibling metric
   `<key>_method` naming the method (`hyperloglog` / `histogram` / `exact`).

`qalita_core.aggregation` holds the cross-pack accumulators and follows the same
rules: `CompletenessAggregator.add_lf`, `DuplicateAggregator.add_lf` (Polars owns
the group state; `get_duplicate_keys` is top-K bounded),
`TimelinessAggregator.add_lf`, and `streaming_outliers()` (two-pass global
IQR/z-score) feeding `OutlierAggregator.add_streaming_outliers`.

## Build/Lint/Test Commands

```bash
# Install dependencies
uv sync --extra dev

# Run tests
uv run pytest tests/ -v

# Linting
uv run black qalita_core/ tests/ --check
uv run pylint qalita_core/

# Format code
uv run black qalita_core/ tests/

# Type checking (if mypy configured)
uv run mypy qalita_core/
```

## Code Conventions

- **Formatter** : Black (line length 79)
- **Linting** : Pylint, Flake8
- **Tests** : pytest (tests in `tests/`)
- **Data** : Polars only for new code; pandas is an optional extra kept for
  packs that have not been ported
- **Imports** : Absolute imports from `qalita_core`
- **Types** : Type hints recommended for public APIs
- **Build** : hatchling, publish with `uv build && uv publish`

## Architecture

```
qalita-core/
├── qalita_core/       # Main Python package
│   ├── datasource/    # DataSource abstraction + factory
│   ├── loaders/       # Loaders by type (file, db, object storage)
│   ├── aggregators/   # Shared aggregators (completeness, outliers, duplicates, timeliness)
│   └── utils/         # Utilities (sanitization, Parquet helpers)
├── parquet/           # Test Parquet data
├── tests/             # pytest tests
└── pyproject.toml     # Dependencies (uv/hatchling)
```

## Git Workflow

- **Tags** : Strict semver `X.Y.Z` (⚠️ NO `v` prefix)
- **Commits** : English, conventional commits (`feat:`, `fix:`, `chore:`)
- **Branches** : `main` (prod), feature branches for development

### Release order: core before packs

`version` in `pyproject.toml` is literally `0.0.0-dev`; a real version only
exists once a human pushes the tag, and `ci.yml` publishes to PyPI from the tag.
Every pack in `qalita/packs` now declares `qalita-core>=2.0.0` because it uses
the streaming API (`analytics`, `Pack.scan_data`), and `scripts/run.sh` runs
`uv lock` on the worker for every job. So the order is not optional:

1. Merge core to `main`.
2. Push tag `2.0.0` on `main`, wait for `ci.yml` to publish it to PyPI.
3. Only then merge the matching work into `packs/main`.

Merging packs first publishes packs that no worker can install — `uv lock`
fails with `No solution found ... qalita-core>=2.0.0` and every analysis job on
every source fails until core is tagged. The `resolve` job in
`packs/.github/workflows/publish.yml` blocks that merge, so a packs PR staying
red on `resolve` usually means step 2 has not happened yet.

## Security Rules

- ❌ NEVER commit secrets or database credentials
- ✅ Use DataSource abstraction for all new data access
- ✅ Test with Parquet files in `parquet/` directory
