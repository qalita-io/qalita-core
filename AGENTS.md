# AGENTS.md — Qalita Core

Instructions for AI agents working on this repository.

## Project

**Qalita Core** — Python library used by Qalita packs to load multi-source data, materialize as Parquet, and share common utilities.

- **Organization** : `qalita-io`
- **Package** : `qalita_core` (PyPI)
- **Python** : >= 3.10
- **Package Manager** : uv

## Tech Stack

| Component | Technologies |
|-----------|-------------|
| **Data processing** | Polars (primary, 100GB+ datasets), pandas (legacy compat) |
| **Formats** | pyarrow, openpyxl (Excel) |
| **Databases** | SQLAlchemy 2, psycopg2, pymysql, pymongo, oracledb, pymssql |
| **Data warehouses** | Snowflake, BigQuery, Databricks, Redshift, ClickHouse, DuckDB, Trino |
| **Enterprise DB** | Teradata, SAP HANA, Cassandra, Elasticsearch, IBM DB2, Athena |
| **Object storage** | boto3 (S3), google-cloud-storage, azure-storage-blob, hdfs, paramiko (SFTP) |
| **Build** | hatchling |
| **Linting** | Black, Pylint, Flake8 |

## Dependencies

See `pyproject.toml` for full dependency list. Key dependencies:
- `polars>=1.0`, `pandas>=2.0`, `pyarrow>=14.0`
- `sqlalchemy>=2.0`, `psycopg2-binary`, `pymongo>=4.0`
- `boto3`, `google-cloud-storage`, `azure-storage-blob`

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

- **Formatter** : Black
- **Linting** : Pylint, Flake8
- **Tests** : pytest (tests in `tests/`)
- **Data** : Prefer Polars for new code, pandas for compatibility
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

## Security Rules

- ❌ NEVER commit secrets or database credentials
- ✅ Use DataSource abstraction for all new data access
- ✅ Test with Parquet files in `parquet/` directory
