# AGENTS.md — Qalita Core

Ce fichier fournit des instructions aux agents IA pour travailler sur ce dépôt.

## Projet

**Qalita Core** — Bibliothèque Python utilisée par les packs Qalita pour charger des données multi-sources, les matérialiser en Parquet et partager des utilitaires communs.

- **Organisation GitHub** : `qalita-io`
- **Package PyPI** : `qalita_core`
- **Python** : >= 3.10

## Architecture

```
qalita-core/
├── qalita_core/       # Package Python principal
│   ├── datasource/    # Abstraction DataSource + factory
│   ├── loaders/       # Loaders par type (file, db, object storage)
│   ├── aggregators/   # Aggregateurs partagés (completeness, outliers, duplicates, timeliness)
│   └── utils/         # Utilitaires (sanitization, Parquet helpers)
├── parquet/           # Données de test Parquet
├── tests/             # Tests pytest
└── pyproject.toml     # Dépendances (uv/hatchling)
```

## Stack technique

| Composant | Technologies |
|-----------|-------------|
| **Data processing** | Polars (principal, big data 100GB+), pandas (compatibilité legacy) |
| **Formats** | pyarrow, openpyxl (Excel) |
| **Databases** | SQLAlchemy 2, psycopg2, pymysql, pymongo, oracledb, pymssql |
| **Data warehouses** | Snowflake, BigQuery, Databricks, Redshift, ClickHouse, DuckDB, Trino |
| **Enterprise DB** | Teradata, SAP HANA, Cassandra, Elasticsearch, IBM DB2, Athena |
| **Object storage** | boto3 (S3), google-cloud-storage, azure-storage-blob, hdfs, paramiko (SFTP) |

## Commandes de développement

```bash
# Installer les dépendances
uv sync

# Installer avec extras dev
uv sync --extra dev

# Lancer les tests
uv run pytest

# Linting
uv run black qalita_core/ tests/
uv run pylint qalita_core/
```

## Conventions de code

- **Formatter** : Black
- **Linting** : Pylint, Flake8
- **Tests** : pytest
- **Data** : Préférer Polars pour le nouveau code, pandas pour la compatibilité
- **Build** : hatchling
- **Package manager** : uv

## Git workflow

- **Tags** : Semver strict `X.Y.Z` (⚠️ PAS de préfixe `v`)
- **Commits** : Messages en anglais, conventionnels (`feat:`, `fix:`, `chore:`)

## Règles de sécurité

- ❌ Ne JAMAIS commiter de secrets ou credentials de base de données
- ✅ Utiliser l'abstraction DataSource pour tout nouvel accès de données
- ✅ Tester avec les fichiers Parquet du dossier `parquet/`
