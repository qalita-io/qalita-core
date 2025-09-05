"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import os
import glob
import pandas as pd
from typing import Optional, List, Iterable
from sqlalchemy import create_engine, inspect, text
from abc import ABC, abstractmethod
from pathlib import Path
from qalita_core.utils import slugify

DEFAULT_PORTS = {
    "5432": "postgresql",
    "3306": "mysql",
    "1433": "mssql+pymssql",
    "1521": "oracle",
    "27017": "mongodb",
    "6379": "redis",
    "50000": "db2",
    "1434": "sybase",
    "3307": "mariadb",
    "5433": "greenplum",
    "5000": "sqlite",
}

class DataSource(ABC):
    @abstractmethod
    def get_data(self, table_or_query=None, pack_config=None):
        """Return a list of parquet file paths for the requested data."""
        pass


# -----------------------------
# Helper utilities for Parquet
# -----------------------------

def _ensure_output_dir(pack_config: Optional[dict]) -> str:
    base_dir = (pack_config or {}).get("parquet_output_dir") or "./parquet"
    Path(base_dir).mkdir(parents=True, exist_ok=True)
    return str(base_dir)


def _build_base_name(source_type: str, object_identifier: str) -> str:
    normalized_source = slugify(source_type or "source")
    normalized_object = slugify(object_identifier or "data")
    return f"{normalized_source}_{normalized_object}"


def _build_parquet_path(output_dir: str, base_name: str, part_index: int) -> str:
    return os.path.join(output_dir, f"{base_name}_part_{part_index}.parquet")


def _write_df_to_parquet(df: pd.DataFrame, output_path: str) -> str:
    # Always write with pyarrow for Arrow/Polars/DuckDB compatibility
    df.to_parquet(output_path, engine="pyarrow", index=False)
    return output_path


def _write_pandas_chunks(
    df_iter: Iterable[pd.DataFrame], output_dir: str, base_name: str, start_part: int = 1
) -> List[str]:
    paths: List[str] = []
    part = start_part
    for chunk_df in df_iter:
        path = _build_parquet_path(output_dir, base_name, part)
        _write_df_to_parquet(chunk_df, path)
        paths.append(path)
        part += 1
    return paths

class FileSource(DataSource):
    def __init__(self, file_path):
        self.file_path = file_path

    def get_data(self, table_or_query=None, pack_config=None):
        output_dir = _ensure_output_dir(pack_config)
        if os.path.isfile(self.file_path):
            return self._load_file(self.file_path, pack_config, output_dir)
        if os.path.isdir(self.file_path):
            data_files = glob.glob(os.path.join(self.file_path, "*.csv")) + glob.glob(
                os.path.join(self.file_path, "*.xlsx")
            )
            if not data_files:
                raise FileNotFoundError(
                    "No CSV or XLSX files found in the provided path."
                )
            return self._load_file(data_files[0], pack_config, output_dir)
        raise FileNotFoundError(
            f"The path {self.file_path} is neither a file nor a directory, or it can't be reached."
        )

    @staticmethod
    def _load_file(file_path, pack_config, output_dir: str) -> List[str]:
        skiprows = 0
        chunk_rows = (pack_config or {}).get("chunk_rows", 100000)
        if pack_config:
            skiprows = pack_config.get("job", {}).get("source", {}).get("skiprows", 0)

        base_name = _build_base_name("file", os.path.splitext(os.path.basename(file_path))[0])
        # CSV: stream with chunksize
        if file_path.endswith(".csv"):
            df_iter = pd.read_csv(
                file_path,
                low_memory=False,
                memory_map=True,
                skiprows=int(skiprows),
                on_bad_lines="warn",
                encoding="utf-8",
                chunksize=int(chunk_rows),
            )
            return _write_pandas_chunks(df_iter, output_dir, base_name)

        # XLSX: stream via openpyxl read_only
        if file_path.endswith(".xlsx"):
            try:
                from openpyxl import load_workbook
            except Exception:
                # Fallback: load entire file (may be memory heavy)
                df = pd.read_excel(file_path, engine="openpyxl", skiprows=int(skiprows))
                path = _build_parquet_path(output_dir, base_name, 1)
                return [_write_df_to_parquet(df, path)]

            wb = load_workbook(filename=file_path, read_only=True, data_only=True)
            ws = wb.active
            if ws is None:
                # Fallback: load entire file
                df = pd.read_excel(file_path, engine="openpyxl", skiprows=int(skiprows))
                path = _build_parquet_path(output_dir, base_name, 1)
                return [_write_df_to_parquet(df, path)]

            rows_iter = ws.iter_rows(values_only=True)
            # Apply skiprows on header discovery
            for _ in range(int(skiprows)):
                try:
                    next(rows_iter)
                except StopIteration:
                    break
            try:
                headers = list(next(rows_iter))
            except StopIteration:
                headers = []

            batch: List[list] = []
            part = 1
            paths: List[str] = []
            for row in rows_iter:
                batch.append(list(row))
                if len(batch) >= int(chunk_rows):
                    df = pd.DataFrame(batch, columns=headers)
                    path = _build_parquet_path(output_dir, base_name, part)
                    _write_df_to_parquet(df, path)
                    paths.append(path)
                    batch = []
                    part += 1
            if batch:
                df = pd.DataFrame(batch, columns=headers)
                path = _build_parquet_path(output_dir, base_name, part)
                _write_df_to_parquet(df, path)
                paths.append(path)
            return paths

        raise ValueError(
            f"Unsupported file extension or missing 'skiprows' for file: {file_path}"
        )

class DatabaseSource(DataSource):
    def __init__(self, connection_string=None, config=None):
        # Keep the config available for schema preference and other options
        self.config = config or {}

        if connection_string:
            self.engine = create_engine(connection_string)
        elif config:
            db_type = config.get("type") or DEFAULT_PORTS.get(str(config.get("port")), "unknown")
            if db_type == "unknown":
                raise ValueError(f"Unsupported or unknown database port: {config.get('port')}")
            elif db_type == "oracle":
                db_type = "oracle+oracledb"
                conn_str = (
                    f"{db_type}://{config['username']}:{config['password']}"
                    f"@{config['host']}:{config['port']}/?service_name={config['database']}"
                )
                self.engine = create_engine(conn_str)
            elif db_type.startswith("sqlite"):
                database_path = config.get("database") or ":memory:"
                if database_path == ":memory:":
                    conn_str = "sqlite:///:memory:"
                else:
                    # Accept absolute or relative filesystem path
                    conn_str = f"sqlite:///{database_path}"
                self.engine = create_engine(conn_str)
            else:
                self.engine = create_engine(
                    f"{db_type}://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
                )
        else:
            raise ValueError("DatabaseSource requires a connection_string or a config dict.")

    def get_data(self, table_or_query=None, pack_config=None):
        """
        Load data from the database and write Parquet chunks.

        - If table_or_query is a string table name: returns list of parquet paths for that table
        - If table_or_query is a SQL query string: returns list of parquet paths for the query result
        - If table_or_query is a list/tuple/set of table names: returns parquet paths for each table
        - If table_or_query is '*' or None: scan all tables and return parquet paths for each
        """

        # Determine schema: prefer source config over pack config; both are optional
        schema = None
        cfg_schema = None
        try:
            cfg_schema = (self.config or {}).get("schema")
        except Exception:
            cfg_schema = None
        if cfg_schema:
            schema = cfg_schema
        elif pack_config:
            schema = (
                pack_config.get("job", {})
                .get("source", {})
                .get("schema")
            )

        output_dir = _ensure_output_dir(pack_config)
        chunk_rows = int((pack_config or {}).get("chunk_rows", 100000))
        dialect_name = None
        try:
            dialect_name = self.engine.dialect.name
        except Exception:
            dialect_name = None

        # Default behavior: scan all tables
        if table_or_query is None or (isinstance(table_or_query, str) and table_or_query.strip() == "*"):
            table_names = self._get_all_table_names(schema)
            if not table_names:
                raise ValueError("No tables found in the database for the given schema.")
            all_paths: List[str] = []
            for table_name in table_names:
                all_paths.extend(self._read_table_to_parquet(table_name, schema, output_dir, chunk_rows, dialect_name))
            return all_paths

        # If a list/tuple/set of table names is provided
        if isinstance(table_or_query, (list, tuple, set)):
            table_names = list(table_or_query)
            all_paths: List[str] = []
            for table_name in table_names:
                all_paths.extend(self._read_table_to_parquet(table_name, schema, output_dir, chunk_rows, dialect_name))
            return all_paths

        # If a single string is provided, determine if it's a table name or SQL query
        if isinstance(table_or_query, str):
            if self._is_sql_query(table_or_query):
                base_name = _build_base_name(dialect_name or "db", "query")
                df_iter = pd.read_sql(table_or_query, self.engine, chunksize=chunk_rows)
                return _write_pandas_chunks(df_iter, output_dir, base_name)
            return self._read_table_to_parquet(table_or_query, schema, output_dir, chunk_rows, dialect_name)

        raise TypeError(
            "table_or_query must be None, '*', a string (table name or SQL), or a list/tuple/set of table names."
        )

    def _read_table(self, table_name: str, schema: Optional[str] = None) -> pd.DataFrame:
        """Read a full table as a DataFrame using dialect-aware SQL. (Compatibility only)"""
        # Support fully-qualified names like "SCHEMA.TABLE" when no schema is explicitly provided
        effective_schema = schema
        effective_table = table_name
        if not schema and "." in table_name:
            try:
                effective_schema, effective_table = table_name.split(".", 1)
            except ValueError:
                effective_schema = None
                effective_table = table_name

        try:
            return pd.read_sql_table(effective_table, self.engine, schema=effective_schema)
        except Exception:
            # Fallback to a simple SELECT * if read_sql_table is unsupported for the dialect
            qualified = f"{effective_schema}.{effective_table}" if effective_schema else effective_table
            return pd.read_sql(f"SELECT * FROM {qualified}", self.engine)

    def _read_table_to_parquet(
        self,
        table_name: str,
        schema: Optional[str],
        output_dir: str,
        chunk_rows: int,
        dialect_name: Optional[str],
    ) -> List[str]:
        effective_schema = schema
        effective_table = table_name
        if not schema and "." in table_name:
            try:
                effective_schema, effective_table = table_name.split(".", 1)
            except ValueError:
                effective_schema = None
                effective_table = table_name

        qualified = f"{effective_schema}.{effective_table}" if effective_schema else effective_table
        base_name = _build_base_name(dialect_name or "db", qualified)
        # Use streaming SQL with chunksize
        sql = f"SELECT * FROM {qualified}"
        df_iter = pd.read_sql(sql, self.engine, chunksize=int(chunk_rows))
        return _write_pandas_chunks(df_iter, output_dir, base_name)

    def _get_all_table_names(self, schema: Optional[str] = None) -> List[str]:
        """Return all table names (and views) in the database for the given schema, sorted alphabetically.
        For Oracle and PostgreSQL, if no schema is provided and none are found in the default schema,
        iterate over accessible schemas and return fully-qualified names ("SCHEMA.TABLE").
        """
        inspector = inspect(self.engine)

        def _collect_for_schema(target_schema: Optional[str]) -> List[str]:
            try:
                tables = inspector.get_table_names(schema=target_schema)
            except Exception:
                tables = []
            try:
                views = inspector.get_view_names(schema=target_schema)
            except Exception:
                views = []
            return list(set((tables or []) + (views or [])))

        # First pass: use the provided schema (or None)
        initial = sorted(_collect_for_schema(schema))
        if initial:
            return initial

        # Special handling for Oracle when no schema specified and nothing found
        try:
            dialect_name = self.engine.dialect.name
        except Exception:
            dialect_name = None

        if dialect_name == "oracle" and schema is None:
            try:
                schemas = inspector.get_schema_names()
            except Exception:
                schemas = []

            system_schemas = {
                "SYS","SYSTEM","OUTLN","XDB","MDSYS","CTXSYS","ORDSYS","ORDDATA","DBSNMP",
                "APPQOSSYS","WMSYS","OLAPSYS","LBACSYS","GSMADMIN_INTERNAL","OJVMSYS",
                "DVF","DVSYS","REMOTE_SCHEDULER_AGENT","SYS$UMF","GGSYS","AUDSYS","ANONYMOUS"
            }

            qualified: List[str] = []
            for sch in schemas or []:
                if not sch or sch.upper() in system_schemas:
                    continue
                names = _collect_for_schema(sch)
                for n in names:
                    qualified.append(f"{sch}.{n}")

            qualified = sorted(set(qualified))
            if qualified:
                return qualified

            # Final fallback: try CURRENT_SCHEMA if available
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text("SELECT sys_context('USERENV','CURRENT_SCHEMA') FROM dual"))
                    row = result.fetchone()
                    current_schema = row[0] if row and row[0] else None
                if current_schema:
                    names = _collect_for_schema(current_schema)
                    if names:
                        return sorted([f"{current_schema}.{n}" for n in names])
            except Exception:
                pass

        # Special handling for PostgreSQL when no schema specified and nothing found
        if dialect_name == "postgresql" and schema is None:
            try:
                schemas = inspector.get_schema_names()
            except Exception:
                schemas = []

            # Filter out system schemas
            system_prefixes = ("pg_temp_", "pg_toast_temp_")
            system_schemas = {
                "information_schema",
                "pg_catalog",
                "pg_toast",
            }

            qualified: List[str] = []
            for sch in schemas or []:
                if not sch:
                    continue
                if sch in system_schemas or any(sch.startswith(pfx) for pfx in system_prefixes):
                    continue
                names = _collect_for_schema(sch)
                for n in names:
                    qualified.append(f"{sch}.{n}")

            qualified = sorted(set(qualified))
            if qualified:
                return qualified

        # Return whatever was found initially (likely empty)
        return initial

    def _is_sql_query(self, s: str) -> bool:
        """Heuristic to detect if a string is a SQL query rather than a bare table name."""
        sql = s.strip().lower()
        if ";" in sql or "\n" in sql:
            return True
        starters = ("select", "with", "show", "describe", "pragma", "explain")
        return any(sql.startswith(token) for token in starters)

def _infer_format_from_path(path: str, explicit_format: Optional[str] = None) -> str:
    if explicit_format:
        return explicit_format.lower()
    lower = path.lower()
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith(".json"):
        return "json"
    if lower.endswith(".parquet") or lower.endswith(".pq"):
        return "parquet"
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return "excel"
    return "csv"


def _materialize_remote_to_parquet(
    path: str,
    fmt: str,
    storage_options: Optional[dict],
    pack_config: Optional[dict],
) -> List[str]:
    # If already parquet, pass-through by returning the remote path
    if fmt == "parquet":
        return [path]

    output_dir = _ensure_output_dir(pack_config)
    chunk_rows = int((pack_config or {}).get("chunk_rows", 100000))
    skiprows = 0
    if pack_config:
        skiprows = pack_config.get("job", {}).get("source", {}).get("skiprows", 0)

    base_name = _build_base_name("remote", os.path.splitext(os.path.basename(path))[0])

    if fmt == "csv":
        df_iter = pd.read_csv(
            path,
            storage_options=storage_options,
            low_memory=False,
            memory_map=True,
            skiprows=int(skiprows),
            on_bad_lines="warn",
            encoding="utf-8",
            chunksize=chunk_rows,
        )
        return _write_pandas_chunks(df_iter, output_dir, base_name)
    if fmt == "json":
        # Attempt newline-delimited JSON if hinted
        lines = bool((pack_config or {}).get("json_lines", False))
        if lines:
            df_iter = pd.read_json(path, storage_options=storage_options, lines=True, chunksize=chunk_rows)
            return _write_pandas_chunks(df_iter, output_dir, base_name)
        # Fallback: load once, write once (may be memory heavy)
        df = pd.read_json(path, storage_options=storage_options)
        return [_write_df_to_parquet(df, _build_parquet_path(output_dir, base_name, 1))]
    if fmt == "excel":
        # Excel streaming from remote is complex; fallback to single load
        df = pd.read_excel(path, storage_options=storage_options, engine="openpyxl", skiprows=int(skiprows))
        return [_write_df_to_parquet(df, _build_parquet_path(output_dir, base_name, 1))]
    # Fallback to CSV behavior
    df_iter = pd.read_csv(path, storage_options=storage_options, chunksize=chunk_rows)
    return _write_pandas_chunks(df_iter, output_dir, base_name)


class S3Source(DataSource):
    def __init__(self, config):
        self.config = config or {}

    def get_data(self, table_or_query=None, pack_config=None):
        # Expect either a full s3 path in config['path'] or bucket/key
        path = self.config.get("path")
        if not path:
            bucket = self.config.get("bucket")
            key = self.config.get("key")
            if bucket and key:
                path = f"s3://{bucket}/{key}"
        if not path:
            raise ValueError("S3Source requires either 'path' or 'bucket'+'key' in config.")

        storage_options = {}
        for opt_key in [
            "key",  # aws_access_key_id
            "secret",  # aws_secret_access_key
            "token",  # aws_session_token
            "client_kwargs",  # e.g., {"region_name": "us-east-1"}
        ]:
            if opt_key in self.config:
                storage_options[opt_key] = self.config[opt_key]

        fmt = _infer_format_from_path(path, self.config.get("format"))
        return _materialize_remote_to_parquet(path, fmt, storage_options or None, pack_config)

class GCSSource(DataSource):
    def __init__(self, config):
        self.config = config or {}

    def get_data(self, table_or_query=None, pack_config=None):
        # Expect gs:// style path or bucket/object
        path = self.config.get("path")
        if not path:
            bucket = self.config.get("bucket")
            blob = self.config.get("blob") or self.config.get("key")
            if bucket and blob:
                path = f"gs://{bucket}/{blob}"
        if not path:
            raise ValueError("GCSSource requires either 'path' or 'bucket'+'blob' in config.")

        storage_options = {}
        for opt_key in [
            "token",  # path to service account json or dict credentials
            "project",
        ]:
            if opt_key in self.config:
                storage_options[opt_key] = self.config[opt_key]

        fmt = _infer_format_from_path(path, self.config.get("format"))
        return _materialize_remote_to_parquet(path, fmt, storage_options or None, pack_config)

class AzureBlobSource(DataSource):
    def __init__(self, config):
        self.config = config or {}

    def get_data(self, table_or_query=None, pack_config=None):
        # Accept full abfs(s):// path or account/container/blob components
        path = self.config.get("path")
        if not path:
            account_name = self.config.get("account_name")
            container = self.config.get("container")
            blob = self.config.get("blob") or self.config.get("key")
            if account_name and container and blob:
                path = f"abfs://{container}@{account_name}.dfs.core.windows.net/{blob}"
        if not path:
            raise ValueError("AzureBlobSource requires either 'path' or 'account_name'+'container'+'blob'.")

        storage_options = {}
        # adlfs uses Azure credentials via storage_options
        for opt_key in [
            "account_name",
            "account_key",
            "sas_token",
            "tenant_id",
            "client_id",
            "client_secret",
        ]:
            if opt_key in self.config:
                storage_options[opt_key] = self.config[opt_key]

        fmt = _infer_format_from_path(path, self.config.get("format"))
        return _materialize_remote_to_parquet(path, fmt, storage_options or None, pack_config)

class HDFSSource(DataSource):
    def __init__(self, config):
        self.config = config or {}

    def get_data(self, table_or_query=None, pack_config=None):
        # Expect hdfs://host:port/path
        path = self.config.get("path")
        if not path:
            host = self.config.get("host")
            port = self.config.get("port") or 8020
            hdfs_path = self.config.get("hdfs_path") or self.config.get("key")
            if host and hdfs_path:
                path = f"hdfs://{host}:{port}/{hdfs_path.lstrip('/')}"
        if not path:
            raise ValueError("HDFSSource requires 'path' or 'host'+'hdfs_path' in config.")

        storage_options = {}
        for opt_key in [
            "host",
            "port",
            "user",
            "kerb_kwargs",  # kerberos parameters if applicable
        ]:
            if opt_key in self.config:
                storage_options[opt_key] = self.config[opt_key]

        fmt = _infer_format_from_path(path, self.config.get("format"))
        return _materialize_remote_to_parquet(path, fmt, storage_options or None, pack_config)

class FolderSource(DataSource):
    def __init__(self, config):
        self.config = config
    def get_data(self, table_or_query=None, pack_config=None):
        raise NotImplementedError("FolderSource.get_data Not yet Implemented.")

class MongoDBSource(DataSource):
    def __init__(self, config):
        self.config = config
    def get_data(self, table_or_query=None, pack_config=None):
        raise NotImplementedError("MongoDBSource.get_data Not yet Implemented.")

class SqliteSource(DataSource):
    def __init__(self, config):
        self.config = config
    def get_data(self, table_or_query=None, pack_config=None):
        raise NotImplementedError("SqliteSource.get_data Not yet Implemented.")


def get_data_source(source_config):
    type_ = source_config.get("type")
    if type_ == "file":
        return FileSource(source_config.get("config", {}).get("path"))
    elif type_ == "folder":
        return FolderSource(source_config.get("config", {}))
    elif type_ == "postgresql":
        return DatabaseSource(connection_string=source_config.get("config", {}).get("connection_string"), config=source_config.get("config"))
    elif type_ == "mysql":
        return DatabaseSource(connection_string=source_config.get("config", {}).get("connection_string"), config=source_config.get("config"))
    elif type_ == "oracle":
        return DatabaseSource(connection_string=source_config.get("config", {}).get("connection_string"), config=source_config.get("config"))
    elif type_ == "mssql":
        return DatabaseSource(connection_string=source_config.get("config", {}).get("connection_string"), config=source_config.get("config"))
    elif type_ == "sqlite":
        return DatabaseSource(connection_string=source_config.get("config", {}).get("connection_string"), config=source_config.get("config"))
    elif type_ == "mongodb":
        return DatabaseSource(connection_string=source_config.get("config", {}).get("connection_string"), config=source_config.get("config"))
    elif type_ == "s3":
        return S3Source(source_config.get("config", {}))
    elif type_ == "gcs":
        return GCSSource(source_config.get("config", {}))
    elif type_ == "azure_blob":
        return AzureBlobSource(source_config.get("config", {}))
    elif type_ == "hdfs":
        return HDFSSource(source_config.get("config", {}))
    else:
        raise ValueError(f"Unsupported source type: {type_}")
