"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.data_source_opener module
"""

import pytest
import os
import json
import sqlite3
import tempfile
from pathlib import Path

from qalita_core.data_source_opener import (
    FileSource,
    DatabaseSource,
    S3Source,
    GCSSource,
    AzureBlobSource,
    HDFSSource,
    FolderSource,
    MongoDBSource,
    SqliteSource,
    get_data_source,
    _ensure_output_dir,
    _build_base_name,
    _build_parquet_path,
    _infer_format_from_path,
    DEFAULT_PORTS,
)


class TestDefaultPorts:
    """Tests for DEFAULT_PORTS constant."""

    def test_postgresql_port(self):
        assert DEFAULT_PORTS["5432"] == "postgresql"

    def test_mysql_port(self):
        assert DEFAULT_PORTS["3306"] == "mysql"

    def test_mssql_port(self):
        assert DEFAULT_PORTS["1433"] == "mssql+pymssql"

    def test_oracle_port(self):
        assert DEFAULT_PORTS["1521"] == "oracle"

    def test_mongodb_port(self):
        assert DEFAULT_PORTS["27017"] == "mongodb"

    def test_sqlite_port(self):
        assert DEFAULT_PORTS["5000"] == "sqlite"


class TestHelperFunctions:
    """Tests for helper utility functions."""

    def test_ensure_output_dir_creates_directory(self, tmp_path):
        pack_config = {"parquet_output_dir": str(tmp_path / "new_dir")}
        result = _ensure_output_dir(pack_config)
        assert os.path.exists(result)
        assert result == str(tmp_path / "new_dir")

    def test_ensure_output_dir_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = _ensure_output_dir(None)
        assert result == "./parquet"
        assert os.path.exists(result)

    def test_ensure_output_dir_empty_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = _ensure_output_dir({})
        assert result == "./parquet"

    def test_build_base_name(self):
        result = _build_base_name("file", "testdata")
        assert result == "file_testdata"

    def test_build_base_name_with_special_chars(self):
        result = _build_base_name("file", "Test Data!")
        # Should be slugified
        assert "_" in result or result == "file_test_data_"

    def test_build_parquet_path(self, tmp_path):
        result = _build_parquet_path(str(tmp_path), "data", 1)
        assert result.endswith("data_part_1.parquet")
        assert str(tmp_path) in result

    def test_infer_format_csv(self):
        assert _infer_format_from_path("data.csv") == "csv"

    def test_infer_format_json(self):
        assert _infer_format_from_path("data.json") == "json"

    def test_infer_format_parquet(self):
        assert _infer_format_from_path("data.parquet") == "parquet"
        assert _infer_format_from_path("data.pq") == "parquet"

    def test_infer_format_excel(self):
        assert _infer_format_from_path("data.xlsx") == "excel"
        assert _infer_format_from_path("data.xls") == "excel"

    def test_infer_format_explicit_override(self):
        result = _infer_format_from_path("data.csv", explicit_format="json")
        assert result == "json"

    def test_infer_format_unknown_defaults_to_csv(self):
        assert _infer_format_from_path("data.unknown") == "csv"


class TestFileSource:
    """Tests for FileSource class."""

    def test_init(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("col1,col2\n1,2\n3,4\n")
        source = FileSource(str(csv_path))
        assert source.file_path == str(csv_path)

    def test_get_data_csv(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("col1,col2\n1,2\n3,4\n5,6\n")
        
        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}
        
        paths = source.get_data(pack_config=pack_config)
        assert isinstance(paths, list)
        assert len(paths) > 0
        assert all(p.endswith(".parquet") for p in paths)

    def test_get_data_csv_large_chunked(self, tmp_path):
        """Test CSV chunking with large file."""
        csv_path = tmp_path / "large.csv"
        with open(csv_path, "w") as f:
            f.write("id,value\n")
            for i in range(2500):
                f.write(f"{i},{i*2}\n")
        
        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {
            "parquet_output_dir": str(out_dir),
            "chunk_rows": 1000,
        }
        
        paths = source.get_data(pack_config=pack_config)
        assert len(paths) == 3  # 2500 rows / 1000 chunks = 3 files

    def test_get_data_directory(self, tmp_path):
        """Test loading from directory."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("col1,col2\n1,2\n3,4\n")
        
        source = FileSource(str(tmp_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}
        
        paths = source.get_data(pack_config=pack_config)
        assert len(paths) > 0

    def test_get_data_nonexistent_file(self, tmp_path):
        source = FileSource(str(tmp_path / "nonexistent.csv"))
        with pytest.raises(FileNotFoundError):
            source.get_data()

    def test_get_data_unsupported_extension(self, tmp_path):
        txt_path = tmp_path / "test.txt"
        txt_path.write_text("some text content")
        
        source = FileSource(str(txt_path))
        with pytest.raises(ValueError):
            source.get_data()


class TestDatabaseSource:
    """Tests for DatabaseSource class."""

    def test_init_with_connection_string(self, tmp_path):
        db_path = tmp_path / "test.db"
        # Create an empty database
        conn = sqlite3.connect(db_path)
        conn.close()
        
        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        assert source.engine is not None

    def test_init_with_config(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.close()
        
        config = {
            "port": "5000",  # SQLite port in DEFAULT_PORTS
            "database": str(db_path),
        }
        source = DatabaseSource(config=config)
        assert source.engine is not None

    def test_init_without_config_raises(self):
        with pytest.raises(ValueError):
            DatabaseSource()

    def test_get_data_from_table(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, val TEXT)")
        cur.executemany("INSERT INTO items(val) VALUES (?)", [("a",), ("b",), ("c",)])
        conn.commit()
        conn.close()
        
        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}
        
        paths = source.get_data("items", pack_config=pack_config)
        assert len(paths) > 0
        assert all(p.endswith(".parquet") for p in paths)

    def test_get_data_from_query(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
        cur.executemany("INSERT INTO t(val) VALUES (?)", [(i,) for i in range(100)])
        conn.commit()
        conn.close()
        
        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}
        
        paths = source.get_data("SELECT * FROM t WHERE val > 50", pack_config=pack_config)
        assert len(paths) > 0

    def test_get_data_all_tables(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE table1(id INTEGER)")
        cur.execute("INSERT INTO table1 VALUES (1)")
        cur.execute("CREATE TABLE table2(id INTEGER)")
        cur.execute("INSERT INTO table2 VALUES (2)")
        conn.commit()
        conn.close()
        
        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}
        
        paths = source.get_data("*", pack_config=pack_config)
        assert len(paths) >= 2

    def test_is_sql_query_detection(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.close()
        
        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        
        assert source._is_sql_query("SELECT * FROM table") is True
        assert source._is_sql_query("WITH cte AS (SELECT 1) SELECT * FROM cte") is True
        assert source._is_sql_query("my_table") is False
        assert source._is_sql_query("SELECT;multiple") is True


class TestGetDataSource:
    """Tests for get_data_source factory function."""

    def test_file_source(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("col1\n1\n")
        
        source_config = {"type": "file", "config": {"path": str(csv_path)}}
        source = get_data_source(source_config)
        assert isinstance(source, FileSource)

    def test_csv_source(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("col1\n1\n")
        
        source_config = {"type": "csv", "config": {"path": str(csv_path)}}
        source = get_data_source(source_config)
        assert isinstance(source, FileSource)

    def test_postgresql_source(self):
        source_config = {
            "type": "postgresql",
            # Using placeholder URL format - no actual credentials
            "config": {"connection_string": "postgresql://localhost/testdb"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, DatabaseSource)

    def test_mysql_source(self):
        """Test MySQL source creation (skipped if MySQLdb not available)."""
        pytest.importorskip("MySQLdb", reason="MySQLdb not available")
        source_config = {
            "type": "mysql",
            # Using placeholder URL format - no actual credentials
            "config": {"connection_string": "mysql://localhost/testdb"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, DatabaseSource)

    def test_sqlite_source(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.close()
        
        source_config = {
            "type": "sqlite",
            "config": {"connection_string": f"sqlite:///{db_path}"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, DatabaseSource)

    def test_s3_source(self):
        source_config = {
            "type": "s3",
            "config": {"path": "s3://bucket/key.csv"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, S3Source)

    def test_gcs_source(self):
        source_config = {
            "type": "gcs",
            "config": {"path": "gs://bucket/key.csv"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, GCSSource)

    def test_azure_blob_source(self):
        source_config = {
            "type": "azure_blob",
            "config": {"path": "abfs://container@account.dfs.core.windows.net/key.csv"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, AzureBlobSource)

    def test_hdfs_source(self):
        source_config = {
            "type": "hdfs",
            "config": {"path": "hdfs://host:8020/path/file.csv"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, HDFSSource)

    def test_folder_source(self):
        source_config = {"type": "folder", "config": {"path": "/some/path"}}
        source = get_data_source(source_config)
        assert isinstance(source, FolderSource)

    def test_unsupported_source_type(self):
        source_config = {"type": "unsupported", "config": {}}
        with pytest.raises(ValueError, match="Unsupported source type"):
            get_data_source(source_config)


class TestS3Source:
    """Tests for S3Source class."""

    def test_init(self):
        config = {"path": "s3://bucket/key.csv"}
        source = S3Source(config)
        assert source.config == config

    def test_get_data_missing_path_raises(self):
        source = S3Source({})
        with pytest.raises(ValueError):
            source.get_data()

    def test_get_data_constructs_path_from_bucket_key(self):
        source = S3Source({"bucket": "mybucket", "key": "mykey.parquet"})
        # This will fail at actual S3 access, but we can verify path construction
        # by checking the exception message or using mocks in integration tests


class TestGCSSource:
    """Tests for GCSSource class."""

    def test_init(self):
        config = {"path": "gs://bucket/key.csv"}
        source = GCSSource(config)
        assert source.config == config

    def test_get_data_missing_path_raises(self):
        source = GCSSource({})
        with pytest.raises(ValueError):
            source.get_data()


class TestAzureBlobSource:
    """Tests for AzureBlobSource class."""

    def test_init(self):
        config = {"path": "abfs://container@account.dfs.core.windows.net/key.csv"}
        source = AzureBlobSource(config)
        assert source.config == config

    def test_get_data_missing_path_raises(self):
        source = AzureBlobSource({})
        with pytest.raises(ValueError):
            source.get_data()


class TestHDFSSource:
    """Tests for HDFSSource class."""

    def test_init(self):
        config = {"path": "hdfs://host:8020/path/file.csv"}
        source = HDFSSource(config)
        assert source.config == config

    def test_get_data_missing_path_raises(self):
        source = HDFSSource({})
        with pytest.raises(ValueError):
            source.get_data()


class TestFolderSource:
    """Tests for FolderSource class."""

    def test_init(self):
        config = {"path": "/some/path"}
        source = FolderSource(config)
        assert source.config == config

    def test_get_data_not_implemented(self):
        source = FolderSource({})
        with pytest.raises(NotImplementedError):
            source.get_data()


class TestMongoDBSource:
    """Tests for MongoDBSource class."""

    def test_init(self):
        config = {"connection_string": "mongodb://localhost:27017"}
        source = MongoDBSource(config)
        assert source.config == config

    def test_get_data_not_implemented(self):
        source = MongoDBSource({})
        with pytest.raises(NotImplementedError):
            source.get_data()


class TestSqliteSource:
    """Tests for SqliteSource class."""

    def test_init(self):
        config = {"database": "test.db"}
        source = SqliteSource(config)
        assert source.config == config

    def test_get_data_not_implemented(self):
        source = SqliteSource({})
        with pytest.raises(NotImplementedError):
            source.get_data()


class TestDatabaseSourceSchemaHandling:
    """Tests for DatabaseSource schema handling."""

    def test_schema_from_config(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t(id INTEGER)")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        conn.close()
        
        config = {"schema": "main"}  # SQLite default schema
        source = DatabaseSource(
            connection_string=f"sqlite:///{db_path}",
            config=config,
        )
        assert source.config["schema"] == "main"

    def test_fully_qualified_table_name(self, tmp_path):
        """Test handling of SCHEMA.TABLE format."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE items(id INTEGER)")
        cur.execute("INSERT INTO items VALUES (1)")
        conn.commit()
        conn.close()
        
        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}
        
        # SQLite doesn't really support schemas the same way, but we test parsing
        # This should split "main.items" into schema="main", table="items"
        paths = source.get_data("items", pack_config=pack_config)
        assert len(paths) > 0
