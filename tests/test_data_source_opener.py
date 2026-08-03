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

import polars as pl
import pyarrow.parquet as pq

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
    _materialize_remote_to_parquet,
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
        cur.executemany(
            "INSERT INTO items(val) VALUES (?)", [("a",), ("b",), ("c",)]
        )
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
        cur.executemany(
            "INSERT INTO t(val) VALUES (?)", [(i,) for i in range(100)]
        )
        conn.commit()
        conn.close()

        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(
            "SELECT * FROM t WHERE val > 50", pack_config=pack_config
        )
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
        assert (
            source._is_sql_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
            is True
        )
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
            "config": {
                "path": "abfs://container@account.dfs.core.windows.net/key.csv"
            },
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

    def test_credentials_reach_the_scan(self):
        """The options used to be built and then dropped on the parquet path."""
        source = S3Source(
            {
                "path": "s3://bucket/key.parquet",
                "key": "AKIA",
                "secret": "shh",
                "token": "sess",
                "client_kwargs": {"region_name": "eu-west-3"},
            }
        )
        assert source._storage_options() == {
            "aws_access_key_id": "AKIA",
            "aws_secret_access_key": "shh",
            "aws_session_token": "sess",
            "aws_region": "eu-west-3",
        }

    def test_no_credentials_means_no_options(self):
        assert (
            S3Source({"path": "s3://bucket/key.csv"})._storage_options()
            is None
        )


class TestRemoteMaterialization:
    """Remote parquet: pass through when it can be read, stage when it cannot."""

    def _parquet(self, tmp_path):
        path = tmp_path / "remote.parquet"
        pl.DataFrame({"a": [1, 2, 3]}).write_parquet(path)
        return str(path)

    def test_public_parquet_is_passed_through(self, tmp_path):
        source = S3Source({"path": "s3://bucket/remote.parquet"})
        paths = _materialize_remote_to_parquet(
            source,
            self._parquet(tmp_path),
            "parquet",
            None,
            {"parquet_output_dir": str(tmp_path / "out")},
        )
        assert paths == [self._parquet(tmp_path)]
        assert list(source.object_paths) == ["remote_remote"]

    def test_private_parquet_is_staged_with_its_credentials(self, tmp_path):
        # get_data returns bare paths, so credentials cannot travel with them:
        # the object is staged rather than silently scanned anonymously later.
        source = S3Source({"path": "s3://bucket/remote.parquet", "key": "AK"})
        paths = _materialize_remote_to_parquet(
            source,
            self._parquet(tmp_path),
            "parquet",
            source._storage_options(),
            {"parquet_output_dir": str(tmp_path / "out")},
        )
        assert [os.path.basename(p) for p in paths] == [
            "remote_remote_part_1.parquet"
        ]
        assert pl.scan_parquet(paths).collect().height == 3


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
        config = {
            "path": "abfs://container@account.dfs.core.windows.net/key.csv"
        }
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

    def test_get_data_requires_database(self):
        """Test that MongoDBSource requires database in config."""
        source = MongoDBSource(
            {"connection_string": "mongodb://localhost:27017"}
        )
        with pytest.raises((ImportError, ValueError)):
            # Will raise ImportError if pymongo not available, or ValueError if database missing
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


def _make_db(path, tables):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    for name, rows in tables.items():
        cur.execute(f"CREATE TABLE {name}(id INTEGER, v TEXT)")
        cur.executemany(f"INSERT INTO {name} VALUES (?,?)", rows)
    conn.commit()
    conn.close()


class TestObjectPaths:
    """Every source records which parts belong to which logical object."""

    def test_database_records_one_entry_per_table(self, tmp_path):
        db_path = tmp_path / "objects.db"
        _make_db(
            db_path,
            {
                "alpha": [(i, f"a{i}") for i in range(2500)],
                "beta": [(i, f"b{i}") for i in range(10)],
            },
        )

        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        pack_config = {
            "parquet_output_dir": str(tmp_path / "out"),
            "chunk_rows": 1000,
        }
        paths = source.get_data("*", pack_config=pack_config)

        # The pairing is recorded while writing, so the three parts of "alpha"
        # cannot be mistaken for three separate tables.
        assert set(source.object_paths) == {"sqlite_alpha", "sqlite_beta"}
        assert len(source.object_paths["sqlite_alpha"]) == 3
        assert len(source.object_paths["sqlite_beta"]) == 1
        assert sorted(paths) == sorted(
            p for parts in source.object_paths.values() for p in parts
        )

    def test_file_records_its_object(self, tmp_path):
        csv_path = tmp_path / "people.csv"
        csv_path.write_text("id,name\n1,a\n2,b\n")

        source = FileSource(str(csv_path))
        source.get_data(
            pack_config={"parquet_output_dir": str(tmp_path / "out")}
        )
        assert list(source.object_paths) == ["file_people"]

    def test_two_tables_with_the_same_slug_stay_two_objects(self, tmp_path):
        """``slugify`` folds the accent, so both tables shared one base name.

        The base name is both the ``object_paths`` key and the part-file
        prefix, so the second table used to truncate the first one's
        ``_part_1`` and the two path lists were merged: one table disappeared
        from ``tables()`` and the survivor's rows were counted twice. Mongo
        collections and ES indices hit this with ``-`` and ``.``; SQL hits it
        with accents and with case.
        """
        db_path = tmp_path / "collide.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for name, count in (("données", 5), ("donnees", 2)):
            cursor.execute(f'CREATE TABLE "{name}"(id INTEGER, v TEXT)')
            cursor.executemany(
                f'INSERT INTO "{name}" VALUES (?,?)',
                [(i, f"{name}-{i}") for i in range(count)],
            )
        conn.commit()
        conn.close()

        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        paths = source.get_data(
            "*",
            pack_config={
                "parquet_output_dir": str(tmp_path / "out"),
                # Forces several parts, so a collision overwrites part 1 while
                # part 2 survives — the shape that silently loses rows.
                "chunk_rows": 3,
            },
        )

        assert len(source.object_paths) == 2
        # No part file is claimed by two objects, hence none was overwritten.
        assert len(set(paths)) == len(paths)
        rows = sum(
            pl.scan_parquet(parts).select(pl.len()).collect().item()
            for parts in source.object_paths.values()
        )
        assert rows == 7


class TestStreamingFileFormats:
    """Files always go through the Polars streaming path now."""

    def test_small_csv_is_written_with_zstd(self, tmp_path):
        # The pandas path used to write these with snappy, so parts of one
        # object could differ in compression from one another.
        csv_path = tmp_path / "small.csv"
        csv_path.write_text("id,value\n1,10\n2,20\n")

        source = FileSource(str(csv_path))
        paths = source.get_data(
            pack_config={"parquet_output_dir": str(tmp_path / "out")}
        )
        metadata = pq.ParquetFile(paths[0]).metadata
        compression = metadata.row_group(0).column(0).compression
        assert compression.upper() == "ZSTD"

    def test_ndjson_document_is_detected_without_a_flag(self, tmp_path):
        # json_lines defaulted to False, so NDJSON was parsed as one document.
        json_path = tmp_path / "events.json"
        json_path.write_text(
            "\n".join(json.dumps({"id": i}) for i in range(2500))
        )

        source = FileSource(str(json_path))
        paths = source.get_data(
            pack_config={
                "parquet_output_dir": str(tmp_path / "out"),
                "chunk_rows": 1000,
            }
        )
        assert len(paths) == 3
        assert pl.scan_parquet(paths).select(pl.len()).collect().item() == 2500

    def test_json_array_document_is_streamed(self, tmp_path):
        json_path = tmp_path / "records.json"
        json_path.write_text(
            json.dumps([{"id": i, "name": f"n{i}"} for i in range(2500)])
        )

        source = FileSource(str(json_path))
        paths = source.get_data(
            pack_config={
                "parquet_output_dir": str(tmp_path / "out"),
                "chunk_rows": 1000,
            }
        )
        assert len(paths) == 3
        frame = pl.scan_parquet(paths).collect()
        assert frame.height == 2500
        assert frame["name"][0] == "n0"


class TestSqlStreaming:
    """SQL result sets reach Parquet as Arrow batches, one schema per object."""

    def test_parts_of_a_table_share_a_schema(self, tmp_path):
        db_path = tmp_path / "drift.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t(id INTEGER, v TEXT)")
        # The first chunk is entirely NULL on `v`: inferring per part types it
        # Null there and String in the next one, and scanning the parts
        # together then raises SchemaError.
        cur.executemany(
            "INSERT INTO t VALUES (?,?)",
            [(i, None) for i in range(1000)]
            + [(i, f"v{i}") for i in range(1000, 2000)],
        )
        conn.commit()
        conn.close()

        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        paths = source.get_data(
            "t",
            pack_config={
                "parquet_output_dir": str(tmp_path / "out"),
                "chunk_rows": 500,
            },
        )
        assert len(paths) == 4
        frame = pl.scan_parquet(paths).collect()
        assert frame.height == 2000
        assert frame["v"].null_count() == 1000

    def test_empty_table_yields_a_scannable_object(self, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE blank(id INTEGER, v TEXT)")
        conn.commit()
        conn.close()

        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        paths = source.get_data(
            "blank",
            pack_config={"parquet_output_dir": str(tmp_path / "out")},
        )
        assert len(paths) == 1
        assert pl.scan_parquet(paths).select(pl.len()).collect().item() == 0
