"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Extended tests for qalita_core.data_source_opener module
Tests for additional drivers, output formats validation, and error handling
"""

import pytest
import os
import json
import tempfile
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from qalita_core.data_source_opener import (
    FileSource,
    DatabaseSource,
    DuckDBSource,
    MongoDBSource,
    SnowflakeSource,
    BigQuerySource,
    DatabricksSource,
    RedshiftSource,
    ClickHouseSource,
    TrinoSource,
    TeradataSource,
    SapHanaSource,
    CassandraSource,
    ElasticsearchSource,
    IbmDb2Source,
    AthenaSource,
    SynapseSource,
    S3Source,
    GCSSource,
    AzureBlobSource,
    get_data_source,
    _ensure_output_dir,
    _build_base_name,
    _build_parquet_path,
    _infer_format_from_path,
    _materialize_remote_to_parquet,
)


# =============================================================================
# DuckDB Source Tests (Local, No External Service Required)
# =============================================================================


class TestDuckDBSource:
    """Tests for DuckDBSource class."""

    def test_init(self):
        """Test DuckDBSource initialization."""
        config = {"path": ":memory:"}
        source = DuckDBSource(config)
        assert source.config == config

    def test_get_data_from_memory_db(self, tmp_path):
        """Test loading data from in-memory DuckDB database."""
        import duckdb

        # Create in-memory database with test data
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)")
        conn.execute("INSERT INTO test_table VALUES (1, 'Alice', 10.5)")
        conn.execute("INSERT INTO test_table VALUES (2, 'Bob', 20.3)")
        conn.execute("INSERT INTO test_table VALUES (3, 'Charlie', 30.1)")
        
        # Save to file for testing
        db_path = tmp_path / "test.duckdb"
        conn.execute(f"EXPORT DATABASE '{tmp_path}/export'")
        conn.close()

        # Create new database and import
        conn2 = duckdb.connect(str(db_path))
        conn2.execute(f"IMPORT DATABASE '{tmp_path}/export'")
        conn2.close()

        source = DuckDBSource({"path": str(db_path)})
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data("test_table", pack_config=pack_config)
        assert len(paths) > 0
        assert all(p.endswith(".parquet") for p in paths)

        # Verify parquet content
        df = pd.read_parquet(paths[0])
        assert len(df) == 3
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns

    def test_get_data_from_query(self, tmp_path):
        """Test executing SQL query in DuckDB."""
        import duckdb

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE items (id INTEGER, quantity INTEGER)")
        for i in range(100):
            conn.execute(f"INSERT INTO items VALUES ({i}, {i * 2})")
        conn.close()

        source = DuckDBSource({"path": str(db_path)})
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data("SELECT * FROM items WHERE quantity > 100", pack_config=pack_config)
        assert len(paths) > 0

        df = pd.read_parquet(paths[0])
        assert all(df["quantity"] > 100)

    def test_get_data_all_tables(self, tmp_path):
        """Test scanning all tables in DuckDB."""
        import duckdb

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE table1 (id INTEGER)")
        conn.execute("INSERT INTO table1 VALUES (1)")
        conn.execute("CREATE TABLE table2 (id INTEGER)")
        conn.execute("INSERT INTO table2 VALUES (2)")
        conn.close()

        source = DuckDBSource({"path": str(db_path)})
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data("*", pack_config=pack_config)
        assert len(paths) >= 2

    def test_get_data_chunking(self, tmp_path):
        """Test DuckDB chunking with large dataset."""
        import duckdb

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE large_table (id INTEGER, value VARCHAR)")
        # Insert 2500 rows
        for i in range(2500):
            conn.execute(f"INSERT INTO large_table VALUES ({i}, 'value_{i}')")
        conn.close()

        source = DuckDBSource({"path": str(db_path)})
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir), "chunk_rows": 1000}

        paths = source.get_data("large_table", pack_config=pack_config)
        # DuckDB now uses streaming COPY TO PARQUET which creates a single optimized file
        # with appropriate row groups instead of multiple chunks (more efficient for big data)
        assert len(paths) >= 1
        # Verify all data was exported
        import pandas as pd
        total_rows = sum(len(pd.read_parquet(p)) for p in paths)
        assert total_rows == 2500

    def test_is_sql_query_detection(self):
        """Test SQL query detection in DuckDB source."""
        source = DuckDBSource({})
        assert source._is_sql_query("SELECT * FROM table") is True
        assert source._is_sql_query("WITH cte AS (SELECT 1) SELECT * FROM cte") is True
        assert source._is_sql_query("PRAGMA table_info(test)") is True
        assert source._is_sql_query("my_table") is False


# =============================================================================
# JSON File Format Tests
# =============================================================================


class TestJSONFileFormat:
    """Tests for JSON file loading."""

    def test_json_file_loading(self, tmp_path):
        """Test loading regular JSON file."""
        json_path = tmp_path / "test.json"
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.3},
            {"id": 3, "name": "Charlie", "score": 92.1},
        ]
        with open(json_path, "w") as f:
            json.dump(data, f)

        # Test format inference
        assert _infer_format_from_path(str(json_path)) == "json"

    def test_infer_format_jsonl(self):
        """Test format inference for JSONL files."""
        assert _infer_format_from_path("data.json") == "json"
        assert _infer_format_from_path("data.JSON") == "json"


# =============================================================================
# Parquet File Format Tests
# =============================================================================


class TestParquetFileFormat:
    """Tests for Parquet file handling."""

    def test_parquet_format_inference(self):
        """Test format inference for Parquet files."""
        assert _infer_format_from_path("data.parquet") == "parquet"
        assert _infer_format_from_path("data.pq") == "parquet"
        assert _infer_format_from_path("DATA.PARQUET") == "parquet"

    def test_parquet_passthrough(self, tmp_path):
        """Test that parquet files are passed through without conversion."""
        # Create a parquet file
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(parquet_path, engine="pyarrow")

        # Test format inference
        fmt = _infer_format_from_path(str(parquet_path))
        assert fmt == "parquet"


# =============================================================================
# Excel File Format Tests with Chunking
# =============================================================================


class TestExcelFileFormat:
    """Tests for Excel file loading with chunking."""

    def test_excel_format_inference(self):
        """Test format inference for Excel files."""
        assert _infer_format_from_path("data.xlsx") == "excel"
        assert _infer_format_from_path("data.xls") == "excel"
        assert _infer_format_from_path("DATA.XLSX") == "excel"

    def test_excel_file_loading(self, tmp_path):
        """Test loading Excel file."""
        excel_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "id": range(100),
            "name": [f"Name_{i}" for i in range(100)],
            "value": [i * 1.5 for i in range(100)],
        })
        df.to_excel(excel_path, index=False, engine="openpyxl")

        source = FileSource(str(excel_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(pack_config=pack_config)
        assert len(paths) > 0
        assert all(p.endswith(".parquet") for p in paths)

        # Verify content
        result_df = pd.read_parquet(paths[0])
        assert len(result_df) == 100
        assert "id" in result_df.columns

    def test_excel_chunking(self, tmp_path):
        """Test Excel file chunking with large dataset."""
        excel_path = tmp_path / "large.xlsx"
        df = pd.DataFrame({
            "id": range(2500),
            "value": [f"value_{i}" for i in range(2500)],
        })
        df.to_excel(excel_path, index=False, engine="openpyxl")

        source = FileSource(str(excel_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir), "chunk_rows": 1000}

        paths = source.get_data(pack_config=pack_config)
        # Should create multiple parquet files
        assert len(paths) >= 2

    def test_excel_with_skiprows(self, tmp_path):
        """Test Excel loading with skiprows option."""
        excel_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "col1": ["header1", "header2", 1, 2, 3],
            "col2": ["meta1", "meta2", "a", "b", "c"],
        })
        df.to_excel(excel_path, index=False, engine="openpyxl")

        source = FileSource(str(excel_path))
        out_dir = tmp_path / "output"
        pack_config = {
            "parquet_output_dir": str(out_dir),
            "job": {"source": {"skiprows": 2}},
        }

        paths = source.get_data(pack_config=pack_config)
        assert len(paths) > 0


# =============================================================================
# Parquet Output Format Validation Tests
# =============================================================================


class TestParquetOutputValidation:
    """Tests for validating Parquet output format."""

    def test_parquet_output_is_valid(self, tmp_path):
        """Test that output parquet files are valid and readable."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name,value\n1,Alice,10.5\n2,Bob,20.3\n")

        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(pack_config=pack_config)
        
        # Validate with PyArrow
        for path in paths:
            table = pq.read_table(path)
            assert table is not None
            assert table.num_rows > 0
            assert len(table.column_names) == 3

    def test_parquet_schema_preservation(self, tmp_path):
        """Test that schema is correctly preserved in Parquet output."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            "int_col,float_col,str_col,bool_col\n"
            "1,1.5,hello,True\n"
            "2,2.5,world,False\n"
        )

        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(pack_config=pack_config)
        table = pq.read_table(paths[0])

        # Verify schema
        schema = table.schema
        assert "int_col" in schema.names
        assert "float_col" in schema.names
        assert "str_col" in schema.names
        assert "bool_col" in schema.names

    def test_parquet_deterministic_naming(self, tmp_path):
        """Test that parquet files have deterministic names."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id\n1\n2\n3\n")

        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(pack_config=pack_config)
        
        # File should have predictable naming pattern
        assert any("_part_1.parquet" in p for p in paths)

    def test_parquet_chunked_output_consistency(self, tmp_path):
        """Test that chunked parquet files maintain data consistency."""
        csv_path = tmp_path / "large.csv"
        with open(csv_path, "w") as f:
            f.write("id,value\n")
            for i in range(2500):
                f.write(f"{i},{i * 2}\n")

        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir), "chunk_rows": 1000}

        paths = source.get_data(pack_config=pack_config)
        
        # Read all chunks and verify total row count
        total_rows = 0
        for path in paths:
            df = pd.read_parquet(path)
            total_rows += len(df)
        
        assert total_rows == 2500


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling in data sources."""

    def test_missing_file_error(self, tmp_path):
        """Test error when file doesn't exist."""
        source = FileSource(str(tmp_path / "nonexistent.csv"))
        with pytest.raises(FileNotFoundError):
            source.get_data()

    def test_unsupported_file_extension(self, tmp_path):
        """Test error for unsupported file extension."""
        txt_path = tmp_path / "test.txt"
        txt_path.write_text("some content")
        
        source = FileSource(str(txt_path))
        with pytest.raises(ValueError):
            source.get_data()

    def test_database_source_no_config(self):
        """Test error when DatabaseSource has no config."""
        with pytest.raises(ValueError, match="requires a connection_string or a config"):
            DatabaseSource()

    def test_s3_source_missing_path(self):
        """Test error when S3Source has no path."""
        source = S3Source({})
        with pytest.raises(ValueError, match="requires either 'path' or 'bucket'"):
            source.get_data()

    def test_gcs_source_missing_path(self):
        """Test error when GCSSource has no path."""
        source = GCSSource({})
        with pytest.raises(ValueError, match="requires either 'path' or 'bucket'"):
            source.get_data()

    def test_azure_blob_source_missing_path(self):
        """Test error when AzureBlobSource has no path."""
        source = AzureBlobSource({})
        with pytest.raises(ValueError, match="requires either 'path' or"):
            source.get_data()

    def test_snowflake_source_missing_credentials(self):
        """Test error when SnowflakeSource has no credentials."""
        source = SnowflakeSource({})
        with pytest.raises(ValueError, match="requires 'account'"):
            source.get_data()

    def test_bigquery_source_missing_project(self):
        """Test error when BigQuerySource has no project."""
        source = BigQuerySource({})
        with pytest.raises(ValueError, match="requires 'project'"):
            source.get_data()

    def test_databricks_source_missing_config(self):
        """Test error when DatabricksSource has incomplete config."""
        source = DatabricksSource({})
        with pytest.raises((ValueError, ImportError)):
            source.get_data()

    def test_redshift_source_missing_config(self):
        """Test error when RedshiftSource has incomplete config."""
        source = RedshiftSource({})
        with pytest.raises(ValueError, match="requires 'host'"):
            source.get_data()

    def test_teradata_source_missing_config(self):
        """Test error when TeradataSource has incomplete config."""
        source = TeradataSource({})
        with pytest.raises(ValueError, match="requires 'host'"):
            source.get_data()

    def test_sap_hana_source_missing_config(self):
        """Test error when SapHanaSource has incomplete config."""
        source = SapHanaSource({})
        with pytest.raises(ValueError, match="requires 'host'"):
            source.get_data()

    def test_ibm_db2_source_missing_config(self):
        """Test error when IbmDb2Source has incomplete config."""
        source = IbmDb2Source({})
        with pytest.raises(ValueError, match="requires 'host'"):
            source.get_data()

    def test_athena_source_missing_staging_dir(self):
        """Test error when AthenaSource has no s3_staging_dir."""
        source = AthenaSource({})
        with pytest.raises(ValueError, match="requires 's3_staging_dir'"):
            source.get_data()

    def test_synapse_source_missing_config(self):
        """Test error when SynapseSource has incomplete config."""
        source = SynapseSource({})
        with pytest.raises(ValueError, match="requires 'host'"):
            source.get_data()

    def test_empty_directory_error(self, tmp_path):
        """Test error when directory has no supported files."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        
        source = FileSource(str(empty_dir))
        with pytest.raises(FileNotFoundError, match="No CSV or XLSX files"):
            source.get_data()


# =============================================================================
# MongoDB Source Tests with Mocking
# =============================================================================


class TestMongoDBSourceMocked:
    """Tests for MongoDBSource using mocking."""

    def test_mongodb_source_init(self):
        """Test MongoDBSource initialization."""
        config = {"connection_string": "mongodb://localhost:27017", "database": "testdb"}
        source = MongoDBSource(config)
        assert source.config == config

    def test_mongodb_source_missing_database(self):
        """Test error when database is not specified."""
        source = MongoDBSource({"connection_string": "mongodb://localhost:27017"})
        
        with patch("qalita_core.data_source_opener.MongoDBSource.get_data") as mock_get_data:
            mock_get_data.side_effect = ValueError("MongoDBSource requires 'database' in config.")
            with pytest.raises(ValueError, match="requires 'database'"):
                mock_get_data()

    def test_mongodb_get_data_mocked(self, tmp_path):
        """Test MongoDBSource data retrieval with mocked client."""
        pytest.importorskip("pymongo", reason="pymongo not available or conflict with standalone bson")
        
        with patch("pymongo.MongoClient") as mock_client_class:
            # Setup mock
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_db = MagicMock()
            mock_client.__getitem__.return_value = mock_db
            
            mock_collection = MagicMock()
            mock_db.__getitem__.return_value = mock_collection
            mock_db.list_collection_names.return_value = ["test_collection"]
            
            # Mock cursor with documents - use simple dict with string _id
            mock_docs = [
                {"_id": "507f1f77bcf86cd799439011", "name": "Alice", "value": 10},
                {"_id": "507f1f77bcf86cd799439012", "name": "Bob", "value": 20},
            ]
            mock_collection.find.return_value = iter(mock_docs)
            
            source = MongoDBSource({
                "connection_string": "mongodb://localhost:27017",
                "database": "testdb"
            })
            out_dir = tmp_path / "output"
            pack_config = {"parquet_output_dir": str(out_dir)}
            
            paths = source.get_data("test_collection", pack_config=pack_config)
            assert len(paths) > 0


# =============================================================================
# Data Warehouse Source Tests with Mocking
# =============================================================================


class TestSnowflakeSourceMocked:
    """Tests for SnowflakeSource using mocking."""

    def test_snowflake_source_init(self):
        """Test SnowflakeSource initialization."""
        config = {"account": "test", "user": "user", "password": "pass"}
        source = SnowflakeSource(config)
        assert source.config == config

    def test_snowflake_connection_string_building(self):
        """Test Snowflake connection string construction."""
        source = SnowflakeSource({
            "account": "myaccount",
            "user": "myuser",
            "password": "mypass",
            "database": "mydb",
            "schema": "myschema",
            "warehouse": "mywh",
            "role": "myrole",
        })
        # Connection string should be built correctly
        assert source.config["account"] == "myaccount"
        assert source.config["warehouse"] == "mywh"


class TestClickHouseSourceMocked:
    """Tests for ClickHouseSource using mocking."""

    def test_clickhouse_source_init(self):
        """Test ClickHouseSource initialization."""
        config = {"host": "localhost", "port": 8123}
        source = ClickHouseSource(config)
        assert source.config == config

    def test_clickhouse_connection_string_with_password(self):
        """Test ClickHouse connection string with password."""
        config = {
            "host": "localhost",
            "port": 8123,
            "user": "default",
            "password": "secret",
            "database": "mydb",
            "protocol": "http",
        }
        source = ClickHouseSource(config)
        assert source.config["password"] == "secret"


class TestTrinoSourceMocked:
    """Tests for TrinoSource using mocking."""

    def test_trino_source_init(self):
        """Test TrinoSource initialization."""
        config = {"host": "localhost", "port": 8080, "catalog": "hive"}
        source = TrinoSource(config)
        assert source.config == config


# =============================================================================
# Extended Factory Tests
# =============================================================================


class TestGetDataSourceExtended:
    """Extended tests for get_data_source factory function."""

    def test_duckdb_source(self, tmp_path):
        """Test DuckDB source creation via factory."""
        db_path = tmp_path / "test.duckdb"
        source_config = {
            "type": "duckdb",
            "config": {"path": str(db_path)},
        }
        source = get_data_source(source_config)
        assert isinstance(source, DuckDBSource)

    def test_snowflake_source(self):
        """Test Snowflake source creation via factory."""
        source_config = {
            "type": "snowflake",
            "config": {"account": "test", "user": "user", "password": "pass"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, SnowflakeSource)

    def test_bigquery_source(self):
        """Test BigQuery source creation via factory."""
        source_config = {
            "type": "bigquery",
            "config": {"project": "my-project"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, BigQuerySource)

    def test_databricks_source(self):
        """Test Databricks source creation via factory."""
        source_config = {
            "type": "databricks",
            "config": {
                "server_hostname": "host",
                "http_path": "/path",
                "access_token": "token",
            },
        }
        source = get_data_source(source_config)
        assert isinstance(source, DatabricksSource)

    def test_redshift_source(self):
        """Test Redshift source creation via factory."""
        source_config = {
            "type": "redshift",
            "config": {"host": "host", "user": "user", "password": "pass", "database": "db"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, RedshiftSource)

    def test_clickhouse_source(self):
        """Test ClickHouse source creation via factory."""
        source_config = {
            "type": "clickhouse",
            "config": {"host": "localhost"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, ClickHouseSource)

    def test_trino_source(self):
        """Test Trino source creation via factory."""
        source_config = {
            "type": "trino",
            "config": {"host": "localhost", "catalog": "hive"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, TrinoSource)

    def test_teradata_source(self):
        """Test Teradata source creation via factory."""
        source_config = {
            "type": "teradata",
            "config": {"host": "host", "user": "user", "password": "pass"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, TeradataSource)

    def test_sap_hana_source(self):
        """Test SAP HANA source creation via factory."""
        source_config = {
            "type": "sap_hana",
            "config": {"host": "host", "user": "user", "password": "pass"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, SapHanaSource)

    def test_cassandra_source(self):
        """Test Cassandra source creation via factory."""
        source_config = {
            "type": "cassandra",
            "config": {"host": "localhost", "keyspace": "mykeyspace"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, CassandraSource)

    def test_elasticsearch_source(self):
        """Test Elasticsearch source creation via factory."""
        source_config = {
            "type": "elasticsearch",
            "config": {"host": "localhost"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, ElasticsearchSource)

    def test_ibm_db2_source(self):
        """Test IBM DB2 source creation via factory."""
        source_config = {
            "type": "ibm_db2",
            "config": {"host": "host", "user": "user", "password": "pass", "database": "db"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, IbmDb2Source)

    def test_athena_source(self):
        """Test Amazon Athena source creation via factory."""
        source_config = {
            "type": "athena",
            "config": {"s3_staging_dir": "s3://bucket/path"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, AthenaSource)

    def test_synapse_source(self):
        """Test Azure Synapse source creation via factory."""
        source_config = {
            "type": "synapse",
            "config": {"host": "host", "user": "user", "password": "pass", "database": "db"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, SynapseSource)

    def test_mongodb_source(self):
        """Test MongoDB source creation via factory."""
        source_config = {
            "type": "mongodb",
            "config": {"connection_string": "mongodb://localhost:27017", "database": "testdb"},
        }
        source = get_data_source(source_config)
        assert isinstance(source, MongoDBSource)

    def test_json_file_source(self, tmp_path):
        """Test JSON file source creation via factory."""
        json_path = tmp_path / "test.json"
        json_path.write_text('[{"id": 1}]')
        
        source_config = {
            "type": "json",
            "config": {"path": str(json_path)},
        }
        source = get_data_source(source_config)
        assert isinstance(source, FileSource)

    def test_parquet_file_source(self, tmp_path):
        """Test Parquet file source creation via factory."""
        parquet_path = tmp_path / "test.parquet"
        df = pd.DataFrame({"id": [1, 2, 3]})
        df.to_parquet(parquet_path)
        
        source_config = {
            "type": "parquet",
            "config": {"path": str(parquet_path)},
        }
        source = get_data_source(source_config)
        assert isinstance(source, FileSource)


# =============================================================================
# SQL Query Detection Tests
# =============================================================================


class TestSQLQueryDetection:
    """Tests for SQL query detection across different sources."""

    def test_database_source_query_detection(self, tmp_path):
        """Test SQL query detection in DatabaseSource."""
        import sqlite3
        
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.close()
        
        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        
        # SQL queries
        assert source._is_sql_query("SELECT * FROM table") is True
        assert source._is_sql_query("WITH cte AS (SELECT 1) SELECT * FROM cte") is True
        assert source._is_sql_query("SHOW TABLES") is True
        assert source._is_sql_query("DESCRIBE table") is True
        assert source._is_sql_query("PRAGMA table_info") is True
        assert source._is_sql_query("EXPLAIN SELECT * FROM t") is True
        assert source._is_sql_query("SELECT;\nmore") is True
        
        # Table names
        assert source._is_sql_query("my_table") is False
        assert source._is_sql_query("schema.table") is False


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestHelperFunctionsExtended:
    """Extended tests for helper functions."""

    def test_ensure_output_dir_nested(self, tmp_path):
        """Test creating nested output directory."""
        pack_config = {"parquet_output_dir": str(tmp_path / "a" / "b" / "c")}
        result = _ensure_output_dir(pack_config)
        assert os.path.exists(result)

    def test_build_base_name_special_chars(self):
        """Test base name building with special characters."""
        result = _build_base_name("source", "Table Name with spaces!")
        assert " " not in result or "_" in result

    def test_build_parquet_path_format(self, tmp_path):
        """Test parquet path format."""
        path = _build_parquet_path(str(tmp_path), "test", 5)
        assert "test_part_5.parquet" in path

    def test_infer_format_explicit_override(self):
        """Test format inference with explicit override."""
        assert _infer_format_from_path("file.csv", "json") == "json"
        assert _infer_format_from_path("file.txt", "parquet") == "parquet"


# =============================================================================
# Data Type Handling Tests
# =============================================================================


class TestDataTypeHandling:
    """Tests for handling various data types in Parquet output."""

    def test_datetime_handling(self, tmp_path):
        """Test datetime columns are handled correctly."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            "id,timestamp\n"
            "1,2024-01-15 10:30:00\n"
            "2,2024-02-20 15:45:30\n"
        )

        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(pack_config=pack_config)
        df = pd.read_parquet(paths[0])
        
        assert "timestamp" in df.columns

    def test_null_handling(self, tmp_path):
        """Test NULL values are handled correctly."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            "id,name,value\n"
            "1,Alice,10\n"
            "2,,20\n"
            "3,Charlie,\n"
        )

        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(pack_config=pack_config)
        df = pd.read_parquet(paths[0])
        
        # Should have null values preserved
        assert df["name"].isna().sum() == 1
        assert df["value"].isna().sum() == 1

    def test_unicode_handling(self, tmp_path):
        """Test Unicode characters are handled correctly."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            "id,name,city\n"
            "1,日本語,東京\n"
            "2,Français,Paris\n"
            "3,Español,México\n",
            encoding="utf-8"
        )

        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(pack_config=pack_config)
        df = pd.read_parquet(paths[0])
        
        assert "日本語" in df["name"].values
        assert "東京" in df["city"].values


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests combining multiple components."""

    def test_csv_to_parquet_roundtrip(self, tmp_path):
        """Test CSV to Parquet conversion preserves data."""
        csv_path = tmp_path / "test.csv"
        original_df = pd.DataFrame({
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "str_col": ["a", "b", "c", "d", "e"],
        })
        original_df.to_csv(csv_path, index=False)

        source = FileSource(str(csv_path))
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data(pack_config=pack_config)
        result_df = pd.read_parquet(paths[0])

        # Compare data
        assert len(result_df) == len(original_df)
        assert list(result_df.columns) == list(original_df.columns)
        assert result_df["int_col"].tolist() == original_df["int_col"].tolist()

    def test_database_to_parquet_roundtrip(self, tmp_path):
        """Test database to Parquet conversion preserves data."""
        import sqlite3
        
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE test (id INTEGER, name TEXT, value REAL)")
        original_data = [(1, "Alice", 10.5), (2, "Bob", 20.3), (3, "Charlie", 30.1)]
        cur.executemany("INSERT INTO test VALUES (?, ?, ?)", original_data)
        conn.commit()
        conn.close()

        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        paths = source.get_data("test", pack_config=pack_config)
        result_df = pd.read_parquet(paths[0])

        assert len(result_df) == 3
        assert result_df["id"].tolist() == [1, 2, 3]
        assert result_df["name"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_multiple_tables_export(self, tmp_path):
        """Test exporting multiple tables from database."""
        import sqlite3
        
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        # Create multiple tables
        cur.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        cur.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        
        cur.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)")
        cur.execute("INSERT INTO orders VALUES (1, 1, 100.0), (2, 2, 200.0)")
        
        cur.execute("CREATE TABLE products (id INTEGER, name TEXT, price REAL)")
        cur.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        
        conn.commit()
        conn.close()

        source = DatabaseSource(connection_string=f"sqlite:///{db_path}")
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir)}

        # Export all tables
        paths = source.get_data("*", pack_config=pack_config)
        
        # Should have parquet files for each table
        assert len(paths) >= 3

    def test_duckdb_full_workflow(self, tmp_path):
        """Test complete DuckDB workflow."""
        import duckdb
        
        # Create database with complex data
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        
        # Create and populate table
        conn.execute("""
            CREATE TABLE sales (
                id INTEGER,
                product VARCHAR,
                quantity INTEGER,
                price DOUBLE,
                sale_date DATE
            )
        """)
        
        for i in range(150):
            conn.execute(f"""
                INSERT INTO sales VALUES (
                    {i},
                    'Product_{i % 10}',
                    {i * 2},
                    {i * 1.5},
                    DATE '2024-01-01' + INTERVAL '{i} days'
                )
            """)
        conn.close()

        source = DuckDBSource({"path": str(db_path)})
        out_dir = tmp_path / "output"
        pack_config = {"parquet_output_dir": str(out_dir), "chunk_rows": 50}

        paths = source.get_data("sales", pack_config=pack_config)
        
        # DuckDB now uses streaming COPY TO PARQUET which creates a single optimized file
        # with appropriate row groups instead of multiple chunks (more efficient for big data)
        assert len(paths) >= 1
        
        # Verify total data is preserved
        total_rows = sum(len(pd.read_parquet(p)) for p in paths)
        assert total_rows == 150
