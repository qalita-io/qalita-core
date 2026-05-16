"""
Tests for Polars streaming functionality for big data (100GB+).

These tests validate that the streaming capabilities work correctly
without loading entire datasets into memory.
"""

import pytest
import os
import tempfile
from pathlib import Path

# Skip all tests if polars is not available
pytest.importorskip("polars")

import polars as pl
import pandas as pd


@pytest.fixture
def temp_parquet_dir():
    """Create temporary directory for test parquet files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_parquet_file(temp_parquet_dir):
    """Create a sample parquet file for testing."""
    # Create sample data
    n_rows = 100_000
    df = pl.DataFrame(
        {
            "id": range(n_rows),
            "value": [f"value_{i}" for i in range(n_rows)],
            "amount": [float(i) * 1.5 for i in range(n_rows)],
            "category": [f"cat_{i % 10}" for i in range(n_rows)],
        }
    )

    path = os.path.join(temp_parquet_dir, "test_data.parquet")
    df.write_parquet(path)
    return path


@pytest.fixture
def chunked_parquet_files(temp_parquet_dir):
    """Create multiple parquet chunk files for testing."""
    paths = []
    for i in range(3):
        df = pl.DataFrame(
            {
                "id": range(i * 10000, (i + 1) * 10000),
                "value": [
                    f"value_{j}" for j in range(i * 10000, (i + 1) * 10000)
                ],
                "amount": [
                    float(j) * 1.5 for j in range(i * 10000, (i + 1) * 10000)
                ],
            }
        )
        path = os.path.join(
            temp_parquet_dir, f"test_data_part_{i + 1}.parquet"
        )
        df.write_parquet(path)
        paths.append(path)
    return paths


class TestPolarsIO:
    """Test qalita_core.polars_io module."""

    def test_scan_csv_creates_lazyframe(self, temp_parquet_dir):
        """Test that scan_csv returns a LazyFrame without loading data."""
        from qalita_core.polars_io import scan_csv

        # Create a test CSV
        csv_path = os.path.join(temp_parquet_dir, "test.csv")
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df.write_csv(csv_path)

        lf = scan_csv(csv_path)
        assert isinstance(lf, pl.LazyFrame)

        # Verify data when collected
        result = lf.collect()
        assert len(result) == 3

    def test_scan_parquet_creates_lazyframe(self, sample_parquet_file):
        """Test that scan_parquet returns a LazyFrame."""
        from qalita_core.polars_io import scan_parquet

        lf = scan_parquet(sample_parquet_file)
        assert isinstance(lf, pl.LazyFrame)

        # Verify schema without loading data
        schema = lf.collect_schema()
        assert "id" in schema
        assert "value" in schema

    def test_scan_parquet_multiple_files(self, chunked_parquet_files):
        """Test scanning multiple parquet files."""
        from qalita_core.polars_io import scan_parquet

        lf = scan_parquet(chunked_parquet_files)

        # Count rows using streaming
        count = lf.select(pl.len()).collect(engine="streaming").item()
        assert count == 30000  # 3 files * 10000 rows

    def test_stream_to_parquet(self, temp_parquet_dir):
        """Test streaming write to parquet."""
        from qalita_core.polars_io import stream_to_parquet

        # Create a LazyFrame
        lf = pl.LazyFrame(
            {
                "x": range(1000),
                "y": [f"val_{i}" for i in range(1000)],
            }
        )

        output_path = os.path.join(temp_parquet_dir, "output.parquet")
        result_path = stream_to_parquet(lf, output_path)

        assert os.path.exists(result_path)

        # Verify written data
        read_df = pl.read_parquet(result_path)
        assert len(read_df) == 1000

    def test_estimate_memory_usage(self, sample_parquet_file):
        """Test memory usage estimation."""
        from qalita_core.polars_io import estimate_memory_usage, scan_parquet

        lf = scan_parquet(sample_parquet_file)
        estimate = estimate_memory_usage(lf)

        assert "row_count" in estimate
        assert estimate["row_count"] == 100_000
        assert "estimated_size_mb" in estimate
        assert estimate["estimated_size_mb"] > 0

    def test_should_use_streaming(self, sample_parquet_file):
        """Test streaming recommendation based on data size."""
        from qalita_core.polars_io import should_use_streaming, scan_parquet

        lf = scan_parquet(sample_parquet_file)

        # 100K rows should not trigger streaming by default
        assert not should_use_streaming(lf, threshold_rows=200_000)

        # But should trigger with lower threshold
        assert should_use_streaming(lf, threshold_rows=50_000)


class TestCompletenessAggregatorPolars:
    """Test CompletenessAggregator with Polars."""

    def test_add_lf_streaming(self, sample_parquet_file):
        """Test adding LazyFrame to CompletenessAggregator."""
        from qalita_core.aggregation import CompletenessAggregator
        from qalita_core.polars_io import scan_parquet

        agg = CompletenessAggregator()
        lf = scan_parquet(sample_parquet_file)

        agg.add_lf(lf, streaming=True)

        assert agg.total_rows == 100_000
        assert len(agg.unique_columns) == 4  # id, value, amount, category

    def test_add_method_auto_detection(self, sample_parquet_file):
        """Test auto-detection of data type in add() method."""
        from qalita_core.aggregation import CompletenessAggregator
        from qalita_core.polars_io import scan_parquet

        agg = CompletenessAggregator()
        lf = scan_parquet(sample_parquet_file)

        # Should auto-detect LazyFrame
        agg.add(lf)

        assert agg.total_rows == 100_000


class TestDuplicateAggregatorPolars:
    """Test DuplicateAggregator with Polars."""

    def test_add_lf_streaming(self, temp_parquet_dir):
        """Test duplicate detection with LazyFrame."""
        from qalita_core.aggregation import DuplicateAggregator

        # Create data with known duplicates
        df = pl.DataFrame(
            {
                "key1": ["a", "b", "a", "c", "b", "a"],
                "key2": [1, 2, 1, 3, 2, 1],
                "value": [10, 20, 30, 40, 50, 60],
            }
        )
        path = os.path.join(temp_parquet_dir, "dups.parquet")
        df.write_parquet(path)

        lf = pl.scan_parquet(path)
        agg = DuplicateAggregator(["key1", "key2"])

        agg.add_lf(lf, streaming=True)

        assert agg.total_rows == 6
        # Should have 3 unique combinations, with duplicates
        dup_keys = agg.get_duplicate_keys()
        assert len(dup_keys) > 0


class TestDataSourceOpenerPolars:
    """Test data_source_opener Polars integration."""

    def test_file_source_uses_polars_for_large_csv(self, temp_parquet_dir):
        """Test that FileSource uses Polars for large files."""
        from qalita_core.data_source_opener import FileSource, POLARS_AVAILABLE

        if not POLARS_AVAILABLE:
            pytest.skip("Polars not available")

        # Create a CSV file
        csv_path = os.path.join(temp_parquet_dir, "large.csv")
        df = pl.DataFrame(
            {
                "id": range(1000),
                "value": [f"val_{i}" for i in range(1000)],
            }
        )
        df.write_csv(csv_path)

        source = FileSource(csv_path)
        paths = source.get_data(
            pack_config={
                "parquet_output_dir": temp_parquet_dir,
                "chunk_rows": 500,
                "use_polars": True,  # Force Polars
            }
        )

        assert len(paths) >= 1
        assert all(p.endswith(".parquet") for p in paths)

        # Verify data integrity
        result_lf = pl.scan_parquet(paths)
        count = result_lf.select(pl.len()).collect().item()
        assert count == 1000


class TestPackPolarsIntegration:
    """Test Pack class Polars integration."""

    def test_scan_data_returns_lazyframe(self, temp_parquet_dir, monkeypatch):
        """Test Pack.scan_data() returns a LazyFrame."""
        from qalita_core.pack import Pack, POLARS_AVAILABLE

        if not POLARS_AVAILABLE:
            pytest.skip("Polars not available")

        # Create test parquet file
        parquet_path = os.path.join(temp_parquet_dir, "source.parquet")
        df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df.write_parquet(parquet_path)

        # Create minimal config files
        source_conf = {
            "name": "test",
            "type": "file",
            "config": {"path": parquet_path},
        }
        pack_conf = {"job": {}}

        import json

        source_conf_path = os.path.join(temp_parquet_dir, "source_conf.json")
        pack_conf_path = os.path.join(temp_parquet_dir, "pack_conf.json")

        with open(source_conf_path, "w") as f:
            json.dump(source_conf, f)
        with open(pack_conf_path, "w") as f:
            json.dump(pack_conf, f)

        # Create Pack instance
        pack = Pack(
            configs={
                "source_conf": source_conf_path,
                "pack_conf": pack_conf_path,
                "target_conf": source_conf_path,  # Use same for target
            }
        )

        pack.paths_source = [parquet_path]

        # Test scan_data
        lf = pack.scan_data("source")
        assert isinstance(lf, pl.LazyFrame)

        # Verify data
        result = lf.collect()
        assert len(result) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
