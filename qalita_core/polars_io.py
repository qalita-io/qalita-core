"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Polars-based I/O utilities for streaming large datasets (100GB+).

This module provides memory-efficient data loading using Polars lazy evaluation
and streaming capabilities. It replaces pandas-based loading for better performance
with large datasets.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Iterator
import logging

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore

logger = logging.getLogger(__name__)


# Default configuration
DEFAULT_CHUNK_ROWS = 100_000
DEFAULT_ROW_GROUP_SIZE = 100_000
MAX_ROWS_FOR_FULL_LOAD = 10_000_000  # 10M rows threshold for streaming


def ensure_polars() -> None:
    """Raise ImportError if polars is not available."""
    if not POLARS_AVAILABLE:
        raise ImportError(
            "polars is required for big data processing. "
            "Install with: pip install polars>=1.0.0"
        )


def scan_csv(
    file_path: str,
    *,
    skip_rows: int = 0,
    encoding: str = "utf8",
    infer_schema_length: int = 10000,
    ignore_errors: bool = True,
    **kwargs: Any,
) -> "pl.LazyFrame":
    """
    Create a lazy scan of a CSV file for streaming processing.

    This does NOT load data into memory - it creates a query plan
    that will be executed when .collect() or .sink_parquet() is called.

    Args:
        file_path: Path to the CSV file
        skip_rows: Number of rows to skip at the beginning
        encoding: File encoding (default: utf8). Must be 'utf8' or 'utf8-lossy'
        infer_schema_length: Number of rows to use for schema inference
        ignore_errors: Whether to ignore parsing errors
        **kwargs: Additional arguments passed to pl.scan_csv()

    Returns:
        LazyFrame for deferred execution
    """
    ensure_polars()
    # Normalize encoding name for Polars (utf-8 -> utf8)
    if encoding.lower() in ("utf-8", "utf_8"):
        encoding = "utf8"

    return pl.scan_csv(
        file_path,
        skip_rows=skip_rows,
        encoding=encoding,
        infer_schema_length=infer_schema_length,
        ignore_errors=ignore_errors,
        **kwargs,
    )


def scan_parquet(
    source: Union[str, List[str], Path],
    **kwargs: Any,
) -> "pl.LazyFrame":
    """
    Create a lazy scan of Parquet file(s) for streaming processing.

    Args:
        source: Path to parquet file, list of paths, or glob pattern
        **kwargs: Additional arguments passed to pl.scan_parquet()

    Returns:
        LazyFrame for deferred execution
    """
    ensure_polars()
    return pl.scan_parquet(source, **kwargs)


def scan_excel(
    file_path: str,
    *,
    sheet_name: Optional[str] = None,
    skip_rows: int = 0,
    **kwargs: Any,
) -> "pl.LazyFrame":
    """
    Read Excel file and return as LazyFrame.

    Note: Excel files cannot be truly streamed, but we minimize memory
    by reading directly into Polars format without pandas intermediate.

    Args:
        file_path: Path to the Excel file
        sheet_name: Name of sheet to read (None for first sheet)
        skip_rows: Number of rows to skip
        **kwargs: Additional arguments

    Returns:
        LazyFrame for deferred execution
    """
    ensure_polars()
    # Polars read_excel returns DataFrame, we convert to lazy
    df = pl.read_excel(
        file_path,
        sheet_name=sheet_name,
        read_options={"skip_rows": skip_rows} if skip_rows else None,
        **kwargs,
    )
    return df.lazy()


def scan_json(
    file_path: str,
    *,
    ndjson: bool = False,
    **kwargs: Any,
) -> "pl.LazyFrame":
    """
    Create a lazy scan of JSON/NDJSON file.

    Args:
        file_path: Path to the JSON file
        ndjson: If True, treat as newline-delimited JSON (streamable)
        **kwargs: Additional arguments

    Returns:
        LazyFrame for deferred execution
    """
    ensure_polars()
    if ndjson:
        return pl.scan_ndjson(file_path, **kwargs)
    else:
        # Regular JSON must be read fully, then converted to lazy
        df = pl.read_json(file_path, **kwargs)
        return df.lazy()


def stream_to_parquet(
    lf: "pl.LazyFrame",
    output_path: str,
    *,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    compression: str = "zstd",
    **kwargs: Any,
) -> str:
    """
    Stream a LazyFrame to a Parquet file using Polars streaming engine.

    This processes data in batches without loading everything into memory.

    Args:
        lf: LazyFrame to write
        output_path: Destination path for parquet file
        row_group_size: Number of rows per row group (affects memory usage)
        compression: Compression algorithm (zstd, snappy, lz4, gzip, none)
        **kwargs: Additional arguments passed to sink_parquet()

    Returns:
        Path to the written parquet file
    """
    ensure_polars()

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Use sink_parquet for streaming write
    lf.sink_parquet(
        output_path,
        row_group_size=row_group_size,
        compression=compression,
        **kwargs,
    )

    return output_path


def stream_to_parquet_chunks(
    lf: "pl.LazyFrame",
    output_dir: str,
    base_name: str,
    *,
    rows_per_chunk: int = DEFAULT_CHUNK_ROWS,
    compression: str = "zstd",
) -> List[str]:
    """
    Stream a LazyFrame to multiple Parquet chunk files.

    This is useful when downstream processing expects multiple smaller files.

    Args:
        lf: LazyFrame to write
        output_dir: Directory for output files
        base_name: Base name for chunk files (will be {base_name}_part_{n}.parquet)
        rows_per_chunk: Approximate number of rows per chunk file
        compression: Compression algorithm

    Returns:
        List of paths to written parquet files
    """
    ensure_polars()

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Get row count efficiently (streaming count)
    try:
        row_count = lf.select(pl.len()).collect(engine="streaming").item()
    except Exception:
        # Fallback: collect and count
        row_count = lf.select(pl.len()).collect().item()

    if row_count == 0:
        # Empty dataset - write single empty file
        output_path = os.path.join(output_dir, f"{base_name}_part_1.parquet")
        lf.collect().write_parquet(output_path, compression=compression)
        return [output_path]

    # Calculate number of chunks needed
    num_chunks = max(1, (row_count + rows_per_chunk - 1) // rows_per_chunk)

    if num_chunks == 1:
        # Single chunk - use streaming write
        output_path = os.path.join(output_dir, f"{base_name}_part_1.parquet")
        try:
            lf.sink_parquet(output_path, compression=compression)
        except Exception:
            # Fallback if streaming fails
            lf.collect().write_parquet(output_path, compression=compression)
        return [output_path]

    # Multiple chunks - collect and split
    # Note: For very large datasets, consider using streaming with row limits
    paths = []

    for i in range(num_chunks):
        offset = i * rows_per_chunk
        chunk_lf = lf.slice(offset, rows_per_chunk)
        output_path = os.path.join(
            output_dir, f"{base_name}_part_{i + 1}.parquet"
        )

        try:
            chunk_lf.sink_parquet(output_path, compression=compression)
        except Exception:
            # Fallback
            chunk_lf.collect().write_parquet(
                output_path, compression=compression
            )

        paths.append(output_path)

    return paths


def collect_streaming(
    lf: "pl.LazyFrame",
    *,
    streaming: bool = True,
) -> "pl.DataFrame":
    """
    Collect a LazyFrame with streaming mode when possible.

    Args:
        lf: LazyFrame to collect
        streaming: Whether to use streaming mode (recommended for large data)

    Returns:
        Collected DataFrame
    """
    ensure_polars()
    try:
        # Use engine="streaming" for Polars 1.25+
        if streaming:
            return lf.collect(engine="streaming")
        else:
            return lf.collect()
    except Exception:
        # Fallback to non-streaming if streaming fails
        logger.warning(
            "Streaming collection failed, falling back to standard collect"
        )
        return lf.collect()


def estimate_memory_usage(lf: "pl.LazyFrame") -> Dict[str, Any]:
    """
    Estimate memory usage for a LazyFrame without fully loading it.

    Args:
        lf: LazyFrame to analyze

    Returns:
        Dictionary with row count, column count, and estimated size
    """
    ensure_polars()

    # Get schema without loading data
    schema = lf.collect_schema()
    column_count = len(schema)

    # Get row count efficiently
    try:
        row_count = lf.select(pl.len()).collect(engine="streaming").item()
    except Exception:
        row_count = None

    # Estimate bytes per row based on schema
    bytes_per_row = 0
    for dtype in schema.values():
        if dtype in (pl.Int8, pl.UInt8, pl.Boolean):
            bytes_per_row += 1
        elif dtype in (pl.Int16, pl.UInt16):
            bytes_per_row += 2
        elif dtype in (pl.Int32, pl.UInt32, pl.Float32, pl.Date):
            bytes_per_row += 4
        elif dtype in (
            pl.Int64,
            pl.UInt64,
            pl.Float64,
            pl.Datetime,
            pl.Duration,
        ):
            bytes_per_row += 8
        elif dtype == pl.String:
            bytes_per_row += 50  # Estimate for variable-length strings
        else:
            bytes_per_row += 8  # Default estimate

    estimated_size_bytes = (
        row_count * bytes_per_row if row_count is not None else None
    )

    return {
        "row_count": row_count,
        "column_count": column_count,
        "bytes_per_row_estimate": bytes_per_row,
        "estimated_size_bytes": estimated_size_bytes,
        "estimated_size_mb": (
            estimated_size_bytes / (1024 * 1024)
            if estimated_size_bytes is not None
            else None
        ),
        "schema": {str(k): str(v) for k, v in schema.items()},
    }


def should_use_streaming(
    lf: "pl.LazyFrame",
    threshold_rows: int = MAX_ROWS_FOR_FULL_LOAD,
) -> bool:
    """
    Determine if streaming mode should be used based on data size.

    Args:
        lf: LazyFrame to check
        threshold_rows: Row count threshold for streaming recommendation

    Returns:
        True if streaming is recommended
    """
    ensure_polars()

    try:
        row_count = lf.select(pl.len()).collect(engine="streaming").item()
        return row_count > threshold_rows
    except Exception:
        # If we can't count, assume streaming is needed
        return True


def read_database_streaming(
    query: str,
    connection_string: str,
    *,
    chunk_size: int = DEFAULT_CHUNK_ROWS,
) -> "pl.LazyFrame":
    """
    Read from database with efficient memory usage.

    Args:
        query: SQL query to execute
        connection_string: Database connection string
        chunk_size: Not used directly (Polars handles batching internally)

    Returns:
        LazyFrame with query results
    """
    ensure_polars()

    # Polars read_database returns DataFrame, convert to lazy
    df = pl.read_database(query, connection_string)
    return df.lazy()


def iter_parquet_row_groups(
    file_path: str,
) -> Iterator["pl.DataFrame"]:
    """
    Iterate over row groups in a Parquet file for memory-efficient processing.

    Args:
        file_path: Path to parquet file

    Yields:
        DataFrame for each row group
    """
    ensure_polars()

    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(file_path)

    for i in range(parquet_file.metadata.num_row_groups):
        table = parquet_file.read_row_group(i)
        yield pl.from_arrow(table)


def polars_to_pandas(
    lf_or_df: Union["pl.LazyFrame", "pl.DataFrame"],
    *,
    streaming: bool = True,
) -> "Any":
    """
    Convert Polars LazyFrame/DataFrame to pandas DataFrame.

    Use this for backward compatibility with pandas-based code.

    Args:
        lf_or_df: Polars LazyFrame or DataFrame
        streaming: Use streaming collection for LazyFrames

    Returns:
        pandas DataFrame
    """
    ensure_polars()

    if isinstance(lf_or_df, pl.LazyFrame):
        df = collect_streaming(lf_or_df, streaming=streaming)
    else:
        df = lf_or_df

    return df.to_pandas()


def pandas_to_polars(
    pdf: "Any",
) -> "pl.DataFrame":
    """
    Convert pandas DataFrame to Polars DataFrame.

    Args:
        pdf: pandas DataFrame

    Returns:
        Polars DataFrame
    """
    ensure_polars()
    return pl.from_pandas(pdf)
