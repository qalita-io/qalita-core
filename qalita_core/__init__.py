from .pandas_sanitization import (
    install_pandas_parquet_sanitization,
    sanitize_dataframe_for_parquet,
)

# Polars-based streaming I/O for big data (100GB+)
try:
    from .polars_io import (
        POLARS_AVAILABLE,
        scan_csv,
        scan_parquet,
        scan_excel,
        scan_json,
        stream_to_parquet,
        stream_to_parquet_chunks,
        collect_streaming,
        estimate_memory_usage,
        should_use_streaming,
        read_database_streaming,
        iter_parquet_row_groups,
        polars_to_pandas,
        pandas_to_polars,
    )
except ImportError:
    POLARS_AVAILABLE = False
    scan_csv = None  # type: ignore
    scan_parquet = None  # type: ignore
    scan_excel = None  # type: ignore
    scan_json = None  # type: ignore
    stream_to_parquet = None  # type: ignore
    stream_to_parquet_chunks = None  # type: ignore
    collect_streaming = None  # type: ignore
    estimate_memory_usage = None  # type: ignore
    should_use_streaming = None  # type: ignore
    read_database_streaming = None  # type: ignore
    iter_parquet_row_groups = None  # type: ignore
    polars_to_pandas = None  # type: ignore
    pandas_to_polars = None  # type: ignore

try:
    # Optional aggregation helpers for packs
    from .aggregation import (
        detect_chunked_from_items,
        normalize_and_dedupe_recommendations,
        CompletenessAggregator,
        OutlierAggregator,
        DuplicateAggregator,
        TimelinessAggregator,
    )
except Exception:
    # Keep core importable even if optional module not present in installed version
    detect_chunked_from_items = None  # type: ignore
    normalize_and_dedupe_recommendations = None  # type: ignore
    CompletenessAggregator = None  # type: ignore
    OutlierAggregator = None  # type: ignore
    DuplicateAggregator = None  # type: ignore
    TimelinessAggregator = None  # type: ignore

# Install the sanitizing parquet hook at import time
install_pandas_parquet_sanitization()

__all__ = [
    # Pandas sanitization (legacy)
    "install_pandas_parquet_sanitization",
    "sanitize_dataframe_for_parquet",
    # Polars streaming I/O (recommended for big data)
    "POLARS_AVAILABLE",
    "scan_csv",
    "scan_parquet",
    "scan_excel",
    "scan_json",
    "stream_to_parquet",
    "stream_to_parquet_chunks",
    "collect_streaming",
    "estimate_memory_usage",
    "should_use_streaming",
    "read_database_streaming",
    "iter_parquet_row_groups",
    "polars_to_pandas",
    "pandas_to_polars",
    # Aggregation helpers
    "detect_chunked_from_items",
    "normalize_and_dedupe_recommendations",
    "CompletenessAggregator",
    "OutlierAggregator",
    "DuplicateAggregator",
    "TimelinessAggregator",
]
