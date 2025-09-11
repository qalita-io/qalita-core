from .pandas_sanitization import install_pandas_parquet_sanitization, sanitize_dataframe_for_parquet
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
    "install_pandas_parquet_sanitization",
    "sanitize_dataframe_for_parquet",
    "detect_chunked_from_items",
    "normalize_and_dedupe_recommendations",
    "CompletenessAggregator",
    "OutlierAggregator",
    "DuplicateAggregator",
    "TimelinessAggregator",
]


