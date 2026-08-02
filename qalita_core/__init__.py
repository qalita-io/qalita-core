"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Qalita Core — multi-source loading, Parquet materialization and streaming
analytics for Qalita packs.

The public surface is deliberately small:

- :class:`qalita_core.pack.Pack` — configuration, loading, and the ``scan()``
  front door that hands a pack a Polars LazyFrame.
- :mod:`qalita_core.analytics` — the streaming primitives a pack computes with.
- :mod:`qalita_core.aggregation` — cross-pack metric accumulators.
- :mod:`qalita_core.figures` — bounded chart payloads.

pandas is NOT imported by importing this package. It is an optional extra
(``qalita-core[pandas]``) needed only by packs that wrap a pandas-only third
party. Everything on the hot path is Polars.
"""

from typing import Any

from .analytics import (
    StreamingCollectError,
    agg,
    approx_n_unique,
    failures,
    numeric_columns,
    quantiles,
    row_count,
    sample,
    sink,
    string_columns,
    temporal_columns,
    top_k,
    value_counts,
)
from .aggregation import (
    detect_chunked_from_items,
    normalize_and_dedupe_recommendations,
    CompletenessAggregator,
    OutlierAggregator,
    DuplicateAggregator,
    TimelinessAggregator,
)

POLARS_AVAILABLE = True

__all__ = [
    # Streaming analytics — what packs compute with
    "StreamingCollectError",
    "agg",
    "approx_n_unique",
    "failures",
    "numeric_columns",
    "quantiles",
    "row_count",
    "sample",
    "sink",
    "string_columns",
    "temporal_columns",
    "top_k",
    "value_counts",
    # Aggregation helpers
    "detect_chunked_from_items",
    "normalize_and_dedupe_recommendations",
    "CompletenessAggregator",
    "OutlierAggregator",
    "DuplicateAggregator",
    "TimelinessAggregator",
    # Legacy pandas compatibility (resolved lazily, see __getattr__)
    "install_pandas_parquet_sanitization",
    "sanitize_dataframe_for_parquet",
    "POLARS_AVAILABLE",
]


_PANDAS_COMPAT = {
    "install_pandas_parquet_sanitization",
    "sanitize_dataframe_for_parquet",
}


def __getattr__(name: str) -> Any:
    """Resolve the pandas compatibility helpers on first use.

    Importing them eagerly would drag pandas into every pack process, including
    the ones that no longer touch it — and the sanitizing hook they install
    copies the frame on every ``to_parquet``, which doubles peak RAM on exactly
    the writes this package exists to keep small.
    """
    if name in _PANDAS_COMPAT:
        from . import pandas_sanitization

        return getattr(pandas_sanitization, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
