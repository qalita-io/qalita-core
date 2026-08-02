"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Streaming analytics primitives for packs.

This is the only sanctioned way a pack computes anything over source data. It
exists to make the 100 GiB / 16 GB target structurally reachable rather than
merely intended, and it enforces three invariants:

1. Every collect goes through ``engine="streaming"`` and RAISES on failure.
   There is no in-memory fallback. A streaming failure on a 100 GiB source is
   precisely the case where the in-memory engine cannot succeed, so falling
   back to it converts a clear error into an OOM kill.

2. Every function that returns ROWS is bounded by construction. There is no way
   to ask this module for an unbounded row set.

3. Packs never see a file path, a pandas DataFrame, or a chunk. They see
   LazyFrames in, and small results out.

The workhorse is :func:`agg`. A lazy port that loops ``for column in columns``
re-scans the whole source once per column; batching every expression into a
single :func:`agg` call turns that into one pass returning one row.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import polars as pl

__all__ = [
    "StreamingCollectError",
    "agg",
    "approx_n_unique",
    "failures",
    "quantiles",
    "row_count",
    "sample",
    "sink",
    "top_k",
    "value_counts",
]


# Default number of histogram buckets used for approximate quantiles. The
# absolute error on a quantile is bounded by (max - min) / DEFAULT_QUANTILE_BINS
# and memory is O(bins) per column regardless of the row count.
DEFAULT_QUANTILE_BINS = 10_000

# Default bound for row-returning helpers.
DEFAULT_ROW_LIMIT = 1_000

# Default bound for sampling.
DEFAULT_SAMPLE_ROWS = 100_000


class StreamingCollectError(RuntimeError):
    """A streaming collect failed.

    Deliberately not caught anywhere in qalita_core: retrying in memory is what
    turns a diagnosable error into an OOM kill.
    """


def _collect(lf: "pl.LazyFrame") -> "pl.DataFrame":
    """Collect a LazyFrame with the streaming engine, or raise.

    Every materialization in this module goes through here, which is what makes
    invariant (1) an enforced property rather than a convention.
    """
    try:
        return lf.collect(engine="streaming")
    except Exception as exc:  # noqa: BLE001 - re-raised with context below
        raise StreamingCollectError(
            f"streaming collect failed: {exc}. This is not retried with the "
            f"in-memory engine on purpose: on a large source the in-memory "
            f"engine cannot succeed where the streaming engine failed."
        ) from exc


def _as_lazy(frame: "pl.LazyFrame | pl.DataFrame") -> "pl.LazyFrame":
    if isinstance(frame, pl.LazyFrame):
        return frame
    if isinstance(frame, pl.DataFrame):
        return frame.lazy()
    raise TypeError(
        f"expected a polars LazyFrame or DataFrame, got {type(frame).__name__}"
    )


def row_count(lf: "pl.LazyFrame | pl.DataFrame") -> int:
    """Exact row count in one streaming pass.

    On Parquet inputs Polars answers this from the file footers without reading
    a single data page.
    """
    return int(_collect(_as_lazy(lf).select(pl.len())).item())


def agg(
    lf: "pl.LazyFrame | pl.DataFrame",
    exprs: Mapping[str, "pl.Expr"],
) -> dict[str, Any]:
    """Evaluate many aggregate expressions in ONE streaming pass.

    This is the primitive every pack should reach for first. Build every metric
    for every column into a single mapping, call this once, and the source is
    read exactly once no matter how many statistics come out.

    Args:
        lf: LazyFrame (or DataFrame) to aggregate.
        exprs: ``{result_name: aggregate_expression}``. Each expression must
            reduce to a single value.

    Returns:
        ``{result_name: python_scalar}``. Empty mapping in, empty dict out.

    Example:
        >>> agg(lf, {"rows": pl.len(),
        ...          **{f"{c}__nulls": pl.col(c).null_count() for c in cols}})
    """
    if not exprs:
        return {}

    names = list(exprs.keys())
    frame = _collect(
        _as_lazy(lf).select([exprs[name].alias(name) for name in names])
    )
    if frame.height == 0:
        return {name: None for name in names}
    row = frame.row(0)
    return dict(zip(names, row))


def sample(
    lf: "pl.LazyFrame | pl.DataFrame",
    n: int = DEFAULT_SAMPLE_ROWS,
    *,
    seed: int = 0,
    method: str = "reservoir",
    total_rows: int | None = None,
) -> "pl.DataFrame":
    """Return at most ``n`` rows, drawn from the WHOLE dataset by default.

    ``method="reservoir"`` (the default) hashes a row index to draw a uniform
    pseudo-random sample in a single streaming pass. It is deterministic for a
    given ``seed``.

    ``method="head"`` takes the first ``n`` rows and must be requested
    explicitly. It is not a sample: on a chunked source it reads only the first
    parts, so every distribution derived from it describes one partition rather
    than the dataset. It exists for previews, not for statistics.

    The return type is eager on purpose: a bounded row count is what makes the
    memory bounded.
    """
    if n <= 0:
        raise ValueError(f"sample size must be positive, got {n}")

    lazy = _as_lazy(lf)

    if method == "head":
        return _collect(lazy.head(n))
    if method != "reservoir":
        raise ValueError(
            f"unknown sample method {method!r}, expected 'reservoir' or 'head'"
        )

    total = row_count(lazy) if total_rows is None else int(total_rows)
    if total <= n:
        return _collect(lazy)

    # Keep the draw spread across every part file rather than the first ones:
    # hash a row index, which is uniform, and admit rows below a threshold.
    # Oversample slightly so the draw is very unlikely to fall short, then trim
    # with an eager uniform sample — trimming with head() would truncate the
    # tail of the dataset and bias every statistic derived from the result.
    fraction = min(1.0, (n / total) * 1.2)
    threshold = int(fraction * (2**32))
    index = "__qalita_row_index"
    drawn = _collect(
        lazy.with_row_index(index)
        .filter((pl.col(index).hash(seed=seed) % (2**32)) < threshold)
        .drop(index)
    )
    if drawn.height <= n:
        return drawn
    return drawn.sample(n, seed=seed, shuffle=False)


def approx_n_unique(
    lf: "pl.LazyFrame | pl.DataFrame",
    columns: Sequence[str],
    *,
    exact: bool = False,
) -> dict[str, int]:
    """Distinct-value counts for many columns in one streaming pass.

    Approximate by default: Polars' HyperLogLog uses memory independent of
    cardinality. ``exact=True`` costs O(cardinality) memory — on a primary key
    that is one entry per row, which is exactly the case that cannot fit. It is
    allowed, but you have to ask.
    """
    if not columns:
        return {}
    op = "n_unique" if exact else "approx_n_unique"
    result = agg(
        lf,
        {col: getattr(pl.col(col), op)() for col in columns},
    )
    return {
        col: int(value) if value is not None else 0
        for col, value in result.items()
    }


def quantiles(
    lf: "pl.LazyFrame | pl.DataFrame",
    columns: Sequence[str],
    qs: Sequence[float],
    *,
    exact: bool = False,
    bins: int = DEFAULT_QUANTILE_BINS,
) -> dict[str, dict[float, float]]:
    """Quantiles for many columns and many probabilities.

    Approximate by default via a fixed-width histogram: two streaming passes
    (min/max, then bucket counts) and O(bins) memory per column, whatever the
    row count. The absolute error on any quantile is bounded by the bucket
    width, ``(max - min) / bins``.

    ``exact=True`` delegates to Polars' exact quantile, which needs to order the
    column. The streaming engine spills to disk rather than to RAM, so it stays
    within the memory budget, but it is markedly slower and needs scratch space.

    Returns:
        ``{column: {q: value}}``. Columns that are entirely null are omitted.
    """
    if not columns or not qs:
        return {}

    probabilities = [float(q) for q in qs]
    for q in probabilities:
        if not 0.0 <= q <= 1.0:
            raise ValueError(f"quantile must be within [0, 1], got {q}")

    if exact:
        flat = agg(
            lf,
            {
                f"{i}|{j}": pl.col(col).quantile(q)
                for i, col in enumerate(columns)
                for j, q in enumerate(probabilities)
            },
        )
        out: dict[str, dict[float, float]] = {}
        for i, col in enumerate(columns):
            values = {
                probabilities[j]: flat[f"{i}|{j}"]
                for j in range(len(probabilities))
                if flat.get(f"{i}|{j}") is not None
            }
            if values:
                out[col] = {q: float(v) for q, v in values.items()}
        return out

    return _histogram_quantiles(_as_lazy(lf), columns, probabilities, bins)


def _histogram_quantiles(
    lf: "pl.LazyFrame",
    columns: Sequence[str],
    probabilities: Sequence[float],
    bins: int,
) -> dict[str, dict[float, float]]:
    """Approximate quantiles from a fixed-width histogram, O(bins) per column."""
    if bins < 2:
        raise ValueError(f"bins must be at least 2, got {bins}")

    bounds = agg(
        lf,
        {
            **{f"min|{i}": pl.col(col).min() for i, col in enumerate(columns)},
            **{f"max|{i}": pl.col(col).max() for i, col in enumerate(columns)},
            **{
                f"cnt|{i}": pl.col(col).count()
                for i, col in enumerate(columns)
            },
        },
    )

    # Bucket every column in one pass. Constant columns and all-null columns are
    # resolved from the bounds alone and never reach the histogram.
    bucket_exprs: dict[str, pl.Expr] = {}
    resolved: dict[str, dict[float, float]] = {}
    widths: dict[str, tuple[float, float]] = {}

    for i, col in enumerate(columns):
        lo, hi = bounds.get(f"min|{i}"), bounds.get(f"max|{i}")
        count = bounds.get(f"cnt|{i}") or 0
        if lo is None or hi is None or count == 0:
            continue
        lo, hi = float(lo), float(hi)
        if hi <= lo:
            resolved[col] = {q: lo for q in probabilities}
            continue
        width = (hi - lo) / bins
        widths[col] = (lo, width)
        bucket_exprs[f"b|{i}"] = (
            ((pl.col(col).cast(pl.Float64) - lo) / width)
            .floor()
            .clip(0, bins - 1)
            .cast(pl.Int32)
        )

    if not bucket_exprs:
        return resolved

    for i, col in enumerate(columns):
        alias = f"b|{i}"
        if alias not in bucket_exprs:
            continue
        counts = _collect(
            lf.select(bucket_exprs[alias].alias("bucket"))
            .drop_nulls()
            .group_by("bucket")
            .agg(pl.len().alias("n"))
            .sort("bucket")
        )
        resolved[col] = _quantiles_from_histogram(
            counts, widths[col], probabilities
        )

    return resolved


def _quantiles_from_histogram(
    counts: "pl.DataFrame",
    bounds: tuple[float, float],
    probabilities: Sequence[float],
) -> dict[float, float]:
    """Interpolate quantiles from a bucket-count table (at most ``bins`` rows)."""
    lo, width = bounds
    buckets: list[int] = counts["bucket"].to_list()
    ns: list[int] = counts["n"].to_list()
    total = sum(ns)
    if total == 0:
        return {}

    out: dict[float, float] = {}
    for q in probabilities:
        target = q * total
        cumulative = 0
        position = len(buckets) - 1
        for position, n in enumerate(ns):
            if cumulative + n >= target:
                break
            cumulative += n
        # Interpolate inside the chosen bucket rather than snapping to its edge.
        span = ns[position]
        offset = ((target - cumulative) / span) if span else 0.0
        out[q] = lo + (buckets[position] + min(max(offset, 0.0), 1.0)) * width
    return out


def top_k(
    lf: "pl.LazyFrame | pl.DataFrame",
    by: str,
    k: int = 50,
    *,
    descending: bool = True,
    other: bool = False,
) -> "pl.DataFrame":
    """The ``k`` rows of an aggregate frame with the largest ``by`` value.

    The ordering happens inside the engine, so the caller never materializes the
    full frame. When ``other`` is set, the tail is folded into a single extra
    row whose ``by`` value is the sum of everything not in the top ``k`` and
    whose other columns are null.
    """
    if k <= 0:
        raise ValueError(f"k must be positive, got {k}")

    lazy = _as_lazy(lf)
    ranked = lazy.sort(by, descending=descending, nulls_last=True)
    head = _collect(ranked.head(k))

    if not other:
        return head

    tail_total = _collect(ranked.slice(k).select(pl.col(by).sum().alias(by)))
    if tail_total.height == 0 or tail_total[by][0] in (None, 0):
        return head

    filler = {name: [None] for name in head.columns if name != by}
    filler[by] = [tail_total[by][0]]
    tail_row = pl.DataFrame(
        filler, schema={c: head.schema[c] for c in head.columns}
    )
    return pl.concat(
        [head, tail_row.select(head.columns)], how="vertical_relaxed"
    )


def value_counts(
    lf: "pl.LazyFrame | pl.DataFrame",
    column: str,
    k: int = 50,
    *,
    other: bool = False,
) -> "pl.DataFrame":
    """The ``k`` most frequent values of ``column``, with their counts.

    Replaces the full ``value_counts`` / ``.unique()`` calls that build one
    Python entry per distinct value. The grouping runs inside the streaming
    engine, which spills group state to disk instead of RAM.

    Returns a frame with columns ``[column, "count"]``, at most ``k`` rows
    (``k + 1`` when ``other`` is set and the tail is non-empty).
    """
    grouped = _as_lazy(lf).group_by(column).agg(pl.len().alias("count"))
    return top_k(grouped, "count", k, other=other)


def failures(
    lf: "pl.LazyFrame | pl.DataFrame",
    predicate: "pl.Expr",
    *,
    limit: int = DEFAULT_ROW_LIMIT,
    columns: Sequence[str] | None = None,
) -> tuple[int, "pl.DataFrame"]:
    """Count failing rows exactly, and return at most ``limit`` of them.

    The count never materializes a row; the examples are capped inside the lazy
    plan, so the cap holds however many rows fail. This is the only sanctioned
    way for a pack to emit row-level evidence, which makes an unbounded export
    structurally impossible.

    Args:
        lf: data to check.
        predicate: boolean expression that is true for a FAILING row. Nulls are
            treated as passing.
        limit: maximum number of example rows. ``0`` returns the count only.
        columns: restrict the example rows to these columns.

    Returns:
        ``(exact_failing_row_count, example_rows)``.
    """
    lazy = _as_lazy(lf)
    failing = predicate.fill_null(False)

    count = int(agg(lazy, {"n": failing.sum()})["n"] or 0)

    if limit <= 0 or count == 0:
        empty = lazy.head(0)
        if columns:
            empty = empty.select(columns)
        return count, _collect(empty)

    examples = lazy.filter(failing)
    if columns:
        examples = examples.select(columns)
    return count, _collect(examples.head(limit))


def sink(
    lf: "pl.LazyFrame | pl.DataFrame",
    path: str,
    *,
    compression: str = "zstd",
    row_group_size: int | None = None,
    max_rows_per_file: int | None = None,
) -> list[str]:
    """Write a LazyFrame to Parquet without materializing it.

    ``max_rows_per_file`` splits the output in a SINGLE pass using Polars'
    partitioned sink. The previous helper re-executed the whole query plan once
    per slice, so writing N parts meant N full scans of the source.

    Returns the list of files written.
    """
    from pathlib import Path

    target = Path(path)

    if max_rows_per_file:
        target.mkdir(parents=True, exist_ok=True)
        _as_lazy(lf).sink_parquet(
            pl.PartitionMaxSize(
                str(target / "part-{part}.parquet"),
                max_size=int(max_rows_per_file),
            ),
            compression=compression,
        )
        return sorted(str(p) for p in target.glob("part-*.parquet"))

    target.parent.mkdir(parents=True, exist_ok=True)
    kwargs: dict[str, Any] = {"compression": compression}
    if row_group_size:
        kwargs["row_group_size"] = int(row_group_size)
    _as_lazy(lf).sink_parquet(str(target), **kwargs)
    return [str(target)]


def numeric_columns(schema: Mapping[str, Any]) -> list[str]:
    """Column names with a numeric dtype, from a schema mapping.

    Derived from Parquet footers via ``Pack.schema()``, so this costs no data
    read at all — unlike the ``select_dtypes`` calls it replaces, which needed
    the frame in memory first.
    """
    return [name for name, dtype in schema.items() if dtype.is_numeric()]


def string_columns(schema: Mapping[str, Any]) -> list[str]:
    """Column names with a string dtype, from a schema mapping."""
    return [name for name, dtype in schema.items() if dtype == pl.String]


def temporal_columns(schema: Mapping[str, Any]) -> list[str]:
    """Column names with a date/datetime dtype, from a schema mapping."""
    return [name for name, dtype in schema.items() if dtype.is_temporal()]


def batched(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    """Yield ``items`` in lists of at most ``size``.

    Used to keep a single :func:`agg` call from growing an unbounded expression
    list on very wide tables.
    """
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
