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
    "COUNT_COLUMN",
    "CardinalityTooHigh",
    "StreamingCollectError",
    "agg",
    "approx_n_unique",
    "estimate_groups",
    "failures",
    "quantiles",
    "row_count",
    "sample",
    "sink",
    "skew_kurtosis",
    "top_k",
    "value_counts",
]


# Histogram buckets used for approximate quantiles. The absolute error on a
# quantile is bounded by (max - min) / DEFAULT_QUANTILE_BINS.
#
# 1000, not more: the bucket counting is a group_by, and past roughly a thousand
# groups this Polars build stops keeping the aggregation bounded and its cost
# starts tracking the row count instead. Measured over a 160M-row source
# (320M in brackets):
#
#     bins =    512 ->     1 MiB (    1 MiB)
#     bins =  1,000 ->     9 MiB (   12 MiB)
#     bins = 10,000 -> 1,746 MiB (3,471 MiB)
#
# So the last option buys three extra digits of quantile precision at the price
# of the memory budget.
DEFAULT_QUANTILE_BINS = 1_000

# Default bound for row-returning helpers.
DEFAULT_ROW_LIMIT = 1_000

# Default bound for sampling.
DEFAULT_SAMPLE_ROWS = 100_000

# Largest group count for which an EXACT group_by is affordable.
#
# Deliberately small, and measured rather than reasoned. Polars 1.37's streaming
# group_by does not keep memory bounded by the group count: past a low
# cardinality its cost tracks the SOURCE ROW COUNT instead. On a 160M-row source
# (doubling to 320M in brackets):
#
#     1,025 groups ->     0 MiB (    0 MiB)
#     2,000 groups ->   194 MiB (  343 MiB)
#     8,000 groups -> 1,708 MiB (3,548 MiB)
#   102,054 groups -> 3,298 MiB (6,775 MiB)
#
# Only the first row is genuinely flat, so that is where the ceiling goes.
# Above it, callers must sample rather than aggregate exactly — which is what
# qalita_core.profiling does for top values.
DEFAULT_MAX_GROUPS = 1_000

# Relative error allowed for the HyperLogLog group-count estimate when it is
# compared against DEFAULT_MAX_GROUPS. Observed within 6% on the test fixtures;
# 20% leaves room for a sketch retune without turning it into a test failure.
HLL_OVERSHOOT = 0.20

# Name of the frequency column produced by :func:`value_counts`.
#
# Prefixed rather than the obvious "count" because the group key keeps the
# user's column name, and Polars refuses a group key and an aggregate output
# that share a name: a dataset with a column literally called "count" used to
# abort the whole job. Readers must use this constant rather than a literal,
# and :func:`value_counts` renames it back to "count" whenever that is free.
COUNT_COLUMN = "__qalita_count"


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

    ``method="reservoir"`` (the default) hashes the row content to draw a
    uniform pseudo-random sample in one pass. It is deterministic for a given
    ``seed``, and unbiased across part files — verified by the mean of a
    monotonic id column landing on the dataset midpoint.

    ``method="head"`` takes the first ``n`` rows and must be requested
    explicitly. It is not a sample: on a chunked source it reads only the first
    parts, so every distribution derived from it describes one partition rather
    than the dataset. It exists for previews, not for statistics.

    MEMORY: the OUTPUT is bounded by ``n``, but the draw itself is not free on
    this Polars build. Measured drawing 1M rows, peak grew from 3.2 GiB on a
    160M-row source to 3.8 GiB on a 320M-row one — sub-linear, but not flat.
    Prefer a scalar :func:`agg` wherever a sample is not genuinely required, and
    treat this as the escape hatch for algorithms that cannot be expressed as an
    aggregation at all (a pandas-only third-party engine, a model fit).

    Identical rows share a hash, so they are drawn or skipped together. That is
    immaterial for distributions and matters only for a source that is mostly
    exact duplicates.
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
    # hash each row and admit the ones below a threshold. Hashing the CONTENT
    # rather than a row index is deliberate — with_row_index needs a global
    # counter, which this engine cannot do incrementally, and it measured 5.9 GiB
    # against 3.8 GiB for the content hash on the same 320M-row draw.
    #
    # Oversample slightly so the draw is very unlikely to fall short, then trim
    # with an eager uniform sample — trimming with head() would truncate the
    # tail of the dataset and bias every statistic derived from the result.
    fraction = min(1.0, (n / total) * 1.2)
    threshold = int(fraction * (2**32))
    columns = list(lazy.collect_schema().keys())
    drawn = _collect(
        lazy.filter((pl.struct(columns).hash(seed=seed) % (2**32)) < threshold)
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


def skew_kurtosis(
    lf: "pl.LazyFrame | pl.DataFrame",
    columns: Sequence[str],
) -> dict[str, dict[str, float]]:
    """Sample skewness and excess kurtosis, in two streaming passes.

    Polars' own ``Expr.skew`` / ``Expr.kurtosis`` are not streaming
    aggregations: they materialize the column. Measured on 160M rows, asking for
    them on four numeric columns peaked at 6.6 GiB, against 594 MiB here.

    Both statistics are functions of the central moments, so they come from a
    pass for the means followed by a pass for the centered power sums. Centering
    in the second pass rather than expanding raw power sums is what keeps this
    numerically usable: a column of large values would lose most of its
    precision to cancellation in a Sum(x^4) formulation.

    Matches ``bias=False`` (the adjusted Fisher-Pearson definitions, which is
    what pandas' ``.skew()`` / ``.kurt()`` report) to about six significant
    figures. Columns with fewer than 4 rows or zero variance are omitted.

    Returns:
        ``{column: {"skew": float, "kurtosis": float}}``.
    """
    if not columns:
        return {}

    lazy = _as_lazy(lf)
    means = agg(lazy, {col: pl.col(col).mean() for col in columns})

    usable = [c for c in columns if means.get(c) is not None]
    if not usable:
        return {}

    exprs: dict[str, pl.Expr] = {}
    for col in usable:
        centered = pl.col(col).cast(pl.Float64) - float(means[col])
        exprs[f"n|{col}"] = pl.col(col).count()
        exprs[f"m2|{col}"] = (centered**2).sum()
        exprs[f"m3|{col}"] = (centered**3).sum()
        exprs[f"m4|{col}"] = (centered**4).sum()
    sums = agg(lazy, exprs)

    out: dict[str, dict[str, float]] = {}
    for col in usable:
        n = int(sums.get(f"n|{col}") or 0)
        if n < 4:
            continue
        m2 = float(sums[f"m2|{col}"] or 0.0) / n
        m3 = float(sums[f"m3|{col}"] or 0.0) / n
        m4 = float(sums[f"m4|{col}"] or 0.0) / n
        if m2 <= 0:
            continue
        g1 = m3 / (m2**1.5)
        g2 = m4 / (m2**2) - 3.0
        out[col] = {
            "skew": g1 * ((n * (n - 1)) ** 0.5) / (n - 2),
            "kurtosis": ((n + 1) * g2 + 6) * (n - 1) / ((n - 2) * (n - 3)),
        }
    return out


def _as_float(column: str) -> "pl.Expr":
    """``column`` as Float64.

    The cast comes FIRST everywhere non-finite values are tested, because
    ``is_finite`` is not implemented for Decimal (nor for temporal dtypes) and
    :func:`numeric_columns` reports Decimal as numeric — so guarding with a
    bare ``pl.col(c).is_finite()`` would trade a NaN crash for a Decimal one.
    """
    return pl.col(column).cast(pl.Float64)


def _finite(column: str) -> "pl.Expr":
    """``column`` as Float64, with NaN and +/-Inf dropped."""
    as_float = _as_float(column)
    return as_float.filter(as_float.is_finite())


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
    whole column. Polars 1.37 has no out-of-core execution — the old
    ``polars-pipe`` engine is gone and the binary contains no spill path — so
    that ordering happens in RAM and its cost follows the row count. Use it only
    when the column is known to fit.

    NaN and +/-Inf are excluded from both paths, which is what the pandas
    implementation this replaced did (``dropna()`` before ``.quantile()``).
    They are not missing data, but they have no place on a number line: a
    single NaN would otherwise abort the histogram, and a single Inf would
    stretch the bucket width to infinity and collapse every quantile onto the
    minimum. Both paths exclude them so that flipping ``exact`` does not
    silently change the numbers.

    Returns:
        ``{column: {q: value}}``. Columns with no finite value (entirely null,
        or entirely NaN/Inf) are omitted.
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
                f"{i}|{j}": _finite(col).quantile(q)
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

    # Bounds come from the FINITE values only. Taking them raw would let a
    # single +Inf make `hi - lo` infinite, and then every finite value lands in
    # bucket 0 and every quantile collapses onto `lo`.
    bounds = agg(
        lf,
        {
            **{
                f"min|{i}": _finite(col).min() for i, col in enumerate(columns)
            },
            **{
                f"max|{i}": _finite(col).max() for i, col in enumerate(columns)
            },
            **{
                f"cnt|{i}": _finite(col).count()
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
        # The cast to Int32 is strict, and NaN has no integer image, so a
        # non-finite value has to become NULL BEFORE it reaches the cast —
        # hence the cast sits outside the when/then. The nulls are removed by
        # the drop_nulls below, which also keeps them out of the histogram
        # denominator in _quantiles_from_histogram.
        as_float = _as_float(col)
        bucket_exprs[f"b|{i}"] = (
            pl.when(as_float.is_finite())
            .then(((as_float - lo) / width).floor().clip(0, bins - 1))
            .cast(pl.Int32)
        )

    if not bucket_exprs:
        return resolved

    for i, col in enumerate(columns):
        alias = f"b|{i}"
        if alias not in bucket_exprs:
            continue
        # The sort happens AFTER collecting, on at most `bins` rows. Putting it
        # in the lazy plan instead defeats the streaming group_by: measured on a
        # 160M-row column, sorting inside the plan cost 1756 MiB against 121 MiB
        # here, and it grew with the row count rather than staying flat.
        counts = _collect(
            lf.group_by(bucket_exprs[alias].alias("bucket")).agg(
                pl.len().alias("n")
            )
        ).drop_nulls("bucket")
        resolved[col] = _quantiles_from_histogram(
            counts.sort("bucket"), widths[col], probabilities
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

    Intended for a frame that is already an aggregate — one row per group, not
    one per source row. When ``other`` is set, the tail is folded into a single
    extra row whose ``by`` value is the sum of everything not in the top ``k``
    and whose other columns are null.

    The input is materialized before the sort. Leaving the sort in the lazy plan
    would look cheaper but is not: a sort node upstream of a streaming group_by
    defeats it, and the cost then grows with the SOURCE row count rather than
    with the group count — measured at 1756 MiB against 121 MiB on a 160M-row
    column, and doubling when the source doubled. Callers that group first must
    bound the group count themselves; :func:`value_counts` does.
    """
    if k <= 0:
        raise ValueError(f"k must be positive, got {k}")

    frame = lf if isinstance(lf, pl.DataFrame) else _collect(_as_lazy(lf))
    ranked = frame.sort(by, descending=descending, nulls_last=True)
    head = ranked.head(k)

    if not other:
        return head

    tail_total = ranked.slice(k).select(pl.col(by).sum().alias(by))
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


class CardinalityTooHigh(RuntimeError):
    """A grouped aggregation was refused because it would not fit in memory.

    Raised before the work starts, from a cheap estimate, so the caller gets a
    diagnosable error instead of an OOM kill.
    """

    def __init__(self, column: str, estimated: int, limit: int) -> None:
        super().__init__(
            f"{column!r} has about {estimated:,} distinct values, above the "
            f"{limit:,} group limit. Grouping it would hold one entry per "
            f"distinct value in memory. Raise max_groups if you have the RAM, "
            f"or skip this column — the most frequent values of a near-unique "
            f"column carry no information."
        )
        self.column = column
        self.estimated = estimated
        self.limit = limit


def estimate_groups(
    lf: "pl.LazyFrame | pl.DataFrame",
    keys: str | Sequence[str],
) -> int:
    """Estimate how many groups a group_by on ``keys`` would produce.

    HyperLogLog over the keys, so this costs constant memory and one streaming
    pass whatever the cardinality. Use it to decide whether the grouped
    aggregation is affordable BEFORE running it.
    """
    if isinstance(keys, str):
        expr = pl.col(keys)
    else:
        columns = list(keys)
        if len(columns) == 1:
            expr = pl.col(columns[0])
        else:
            expr = pl.concat_str(
                [pl.col(c).cast(pl.String).fill_null("\x00") for c in columns],
                separator="\x1f",
            ).hash()
    return int(agg(lf, {"n": expr.approx_n_unique()})["n"] or 0)


def value_counts(
    lf: "pl.LazyFrame | pl.DataFrame",
    column: str,
    k: int = 50,
    *,
    other: bool = False,
    max_groups: int | None = DEFAULT_MAX_GROUPS,
) -> "pl.DataFrame":
    """The ``k`` most frequent values of ``column``, with their counts.

    The RESULT is bounded by ``k``, but the group table that produces it is not:
    a group_by holds one entry per distinct value, and Polars' streaming engine
    does not spill it. Measured on 160M rows, grouping a column with one
    distinct value per row peaked at 9.7 GiB; grouping a 1000-value column
    peaked at 74 MiB. Cardinality, not row count, is what costs memory here.

    So the cardinality is estimated first — constant memory, one pass — and a
    column above ``max_groups`` raises :class:`CardinalityTooHigh` instead of
    being attempted. Pass ``max_groups=None`` to skip the guard when you already
    know the column is coarse.

    Returns a frame with columns ``[column, "count"]``, at most ``k`` rows
    (``k + 1`` when ``other`` is set and the tail is non-empty). When
    ``column`` is itself called ``"count"`` the frequency column keeps its
    reserved name :data:`COUNT_COLUMN` instead — Polars cannot hold two
    columns of the same name, so one of them has to give. The frequency is
    always the SECOND column; read it positionally rather than by name.
    """
    lazy = _as_lazy(lf)
    if max_groups is not None:
        estimated = estimate_groups(lazy, column)
        # The estimate is a HyperLogLog sketch and overshoots by a few percent,
        # so comparing it raw would refuse a column that sits exactly on the
        # limit. The slack is the sketch's error, not extra memory budget.
        if estimated > max_groups * (1 + HLL_OVERSHOOT):
            raise CardinalityTooHigh(column, estimated, max_groups)

    # Group and sort under a name the group key cannot already have. The
    # grouped frame holds exactly [key, aggregate], so avoiding `column` is
    # enough — and it has to be avoided, because Polars refuses two columns of
    # the same name and a dataset is allowed to call a column anything.
    alias = COUNT_COLUMN if column != COUNT_COLUMN else COUNT_COLUMN + "_"
    grouped = lazy.group_by(column).agg(pl.len().alias(alias))
    ranked = top_k(grouped, alias, k, other=other)
    if column == "count":
        return ranked
    return ranked.rename({alias: "count"})


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
        # PartitionBy superseded PartitionMaxSize in Polars 1.36.1; keep working
        # on both so the pinned floor (>=1.0) stays honest.
        if hasattr(pl, "PartitionBy"):
            partitioning: Any = pl.PartitionBy(
                str(target),
                max_rows_per_file=int(max_rows_per_file),
                # Let the row count alone decide the split, rather than an
                # estimate of the in-memory size of a batch.
                approximate_bytes_per_file=None,
            )
        else:  # pragma: no cover - Polars < 1.36.1
            partitioning = pl.PartitionMaxSize(
                str(target), max_size=int(max_rows_per_file)
            )
        _as_lazy(lf).sink_parquet(partitioning, compression=compression)
        return sorted(str(p) for p in target.glob("**/*.parquet"))

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
