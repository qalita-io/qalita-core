"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Streaming column profiler.

Replaces ydata-profiling on the hot path. ydata-profiling is pandas-only with no
lazy or streaming mode: it needs the frame in RAM, computes exact ``n_distinct``
and exact quantiles by hashing and sorting whole columns, and therefore cannot
describe a dataset larger than the worker. What it produced instead was a
profile of ``head(500_000)`` — which, on a chunked source, describes the first
part files rather than the dataset.

This profiler reads the whole dataset in a bounded number of streaming passes:

- **one** pass for every scalar statistic of every column, batched;
- **two** passes for approximate quantiles (bounds, then bucket counts);
- one pass per column for top-K values, which is the only per-column cost and
  can be turned off.

Which statistics are exact and which are approximate is reported per column in
``methods``, so the caller can label them rather than guess.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import polars as pl

from . import analytics

__all__ = ["profile", "DEFAULT_QUANTILES"]


DEFAULT_QUANTILES = (0.05, 0.25, 0.5, 0.75, 0.95)

# Above this many columns the per-column top-K pass is skipped by default: on a
# very wide table it would dominate the whole profile.
DEFAULT_MAX_TOPK_COLUMNS = 200

# Rows drawn when top values have to be estimated rather than counted exactly.
#
# 0 — do not estimate, report nothing — because drawing the sample costs more
# than the statistic is worth: with sampling enabled a profile of a 160M-row
# source peaked at 4.0 GiB and grew to 7.4 GiB when the source doubled, against
# a flat 0.4 GiB with it off. Set it to a row count to trade that back.
DEFAULT_TOPK_SAMPLE_ROWS = 0


def profile(
    lf: "pl.LazyFrame",
    *,
    schema: Mapping[str, Any] | None = None,
    exact: bool = False,
    top_k: int = 10,
    quantiles: Sequence[float] = DEFAULT_QUANTILES,
    max_topk_columns: int = DEFAULT_MAX_TOPK_COLUMNS,
    max_groups: int = analytics.DEFAULT_MAX_GROUPS,
    sample_rows: int = DEFAULT_TOPK_SAMPLE_ROWS,
) -> dict[str, dict[str, Any]]:
    """Profile every column of a LazyFrame in a bounded number of passes.

    Args:
        lf: data to profile.
        schema: pre-read schema. Omit to read it from the parquet footers.
        exact: compute distinct counts and quantiles exactly. Costs
            O(cardinality) memory for distinct counts — on a primary key that is
            one entry per row.
        top_k: number of most frequent values to report per column. ``0``
            disables the per-column pass entirely.
        quantiles: probabilities to report for numeric columns.
        max_topk_columns: skip top-K when the table has more columns than this.
        max_groups: above this many distinct values, top-K is estimated from a
            sample rather than counted exactly. Grouping is what blows the
            memory budget, and it does so well below this cardinality.
        sample_rows: size of that sample.

    Returns:
        ``{column: {statistic: value}}``. Each column dict carries a ``methods``
        sub-dict naming how the inexact statistics were obtained
        (``"exact"`` / ``"hyperloglog"`` / ``"histogram"``).
    """
    columns = dict(schema) if schema else dict(lf.collect_schema())
    if not columns:
        return {}

    names = list(columns)
    numeric = analytics.numeric_columns(columns)
    strings = analytics.string_columns(columns)
    temporal = analytics.temporal_columns(columns)

    stats = _scalar_pass(lf, names, numeric, strings, temporal, exact=exact)
    total_rows = int(stats.get("__rows") or 0)

    quantile_values: dict[str, dict[float, float]] = {}
    if numeric and quantiles:
        quantile_values = analytics.quantiles(
            lf, numeric, quantiles, exact=exact
        )

    result: dict[str, dict[str, Any]] = {}
    for name in names:
        dtype = columns[name]
        non_null = int(stats.get(f"count|{name}") or 0)
        nulls = total_rows - non_null
        distinct = stats.get(f"distinct|{name}")

        column: dict[str, Any] = {
            "type": str(dtype),
            "n": total_rows,
            "count": non_null,
            "n_missing": nulls,
            "p_missing": (nulls / total_rows) if total_rows else 0.0,
            "n_distinct": int(distinct) if distinct is not None else 0,
            # HyperLogLog can overshoot the true cardinality by a few percent,
            # which would otherwise report a ratio above 1.
            "p_distinct": (
                min(int(distinct) / non_null, 1.0)
                if non_null and distinct
                else 0.0
            ),
            "methods": {
                "n_distinct": "exact" if exact else "hyperloglog",
            },
        }

        if name in numeric:
            column.update(
                {
                    "min": stats.get(f"min|{name}"),
                    "max": stats.get(f"max|{name}"),
                    "mean": stats.get(f"mean|{name}"),
                    "std": stats.get(f"std|{name}"),
                    "sum": stats.get(f"sum|{name}"),
                    "n_zeros": int(stats.get(f"zeros|{name}") or 0),
                    "n_negative": int(stats.get(f"negative|{name}") or 0),
                }
            )
            qs = quantile_values.get(name)
            if qs:
                column["quantiles"] = {str(q): v for q, v in qs.items()}
                column["methods"]["quantiles"] = (
                    "exact" if exact else "histogram"
                )
                if 0.25 in qs and 0.75 in qs:
                    column["iqr"] = qs[0.75] - qs[0.25]
        elif name in strings:
            column.update(
                {
                    "min_length": stats.get(f"minlen|{name}"),
                    "max_length": stats.get(f"maxlen|{name}"),
                    "mean_length": stats.get(f"meanlen|{name}"),
                    "n_empty": int(stats.get(f"empty|{name}") or 0),
                }
            )
        elif name in temporal:
            column.update(
                {
                    "min": stats.get(f"min|{name}"),
                    "max": stats.get(f"max|{name}"),
                }
            )

        result[name] = column

    if top_k and len(names) <= max_topk_columns:
        for name in names:
            values, method = _top_values(
                lf,
                name,
                top_k,
                result[name]["n_distinct"],
                max_groups,
                total_rows,
                sample_rows,
            )
            result[name]["top_values"] = values
            if method:
                result[name]["methods"]["top_values"] = method

    return result


def _scalar_pass(
    lf: "pl.LazyFrame",
    names: Sequence[str],
    numeric: Sequence[str],
    strings: Sequence[str],
    temporal: Sequence[str],
    *,
    exact: bool,
) -> dict[str, Any]:
    """Every scalar statistic of every column, in one streaming pass."""
    distinct_op = "n_unique" if exact else "approx_n_unique"

    exprs: dict[str, pl.Expr] = {"__rows": pl.len()}
    for name in names:
        col = pl.col(name)
        exprs[f"count|{name}"] = col.count()
        exprs[f"distinct|{name}"] = getattr(col, distinct_op)()

    for name in numeric:
        col = pl.col(name)
        exprs[f"min|{name}"] = col.min()
        exprs[f"max|{name}"] = col.max()
        exprs[f"mean|{name}"] = col.mean()
        exprs[f"std|{name}"] = col.std()
        exprs[f"sum|{name}"] = col.sum()
        exprs[f"zeros|{name}"] = (col == 0).sum()
        exprs[f"negative|{name}"] = (col < 0).sum()

    for name in strings:
        col = pl.col(name)
        exprs[f"minlen|{name}"] = col.str.len_chars().min()
        exprs[f"maxlen|{name}"] = col.str.len_chars().max()
        exprs[f"meanlen|{name}"] = col.str.len_chars().mean()
        exprs[f"empty|{name}"] = (col.str.len_chars() == 0).sum()

    for name in temporal:
        col = pl.col(name)
        exprs[f"min|{name}"] = col.min()
        exprs[f"max|{name}"] = col.max()

    return analytics.agg(lf, exprs)


def _top_values(
    lf: "pl.LazyFrame",
    column: str,
    k: int,
    n_distinct: int,
    max_groups: int,
    total_rows: int,
    sample_rows: int,
) -> tuple[list[dict[str, Any]], str | None]:
    """The k most frequent values of a column, and how they were obtained.

    An exact group_by is only affordable at low cardinality: Polars 1.37 has no
    spilling, so past a small group count the cost of grouping tracks the row
    count rather than the group count. Above ``max_groups`` the counts are taken
    from a bounded random sample instead, which caps memory at the sample size
    whatever the source.

    Sampling is the right trade here: top-K is a statement about the FREQUENT
    values, and frequent values are exactly what survives a random sample. The
    counts become estimates, so the caller is told the method and can label it.

    Returns ``(records, method)`` where method is ``None`` for an exact count.
    """
    if n_distinct <= max_groups:
        counts = analytics.value_counts(lf, column, k, max_groups=max_groups)
        return _records(counts, column), None

    if sample_rows <= 0:
        # Estimating is off: say nothing rather than report a number whose cost
        # is proportional to the source.
        return [], "skipped-high-cardinality"

    if total_rows <= sample_rows:
        # Small enough to count exactly regardless of cardinality.
        counts = analytics.value_counts(lf, column, k, max_groups=None)
        return _records(counts, column), None

    drawn = analytics.sample(lf, sample_rows, total_rows=total_rows).select(
        column
    )
    counts = analytics.value_counts(drawn, column, k, max_groups=None)
    # Scale the sample counts back up so they are comparable with the row count
    # reported next to them.
    factor = total_rows / drawn.height if drawn.height else 1.0
    return [
        {"value": rec["value"], "count": int(round(rec["count"] * factor))}
        for rec in _records(counts, column)
    ], "sampled"


def _records(counts: "pl.DataFrame", column: str) -> list[dict[str, Any]]:
    return [
        {"value": row[0], "count": int(row[1])}
        for row in counts.select(column, "count").iter_rows()
    ]
