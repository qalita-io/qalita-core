"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Unit tests for :mod:`qalita_core.analytics`.

These assert the properties packs are allowed to rely on — one pass per
:func:`agg`, a sample drawn from the whole dataset, bounded row sets, a stated
error bound on approximate statistics — rather than exact numeric outputs,
which would only pin the current implementation in place.

The fixtures come from :mod:`tests.bigdata`, so every dataset here is
multi-part: a helper that only ever looks at the first Parquet part passes on a
single-file fixture and fails here, which is the whole point.
"""

from __future__ import annotations

import math
import os

import polars as pl
import pytest

from qalita_core import analytics

from tests.bigdata import (  # noqa: F401 - imported for pytest fixture lookup
    Dataset,
    generate,
    small_dataset,
)


@pytest.fixture(scope="module")
def many_failures(tmp_path_factory) -> Dataset:
    """Big enough that millions of rows fail a predicate.

    Deliberately not the memory harness's fixture: bounding an example row set
    is a correctness property, and it should not cost this file a 16M-row
    dataset to assert it.
    """
    root = tmp_path_factory.mktemp("qalita-failures")
    return generate(root, rows=2_400_000, parts=4)


# --------------------------------------------------------------------------
# row_count / agg
# --------------------------------------------------------------------------


def test_row_count_covers_every_part(small_dataset):
    assert analytics.row_count(small_dataset.scan()) == small_dataset.rows


def test_row_count_accepts_an_eager_frame():
    assert analytics.row_count(pl.DataFrame({"a": [1, 2, 3]})) == 3


def test_agg_returns_one_row_of_scalars(small_dataset):
    result = analytics.agg(
        small_dataset.scan(),
        {
            "rows": pl.len(),
            "id_min": pl.col("id").min(),
            "id_max": pl.col("id").max(),
            "value_nulls": pl.col("value").null_count(),
            "flag_true": pl.col("flag").sum(),
        },
    )
    assert set(result) == {
        "rows",
        "id_min",
        "id_max",
        "value_nulls",
        "flag_true",
    }
    assert all(
        not isinstance(v, (pl.Series, pl.DataFrame)) for v in result.values()
    )
    assert result["rows"] == small_dataset.rows
    assert result["id_min"] == 0
    assert result["id_max"] == small_dataset.rows - 1
    assert 0 < result["value_nulls"] < small_dataset.rows


def test_agg_batches_every_expression_into_a_single_pass(
    small_dataset, monkeypatch
):
    """The property the whole design rests on.

    A per-column loop would re-scan the source once per statistic. Counting
    collects is the only assertion that actually distinguishes the two.
    """
    calls: list[int] = []
    original = analytics._collect

    def counting(lf):
        calls.append(1)
        return original(lf)

    monkeypatch.setattr(analytics, "_collect", counting)

    exprs = {
        f"{column}__nulls": pl.col(column).null_count()
        for column in small_dataset.columns
    }
    exprs.update(
        {
            f"{column}__count": pl.col(column).count()
            for column in small_dataset.columns
        }
    )
    result = analytics.agg(small_dataset.scan(), exprs)

    assert len(result) == 2 * len(small_dataset.columns)
    assert sum(calls) == 1


def test_agg_with_no_expressions_does_not_touch_the_data(monkeypatch):
    monkeypatch.setattr(
        analytics,
        "_collect",
        lambda lf: pytest.fail("agg({}) must not collect"),
    )
    assert analytics.agg(pl.LazyFrame({"a": [1]}), {}) == {}


def test_agg_on_an_empty_frame_yields_none_per_name():
    empty = pl.LazyFrame({"a": []}, schema={"a": pl.Int64}).filter(
        pl.col("a") > 0
    )
    result = analytics.agg(empty, {"total": pl.col("a").sum()})
    assert set(result) == {"total"}


def test_streaming_collect_error_is_raised_not_swallowed(small_dataset):
    with pytest.raises(analytics.StreamingCollectError) as excinfo:
        analytics.agg(
            small_dataset.scan(), {"x": pl.col("does_not_exist").sum()}
        )
    # The message has to say why there is no retry, or the next reader adds one.
    assert "in-memory engine" in str(excinfo.value)
    assert excinfo.value.__cause__ is not None


# --------------------------------------------------------------------------
# sample
# --------------------------------------------------------------------------


def test_sample_is_drawn_from_the_whole_dataset(small_dataset):
    """``id`` is monotonic, so a first-parts-only draw is visible in the mean."""
    drawn = analytics.sample(small_dataset.scan(), 5_000, seed=0)
    midpoint = (small_dataset.rows - 1) / 2

    assert drawn.height == 5_000
    assert drawn["id"].mean() == pytest.approx(midpoint, rel=0.05)
    # And it really does reach the last part, not just a wide middle.
    assert drawn["id"].max() > small_dataset.rows * 0.95
    assert drawn["id"].min() < small_dataset.rows * 0.05


def test_head_method_is_documented_biased(small_dataset):
    """``method='head'`` is a preview, and the test states in what way."""
    n = 5_000
    head = analytics.sample(small_dataset.scan(), n, method="head")
    midpoint = (small_dataset.rows - 1) / 2

    assert head.height == n
    assert head["id"].to_list() == list(range(n))
    # Nowhere near the dataset midpoint: this is why it is not the default.
    assert head["id"].mean() < midpoint / 2


def test_sample_is_deterministic_per_seed(small_dataset):
    first = analytics.sample(small_dataset.scan(), 2_000, seed=11)
    again = analytics.sample(small_dataset.scan(), 2_000, seed=11)
    other = analytics.sample(small_dataset.scan(), 2_000, seed=12)

    assert first.equals(again)
    assert not first.equals(other)


def test_sample_returns_everything_when_the_dataset_is_smaller(small_dataset):
    drawn = analytics.sample(small_dataset.scan(), small_dataset.rows * 2)
    assert drawn.height == small_dataset.rows


def test_sample_accepts_a_precomputed_total(small_dataset, monkeypatch):
    """Passing ``total_rows`` must skip the counting pass, not just ignore it."""
    monkeypatch.setattr(
        analytics,
        "row_count",
        lambda lf: pytest.fail("total_rows was provided; do not recount"),
    )
    drawn = analytics.sample(
        small_dataset.scan(), 1_000, total_rows=small_dataset.rows
    )
    assert drawn.height == 1_000


def test_sample_rejects_a_non_positive_size(small_dataset):
    with pytest.raises(ValueError):
        analytics.sample(small_dataset.scan(), 0)


def test_sample_rejects_an_unknown_method(small_dataset):
    with pytest.raises(ValueError, match="unknown sample method"):
        analytics.sample(small_dataset.scan(), 10, method="first")


# --------------------------------------------------------------------------
# approx_n_unique
# --------------------------------------------------------------------------


# Observed error of Polars' HyperLogLog on this dataset is under 6%; the bound
# is set loosely enough that a sketch retune is not a test failure, and tightly
# enough that a fall back to something dumber is.
HLL_TOLERANCE = 0.10


def test_approx_n_unique_is_close_to_exact(small_dataset):
    columns = ["uid", "key", "category"]
    approximate = analytics.approx_n_unique(small_dataset.scan(), columns)
    exact = analytics.approx_n_unique(
        small_dataset.scan(), columns, exact=True
    )

    assert exact["uid"] == small_dataset.rows  # one distinct value per row
    assert exact["key"] == small_dataset.cardinality
    for column in columns:
        assert approximate[column] == pytest.approx(
            exact[column], rel=HLL_TOLERANCE
        ), f"{column}: {approximate[column]} vs exact {exact[column]}"


def test_approx_n_unique_of_no_columns_is_empty(small_dataset):
    assert analytics.approx_n_unique(small_dataset.scan(), []) == {}


def test_approx_n_unique_reports_zero_for_an_all_null_column():
    lf = pl.LazyFrame({"a": [None, None]}, schema={"a": pl.Int64})
    assert analytics.approx_n_unique(lf, ["a"])["a"] in (0, 1)


# --------------------------------------------------------------------------
# quantiles
# --------------------------------------------------------------------------


def test_histogram_quantiles_stay_within_the_documented_error(small_dataset):
    bins = 1_000
    probabilities = [0.05, 0.25, 0.5, 0.75, 0.95]

    approximate = analytics.quantiles(
        small_dataset.scan(), ["id"], probabilities, bins=bins
    )
    exact = analytics.quantiles(
        small_dataset.scan(), ["id"], probabilities, exact=True
    )

    # The docstring promises an absolute error bounded by the bucket width.
    bucket_width = (small_dataset.rows - 1) / bins
    for q in probabilities:
        assert abs(approximate["id"][q] - exact["id"][q]) <= bucket_width, (
            f"q={q}: {approximate['id'][q]} vs {exact['id'][q]}, "
            f"bucket width {bucket_width}"
        )


def test_quantiles_of_a_constant_column_are_the_constant():
    lf = pl.LazyFrame({"a": [7.0] * 1_000})
    assert analytics.quantiles(lf, ["a"], [0.1, 0.9]) == {
        "a": {0.1: 7.0, 0.9: 7.0}
    }


def test_quantiles_omit_an_all_null_column():
    lf = pl.LazyFrame({"a": [None] * 10}, schema={"a": pl.Float64})
    assert analytics.quantiles(lf, ["a"], [0.5]) == {}


def test_quantiles_ignore_nan_values():
    """A single NaN must not take the whole column with it.

    NaN is not null in Polars: a literal `NaN` token in a CSV, or a Parquet
    file written by polars/pyarrow/Spark, lands as a float NaN. It used to
    reach the strict Int32 cast of the bucket index and abort the job.
    """
    values = [float(i) for i in range(1, 1_001)] + [float("nan")]
    lf = pl.LazyFrame({"a": values})

    approximate = analytics.quantiles(lf, ["a"], [0.5])
    exact = analytics.quantiles(lf, ["a"], [0.5], exact=True)

    # NaN is excluded from the sample, so the median is the median of 1..1000.
    assert approximate["a"][0.5] == pytest.approx(500.0, abs=2.0)
    assert exact["a"][0.5] == pytest.approx(500.0, abs=1.0)


def test_quantiles_ignore_infinite_values():
    """+/-Inf poisons the bucket WIDTH, not just one row.

    With Inf in the bounds the width is inf, every finite value falls in
    bucket 0 and every quantile collapses to the minimum — so excluding
    non-finite values from min/max is the load-bearing half of the fix.
    """
    values = [float(i) for i in range(1, 1_001)]
    values += [float("inf"), float("-inf")]
    lf = pl.LazyFrame({"a": values})

    approximate = analytics.quantiles(lf, ["a"], [0.5])
    exact = analytics.quantiles(lf, ["a"], [0.5], exact=True)

    assert approximate["a"][0.5] == pytest.approx(500.0, abs=2.0)
    assert exact["a"][0.5] == pytest.approx(500.0, abs=1.0)


def test_quantiles_omit_an_all_nan_column():
    """Same convention as an all-null column: omitted, not crashed."""
    lf = pl.LazyFrame({"a": [float("nan")] * 10})
    assert analytics.quantiles(lf, ["a"], [0.5]) == {}
    assert analytics.quantiles(lf, ["a"], [0.5], exact=True) == {}


def test_quantiles_still_work_on_a_decimal_column():
    """`is_finite` is not implemented for Decimal, so the cast comes first.

    `numeric_columns` reports Decimal as numeric, so guarding non-finite
    values with a naive `pl.col(c).is_finite()` would trade the NaN crash for
    a Decimal crash.
    """
    lf = pl.LazyFrame({"a": [str(i) for i in range(1, 1_001)]}).with_columns(
        pl.col("a").cast(pl.Decimal(12, 2))
    )

    approximate = analytics.quantiles(lf, ["a"], [0.5])
    exact = analytics.quantiles(lf, ["a"], [0.5], exact=True)

    assert approximate["a"][0.5] == pytest.approx(500.0, abs=2.0)
    assert exact["a"][0.5] == pytest.approx(500.0, abs=1.0)


def test_quantiles_of_a_column_named_like_the_bucket_alias():
    """The histogram's internal aliases must not shadow a user column."""
    for name in ("bucket", "n"):
        lf = pl.LazyFrame({name: [float(i) for i in range(1, 1_001)]})
        assert analytics.quantiles(lf, [name], [0.5])[name][0.5] == (
            pytest.approx(500.0, abs=2.0)
        )


def test_quantiles_reject_probabilities_outside_zero_one(small_dataset):
    with pytest.raises(ValueError, match=r"within \[0, 1\]"):
        analytics.quantiles(small_dataset.scan(), ["id"], [1.5])


def test_quantiles_reject_a_degenerate_bin_count(small_dataset):
    with pytest.raises(ValueError, match="bins must be at least 2"):
        analytics.quantiles(small_dataset.scan(), ["id"], [0.5], bins=1)


def test_quantiles_of_nothing_is_empty(small_dataset):
    assert analytics.quantiles(small_dataset.scan(), [], [0.5]) == {}
    assert analytics.quantiles(small_dataset.scan(), ["id"], []) == {}


# --------------------------------------------------------------------------
# top_k / value_counts
# --------------------------------------------------------------------------


def test_top_k_returns_at_most_k_rows_in_order():
    lf = pl.LazyFrame({"name": list("abcdef"), "n": [1, 9, 3, 7, 5, 2]})
    top = analytics.top_k(lf, "n", 3)
    assert top.height == 3
    assert top["n"].to_list() == [9, 7, 5]


def test_top_k_with_other_folds_the_tail_into_one_row():
    lf = pl.LazyFrame({"name": list("abcdef"), "n": [1, 9, 3, 7, 5, 2]})
    folded = analytics.top_k(lf, "n", 3, other=True)

    assert folded.height == 4
    assert folded["n"].to_list() == [9, 7, 5, 1 + 3 + 2]
    # The fold row carries no identity, only the residual weight.
    assert folded["name"][-1] is None
    # Nothing is lost or double counted.
    assert folded["n"].sum() == 27


def test_top_k_with_other_omits_an_empty_tail():
    lf = pl.LazyFrame({"name": list("ab"), "n": [1, 2]})
    assert analytics.top_k(lf, "n", 5, other=True).height == 2


def test_top_k_rejects_a_non_positive_k():
    with pytest.raises(ValueError, match="k must be positive"):
        analytics.top_k(pl.LazyFrame({"n": [1]}), "n", 0)


def test_value_counts_is_bounded_and_totals_the_dataset(small_dataset):
    counts = analytics.value_counts(small_dataset.scan(), "key", 5, other=True)
    assert counts.height == 6  # 5 values plus the folded tail
    assert counts["count"].sum() == small_dataset.rows


def test_value_counts_survives_a_column_named_count():
    """A dataset is allowed to have a column called "count".

    The frequency column used to be aliased "count" too, and Polars refuses a
    group key and an aggregate output that share a name.
    """
    lf = pl.LazyFrame({"count": [3, 4, 3, 5]})
    counts = analytics.value_counts(lf, "count", 10)

    # The group key keeps the user's name, so the frequency column has to be
    # the reserved one — they cannot both be called "count".
    assert counts.columns == ["count", analytics.COUNT_COLUMN]
    assert dict(counts.iter_rows()) == {3: 2, 4: 1, 5: 1}


def test_value_counts_survives_a_column_named_after_the_reserved_alias():
    """Even the reserved name itself is a legal column name."""
    lf = pl.LazyFrame({analytics.COUNT_COLUMN: [3, 4, 3, 5]})
    counts = analytics.value_counts(lf, analytics.COUNT_COLUMN, 10)

    assert counts.columns == [analytics.COUNT_COLUMN, "count"]
    assert dict(counts.iter_rows()) == {3: 2, 4: 1, 5: 1}


def test_value_counts_refuses_a_near_unique_column(small_dataset):
    """The guard fires BEFORE the work, so the caller gets an error not an OOM.

    Bounding the k rows that come out is not enough: the group table that
    produces them holds one entry per distinct value, and this Polars build has
    no spilling at all, so grouping a near-unique column is what actually
    exhausts the machine.
    """
    with pytest.raises(analytics.CardinalityTooHigh) as excinfo:
        analytics.value_counts(small_dataset.scan(), "uid", 10)
    assert "uid" in str(excinfo.value)

    # Opting out is possible, and then the result is still capped at k.
    counts = analytics.value_counts(
        small_dataset.scan(), "uid", 10, max_groups=None
    )
    assert counts.height == 10
    assert counts["count"].to_list() == [1] * 10


# --------------------------------------------------------------------------
# failures
# --------------------------------------------------------------------------


def test_failures_counts_exactly_and_bounds_the_examples(many_failures):
    """Millions of rows fail; the evidence stays at ``limit``."""
    count, rows = analytics.failures(
        many_failures.scan(), pl.col("flag"), limit=10
    )
    assert count == many_failures.rows // 2  # flag is true on every even id
    assert count >= 1_000_000, "the bound is only interesting above the limit"
    assert rows.height == 10
    assert rows["flag"].to_list() == [True] * 10


def test_failures_can_restrict_the_example_columns(small_dataset):
    _, rows = analytics.failures(
        small_dataset.scan(),
        pl.col("value").is_null(),
        limit=5,
        columns=["id", "value"],
    )
    assert rows.columns == ["id", "value"]
    assert rows["value"].null_count() == rows.height


def test_failures_with_limit_zero_returns_the_count_only(small_dataset):
    count, rows = analytics.failures(
        small_dataset.scan(), pl.col("value").is_null(), limit=0
    )
    assert count > 0
    assert rows.height == 0


def test_failures_treats_null_predicates_as_passing():
    lf = pl.LazyFrame({"a": [1, None, 3]}, schema={"a": pl.Int64})
    count, rows = analytics.failures(lf, pl.col("a") > 2, limit=10)
    assert count == 1
    assert rows["a"].to_list() == [3]


def test_failures_with_no_failing_row_returns_an_empty_frame(small_dataset):
    count, rows = analytics.failures(
        small_dataset.scan(), pl.col("id") < 0, limit=10
    )
    assert count == 0
    assert rows.height == 0
    assert rows.columns == list(small_dataset.columns)


# --------------------------------------------------------------------------
# sink
# --------------------------------------------------------------------------


def test_sink_writes_a_single_file_by_default(tmp_path, small_dataset):
    target = tmp_path / "out.parquet"
    written = analytics.sink(small_dataset.scan(), str(target))

    assert written == [str(target)]
    assert target.is_file()
    assert analytics.row_count(pl.scan_parquet(written)) == small_dataset.rows


def test_sink_splits_into_several_parts_in_one_pass(
    tmp_path, small_dataset, monkeypatch
):
    """``max_rows_per_file`` must not cost one full scan per output part.

    Asserted on what reaches the disk, because the previous helper re-executed
    the whole plan once per slice and that is invisible in the file contents.
    """
    executions: list[int] = []
    original = pl.LazyFrame.sink_parquet

    def counting(self, *args, **kwargs):
        executions.append(1)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", counting)

    root = tmp_path / "parts"
    rows_per_file = small_dataset.rows // 4
    analytics.sink(
        small_dataset.scan(), str(root), max_rows_per_file=rows_per_file
    )

    assert sum(executions) == 1, "the source must be scanned exactly once"

    on_disk = sorted(str(p) for p in root.rglob("*.parquet") if p.is_file())
    assert len(on_disk) >= 4, f"expected several parts, got {on_disk}"
    assert analytics.row_count(pl.scan_parquet(on_disk)) == small_dataset.rows
    for path in on_disk:
        assert analytics.row_count(pl.scan_parquet(path)) <= rows_per_file


@pytest.mark.xfail(
    strict=False,
    reason=(
        "analytics.sink() passes 'part-{part}.parquet' as PartitionMaxSize's "
        "base_path, which Polars 1.36+ treats as a DIRECTORY name and never "
        "interpolates. The split itself happens, but the parts land in a "
        "directory literally named 'part-{part}.parquet' and sink() returns "
        "that directory instead of the files. Fix: "
        "pl.PartitionMaxSize(str(target), max_size=n) plus "
        "file_path=lambda ctx: f'part-{ctx.file_idx:05d}.parquet', then glob "
        "target/'*.parquet'."
    ),
)
def test_sink_returns_the_files_it_wrote(tmp_path, small_dataset):
    written = analytics.sink(
        small_dataset.scan(),
        str(tmp_path / "parts"),
        max_rows_per_file=small_dataset.rows // 4,
    )
    assert len(written) >= 4, f"expected several parts, got {written}"
    assert all(os.path.isfile(path) for path in written), written
    assert analytics.row_count(pl.scan_parquet(written)) == small_dataset.rows


def test_sink_honours_a_row_group_size(tmp_path, small_dataset):
    target = tmp_path / "grouped.parquet"
    analytics.sink(small_dataset.scan(), str(target), row_group_size=10_000)
    assert analytics.row_count(pl.scan_parquet(str(target))) == (
        small_dataset.rows
    )


# --------------------------------------------------------------------------
# schema helpers
# --------------------------------------------------------------------------


def test_column_helpers_split_a_schema_by_dtype(small_dataset):
    schema = dict(small_dataset.scan().collect_schema())

    numeric = analytics.numeric_columns(schema)
    strings = analytics.string_columns(schema)
    temporal = analytics.temporal_columns(schema)

    assert set(numeric) == {"id", "key", "value", "amount"}
    assert set(strings) == {"uid", "category"}
    assert set(temporal) == {"ts"}
    # Booleans are neither numeric nor a string here: nothing may claim `flag`.
    assert "flag" not in numeric + strings + temporal


def test_batched_splits_without_losing_items():
    batches = list(analytics.batched(range(7), 3))
    assert batches == [[0, 1, 2], [3, 4, 5], [6]]
    assert list(analytics.batched([], 3)) == []


def test_as_lazy_rejects_anything_else():
    with pytest.raises(TypeError):
        analytics.row_count([1, 2, 3])


# --------------------------------------------------------------------------
# invariants of the module as a whole
# --------------------------------------------------------------------------


def test_every_row_returning_helper_is_bounded(small_dataset):
    """No public helper may hand a pack an unbounded row set."""
    lf = small_dataset.scan()
    bounded = {
        "sample": analytics.sample(lf, 50).height,
        "top_k": analytics.top_k(
            lf.group_by("key").agg(pl.len().alias("count")), "count", 5
        ).height,
        "value_counts": analytics.value_counts(lf, "key", 5).height,
        "failures": analytics.failures(lf, pl.col("flag"), limit=5)[1].height,
    }
    for name, height in bounded.items():
        assert height <= 50, f"{name} returned {height} rows"
    assert not math.isnan(float(bounded["sample"]))
