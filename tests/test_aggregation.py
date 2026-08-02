"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.aggregation module
"""

import subprocess
import sys

import pytest
import pandas as pd
import numpy as np
import polars as pl
import datetime as dt
from qalita_core.aggregation import (
    detect_chunked_from_items,
    normalize_and_dedupe_recommendations,
    streaming_outliers,
    CompletenessAggregator,
    OutlierAggregator,
    DuplicateAggregator,
    TimelinessAggregator,
    _determine_recommendation_level,
)


class TestDetectChunkedFromItems:
    """Tests for detect_chunked_from_items function."""

    def test_empty_items(self):
        result = detect_chunked_from_items([], None, "dataset")
        assert result == (False, False, False)

    def test_single_item(self):
        result = detect_chunked_from_items(
            ["file.parquet"], ["name"], "dataset"
        )
        assert result == (False, False, False)

    def test_auto_named_chunks(self):
        items = ["item1", "item2"]
        names = ["dataset_0", "dataset_1"]
        result = detect_chunked_from_items(items, names, "dataset")
        treat_chunks, auto_named, common_base = result
        assert treat_chunks is True
        assert auto_named is True

    def test_parquet_chunk_detection(self):
        items = [
            "/path/data_part_1.parquet",
            "/path/data_part_2.parquet",
            "/path/data_part_3.parquet",
        ]
        result = detect_chunked_from_items(items, None, "dataset")
        treat_chunks, auto_named, common_base = result
        assert treat_chunks is True
        assert common_base is True

    def test_parquet_pq_extension(self):
        items = [
            "/path/data_part_1.pq",
            "/path/data_part_2.pq",
        ]
        result = detect_chunked_from_items(items, None, "dataset")
        treat_chunks, auto_named, common_base = result
        assert treat_chunks is True
        assert common_base is True

    def test_different_bases_not_chunked(self):
        items = [
            "/path/file1_part_1.parquet",
            "/path/file2_part_1.parquet",
        ]
        result = detect_chunked_from_items(items, None, "dataset")
        _, _, common_base = result
        assert common_base is False

    def test_non_parquet_items(self):
        items = ["file1.csv", "file2.csv"]
        result = detect_chunked_from_items(items, None, "dataset")
        _, _, common_base = result
        assert common_base is False

    def test_none_names_auto_named(self):
        items = ["a", "b"]
        result = detect_chunked_from_items(items, None, "dataset")
        _, auto_named, _ = result
        assert auto_named is True


class TestNormalizeAndDedupeRecommendations:
    """Tests for normalize_and_dedupe_recommendations function."""

    def test_empty_records(self):
        result = normalize_and_dedupe_recommendations([], "root_dataset")
        assert result == []

    def test_none_records(self):
        result = normalize_and_dedupe_recommendations(None, "root_dataset")
        assert result == []

    def test_normalize_dataset_scope(self):
        records = [
            {
                "content": "test recommendation",
                "type": "warning",
                "scope": {"perimeter": "dataset", "value": "old_dataset"},
            }
        ]
        result = normalize_and_dedupe_recommendations(records, "new_dataset")
        assert result[0]["scope"]["value"] == "new_dataset"

    def test_normalize_parent_scope(self):
        records = [
            {
                "content": "test",
                "type": "info",
                "scope": {
                    "perimeter": "column",
                    "value": "col1",
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": "old_dataset",
                    },
                },
            }
        ]
        result = normalize_and_dedupe_recommendations(records, "new_dataset")
        assert result[0]["scope"]["parent_scope"]["value"] == "new_dataset"

    def test_deduplicate_by_content_type_scope(self):
        records = [
            {
                "content": "duplicate",
                "type": "warning",
                "scope": {"value": "ds1"},
            },
            {
                "content": "duplicate",
                "type": "warning",
                "scope": {"value": "ds1"},
            },
            {"content": "unique", "type": "info", "scope": {"value": "ds1"}},
        ]
        result = normalize_and_dedupe_recommendations(records, "ds1")
        assert len(result) == 2

    def test_different_types_not_deduplicated(self):
        records = [
            {"content": "same", "type": "warning", "scope": {"value": "ds1"}},
            {"content": "same", "type": "error", "scope": {"value": "ds1"}},
        ]
        result = normalize_and_dedupe_recommendations(records, "ds1")
        assert len(result) == 2


class TestCompletenessAggregator:
    """Tests for CompletenessAggregator class."""

    def test_init(self):
        agg = CompletenessAggregator()
        assert agg.total_rows == 0
        assert agg.total_non_null_cells == 0
        assert agg.total_cells == 0
        assert agg.per_column == {}
        assert agg.unique_columns == set()

    def test_add_single_df(self):
        agg = CompletenessAggregator()
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, None, 6]})
        agg.add_df(df)
        assert agg.total_rows == 3
        assert "col1" in agg.unique_columns
        assert "col2" in agg.unique_columns

    def test_add_multiple_dfs(self):
        agg = CompletenessAggregator()
        df1 = pd.DataFrame({"col1": [1, 2]})
        df2 = pd.DataFrame({"col1": [3, 4]})
        agg.add_df(df1)
        agg.add_df(df2)
        assert agg.total_rows == 4

    def test_finalize_metrics_and_schemas(self):
        agg = CompletenessAggregator()
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, None, 6]})
        agg.add_df(df)
        metrics, schemas = agg.finalize_metrics_and_schemas("test_dataset")

        assert len(metrics) > 0
        assert len(schemas) > 0

        # Check for expected metric keys
        metric_keys = [m["key"] for m in metrics]
        assert "n" in metric_keys
        assert "n_var" in metric_keys
        assert "score" in metric_keys

    def test_column_completeness_score(self):
        agg = CompletenessAggregator()
        # Column with 50% nulls
        df = pd.DataFrame({"col1": [1, None]})
        agg.add_df(df)
        metrics, _ = agg.finalize_metrics_and_schemas("test")

        col_metrics = [
            m
            for m in metrics
            if m.get("scope", {}).get("perimeter") == "column"
        ]
        assert len(col_metrics) > 0

    def test_empty_df(self):
        agg = CompletenessAggregator()
        df = pd.DataFrame()
        agg.add_df(df)
        metrics, schemas = agg.finalize_metrics_and_schemas("test")
        # Should not crash, metrics should be generated
        assert isinstance(metrics, list)


def _outlier_frame():
    """0..99 plus one value far outside any fence."""
    return pl.DataFrame(
        {
            "v": [float(i) for i in range(100)] + [10_000.0],
            "flat": [7.0] * 101,
        }
    )


class TestStreamingOutliers:
    """Tests for the two-pass streaming outlier helper."""

    def test_iqr_counts_the_outlier(self):
        result = streaming_outliers(_outlier_frame().lazy(), ["v"])
        assert result["v"]["outlier_count"] == 1
        assert result["v"]["non_null"] == 101
        assert result["v"]["normality_score"] == pytest.approx(1 - 1 / 101)
        assert result["v"]["upper"] < 10_000

    def test_iqr_is_approximate_by_default(self):
        result = streaming_outliers(_outlier_frame().lazy(), ["v"])
        assert result["v"]["bounds_method"] == "histogram"

    def test_exact_flips_the_method_label(self):
        result = streaming_outliers(_outlier_frame().lazy(), ["v"], exact=True)
        assert result["v"]["bounds_method"] == "exact"
        assert result["v"]["outlier_count"] == 1

    def test_zscore_bounds_are_exact(self):
        result = streaming_outliers(
            _outlier_frame().lazy(), ["v"], method="zscore"
        )
        assert result["v"]["bounds_method"] == "exact"
        assert result["v"]["method"] == "zscore"
        assert result["v"]["outlier_count"] == 1

    def test_constant_column_has_no_fence(self):
        result = streaming_outliers(_outlier_frame().lazy(), ["flat"])
        assert result["flat"]["outlier_count"] == 0
        assert result["flat"]["normality_score"] == 1.0
        assert result["flat"]["lower"] is None

    def test_all_null_column_is_not_an_outlier_factory(self):
        frame = pl.DataFrame(
            {"v": [None, None, None]}, schema={"v": pl.Float64}
        )
        result = streaming_outliers(frame.lazy(), ["v"])
        assert result["v"]["outlier_count"] == 0
        assert result["v"]["non_null"] == 0
        assert result["v"]["normality_score"] == 1.0

    def test_chunks_share_one_global_fence(self):
        """The fence must come from the whole dataset, not from a chunk.

        Split in two, the first chunk alone would flag nothing: its own IQR
        covers its own range. Only a global fence sees the tail.
        """
        frame = _outlier_frame()
        whole = streaming_outliers(frame.lazy(), ["v"])
        halves = pl.concat([frame.head(50), frame.tail(51)]).lazy()
        assert streaming_outliers(halves, ["v"]) == whole

    def test_empty_columns_costs_nothing(self):
        assert streaming_outliers(_outlier_frame().lazy(), []) == {}

    def test_unknown_method_is_refused(self):
        with pytest.raises(ValueError, match="unknown outlier method"):
            streaming_outliers(_outlier_frame().lazy(), ["v"], method="mad")


class TestOutlierAggregator:
    """Tests for OutlierAggregator class."""

    def test_init(self):
        agg = OutlierAggregator()
        assert agg.columns == {}
        assert agg.dataset_outliers == 0
        assert agg.total_rows == 0

    def test_add_column_result(self):
        agg = OutlierAggregator()
        agg.add_column_result(
            "col1", outlier_count=5, normality_score=0.95, rows=100
        )
        assert agg.columns["col1"]["outlier_count"] == 5
        assert agg.columns["col1"]["rows"] == 100

    def test_a_column_is_recorded_once_not_accumulated(self):
        """Global results replace each other; they are never summed."""
        agg = OutlierAggregator()
        agg.add_column_result("col1", outlier_count=5, normality_score=0.95)
        agg.add_column_result("col1", outlier_count=8, normality_score=0.92)
        assert agg.columns["col1"]["outlier_count"] == 8

    def test_add_streaming_outliers(self):
        agg = OutlierAggregator()
        results = streaming_outliers(_outlier_frame().lazy(), ["v", "flat"])
        agg.add_streaming_outliers(results, rows=101)
        assert agg.total_rows == 101
        assert agg.columns["v"]["outlier_count"] == 1

    def test_add_dataset_result(self):
        agg = OutlierAggregator()
        agg.add_dataset_result(rows=100, multivariate_outliers_count=10)
        assert agg.dataset_outliers == 10
        assert agg.total_rows == 100

    def test_finalize_metrics_and_recommendations(self):
        agg = OutlierAggregator()
        agg.add_column_result(
            "col1", outlier_count=5, normality_score=0.95, rows=100
        )
        agg.add_dataset_result(rows=100, multivariate_outliers_count=10)

        metrics, recommendations = agg.finalize_metrics_and_recommendations(
            "test_dataset", normality_threshold=0.8
        )

        metric_keys = [m["key"] for m in metrics]
        assert "outliers" in metric_keys
        assert "score" in metric_keys
        assert "total_outliers_count" in metric_keys

    def test_approximate_metrics_carry_a_method_sibling(self):
        agg = OutlierAggregator()
        agg.add_streaming_outliers(
            streaming_outliers(_outlier_frame().lazy(), ["v"]), rows=101
        )
        metrics, _ = agg.finalize_metrics_and_recommendations(
            "test_dataset", normality_threshold=0.8
        )
        methods = {
            m["key"]: m["value"]
            for m in metrics
            if m["key"].endswith("_method")
        }
        assert methods["normality_score_method"] == "histogram"
        assert methods["outliers_method"] == "histogram"

    def test_exact_metrics_say_so(self):
        agg = OutlierAggregator()
        agg.add_streaming_outliers(
            streaming_outliers(_outlier_frame().lazy(), ["v"], exact=True),
            rows=101,
        )
        metrics, _ = agg.finalize_metrics_and_recommendations(
            "test_dataset", normality_threshold=0.8
        )
        methods = {
            m["key"]: m["value"]
            for m in metrics
            if m["key"].endswith("_method")
        }
        assert set(methods.values()) == {"exact"}

    def test_low_normality_generates_recommendation(self):
        agg = OutlierAggregator()
        agg.add_column_result(
            "col1", outlier_count=50, normality_score=0.5, rows=100
        )
        agg.add_dataset_result(rows=100, multivariate_outliers_count=50)

        metrics, recommendations = agg.finalize_metrics_and_recommendations(
            "test_dataset", normality_threshold=0.8
        )

        assert len(recommendations) > 0

    def test_dataset_score_is_the_mean_of_column_scores(self):
        agg = OutlierAggregator()
        agg.add_column_result("a", outlier_count=0, normality_score=1.0)
        agg.add_column_result("b", outlier_count=10, normality_score=0.5)
        metrics, _ = agg.finalize_metrics_and_recommendations(
            "test_dataset", normality_threshold=0.0
        )
        score = [m for m in metrics if m["key"] == "normality_score_dataset"]
        assert score[0]["value"] == 0.75


class TestDuplicateAggregator:
    """Tests for DuplicateAggregator class."""

    def test_init(self):
        agg = DuplicateAggregator(["col1", "col2"])
        assert agg.uniqueness_columns == ["col1", "col2"]
        assert agg.total_rows == 0
        assert agg.combo_to_count == {}

    def test_add_df_with_duplicates(self):
        agg = DuplicateAggregator(["col1"])
        df = pd.DataFrame({"col1": [1, 1, 2, 3, 3, 3]})
        agg.add_df(df)
        assert agg.total_rows == 6
        # Check duplicate counts
        assert (1,) in agg.combo_to_count
        assert agg.combo_to_count[(1,)] == 2

    def test_add_multiple_dfs(self):
        agg = DuplicateAggregator(["col1"])
        df1 = pd.DataFrame({"col1": [1, 2]})
        df2 = pd.DataFrame({"col1": [1, 3]})
        agg.add_df(df1)
        agg.add_df(df2)
        assert agg.total_rows == 4
        assert agg.combo_to_count[(1,)] == 2

    def test_finalize_metrics(self):
        agg = DuplicateAggregator(["col1"])
        df = pd.DataFrame({"col1": [1, 1, 2, 3]})  # 1 duplicate
        agg.add_df(df)

        metrics, recommendations = agg.finalize_metrics("test_dataset")

        assert len(metrics) > 0
        metric_keys = [m["key"] for m in metrics]
        assert "score" in metric_keys
        assert "duplicates" in metric_keys

    def test_get_duplicate_keys(self):
        agg = DuplicateAggregator(["col1"])
        df = pd.DataFrame({"col1": [1, 1, 2, 3, 3]})
        agg.add_df(df)

        dup_keys = agg.get_duplicate_keys()
        assert (1,) in dup_keys
        assert (3,) in dup_keys
        assert (2,) not in dup_keys

    def test_multi_column_uniqueness(self):
        agg = DuplicateAggregator(["col1", "col2"])
        df = pd.DataFrame(
            {
                "col1": [1, 1, 2],
                "col2": ["a", "b", "a"],
            }
        )
        agg.add_df(df)
        # All combinations are unique
        dup_keys = agg.get_duplicate_keys()
        assert len(dup_keys) == 0

    def test_sanitize_key_tuple_with_nan(self):
        agg = DuplicateAggregator(["col1"])
        df = pd.DataFrame({"col1": [1, np.nan, np.nan]})
        agg.add_df(df)
        # NaN should be sanitized to None
        assert (None,) in agg.combo_to_count or any(
            k[0] is None for k in agg.combo_to_count.keys()
        )


class TestTimelinessAggregator:
    """Tests for TimelinessAggregator class."""

    def test_init(self):
        agg = TimelinessAggregator()
        assert agg.date_cols == {}

    def test_add_year_obs(self):
        agg = TimelinessAggregator()
        agg.add_year_obs("year_col", earliest_year=2020, latest_year=2024)
        assert "year_col" in agg.date_cols
        assert agg.date_cols["year_col"]["kind"] == "year"
        assert agg.date_cols["year_col"]["min"] == 2020
        assert agg.date_cols["year_col"]["max"] == 2024

    def test_add_year_obs_updates_min_max(self):
        agg = TimelinessAggregator()
        agg.add_year_obs("year_col", 2020, 2022)
        agg.add_year_obs("year_col", 2018, 2024)
        assert agg.date_cols["year_col"]["min"] == 2018
        assert agg.date_cols["year_col"]["max"] == 2024

    def test_add_date_obs(self):
        agg = TimelinessAggregator()
        earliest = dt.date(2020, 1, 1)
        latest = dt.date(2024, 12, 31)
        agg.add_date_obs("date_col", earliest, latest)
        assert "date_col" in agg.date_cols
        assert agg.date_cols["date_col"]["kind"] == "date"
        assert agg.date_cols["date_col"]["min"] == earliest
        assert agg.date_cols["date_col"]["max"] == latest

    def test_add_date_obs_updates_min_max(self):
        agg = TimelinessAggregator()
        agg.add_date_obs("date_col", dt.date(2020, 6, 1), dt.date(2022, 6, 1))
        agg.add_date_obs("date_col", dt.date(2019, 1, 1), dt.date(2024, 12, 1))
        assert agg.date_cols["date_col"]["min"] == dt.date(2019, 1, 1)
        assert agg.date_cols["date_col"]["max"] == dt.date(2024, 12, 1)

    def test_finalize_metrics_year(self):
        agg = TimelinessAggregator()
        agg.add_year_obs("year_col", 2020, 2024)

        def calc_score(days):
            return max(0, 1 - (days / 365))

        metrics, recommendations = agg.finalize_metrics(
            "test_dataset",
            compute_score_columns=None,
            calc_timeliness_score=calc_score,
        )

        metric_keys = [m["key"] for m in metrics]
        assert "earliest_year" in metric_keys
        assert "latest_year" in metric_keys

    def test_finalize_metrics_date(self):
        agg = TimelinessAggregator()
        recent_date = dt.date.today() - dt.timedelta(days=30)
        agg.add_date_obs("date_col", dt.date(2020, 1, 1), recent_date)

        def calc_score(days):
            return max(0, 1 - (days / 365))

        metrics, recommendations = agg.finalize_metrics(
            "test_dataset",
            compute_score_columns=None,
            calc_timeliness_score=calc_score,
        )

        metric_keys = [m["key"] for m in metrics]
        assert "earliest_date" in metric_keys
        assert "latest_date" in metric_keys

    def test_finalize_metrics_datetime(self):
        """Test that datetime objects also work (converted to date internally)."""
        agg = TimelinessAggregator()
        recent_dt = dt.datetime.today() - dt.timedelta(days=30)
        earliest_dt = dt.datetime(2020, 1, 1)
        agg.add_date_obs("date_col", earliest_dt, recent_dt)

        def calc_score(days):
            return max(0, 1 - (days / 365))

        metrics, recommendations = agg.finalize_metrics(
            "test_dataset",
            compute_score_columns=None,
            calc_timeliness_score=calc_score,
        )

        metric_keys = [m["key"] for m in metrics]
        assert "earliest_date" in metric_keys
        assert "latest_date" in metric_keys

    def test_old_data_generates_recommendation(self):
        agg = TimelinessAggregator()
        # Very old data (more than 1 year)
        agg.add_year_obs("old_col", 2015, 2018)

        def calc_score(days):
            return max(0, 1 - (days / 365))

        metrics, recommendations = agg.finalize_metrics(
            "test_dataset",
            compute_score_columns=None,
            calc_timeliness_score=calc_score,
        )

        # Should generate recommendations for old data
        assert len(recommendations) > 0

    def test_score_aggregation_with_eligible_columns(self):
        agg = TimelinessAggregator()
        agg.add_year_obs("col1", 2020, 2024)
        agg.add_year_obs("col2", 2015, 2018)  # Old column

        def calc_score(days):
            return max(0, 1 - (days / 365))

        # Only compute score for col1
        metrics, _ = agg.finalize_metrics(
            "test_dataset",
            compute_score_columns=["col1"],
            calc_timeliness_score=calc_score,
        )

        score_metric = [m for m in metrics if m["key"] == "score"]
        assert len(score_metric) == 1


class TestDetermineRecommendationLevel:
    """Tests for _determine_recommendation_level helper function."""

    def test_high_proportion(self):
        assert _determine_recommendation_level(0.6) == "high"
        assert _determine_recommendation_level(0.8) == "high"

    def test_warning_proportion(self):
        assert _determine_recommendation_level(0.4) == "warning"
        assert _determine_recommendation_level(0.5) == "warning"

    def test_info_proportion(self):
        assert _determine_recommendation_level(0.2) == "info"
        assert _determine_recommendation_level(0.1) == "info"

    def test_boundary_values(self):
        assert _determine_recommendation_level(0.3) == "info"
        assert _determine_recommendation_level(0.31) == "warning"
        assert _determine_recommendation_level(0.5) == "warning"
        assert _determine_recommendation_level(0.51) == "high"


class TestCompletenessAggregatorPolars:
    """The streaming path: LazyFrames in, one pass each."""

    def test_add_lf_counts_nulls_per_column(self):
        agg = CompletenessAggregator()
        frame = pl.DataFrame({"a": [1, None, 3], "b": [None, None, "x"]})
        agg.add_lf(frame.lazy())
        assert agg.total_rows == 3
        assert agg.per_column["a"] == {"non_null": 2, "rows": 3}
        assert agg.per_column["b"] == {"non_null": 1, "rows": 3}
        assert agg.total_cells == 6
        assert agg.total_non_null_cells == 3

    def test_chunks_accumulate(self):
        agg = CompletenessAggregator()
        agg.add_lf(pl.DataFrame({"a": [1, None]}).lazy())
        agg.add_lf(pl.DataFrame({"a": [3, 4]}).lazy())
        assert agg.total_rows == 4
        assert agg.per_column["a"]["non_null"] == 3

    def test_add_pl_matches_add_lf(self):
        frame = pl.DataFrame({"a": [1, None, 3]})
        lazy_agg = CompletenessAggregator()
        lazy_agg.add_lf(frame.lazy())
        eager_agg = CompletenessAggregator()
        eager_agg.add_pl(frame)
        assert eager_agg.per_column == lazy_agg.per_column
        assert eager_agg.total_rows == lazy_agg.total_rows

    def test_add_dispatches_on_type(self):
        agg = CompletenessAggregator()
        agg.add(pl.DataFrame({"a": [1, 2]}).lazy())
        agg.add(pl.DataFrame({"a": [3]}))
        agg.add(pd.DataFrame({"a": [4]}))
        assert agg.total_rows == 4

    def test_add_refuses_an_unsupported_type(self):
        with pytest.raises(TypeError):
            CompletenessAggregator().add([{"a": 1}])

    def test_a_frame_with_no_rows_is_not_an_error(self):
        agg = CompletenessAggregator()
        agg.add_lf(pl.DataFrame(schema={"a": pl.Int64}).lazy())
        assert agg.total_rows == 0
        assert agg.per_column["a"] == {"non_null": 0, "rows": 0}
        assert agg.unique_columns == {"a"}

    def test_a_frame_with_no_columns_is_not_an_error(self):
        agg = CompletenessAggregator()
        agg.add_lf(pl.DataFrame().lazy())
        assert agg.total_rows == 0
        assert agg.per_column == {}

    def test_metrics_match_the_pandas_path(self):
        frame = pd.DataFrame({"a": [1, None, 3], "b": [1, 2, 3]})
        pandas_agg = CompletenessAggregator()
        pandas_agg.add_df(frame)
        polars_agg = CompletenessAggregator()
        polars_agg.add_lf(pl.from_pandas(frame).lazy())
        assert polars_agg.finalize_metrics_and_schemas(
            "d"
        ) == pandas_agg.finalize_metrics_and_schemas("d")


class TestDuplicateAggregatorPolars:
    """The streaming path: Polars owns the group state, not a Python dict."""

    def test_duplicates_are_counted_across_chunks(self):
        # id 2 appears once per chunk: only a group-by that sees both chunks
        # can call it a duplicate.
        agg = DuplicateAggregator(["id"])
        agg.add_lf(pl.DataFrame({"id": [1, 2]}).lazy())
        agg.add_lf(pl.DataFrame({"id": [2, 3]}).lazy())
        assert agg.total_rows == 4
        assert agg.duplicate_count() == 1

    def test_no_python_dict_is_built_on_the_lazy_path(self):
        agg = DuplicateAggregator(["id"])
        agg.add_lf(pl.DataFrame({"id": list(range(1000))}).lazy())
        agg.duplicate_count()
        assert agg.combo_to_count == {}

    def test_finalize_metrics(self):
        agg = DuplicateAggregator(["id"])
        agg.add_lf(pl.DataFrame({"id": [1, 1, 2, 3]}).lazy())
        metrics, _ = agg.finalize_metrics("test_dataset")
        by_key = {m["key"]: m["value"] for m in metrics}
        assert by_key["duplicates"] == 1
        assert by_key["score"] == "0.75"

    def test_multi_column_keys(self):
        agg = DuplicateAggregator(["a", "b"])
        agg.add_pl(pl.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "x"]}))
        assert agg.duplicate_count() == 1
        assert agg.get_duplicate_keys() == [(1, "x")]

    def test_get_duplicate_keys_is_bounded(self):
        agg = DuplicateAggregator(["id"])
        # 500 distinct keys, every one duplicated: the unbounded version
        # returned 500 tuples, i.e. the dataset.
        agg.add_lf(pl.DataFrame({"id": list(range(500)) * 2}).lazy())
        assert len(agg.get_duplicate_keys(limit=10)) == 10

    def test_get_duplicate_keys_returns_the_worst_first(self):
        agg = DuplicateAggregator(["id"])
        agg.add_lf(pl.DataFrame({"id": [1, 1, 2, 2, 2, 3]}).lazy())
        assert agg.get_duplicate_keys(limit=1) == [(2,)]

    def test_get_duplicate_key_counts_columns(self):
        agg = DuplicateAggregator(["id"])
        agg.add_lf(pl.DataFrame({"id": [1, 1, 2]}).lazy())
        counts = agg.get_duplicate_key_counts()
        assert counts.columns == ["id", "count"]
        assert counts.to_dicts() == [{"id": 1, "count": 2}]

    def test_get_duplicate_keys_refuses_an_empty_bound(self):
        agg = DuplicateAggregator(["id"])
        agg.add_lf(pl.DataFrame({"id": [1, 1]}).lazy())
        with pytest.raises(ValueError, match="limit must be positive"):
            agg.get_duplicate_keys(limit=0)

    def test_registering_a_frame_invalidates_the_counts(self):
        agg = DuplicateAggregator(["id"])
        agg.add_lf(pl.DataFrame({"id": [1, 2]}).lazy())
        assert agg.duplicate_count() == 0
        agg.add_lf(pl.DataFrame({"id": [1]}).lazy())
        assert agg.duplicate_count() == 1
        assert agg.total_rows == 3

    def test_nothing_added_is_not_an_error(self):
        agg = DuplicateAggregator(["id"])
        assert agg.total_rows == 0
        assert agg.duplicate_count() == 0
        assert agg.get_duplicate_keys() == []


class TestTimelinessAggregatorAddLf:
    """One batched min/max pass instead of one distinct-value pass per column."""

    def test_dates_and_years_in_a_single_pass(self):
        frame = pl.DataFrame(
            {
                "d": [dt.date(2020, 1, 1), dt.date(2024, 6, 1), None],
                "y": [2015, 2018, None],
                "label": ["a", "b", "c"],
            }
        )
        agg = TimelinessAggregator()
        agg.add_lf(frame.lazy(), ["d", "y", "label"])
        assert agg.date_cols["d"] == {
            "kind": "date",
            "min": dt.date(2020, 1, 1),
            "max": dt.date(2024, 6, 1),
        }
        assert agg.date_cols["y"] == {"kind": "year", "min": 2015, "max": 2018}
        # a string column is neither a date nor a year: skipped, not guessed
        assert "label" not in agg.date_cols

    def test_datetime_columns_are_observed(self):
        frame = pl.DataFrame(
            {"ts": [dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1)]}
        )
        agg = TimelinessAggregator()
        agg.add_lf(frame.lazy(), ["ts"])
        assert agg.date_cols["ts"]["kind"] == "date"
        assert agg.date_cols["ts"]["max"] == dt.datetime(2021, 1, 1)

    def test_chunks_widen_the_interval(self):
        agg = TimelinessAggregator()
        agg.add_lf(pl.DataFrame({"d": [dt.date(2021, 1, 1)]}).lazy(), ["d"])
        agg.add_lf(
            pl.DataFrame(
                {"d": [dt.date(2019, 1, 1), dt.date(2023, 1, 1)]}
            ).lazy(),
            ["d"],
        )
        assert agg.date_cols["d"]["min"] == dt.date(2019, 1, 1)
        assert agg.date_cols["d"]["max"] == dt.date(2023, 1, 1)

    def test_all_null_column_is_ignored(self):
        frame = pl.DataFrame({"d": [None, None]}, schema={"d": pl.Date})
        agg = TimelinessAggregator()
        agg.add_lf(frame.lazy(), ["d"])
        assert agg.date_cols == {}

    def test_defaults_to_every_column(self):
        frame = pl.DataFrame({"d": [dt.date(2020, 1, 1)]})
        agg = TimelinessAggregator()
        agg.add_lf(frame.lazy())
        assert "d" in agg.date_cols

    def test_finalize_after_add_lf(self):
        frame = pl.DataFrame({"d": [dt.date(2020, 1, 1)]})
        agg = TimelinessAggregator()
        agg.add_lf(frame.lazy(), ["d"])
        metrics, recommendations = agg.finalize_metrics(
            "ds", None, lambda days: max(0.0, 1 - days / 365)
        )
        keys = [m["key"] for m in metrics]
        assert "earliest_date" in keys and "latest_date" in keys
        assert recommendations  # 2020 is more than a year old


class TestPandasIsOptional:
    """pandas must not be imported by importing qalita_core."""

    def test_importing_qalita_core_does_not_import_pandas(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, qalita_core; print('pandas' in sys.modules)",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout.strip() == "False"

    def test_importing_aggregation_does_not_import_pandas(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, qalita_core.aggregation as a;"
                " print('pandas' in sys.modules)",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout.strip() == "False"

    def test_missing_values_are_detected_without_pandas(self):
        agg = DuplicateAggregator(["a"])
        assert agg._sanitize_key_tuple((np.nan, None, 1)) == (None, None, 1)
