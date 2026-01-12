"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.aggregation module
"""

import pytest
import pandas as pd
import numpy as np
import datetime as dt
from qalita_core.aggregation import (
    detect_chunked_from_items,
    normalize_and_dedupe_recommendations,
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
        result = detect_chunked_from_items(["file.parquet"], ["name"], "dataset")
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
                    "parent_scope": {"perimeter": "dataset", "value": "old_dataset"},
                },
            }
        ]
        result = normalize_and_dedupe_recommendations(records, "new_dataset")
        assert result[0]["scope"]["parent_scope"]["value"] == "new_dataset"

    def test_deduplicate_by_content_type_scope(self):
        records = [
            {"content": "duplicate", "type": "warning", "scope": {"value": "ds1"}},
            {"content": "duplicate", "type": "warning", "scope": {"value": "ds1"}},
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
        
        col_metrics = [m for m in metrics if m.get("scope", {}).get("perimeter") == "column"]
        assert len(col_metrics) > 0

    def test_empty_df(self):
        agg = CompletenessAggregator()
        df = pd.DataFrame()
        agg.add_df(df)
        metrics, schemas = agg.finalize_metrics_and_schemas("test")
        # Should not crash, metrics should be generated
        assert isinstance(metrics, list)


class TestOutlierAggregator:
    """Tests for OutlierAggregator class."""

    def test_init(self):
        agg = OutlierAggregator()
        assert agg.col_outliers == {}
        assert agg.col_norm_weighted_sum == {}
        assert agg.col_rows == {}
        assert agg.dataset_outliers == 0
        assert agg.total_rows == 0

    def test_add_column_stats(self):
        agg = OutlierAggregator()
        agg.add_column_stats("col1", mean_normality=0.9, outlier_count=5, rows=100)
        assert agg.col_outliers["col1"] == 5
        assert agg.col_rows["col1"] == 100

    def test_add_multiple_column_stats(self):
        agg = OutlierAggregator()
        agg.add_column_stats("col1", 0.9, 5, 100)
        agg.add_column_stats("col1", 0.8, 3, 50)
        assert agg.col_outliers["col1"] == 8
        assert agg.col_rows["col1"] == 150

    def test_add_dataset_stats(self):
        agg = OutlierAggregator()
        agg.add_dataset_stats(mean_normality=0.85, rows=100, multivariate_outliers_count=10)
        assert agg.dataset_outliers == 10
        assert agg.total_rows == 100

    def test_finalize_metrics_and_recommendations(self):
        agg = OutlierAggregator()
        agg.add_column_stats("col1", 0.9, 5, 100)
        agg.add_dataset_stats(0.85, 100, 10)
        
        metrics, recommendations = agg.finalize_metrics_and_recommendations(
            "test_dataset", normality_threshold=0.8
        )
        
        assert len(metrics) > 0
        metric_keys = [m["key"] for m in metrics]
        assert "outliers" in metric_keys
        assert "score" in metric_keys

    def test_low_normality_generates_recommendation(self):
        agg = OutlierAggregator()
        agg.add_column_stats("col1", 0.5, 50, 100)  # Low normality
        agg.add_dataset_stats(0.5, 100, 50)
        
        metrics, recommendations = agg.finalize_metrics_and_recommendations(
            "test_dataset", normality_threshold=0.8
        )
        
        # Should generate recommendations for low normality
        assert len(recommendations) > 0


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
        df = pd.DataFrame({
            "col1": [1, 1, 2],
            "col2": ["a", "b", "a"],
        })
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
