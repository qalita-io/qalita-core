"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.utils module
"""

import pytest
import pandas as pd
from qalita_core.utils import (
    get_version,
    determine_recommendation_level,
    extract_variable_name,
    round_if_numeric,
    determine_level,
    denormalize,
    slugify,
    replace_whitespaces_with_underscores,
    INFO_THRESHOLD,
    WARNING_THRESHOLD,
    HIGH_THRESHOLD,
)


class TestGetVersion:
    """Tests for get_version function."""

    def test_returns_string(self):
        version = get_version()
        assert isinstance(version, str)

    def test_version_format(self):
        version = get_version()
        # Should return a version string (dev or semver format)
        assert len(version) > 0


class TestDetermineRecommendationLevel:
    """Tests for determine_recommendation_level function."""

    def test_high_level(self):
        assert determine_recommendation_level(0.8) == "high"
        assert determine_recommendation_level(0.71) == "high"
        assert determine_recommendation_level(1.0) == "high"

    def test_warning_level(self):
        assert determine_recommendation_level(0.5) == "warning"
        assert determine_recommendation_level(0.31) == "warning"
        assert determine_recommendation_level(0.7) == "warning"

    def test_info_level(self):
        assert determine_recommendation_level(0.3) == "info"
        assert determine_recommendation_level(0.1) == "info"
        assert determine_recommendation_level(0.0) == "info"

    def test_boundary_values(self):
        # Exactly at boundary 0.3
        assert determine_recommendation_level(0.3) == "info"
        # Just above 0.3
        assert determine_recommendation_level(0.31) == "warning"
        # Exactly at boundary 0.7
        assert determine_recommendation_level(0.7) == "warning"
        # Just above 0.7
        assert determine_recommendation_level(0.71) == "high"


class TestExtractVariableName:
    """Tests for extract_variable_name function."""

    def test_extract_with_has(self):
        content = "column_name has many null values"
        assert extract_variable_name(content) == "column_name"

    def test_extract_with_is(self):
        content = "my_variable is missing data"
        assert extract_variable_name(content) == "my_variable"

    def test_no_match(self):
        content = "This does not match the pattern"
        assert extract_variable_name(content) == ""

    def test_complex_name(self):
        content = "user_email_address has invalid format"
        assert extract_variable_name(content) == "user_email_address"

    def test_name_with_spaces(self):
        content = "User Name is not valid"
        assert extract_variable_name(content) == "User Name"


class TestRoundIfNumeric:
    """Tests for round_if_numeric function."""

    def test_round_float(self):
        assert round_if_numeric(3.14159) == "3.14"
        assert round_if_numeric(2.5) == "2.5"
        assert round_if_numeric(10.0) == "10"

    def test_round_integer(self):
        assert round_if_numeric(42) == "42"
        assert round_if_numeric(100) == "100"

    def test_round_string_number(self):
        assert round_if_numeric("3.14159") == "3.14"
        assert round_if_numeric("42") == "42"

    def test_non_numeric(self):
        assert round_if_numeric("hello") == "hello"
        assert round_if_numeric("not a number") == "not a number"

    def test_custom_decimals(self):
        assert round_if_numeric(3.14159, decimals=4) == "3.1416"
        assert round_if_numeric(3.14159, decimals=1) == "3.1"

    def test_none_value(self):
        assert round_if_numeric(None) == "None"


class TestDetermineLevel:
    """Tests for determine_level function based on percentage thresholds."""

    def test_info_level(self):
        assert determine_level("The value is 50%") == "info"
        assert determine_level("Completeness: 70%") == "info"

    def test_warning_level(self):
        assert determine_level("The score is 85%") == "warning"
        assert determine_level("Progress: 90%") == "warning"

    def test_high_level(self):
        assert determine_level("Completion: 95%") == "high"
        assert determine_level("Reached 100%") == "high"

    def test_no_percentage(self):
        assert determine_level("No percentage here") == "info"

    def test_decimal_percentage(self):
        assert determine_level("Score: 75.5%") == "warning"
        assert determine_level("Rate: 95.5%") == "high"

    def test_boundary_values(self):
        # INFO_THRESHOLD = 70
        assert determine_level(f"Value: {INFO_THRESHOLD}%") == "info"
        assert determine_level("Value: 71%") == "warning"
        # WARNING_THRESHOLD = 90
        assert determine_level(f"Value: {WARNING_THRESHOLD}%") == "warning"
        assert determine_level("Value: 91%") == "high"


class TestDenormalize:
    """Tests for denormalize function."""

    def test_flat_dict(self):
        data = {"a": 1, "b": 2}
        result = denormalize(data)
        assert result == {"a": 1, "b": 2}

    def test_nested_dict(self):
        data = {
            "stats": {"count": 10, "total": 100},
            "name": "test",
        }
        result = denormalize(data)
        assert result == {
            "stats_count": 10,
            "stats_total": 100,
            "name": "test",
        }

    def test_empty_dict(self):
        result = denormalize({})
        assert result == {}

    def test_mixed_nesting(self):
        data = {
            "level1": {"nested_key": "value"},
            "flat_key": "flat_value",
        }
        result = denormalize(data)
        assert "level1_nested_key" in result
        assert "flat_key" in result


class TestSlugify:
    """Tests for slugify function."""

    def test_simple_string(self):
        assert slugify("Hello World") == "hello_world"

    def test_accented_characters(self):
        assert slugify("Héllo Wörld") == "hello_world"
        assert slugify("café") == "cafe"

    def test_special_characters(self):
        assert slugify("hello@world!") == "hello_world_"
        assert slugify("test#123") == "test_123"

    def test_multiple_spaces(self):
        assert slugify("hello   world") == "hello_world"

    def test_leading_trailing_spaces(self):
        assert slugify("  hello world  ") == "hello_world"

    def test_uppercase(self):
        assert slugify("HELLO WORLD") == "hello_world"

    def test_mixed_case(self):
        assert slugify("HeLLo WoRLd") == "hello_world"

    def test_numbers(self):
        assert slugify("test123") == "test123"
        assert slugify("123test") == "123test"

    def test_underscores(self):
        assert slugify("hello_world") == "hello_world"

    def test_consecutive_special_chars(self):
        result = slugify("hello---world")
        assert result == "hello_world"


class TestReplaceWhitespacesWithUnderscores:
    """Tests for replace_whitespaces_with_underscores function."""

    def test_basic_replacement(self):
        df = pd.DataFrame({"Hello World": [1, 2], "Test Column": [3, 4]})
        result_df, mapping = replace_whitespaces_with_underscores(df)
        assert "hello_world" in result_df.columns
        assert "test_column" in result_df.columns
        assert mapping["hello_world"] == "Hello World"
        assert mapping["test_column"] == "Test Column"

    def test_no_spaces(self):
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        result_df, mapping = replace_whitespaces_with_underscores(df)
        assert "col1" in result_df.columns
        assert "col2" in result_df.columns

    def test_accented_columns(self):
        df = pd.DataFrame({"Café Name": [1, 2]})
        result_df, mapping = replace_whitespaces_with_underscores(df)
        assert "cafe_name" in result_df.columns

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result_df, mapping = replace_whitespaces_with_underscores(df)
        assert len(result_df.columns) == 0
        assert mapping == {}

    def test_preserves_data(self):
        df = pd.DataFrame({"Test Col": [1, 2, 3]})
        result_df, _ = replace_whitespaces_with_underscores(df)
        assert list(result_df["test_col"]) == [1, 2, 3]


class TestThresholdConstants:
    """Tests to verify threshold constants are correctly defined."""

    def test_info_threshold(self):
        assert INFO_THRESHOLD == 70

    def test_warning_threshold(self):
        assert WARNING_THRESHOLD == 90

    def test_high_threshold(self):
        assert HIGH_THRESHOLD == 100

    def test_threshold_order(self):
        assert INFO_THRESHOLD < WARNING_THRESHOLD < HIGH_THRESHOLD
