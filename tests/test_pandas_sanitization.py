"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.pandas_sanitization module
"""

import pytest
import pandas as pd
import numpy as np
from qalita_core.pandas_sanitization import (
    sanitize_dataframe_for_parquet,
    install_pandas_parquet_sanitization,
)


class TestSanitizeDataframeForParquet:
    """Tests for sanitize_dataframe_for_parquet function."""

    def test_string_column_names(self):
        """Test that non-string column names are converted to strings."""
        df = pd.DataFrame({1: [1, 2], 2: [3, 4]})
        result = sanitize_dataframe_for_parquet(df)
        assert all(isinstance(col, str) for col in result.columns)
        assert "1" in result.columns
        assert "2" in result.columns

    def test_mixed_column_names(self):
        """Test mixed string/non-string column names."""
        df = pd.DataFrame({"col1": [1, 2], 3: [3, 4]})
        result = sanitize_dataframe_for_parquet(df)
        assert "col1" in result.columns
        assert "3" in result.columns

    def test_bytes_to_string_conversion(self):
        """Test that bytes values are converted to UTF-8 strings."""
        df = pd.DataFrame({"col": [b"hello", b"world"]})
        result = sanitize_dataframe_for_parquet(df)
        assert result["col"].iloc[0] == "hello"
        assert result["col"].iloc[1] == "world"

    def test_bytearray_to_string_conversion(self):
        """Test that bytearray values are converted to UTF-8 strings."""
        df = pd.DataFrame({"col": [bytearray(b"test"), bytearray(b"data")]})
        result = sanitize_dataframe_for_parquet(df)
        assert result["col"].iloc[0] == "test"
        assert result["col"].iloc[1] == "data"

    def test_mixed_bytes_and_strings(self):
        """Test mixed bytes and string values in same column."""
        df = pd.DataFrame({"col": [b"bytes", "string", b"more"]})
        result = sanitize_dataframe_for_parquet(df)
        # Should convert bytes and keep strings
        assert result["col"].iloc[0] == "bytes"
        assert result["col"].iloc[1] == "string"
        assert result["col"].iloc[2] == "more"

    def test_numeric_object_column_conversion(self):
        """Test that object columns with numeric values are converted."""
        df = pd.DataFrame({"col": ["1", "2", "3"]})
        result = sanitize_dataframe_for_parquet(df)
        # Should try to convert to numeric or string
        assert len(result["col"]) == 3

    def test_categorical_to_string(self):
        """Test that categorical columns are converted to strings."""
        df = pd.DataFrame({"col": pd.Categorical(["a", "b", "c"])})
        result = sanitize_dataframe_for_parquet(df)
        # Should be string type after sanitization
        assert result["col"].dtype == "string" or result["col"].dtype == object

    def test_preserves_data_integrity(self):
        """Test that data values are preserved after sanitization."""
        df = pd.DataFrame({"num": [1, 2, 3], "str": ["a", "b", "c"]})
        result = sanitize_dataframe_for_parquet(df)
        assert list(result["num"]) == [1, 2, 3]
        # String values might be converted to string dtype
        assert len(result["str"]) == 3

    def test_returns_copy(self):
        """Test that original dataframe is not modified."""
        df = pd.DataFrame({1: [1, 2]})
        original_columns = list(df.columns)
        result = sanitize_dataframe_for_parquet(df)
        assert list(df.columns) == original_columns  # Original unchanged

    def test_empty_dataframe(self):
        """Test handling of empty dataframe."""
        df = pd.DataFrame()
        result = sanitize_dataframe_for_parquet(df)
        assert len(result) == 0

    def test_dataframe_with_nan(self):
        """Test handling of NaN values."""
        df = pd.DataFrame({"col": [1.0, np.nan, 3.0]})
        result = sanitize_dataframe_for_parquet(df)
        assert pd.isna(result["col"].iloc[1])

    def test_object_column_with_none(self):
        """Test object column with None values."""
        df = pd.DataFrame({"col": ["a", None, "c"]})
        result = sanitize_dataframe_for_parquet(df)
        assert len(result["col"]) == 3

    def test_mixed_type_object_column(self):
        """Test object column with mixed types."""
        df = pd.DataFrame({"col": [1, "two", 3.0, None]})
        result = sanitize_dataframe_for_parquet(df)
        assert len(result["col"]) == 4

    def test_invalid_utf8_bytes(self):
        """Test handling of invalid UTF-8 bytes."""
        # Create bytes that are not valid UTF-8
        df = pd.DataFrame({"col": [b"\xff\xfe", b"valid"]})
        result = sanitize_dataframe_for_parquet(df)
        # Should handle with errors='replace'
        assert len(result["col"]) == 2


class TestInstallPandasParquetSanitization:
    """Tests for install_pandas_parquet_sanitization function."""

    def test_installation_is_idempotent(self):
        """Test that calling install multiple times is safe."""
        # Call install multiple times
        install_pandas_parquet_sanitization()
        install_pandas_parquet_sanitization()
        install_pandas_parquet_sanitization()
        # Should not raise any errors
        assert hasattr(pd.DataFrame, "_qalita_safe_to_parquet_installed")

    def test_marker_attribute_set(self):
        """Test that the marker attribute is set after installation."""
        install_pandas_parquet_sanitization()
        assert getattr(
            pd.DataFrame, "_qalita_safe_to_parquet_installed", False
        )

    def test_to_parquet_method_exists(self):
        """Test that to_parquet method still exists after patching."""
        install_pandas_parquet_sanitization()
        df = pd.DataFrame({"col": [1, 2, 3]})
        assert hasattr(df, "to_parquet")


class TestSanitizationIntegration:
    """Integration tests for sanitization with parquet writing."""

    def test_sanitized_df_can_write_parquet(self, tmp_path):
        """Test that sanitized dataframe can be written to parquet."""
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "str_col": ["a", "b", "c"],
                "float_col": [1.1, 2.2, 3.3],
            }
        )
        result = sanitize_dataframe_for_parquet(df)
        output_path = tmp_path / "test.parquet"
        result.to_parquet(output_path, engine="pyarrow")
        assert output_path.exists()

    def test_problematic_df_can_write_after_sanitization(self, tmp_path):
        """Test that problematic dataframe can write after sanitization."""
        # Create a dataframe that might cause issues
        df = pd.DataFrame(
            {
                1: [1, 2],  # Non-string column name
                "bytes_col": [b"hello", b"world"],
            }
        )
        result = sanitize_dataframe_for_parquet(df)
        output_path = tmp_path / "test_sanitized.parquet"
        result.to_parquet(output_path, engine="pyarrow")
        assert output_path.exists()

        # Verify data can be read back
        read_df = pd.read_parquet(output_path)
        assert len(read_df) == 2

    def test_categorical_df_can_write_after_sanitization(self, tmp_path):
        """Test categorical columns after sanitization."""
        df = pd.DataFrame(
            {
                "cat_col": pd.Categorical(["low", "medium", "high"]),
                "normal_col": [1, 2, 3],
            }
        )
        result = sanitize_dataframe_for_parquet(df)
        output_path = tmp_path / "test_cat.parquet"
        result.to_parquet(output_path, engine="pyarrow")
        assert output_path.exists()


class TestSanitizationIsNotInstalledOnImport:
    """Importing qalita_core must not patch pandas, nor import it."""

    def _run(self, code):
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def test_importing_qalita_core_does_not_import_pandas(self):
        assert (
            self._run(
                "import sys, qalita_core; print('pandas' in sys.modules)"
            )
            == "False"
        )

    def test_importing_qalita_core_does_not_patch_to_parquet(self):
        # The patch copies the frame on every write, doubling peak RAM; a pack
        # that never asked for it must never pay for it.
        assert (
            self._run(
                "import qalita_core, pandas as pd;"
                " print(getattr(pd.DataFrame,"
                " '_qalita_safe_to_parquet_installed', False))"
            )
            == "False"
        )

    def test_the_helpers_are_still_reachable_from_the_package(self):
        assert (
            self._run(
                "import qalita_core;"
                " print(callable(qalita_core.sanitize_dataframe_for_parquet)"
                " and callable("
                "qalita_core.install_pandas_parquet_sanitization))"
            )
            == "True"
        )

    def test_an_unknown_attribute_still_raises(self):
        import qalita_core

        with pytest.raises(AttributeError):
            qalita_core.does_not_exist


class TestSanitizeCopySemantics:
    """`copy=False` exists to avoid doubling peak RAM on a big frame."""

    def test_copy_true_leaves_the_input_alone(self):
        df = pd.DataFrame({"col": [b"a", b"b"]})
        sanitize_dataframe_for_parquet(df, copy=True)
        assert df["col"].iloc[0] == b"a"

    def test_copy_false_sanitizes_in_place(self):
        df = pd.DataFrame({"col": [b"a", b"b"]})
        out = sanitize_dataframe_for_parquet(df, copy=False)
        assert out is df
        assert df["col"].iloc[0] == "a"


class TestSanitizeWarningsAndFallbacks:
    """Covers the warning and error-fallback branches."""

    def test_warn_deprecated_emits_warning(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.warns(DeprecationWarning):
            sanitize_dataframe_for_parquet(df, warn_deprecated=True)

    def test_astype_string_failure_falls_back_to_str(self):
        # A dict value makes to_numeric raise and astype("string") fail, so the
        # final apply(str) fallback must kick in.
        df = pd.DataFrame({"col": [{"a": 1}, "x"]})
        out = sanitize_dataframe_for_parquet(df)
        assert out["col"].iloc[0] == "{'a': 1}"
        assert out["col"].iloc[1] == "x"


class TestInstallFallbackBranches:
    """Covers the skip_sanitize and error-fallback paths of the hook."""

    def test_skip_sanitize_passes_through(self, tmp_path):
        install_pandas_parquet_sanitization()
        df = pd.DataFrame({"col": [b"a", b"b"]})
        p = tmp_path / "out.parquet"
        df.to_parquet(p, _skip_sanitize=True)
        assert p.exists()

    def test_sanitize_failure_falls_back(self, tmp_path, monkeypatch):
        import qalita_core.pandas_sanitization as ps

        def _boom(*args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(ps, "sanitize_dataframe_for_parquet", _boom)
        install_pandas_parquet_sanitization()
        df = pd.DataFrame({"col": [b"a", b"b"]})
        p = tmp_path / "out.parquet"
        df.to_parquet(p)
        assert p.exists()

    def test_install_swallows_patch_failure(self, monkeypatch):
        class _ReadOnly:
            @property
            def to_parquet(self):
                return None

            @to_parquet.setter
            def to_parquet(self, value):
                raise AttributeError("read-only")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", _ReadOnly.to_parquet)
        monkeypatch.setattr(
            pd.DataFrame, "_qalita_safe_to_parquet_installed", False
        )
        # Must not raise even though the assignment is rejected.
        install_pandas_parquet_sanitization()
