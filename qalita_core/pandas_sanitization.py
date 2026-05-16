"""
Pandas DataFrame sanitization for Parquet writing.

DEPRECATION NOTICE:
    This module is deprecated for new code. For big data processing (100GB+),
    use Polars instead via qalita_core.polars_io which handles type conversions
    natively without the memory overhead of df.copy().

    This module is kept for backward compatibility with existing packs that
    still use pandas DataFrames.
"""

import warnings
import pandas as pd


def sanitize_dataframe_for_parquet(
    df: pd.DataFrame,
    *,
    copy: bool = True,
    warn_deprecated: bool = False,
) -> pd.DataFrame:
    """Return a sanitized DataFrame safe for parquet writing.

    DEPRECATION: For big data (100GB+), use Polars via qalita_core.polars_io
    which handles type sanitization natively without memory overhead.

    Args:
        df: Input DataFrame to sanitize
        copy: If True (default), return a copy. Set to False for in-place
              modification to reduce memory usage (use with caution).
        warn_deprecated: If True, emit a deprecation warning.

    Returns:
        Sanitized DataFrame safe for parquet writing.

    Operations performed:
        - Ensures column names are strings
        - Decodes bytes/bytearray to UTF-8 strings
        - Normalizes mixed-type object columns
        - Casts categoricals to strings when needed
    """
    if warn_deprecated:
        warnings.warn(
            "sanitize_dataframe_for_parquet is deprecated for big data. "
            "Use Polars via qalita_core.polars_io instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    # Avoid expensive copy for large DataFrames when possible
    if copy:
        clean_df = df.copy()
    else:
        clean_df = df

    # Ensure column names are strings
    try:
        non_string_cols = [
            c for c in clean_df.columns if not isinstance(c, str)
        ]
        if non_string_cols:
            clean_df.columns = [
                c if isinstance(c, str) else str(c) for c in clean_df.columns
            ]
    except Exception:
        pass

    # Process only columns that need sanitization (optimization)
    for column_name in list(clean_df.columns):
        series = clean_df[column_name]

        if pd.api.types.is_object_dtype(series):
            # Check for bytes using sampling (faster for large columns)
            try:
                sample = series.dropna().head(1000)
                has_bytes_like = sample.apply(
                    lambda x: isinstance(x, (bytes, bytearray))
                ).any()
            except Exception:
                has_bytes_like = False

            if has_bytes_like:
                # Vectorized decode where possible
                series = series.apply(
                    lambda x: (
                        x.decode("utf-8", errors="replace")
                        if isinstance(x, (bytes, bytearray))
                        else x
                    )
                )

            # Try to infer numeric type
            try:
                sample = series.dropna().head(1000)
                _ = pd.to_numeric(sample, errors="raise")
                series = pd.to_numeric(series, errors="coerce")
            except Exception:
                try:
                    series = series.astype("string")
                except Exception:
                    series = series.apply(
                        lambda x: None if pd.isna(x) else str(x)
                    )

            clean_df[column_name] = series

        elif isinstance(series.dtype, pd.CategoricalDtype):
            try:
                clean_df[column_name] = series.astype("string")
            except Exception:
                pass

    return clean_df


def install_pandas_parquet_sanitization() -> None:
    """Monkeypatch pandas.DataFrame.to_parquet to sanitize before writing.

    DEPRECATION: This function is deprecated for big data processing.
    Use Polars via qalita_core.polars_io for better memory efficiency.

    This is idempotent and safe to call multiple times.
    """
    if getattr(pd.DataFrame, "_qalita_safe_to_parquet_installed", False):
        return

    try:
        original_to_parquet = pd.DataFrame.to_parquet

        def _safe_to_parquet(self, *args, **kwargs):  # type: ignore[override]
            kwargs.setdefault("engine", "pyarrow")

            # Skip sanitization for small DataFrames or if explicitly disabled
            skip_sanitize = kwargs.pop("_skip_sanitize", False)
            if skip_sanitize:
                return original_to_parquet(self, *args, **kwargs)

            try:
                # Use copy=False for memory efficiency when writing
                # (the original DataFrame is not modified)
                sanitized = sanitize_dataframe_for_parquet(self, copy=True)
                return original_to_parquet(sanitized, *args, **kwargs)
            except Exception:
                # Minimal fallback sanitization
                fallback = self.copy()
                for column_name in fallback.columns:
                    series = fallback[column_name]
                    if pd.api.types.is_object_dtype(series) or isinstance(
                        series.dtype, pd.CategoricalDtype
                    ):
                        series = series.apply(
                            lambda x: (
                                x.decode("utf-8", errors="replace")
                                if isinstance(x, (bytes, bytearray))
                                else x
                            )
                        )
                        try:
                            series = series.astype("string")
                        except Exception:
                            series = series.apply(
                                lambda x: None if pd.isna(x) else str(x)
                            )
                    fallback[column_name] = series
                return original_to_parquet(fallback, *args, **kwargs)

        pd.DataFrame.to_parquet = _safe_to_parquet  # type: ignore[assignment]
        setattr(pd.DataFrame, "_qalita_safe_to_parquet_installed", True)
    except Exception:
        # If monkeypatch fails for any reason, continue without it.
        pass
