import pandas as pd


def sanitize_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Return a sanitized copy of the DataFrame safe for parquet writing.

    - Ensures column names are strings
    - Decodes bytes/bytearray to UTF-8 strings
    - Normalizes mixed-type object columns
    - Casts categoricals to strings when needed
    """
    clean_df = df.copy()

    try:
        clean_df.columns = [c if isinstance(c, str) else str(c) for c in clean_df.columns]
    except Exception:
        pass

    for column_name in list(clean_df.columns):
        series = clean_df[column_name]

        if pd.api.types.is_object_dtype(series):
            try:
                has_bytes_like = series.map(lambda x: isinstance(x, (bytes, bytearray))).any()
            except Exception:
                has_bytes_like = False
            if has_bytes_like:
                series = series.map(
                    lambda x: x.decode("utf-8", errors="replace") if isinstance(x, (bytes, bytearray)) else x
                )

            try:
                _ = pd.to_numeric(series.dropna(), errors="raise")
                series = pd.to_numeric(series, errors="coerce")
            except Exception:
                try:
                    series = series.astype("string")
                except Exception:
                    series = series.map(lambda x: None if pd.isna(x) else str(x))

        elif isinstance(series.dtype, pd.CategoricalDtype):
            try:
                series = series.astype("string")
            except Exception:
                pass

        clean_df[column_name] = series

    return clean_df


def install_pandas_parquet_sanitization() -> None:
    """Monkeypatch pandas.DataFrame.to_parquet to sanitize before writing.

    This is idempotent and safe to call multiple times.
    """
    if getattr(pd.DataFrame, "_qalita_safe_to_parquet_installed", False):
        return

    try:
        original_to_parquet = pd.DataFrame.to_parquet

        def _safe_to_parquet(self, *args, **kwargs):  # type: ignore[override]
            kwargs.setdefault("engine", "pyarrow")
            try:
                sanitized = sanitize_dataframe_for_parquet(self)
                return original_to_parquet(sanitized, *args, **kwargs)
            except Exception:
                fallback = self.copy()
                for column_name in fallback.columns:
                    series = fallback[column_name]
                    if pd.api.types.is_object_dtype(series) or isinstance(series.dtype, pd.CategoricalDtype):
                        series = series.map(
                            lambda x: x.decode("utf-8", errors="replace") if isinstance(x, (bytes, bytearray)) else x
                        )
                        try:
                            series = series.astype("string")
                        except Exception:
                            series = series.map(lambda x: None if pd.isna(x) else str(x))
                    fallback[column_name] = series
                return original_to_parquet(fallback, *args, **kwargs)

        pd.DataFrame.to_parquet = _safe_to_parquet  # type: ignore[assignment]
        setattr(pd.DataFrame, "_qalita_safe_to_parquet_installed", True)
    except Exception:
        # If monkeypatch fails for any reason, continue without it.
        pass


