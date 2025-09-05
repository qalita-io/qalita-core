from .pandas_sanitization import install_pandas_parquet_sanitization, sanitize_dataframe_for_parquet

# Install the sanitizing parquet hook at import time
install_pandas_parquet_sanitization()

__all__ = [
    "install_pandas_parquet_sanitization",
    "sanitize_dataframe_for_parquet",
]


