"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import os
import json
import base64
import logging
from typing import List, Optional, Union, TYPE_CHECKING
from qalita_core.data_source_opener import (
    get_data_source,
    cleanup_parquet_files,
)
from urllib.parse import urlsplit
import math
from decimal import Decimal
import datetime as _dt

# Polars support for big data (100GB+)
try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    pl = None  # type: ignore
    POLARS_AVAILABLE = False

if TYPE_CHECKING:
    import polars as pl


class Pack:
    """
    Represents a pack in the system, handling configurations and data loading.
    """

    # Default configuration paths
    default_configs = {
        "pack_conf": "pack_conf.json",
        "source_conf": "source_conf.json",
        "target_conf": "target_conf.json",
        "agent_file": "~/.qalita/.worker",
    }

    def __init__(self, configs=None):
        self.logger = logging.getLogger(self.__class__.__name__)

        if configs is None:
            configs = {}

        # Update default paths with any provided configurations
        self.config_paths = {**self.default_configs, **configs}
        self.pack_config = ConfigLoader.load_config(
            self.config_paths["pack_conf"]
        )
        self.source_config = ConfigLoader.load_config(
            self.config_paths["source_conf"]
        )
        self.target_config = ConfigLoader.load_config(
            self.config_paths["target_conf"]
        )
        self.agent_config = self.load_agent_config(
            self.config_paths["agent_file"]
        )
        self.metrics = PlatformAsset("metrics")
        self.recommendations = PlatformAsset("recommendations")
        self.schemas = PlatformAsset("schemas")

        # Import local : figures.py dérive de PlatformAsset, défini plus bas
        # dans ce même module — un import en tête créerait un cycle.
        from qalita_core.figures import FiguresAsset

        self.figures = FiguresAsset()

        # Initialize paths for cleanup tracking
        self.paths_source = None
        self.paths_target = None
        self.df_source = None
        self.df_target = None

        # Validate configurations
        if not self.source_config:
            self.logger.error("Source configuration is empty.")
        elif "type" not in self.source_config:
            self.logger.error(
                "Source configuration is missing the 'type' key."
            )

    def __enter__(self):
        """Context manager entry - returns self for use in 'with' statements."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup of temporary parquet files."""
        self.cleanup()
        # Don't suppress exceptions - return False or None
        return False

    def cleanup(self):
        """
        Remove temporary parquet files created during data loading.

        This method should be called when the analysis is complete (success or failure)
        to free up disk space. It is automatically called when using Pack as a context manager.

        Returns:
            int: Total number of files removed.
        """
        total_removed = 0
        if self.paths_source:
            removed = cleanup_parquet_files(self.paths_source, self.logger)
            self.logger.debug(f"Cleaned up {removed} source parquet file(s)")
            total_removed += removed
        if self.paths_target:
            removed = cleanup_parquet_files(self.paths_target, self.logger)
            self.logger.debug(f"Cleaned up {removed} target parquet file(s)")
            total_removed += removed
        if total_removed > 0:
            self.logger.info(
                f"Cleaned up {total_removed} temporary parquet file(s)"
            )
        return total_removed

    def load_agent_config(self, agent_file_path):
        try:
            abs_agent_file_path = os.path.expanduser(
                agent_file_path
            )  # Resolve any user-relative paths
            with open(abs_agent_file_path, "r") as agent_file:
                encoded_content = agent_file.read()
                decoded_content = base64.b64decode(encoded_content).decode(
                    "utf-8"
                )
                data = json.loads(decoded_content)
                # Normalize local context URL to scheme://host without trailing API paths
                try:
                    local_ctx = data.get("context", {}).get("local", {})
                    original_url = local_ctx.get("url")
                    if isinstance(original_url, str) and original_url:
                        parts = urlsplit(original_url)
                        if parts.scheme and parts.netloc:
                            base_url = f"{parts.scheme}://{parts.netloc}"
                            local_ctx["url"] = base_url
                except Exception:
                    # Do not fail if normalization cannot be applied
                    pass
                return data
        except Exception as e:
            self.logger.error(f"Error loading agent configuration: {e}")
            return {}

    def load_data(self, trigger, table_or_query=None) -> List[str]:
        """
        Load data from source/target and return list of parquet file paths.

        Args:
            trigger: "source" or "target"
            table_or_query: Optional table name or SQL query

        Returns:
            List of parquet file paths containing the loaded data.
        """
        source_conf = (
            self.source_config if trigger == "source" else self.target_config
        )
        pack_conf = self.pack_config
        ds = get_data_source(source_conf)
        table_or_query = table_or_query or source_conf.get("config", {}).get(
            "table_or_query"
        )
        # Enrich pack_config with deterministic output directory and chunking hints
        job_cfg = (pack_conf or {}).get("job", {})
        trigger_cfg = (
            (job_cfg.get(trigger) or {}) if isinstance(job_cfg, dict) else {}
        )
        parquet_output_dir = (
            trigger_cfg.get("parquet_output_dir")
            or job_cfg.get("parquet_output_dir")
            or "./parquet"
        )
        chunk_rows = (
            trigger_cfg.get("chunk_rows")
            or job_cfg.get("chunk_rows")
            or 100000
        )
        effective_pack_conf = {
            **(pack_conf or {}),
            "parquet_output_dir": parquet_output_dir,
            "chunk_rows": chunk_rows,
            "_trigger": trigger,
        }
        paths = ds.get_data(table_or_query, pack_config=effective_pack_conf)
        if trigger == "source":
            # Keep legacy attribute names for backward compatibility
            self.paths_source = paths
            self.df_source = paths
            return self.paths_source
        elif trigger == "target":
            self.paths_target = paths
            self.df_target = paths
            return self.paths_target

    def scan_data(self, trigger: str) -> "pl.LazyFrame":
        """
        Return a Polars LazyFrame for streaming data processing.

        This is the recommended method for big data (100GB+) as it uses
        lazy evaluation and streaming to process data without loading
        everything into memory.

        Args:
            trigger: "source" or "target"

        Returns:
            Polars LazyFrame for deferred/streaming execution.

        Example:
            ```python
            with Pack() as pack:
                pack.load_data("source")
                lf = pack.scan_data("source")

                # Streaming aggregation (memory efficient)
                stats = lf.select([
                    pl.len().alias("row_count"),
                    pl.all().is_not_null().sum()
                ]).collect(engine="streaming")
            ```

        Raises:
            ImportError: If Polars is not installed
            RuntimeError: If data has not been loaded yet
        """
        if not POLARS_AVAILABLE:
            raise ImportError(
                "Polars is required for scan_data(). "
                "Install with: pip install polars>=1.0.0"
            )

        paths = self.paths_source if trigger == "source" else self.paths_target
        if not paths:
            raise RuntimeError(
                f"No data loaded for trigger '{trigger}'. "
                f"Call load_data('{trigger}') first."
            )

        # Scan parquet files lazily (does not load into memory)
        return pl.scan_parquet(paths)

    def get_row_count(self, trigger: str) -> int:
        """
        Get row count efficiently without loading all data into memory.

        Uses Polars streaming if available, falls back to parquet metadata.

        Args:
            trigger: "source" or "target"

        Returns:
            Total number of rows across all parquet files.
        """
        paths = self.paths_source if trigger == "source" else self.paths_target
        if not paths:
            return 0

        if POLARS_AVAILABLE:
            try:
                lf = pl.scan_parquet(paths)
                return lf.select(pl.len()).collect(engine="streaming").item()
            except Exception:
                pass

        # Fallback: read parquet metadata
        try:
            import pyarrow.parquet as pq

            total = 0
            for path in paths:
                pf = pq.ParquetFile(path)
                total += pf.metadata.num_rows
            return total
        except Exception:
            return 0

    def estimate_memory_mb(self, trigger: str) -> float:
        """
        Estimate memory required to load all data into memory.

        Useful for deciding whether to use streaming or full load.

        Args:
            trigger: "source" or "target"

        Returns:
            Estimated memory in megabytes.
        """
        paths = self.paths_source if trigger == "source" else self.paths_target
        if not paths:
            return 0.0

        # Sum file sizes as rough estimate (parquet is compressed)
        total_bytes = 0
        for path in paths:
            try:
                total_bytes += os.path.getsize(path)
            except OSError:
                pass

        # Multiply by ~3-5x for decompression overhead
        return (total_bytes * 4) / (1024 * 1024)


class ConfigLoader:
    """Utility class for loading configuration files."""

    @staticmethod
    def load_config(file_name):
        # logger = logging.getLogger("ConfigLoader")
        try:
            with open(file_name, "r", encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError as e:
            # logger.warning(f"Configuration file not found: {file_name}")
            return {}


def _sanitize_for_json(obj):
    """Recursively convert objects to JSON-serializable forms.

    Handles pandas NA/NaT, numpy scalars/arrays, datetimes, Decimals, sets, and
    non-string dict keys.
    """
    # Fast-path for basic JSON types
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj

    # Floats: normalize non-finite values
    if isinstance(obj, float):
        if math.isfinite(obj):
            return obj
        return None

    # Datetime-like objects
    if isinstance(obj, (_dt.datetime, _dt.date, _dt.time)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    # Decimal -> float (or str if not finite)
    if isinstance(obj, Decimal):
        try:
            return float(obj)
        except Exception:
            return str(obj)

    # pandas NA / NaT without importing pandas
    tname = type(obj).__name__
    if tname in ("NAType", "NaTType"):
        return None

    # numpy scalars and arrays without importing numpy
    # Many numpy scalars have .item(); arrays often have .tolist()
    try:
        if hasattr(obj, "tolist"):
            return _sanitize_for_json(obj.tolist())
    except Exception:
        pass
    try:
        if hasattr(obj, "item"):
            return _sanitize_for_json(obj.item())
    except Exception:
        pass

    # Dict: ensure keys are strings and values sanitized
    if isinstance(obj, dict):
        sanitized = {}
        for k, v in obj.items():
            if isinstance(k, str):
                key = k
            else:
                try:
                    key = str(k)
                except Exception:
                    key = repr(k)
            sanitized[key] = _sanitize_for_json(v)
        return sanitized

    # Iterables (list/tuple/set)
    if isinstance(obj, (list, tuple, set)):
        return [_sanitize_for_json(x) for x in obj]

    # Fallback: best-effort string conversion
    try:
        return str(obj)
    except Exception:
        return None


class PlatformAsset:
    """
    A platform asset is a json formated data that can be pushed to the platform
    """

    def __init__(self, type):
        self.type = type
        self.data = []

    def save(self):
        # Writing data to metrics.json
        with open(self.type + ".json", "w", encoding="utf-8") as file:
            json.dump(_sanitize_for_json(self.data), file, indent=4)
