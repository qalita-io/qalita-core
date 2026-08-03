"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Synthetic big-data generator for the memory regression harness.

The point of this module is to produce a dataset that is *larger than the
memory ceiling the tests assert*, which means the generator itself must never
hold the dataset it writes. It never does: every column is a pure function of a
monotonic row id, so the whole dataset is derived lazily from a small `id` seed
file that is scanned once per part and sunk straight to Parquet by the
streaming engine. Peak generator memory is therefore a function of
``part_rows``, not of ``rows``.

The column mix is chosen for what breaks memory rather than for realism:

- ``id``      monotonic Int64, no nulls. A ``head()``-based "sample" is
              detectable because the mean of this column is far from the
              dataset midpoint.
- ``uid``     one distinct String value per row. This is the column that kills
              hash aggregations: every exact ``n_unique`` / ``value_counts`` /
              ``group_by`` over it costs O(rows) state.
- ``key``     low-cardinality Int32, the well-behaved grouping key.
- ``category``low-cardinality String.
- ``value``   Float64 carrying the null rate.
- ``amount``  Int64 with negatives and zeros.
- ``flag``    Boolean.
- ``ts``      Datetime, so temporal code paths are exercised.

Sizes are parametrized and the default is deliberately small so CI stays fast;
set ``QALITA_BIGDATA_ROWS`` to run the same assertions against a big dataset.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

import polars as pl
import pytest

__all__ = [
    "Dataset",
    "DEFAULT_ROWS",
    "DEFAULT_PARTS",
    "COLUMNS",
    "dataset_rows",
    "generate",
    "bigdata",
    "small_dataset",
]


# Small enough that the whole harness stays well under a couple of minutes
# (generation is ~0.5 s, ~97 MB on disk), yet an eager read of it costs
# ~1.1 GiB -- several times the ceiling the streaming tests assert.
DEFAULT_ROWS = 16_000_000
DEFAULT_PARTS = 8

# Rows generated per lazy block inside a part. Bounds the generator's own peak.
_BLOCK_ROWS = 100_000

# Multiplier used by :meth:`Dataset.in_memory_bytes`: bytes an eager Polars
# frame needs per row for :data:`COLUMNS`. Measured, not guessed —
# 8 (id) + 8 (amount) + 8 (value) + 4 (key) + 1 (flag) + 8 (ts) plus the
# string payload and offsets of ``uid`` and ``category``.
_BYTES_PER_ROW = 70

COLUMNS = (
    "id",
    "uid",
    "key",
    "category",
    "value",
    "amount",
    "flag",
    "ts",
)

_CATEGORIES = ("alpha", "beta", "gamma", "delta", "epsilon")

# Odd 32-bit constant (Knuth). Multiplying the row id by it and taking the low
# digits gives a deterministic, well-spread pseudo-random draw without a hash
# function, so the dataset is byte-identical across Polars versions.
_MIX = 2654435761


@dataclass(frozen=True)
class Dataset:
    """A generated dataset and the facts a memory test needs about it."""

    root: Path
    files: list[str]
    rows: int
    parts: int
    cardinality: int
    null_rate: float
    columns: tuple[str, ...] = field(default=COLUMNS)

    def scan(self) -> "pl.LazyFrame":
        """Lazily scan every part, the way ``Pack.scan`` does."""
        return pl.scan_parquet(self.files)

    @property
    def bytes_on_disk(self) -> int:
        return sum(os.path.getsize(f) for f in self.files)

    @property
    def in_memory_bytes(self) -> int:
        """What an eager read of this dataset would cost.

        Memory assertions are expressed as a fraction of this, so the ceiling
        keeps its meaning when ``QALITA_BIGDATA_ROWS`` changes the size.
        """
        return self.rows * _BYTES_PER_ROW

    @property
    def in_memory_mb(self) -> float:
        return self.in_memory_bytes / (1024 * 1024)


def dataset_rows(default: int = DEFAULT_ROWS) -> int:
    """Row count for the harness, overridable with ``QALITA_BIGDATA_ROWS``."""
    raw = os.environ.get("QALITA_BIGDATA_ROWS")
    if not raw:
        return default
    rows = int(raw)
    if rows <= 0:
        raise ValueError(f"QALITA_BIGDATA_ROWS must be positive, got {rows}")
    return rows


def _row_exprs(
    cardinality: int, null_rate: float, categories: Sequence[str]
) -> list["pl.Expr"]:
    """Every column as a pure function of ``id``.

    Purity is what lets the generator stream: no state crosses a row boundary,
    so any part can be produced from its id range alone.
    """
    rid = pl.col("id")
    draw = (rid * _MIX) % 1000
    is_null = draw < int(round(null_rate * 1000))

    vocabulary = list(categories)
    category = (rid % len(vocabulary)).replace_strict(
        old=list(range(len(vocabulary))),
        new=vocabulary,
        return_dtype=pl.String,
    )

    return [
        # One distinct value per row: the hash-aggregation killer.
        pl.concat_str([pl.lit("u"), rid.cast(pl.String)]).alias("uid"),
        (rid % cardinality).cast(pl.Int32).alias("key"),
        category.alias("category"),
        pl.when(is_null)
        .then(None)
        .otherwise((rid.cast(pl.Float64) * 1.5) - 1000.0)
        .alias("value"),
        # A second, much rarer null pattern than ``value``'s, so tests can tell
        # a per-column null count apart from a frame-wide one.
        pl.when(draw < 5)
        .then(None)
        .otherwise((rid % 2001) - 1000)
        .cast(pl.Int64)
        .alias("amount"),
        ((rid % 2) == 0).alias("flag"),
        (
            pl.lit("2020-01-01").str.to_datetime()
            + pl.duration(seconds=rid % 86_400)
        ).alias("ts"),
    ]


def generate(
    directory: "str | os.PathLike[str]",
    *,
    rows: int = DEFAULT_ROWS,
    parts: int = DEFAULT_PARTS,
    cardinality: int = 1_000,
    null_rate: float = 0.05,
    categories: Sequence[str] = _CATEGORIES,
) -> Dataset:
    """Write a synthetic parquet dataset without ever materializing it.

    Args:
        directory: where the part files land. Created if missing.
        rows: total row count across all parts.
        parts: number of part files. Several parts is not cosmetic: it is what
            makes a first-part-only "sample" and the old
            ``zip(table_names, paths)`` idiom detectable.
        cardinality: distinct values of the ``key`` column. ``uid`` is always
            one distinct value per row, whatever this is.
        null_rate: fraction of null values in ``value``.
        categories: vocabulary size for the ``category`` column.
    """
    if rows <= 0:
        raise ValueError(f"rows must be positive, got {rows}")
    if parts <= 0:
        raise ValueError(f"parts must be positive, got {parts}")
    if not 0.0 <= null_rate < 1.0:
        raise ValueError(f"null_rate must be within [0, 1), got {null_rate}")
    if cardinality <= 0:
        raise ValueError(f"cardinality must be positive, got {cardinality}")

    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)

    part_rows = -(-rows // parts)  # ceil, so the parts cover every row
    exprs = _row_exprs(cardinality, null_rate, categories)

    # The seed holds `part_rows` ids and nothing else. It is built from blocks
    # of `_BLOCK_ROWS` concatenated lazily rather than from one big int_range,
    # because a single int_range is produced in one allocation and would put
    # the whole part in RAM before the writer ever sees it.
    seed_path = root / "_ids.seed.parquet"
    blocks = [
        pl.LazyFrame().select(
            (
                pl.int_range(0, min(_BLOCK_ROWS, part_rows - start)) + start
            ).alias("id")
        )
        for start in range(0, part_rows, _BLOCK_ROWS)
    ]
    pl.concat(blocks, how="vertical").sink_parquet(
        str(seed_path), engine="streaming"
    )

    files: list[str] = []
    written = 0
    for part in range(parts):
        remaining = rows - written
        if remaining <= 0:
            break
        target = root / f"bigdata_part_{part:05d}.parquet"
        lazy = pl.scan_parquet(str(seed_path))
        if remaining < part_rows:
            lazy = lazy.head(remaining)
        lazy = lazy.with_columns(
            (pl.col("id") + written).alias("id")
        ).with_columns(exprs)
        lazy.sink_parquet(str(target), engine="streaming")
        files.append(str(target))
        written += min(remaining, part_rows)

    seed_path.unlink()

    return Dataset(
        root=root,
        files=files,
        rows=written,
        parts=len(files),
        cardinality=cardinality,
        null_rate=null_rate,
    )


@pytest.fixture(scope="session")
def bigdata(tmp_path_factory) -> Dataset:
    """A multi-part dataset far larger than the asserted memory ceiling.

    Session-scoped: regenerating it per test would dominate the run, and every
    consumer treats it as read-only.
    """
    root = tmp_path_factory.mktemp("qalita-bigdata")
    return generate(root, rows=dataset_rows(), parts=DEFAULT_PARTS)


@pytest.fixture(scope="session")
def small_dataset(tmp_path_factory) -> Dataset:
    """A fast multi-part dataset for correctness (not memory) assertions."""
    root = tmp_path_factory.mktemp("qalita-smalldata")
    return generate(root, rows=200_000, parts=4, cardinality=97)


if __name__ == "__main__":  # pragma: no cover - manual harness driver
    import argparse
    import json

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--parts", type=int, default=DEFAULT_PARTS)
    parser.add_argument("--cardinality", type=int, default=1_000)
    parser.add_argument("--null-rate", type=float, default=0.05)
    args = parser.parse_args()

    ds = generate(
        args.directory,
        rows=args.rows,
        parts=args.parts,
        cardinality=args.cardinality,
        null_rate=args.null_rate,
    )
    print(
        json.dumps(
            {
                "rows": ds.rows,
                "parts": ds.parts,
                "bytes_on_disk": ds.bytes_on_disk,
                "in_memory_mb": round(ds.in_memory_mb, 1),
                "files": ds.files,
            },
            indent=2,
        )
    )
