"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Shared metric accumulators for packs.

Every accumulator here is fed Polars LazyFrames and computes through
:mod:`qalita_core.analytics`, so the memory it uses is a function of the number
of columns and of the requested top-K, never of the number of rows. Two shapes
were removed on purpose because they broke that property:

- a Python dict with one entry per distinct key combination (on a primary key,
  one entry per row);
- per-chunk statistics combined afterwards by a row-weighted mean, which is not
  the statistic of the dataset for anything non-associative — normality above
  all.

The pandas entry points are kept for packs that have not been ported yet. They
are the only place pandas is touched, and nothing here imports it: a pandas
DataFrame cannot exist unless the caller already imported pandas.
"""

from __future__ import annotations

import datetime as _dt
import logging
import sys
from typing import Any, Dict, Iterable, List, Sequence, Tuple, Union
from typing import TYPE_CHECKING

import polars as pl

from . import analytics

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

# polars is a hard dependency of qalita_core; the flag is kept because packs
# still branch on it.
POLARS_AVAILABLE = True

try:  # pragma: no cover - trivial
    from importlib.util import find_spec

    # find_spec only searches sys.path; it does NOT import pandas, which is the
    # whole point — importing qalita_core must not cost a pandas import.
    PANDAS_AVAILABLE = find_spec("pandas") is not None
except Exception:  # pragma: no cover
    PANDAS_AVAILABLE = False

# Upper bound on the number of aggregate expressions sent in a single pass. One
# pass is what we want (the source is read once); this only keeps a pathological
# table of tens of thousands of columns from building one gigantic query plan.
_MAX_EXPRS_PER_PASS = 2000

# Default cap on the number of duplicated keys reported. Row-returning results
# are bounded by construction here, as in analytics.
DEFAULT_DUPLICATE_KEYS = 1000


def _is_pandas_frame(data: Any) -> bool:
    """True for a pandas DataFrame, without importing pandas.

    A pandas DataFrame cannot exist unless pandas is already in ``sys.modules``,
    so its absence is a definitive "no" — and asking the question never drags
    pandas into a pack that does not use it.
    """
    pandas = sys.modules.get("pandas")
    return pandas is not None and isinstance(data, pandas.DataFrame)


def _is_missing(value: Any) -> bool:
    """None, NaN or NaT — recognised without pandas.

    NaN and NaT are the only values that differ from themselves, which is what
    ``pandas.isna`` checks for scalars anyway.
    """
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:  # noqa: BLE001 - exotic objects compare by identity
        return False


def _schema_of(frame: Union["pl.LazyFrame", "pl.DataFrame"]) -> Dict[str, Any]:
    """Column names and dtypes, from Parquet footers when the frame is lazy."""
    return dict(frame.collect_schema())


def detect_chunked_from_items(
    raw_items: Iterable[Any],
    names: Iterable[str] | None,
    dataset_scope_name: str,
) -> Tuple[bool, bool, bool]:
    """
    Detect whether a list of items represents chunked parts of a single dataset.

    Returns (treat_chunks_as_one, auto_named, common_base_detected).
    - auto_named: names like "<dataset_scope_name>_<i>"
    - common_base_detected: all string paths to parquet with the same base before "_part_"
    """
    try:
        items = list(raw_items or [])
    except Exception:
        items = []

    if len(items) <= 1:
        return (False, False, False)

    auto_named = False
    try:
        if names is not None:
            str_names = [str(n) for n in names]
            auto_named = all(
                n.startswith(f"{dataset_scope_name}_")
                and n[len(dataset_scope_name) + 1 :].isdigit()
                for n in str_names
            )
        else:
            auto_named = True
    except Exception:
        auto_named = False

    # Detect parquet base prefix: <base>_part_<k>.parquet
    common_base_detected = False
    try:
        import os as _os

        parquet_like = [
            x
            for x in items
            if isinstance(x, str) and x.lower().endswith((".parquet", ".pq"))
        ]
        if len(parquet_like) == len(items) and len(parquet_like) > 1:
            bases = set()
            for p in parquet_like:
                stem = _os.path.splitext(_os.path.basename(p))[0]
                parts = stem.split("_part_")
                bases.add(parts[0] if parts else stem)
            common_base_detected = len(bases) == 1
    except Exception:
        common_base_detected = False

    treat_chunks_as_one = bool(
        len(items) > 1 and (auto_named or common_base_detected)
    )
    return (treat_chunks_as_one, auto_named, common_base_detected)


def normalize_and_dedupe_recommendations(
    records: List[Dict[str, Any]], root_dataset_name: str
) -> List[Dict[str, Any]]:
    """
    Normalize recommendations to the root dataset scope and deduplicate
    by (content, type, scope.value).
    """
    normalized: List[Dict[str, Any]] = []
    for rec in records or []:
        sc = (rec.get("scope") or {}).copy()
        parent = (sc.get("parent_scope") or {}).copy()
        if parent.get("perimeter") == "dataset":
            parent["value"] = root_dataset_name
            sc["parent_scope"] = parent
        if sc.get("perimeter") == "dataset":
            sc["value"] = root_dataset_name
        rec2 = {**rec, "scope": sc}
        normalized.append(rec2)

    seen = set()
    dedup: List[Dict[str, Any]] = []
    for rec in normalized:
        key = (
            rec.get("content"),
            rec.get("type"),
            ((rec.get("scope") or {}).get("value")),
        )
        if key in seen:
            continue
        seen.add(key)
        dedup.append(rec)
    return dedup


def _column_scope(column: str, dataset_scope_name: str) -> Dict[str, Any]:
    return {
        "perimeter": "column",
        "value": column,
        "parent_scope": {
            "perimeter": "dataset",
            "value": dataset_scope_name,
        },
    }


class CompletenessAggregator:
    """Accumulate completeness signals and finalize metrics/schemas.

    Feed it LazyFrames (``add_lf``). Every frame costs exactly one streaming
    pass whatever its width, because all the null counts travel in a single
    :func:`qalita_core.analytics.agg` call.
    """

    def __init__(self) -> None:
        self.total_rows: int = 0
        self.total_non_null_cells: int = 0
        self.total_cells: int = 0
        self.per_column: Dict[str, Dict[str, int]] = {}
        self.unique_columns: set[str] = set()

    def _record(self, rows: int, non_null: Dict[str, int]) -> None:
        total_non_null = 0
        for col, count in non_null.items():
            rec = self.per_column.get(col) or {"non_null": 0, "rows": 0}
            rec["non_null"] += int(count)
            rec["rows"] += rows
            self.per_column[col] = rec
            total_non_null += int(count)

        self.total_rows += rows
        self.total_non_null_cells += total_non_null
        self.total_cells += rows * max(len(non_null), 1)

    def _accumulate(
        self, frame: Union["pl.LazyFrame", "pl.DataFrame"]
    ) -> None:
        cols = list(_schema_of(frame))
        self.unique_columns.update(cols)

        # ONE pass: pl.len() plus one null-count expression per column, batched
        # only to keep the query plan finite on pathologically wide tables.
        rows = 0
        non_null: Dict[str, int] = {}
        batches = list(analytics.batched(cols, _MAX_EXPRS_PER_PASS)) or [[]]
        for index, batch in enumerate(batches):
            exprs: Dict[str, pl.Expr] = {
                f"nn|{name}": pl.col(name).is_not_null().sum()
                for name in batch
            }
            if index == 0:
                exprs["__rows"] = pl.len()
            result = analytics.agg(frame, exprs)
            if index == 0:
                rows = int(result.get("__rows") or 0)
            for name in batch:
                non_null[name] = int(result.get(f"nn|{name}") or 0)

        self._record(rows, non_null)

    def add_lf(self, lf: "pl.LazyFrame", *, streaming: bool = True) -> None:
        """Add a LazyFrame in one streaming pass.

        ``streaming`` is accepted for backward compatibility and ignored: every
        collect goes through :mod:`qalita_core.analytics`, which is
        streaming-only and raises rather than retrying in memory.
        """
        self._accumulate(lf)

    def add_pl(self, df: "pl.DataFrame") -> None:
        """Add a Polars DataFrame that is already in memory."""
        self._accumulate(df)

    def add_df(self, df: "pd.DataFrame") -> None:  # type: ignore[name-defined]
        """Add a pandas DataFrame (legacy path, frame already in memory)."""
        rows = int(len(df))
        cols_list = [str(c) for c in df.columns]
        self.unique_columns.update(cols_list)
        counts = df.notnull().sum()
        non_null = {
            name: int(counts.iloc[position])
            for position, name in enumerate(cols_list)
        }
        self._record(rows, non_null)

    def add(
        self, data: Union["pd.DataFrame", "pl.DataFrame", "pl.LazyFrame"]
    ) -> None:
        """Add statistics from any supported data type (auto-detection)."""
        if isinstance(data, (pl.LazyFrame, pl.DataFrame)):
            self._accumulate(data)
        elif _is_pandas_frame(data):
            self.add_df(data)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

    def finalize_metrics_and_schemas(
        self, dataset_scope_name: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        metrics: List[Dict[str, Any]] = []
        schemas: List[Dict[str, Any]] = []

        # Column completeness
        for col, rec in self.per_column.items():
            rows = max(int(rec.get("rows", 0)), 1)
            completeness = round(rec.get("non_null", 0) / rows, 2)
            metrics.append(
                {
                    "key": "completeness_score",
                    "value": str(completeness),
                    "scope": _column_scope(col, dataset_scope_name),
                }
            )

        total_rows = max(int(self.total_rows), 1)
        n_var = max(int(len(self.unique_columns)), 1)
        total_cells = max(int(self.total_cells), 1)
        p_cells_missing = max(
            min(
                1 - (float(self.total_non_null_cells) / float(total_cells)), 1
            ),
            0,
        )
        score_value = max(min(1 - p_cells_missing, 1), 0)

        # Dataset-level metrics
        for key, value in {
            "n": total_rows,
            "n_var": n_var,
            "n_cells_missing": int(total_cells - self.total_non_null_cells),
            "p_cells_missing": round(p_cells_missing, 6),
        }.items():
            metrics.append(
                {
                    "key": key,
                    "value": (
                        value
                        if isinstance(value, (int, str))
                        else float(value)
                    ),
                    "scope": {
                        "perimeter": "dataset",
                        "value": dataset_scope_name,
                    },
                }
            )

        metrics.append(
            {
                "key": "score",
                "value": str(round(score_value, 2)),
                "scope": {"perimeter": "dataset", "value": dataset_scope_name},
            }
        )

        # Schemas
        for variable_name in sorted(self.unique_columns):
            schemas.append(
                {
                    "key": "column",
                    "value": variable_name,
                    "scope": _column_scope(variable_name, dataset_scope_name),
                }
            )
        schemas.append(
            {
                "key": "dataset",
                "value": dataset_scope_name,
                "scope": {"perimeter": "dataset", "value": dataset_scope_name},
            }
        )

        return metrics, schemas


def streaming_outliers(
    lf: Union["pl.LazyFrame", "pl.DataFrame"],
    columns: Sequence[str],
    *,
    method: str = "iqr",
    threshold: float | None = None,
    exact: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """Count outliers over a whole dataset, in bounded memory.

    Two passes. Pass 1 computes GLOBAL fences for every column; pass 2 counts,
    in ONE aggregation covering every column at once, the rows that fall outside
    them. Global fences are the whole point: an outlier is defined relative to
    the entire column, so fences computed per chunk answer a different question,
    and a row-weighted mean of per-chunk normality is not the normality of the
    dataset.

    Args:
        lf: LazyFrame (or DataFrame) to scan. It is read twice with
            ``method="zscore"``, three times with ``method="iqr"`` and
            ``exact=False`` (the histogram needs the min/max first).
        columns: numeric columns to check — see
            :func:`qalita_core.analytics.numeric_columns`, which reads them off
            the Parquet footers.
        method: ``"iqr"`` puts the fences at ``q1 - threshold * IQR`` and
            ``q3 + threshold * IQR``; ``"zscore"`` at ``mean ± threshold * std``.
        threshold: defaults to ``1.5`` for ``"iqr"`` and ``3.0`` for
            ``"zscore"``.
        exact: compute the quartiles exactly instead of from a histogram. Costs
            an ordering of each column (the streaming engine spills to disk, so
            it stays within the memory budget but is markedly slower).

    Returns:
        ``{column: {...}}`` with, per column:

        - ``outlier_count`` (int): rows outside the fences, nulls excluded;
        - ``non_null`` (int): rows the score is computed on;
        - ``normality_score`` (float): ``1 - outlier_count / non_null``, ``1.0``
          when the column has no usable fence (all-null or zero spread);
        - ``lower`` / ``upper`` (float or None): the fences themselves;
        - ``method`` (str): the method asked for;
        - ``bounds_method`` (str): ``"histogram"`` when the fences come from
          approximate quantiles, ``"exact"`` otherwise. Feed it to the
          ``<key>_method`` sibling metric so the UI can label the number.

    Example:
        >>> schema = pack.schema("source")
        >>> results = streaming_outliers(
        ...     pack.scan("source"), analytics.numeric_columns(schema)
        ... )
        >>> results["price"]["outlier_count"]
        1734
    """
    names = list(columns)
    if not names:
        return {}

    if method not in ("iqr", "zscore"):
        raise ValueError(
            f"unknown outlier method {method!r}, expected 'iqr' or 'zscore'"
        )
    if threshold is None:
        threshold = 1.5 if method == "iqr" else 3.0
    threshold = float(threshold)

    bounds: Dict[str, Tuple[float, float]] = {}
    if method == "iqr":
        bounds_method = "exact" if exact else "histogram"
        quartiles = analytics.quantiles(lf, names, (0.25, 0.75), exact=exact)
        for name in names:
            values = quartiles.get(name) or {}
            q1, q3 = values.get(0.25), values.get(0.75)
            if q1 is None or q3 is None:
                continue
            spread = float(q3) - float(q1)
            if spread <= 0:
                # Constant (or near-constant) column: any fence would flag
                # either nothing or everything.
                continue
            bounds[name] = (
                float(q1) - threshold * spread,
                float(q3) + threshold * spread,
            )
    else:
        # mean/std are exact streaming aggregates, so no approximation label.
        bounds_method = "exact"
        moments = analytics.agg(
            lf,
            {
                **{f"mean|{i}": pl.col(c).mean() for i, c in enumerate(names)},
                **{f"std|{i}": pl.col(c).std() for i, c in enumerate(names)},
            },
        )
        for index, name in enumerate(names):
            mean = moments.get(f"mean|{index}")
            std = moments.get(f"std|{index}")
            if mean is None or std is None or float(std) <= 0:
                continue
            bounds[name] = (
                float(mean) - threshold * float(std),
                float(mean) + threshold * float(std),
            )

    # Pass 2: every column counted in the same pass over the source.
    exprs: Dict[str, pl.Expr] = {}
    for index, name in enumerate(names):
        exprs[f"cnt|{index}"] = pl.col(name).count()
        if name in bounds:
            low, high = bounds[name]
            exprs[f"out|{index}"] = (
                (pl.col(name) < low) | (pl.col(name) > high)
            ).sum()
    counted = analytics.agg(lf, exprs)

    results: Dict[str, Dict[str, Any]] = {}
    for index, name in enumerate(names):
        non_null = int(counted.get(f"cnt|{index}") or 0)
        outliers = int(counted.get(f"out|{index}") or 0)
        low, high = bounds.get(name, (None, None))
        results[name] = {
            "outlier_count": outliers,
            "non_null": non_null,
            "normality_score": (
                1.0 - (outliers / non_null) if non_null else 1.0
            ),
            "lower": low,
            "upper": high,
            "method": method,
            "bounds_method": bounds_method,
        }
    return results


class OutlierAggregator:
    """Turn global outlier results into metrics and recommendations.

    It stores results, it does not combine chunks: the counts handed to it by
    :func:`streaming_outliers` already cover the whole dataset. The previous
    shape asked the caller for per-chunk normality and averaged it weighted by
    rows, which is not the normality of the dataset — the statistic is not
    associative, so no combination of per-chunk values can reconstruct it.
    """

    def __init__(self) -> None:
        self.columns: Dict[str, Dict[str, Any]] = {}
        self.dataset_outliers: int = 0
        self.total_rows: int = 0
        self.bounds_method: str = "exact"

    def add_column_result(
        self,
        column: str,
        *,
        outlier_count: int,
        normality_score: float,
        rows: int = 0,
        method: str = "iqr",
        bounds_method: str = "exact",
        lower: float | None = None,
        upper: float | None = None,
    ) -> None:
        """Record the GLOBAL result of one column."""
        self.columns[column] = {
            "outlier_count": int(outlier_count),
            "normality_score": float(normality_score),
            "rows": int(rows),
            "method": method,
            "bounds_method": bounds_method,
            "lower": lower,
            "upper": upper,
        }
        if bounds_method != "exact":
            # One approximate column makes the dataset score approximate.
            self.bounds_method = bounds_method

    def add_streaming_outliers(
        self, results: Dict[str, Dict[str, Any]], *, rows: int = 0
    ) -> None:
        """Record a whole :func:`streaming_outliers` result at once."""
        for column, stats in results.items():
            self.add_column_result(
                column,
                outlier_count=stats.get("outlier_count", 0),
                normality_score=stats.get("normality_score", 1.0),
                rows=stats.get("non_null", 0),
                method=stats.get("method", "iqr"),
                bounds_method=stats.get("bounds_method", "exact"),
                lower=stats.get("lower"),
                upper=stats.get("upper"),
            )
        if rows:
            self.total_rows = int(rows)

    def add_dataset_result(
        self, *, rows: int = 0, multivariate_outliers_count: int = 0
    ) -> None:
        """Record dataset-level counts (row count, multivariate outliers)."""
        self.dataset_outliers += int(multivariate_outliers_count)
        if rows:
            self.total_rows = int(rows)

    def finalize_metrics_and_recommendations(
        self, root_dataset_name: str, normality_threshold: float
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        metrics: List[Dict[str, Any]] = []
        recommendations: List[Dict[str, Any]] = []

        dataset_scope = {"perimeter": "dataset", "value": root_dataset_name}

        for col, stats in self.columns.items():
            scope = _column_scope(col, root_dataset_name)
            normality = round(float(stats["normality_score"]), 2)
            metrics.append(
                {"key": "normality_score", "value": normality, "scope": scope}
            )
            metrics.append(
                {
                    "key": "outliers",
                    "value": int(stats["outlier_count"]),
                    "scope": scope,
                }
            )
            # Both numbers are derived from the fences, so both carry the label
            # that says how the fences were obtained.
            for key in ("normality_score_method", "outliers_method"):
                metrics.append(
                    {
                        "key": key,
                        "value": stats["bounds_method"],
                        "scope": scope,
                    }
                )
            if normality < normality_threshold:
                recommendations.append(
                    {
                        "content": (
                            f"Column '{col}' has a normality score of "
                            f"{normality * 100}%."
                        ),
                        "type": "Outliers",
                        "scope": scope,
                        "level": _determine_recommendation_level(
                            1 - normality
                        ),
                    }
                )

        scores = [
            float(stats["normality_score"]) for stats in self.columns.values()
        ]
        dataset_norm = round(sum(scores) / len(scores), 2) if scores else 1.0

        metrics.append(
            {
                "key": "outliers",
                "value": int(self.dataset_outliers),
                "scope": dataset_scope,
            }
        )
        metrics.append(
            {
                "key": "normality_score_dataset",
                "value": dataset_norm,
                "scope": dataset_scope,
            }
        )
        metrics.append(
            {
                "key": "score",
                "value": str(dataset_norm),
                "scope": dataset_scope,
            }
        )
        total_outliers_count = int(
            sum(int(s["outlier_count"]) for s in self.columns.values())
        )
        metrics.append(
            {
                "key": "total_outliers_count",
                "value": total_outliers_count,
                "scope": dataset_scope,
            }
        )
        metrics.append(
            {
                "key": "outliers_method",
                "value": self.bounds_method,
                "scope": dataset_scope,
            }
        )

        if dataset_norm < normality_threshold:
            recommendations.append(
                {
                    "content": (
                        f"The dataset '{root_dataset_name}' has a normality "
                        f"score of {dataset_norm * 100}%."
                    ),
                    "type": "Outliers",
                    "scope": dataset_scope,
                    "level": _determine_recommendation_level(1 - dataset_norm),
                }
            )

        recommendations.append(
            {
                "content": (
                    f"The dataset '{root_dataset_name}' has a total of "
                    f"{total_outliers_count} outliers. Check them in output "
                    "file."
                ),
                "type": "Outliers",
                "scope": dataset_scope,
                "level": _determine_recommendation_level(
                    total_outliers_count / max(1, self.total_rows)
                ),
            }
        )

        return metrics, recommendations


def _determine_recommendation_level(proportion_outliers: float) -> str:
    if proportion_outliers > 0.5:
        return "high"
    elif proportion_outliers > 0.3:
        return "warning"
    return "info"


class DuplicateAggregator:
    """Count duplicated key combinations over a whole dataset.

    Polars owns the counting state: every frame added is registered in a single
    lazy plan, grouped once at the end, and the engine spills the group state to
    disk. The previous shape collected the group-by result into a Python dict
    with one entry per distinct key combination — on a primary key that is one
    tuple per row, which is precisely the case this class exists to answer.

    ``add_df`` (pandas) still fills :attr:`combo_to_count`, because the frames
    it is handed are already in memory; do not use it on a large source.
    """

    def __init__(self, uniqueness_columns: Iterable[str]):
        self.uniqueness_columns = list(uniqueness_columns)
        self.combo_to_count: Dict[Tuple[Any, ...], int] = {}
        self._frames: List["pl.LazyFrame"] = []
        self._eager_rows: int = 0
        self._lazy_rows: int | None = None
        self._lazy_duplicates: int | None = None

    # -- accumulation ----------------------------------------------------

    def _invalidate(self) -> None:
        self._lazy_rows = None
        self._lazy_duplicates = None

    def add_lf(self, lf: "pl.LazyFrame", *, streaming: bool = True) -> None:
        """Register a LazyFrame. Nothing is read here.

        The counting is deferred so that every registered frame is grouped by
        the SAME plan: a key duplicated across two chunks is only visible to a
        group-by that sees both.

        ``streaming`` is accepted for backward compatibility and ignored.
        """
        self._frames.append(lf)
        self._invalidate()

    def add_pl(self, df: "pl.DataFrame") -> None:
        """Register a Polars DataFrame already in memory."""
        self.add_lf(df.lazy())

    def add_df(self, df: "pd.DataFrame") -> None:  # type: ignore[name-defined]
        """Add a pandas DataFrame (legacy path, frame already in memory)."""
        self._eager_rows += int(len(df))
        subset = df[self.uniqueness_columns]
        # value_counts on a DataFrame returns a Series with MultiIndex keys
        counts = subset.value_counts(dropna=False)
        if hasattr(counts, "items"):
            for key, count in counts.items():
                if not isinstance(key, tuple):
                    key = (key,)
                key_t = self._sanitize_key_tuple(key)
                self.combo_to_count[key_t] = self.combo_to_count.get(
                    key_t, 0
                ) + int(count)

    def add(
        self, data: Union["pd.DataFrame", "pl.DataFrame", "pl.LazyFrame"]
    ) -> None:
        """Add statistics from any supported data type (auto-detection)."""
        if isinstance(data, pl.LazyFrame):
            self.add_lf(data)
        elif isinstance(data, pl.DataFrame):
            self.add_pl(data)
        elif _is_pandas_frame(data):
            self.add_df(data)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

    def _sanitize_key_tuple(self, values: Tuple[Any, ...]) -> Tuple[Any, ...]:
        return tuple(None if _is_missing(v) else v for v in values)

    # -- lazy plan -------------------------------------------------------

    def _plan(self) -> "pl.LazyFrame | None":
        if not self._frames:
            return None
        parts = [
            frame.select(self.uniqueness_columns) for frame in self._frames
        ]
        if len(parts) == 1:
            return parts[0]
        # relaxed: two chunks of the same table can disagree on an integer width
        # when they came from different files.
        return pl.concat(parts, how="vertical_relaxed")

    def _key_counts(self) -> "pl.LazyFrame | None":
        plan = self._plan()
        if plan is None:
            return None
        return plan.group_by(self.uniqueness_columns).agg(
            pl.len().alias("count")
        )

    @property
    def total_rows(self) -> int:
        """Rows seen, lazy frames included.

        On Parquet the lazy part answers from the file footers, so this costs
        no data read.
        """
        if self._lazy_rows is None:
            plan = self._plan()
            self._lazy_rows = 0 if plan is None else analytics.row_count(plan)
        return self._eager_rows + self._lazy_rows

    def duplicate_count(self) -> int:
        """Rows in excess of one per key combination, over the whole dataset."""
        pandas_dups = sum(
            count - 1 for count in self.combo_to_count.values() if count > 1
        )
        if self._lazy_duplicates is None:
            grouped = self._key_counts()
            if grouped is None:
                self._lazy_duplicates = 0
            else:
                # (n - 1) summed inside the engine: no per-key Python object is
                # ever created, whatever the cardinality of the key.
                self._lazy_duplicates = int(
                    analytics.agg(
                        grouped,
                        {
                            "duplicates": (
                                (pl.col("count").cast(pl.Int64) - 1)
                                .clip(lower_bound=0)
                                .sum()
                            )
                        },
                    )["duplicates"]
                    or 0
                )
        return int(pandas_dups + self._lazy_duplicates)

    def get_duplicate_key_counts(
        self, limit: int = DEFAULT_DUPLICATE_KEYS
    ) -> "pl.DataFrame":
        """The ``limit`` most duplicated key combinations, worst first.

        Bounded by construction: the ordering happens inside the engine and only
        ``limit`` rows ever reach Python. Columns are the uniqueness columns
        plus ``count``.
        """
        if limit <= 0:
            raise ValueError(f"limit must be positive, got {limit}")

        grouped = self._key_counts()
        if grouped is not None:
            top = analytics.top_k(
                grouped.filter(pl.col("count") > 1), "count", limit
            )
            frame = top.select([*self.uniqueness_columns, "count"])
        else:
            frame = pl.DataFrame(
                schema={
                    **{name: pl.Null for name in self.uniqueness_columns},
                    "count": pl.Int64,
                }
            )

        if not self.combo_to_count:
            return frame

        # The pandas path keeps its own counts; merge them in and re-cap.
        eager = [
            {
                **dict(zip(self.uniqueness_columns, key)),
                "count": count,
            }
            for key, count in self.combo_to_count.items()
            if count > 1
        ]
        eager.sort(key=lambda row: row["count"], reverse=True)
        merged = frame.to_dicts() + eager[:limit]
        merged.sort(key=lambda row: row["count"], reverse=True)
        return pl.DataFrame(
            merged[:limit],
            schema=[*self.uniqueness_columns, "count"],
            strict=False,
        )

    def get_duplicate_keys(
        self, limit: int = DEFAULT_DUPLICATE_KEYS
    ) -> List[Tuple[Any, ...]]:
        """The ``limit`` most duplicated key combinations, as tuples.

        BOUNDED on purpose: the previous version returned every duplicated key,
        which on a badly-keyed source is the whole dataset in a Python list.
        """
        counts = self.get_duplicate_key_counts(limit)
        if counts.height == 0:
            return []
        keys = counts.select(self.uniqueness_columns).rows()
        return [self._sanitize_key_tuple(key) for key in keys]

    def finalize_metrics(
        self, dataset_scope_name: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        metrics: List[Dict[str, Any]] = []
        recommendations: List[Dict[str, Any]] = []
        total_dups = self.duplicate_count()
        total_rows = self.total_rows
        duplication_rate = (
            (float(total_dups) / float(max(total_rows, 1)))
            if total_rows
            else 0.0
        )
        score = max(0.0, min(1.0, 1.0 - duplication_rate))
        metrics.append(
            {
                "key": "score",
                "value": str(round(score, 2)),
                "scope": {"perimeter": "dataset", "value": dataset_scope_name},
            }
        )
        metrics.append(
            {
                "key": "duplicates",
                "value": int(total_dups),
                "scope": {"perimeter": "dataset", "value": dataset_scope_name},
            }
        )
        return metrics, recommendations


class TimelinessAggregator:
    """Aggregate earliest/latest per column to compute timeliness metrics."""

    def __init__(self) -> None:
        self.date_cols: Dict[str, Dict[str, Any]] = {}
        # structure: col -> {kind: "date"|"year", min: value, max: value}

    def add_lf(
        self,
        lf: Union["pl.LazyFrame", "pl.DataFrame"],
        date_columns: Sequence[str] | None = None,
        *,
        schema: Dict[str, Any] | None = None,
    ) -> None:
        """Observe min/max of every date column in ONE streaming pass.

        Temporal columns produce date observations; numeric columns are read as
        years, which is what the caller asked for by listing them. Columns of
        any other dtype are skipped.

        This exists so packs stop calling ``unique()``/``dropna()`` per column:
        that idiom re-read the source once per column and built one Python
        object per distinct value, to end up keeping two of them.

        Args:
            lf: LazyFrame (or DataFrame) to scan.
            date_columns: columns to observe. Defaults to every column, which
                is only sensible on an already-filtered frame.
            schema: pre-computed schema, e.g. from ``Pack.schema()``. Avoids
                touching the Parquet footers twice.
        """
        resolved = schema if schema is not None else _schema_of(lf)
        names = (
            list(date_columns) if date_columns is not None else list(resolved)
        )
        if not names:
            return

        stats: Dict[str, Any] = {}
        for batch in analytics.batched(names, _MAX_EXPRS_PER_PASS // 2):
            stats.update(
                analytics.agg(
                    lf,
                    {
                        **{f"min|{c}": pl.col(c).min() for c in batch},
                        **{f"max|{c}": pl.col(c).max() for c in batch},
                    },
                )
            )

        for name in names:
            low, high = stats.get(f"min|{name}"), stats.get(f"max|{name}")
            if low is None or high is None:
                continue
            dtype = resolved.get(name)
            if dtype in (pl.Date, pl.Datetime):
                self.add_date_obs(name, low, high)
            elif dtype is not None and dtype.is_numeric():
                self.add_year_obs(name, int(low), int(high))
            else:
                logger.debug(
                    "timeliness: column %r has dtype %s, neither a date nor a "
                    "year — skipped",
                    name,
                    dtype,
                )

    def add_year_obs(
        self, column: str, earliest_year: int, latest_year: int
    ) -> None:
        entry = self.date_cols.get(column) or {
            "kind": "year",
            "min": None,
            "max": None,
        }
        entry["kind"] = "year"
        entry["min"] = (
            earliest_year
            if entry.get("min") is None
            else min(int(entry["min"]), int(earliest_year))
        )
        entry["max"] = (
            latest_year
            if entry.get("max") is None
            else max(int(entry["max"]), int(latest_year))
        )
        self.date_cols[column] = entry

    def add_date_obs(
        self, column: str, earliest_date: "_dt.date", latest_date: "_dt.date"
    ) -> None:
        entry = self.date_cols.get(column) or {
            "kind": "date",
            "min": None,
            "max": None,
        }
        entry["kind"] = "date"
        if entry.get("min") is None or (
            earliest_date and earliest_date < entry["min"]
        ):
            entry["min"] = earliest_date
        if entry.get("max") is None or (
            latest_date and latest_date > entry["max"]
        ):
            entry["max"] = latest_date
        self.date_cols[column] = entry

    def finalize_metrics(
        self,
        dataset_scope_name: str,
        compute_score_columns: Iterable[str] | None,
        calc_timeliness_score,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        metrics: List[Dict[str, Any]] = []
        recommendations: List[Dict[str, Any]] = []

        now = _dt.datetime.now()
        now_date = now.date()  # Use date for comparison with date objects
        eligible_columns = (
            set(compute_score_columns) if compute_score_columns else None
        )
        scores: List[float] = []

        for col, info in self.date_cols.items():
            scope = _column_scope(col, dataset_scope_name)
            kind = info.get("kind")
            if kind == "year":
                earliest_year = (
                    int(info.get("min"))
                    if info.get("min") is not None
                    else None
                )
                latest_year = (
                    int(info.get("max"))
                    if info.get("max") is not None
                    else None
                )
                if earliest_year is None or latest_year is None:
                    continue
                days_since_latest_year = (now.year - latest_year) * 365
                days_since_earliest_year = (now.year - earliest_year) * 365
                timeliness_score = calc_timeliness_score(
                    days_since_latest_year
                )
                metrics.extend(
                    [
                        {
                            "key": "earliest_year",
                            "value": str(earliest_year),
                            "scope": scope,
                        },
                        {
                            "key": "latest_year",
                            "value": str(latest_year),
                            "scope": scope,
                        },
                        {
                            "key": "days_since_earliest_year",
                            "value": str(days_since_earliest_year),
                            "scope": scope,
                        },
                        {
                            "key": "days_since_latest_year",
                            "value": str(days_since_latest_year),
                            "scope": scope,
                        },
                        {
                            "key": "timeliness_score",
                            "value": str(round(timeliness_score, 2)),
                            "scope": scope,
                        },
                    ]
                )
                if days_since_latest_year > 365:
                    recommendations.append(
                        {
                            "content": (
                                f"The latest date in column '{col}' is more "
                                "than one year old."
                            ),
                            "type": "Latest Date far in the past",
                            "scope": scope,
                            "level": "high",
                        }
                    )
                if (eligible_columns is None) or (col in eligible_columns):
                    scores.append(float(timeliness_score))
            else:
                earliest_date = info.get("min")
                latest_date = info.get("max")
                if earliest_date is None or latest_date is None:
                    continue
                # Convert datetime to date if needed for consistent comparison
                if isinstance(latest_date, _dt.datetime):
                    latest_date = latest_date.date()
                if isinstance(earliest_date, _dt.datetime):
                    earliest_date = earliest_date.date()
                days_since_latest = (now_date - latest_date).days
                days_since_earliest = (now_date - earliest_date).days
                timeliness_score = calc_timeliness_score(days_since_latest)
                metrics.extend(
                    [
                        {
                            "key": "earliest_date",
                            "value": earliest_date.strftime("%Y-%m-%d"),
                            "scope": scope,
                        },
                        {
                            "key": "latest_date",
                            "value": latest_date.strftime("%Y-%m-%d"),
                            "scope": scope,
                        },
                        {
                            "key": "days_since_earliest_date",
                            "value": str(days_since_earliest),
                            "scope": scope,
                        },
                        {
                            "key": "days_since_latest_date",
                            "value": str(days_since_latest),
                            "scope": scope,
                        },
                        {
                            "key": "timeliness_score",
                            "value": str(round(timeliness_score, 2)),
                            "scope": scope,
                        },
                    ]
                )
                if days_since_latest > 365:
                    recommendations.append(
                        {
                            "content": (
                                f"The latest date in column '{col}' is more "
                                "than one year old."
                            ),
                            "type": "Latest Date far in the past",
                            "scope": scope,
                            "level": "high",
                        }
                    )
                if (eligible_columns is None) or (col in eligible_columns):
                    scores.append(float(timeliness_score))

        if scores:
            avg_score = sum(scores) / float(len(scores))
            metrics.append(
                {
                    "key": "score",
                    "value": str(round(avg_score, 2)),
                    "scope": {
                        "perimeter": "dataset",
                        "value": dataset_scope_name,
                    },
                }
            )
        return metrics, recommendations
