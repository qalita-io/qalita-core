"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Shared aggregation helpers for packs.
"""

from __future__ import annotations

from typing import List, Dict, Any, Iterable, Tuple
import math
import datetime as _dt

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - pandas is expected at runtime
    pd = None  # type: ignore


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
                n.startswith(f"{dataset_scope_name}_") and n[len(dataset_scope_name) + 1 :].isdigit()
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

    treat_chunks_as_one = bool(len(items) > 1 and (auto_named or common_base_detected))
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
        key = (rec.get("content"), rec.get("type"), ((rec.get("scope") or {}).get("value")))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(rec)
    return dedup


class CompletenessAggregator:
    """Accumulate completeness signals across chunks and finalize metrics/schemas."""

    def __init__(self) -> None:
        self.total_rows: int = 0
        self.total_non_null_cells: int = 0
        self.total_cells: int = 0
        self.per_column: Dict[str, Dict[str, int]] = {}
        self.unique_columns: set[str] = set()

    def add_df(self, df: "pd.DataFrame") -> None:  # type: ignore[name-defined]
        if pd is None:
            raise RuntimeError("pandas is required for CompletenessAggregator")
        rows = int(len(df))
        cols_list = list(df.columns)
        self.unique_columns.update(cols_list)
        # Per-column non-null
        for col in cols_list:
            try:
                nn = int(df[col].notnull().sum())
            except Exception:
                nn = int(pd.Series([x for x in df[col] if x is not None]).shape[0])
            rec = self.per_column.get(col) or {"non_null": 0, "rows": 0}
            rec["non_null"] += nn
            rec["rows"] += rows
            self.per_column[col] = rec
        # Dataset-level
        try:
            non_null_cells = int(df.notnull().sum().sum())
        except Exception:
            non_null_cells = sum(self.per_column[c]["non_null"] for c in cols_list)
        self.total_rows += rows
        self.total_non_null_cells += non_null_cells
        self.total_cells += rows * max(len(cols_list), 1)

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
                    "scope": {
                        "perimeter": "column",
                        "value": col,
                        "parent_scope": {
                            "perimeter": "dataset",
                            "value": dataset_scope_name,
                        },
                    },
                }
            )

        total_rows = max(int(self.total_rows), 1)
        n_var = max(int(len(self.unique_columns)), 1)
        total_cells = max(int(self.total_cells), 1)
        p_cells_missing = max(min(1 - (float(self.total_non_null_cells) / float(total_cells)), 1), 0)
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
                    "value": value if isinstance(value, (int, str)) else float(value),
                    "scope": {"perimeter": "dataset", "value": dataset_scope_name},
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
                    "scope": {
                        "perimeter": "column",
                        "value": variable_name,
                        "parent_scope": {
                            "perimeter": "dataset",
                            "value": dataset_scope_name,
                        },
                    },
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


class OutlierAggregator:
    """Accumulate outlier and normality signals across chunks and finalize metrics/recommendations."""

    def __init__(self) -> None:
        self.col_outliers: Dict[str, int] = {}
        self.col_norm_weighted_sum: Dict[str, float] = {}
        self.col_rows: Dict[str, int] = {}
        self.dataset_outliers: int = 0
        self.dataset_norm_weighted_sum: float = 0.0
        self.dataset_norm_weight: int = 0
        self.total_rows: int = 0

    def add_column_stats(self, column: str, mean_normality: float, outlier_count: int, rows: int) -> None:
        self.col_outliers[column] = self.col_outliers.get(column, 0) + int(outlier_count)
        self.col_norm_weighted_sum[column] = self.col_norm_weighted_sum.get(column, 0.0) + (float(mean_normality) * int(rows))
        self.col_rows[column] = self.col_rows.get(column, 0) + int(rows)

    def add_dataset_stats(self, mean_normality: float, rows: int, multivariate_outliers_count: int) -> None:
        self.dataset_outliers += int(multivariate_outliers_count)
        self.dataset_norm_weighted_sum += float(mean_normality) * int(rows)
        self.dataset_norm_weight += int(rows)
        self.total_rows += int(rows)

    def finalize_metrics_and_recommendations(
        self, root_dataset_name: str, normality_threshold: float
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        metrics: List[Dict[str, Any]] = []
        recommendations: List[Dict[str, Any]] = []

        # Columns
        for col, rows in self.col_rows.items():
            r = max(int(rows), 1)
            mean_norm = round(self.col_norm_weighted_sum.get(col, 0.0) / r, 2)
            metrics.append(
                {
                    "key": "normality_score",
                    "value": mean_norm,
                    "scope": {
                        "perimeter": "column",
                        "value": col,
                        "parent_scope": {"perimeter": "dataset", "value": root_dataset_name},
                    },
                }
            )
            metrics.append(
                {
                    "key": "outliers",
                    "value": int(self.col_outliers.get(col, 0)),
                    "scope": {
                        "perimeter": "column",
                        "value": col,
                        "parent_scope": {"perimeter": "dataset", "value": root_dataset_name},
                    },
                }
            )
            if mean_norm < normality_threshold:
                recommendations.append(
                    {
                        "content": f"Column '{col}' has a normality score of {mean_norm*100}%.",
                        "type": "Outliers",
                        "scope": {
                            "perimeter": "column",
                            "value": col,
                            "parent_scope": {"perimeter": "dataset", "value": root_dataset_name},
                        },
                        "level": _determine_recommendation_level(1 - mean_norm),
                    }
                )

        # Dataset
        dataset_norm = round(self.dataset_norm_weighted_sum / max(self.dataset_norm_weight, 1), 2)
        metrics.append({"key": "outliers", "value": int(self.dataset_outliers), "scope": {"perimeter": "dataset", "value": root_dataset_name}})
        metrics.append({"key": "normality_score_dataset", "value": dataset_norm, "scope": {"perimeter": "dataset", "value": root_dataset_name}})
        metrics.append({"key": "score", "value": str(dataset_norm), "scope": {"perimeter": "dataset", "value": root_dataset_name}})
        total_outliers_count = int(sum(self.col_outliers.values()))
        metrics.append({"key": "total_outliers_count", "value": total_outliers_count, "scope": {"perimeter": "dataset", "value": root_dataset_name}})

        if dataset_norm < normality_threshold:
            recommendations.append(
                {
                    "content": f"The dataset '{root_dataset_name}' has a normality score of {dataset_norm*100}%.",
                    "type": "Outliers",
                    "scope": {"perimeter": "dataset", "value": root_dataset_name},
                    "level": _determine_recommendation_level(1 - dataset_norm),
                }
            )

        recommendations.append(
            {
                "content": f"The dataset '{root_dataset_name}' has a total of {total_outliers_count} outliers. Check them in output file.",
                "type": "Outliers",
                "scope": {"perimeter": "dataset", "value": root_dataset_name},
                "level": _determine_recommendation_level(total_outliers_count / max(1, self.total_rows)),
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
    """Aggregate duplicate statistics across chunks for a set of uniqueness columns."""

    def __init__(self, uniqueness_columns: Iterable[str]):
        self.uniqueness_columns = list(uniqueness_columns)
        self.total_rows: int = 0
        self.combo_to_count: Dict[Tuple[Any, ...], int] = {}

    def _sanitize_key_tuple(self, values: Tuple[Any, ...]) -> Tuple[Any, ...]:
        sanitized: List[Any] = []
        for v in values:
            if v is None:
                sanitized.append(None)
                continue
            try:
                if pd is not None and pd.isna(v):  # type: ignore[attr-defined]
                    sanitized.append(None)
                else:
                    sanitized.append(v)
            except Exception:
                sanitized.append(v)
        return tuple(sanitized)

    def add_df(self, df: "pd.DataFrame") -> None:  # type: ignore[name-defined]
        if pd is None:
            raise RuntimeError("pandas is required for DuplicateAggregator")
        self.total_rows += int(len(df))
        subset = df[self.uniqueness_columns]
        # value_counts on DataFrame returns a Series with MultiIndex keys
        counts = subset.value_counts(dropna=False)
        if hasattr(counts, "items"):
            for key, count in counts.items():
                if not isinstance(key, tuple):
                    key = (key,)
                key_t = self._sanitize_key_tuple(key)
                self.combo_to_count[key_t] = self.combo_to_count.get(key_t, 0) + int(count)

    def finalize_metrics(
        self, dataset_scope_name: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        metrics: List[Dict[str, Any]] = []
        recommendations: List[Dict[str, Any]] = []
        total_dups = 0
        for c in self.combo_to_count.values():
            if c > 1:
                total_dups += (c - 1)
        duplication_rate = (float(total_dups) / float(max(self.total_rows, 1))) if self.total_rows else 0.0
        score = max(0.0, min(1.0, 1.0 - duplication_rate))
        metrics.append({"key": "score", "value": str(round(score, 2)), "scope": {"perimeter": "dataset", "value": dataset_scope_name}})
        metrics.append({"key": "duplicates", "value": int(total_dups), "scope": {"perimeter": "dataset", "value": dataset_scope_name}})
        return metrics, recommendations

    def get_duplicate_keys(self) -> List[Tuple[Any, ...]]:
        return [k for k, c in self.combo_to_count.items() if c and c > 1]


class TimelinessAggregator:
    """Aggregate earliest/latest per column across chunks to compute timeliness metrics consistently."""

    def __init__(self) -> None:
        self.date_cols: Dict[str, Dict[str, Any]] = {}
        # structure: col -> {kind: "date"|"year", min: value, max: value}

    def add_year_obs(self, column: str, earliest_year: int, latest_year: int) -> None:
        entry = self.date_cols.get(column) or {"kind": "year", "min": None, "max": None}
        entry["kind"] = "year"
        entry["min"] = earliest_year if entry.get("min") is None else min(int(entry["min"]), int(earliest_year))
        entry["max"] = latest_year if entry.get("max") is None else max(int(entry["max"]), int(latest_year))
        self.date_cols[column] = entry

    def add_date_obs(self, column: str, earliest_date: "_dt.date", latest_date: "_dt.date") -> None:
        entry = self.date_cols.get(column) or {"kind": "date", "min": None, "max": None}
        entry["kind"] = "date"
        if entry.get("min") is None or (earliest_date and earliest_date < entry["min"]):
            entry["min"] = earliest_date
        if entry.get("max") is None or (latest_date and latest_date > entry["max"]):
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
        eligible_columns = set(compute_score_columns) if compute_score_columns else None
        scores: List[float] = []

        for col, info in self.date_cols.items():
            kind = info.get("kind")
            if kind == "year":
                earliest_year = int(info.get("min")) if info.get("min") is not None else None
                latest_year = int(info.get("max")) if info.get("max") is not None else None
                if earliest_year is None or latest_year is None:
                    continue
                days_since_latest_year = (now.year - latest_year) * 365
                days_since_earliest_year = (now.year - earliest_year) * 365
                timeliness_score = calc_timeliness_score(days_since_latest_year)
                # metrics
                metrics.extend(
                    [
                        {"key": "earliest_year", "value": str(earliest_year), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                        {"key": "latest_year", "value": str(latest_year), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                        {"key": "days_since_earliest_year", "value": str(days_since_earliest_year), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                        {"key": "days_since_latest_year", "value": str(days_since_latest_year), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                        {"key": "timeliness_score", "value": str(round(timeliness_score, 2)), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                    ]
                )
                if days_since_latest_year > 365:
                    recommendations.append(
                        {
                            "content": f"The latest date in column '{col}' is more than one year old.",
                            "type": "Latest Date far in the past",
                            "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}},
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
                days_since_latest = (now - latest_date).days
                days_since_earliest = (now - earliest_date).days
                timeliness_score = calc_timeliness_score(days_since_latest)
                metrics.extend(
                    [
                        {"key": "earliest_date", "value": earliest_date.strftime("%Y-%m-%d"), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                        {"key": "latest_date", "value": latest_date.strftime("%Y-%m-%d"), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                        {"key": "days_since_earliest_date", "value": str(days_since_earliest), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                        {"key": "days_since_latest_date", "value": str(days_since_latest), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                        {"key": "timeliness_score", "value": str(round(timeliness_score, 2)), "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}},
                    ]
                )
                if days_since_latest > 365:
                    recommendations.append(
                        {
                            "content": f"The latest date in column '{col}' is more than one year old.",
                            "type": "Latest Date far in the past",
                            "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}},
                            "level": "high",
                        }
                    )
                if (eligible_columns is None) or (col in eligible_columns):
                    scores.append(float(timeliness_score))

        if scores:
            avg_score = sum(scores) / float(len(scores))
            metrics.append({"key": "score", "value": str(round(avg_score, 2)), "scope": {"perimeter": "dataset", "value": dataset_scope_name}})
        return metrics, recommendations


