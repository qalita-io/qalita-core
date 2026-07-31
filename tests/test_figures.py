"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.figures
"""

import json
import pandas as pd
import polars as pl
import pytest

from qalita_core.figures import FiguresAsset, top_n


def test_add_builds_positional_rows_in_declared_order():
    fig = FiguresAsset()
    df = pd.DataFrame(
        {
            "column": ["birth_date", "phone"],
            "p_missing": [0.081, 0.007],
            "noise": [1, 2],
        }
    )
    fig.add(
        "missing_by_column",
        intent="breakdown",
        of="p_cells_missing",
        frame=df,
        dims=["column"],
        measures=["p_missing"],
        scope={"perimeter": "dataset", "value": "patients"},
    )
    figure = fig.data["figures"][0]
    assert figure["dims"] == [{"name": "column", "type": "nominal"}]
    assert figure["measures"] == ["p_missing"]
    assert figure["rows"] == [["birth_date", 0.081], ["phone", 0.007]]
    assert figure["truncated"] is False
    assert figure["of"] == "p_cells_missing"


def test_declare_measure_populates_shared_dictionary():
    fig = FiguresAsset()
    fig.declare_measure(
        "p_missing", unit="ratio", direction="lower_is_better", target=0.05
    )
    assert fig.data["measures"]["p_missing"] == {
        "unit": "ratio",
        "direction": "lower_is_better",
        "target": 0.05,
    }


def test_add_truncates_beyond_max_rows_and_flags_it():
    fig = FiguresAsset()
    df = pd.DataFrame({"column": [f"c{i}" for i in range(10)], "v": range(10)})
    fig.add(
        "big",
        intent="breakdown",
        frame=df,
        dims=["column"],
        measures=["v"],
        scope={"perimeter": "dataset", "value": "t"},
        max_rows=4,
    )
    figure = fig.data["figures"][0]
    assert len(figure["rows"]) == 4
    assert figure["truncated"] is True


def test_add_rejects_unknown_intent():
    fig = FiguresAsset()
    df = pd.DataFrame({"column": ["a"], "v": [1]})
    with pytest.raises(ValueError, match="intent"):
        fig.add(
            "x",
            intent="donut",
            frame=df,
            dims=["column"],
            measures=["v"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_normalizes_tuple_dim_with_ordinal_type():
    fig = FiguresAsset()
    df = pd.DataFrame({"grade": ["A", "B"], "v": [1, 2]})
    fig.add(
        "x",
        intent="breakdown",
        frame=df,
        dims=[("grade", "ordinal")],
        measures=["v"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    figure = fig.data["figures"][0]
    assert figure["dims"] == [{"name": "grade", "type": "ordinal"}]


def test_add_normalizes_tuple_dim_with_temporal_type():
    fig = FiguresAsset()
    df = pd.DataFrame({"day": ["2026-01-01", "2026-01-02"], "v": [1, 2]})
    fig.add(
        "x",
        intent="trend",
        frame=df,
        dims=[("day", "temporal")],
        measures=["v"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    figure = fig.data["figures"][0]
    assert figure["dims"] == [{"name": "day", "type": "temporal"}]


def test_add_rejects_invalid_dim_type():
    fig = FiguresAsset()
    df = pd.DataFrame({"column": ["a"], "v": [1]})
    with pytest.raises(ValueError, match="type de dimension"):
        fig.add(
            "x",
            intent="breakdown",
            frame=df,
            dims=[("column", "categorical")],
            measures=["v"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_rejects_missing_column():
    fig = FiguresAsset()
    df = pd.DataFrame({"column": ["a"]})
    with pytest.raises(ValueError, match="absente"):
        fig.add(
            "x",
            intent="breakdown",
            frame=df,
            dims=["column"],
            measures=["ghost"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_accepts_polars_dataframe():
    fig = FiguresAsset()
    df = pl.DataFrame(
        {
            "column": ["birth_date", "phone"],
            "p_missing": [0.081, 0.007],
        }
    )
    fig.add(
        "missing_by_column",
        intent="breakdown",
        frame=df,
        dims=["column"],
        measures=["p_missing"],
        scope={"perimeter": "dataset", "value": "patients"},
    )
    figure = fig.data["figures"][0]
    assert figure["rows"] == [["birth_date", 0.081], ["phone", 0.007]]


def test_add_accepts_list_of_dicts():
    fig = FiguresAsset()
    frame = [
        {"column": "birth_date", "p_missing": 0.081},
        {"column": "phone", "p_missing": 0.007},
    ]
    fig.add(
        "missing_by_column",
        intent="breakdown",
        frame=frame,
        dims=["column"],
        measures=["p_missing"],
        scope={"perimeter": "dataset", "value": "patients"},
    )
    figure = fig.data["figures"][0]
    assert figure["rows"] == [["birth_date", 0.081], ["phone", 0.007]]


def test_add_rejects_missing_column_on_empty_pandas_frame():
    fig = FiguresAsset()
    df = pd.DataFrame({"column": pd.Series(dtype="object")})
    with pytest.raises(ValueError, match="absente"):
        fig.add(
            "x",
            intent="breakdown",
            frame=df,
            dims=["column"],
            measures=["ghost"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_rejects_missing_column_on_empty_polars_frame():
    fig = FiguresAsset()
    df = pl.DataFrame({"column": pl.Series([], dtype=pl.Utf8)})
    with pytest.raises(ValueError, match="absente"):
        fig.add(
            "x",
            intent="breakdown",
            frame=df,
            dims=["column"],
            measures=["ghost"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_accepts_empty_list_of_dicts_without_declared_columns():
    fig = FiguresAsset()
    fig.add(
        "x",
        intent="breakdown",
        frame=[],
        dims=["column"],
        measures=["ghost"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    figure = fig.data["figures"][0]
    assert figure["rows"] == []


def test_top_n_folds_tail_into_other():
    df = pd.DataFrame({"column": list("abcde"), "v": [10, 8, 6, 4, 2]})
    out = top_n(df, by="v", n=3)
    assert list(out["column"]) == ["a", "b", "c", "Autres"]
    assert list(out["v"]) == [10, 8, 6, 6]


def test_top_n_folds_tail_into_other_polars():
    df = pl.DataFrame({"column": list("abcde"), "v": [10, 8, 6, 4, 2]})
    out = top_n(df, by="v", n=3)
    assert isinstance(out, pl.DataFrame)
    assert out["column"].to_list() == ["a", "b", "c", "Autres"]
    assert out["v"].to_list() == [10, 8, 6, 6]


def test_top_n_folds_tail_into_other_list_of_dicts():
    frame = [
        {"column": "a", "v": 10},
        {"column": "b", "v": 8},
        {"column": "c", "v": 6},
        {"column": "d", "v": 4},
        {"column": "e", "v": 2},
    ]
    out = top_n(frame, by="v", n=3)
    assert isinstance(out, list)
    assert [r["column"] for r in out] == ["a", "b", "c", "Autres"]
    assert [r["v"] for r in out] == [10, 8, 6, 6]


def test_save_writes_contract_to_disk(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fig = FiguresAsset()
    fig.declare_measure("v", unit="count")
    fig.add(
        "f",
        intent="breakdown",
        frame=pd.DataFrame({"c": ["a"], "v": [1]}),
        dims=["c"],
        measures=["v"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    fig.save()
    payload = json.loads(
        (tmp_path / "figures.json").read_text(encoding="utf-8")
    )
    assert payload["version"] == 1
    assert payload["measures"]["v"]["unit"] == "count"
    assert payload["figures"][0]["key"] == "f"


def test_add_raw_carries_option_with_no_dims_measures_or_rows():
    fig = FiguresAsset()
    option = {"series": [{"type": "sankey", "data": [{"name": "a"}]}]}
    fig.add_raw(
        "custom_sankey",
        option=option,
        scope={"perimeter": "dataset", "value": "t"},
    )
    figure = fig.data["figures"][0]
    assert figure["intent"] == "raw"
    assert figure["option"] == option
    assert "dims" not in figure
    assert "measures" not in figure
    assert "rows" not in figure


def test_pack_exposes_a_figures_asset(config_paths):
    from qalita_core.pack import Pack

    pack = Pack(configs=config_paths)
    assert isinstance(pack.figures, FiguresAsset)
    assert pack.figures.type == "figures"
    assert pack.figures.data == {"version": 1, "measures": {}, "figures": []}
