"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.figures
"""

import json
import pandas as pd
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


def test_top_n_folds_tail_into_other():
    df = pd.DataFrame({"column": list("abcde"), "v": [10, 8, 6, 4, 2]})
    out = top_n(df, by="v", n=3)
    assert list(out["column"]) == ["a", "b", "c", "Autres"]
    assert list(out["v"]) == [10, 8, 6, 6]


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


def test_pack_exposes_a_figures_asset(config_paths):
    from qalita_core.pack import Pack

    pack = Pack(configs=config_paths)
    assert isinstance(pack.figures, FiguresAsset)
    assert pack.figures.type == "figures"
    assert pack.figures.data == {"version": 1, "measures": {}, "figures": []}
