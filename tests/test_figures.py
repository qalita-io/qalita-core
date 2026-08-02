"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.figures
"""

import json
import math
import pandas as pd
import polars as pl
import pytest

from qalita_core.figures import MAX_ROWS, FiguresAsset, top_n


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


def test_add_rejects_empty_dims():
    fig = FiguresAsset()
    with pytest.raises(ValueError, match="dims"):
        fig.add(
            "x",
            intent="breakdown",
            frame=pd.DataFrame({"v": [1]}),
            dims=[],
            measures=["v"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_rejects_empty_measures():
    # Sans mesure, add devient un projecteur de colonnes : 2 000 000 de lignes
    # d'identifiants patients sortiraient tronquées à 5000, sans une plainte.
    fig = FiguresAsset()
    with pytest.raises(ValueError, match="measures"):
        fig.add(
            "x",
            intent="breakdown",
            frame=pd.DataFrame({"patient_id": ["p1", "p2"]}),
            dims=["patient_id"],
            measures=[],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_rejects_duplicate_dim_tuples():
    # Un agrégat a exactement une ligne par tuple de dimensions : un doublon
    # signe des lignes brutes.
    fig = FiguresAsset()
    raw = pd.DataFrame(
        {"service": ["cardio", "uro", "cardio"], "age": [41, 12, 77]}
    )
    with pytest.raises(ValueError, match="agrégat"):
        fig.add(
            "x",
            intent="breakdown",
            frame=raw,
            dims=["service"],
            measures=["age"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_rejects_duplicate_dim_tuples_across_several_dims():
    fig = FiguresAsset()
    raw = [
        {"service": "cardio", "day": "2026-01-01", "n": 1},
        {"service": "cardio", "day": "2026-01-02", "n": 1},
        {"service": "cardio", "day": "2026-01-01", "n": 1},
    ]
    with pytest.raises(ValueError, match="agrégat"):
        fig.add(
            "x",
            intent="trend",
            frame=raw,
            dims=["service", ("day", "temporal")],
            measures=["n"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_accepts_distinct_dim_tuples_across_several_dims():
    fig = FiguresAsset()
    aggregate = [
        {"service": "cardio", "day": "2026-01-01", "n": 4},
        {"service": "cardio", "day": "2026-01-02", "n": 7},
        {"service": "uro", "day": "2026-01-01", "n": 2},
    ]
    fig.add(
        "x",
        intent="trend",
        frame=aggregate,
        dims=["service", ("day", "temporal")],
        measures=["n"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    assert len(fig.data["figures"][0]["rows"]) == 3


def test_add_documents_the_aggregate_constraint_at_the_call_site():
    assert "agrégat" in FiguresAsset.add.__doc__


def test_add_documents_what_the_guards_do_not_catch():
    # Des lignes brutes clés par identifiant unique n'ont aucun tuple en
    # double : elles passent les trois garde-fous. Le docstring doit le dire.
    assert "identifiant" in FiguresAsset.add.__doc__


def test_add_accepts_a_generator_for_measures():
    fig = FiguresAsset()
    fig.add(
        "x",
        intent="breakdown",
        frame=pd.DataFrame({"patient_id": ["a", "b"], "v": [1, 2]}),
        dims=["patient_id"],
        measures=(m for m in ["v"]),
        scope={"perimeter": "dataset", "value": "t"},
    )
    figure = fig.data["figures"][0]
    assert figure["measures"] == ["v"]
    assert figure["rows"] == [["a", 1], ["b", 2]]


def test_add_accepts_a_generator_for_dims():
    fig = FiguresAsset()
    fig.add(
        "x",
        intent="breakdown",
        frame=pd.DataFrame({"c": ["a", "b"], "v": [1, 2]}),
        dims=(d for d in ["c"]),
        measures=["v"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    figure = fig.data["figures"][0]
    assert figure["dims"] == [{"name": "c", "type": "nominal"}]
    assert figure["rows"] == [["a", 1], ["b", 2]]


def test_add_detects_duplicate_dims_that_only_collapse_after_sanitisation():
    # None et NaN sont deux valeurs distinctes en entrée, une seule en sortie.
    fig = FiguresAsset()
    with pytest.raises(ValueError, match="agrégat"):
        fig.add(
            "x",
            intent="breakdown",
            frame=[{"d": None, "v": 1}, {"d": float("nan"), "v": 2}],
            dims=["d"],
            measures=["v"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_add_detects_duplicate_dims_across_numpy_and_python_scalars():
    import numpy as np

    fig = FiguresAsset()
    with pytest.raises(ValueError, match="agrégat"):
        fig.add(
            "x",
            intent="breakdown",
            frame=[{"d": np.int64(1), "v": 1}, {"d": 1, "v": 2}],
            dims=["d"],
            measures=["v"],
            scope={"perimeter": "dataset", "value": "t"},
        )


def test_top_n_folds_tail_into_other():
    df = pd.DataFrame({"column": list("abcde"), "v": [10, 8, 6, 4, 2]})
    out = top_n(df, by="v", n=3, other=True)
    assert list(out["column"]) == ["a", "b", "c", "Autres"]
    assert list(out["v"]) == [10, 8, 6, 6]
    # 6 == 6.0 : sans contrôle de dtype, un élargissement silencieux de la
    # mesure en float64 passerait l'assertion ci-dessus.
    assert out["v"].dtype == df["v"].dtype
    assert out["column"].dtype == df["column"].dtype


def test_top_n_folds_tail_into_other_polars():
    df = pl.DataFrame({"column": list("abcde"), "v": [10, 8, 6, 4, 2]})
    out = top_n(df, by="v", n=3, other=True)
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
    out = top_n(frame, by="v", n=3, other=True)
    assert isinstance(out, list)
    assert [r["column"] for r in out] == ["a", "b", "c", "Autres"]
    assert [r["v"] for r in out] == [10, 8, 6, 6]


def _two_measure_records():
    return [
        {"column": "a", "n": 10, "ratio": 0.5},
        {"column": "b", "n": 8, "ratio": 0.2},
        {"column": "c", "n": 6, "ratio": 0.1},
        {"column": "d", "n": 4, "ratio": 0.1},
        {"column": "e", "n": 2, "ratio": 0.1},
    ]


def _measure_first_records():
    """Frame dont la dimension n'est pas la première colonne : (ratio, code, n)."""
    return [
        {"ratio": 0.5, "code": 1, "n": 10},
        {"ratio": 0.2, "code": 2, "n": 8},
        {"ratio": 0.1, "code": 3, "n": 6},
        {"ratio": 0.1, "code": 4, "n": 4},
    ]


def test_top_n_labels_the_named_dim_in_a_measure_first_frame_polars():
    frame = pl.DataFrame(_measure_first_records())
    out = top_n(frame, by="n", n=2, other=True, dim="code")
    # `ratio` n'est pas la dimension : il ne reçoit pas le label et ne bascule
    # pas en str, quelle que soit sa position dans le frame.
    assert out["ratio"].dtype == pl.Float64
    assert out["ratio"].to_list() == [0.5, 0.2, None]
    assert out["code"].to_list() == ["1", "2", "Autres"]
    assert out["n"].to_list() == [10, 8, 10]


def test_top_n_labels_the_named_dim_in_a_measure_first_frame_pandas():
    frame = pd.DataFrame(_measure_first_records())
    out = top_n(frame, by="n", n=2, other=True, dim="code")
    records = out.to_dict(orient="records")
    assert [r["code"] for r in records] == [1, 2, "Autres"]
    assert [r["ratio"] for r in records[:2]] == [0.5, 0.2]
    assert records[2]["ratio"] is None or math.isnan(records[2]["ratio"])
    assert out["n"].dtype == frame["n"].dtype


def test_top_n_requires_dim_when_several_columns_could_carry_the_label():
    # Rien dans la librairie ne contraint l'ordre des colonnes : deviner
    # positionnellement, c'est écrire un label dans une mesure.
    with pytest.raises(ValueError, match="dim="):
        top_n(pl.DataFrame(_measure_first_records()), by="n", n=2, other=True)


def test_top_n_requires_dim_even_when_there_is_no_tail_to_fold():
    with pytest.raises(ValueError, match="dim="):
        top_n(pl.DataFrame(_measure_first_records()), by="n", n=99, other=True)


def test_top_n_refuses_to_fold_a_frame_with_no_other_column():
    # Sinon la ligne repliée est indiscernable d'une vraie ligne.
    with pytest.raises(ValueError, match="label"):
        top_n(pl.DataFrame({"n": [5, 4, 3]}), by="n", n=2, other=True)


def test_top_n_rejects_an_unknown_dim():
    with pytest.raises(ValueError, match="absente"):
        top_n(
            pd.DataFrame(_two_measure_records()),
            by="n",
            n=3,
            other=True,
            dim="colunm",
        )


def test_top_n_rejects_a_dim_equal_to_by():
    with pytest.raises(ValueError, match="dim"):
        top_n(
            pd.DataFrame(_two_measure_records()),
            by="n",
            n=3,
            other=True,
            dim="n",
        )


def test_top_n_infers_the_dim_when_it_is_the_only_other_column():
    frame = pl.DataFrame({"column": list("abcde"), "v": [10, 8, 6, 4, 2]})
    out = top_n(frame, by="v", n=3, other=True)
    assert out["column"].to_list() == ["a", "b", "c", "Autres"]


def test_top_n_does_not_fabricate_a_label_in_a_second_measure_pandas():
    out = top_n(
        pd.DataFrame(_two_measure_records()),
        by="n",
        n=3,
        other=True,
        dim="column",
    )
    records = out.to_dict(orient="records")
    assert [r["column"] for r in records] == ["a", "b", "c", "Autres"]
    assert [r["n"] for r in records] == [10, 8, 6, 6]
    assert [r["ratio"] for r in records[:3]] == [0.5, 0.2, 0.1]
    folded_ratio = records[3]["ratio"]
    assert folded_ratio is None or math.isnan(folded_ratio)


def test_top_n_does_not_coerce_untouched_columns_to_str_polars():
    out = top_n(
        pl.DataFrame(_two_measure_records()),
        by="n",
        n=3,
        other=True,
        dim="column",
    )
    assert out["column"].to_list() == ["a", "b", "c", "Autres"]
    assert out["n"].to_list() == [10, 8, 6, 6]
    # the whole ratio column stayed float: 0.5 must not become "0.5"
    assert out["ratio"].dtype == pl.Float64
    assert out["ratio"].to_list() == [0.5, 0.2, 0.1, None]
    assert out["n"].dtype == pl.Int64


def test_top_n_does_not_fabricate_a_label_in_a_second_measure_list():
    out = top_n(_two_measure_records(), by="n", n=3, other=True, dim="column")
    assert out[3] == {"column": "Autres", "n": 6, "ratio": None}
    assert out[:3] == _two_measure_records()[:3]


def test_top_n_drops_the_tail_by_default():
    df = pd.DataFrame(_two_measure_records())
    out = top_n(df, by="n", n=3)
    assert list(out["column"]) == ["a", "b", "c"]
    assert list(out["ratio"]) == [0.5, 0.2, 0.1]


def test_top_n_drops_the_tail_by_default_polars():
    out = top_n(pl.DataFrame(_two_measure_records()), by="n", n=3)
    assert out["column"].to_list() == ["a", "b", "c"]
    assert out["ratio"].dtype == pl.Float64


def test_top_n_drops_the_tail_by_default_list_of_dicts():
    out = top_n(_two_measure_records(), by="n", n=3)
    assert out == _two_measure_records()[:3]


def test_top_n_rejects_unknown_by_pandas():
    with pytest.raises(ValueError, match="absente"):
        top_n(pd.DataFrame(_two_measure_records()), by="nn", n=3, other=True)


def test_top_n_rejects_unknown_by_polars():
    with pytest.raises(ValueError, match="absente"):
        top_n(pl.DataFrame(_two_measure_records()), by="nn", n=3, other=True)


def test_top_n_rejects_unknown_by_list_of_dicts():
    with pytest.raises(ValueError, match="absente"):
        top_n(_two_measure_records(), by="nn", n=3)


def test_top_n_preserves_measure_dtypes_when_no_tail_polars():
    frame = pl.DataFrame(_two_measure_records())
    out = top_n(frame, by="n", n=10, other=True, dim="column")
    assert out.schema == frame.schema
    assert out["ratio"].to_list() == [0.5, 0.2, 0.1, 0.1, 0.1]


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


def _assert_json_scalars(rows):
    for row in rows:
        for value in row:
            assert value is None or type(value) in (
                bool,
                int,
                float,
                str,
            ), f"{value!r} ({type(value).__name__}) n'est pas un scalaire JSON"


def test_add_sanitises_pandas_scalars_end_to_end(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    import numpy as np

    fig = FiguresAsset()
    fig.declare_measure("n", unit="count")
    fig.declare_measure("ratio", unit="ratio")
    frame = pd.DataFrame(
        {
            "day": pd.to_datetime(["2026-01-01", "2026-01-02", None]),
            "n": np.array([3, 7, 1], dtype="int64"),
            "ratio": np.array([0.25, float("nan"), 0.5], dtype="float64"),
        }
    )
    fig.add(
        "admissions",
        intent="trend",
        frame=frame,
        dims=[("day", "temporal")],
        measures=["n", "ratio"],
        scope={"perimeter": "dataset", "value": "patients"},
    )
    rows = fig.data["figures"][0]["rows"]
    _assert_json_scalars(rows)
    # une date manquante est nulle, jamais la catégorie "NaT"
    assert rows == [
        ["2026-01-01T00:00:00", 3, 0.25],
        ["2026-01-02T00:00:00", 7, None],
        [None, 1, 0.5],
    ]
    fig.save()
    payload = json.loads(
        (tmp_path / "figures.json").read_text(encoding="utf-8")
    )
    assert payload["figures"][0]["rows"] == rows


def test_add_sanitises_polars_scalars_end_to_end():
    import datetime as dt

    fig = FiguresAsset()
    frame = pl.DataFrame(
        {
            "day": [dt.date(2026, 1, 1), None],
            "n": pl.Series([3, 7], dtype=pl.Int32),
            "ratio": pl.Series([0.25, None], dtype=pl.Float32),
        }
    )
    fig.add(
        "admissions",
        intent="trend",
        frame=frame,
        dims=[("day", "temporal")],
        measures=["n", "ratio"],
        scope={"perimeter": "dataset", "value": "patients"},
    )
    rows = fig.data["figures"][0]["rows"]
    _assert_json_scalars(rows)
    assert rows[0][0] == "2026-01-01"
    assert rows[1][0] is None
    assert rows[1][2] is None


def test_add_sanitises_decimals_from_a_sql_aggregate():
    from decimal import Decimal

    fig = FiguresAsset()
    frame = [
        {"service": "cardio", "total": Decimal("1234.50")},
        {"service": "uro", "total": Decimal("7.25")},
    ]
    fig.add(
        "totaux",
        intent="breakdown",
        frame=frame,
        dims=["service"],
        measures=["total"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    rows = fig.data["figures"][0]["rows"]
    _assert_json_scalars(rows)
    assert rows == [["cardio", 1234.5], ["uro", 7.25]]


def test_add_tells_a_lazyframe_author_to_collect():
    # scan_data() est l'idiom big-data documenté de la librairie : passer le
    # résultat d'un group_by lazy est du code de pack naturel.
    fig = FiguresAsset()
    lazy = pl.DataFrame({"c": ["a"], "v": [1]}).lazy()
    with pytest.raises(TypeError, match=r"collect\(\)"):
        fig.add(
            "x",
            intent="breakdown",
            frame=lazy,
            dims=["c"],
            measures=["v"],
            scope={"perimeter": "dataset", "value": "t"},
        )


class _MaterialisationSpy:
    """Frame pandas qui refuse d'être matérialisé en entier."""

    def __init__(self, frame):
        self._frame = frame
        self.head_sizes = []

    @property
    def columns(self):
        return list(self._frame.columns)

    def head(self, size):
        self.head_sizes.append(size)
        return self._frame.head(size)

    def to_dict(self, orient):
        raise AssertionError(
            "frame entier matérialisé avant l'application du plafond"
        )


def test_add_slices_to_the_cap_before_materialising_rows():
    spy = _MaterialisationSpy(
        pd.DataFrame({"c": [f"c{i}" for i in range(50)], "v": range(50)})
    )
    fig = FiguresAsset()
    fig.add(
        "big",
        intent="breakdown",
        frame=spy,
        dims=["c"],
        measures=["v"],
        scope={"perimeter": "dataset", "value": "t"},
        max_rows=10,
    )
    assert spy.head_sizes == [11]
    figure = fig.data["figures"][0]
    assert len(figure["rows"]) == 10
    assert figure["truncated"] is True


def test_add_uses_a_default_cap_of_five_thousand_rows():
    assert MAX_ROWS == 5000
    fig = FiguresAsset()
    frame = [{"c": f"c{i}", "v": i} for i in range(MAX_ROWS + 1)]
    fig.add(
        "big",
        intent="breakdown",
        frame=frame,
        dims=["c"],
        measures=["v"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    figure = fig.data["figures"][0]
    assert len(figure["rows"]) == 5000
    assert figure["truncated"] is True


def test_add_does_not_flag_a_frame_exactly_at_the_default_cap():
    fig = FiguresAsset()
    frame = [{"c": f"c{i}", "v": i} for i in range(MAX_ROWS)]
    fig.add(
        "big",
        intent="breakdown",
        frame=frame,
        dims=["c"],
        measures=["v"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    figure = fig.data["figures"][0]
    assert len(figure["rows"]) == 5000
    assert figure["truncated"] is False


def _figure_with(fig, **kwargs):
    defaults = dict(
        intent="breakdown",
        frame=pd.DataFrame({"c": ["a"], "p_missing": [0.5]}),
        dims=["c"],
        measures=["p_missing"],
        scope={"perimeter": "dataset", "value": "t"},
    )
    defaults.update(kwargs)
    fig.add("f", **defaults)


def test_save_rejects_an_of_that_is_not_a_declared_measure(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    fig = FiguresAsset()
    fig.declare_measure("p_missing", unit="ratio")
    _figure_with(fig, of="p_mising")
    with pytest.raises(ValueError, match="p_mising"):
        fig.save()
    assert not (tmp_path / "figures.json").exists()


def test_save_rejects_a_measure_name_that_is_not_declared(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    fig = FiguresAsset()
    fig.declare_measure("p_missing", unit="ratio")
    _figure_with(
        fig,
        frame=pd.DataFrame({"c": ["a"], "p_mising": [0.5]}),
        measures=["p_mising"],
    )
    with pytest.raises(ValueError, match="p_mising"):
        fig.save()
    assert not (tmp_path / "figures.json").exists()


def test_save_names_the_figure_key_in_the_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fig = FiguresAsset()
    fig.declare_measure("p_missing", unit="ratio")
    _figure_with(fig, of="ghost")
    with pytest.raises(ValueError, match="'f'"):
        fig.save()


def test_save_accepts_measures_declared_after_the_figure(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    fig = FiguresAsset()
    _figure_with(fig, of="p_missing")
    fig.declare_measure("p_missing", unit="ratio")
    fig.save()
    payload = json.loads(
        (tmp_path / "figures.json").read_text(encoding="utf-8")
    )
    assert payload["figures"][0]["of"] == "p_missing"


def test_save_does_not_require_measures_for_a_raw_figure(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    fig = FiguresAsset()
    fig.add_raw(
        "custom",
        option={"series": []},
        scope={"perimeter": "dataset", "value": "t"},
    )
    fig.save()
    assert (tmp_path / "figures.json").exists()


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
