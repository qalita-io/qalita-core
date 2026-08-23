"""Regressions for the ways the loader could quietly change the data.

Each test here corresponds to a defect that produced *plausible* output: a
number close to the right one, a file that parsed, an element that decoded.
Nothing raised, so nothing caught them except reading the values back.
"""

import json
import logging
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from qalita_core import polars_io as pio
from qalita_core.data_source_opener import (
    S3Source,
    _SqlAlchemySource,
    _stage_remote_file,
)

# ---------------------------------------------------------------------------
# Decimal fidelity
# ---------------------------------------------------------------------------


def test_decimal_reaches_arrow_untouched():
    """float() used to be applied here, rounding away everything past ~15
    significant digits."""
    value = Decimal("12345678901234567890.12")
    assert pio._arrow_safe(value) is value


def test_decimal_column_keeps_every_digit_through_the_writer(tmp_path):
    exact = Decimal("12345678901234567890.12")
    with pio.ParquetPartWriter(str(tmp_path), "ledger") as writer:
        writer.write(pa.table({"amount": pa.array([exact, Decimal("0.01")])}))
    paths = writer.close()

    import polars as pl

    frame = pl.scan_parquet(paths).collect(engine="streaming")
    assert frame["amount"].to_list()[0] == exact


def test_float_would_have_lost_that_value():
    """The guard behind the test above: this is what the old code emitted."""
    exact = Decimal("12345678901234567890.12")
    assert Decimal(str(float(exact))) != exact


def test_a_decimal_column_still_profiles_as_a_number(tmp_path):
    """Keeping Decimal is only worth anything if the packs can still use the
    column: a NUMERIC that profiles as text would be a worse regression than
    the rounding it replaces."""
    from qalita_core import analytics, profiling

    with pio.ParquetPartWriter(str(tmp_path), "ledger") as writer:
        writer.write(
            pa.table(
                {
                    "amount": pa.array(
                        [Decimal("1.25"), Decimal("2.50"), Decimal("100.00")]
                    )
                }
            )
        )
    paths = writer.close()

    import polars as pl

    lazy = pl.scan_parquet(paths)
    assert analytics.numeric_columns(dict(lazy.collect_schema())) == ["amount"]

    report = profiling.profile(lazy)["amount"]
    # the sum is carried exactly, not as 103.75000000000001
    assert Decimal(str(report["sum"])) == Decimal("103.75")
    assert report["n_missing"] == 0


# ---------------------------------------------------------------------------
# Schema widening
# ---------------------------------------------------------------------------


def test_wider_decimal_covers_both_scales():
    covering = pio._covering_decimal(pa.decimal128(5, 2), pa.decimal128(7, 4))
    # 3 integer digits on the left, 3 on the right, scale 4 -> 7 digits
    assert covering == pa.decimal128(7, 4)


def test_decimal_widens_to_decimal_not_to_text():
    """Collapsing to large_string on a scale change would keep the digits but
    lose the type; collapsing to float64 would keep the type and lose the
    digits. Neither is acceptable for a numeric column."""
    wider = pio._wider_types(pa.decimal128(5, 2), pa.decimal128(7, 4))
    assert wider[0] == pa.decimal128(7, 4)


def test_polars_reads_decimal128_back_and_refuses_decimal256(tmp_path):
    """The ceiling the promotion obeys, measured instead of assumed.

    Arrow writes decimal256 happily; Polars reads Decimal128 and nothing
    wider, so a part written past 38 digits cannot be opened by the step that
    follows the load. In a job the refusal arrives as a Rust panic —
    ``PanicException`` inherits from ``BaseException``, so no ``except
    Exception`` in the chain catches it — while under pytest the same read
    surfaces as ``InvalidOperationError``; either way the object is lost.
    """
    import polars as pl
    from polars.exceptions import InvalidOperationError, PanicException

    readable = tmp_path / "readable.parquet"
    pq.write_table(
        pa.table({"q": pa.array([None], type=pa.decimal128(38, 18))}),
        str(readable),
    )
    assert pl.read_parquet(readable).height == 1

    unreadable = tmp_path / "unreadable.parquet"
    pq.write_table(
        pa.table({"q": pa.array([None], type=pa.decimal256(41, 21))}),
        str(unreadable),
    )
    with pytest.raises((PanicException, InvalidOperationError)) as caught:
        pl.read_parquet(unreadable)
    assert "Decimal256" in str(caught.value) or "38" in str(caught.value)


def test_covering_decimal_never_returns_something_unreadable():
    """Whatever comes in — including the decimal256 Arrow infers on its own —
    what comes out is a decimal128 the reader supports, or nothing at all."""
    types = [pa.int64(), pa.uint64()]
    for precision in (2, 20, 38, 39, 50, 76):
        build = pa.decimal128 if precision <= 38 else pa.decimal256
        for scale in (0, 1, 18, 21, 38):
            if scale <= precision:
                types.append(build(precision, scale))

    for pinned in types:
        for incoming in types:
            covering = pio._covering_decimal(pinned, incoming)
            if covering is None:
                continue
            assert pa.types.is_decimal128(covering), (
                pinned,
                incoming,
                covering,
            )
            assert covering.precision <= 38


def test_decimal_past_decimal128_becomes_readable_text(tmp_path, caplog):
    """The reported trigger: a PostgreSQL ``numeric`` declared without
    precision whose scale grows batch after batch. Twenty integer digits and
    twenty-two decimals fit no decimal Polars can read, so the column falls to
    text — keeping every digit, and keeping the object openable, which used to
    be what the promotion cost us."""
    import polars as pl

    big = Decimal("12345678901234567890.12")
    precise = Decimal("1.234567890123456789012")
    with caplog.at_level(logging.WARNING, logger="qalita_core.polars_io"):
        with pio.ParquetPartWriter(str(tmp_path), "ledger") as writer:
            writer.write(pa.table({"amount": pa.array([big])}))
            writer.write(pa.table({"amount": pa.array([precise])}))
        paths = writer.close()

    assert writer.schema.field("amount").type == pa.large_string()
    frame = pl.scan_parquet(paths).collect(engine="streaming")
    assert frame["amount"].to_list() == [str(big), str(precise)]
    # a column that stops being a number has to be visible in the job log
    assert "large_string" in caplog.text


def test_integer_and_decimal_meet_on_a_decimal():
    covering = pio._covering_decimal(pa.int64(), pa.decimal128(10, 4))
    assert pa.types.is_decimal(covering)
    assert covering.scale == 4
    # int64 needs 19 integer digits, so the result must hold 19 + 4
    assert covering.precision == 23


def test_unsigned_integer_gets_its_extra_digit():
    covering = pio._covering_decimal(pa.uint64(), pa.decimal128(5, 2))
    assert covering.precision == 20 + 2


def test_text_stays_terminal():
    assert pio._wider_types(pa.large_string(), pa.int64()) == []


def test_growing_scale_across_batches_keeps_the_values(tmp_path):
    with pio.ParquetPartWriter(str(tmp_path), "ledger") as writer:
        writer.write(pa.table({"amount": pa.array([Decimal("1.25")])}))
        writer.write(pa.table({"amount": pa.array([Decimal("1.2345")])}))
    paths = writer.close()

    import polars as pl

    values = pl.scan_parquet(paths).collect(engine="streaming")["amount"]
    assert [str(v) for v in values.to_list()] == ["1.2500", "1.2345"]


# ---------------------------------------------------------------------------
# JSON streaming
# ---------------------------------------------------------------------------


def test_scalar_split_across_the_buffer_is_one_value(tmp_path):
    """raw_decode returns 123 for a buffer ending in "123" whose file
    continues "456": the element used to be yielded twice, wrong both times."""
    path = tmp_path / "numbers.json"
    path.write_text("[123456789, 2]", encoding="utf-8")
    # a buffer that ends in the middle of the first number
    values = list(pio.iter_json_array(str(path), buffer_size=6))
    assert values == [123456789, 2]


@pytest.mark.parametrize("buffer_size", list(range(2, 20)))
def test_every_buffer_boundary_yields_the_same_elements(tmp_path, buffer_size):
    path = tmp_path / "mixed.json"
    payload = [1234, "abcdef", {"k": 5678}, [1, 2], True, None, 9.75]
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert list(pio.iter_json_array(str(path), buffer_size=buffer_size)) == (
        payload
    )


def test_json_array_behind_a_utf8_bom_is_read(tmp_path):
    path = tmp_path / "bom.json"
    path.write_text("﻿[1, 2, 3]", encoding="utf-8")
    assert list(pio.iter_json_array(str(path))) == [1, 2, 3]


def test_bom_prefixed_array_is_sniffed_as_an_array(tmp_path):
    """A BOM is not whitespace: with plain utf-8 the first character is
    \\ufeff, not '[', and every Windows-exported file was called NDJSON."""
    path = tmp_path / "bom.json"
    path.write_text("﻿[1, 2]", encoding="utf-8")
    assert pio.sniff_json_format(str(path)) == "array"


def test_ndjson_is_still_ndjson(tmp_path):
    path = tmp_path / "lines.json"
    path.write_text('{"a": 1}\n{"a": 2}\n', encoding="utf-8")
    assert pio.sniff_json_format(str(path)) == "ndjson"


def test_truncated_array_still_raises(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text('[{"a": 1}, {"a":', encoding="utf-8")
    with pytest.raises(ValueError):
        list(pio.iter_json_array(str(path)))


# ---------------------------------------------------------------------------
# S3 credentials
# ---------------------------------------------------------------------------


def test_object_key_is_not_sent_as_the_access_key():
    """bucket+key builds s3://bucket/key, so `key` names the object. Reading
    it as a credential authenticated with the object's own name."""
    source = S3Source(
        {
            "bucket": "reports",
            "key": "2026/august/ledger.parquet",
            "access_key": "AKIAREAL",
            "secret": "s3cret",
        }
    )
    options = source._storage_options()
    assert options["aws_access_key_id"] == "AKIAREAL"


def test_fsspec_style_key_is_still_a_credential_when_path_is_explicit():
    source = S3Source(
        {
            "path": "s3://reports/ledger.parquet",
            "key": "AKIAREAL",
            "secret": "s3cret",
        }
    )
    assert source._storage_options()["aws_access_key_id"] == "AKIAREAL"


def test_bucket_and_key_without_credentials_sends_none():
    source = S3Source({"bucket": "public", "key": "open/data.parquet"})
    assert source._storage_options() is None


# ---------------------------------------------------------------------------
# Remote staging
# ---------------------------------------------------------------------------


class _FakeFs:
    def __init__(self, size):
        self._size = size

    def size(self, _path):
        return self._size


class _FakeOpen:
    def __init__(self, payload, size):
        self.payload = payload
        self.fs = _FakeFs(size)

    def __enter__(self):
        import io

        return io.BytesIO(self.payload)

    def __exit__(self, *exc):
        return False


@pytest.fixture
def fake_fsspec(monkeypatch, tmp_path):
    import sys
    import types

    monkeypatch.setenv("TMPDIR", str(tmp_path))
    module = types.ModuleType("fsspec")
    module.open = lambda path, mode, **kw: module._handle
    monkeypatch.setitem(sys.modules, "fsspec", module)
    return module


def test_same_basename_from_two_buckets_stages_to_two_files(
    fake_fsspec, tmp_path
):
    """Both used to land on <TMPDIR>/qalita-staging/data.csv, so the second
    source analysed the first one's bytes."""
    fake_fsspec._handle = _FakeOpen(b"a,b\n1,2\n", 8)
    first = _stage_remote_file("s3://one/data.csv", None)
    fake_fsspec._handle = _FakeOpen(b"c,d\n3,4\n", 8)
    second = _stage_remote_file("s3://two/data.csv", None)

    assert first != second
    assert open(first, "rb").read() == b"a,b\n1,2\n"
    assert open(second, "rb").read() == b"c,d\n3,4\n"


def test_staging_refuses_an_object_that_will_not_fit(fake_fsspec):
    fake_fsspec._handle = _FakeOpen(b"x", 1 << 60)
    with pytest.raises(pio.InsufficientDiskSpaceError):
        _stage_remote_file("s3://one/huge.csv", None)


def test_an_unknown_remote_size_does_not_block_the_copy(fake_fsspec):
    class _NoSize(_FakeOpen):
        def __init__(self, payload):
            super().__init__(payload, 0)
            self.fs = None

    fake_fsspec._handle = _NoSize(b"ok")
    # fs is None -> .size() raises AttributeError -> treated as "unknown"
    assert open(_stage_remote_file("s3://one/x.csv", None), "rb").read() == (
        b"ok"
    )


def test_a_failed_copy_leaves_no_truncated_file(fake_fsspec, tmp_path):
    class _Exploding(_FakeOpen):
        def __enter__(self):
            class _Reader:
                def read(self, *_a):
                    raise OSError("connection reset")

            return _Reader()

    fake_fsspec._handle = _Exploding(b"", 8)
    with pytest.raises(OSError):
        _stage_remote_file("s3://one/data.csv", None)

    staged = tmp_path / "qalita-staging"
    assert list(staged.glob("*")) == []


# ---------------------------------------------------------------------------
# The shared SQL dispatch
# ---------------------------------------------------------------------------


class _RecordingSource(_SqlAlchemySource):
    """Every warehouse class now shares one _load_data. These tests drive that
    single implementation, which is what all sixteen subclasses inherit."""

    dialect_name = "warehouse"

    def __init__(self):
        self.tables_read = []

    def get_data(self, table_or_query=None, pack_config=None):
        raise NotImplementedError

    def _list_tables(self, engine, schema):
        return ["alpha", "beta"]

    def _read_table_to_parquet(
        self, engine, table_name, schema, output_dir, chunk_rows, dialect, **kw
    ):
        self.tables_read.append(table_name)
        return [f"/out/{table_name}.parquet"]

    def _object_base_name(self, dialect_name, suffix):
        return f"{dialect_name}_{suffix}"


def _load(source, table_or_query):
    return source._load_data(
        engine=object(),
        table_or_query=table_or_query,
        schema=None,
        output_dir="/out",
        chunk_rows=1000,
    )


def test_none_loads_every_table():
    source = _RecordingSource()
    assert _load(source, None) == [
        "/out/alpha.parquet",
        "/out/beta.parquet",
    ]


def test_star_loads_every_table():
    source = _RecordingSource()
    _load(source, "*")
    assert source.tables_read == ["alpha", "beta"]


def test_a_list_loads_exactly_those_tables():
    source = _RecordingSource()
    _load(source, ["gamma", "delta"])
    assert source.tables_read == ["gamma", "delta"]


def test_a_bare_name_is_a_table_not_a_query():
    source = _RecordingSource()
    _load(source, "customers")
    assert source.tables_read == ["customers"]


def test_a_select_goes_down_the_query_path(monkeypatch):
    seen = {}

    def fake_sql_to_parquet(
        _self, _engine, query, output_dir, base, chunk_rows, **kw
    ):
        seen["query"] = query
        seen["base"] = base
        return ["/out/query.parquet"]

    monkeypatch.setattr(
        "qalita_core.data_source_opener._sql_to_parquet", fake_sql_to_parquet
    )
    source = _RecordingSource()
    assert _load(source, "SELECT 1") == ["/out/query.parquet"]
    assert source.tables_read == []
    assert seen["base"] == "warehouse_query"


def test_an_unusable_argument_is_refused():
    with pytest.raises(TypeError):
        _load(_RecordingSource(), 42)


def test_qualify_uses_the_schema_when_there_is_one():
    source = _RecordingSource()
    assert source._qualify("t", "public") == ("public.t", "public.t")
    assert source._qualify("t", None) == ("t", "t")
