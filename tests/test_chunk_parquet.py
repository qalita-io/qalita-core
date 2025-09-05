"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import os
import json
import sqlite3
import tempfile
from pathlib import Path
from qalita_core.pack import Pack


def _write_json(path: Path, data: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _mk_pack(tmp_path: Path, source_conf_obj: dict) -> Pack:
    pack_conf_path = tmp_path / "pack_conf.json"
    source_conf_path = tmp_path / "source_conf.json"
    target_conf_path = tmp_path / "target_conf.json"

    # Minimal pack config with job stanza
    _write_json(
        pack_conf_path,
        {
            "job": {
                "id_columns": [],
                "source": {"skiprows": 0},
            }
        },
    )
    _write_json(source_conf_path, source_conf_obj)
    _write_json(target_conf_path, {"type": "file", "config": {"path": str(tmp_path / "empty.csv")}})
    (tmp_path / "empty.csv").write_text("col\n", encoding="utf-8")

    # Reuse existing agent file in tests data
    tests_data_dir = Path(__file__).parent / "data"
    agent_file = tests_data_dir / ".agent"

    return Pack(
        configs={
            "pack_conf": str(pack_conf_path),
            "source_conf": str(source_conf_path),
            "target_conf": str(target_conf_path),
            "agent_file": str(agent_file),
        }
    )


def test_csv_chunked_parquet_and_filenames(tmp_path):
    # Create a CSV with 2,500 rows
    csv_path = tmp_path / "testdata.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("id,value\n")
        for i in range(2500):
            f.write(f"{i},{i*2}\n")

    pack = _mk_pack(
        tmp_path,
        {"type": "file", "config": {"path": str(csv_path)}},
    )
    # Force small chunks and custom output dir
    out_dir = tmp_path / "parquet_out"
    pack.pack_config.setdefault("job", {})
    pack.pack_config["job"]["parquet_output_dir"] = str(out_dir)
    pack.pack_config["job"]["chunk_rows"] = 1000

    paths = pack.load_data("source")
    assert isinstance(paths, list) and len(paths) == 3
    assert all(p.endswith(".parquet") for p in paths)
    assert all(os.path.exists(p) for p in paths)
    # Deterministic names
    expected_prefix = (out_dir / "file_testdata").as_posix()
    assert paths[0].endswith("file_testdata_part_1.parquet")
    assert paths[1].endswith("file_testdata_part_2.parquet")
    assert paths[2].endswith("file_testdata_part_3.parquet")


def test_sqlite_table_chunked_parquet(tmp_path):
    # Prepare sqlite db
    db_path = tmp_path / "sample.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, val INTEGER)")
    cur.executemany("INSERT INTO items(val) VALUES (?)", [(i,) for i in range(2500)])
    conn.commit()
    conn.close()

    source_conf = {
        "type": "sqlite",
        "config": {"connection_string": f"sqlite:///{db_path}"},
    }
    pack = _mk_pack(tmp_path, source_conf)

    out_dir = tmp_path / "parquet_db"
    pack.pack_config.setdefault("job", {})
    pack.pack_config["job"]["parquet_output_dir"] = str(out_dir)
    pack.pack_config["job"]["chunk_rows"] = 1000

    paths = pack.load_data("source", table_or_query="items")
    assert isinstance(paths, list) and len(paths) == 3
    assert all(p.endswith(".parquet") for p in paths)
    assert all(os.path.exists(p) for p in paths)
    # sqlite table naming
    assert paths[0].endswith("sqlite_items_part_1.parquet")


def test_sqlite_query_chunked_parquet(tmp_path):
    # Prepare sqlite db
    db_path = tmp_path / "query.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
    cur.executemany("INSERT INTO t(val) VALUES (?)", [(i,) for i in range(1500)])
    conn.commit()
    conn.close()

    pack = _mk_pack(
        tmp_path,
        {"type": "sqlite", "config": {"connection_string": f"sqlite:///{db_path}"}},
    )
    out_dir = tmp_path / "parquet_query"
    pack.pack_config.setdefault("job", {})
    pack.pack_config["job"]["parquet_output_dir"] = str(out_dir)
    pack.pack_config["job"]["chunk_rows"] = 1000

    paths = pack.load_data("source", table_or_query="SELECT * FROM t")
    assert isinstance(paths, list) and len(paths) == 2
    assert all(p.endswith(".parquet") for p in paths)
    assert all(os.path.exists(p) for p in paths)
    # query naming
    assert paths[0].endswith("sqlite_query_part_1.parquet")

