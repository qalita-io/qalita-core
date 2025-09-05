"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

from qalita_core.pack import Pack
import pytest
import os


@pytest.fixture(scope="session")
def config_paths():
    base_path = os.path.dirname(__file__)
    configs = {
        "pack_conf": os.path.join(base_path, "data", "pack_conf.json"),
        "source_conf": os.path.join(base_path, "data", "source_conf.json"),
        "target_conf": os.path.join(base_path, "data", "target_conf.json"),
        "agent_file": os.path.join(base_path, "data", ".agent"),
    }
    return configs


@pytest.fixture(scope="session")
def pack(config_paths):
    pack = Pack(configs=config_paths)
    return pack


def test_pack_load_data_source(pack, tmp_path):
    pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
    data = pack.load_data("source")
    assert isinstance(data, list) and all(isinstance(p, str) for p in data)
    assert all(p.endswith(".parquet") for p in data)
    for p in data:
        assert os.path.exists(p)
