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


def test_pack_instantiation(pack):
    assert isinstance(pack, Pack), "Failed to instantiate Pack"


def test_pack_load_pack_config(pack):
    pack_config = pack.pack_config
    assert isinstance(pack_config, dict), "Failed to load pack configuration"

    expected_config = {
        "job": {"id_columns": [], "source": {"skiprows": 0}},
        "charts": {
            "overview": [
                {
                    "metric_key": "score",
                    "chart_type": "text",
                    "display_title": True,
                    "justify": True,
                }
            ],
            "scoped": [
                {
                    "metric_key": "decimal_precision",
                    "chart_type": "text",
                    "display_title": True,
                    "justify": True,
                },
                {
                    "metric_key": "proportion_score",
                    "chart_type": "text",
                    "display_title": True,
                    "justify": True,
                },
                {
                    "metric_key": "proportion_score",
                    "chart_type": "spark_area_chart",
                    "display_title": False,
                    "justify": False,
                },
            ],
        },
    }

    assert (
        pack_config == expected_config
    ), "pack_config does not match the expected configuration"


def test_pack_load_source_config(pack):
    source_config = pack.source_config
    assert isinstance(source_config, dict), "Failed to load source configuration"

    expected_config = {
        "config": {"path": "./tests/data/METABRIC_RNA_Mutation.xlsx"},
        "description": "Clinical attributes, m-RNA levels z-score, and genes mutations for 1904 patients",
        "id": 9,
        "name": "Breast Cancer Gene Expression Profiles (METABRIC)",
        "owner": "admin",
        "owner_id": 1,
        "reference": True,
        "sensitive": False,
        "type": "file",
        "validate": "valid",
        "visibility": "internal",
    }
    assert (
        source_config == expected_config
    ), "source_config does not match the expected configuration"


def test_pack_load_target_config(pack):
    target_config = pack.target_config
    assert isinstance(target_config, dict), "Failed to load target configuration"

    expected_config = {
        "config": {"path": "./tests/data/ref_bio_data.xlsx"},
        "description": "Ref data for clinical dataset",
        "id": 11,
        "name": "Bio referential data",
        "owner": "admin",
        "owner_id": 1,
        "reference": True,
        "sensitive": False,
        "type": "file",
        "validate": "valid",
        "visibility": "public",
    }

    assert (
        target_config == expected_config
    ), "target_config does not match the expected configuration"


def test_pack_load_agent_config(pack):
    agent_config = pack.agent_config

    assert isinstance(agent_config, dict), "Failed to load agent configuration"

    expected_config = {
        "user": {
            "id": 2,
            "email": "armand.leopold@qalita.io",
            "login": "armand.leopold",
            "name": "Armand LEOPOLD",
            "language": None,
            "avatar": None,
            "theme": "light",
            "home": "/home/de/sources",
            "role_id": 3,
            "role": "dataengineer",
            "role_override": True,
            "is_active": True,
            "created_at": "2024-02-18T20:07:36.620938",
            "last_activity": "2024-02-18T20:12:02.632269",
            "habilitations": [],
        },
        "context": {
            "local": {
                "name": "armand.leopold",
                "mode": "worker",
                "token": "",
                "url": "https://api.dev.platform.qalita.io",
                "verbose": False,
            },
            "remote": {
                "name": "armand.leopold",
                "mode": "worker",
                "status": "online",
                "id": 3,
                "is_active": True,
                "registered_at": "2024-02-22T11:34:08.736840",
                "last_status_check": "2024-02-22T11:34:08.736842",
            },
        },
        "registries": [
            {
                "name": "local",
                "id": 1,
                "url": "https://2829b56e82804f0c8acaab6521f17694-platform-dev-qalita-bucket.s3.gra.io.cloud.ovh.net",
            }
        ],
    }

    assert (
        agent_config == expected_config
    ), "agent_config does not match the expected configuration"


def test_pack_load_data_target(pack, tmp_path):
    # Ensure output dir is set via env by overriding pack config at runtime
    pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
    data = pack.load_data("target")
    assert isinstance(data, list) and all(isinstance(p, str) for p in data)
    assert all(p.endswith(".parquet") for p in data)
    # Files should exist
    for p in data:
        assert os.path.exists(p)


def test_pack_load_data_source(pack, tmp_path):
    pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
    data = pack.load_data("source")
    assert isinstance(data, list) and all(isinstance(p, str) for p in data)
    assert all(p.endswith(".parquet") for p in data)
    for p in data:
        assert os.path.exists(p)
