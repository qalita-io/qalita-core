"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Tests for qalita_core.pack module
"""

from qalita_core.pack import Pack, ConfigLoader, PlatformAsset, _sanitize_for_json
from qalita_core.data_source_opener import cleanup_parquet_files
import pytest
import os
import json
import math
import datetime as dt
from decimal import Decimal


@pytest.fixture(scope="session")
def config_paths():
    base_path = os.path.dirname(__file__)
    configs = {
        "pack_conf": os.path.join(base_path, "data", "pack_conf.json"),
        "source_conf": os.path.join(base_path, "data", "source_conf.json"),
        "target_conf": os.path.join(base_path, "data", "target_conf.json"),
        "agent_file": os.path.join(base_path, "data", ".worker"),
    }
    return configs


@pytest.fixture(scope="session")
def pack(config_paths):
    pack = Pack(configs=config_paths)
    return pack


class TestPackInstantiation:
    """Tests for Pack class instantiation."""

    def test_pack_instantiation(self, pack):
        assert isinstance(pack, Pack), "Failed to instantiate Pack"

    def test_pack_has_metrics(self, pack):
        assert hasattr(pack, "metrics")
        assert isinstance(pack.metrics, PlatformAsset)

    def test_pack_has_recommendations(self, pack):
        assert hasattr(pack, "recommendations")
        assert isinstance(pack.recommendations, PlatformAsset)

    def test_pack_has_schemas(self, pack):
        assert hasattr(pack, "schemas")
        assert isinstance(pack.schemas, PlatformAsset)


class TestPackConfigLoading:
    """Tests for Pack configuration loading."""

    def test_pack_load_pack_config(self, pack):
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

    def test_pack_load_source_config(self, pack):
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

    def test_pack_load_target_config(self, pack):
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

    def test_pack_load_agent_config(self, pack):
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


class TestPackDataLoading:
    """Tests for Pack data loading."""

    def test_pack_load_data_target(self, pack, tmp_path):
        # Ensure output dir is set via env by overriding pack config at runtime
        pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
        data = pack.load_data("target")
        assert isinstance(data, list) and all(isinstance(p, str) for p in data)
        assert all(p.endswith(".parquet") for p in data)
        # Files should exist
        for p in data:
            assert os.path.exists(p)

    def test_pack_load_data_source(self, pack, tmp_path):
        pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
        data = pack.load_data("source")
        assert isinstance(data, list) and all(isinstance(p, str) for p in data)
        assert all(p.endswith(".parquet") for p in data)
        for p in data:
            assert os.path.exists(p)


class TestConfigLoader:
    """Tests for ConfigLoader class."""

    def test_load_valid_config(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_data = {"key": "value", "number": 42}
        config_file.write_text(json.dumps(config_data))
        
        result = ConfigLoader.load_config(str(config_file))
        assert result == config_data

    def test_load_missing_config(self):
        result = ConfigLoader.load_config("/nonexistent/path/config.json")
        assert result == {}

    def test_load_empty_config(self, tmp_path):
        config_file = tmp_path / "empty.json"
        config_file.write_text("{}")
        
        result = ConfigLoader.load_config(str(config_file))
        assert result == {}


class TestSanitizeForJson:
    """Tests for _sanitize_for_json function."""

    def test_none_value(self):
        assert _sanitize_for_json(None) is None

    def test_bool_value(self):
        assert _sanitize_for_json(True) is True
        assert _sanitize_for_json(False) is False

    def test_int_value(self):
        assert _sanitize_for_json(42) == 42
        assert _sanitize_for_json(-10) == -10

    def test_str_value(self):
        assert _sanitize_for_json("hello") == "hello"

    def test_finite_float(self):
        assert _sanitize_for_json(3.14) == 3.14

    def test_nan_float(self):
        result = _sanitize_for_json(float("nan"))
        assert result is None

    def test_inf_float(self):
        assert _sanitize_for_json(float("inf")) is None
        assert _sanitize_for_json(float("-inf")) is None

    def test_datetime(self):
        dt_val = dt.datetime(2024, 1, 15, 10, 30, 0)
        result = _sanitize_for_json(dt_val)
        assert result == "2024-01-15T10:30:00"

    def test_date(self):
        d_val = dt.date(2024, 1, 15)
        result = _sanitize_for_json(d_val)
        assert result == "2024-01-15"

    def test_time(self):
        t_val = dt.time(10, 30, 0)
        result = _sanitize_for_json(t_val)
        assert result == "10:30:00"

    def test_decimal(self):
        dec_val = Decimal("3.14159")
        result = _sanitize_for_json(dec_val)
        assert isinstance(result, float)
        assert abs(result - 3.14159) < 0.0001

    def test_dict_with_string_keys(self):
        data = {"key1": "value1", "key2": 42}
        result = _sanitize_for_json(data)
        assert result == data

    def test_dict_with_non_string_keys(self):
        data = {1: "value1", 2: "value2"}
        result = _sanitize_for_json(data)
        assert "1" in result
        assert "2" in result

    def test_nested_dict(self):
        data = {"outer": {"inner": 42}}
        result = _sanitize_for_json(data)
        assert result == data

    def test_list(self):
        data = [1, 2, 3]
        result = _sanitize_for_json(data)
        assert result == data

    def test_tuple(self):
        data = (1, 2, 3)
        result = _sanitize_for_json(data)
        assert result == [1, 2, 3]  # Converted to list

    def test_set(self):
        data = {1, 2, 3}
        result = _sanitize_for_json(data)
        assert sorted(result) == [1, 2, 3]  # Converted to list

    def test_nested_structures(self):
        data = {
            "list": [1, 2, {"nested": "value"}],
            "number": 42,
        }
        result = _sanitize_for_json(data)
        assert result["list"][2]["nested"] == "value"

    def test_complex_object_fallback(self):
        class CustomClass:
            def __str__(self):
                return "custom_object"
        
        result = _sanitize_for_json(CustomClass())
        assert result == "custom_object"


class TestPlatformAsset:
    """Tests for PlatformAsset class."""

    def test_init_metrics(self):
        asset = PlatformAsset("metrics")
        assert asset.type == "metrics"
        assert asset.data == []

    def test_init_recommendations(self):
        asset = PlatformAsset("recommendations")
        assert asset.type == "recommendations"
        assert asset.data == []

    def test_init_schemas(self):
        asset = PlatformAsset("schemas")
        assert asset.type == "schemas"
        assert asset.data == []

    def test_data_manipulation(self):
        asset = PlatformAsset("metrics")
        asset.data.append({"key": "value", "score": 0.95})
        assert len(asset.data) == 1
        assert asset.data[0]["key"] == "value"

    def test_save_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        asset = PlatformAsset("metrics")
        asset.data = [{"key": "test", "value": 42}]
        asset.save()
        
        output_file = tmp_path / "metrics.json"
        assert output_file.exists()
        
        with open(output_file) as f:
            saved_data = json.load(f)
        assert saved_data == [{"key": "test", "value": 42}]

    def test_save_sanitizes_data(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        asset = PlatformAsset("metrics")
        # Add data with non-JSON-serializable values
        asset.data = [{"value": float("nan")}, {"date": dt.date(2024, 1, 15)}]
        asset.save()
        
        output_file = tmp_path / "metrics.json"
        assert output_file.exists()
        
        with open(output_file) as f:
            saved_data = json.load(f)
        # NaN should be sanitized to None
        assert saved_data[0]["value"] is None
        # Date should be converted to ISO string
        assert saved_data[1]["date"] == "2024-01-15"

    def test_save_empty_data(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        asset = PlatformAsset("schemas")
        asset.save()
        
        output_file = tmp_path / "schemas.json"
        assert output_file.exists()
        
        with open(output_file) as f:
            saved_data = json.load(f)
        assert saved_data == []


class TestPackWithMissingConfigs:
    """Tests for Pack behavior with missing configurations."""

    def test_pack_with_missing_source_type(self, tmp_path):
        # Create minimal configs without 'type' in source
        pack_conf = tmp_path / "pack_conf.json"
        source_conf = tmp_path / "source_conf.json"
        target_conf = tmp_path / "target_conf.json"
        agent_file = tmp_path / ".worker"
        
        pack_conf.write_text('{"job": {}}')
        source_conf.write_text('{"config": {}}')  # Missing 'type'
        target_conf.write_text('{"type": "file", "config": {"path": "test.csv"}}')
        
        # Create a valid agent file
        import base64
        agent_data = json.dumps({"user": {}, "context": {"local": {}, "remote": {}}, "registries": []})
        agent_file.write_text(base64.b64encode(agent_data.encode()).decode())
        
        # Should not crash, just log error
        pack = Pack(configs={
            "pack_conf": str(pack_conf),
            "source_conf": str(source_conf),
            "target_conf": str(target_conf),
            "agent_file": str(agent_file),
        })
        assert pack.source_config == {"config": {}}

    def test_pack_with_empty_source_config(self, tmp_path):
        pack_conf = tmp_path / "pack_conf.json"
        source_conf = tmp_path / "source_conf.json"
        target_conf = tmp_path / "target_conf.json"
        agent_file = tmp_path / ".worker"
        
        pack_conf.write_text('{"job": {}}')
        source_conf.write_text('{}')  # Empty
        target_conf.write_text('{"type": "file", "config": {"path": "test.csv"}}')
        
        import base64
        agent_data = json.dumps({"user": {}, "context": {"local": {}, "remote": {}}, "registries": []})
        agent_file.write_text(base64.b64encode(agent_data.encode()).decode())
        
        pack = Pack(configs={
            "pack_conf": str(pack_conf),
            "source_conf": str(source_conf),
            "target_conf": str(target_conf),
            "agent_file": str(agent_file),
        })
        assert pack.source_config == {}


class TestCleanupParquetFiles:
    """Tests for cleanup_parquet_files function."""

    def test_cleanup_removes_files(self, tmp_path):
        # Create some fake parquet files
        files = []
        for i in range(3):
            f = tmp_path / f"test_part_{i}.parquet"
            f.write_text("fake parquet content")
            files.append(str(f))
        
        # Verify files exist
        for f in files:
            assert os.path.exists(f)
        
        # Cleanup
        removed = cleanup_parquet_files(files)
        
        # Verify files are removed
        assert removed == 3
        for f in files:
            assert not os.path.exists(f)

    def test_cleanup_handles_nonexistent_files(self, tmp_path):
        files = [
            str(tmp_path / "nonexistent1.parquet"),
            str(tmp_path / "nonexistent2.parquet"),
        ]
        
        # Should not raise an error
        removed = cleanup_parquet_files(files)
        assert removed == 0

    def test_cleanup_handles_empty_list(self):
        removed = cleanup_parquet_files([])
        assert removed == 0

    def test_cleanup_handles_none(self):
        removed = cleanup_parquet_files(None)
        assert removed == 0

    def test_cleanup_partial_success(self, tmp_path):
        # Create one real file
        real_file = tmp_path / "real.parquet"
        real_file.write_text("content")
        
        files = [
            str(real_file),
            str(tmp_path / "nonexistent.parquet"),
        ]
        
        removed = cleanup_parquet_files(files)
        assert removed == 1
        assert not os.path.exists(real_file)


class TestPackCleanup:
    """Tests for Pack cleanup method and context manager."""

    def test_cleanup_method(self, config_paths, tmp_path):
        pack = Pack(configs=config_paths)
        pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
        
        # Load data
        pack.load_data("source")
        
        # Verify parquet files exist
        assert pack.paths_source is not None
        for p in pack.paths_source:
            assert os.path.exists(p)
        
        # Cleanup
        removed = pack.cleanup()
        
        # Verify files are removed
        assert removed > 0
        for p in pack.paths_source:
            assert not os.path.exists(p)

    def test_cleanup_source_and_target(self, config_paths, tmp_path):
        pack = Pack(configs=config_paths)
        pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
        
        # Load both source and target
        pack.load_data("source")
        pack.load_data("target")
        
        # Verify parquet files exist
        all_paths = pack.paths_source + pack.paths_target
        for p in all_paths:
            assert os.path.exists(p)
        
        # Cleanup
        removed = pack.cleanup()
        
        # Verify all files are removed
        assert removed == len(all_paths)
        for p in all_paths:
            assert not os.path.exists(p)

    def test_cleanup_without_loading_data(self, config_paths):
        pack = Pack(configs=config_paths)
        
        # Should not raise an error
        removed = pack.cleanup()
        assert removed == 0

    def test_context_manager_cleanup(self, config_paths, tmp_path):
        paths_to_check = None
        
        with Pack(configs=config_paths) as pack:
            pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
            pack.load_data("source")
            paths_to_check = list(pack.paths_source)
            
            # Verify files exist inside the context
            for p in paths_to_check:
                assert os.path.exists(p)
        
        # After exiting context, files should be cleaned up
        for p in paths_to_check:
            assert not os.path.exists(p)

    def test_context_manager_cleanup_on_exception(self, config_paths, tmp_path):
        paths_to_check = None
        
        try:
            with Pack(configs=config_paths) as pack:
                pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
                pack.load_data("source")
                paths_to_check = list(pack.paths_source)
                
                # Verify files exist inside the context
                for p in paths_to_check:
                    assert os.path.exists(p)
                
                # Raise an exception
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # After exception, files should still be cleaned up
        for p in paths_to_check:
            assert not os.path.exists(p)

    def test_double_cleanup_safe(self, config_paths, tmp_path):
        pack = Pack(configs=config_paths)
        pack.pack_config["job"]["parquet_output_dir"] = str(tmp_path)
        pack.load_data("source")
        
        # First cleanup
        removed1 = pack.cleanup()
        assert removed1 > 0
        
        # Second cleanup should be safe (files already removed)
        removed2 = pack.cleanup()
        assert removed2 == 0
