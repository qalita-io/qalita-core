"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
Shared pytest fixtures for qalita_core tests.
"""

import os
import pytest


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
