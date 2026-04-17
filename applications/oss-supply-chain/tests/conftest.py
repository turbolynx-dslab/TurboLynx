"""Pytest session fixtures for the OSS supply-chain app."""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from oss_supply_chain.loader import load_fixture


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def workspace(fixture_dir: Path) -> Path:
    tmp_root = Path(tempfile.mkdtemp(prefix="tlx-oss-ws-"))
    workspace_dir = tmp_root / "workspace"
    try:
        load_fixture(workspace_dir, fixture_dir)
        yield workspace_dir
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
