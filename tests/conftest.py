"""Shared pytest configuration and fixtures for all tests.

This conftest.py is auto-discovered by pytest and provides fixtures
available to both unit and integration tests.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Pytest Markers
# =============================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (deselect with '-m \"not integration\"')"
    )
    config.addinivalue_line(
        "markers", "requires_dvc: marks tests requiring DVC to be installed"
    )
    config.addinivalue_line(
        "markers", "requires_git: marks tests requiring git to be installed"
    )
    config.addinivalue_line(
        "markers", "requires_network: marks tests requiring network access"
    )
    config.addinivalue_line(
        "markers", "requires_qxub: marks tests requiring qxub (HPC environment only)"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )


# =============================================================================
# Skip Decorators
# =============================================================================

requires_dvc = pytest.mark.skipif(
    shutil.which('dvc') is None,
    reason="DVC not installed"
)

requires_git = pytest.mark.skipif(
    shutil.which('git') is None,
    reason="git not installed"
)

requires_network = pytest.mark.skipif(
    os.environ.get('DT_TEST_OFFLINE'),
    reason="Network tests disabled (DT_TEST_OFFLINE set)"
)

requires_qxub = pytest.mark.skipif(
    shutil.which('qxub') is None,
    reason="qxub not available (HPC environment only)"
)


# =============================================================================
# Shared Fixtures
# =============================================================================

@pytest.fixture
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def cli_runner():
    """Provide a Click CLI test runner."""
    from click.testing import CliRunner
    return CliRunner()


@pytest.fixture
def isolated_dir(tmp_path, monkeypatch):
    """Change to an isolated temporary directory."""
    monkeypatch.chdir(tmp_path)
    return tmp_path
