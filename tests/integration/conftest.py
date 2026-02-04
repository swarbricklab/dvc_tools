"""Integration test specific fixtures.

These fixtures are available to all tests in tests/integration/.
They create real git/DVC repositories for end-to-end testing.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Skip Conditions
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
# Git Repository Fixtures
# =============================================================================

@pytest.fixture
def git_repo(tmp_path, monkeypatch):
    """Create a real git repository.
    
    Initializes git with proper config for testing.
    """
    monkeypatch.chdir(tmp_path)
    
    subprocess.run(['git', 'init'], check=True, capture_output=True)
    subprocess.run(
        ['git', 'config', 'user.email', 'test@example.com'],
        check=True, capture_output=True
    )
    subprocess.run(
        ['git', 'config', 'user.name', 'Test User'],
        check=True, capture_output=True
    )
    
    return tmp_path


@pytest.fixture
def git_repo_with_commits(git_repo):
    """Create git repo with some commits."""
    # Initial commit
    readme = git_repo / 'README.md'
    readme.write_text('# Test Project\n')
    subprocess.run(['git', 'add', 'README.md'], check=True, capture_output=True)
    subprocess.run(
        ['git', 'commit', '-m', 'Initial commit'],
        check=True, capture_output=True
    )
    
    return git_repo


# =============================================================================
# DVC Repository Fixtures
# =============================================================================

@pytest.fixture
def dvc_repo(git_repo_with_commits):
    """Create a real DVC repository.
    
    Requires DVC to be installed - use @requires_dvc marker.
    """
    subprocess.run(['dvc', 'init'], check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], check=True, capture_output=True)
    subprocess.run(
        ['git', 'commit', '-m', 'Initialize DVC'],
        check=True, capture_output=True
    )
    
    return git_repo_with_commits


@pytest.fixture
def dvc_repo_with_files(dvc_repo):
    """Create DVC repo with tracked files.
    
    Adds a sample data file tracked by DVC.
    """
    # Create a data file
    data_file = dvc_repo / 'data.csv'
    data_file.write_text('id,value\n1,100\n2,200\n3,300\n')
    
    # Add to DVC
    subprocess.run(['dvc', 'add', 'data.csv'], check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], check=True, capture_output=True)
    subprocess.run(
        ['git', 'commit', '-m', 'Add data.csv'],
        check=True, capture_output=True
    )
    
    return dvc_repo


@pytest.fixture
def dvc_repo_with_cache(dvc_repo_with_files, tmp_path):
    """Create DVC repo with external cache configured.
    
    Sets up an external cache directory for testing cache operations.
    """
    cache_dir = tmp_path / 'external_cache'
    cache_dir.mkdir()
    
    # Create cache structure
    files_md5 = cache_dir / 'files' / 'md5'
    for i in range(256):
        (files_md5 / f'{i:02x}').mkdir(parents=True)
    
    # Configure DVC to use external cache
    subprocess.run(
        ['dvc', 'cache', 'dir', '--local', str(cache_dir)],
        check=True, capture_output=True
    )
    
    return {
        'repo': dvc_repo_with_files,
        'cache': cache_dir,
    }


# =============================================================================
# Remote Repository Fixtures
# =============================================================================

@pytest.fixture
def dvc_repo_with_remote(dvc_repo_with_cache, tmp_path):
    """Create DVC repo with local remote configured.
    
    Sets up a local directory as a DVC remote for testing push/pull.
    """
    remote_dir = tmp_path / 'remote_storage'
    remote_dir.mkdir()
    
    # Create remote structure
    files_md5 = remote_dir / 'files' / 'md5'
    for i in range(256):
        (files_md5 / f'{i:02x}').mkdir(parents=True)
    
    # Configure DVC remote
    subprocess.run(
        ['dvc', 'remote', 'add', '-d', 'local', str(remote_dir)],
        check=True, capture_output=True, cwd=dvc_repo_with_cache['repo']
    )
    
    return {
        **dvc_repo_with_cache,
        'remote': remote_dir,
    }


# =============================================================================
# Test Repository Clones (Network Required)
# =============================================================================

@pytest.fixture
def dt_test_fixtures_clone(tmp_path):
    """Clone dt-test-fixtures repository for integration tests.
    
    Use sparingly - prefer local fixtures for faster tests.
    Requires network access.
    """
    repo_path = tmp_path / 'dt-test-fixtures'
    subprocess.run([
        'git', 'clone', '--depth', '1',
        'https://github.com/swarbricklab/dt-test-fixtures',
        str(repo_path)
    ], check=True, capture_output=True)
    
    return repo_path


@pytest.fixture
def dt_test_registry_clone(tmp_path):
    """Clone dt-test-registry repository for import tests.
    
    Use sparingly - prefer local fixtures for faster tests.
    Requires network access.
    """
    repo_path = tmp_path / 'dt-test-registry'
    subprocess.run([
        'git', 'clone', '--depth', '1',
        'https://github.com/swarbricklab/dt-test-registry',
        str(repo_path)
    ], check=True, capture_output=True)
    
    return repo_path


# =============================================================================
# CLI Runner Fixture
# =============================================================================

@pytest.fixture
def run_dt(dvc_repo):
    """Provide a function to run dt commands in test repo.
    
    Returns a wrapper that runs dt commands and captures output.
    """
    def _run_dt(*args, check=True):
        result = subprocess.run(
            ['dt', *args],
            capture_output=True,
            text=True,
            cwd=dvc_repo,
        )
        if check and result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode,
                ['dt', *args],
                result.stdout,
                result.stderr,
            )
        return result
    
    return _run_dt
