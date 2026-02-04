"""Unit test specific fixtures.

These fixtures are available to all tests in tests/unit/.
They focus on mocked dependencies for fast, isolated testing.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock


# =============================================================================
# Mock DVC Repository Fixtures
# =============================================================================

@pytest.fixture
def mock_dvc_repo(tmp_path, monkeypatch):
    """Create a mock DVC repository structure (no real DVC needed).
    
    Creates .dvc and .git directories to simulate a DVC project.
    """
    (tmp_path / '.dvc').mkdir()
    (tmp_path / '.git').mkdir()
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def sample_dvc_files(mock_dvc_repo):
    """Add sample .dvc files to mock repo.
    
    Includes: regular file, directory, and import patterns.
    """
    # Regular file
    (mock_dvc_repo / 'data.csv.dvc').write_text(
        'outs:\n'
        '  - md5: abcdef1234567890abcdef1234567890\n'
        '    size: 1024\n'
        '    path: data.csv\n'
    )
    
    # Directory
    (mock_dvc_repo / 'dir.dvc').write_text(
        'outs:\n'
        '  - md5: 1234567890abcdef1234567890abcdef.dir\n'
        '    size: 2048\n'
        '    nfiles: 5\n'
        '    path: dir\n'
    )
    
    # Import (with deps.repo)
    (mock_dvc_repo / 'imported.dvc').write_text(
        'deps:\n'
        '  - path: data/file.csv\n'
        '    repo:\n'
        '      url: https://github.com/swarbricklab/dt-test-registry\n'
        '      rev_lock: abc123\n'
        'outs:\n'
        '  - md5: fedcba0987654321fedcba0987654321\n'
        '    size: 512\n'
        '    path: imported.csv\n'
    )
    return mock_dvc_repo


# =============================================================================
# Cache Directory Fixtures
# =============================================================================

@pytest.fixture
def cache_structure(tmp_path):
    """Create a DVC cache directory structure.
    
    Creates files/md5/XX directories (256 subdirs).
    Returns the cache root path.
    """
    cache_root = tmp_path / 'cache'
    files_md5 = cache_root / 'files' / 'md5'
    
    # Create the 256 hash prefix directories
    for i in range(256):
        (files_md5 / f'{i:02x}').mkdir(parents=True)
    
    return cache_root


@pytest.fixture
def cache_with_files(cache_structure):
    """Create cache with sample files.
    
    Returns dict with cache_root and hash -> path mapping.
    """
    files_md5 = cache_structure / 'files' / 'md5'
    
    # Add some sample cache files
    files = {
        'abcdef1234567890abcdef1234567890': b'sample content 1',
        'fedcba0987654321fedcba0987654321': b'sample content 2',
    }
    
    for file_hash, content in files.items():
        prefix = file_hash[:2]
        suffix = file_hash[2:]
        cache_file = files_md5 / prefix / suffix
        cache_file.write_bytes(content)
    
    return {
        'cache_root': cache_structure,
        'files': files,
    }


# =============================================================================
# Configuration Fixtures
# =============================================================================

@pytest.fixture
def isolated_config(tmp_path, monkeypatch):
    """Create isolated config environment.
    
    Sets up XDG and DT environment variables to use temp directories.
    """
    config_home = tmp_path / 'config'
    config_home.mkdir()
    
    project_dir = tmp_path / 'project'
    project_dir.mkdir()
    (project_dir / '.git').mkdir()
    
    monkeypatch.setenv('XDG_CONFIG_HOME', str(config_home))
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.chdir(project_dir)
    
    return {
        'config_home': config_home,
        'project_dir': project_dir,
        'tmp_path': tmp_path,
    }
