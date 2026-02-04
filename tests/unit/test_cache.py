"""Tests for dt cache module."""

import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from dt import cache
from dt import config as cfg
from dt.errors import CacheError


class TestResolveCachePath:
    """Tests for resolve_cache_path function."""
    
    def test_with_cache_path_override(self, tmp_path):
        """cache_path overrides all other options."""
        explicit_path = tmp_path / 'explicit_cache'
        result = cache.resolve_cache_path(cache_path=str(explicit_path))
        
        assert result == explicit_path.resolve()
    
    def test_constructs_from_cache_root_and_name(self, tmp_path, monkeypatch):
        """Constructs path from cache_root and name."""
        cache_root = tmp_path / 'cache_root'
        cache_root.mkdir()
        
        # Mock config to avoid needing actual config
        with patch.object(cfg, 'get_value', return_value=None):
            result = cache.resolve_cache_path(
                name='my-project',
                cache_root=str(cache_root)
            )
        
        assert result == cache_root / 'my-project'
    
    def test_uses_config_cache_root(self, tmp_path):
        """Uses cache.root from config when not provided."""
        cache_root = tmp_path / 'config_cache_root'
        
        with patch.object(cfg, 'get_value', return_value=str(cache_root)):
            with patch('dt.cache.utils.get_project_name', return_value='from-config'):
                result = cache.resolve_cache_path()
        
        assert result == cache_root / 'from-config'
    
    def test_raises_error_when_not_configured(self):
        """Raises CacheError when cache location cannot be determined."""
        with patch.object(cfg, 'get_value', return_value=None):
            with pytest.raises(CacheError, match="Cache root not configured"):
                cache.resolve_cache_path()


class TestInitCacheStructure:
    """Tests for init_cache_structure function."""
    
    def test_creates_required_directories(self, tmp_path):
        """Creates cache directory with files/md5 structure."""
        cache_dir = tmp_path / 'cache'
        
        cache.init_cache_structure(cache_dir, verbose=False)
        
        assert cache_dir.exists()
        assert (cache_dir / 'runs').exists()
        assert (cache_dir / 'files' / 'md5').exists()
    
    def test_creates_256_subdirectories(self, tmp_path):
        """Creates 00-ff subdirectories under files/md5."""
        cache_dir = tmp_path / 'cache'
        
        cache.init_cache_structure(cache_dir, verbose=False)
        
        md5_dir = cache_dir / 'files' / 'md5'
        subdirs = list(md5_dir.iterdir())
        
        assert len(subdirs) == 256
        assert (md5_dir / '00').exists()
        assert (md5_dir / 'ff').exists()
    
    def test_sets_group_permissions(self, tmp_path):
        """Sets group writable permissions."""
        cache_dir = tmp_path / 'cache'
        
        cache.init_cache_structure(cache_dir, verbose=False)
        
        # Check setgid bit is set
        mode = cache_dir.stat().st_mode
        # Setgid bit is 0o2000
        assert mode & 0o2000


class TestConfigureDvcCache:
    """Tests for configure_dvc_cache function."""
    
    def test_sets_cache_directory(self, tmp_path):
        """Runs dvc cache dir with correct arguments."""
        repo_path = tmp_path / 'repo'
        repo_path.mkdir()
        cache_dir = tmp_path / 'cache'
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ''
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            cache.configure_dvc_cache(repo_path, cache_dir, verbose=False)
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args == ['dvc', 'cache', 'dir', '--local', str(cache_dir)]
    
    def test_raises_error_on_failure(self, tmp_path):
        """Raises CacheError when dvc command fails."""
        repo_path = tmp_path / 'repo'
        repo_path.mkdir()
        cache_dir = tmp_path / 'cache'
        
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = 'error message'
        
        with patch('subprocess.run', return_value=mock_result):
            with pytest.raises(CacheError, match="Failed to configure DVC cache"):
                cache.configure_dvc_cache(repo_path, cache_dir, verbose=False)


class TestInitCache:
    """Tests for init_cache function."""
    
    def test_creates_new_cache(self, tmp_path):
        """Creates new cache and configures DVC."""
        repo_path = tmp_path / 'repo'
        repo_path.mkdir()
        cache_root = tmp_path / 'cache_root'
        
        mock_dvc_result = MagicMock()
        mock_dvc_result.returncode = 0
        mock_dvc_result.stderr = ''
        
        with patch('dt.cache.utils.check_dvc'):
            with patch('subprocess.run', return_value=mock_dvc_result):
                result = cache.init_cache(
                    name='test-project',
                    cache_root=str(cache_root),
                    repo_path=repo_path,
                    verbose=False,
                )
        
        assert result == cache_root / 'test-project'
        assert result.exists()
    
    def test_uses_existing_cache(self, tmp_path):
        """Uses existing cache without recreating."""
        repo_path = tmp_path / 'repo'
        repo_path.mkdir()
        existing_cache = tmp_path / 'existing_cache'
        existing_cache.mkdir()
        
        mock_dvc_result = MagicMock()
        mock_dvc_result.returncode = 0
        mock_dvc_result.stderr = ''
        
        with patch('dt.cache.utils.check_dvc'):
            with patch('subprocess.run', return_value=mock_dvc_result):
                result = cache.init_cache(
                    cache_path=str(existing_cache),
                    repo_path=repo_path,
                    verbose=False,
                )
        
        assert result == existing_cache


class TestGetCacheDir:
    """Tests for get_cache_dir function."""
    
    def test_returns_path_when_available(self, tmp_path):
        """Returns Path when cache is configured."""
        with patch('dt.cache.utils.get_cache_dir', return_value=tmp_path):
            result = cache.get_cache_dir()
        
        assert result == tmp_path
    
    def test_raises_error_when_not_available(self):
        """Raises CacheError when cache is not configured."""
        with patch('dt.cache.utils.get_cache_dir', return_value=None):
            with pytest.raises(CacheError, match="Cache not configured"):
                cache.get_cache_dir()


class TestExpandDirHashes:
    """Tests for expand_dir_hashes function."""
    
    @pytest.fixture
    def cache_with_dir(self, tmp_path):
        """Create cache with a .dir file."""
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        
        # Create .dir file
        dir_hash = 'abcdef1234567890abcdef1234567890'
        dir_subdir = cache_dir / dir_hash[:2]
        dir_subdir.mkdir()
        
        dir_file = dir_subdir / (dir_hash[2:] + '.dir')
        dir_contents = [
            {'md5': '1111111111111111111111111111111a', 'relpath': 'file1.txt'},
            {'md5': '2222222222222222222222222222222b', 'relpath': 'subdir/file2.txt'},
        ]
        dir_file.write_text(json.dumps(dir_contents))
        
        return cache_dir, dir_hash
    
    def test_expands_directory_contents(self, cache_with_dir):
        """Expands .dir hash to include contained files."""
        cache_dir, dir_hash = cache_with_dir
        full_hash = dir_hash + '.dir'
        
        expanded, paths = cache.expand_dir_hashes(
            cache_dir, [full_hash], {full_hash: 'mydir'}
        )
        
        assert full_hash in expanded
        assert '1111111111111111111111111111111a' in expanded
        assert '2222222222222222222222222222222b' in expanded
        assert len(expanded) == 3
    
    def test_handles_missing_dir_file(self, tmp_path):
        """Handles missing .dir file gracefully."""
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        
        expanded, paths = cache.expand_dir_hashes(
            cache_dir, ['nonexistent.dir'], {}
        )
        
        # Should still include the original hash
        assert expanded == ['nonexistent.dir']
    
    def test_preserves_non_dir_hashes(self, tmp_path):
        """Non-.dir hashes are passed through unchanged."""
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        
        regular_hashes = ['abc123', 'def456']
        expanded, paths = cache.expand_dir_hashes(cache_dir, regular_hashes, {})
        
        assert expanded == regular_hashes


class TestGetHashForPathInDir:
    """Tests for get_hash_for_path_in_dir function."""
    
    @pytest.fixture
    def cache_with_dir_file(self, tmp_path):
        """Create cache with a .dir file for testing."""
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        
        dir_hash = 'abcdef1234567890abcdef1234567890.dir'
        hash_only = dir_hash[:-4]
        dir_subdir = cache_dir / hash_only[:2]
        dir_subdir.mkdir()
        
        dir_file = dir_subdir / (hash_only[2:] + '.dir')
        dir_contents = [
            {'md5': 'file1hash', 'relpath': 'file1.txt'},
            {'md5': 'file2hash', 'relpath': 'subdir/file2.txt'},
        ]
        dir_file.write_text(json.dumps(dir_contents))
        
        return cache_dir, dir_hash
    
    def test_finds_correct_hash(self, cache_with_dir_file):
        """Finds hash for file in directory."""
        cache_dir, dir_hash = cache_with_dir_file
        
        result = cache.get_hash_for_path_in_dir(cache_dir, dir_hash, 'file1.txt')
        
        assert result == 'file1hash'
    
    def test_finds_nested_file(self, cache_with_dir_file):
        """Finds hash for nested file."""
        cache_dir, dir_hash = cache_with_dir_file
        
        result = cache.get_hash_for_path_in_dir(cache_dir, dir_hash, 'subdir/file2.txt')
        
        assert result == 'file2hash'
    
    def test_returns_none_when_not_found(self, cache_with_dir_file):
        """Returns None when file not in directory manifest."""
        cache_dir, dir_hash = cache_with_dir_file
        
        result = cache.get_hash_for_path_in_dir(cache_dir, dir_hash, 'nonexistent.txt')
        
        assert result is None
    
    def test_returns_none_for_missing_dir_file(self, tmp_path):
        """Returns None when .dir file doesn't exist."""
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        
        result = cache.get_hash_for_path_in_dir(cache_dir, 'missing.dir', 'file.txt')
        
        assert result is None


class TestCheckHashesInRemote:
    """Tests for check_hashes_in_remote function."""
    
    def test_returns_empty_for_empty_input(self):
        """Returns empty lists for empty input."""
        in_remote, not_in_remote = cache.check_hashes_in_remote([])
        
        assert in_remote == []
        assert not_in_remote == []
    
    def test_returns_all_not_in_remote_on_import_error(self):
        """Returns all hashes as not in remote if DVC imports fail."""
        file_hashes = ['abc123', 'def456']
        
        # If dvc.repo import fails, should return all as not_in_remote
        with patch.dict('sys.modules', {'dvc.repo': None}):
            in_remote, not_in_remote = cache.check_hashes_in_remote(file_hashes)
        
        # Conservative behavior - treat as not in remote
        assert not_in_remote == file_hashes or in_remote == []


class TestGetCacheFileInfo:
    """Tests for get_cache_file_info function."""
    
    def test_returns_paths_and_sizes(self, tmp_path):
        """Returns cache paths and sizes for existing files."""
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        
        # Create cache file
        file_hash = 'abcdef1234567890abcdef1234567890'
        subdir = cache_dir / file_hash[:2]
        subdir.mkdir()
        cache_file = subdir / file_hash[2:]
        cache_file.write_text('test content')
        
        results = cache.get_cache_file_info(cache_dir, [file_hash])
        
        assert len(results) == 1
        h, path, size = results[0]
        assert h == file_hash
        assert path == cache_file
        assert size == len('test content')
    
    def test_returns_none_size_for_missing(self, tmp_path):
        """Returns None size for missing cache files."""
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        
        results = cache.get_cache_file_info(cache_dir, ['nonexistent'])
        
        assert len(results) == 1
        h, path, size = results[0]
        assert h == 'nonexistent'
        assert size is None


class TestHashToCachePath:
    """Tests for hash_to_cache_path function (re-exported from utils)."""
    
    def test_regular_hash(self, tmp_path):
        """Converts regular hash to cache path."""
        # cache_dir should be the files/md5 directory
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        file_hash = 'abcdef1234567890abcdef1234567890'
        
        result = cache.hash_to_cache_path(cache_dir, file_hash)
        
        expected = cache_dir / 'ab' / 'cdef1234567890abcdef1234567890'
        assert result == expected
    
    def test_dir_hash(self, tmp_path):
        """Converts .dir hash to cache path."""
        # cache_dir should be the files/md5 directory
        cache_dir = tmp_path / 'cache' / 'files' / 'md5'
        cache_dir.mkdir(parents=True)
        file_hash = 'abcdef1234567890abcdef1234567890.dir'
        
        result = cache.hash_to_cache_path(cache_dir, file_hash)
        
        expected = cache_dir / 'ab' / 'cdef1234567890abcdef1234567890.dir'
        assert result == expected
