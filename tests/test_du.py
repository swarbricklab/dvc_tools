"""Unit tests for dt.du module.

Tests disk usage reporting for DVC-tracked files.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.du import (
    collect_tracked_files,
    get_dir_file_count,
    get_cached_size,
    get_cached_file_count,
    aggregate_by_depth,
    calculate_du,
)
from dt.errors import DuError


# =============================================================================
# collect_tracked_files tests
# =============================================================================

class TestCollectTrackedFiles:
    """Tests for the collect_tracked_files function."""

    def test_returns_entries_from_collect_tracked_entries(self):
        """Test that entries are returned from utils.collect_tracked_entries."""
        mock_entries = [
            {"path": "data.csv", "hash": "abc123", "size": 1000, "is_dir": False},
            {"path": "dataset/", "hash": "def456.dir", "size": 5000, "is_dir": True},
        ]
        
        with patch("dt.du.utils.collect_tracked_entries") as mock_collect:
            mock_collect.return_value = {"entries": mock_entries}
            
            result = collect_tracked_files()
            
            assert result == mock_entries

    def test_passes_targets_to_collect(self):
        """Test that targets are passed through."""
        with patch("dt.du.utils.collect_tracked_entries") as mock_collect:
            mock_collect.return_value = {"entries": []}
            
            collect_tracked_files(targets=["data.csv", "images/"])
            
            mock_collect.assert_called_once_with(targets=["data.csv", "images/"], push=False)

    def test_raises_du_error_on_dependency_error(self):
        """Test that DependencyError is wrapped in DuError."""
        with patch("dt.du.utils.collect_tracked_entries") as mock_collect:
            from dt import utils
            mock_collect.side_effect = utils.DependencyError("dvc not found")
            
            with pytest.raises(DuError, match="dvc not found"):
                collect_tracked_files()


# =============================================================================
# get_dir_file_count tests
# =============================================================================

class TestGetDirFileCount:
    """Tests for the get_dir_file_count function."""

    def test_returns_count_from_dir_manifest(self, tmp_path):
        """Test that file count is read from .dir manifest."""
        # Create a mock .dir file
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        dir_file = cache_dir / "ab" / "cdef123456.dir"
        dir_file.write_text(json.dumps([
            {"relpath": "file1.txt", "md5": "hash1"},
            {"relpath": "file2.txt", "md5": "hash2"},
            {"relpath": "file3.txt", "md5": "hash3"},
        ]))
        
        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)
        
        with patch("dt.du.hash_to_cache_path", return_value=dir_file):
            count = get_dir_file_count(mock_repo, "abcdef123456.dir")
            
            assert count == 3

    def test_returns_1_when_cache_path_not_exists(self, tmp_path):
        """Test returns 1 when cache file doesn't exist."""
        cache_dir = tmp_path / "files" / "md5"
        cache_dir.mkdir(parents=True)
        nonexistent = cache_dir / "ab" / "cdef123456.dir"
        
        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)
        
        with patch("dt.du.hash_to_cache_path", return_value=nonexistent):
            count = get_dir_file_count(mock_repo, "abcdef123456.dir")
            
            assert count == 1

    def test_returns_1_on_json_decode_error(self, tmp_path):
        """Test returns 1 when .dir file has invalid JSON."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        dir_file = cache_dir / "ab" / "cdef123456.dir"
        dir_file.write_text("not valid json")
        
        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)
        
        with patch("dt.du.hash_to_cache_path", return_value=dir_file):
            count = get_dir_file_count(mock_repo, "abcdef123456.dir")
            
            assert count == 1


# =============================================================================
# get_cached_size tests
# =============================================================================

class TestGetCachedSize:
    """Tests for the get_cached_size function."""

    def test_returns_0_when_cache_path_not_exists(self, tmp_path):
        """Test returns 0 when cache file doesn't exist."""
        cache_dir = tmp_path / "files" / "md5"
        cache_dir.mkdir(parents=True)
        nonexistent = cache_dir / "ab" / "cdef123456"
        
        file_info = {"hash": "abcdef123456", "is_dir": False}
        
        with patch("dt.du.hash_to_cache_path", return_value=nonexistent):
            size = get_cached_size(cache_dir, file_info)
            
            assert size == 0

    def test_returns_file_size_for_regular_files(self, tmp_path):
        """Test returns file size for regular (non-dir) files."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        cache_file = cache_dir / "ab" / "cdef123456"
        cache_file.write_bytes(b"x" * 1000)
        
        file_info = {"hash": "abcdef123456", "is_dir": False}
        
        with patch("dt.du.hash_to_cache_path", return_value=cache_file):
            size = get_cached_size(cache_dir, file_info)
            
            assert size == 1000

    def test_sums_sizes_for_directories(self, tmp_path):
        """Test that sizes are summed for directory files."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        (cache_dir / "11").mkdir(parents=True)
        (cache_dir / "22").mkdir(parents=True)
        
        # Create .dir manifest
        dir_file = cache_dir / "ab" / "cdef123456.dir"
        dir_file.write_text(json.dumps([
            {"relpath": "file1.txt", "md5": "11aa"},
            {"relpath": "file2.txt", "md5": "22bb"},
        ]))
        
        # Create cached files
        child1 = cache_dir / "11" / "aa"
        child1.write_bytes(b"x" * 100)
        child2 = cache_dir / "22" / "bb"
        child2.write_bytes(b"y" * 200)
        
        file_info = {"hash": "abcdef123456.dir", "is_dir": True}
        
        def mock_hash_to_cache(cache_dir, file_hash):
            clean = file_hash.replace(".dir", "")
            prefix = clean[:2]
            suffix = clean[2:]
            if file_hash.endswith(".dir"):
                suffix += ".dir"
            return cache_dir / prefix / suffix
        
        with patch("dt.du.hash_to_cache_path", side_effect=mock_hash_to_cache):
            size = get_cached_size(cache_dir, file_info)
            
            # Size of .dir file + child1 + child2
            dir_size = dir_file.stat().st_size
            assert size == dir_size + 100 + 200


# =============================================================================
# get_cached_file_count tests
# =============================================================================

class TestGetCachedFileCount:
    """Tests for the get_cached_file_count function."""

    def test_returns_0_when_cache_path_not_exists(self, tmp_path):
        """Test returns 0 when cache file doesn't exist."""
        cache_dir = tmp_path / "files" / "md5"
        cache_dir.mkdir(parents=True)
        nonexistent = cache_dir / "ab" / "cdef123456"
        
        file_info = {"hash": "abcdef123456", "is_dir": False}
        
        with patch("dt.du.hash_to_cache_path", return_value=nonexistent):
            count = get_cached_file_count(cache_dir, file_info)
            
            assert count == 0

    def test_returns_1_for_regular_files(self, tmp_path):
        """Test returns 1 for regular (non-dir) files."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        cache_file = cache_dir / "ab" / "cdef123456"
        cache_file.write_bytes(b"x" * 100)
        
        file_info = {"hash": "abcdef123456", "is_dir": False}
        
        with patch("dt.du.hash_to_cache_path", return_value=cache_file):
            count = get_cached_file_count(cache_dir, file_info)
            
            assert count == 1

    def test_counts_cached_files_in_directory(self, tmp_path):
        """Test that cached files are counted for directories."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        (cache_dir / "11").mkdir(parents=True)
        # 22 is NOT created - file not cached
        
        # Create .dir manifest
        dir_file = cache_dir / "ab" / "cdef123456.dir"
        dir_file.write_text(json.dumps([
            {"relpath": "file1.txt", "md5": "11aa"},
            {"relpath": "file2.txt", "md5": "22bb"},  # Not cached
        ]))
        
        # Create only first cached file
        child1 = cache_dir / "11" / "aa"
        child1.write_bytes(b"x" * 100)
        
        file_info = {"hash": "abcdef123456.dir", "is_dir": True}
        
        def mock_hash_to_cache(cache_dir, file_hash):
            clean = file_hash.replace(".dir", "")
            prefix = clean[:2]
            suffix = clean[2:]
            if file_hash.endswith(".dir"):
                suffix += ".dir"
            return cache_dir / prefix / suffix
        
        with patch("dt.du.hash_to_cache_path", side_effect=mock_hash_to_cache):
            count = get_cached_file_count(cache_dir, file_info)
            
            # Only 1 file is cached
            assert count == 1


# =============================================================================
# aggregate_by_depth tests
# =============================================================================

class TestAggregateByDepth:
    """Tests for the aggregate_by_depth function."""

    def test_returns_files_unchanged_when_depth_is_none(self):
        """Test that files are returned unchanged when max_depth is None."""
        files = [
            {"path": "data.csv", "size": 100},
            {"path": "dir/file.txt", "size": 200},
        ]
        
        result = aggregate_by_depth(files, max_depth=None)
        
        assert result == files

    def test_aggregates_files_beyond_depth(self):
        """Test that files beyond depth are aggregated to parent."""
        files = [
            {"path": "a/b/file1.txt", "size": 100, "cached_size": 50, "nfiles": 1, "cached_nfiles": 1},
            {"path": "a/b/file2.txt", "size": 200, "cached_size": 100, "nfiles": 1, "cached_nfiles": 1},
        ]
        
        result = aggregate_by_depth(files, max_depth=1)
        
        # Both files should be aggregated under "a/b"
        assert len(result) == 1
        assert result[0]["path"] == "a/b"
        assert result[0]["size"] == 300
        assert result[0]["cached_size"] == 150

    def test_keeps_files_within_depth(self):
        """Test that files within depth are kept as-is."""
        files = [
            {"path": "file1.txt", "size": 100, "cached_size": 50, "nfiles": 1, "cached_nfiles": 1},
            {"path": "dir/file2.txt", "size": 200, "cached_size": 100, "nfiles": 1, "cached_nfiles": 1},
        ]
        
        result = aggregate_by_depth(files, max_depth=2)
        
        # All files are within depth, should be unchanged
        assert len(result) == 2

    def test_aggregates_nfiles_count(self):
        """Test that nfiles counts are aggregated."""
        files = [
            {"path": "a/b/c/file1.txt", "size": 100, "cached_size": 50, "nfiles": 3, "cached_nfiles": 2},
            {"path": "a/b/c/file2.txt", "size": 200, "cached_size": 100, "nfiles": 5, "cached_nfiles": 3},
        ]
        
        result = aggregate_by_depth(files, max_depth=1)
        
        assert result[0]["nfiles"] == 8
        assert result[0]["cached_nfiles"] == 5


# =============================================================================
# calculate_du tests
# =============================================================================

class TestCalculateDu:
    """Tests for the calculate_du function."""

    def test_checks_dvc_dependency(self):
        """Test that DVC dependency is checked."""
        with patch("dt.du.utils.check_dvc") as mock_check:
            from dt import utils
            mock_check.side_effect = utils.DependencyError("dvc not found")
            
            with pytest.raises(DuError, match="dvc not found"):
                calculate_du()

    def test_returns_empty_list_when_no_files(self):
        """Test returns empty list when no tracked files."""
        with patch("dt.du.utils.check_dvc"):
            with patch("dt.du.collect_tracked_files", return_value=[]):
                result = calculate_du()
                
                assert result == []

    def test_returns_size_path_tuples(self):
        """Test that result is list of (size, path) tuples."""
        files = [
            {"path": "data.csv", "hash": "abc123", "size": 1000, "is_dir": False},
        ]
        
        with patch("dt.du.utils.check_dvc"):
            with patch("dt.du.collect_tracked_files", return_value=files):
                with patch("dt.du.get_cache_dir", return_value=None):
                    result = calculate_du(cached=False)
                    
                    assert len(result) == 1
                    assert result[0] == (1000, "data.csv")

    def test_results_sorted_by_size_ascending(self):
        """Test that results are sorted by size ascending."""
        files = [
            {"path": "large.csv", "hash": "abc", "size": 5000, "is_dir": False},
            {"path": "small.csv", "hash": "def", "size": 100, "is_dir": False},
            {"path": "medium.csv", "hash": "ghi", "size": 1000, "is_dir": False},
        ]
        
        with patch("dt.du.utils.check_dvc"):
            with patch("dt.du.collect_tracked_files", return_value=files):
                with patch("dt.du.get_cache_dir", return_value=None):
                    result = calculate_du(cached=False)
                    
                    sizes = [r[0] for r in result]
                    assert sizes == [100, 1000, 5000]

    def test_count_inodes_returns_file_counts(self):
        """Test that count_inodes returns file counts instead of sizes."""
        files = [
            {"path": "data.csv", "hash": "abc123", "size": 1000, "nfiles": 5, "is_dir": True},
        ]
        
        with patch("dt.du.utils.check_dvc"):
            with patch("dt.du.collect_tracked_files", return_value=files):
                with patch("dt.du.get_cache_dir", return_value=None):
                    result = calculate_du(cached=False, count_inodes=True)
                    
                    assert result[0] == (5, "data.csv")
