"""Unit tests for dt.find module.

Tests find_by_hash and format_results functions.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

from dt.find import find_by_hash, format_results
from dt.errors import FindError


# =============================================================================
# find_by_hash tests
# =============================================================================

class TestFindByHash:
    """Tests for the find_by_hash function."""

    def test_hash_too_short_raises_error(self):
        """Test that a hash shorter than 4 chars raises FindError."""
        with pytest.raises(FindError, match="at least 4 characters"):
            find_by_hash("abc")

    def test_hash_exactly_4_chars_accepted(self):
        """Test that 4-char hash is accepted (even if no results)."""
        with patch("dt.find.Repo") as mock_repo:
            repo_instance = MagicMock()
            repo_instance.index.outs = []
            mock_repo.return_value = repo_instance
            
            with patch("dt.find.get_cache_dir", return_value=None):
                results = find_by_hash("abcd")
                assert results == []

    def test_not_in_dvc_repo(self):
        """Test error when not in a DVC repository."""
        with patch("dt.find.Repo", side_effect=Exception("Not a DVC repository")):
            with pytest.raises(FindError, match="Not in a DVC repository"):
                find_by_hash("abcd1234")

    def test_no_results_for_nonexistent_hash(self):
        """Test empty results for a hash that doesn't exist."""
        with patch("dt.find.Repo") as mock_repo:
            repo_instance = MagicMock()
            repo_instance.index.outs = []
            mock_repo.return_value = repo_instance
            
            with patch("dt.find.get_cache_dir", return_value=None):
                results = find_by_hash("deadbeef")
                assert results == []

    def test_finds_matching_top_level_file(self):
        """Test finding a top-level tracked file by hash."""
        mock_out = MagicMock()
        mock_out.hash_info = MagicMock()
        mock_out.hash_info.value = "abcd1234567890abcdef1234567890ab"
        mock_out.hash_info.isdir = False
        mock_out.fs_path = "/path/to/data.csv"
        mock_out.stage = MagicMock()
        mock_out.stage.path = "/path/to/data.csv.dvc"
        
        with patch("dt.find.Repo") as mock_repo:
            repo_instance = MagicMock()
            repo_instance.index.outs = [mock_out]
            mock_repo.return_value = repo_instance
            
            with patch("dt.find.get_cache_dir", return_value=None):
                results = find_by_hash("abcd")
                
                assert len(results) == 1
                assert results[0]["path"] == "/path/to/data.csv"
                assert results[0]["hash"] == "abcd1234567890abcdef1234567890ab"

    def test_hash_matching_is_case_insensitive(self):
        """Test that hash matching is case-insensitive."""
        mock_out = MagicMock()
        mock_out.hash_info = MagicMock()
        mock_out.hash_info.value = "ABCD1234567890abcdef1234567890ab"
        mock_out.hash_info.isdir = False
        mock_out.fs_path = "/path/to/file.bin"
        mock_out.stage = None
        
        with patch("dt.find.Repo") as mock_repo:
            repo_instance = MagicMock()
            repo_instance.index.outs = [mock_out]
            mock_repo.return_value = repo_instance
            
            with patch("dt.find.get_cache_dir", return_value=None):
                # Search with lowercase
                results = find_by_hash("abcd")
                assert len(results) == 1

    def test_shows_dvc_file_when_requested(self):
        """Test including .dvc file path in results."""
        mock_out = MagicMock()
        mock_out.hash_info = MagicMock()
        mock_out.hash_info.value = "abcd1234567890abcdef1234567890ab"
        mock_out.hash_info.isdir = False
        mock_out.fs_path = "/path/to/data.csv"
        mock_out.stage = MagicMock()
        mock_out.stage.path = "/path/to/data.csv.dvc"
        
        with patch("dt.find.Repo") as mock_repo:
            repo_instance = MagicMock()
            repo_instance.index.outs = [mock_out]
            mock_repo.return_value = repo_instance
            
            with patch("dt.find.get_cache_dir", return_value=None):
                results = find_by_hash("abcd", show_dvc_file=True)
                
                assert len(results) == 1
                assert results[0]["dvc_file"] == "/path/to/data.csv.dvc"

    def test_shows_cache_path_when_requested(self):
        """Test including cache path in results."""
        mock_out = MagicMock()
        mock_out.hash_info = MagicMock()
        mock_out.hash_info.value = "abcd1234567890abcdef1234567890ab"
        mock_out.hash_info.isdir = False
        mock_out.fs_path = "/path/to/data.csv"
        mock_out.stage = None
        
        cache_dir = Path("/cache/files/md5")
        
        with patch("dt.find.Repo") as mock_repo:
            repo_instance = MagicMock()
            repo_instance.index.outs = [mock_out]
            mock_repo.return_value = repo_instance
            
            with patch("dt.find.get_cache_dir", return_value=cache_dir):
                with patch("dt.find.hash_to_cache_path") as mock_h2c:
                    mock_h2c.return_value = cache_dir / "ab" / "cd1234567890abcdef1234567890ab"
                    
                    results = find_by_hash("abcd", show_cache_path=True)
                    
                    assert len(results) == 1
                    assert "cache_path" in results[0]

    def test_dir_suffix_stripped_for_matching(self):
        """Test that .dir suffix is stripped when matching hashes."""
        mock_out = MagicMock()
        mock_out.hash_info = MagicMock()
        mock_out.hash_info.value = "abcd1234567890abcdef1234567890ab.dir"
        mock_out.hash_info.isdir = True
        mock_out.fs_path = "/path/to/dataset"
        mock_out.stage = None
        mock_out.get_obj = MagicMock(return_value=None)
        
        with patch("dt.find.Repo") as mock_repo:
            repo_instance = MagicMock()
            repo_instance.index.outs = [mock_out]
            mock_repo.return_value = repo_instance
            
            with patch("dt.find.get_cache_dir", return_value=None):
                # Search without .dir suffix
                results = find_by_hash("abcd", expand_dirs=False)
                
                assert len(results) == 1
                assert results[0]["hash"] == "abcd1234567890abcdef1234567890ab.dir"

    def test_skips_outs_without_hash_info(self):
        """Test that outputs without hash_info are skipped."""
        mock_out_no_hash = MagicMock()
        mock_out_no_hash.hash_info = None
        
        mock_out_with_hash = MagicMock()
        mock_out_with_hash.hash_info = MagicMock()
        mock_out_with_hash.hash_info.value = "abcd1234567890abcdef1234567890ab"
        mock_out_with_hash.hash_info.isdir = False
        mock_out_with_hash.fs_path = "/path/to/data.csv"
        mock_out_with_hash.stage = None
        
        with patch("dt.find.Repo") as mock_repo:
            repo_instance = MagicMock()
            repo_instance.index.outs = [mock_out_no_hash, mock_out_with_hash]
            mock_repo.return_value = repo_instance
            
            with patch("dt.find.get_cache_dir", return_value=None):
                results = find_by_hash("abcd")
                
                assert len(results) == 1


# =============================================================================
# format_results tests
# =============================================================================

class TestFormatResults:
    """Tests for the format_results function."""

    def test_json_output_format(self):
        """Test JSON output formatting."""
        results = [
            {"path": "/path/to/file.csv", "hash": "abcd1234"},
            {"path": "/path/to/other.bin", "hash": "efgh5678"},
        ]
        
        output = format_results(results, json_output=True)
        parsed = json.loads(output)
        
        assert len(parsed) == 2
        assert parsed[0]["path"] == "/path/to/file.csv"

    def test_empty_results_message(self):
        """Test message for empty results."""
        output = format_results([])
        assert output == "No matches found"

    def test_simple_path_output(self):
        """Test simple path-only output."""
        results = [
            {"path": "/path/to/file1.csv", "hash": "abcd1234"},
            {"path": "/path/to/file2.csv", "hash": "efgh5678"},
        ]
        
        output = format_results(results)
        lines = output.split("\n")
        
        assert len(lines) == 2
        assert "/path/to/file1.csv" in lines[0]
        assert "/path/to/file2.csv" in lines[1]

    def test_verbose_output_includes_dvc_file(self):
        """Test verbose output includes .dvc file info."""
        results = [
            {
                "path": "/path/to/file.csv",
                "hash": "abcd1234",
                "dvc_file": "/path/to/file.csv.dvc",
            }
        ]
        
        output = format_results(results, verbose=True)
        
        assert "file.csv.dvc" in output
        assert "(dvc:" in output

    def test_verbose_output_includes_dir_hash(self):
        """Test verbose output includes parent .dir hash."""
        results = [
            {
                "path": "/path/to/dataset/subfile.csv",
                "hash": "abcd1234",
                "dir_hash": "parenthash12345678901234567890ab.dir",
            }
        ]
        
        output = format_results(results, verbose=True)
        
        assert "(dir:" in output
        assert "parenthash123456" in output

    def test_verbose_output_includes_cache_path(self):
        """Test verbose output includes cache path on separate line."""
        results = [
            {
                "path": "/path/to/file.csv",
                "hash": "abcd1234",
                "cache_path": "/cache/files/md5/ab/cd1234",
            }
        ]
        
        output = format_results(results, verbose=True)
        
        assert "cache:" in output
        assert "/cache/files/md5/ab/cd1234" in output
