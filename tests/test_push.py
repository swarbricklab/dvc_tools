"""Unit tests for dt.push module.

Tests push functionality for DVC-tracked files.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.push import (
    get_files_size,
    get_project_remotes,
    push_to_remote,
    push_all,
    build_manifest,
)
from dt.errors import PushError


# =============================================================================
# get_files_size tests
# =============================================================================

class TestGetFilesSize:
    """Tests for the get_files_size function."""

    def test_returns_0_when_repo_not_available(self):
        """Test returns 0 when DVC repo not accessible."""
        with patch("dt.push.Repo", side_effect=Exception("No repo")):
            result = get_files_size(["abc123", "def456"])
            
            assert result == 0

    def test_sums_cache_file_sizes(self, tmp_path):
        """Test sums up sizes of cached files."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        (cache_dir / "de").mkdir(parents=True)
        
        # Create mock cache files
        file1 = cache_dir / "ab" / "c123"
        file1.write_bytes(b"x" * 100)
        file2 = cache_dir / "de" / "f456"
        file2.write_bytes(b"y" * 200)
        
        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_files_size(["abc123", "def456"])
            
            assert result == 300

    def test_handles_dir_suffix(self, tmp_path):
        """Test handles .dir suffix in hashes."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        
        dir_file = cache_dir / "ab" / "c123.dir"
        dir_file.write_bytes(b"x" * 50)
        
        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_files_size(["abc123.dir"])
            
            assert result == 50

    def test_skips_nonexistent_files(self, tmp_path):
        """Test skips files that don't exist in cache."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        
        file1 = cache_dir / "ab" / "c123"
        file1.write_bytes(b"x" * 100)
        # def456 doesn't exist
        
        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_files_size(["abc123", "def456"])
            
            assert result == 100


# =============================================================================
# get_project_remotes tests
# =============================================================================

class TestGetProjectRemotes:
    """Tests for the get_project_remotes function."""

    def test_returns_remotes_from_dvc_config(self):
        """Test returns remotes from DVC config API."""
        mock_repo = MagicMock()
        mock_repo.config.read.side_effect = lambda level: {
            "local": {"remote": {"local": {"url": "/local/path"}}},
            "repo": {"remote": {"origin": {"url": "ssh://host/path"}}},
        }.get(level, {})
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_project_remotes()
            
            assert len(result) == 2
            names = [r[0] for r in result]
            assert "local" in names
            assert "origin" in names

    def test_local_overrides_repo_scope(self):
        """Test that local scope overrides repo scope for same name."""
        mock_repo = MagicMock()
        mock_repo.config.read.side_effect = lambda level: {
            "local": {"remote": {"origin": {"url": "/local/override"}}},
            "repo": {"remote": {"origin": {"url": "ssh://original"}}},
        }.get(level, {})
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_project_remotes()
            
            # Should only have one "origin" entry
            origin = [r for r in result if r[0] == "origin"]
            assert len(origin) == 1
            assert origin[0][1] == "/local/override"


# =============================================================================
# push_to_remote tests
# =============================================================================

class TestPushToRemote:
    """Tests for the push_to_remote function."""

    def test_runs_dvc_push_with_remote(self):
        """Test runs dvc push with -r remote."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="Pushed 5 files\n",
                stderr="",
            )
            
            success, output = push_to_remote("origin", [])
            
            assert success is True
            assert "Pushed" in output
            
            call_args = mock_run.call_args[0][0]
            assert call_args == ["dvc", "push", "-r", "origin"]

    def test_passes_through_additional_args(self):
        """Test passes through additional arguments."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            push_to_remote("origin", ["--jobs", "4", "-v"])
            
            call_args = mock_run.call_args[0][0]
            assert "--jobs" in call_args
            assert "4" in call_args
            assert "-v" in call_args

    def test_returns_failure_on_error(self):
        """Test returns (False, error_message) on failure."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="Push failed: connection refused",
            )
            
            success, output = push_to_remote("origin", [])
            
            assert success is False
            assert "connection refused" in output


# =============================================================================
# push_all tests
# =============================================================================

class TestPushAll:
    """Tests for the push_all function."""

    def test_raises_error_when_no_remotes(self):
        """Test PushError raised when no remotes configured."""
        with patch("dt.push.get_project_remotes", return_value=[]):
            with pytest.raises(PushError, match="No remotes configured"):
                push_all([])

    def test_pushes_to_all_remotes(self):
        """Test pushes to all configured remotes."""
        with patch("dt.push.get_project_remotes", return_value=[
            ("origin", "ssh://host/path"),
            ("local", "/local/path"),
        ]):
            with patch("dt.push.push_to_remote") as mock_push:
                mock_push.return_value = (True, "ok")
                
                results = push_all([])
                
                assert len(results) == 2
                assert mock_push.call_count == 2

    def test_returns_results_for_each_remote(self):
        """Test returns (remote_name, success, output) for each remote."""
        with patch("dt.push.get_project_remotes", return_value=[
            ("origin", "ssh://host/path"),
        ]):
            with patch("dt.push.push_to_remote") as mock_push:
                mock_push.return_value = (True, "Pushed 5 files")
                
                results = push_all([])
                
                assert results[0] == ("origin", True, "Pushed 5 files")


# =============================================================================
# build_manifest tests
# =============================================================================

class TestBuildManifest:
    """Tests for the build_manifest function."""

    def test_returns_empty_manifest_when_no_entries(self):
        """Test returns empty manifest when no tracked entries."""
        with patch("dt.push.utils.collect_tracked_entries") as mock_collect:
            mock_collect.return_value = {
                "repo": MagicMock(root_dir="/repo"),
                "indexes": {},
                "hash_to_path": {},
            }
            
            with patch("dvc_data.index.fetch.collect", create=True):
                result = build_manifest()
                
                assert result["files"] == []
                assert result["paths"] == {}
