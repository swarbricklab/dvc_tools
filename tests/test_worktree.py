"""Unit tests for dt.worktree module.

Tests git worktree management with DVC cache configuration.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.worktree import (
    add,
    list_worktrees,
    remove,
)
from dt.errors import WorktreeError


# =============================================================================
# add tests
# =============================================================================

class TestAdd:
    """Tests for the add function."""

    def test_raises_error_when_no_branch_specified(self, tmp_path):
        """Test WorktreeError raised when neither branch nor new_branch specified."""
        worktree_path = tmp_path / "worktree"
        
        with patch("dt.worktree.utils.get_cache_dir", return_value=None):
            with pytest.raises(WorktreeError, match="Must specify either"):
                add(str(worktree_path))

    def test_creates_worktree_with_existing_branch(self, tmp_path):
        """Test creates worktree for existing branch."""
        worktree_path = tmp_path / "worktree"
        
        with patch("dt.worktree.utils.get_cache_dir", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                
                result = add(str(worktree_path), branch="main", verbose=False)
                
                assert result == worktree_path.resolve()
                
                # Check git worktree add was called
                call_args = mock_run.call_args_list[0][0][0]
                assert "git" in call_args
                assert "worktree" in call_args
                assert "add" in call_args
                assert "main" in call_args

    def test_creates_worktree_with_new_branch(self, tmp_path):
        """Test creates worktree with new branch."""
        worktree_path = tmp_path / "worktree"
        
        with patch("dt.worktree.utils.get_cache_dir", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                
                add(str(worktree_path), new_branch="feature/new", verbose=False)
                
                call_args = mock_run.call_args_list[0][0][0]
                assert "-b" in call_args
                assert "feature/new" in call_args

    def test_configures_dvc_cache_in_worktree(self, tmp_path):
        """Test configures DVC cache in new worktree."""
        worktree_path = tmp_path / "worktree"
        cache_dir = Path("/cache/files/md5")
        
        with patch("dt.worktree.utils.get_cache_dir", return_value=cache_dir):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                
                add(str(worktree_path), branch="main", verbose=False)
                
                # Should have called dvc cache dir
                dvc_cache_calls = [
                    c for c in mock_run.call_args_list
                    if "dvc" in c[0][0] and "cache" in c[0][0]
                ]
                assert len(dvc_cache_calls) >= 1

    def test_initializes_submodules(self, tmp_path):
        """Test initializes git submodules in worktree."""
        worktree_path = tmp_path / "worktree"
        
        with patch("dt.worktree.utils.get_cache_dir", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                
                add(str(worktree_path), branch="main", verbose=False)
                
                # Should have called git submodule update
                submodule_calls = [
                    c for c in mock_run.call_args_list
                    if "submodule" in c[0][0]
                ]
                assert len(submodule_calls) >= 1

    def test_raises_error_on_git_failure(self, tmp_path):
        """Test WorktreeError raised when git worktree add fails."""
        worktree_path = tmp_path / "worktree"
        
        with patch("dt.worktree.utils.get_cache_dir", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=1,
                    stdout="",
                    stderr="fatal: already exists",
                )
                
                with pytest.raises(WorktreeError, match="git worktree add failed"):
                    add(str(worktree_path), branch="main")

    def test_returns_resolved_path(self, tmp_path):
        """Test returns resolved absolute path."""
        worktree_path = tmp_path / "worktree"
        
        with patch("dt.worktree.utils.get_cache_dir", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                
                result = add(str(worktree_path), branch="main", verbose=False)
                
                assert result.is_absolute()
                assert result == worktree_path.resolve()


# =============================================================================
# list_worktrees tests
# =============================================================================

class TestListWorktrees:
    """Tests for the list_worktrees function."""

    def test_raises_error_on_failure(self):
        """Test WorktreeError raised when git worktree list fails."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="not a git repository",
            )
            
            with pytest.raises(WorktreeError, match="git worktree list failed"):
                list_worktrees()

    def test_parses_porcelain_output(self):
        """Test parses --porcelain output correctly."""
        porcelain_output = """worktree /path/to/main
HEAD abc123def456
branch refs/heads/main

worktree /path/to/feature
HEAD 789xyz
branch refs/heads/feature
"""
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=porcelain_output,
                stderr="",
            )
            
            result = list_worktrees()
            
            assert len(result) == 2
            assert result[0]["path"] == "/path/to/main"
            assert result[0]["head"] == "abc123def456"
            assert result[0]["branch"] == "refs/heads/main"

    def test_handles_bare_worktree(self):
        """Test handles bare worktree marker."""
        porcelain_output = """worktree /path/to/bare
HEAD abc123
bare
"""
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=porcelain_output,
                stderr="",
            )
            
            result = list_worktrees()
            
            assert len(result) == 1
            assert result[0].get("bare") is True

    def test_handles_detached_head(self):
        """Test handles detached HEAD state."""
        porcelain_output = """worktree /path/to/detached
HEAD abc123
detached
"""
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=porcelain_output,
                stderr="",
            )
            
            result = list_worktrees()
            
            assert len(result) == 1
            assert result[0].get("detached") is True

    def test_returns_empty_list_for_empty_output(self):
        """Test returns empty list for empty output."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            result = list_worktrees()
            
            assert result == []


# =============================================================================
# remove tests
# =============================================================================

class TestRemove:
    """Tests for the remove function."""

    def test_runs_git_worktree_remove(self, tmp_path):
        """Test runs git worktree remove command."""
        worktree_path = tmp_path / "worktree"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            remove(str(worktree_path), verbose=False)
            
            call_args = mock_run.call_args[0][0]
            assert "git" in call_args
            assert "worktree" in call_args
            assert "remove" in call_args
            assert str(worktree_path) in call_args

    def test_includes_force_flag(self, tmp_path):
        """Test includes --force flag when specified."""
        worktree_path = tmp_path / "worktree"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            remove(str(worktree_path), force=True, verbose=False)
            
            call_args = mock_run.call_args[0][0]
            assert "--force" in call_args

    def test_raises_error_on_failure(self, tmp_path):
        """Test WorktreeError raised when removal fails."""
        worktree_path = tmp_path / "worktree"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="worktree has uncommitted changes",
            )
            
            with pytest.raises(WorktreeError, match="git worktree remove failed"):
                remove(str(worktree_path))

    def test_verbose_prints_progress(self, tmp_path, capsys):
        """Test verbose mode prints progress."""
        worktree_path = tmp_path / "worktree"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            remove(str(worktree_path), verbose=True)
            
            captured = capsys.readouterr()
            assert "Removing worktree" in captured.out
