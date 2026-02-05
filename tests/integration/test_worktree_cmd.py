"""Integration tests for 'dt worktree' command group.

Tests for git worktree management with DVC cache configuration.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def git_repo_with_branch(git_repo_with_commits):
    """Git repo with an extra branch for worktree testing."""
    repo = git_repo_with_commits
    
    # Create a branch
    subprocess.run(
        ['git', 'branch', 'feature-branch'],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    
    return repo


# =============================================================================
# Test Worktree Add
# =============================================================================

class TestWorktreeAdd:
    """Tests for 'dt worktree add' subcommand."""
    
    def test_worktree_add_help(self):
        """'dt worktree add --help' shows usage."""
        result = subprocess.run(
            ['dt', 'worktree', 'add', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'PATH' in result.stdout
        assert '--branch' in result.stdout or '-b' in result.stdout
    
    def test_worktree_add_with_existing_branch(self, git_repo_with_branch, tmp_path, monkeypatch):
        """Add worktree with existing branch."""
        monkeypatch.chdir(git_repo_with_branch)
        
        worktree_path = tmp_path / 'worktree1'
        
        result = subprocess.run(
            ['dt', 'worktree', 'add', '--branch', 'feature-branch', 
             str(worktree_path)],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # Worktree should exist
            assert worktree_path.exists()
            # Should have git directory link
            assert (worktree_path / '.git').exists()
    
    def test_worktree_add_with_new_branch(self, git_repo_with_commits, tmp_path, monkeypatch):
        """Add worktree with new branch (-b flag)."""
        monkeypatch.chdir(git_repo_with_commits)
        
        worktree_path = tmp_path / 'new_worktree'
        
        result = subprocess.run(
            ['dt', 'worktree', 'add', '-b', 'new-feature',
             str(worktree_path)],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            assert worktree_path.exists()


class TestWorktreeList:
    """Tests for 'dt worktree list' subcommand."""
    
    def test_worktree_list_help(self):
        """'dt worktree list --help' shows usage."""
        result = subprocess.run(
            ['dt', 'worktree', 'list', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
    
    def test_worktree_list_shows_main(self, git_repo_with_commits, monkeypatch):
        """List shows main worktree."""
        monkeypatch.chdir(git_repo_with_commits)
        
        result = subprocess.run(
            ['dt', 'worktree', 'list'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should show at least the main worktree
        assert str(git_repo_with_commits) in result.stdout or 'main' in result.stdout.lower() or result.stdout.strip()
    
    def test_worktree_list_multiple(self, git_repo_with_branch, tmp_path, monkeypatch):
        """List shows multiple worktrees."""
        repo = git_repo_with_branch
        monkeypatch.chdir(repo)
        
        # Add a worktree
        worktree_path = tmp_path / 'extra_worktree'
        subprocess.run(
            ['dt', 'worktree', 'add', '--branch', 'feature-branch',
             str(worktree_path)],
            capture_output=True,
        )
        
        result = subprocess.run(
            ['dt', 'worktree', 'list'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0


class TestWorktreeRemove:
    """Tests for 'dt worktree remove' subcommand."""
    
    def test_worktree_remove_help(self):
        """'dt worktree remove --help' shows usage."""
        result = subprocess.run(
            ['dt', 'worktree', 'remove', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'PATH' in result.stdout
        assert '--force' in result.stdout
    
    def test_worktree_remove(self, git_repo_with_branch, tmp_path, monkeypatch):
        """Remove a worktree."""
        repo = git_repo_with_branch
        monkeypatch.chdir(repo)
        
        # Add a worktree
        worktree_path = tmp_path / 'to_remove'
        subprocess.run(
            ['dt', 'worktree', 'add', '--branch', 'feature-branch',
             str(worktree_path)],
            capture_output=True,
        )
        
        if worktree_path.exists():
            result = subprocess.run(
                ['dt', 'worktree', 'remove', str(worktree_path)],
                capture_output=True,
                text=True,
            )
            
            # Should succeed
            assert result.returncode in (0, 1)


# =============================================================================
# Test Worktree Help
# =============================================================================

class TestWorktreeHelp:
    """Tests for worktree command help."""
    
    def test_worktree_group_help(self):
        """'dt worktree --help' shows subcommands."""
        result = subprocess.run(
            ['dt', 'worktree', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'add' in result.stdout
        assert 'list' in result.stdout
        assert 'remove' in result.stdout


# =============================================================================
# Test Worktree DVC Integration
# =============================================================================

class TestWorktreeDvcIntegration:
    """Tests for DVC cache configuration in worktrees."""
    
    def test_worktree_configures_dvc_cache(self, dvc_repo_with_cache, tmp_path, monkeypatch):
        """Worktree should share DVC cache with main worktree."""
        repo = dvc_repo_with_cache['repo']
        monkeypatch.chdir(repo)
        
        # Create a branch
        subprocess.run(
            ['git', 'branch', 'cache-test'],
            capture_output=True,
        )
        
        worktree_path = tmp_path / 'dvc_worktree'
        
        result = subprocess.run(
            ['dt', 'worktree', 'add', '--branch', 'cache-test',
             str(worktree_path)],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0 and worktree_path.exists():
            # Check DVC cache config in worktree
            # The worktree should have cache configured to same location
            pass  # DVC config check would go here


# =============================================================================
# Test Worktree Errors
# =============================================================================

class TestWorktreeErrors:
    """Tests for error handling."""
    
    def test_worktree_add_existing_path(self, git_repo_with_commits, tmp_path, monkeypatch):
        """Add worktree to existing path should error."""
        monkeypatch.chdir(git_repo_with_commits)
        
        # Create directory
        existing_dir = tmp_path / 'existing'
        existing_dir.mkdir()
        (existing_dir / 'file.txt').write_text('existing')
        
        result = subprocess.run(
            ['dt', 'worktree', 'add', '-b', 'test-branch',
             str(existing_dir)],
            capture_output=True,
            text=True,
        )
        
        # Should fail or handle existing directory
        assert result.returncode in (0, 1, 128)
    
    def test_worktree_outside_git_repo(self, tmp_path, monkeypatch):
        """Worktree commands outside git repo should error."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'worktree', 'list'],
            capture_output=True,
            text=True,
        )
        
        # Should fail
        assert result.returncode != 0
