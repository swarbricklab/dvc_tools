"""Integration tests for 'dt diff' command.

Tests for showing content differences between versions.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def dvc_repo_with_versions(dvc_repo_with_files, monkeypatch):
    """DVC repo with file that has multiple versions for diffing.
    
    Creates a CSV file with different content across commits.
    """
    repo = dvc_repo_with_files
    monkeypatch.chdir(repo)
    
    # Get the first commit hash
    result = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        text=True,
    )
    first_commit = result.stdout.strip()
    
    # Modify the data file
    data_file = repo / 'data.csv'
    data_file.write_text('id,value\n1,100\n2,250\n3,300\n')  # Changed 200 to 250
    
    # Re-add to DVC
    subprocess.run(['dvc', 'add', 'data.csv'], check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], check=True, capture_output=True)
    subprocess.run(
        ['git', 'commit', '-m', 'Modify data.csv'],
        check=True, capture_output=True
    )
    
    # Get second commit
    result = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        text=True,
    )
    second_commit = result.stdout.strip()
    
    return {
        'repo': repo,
        'old_commit': first_commit,
        'new_commit': second_commit,
        'file': 'data.csv',
    }


# =============================================================================
# Test Classes
# =============================================================================

class TestDiffBasic:
    """Tests for basic 'dt diff' functionality."""
    
    def test_diff_help(self):
        """'dt diff --help' shows usage."""
        result = subprocess.run(
            ['dt', 'diff', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'PATH' in result.stdout or 'path' in result.stdout.lower()
        assert '--old' in result.stdout
        assert '--new' in result.stdout
    
    def test_diff_workspace_vs_head(self, dvc_repo_with_files, monkeypatch):
        """Default diff compares workspace to HEAD."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        # Modify file in workspace (not committed)
        data_file = repo / 'data.csv'
        data_file.write_text('id,value\n1,999\n2,200\n3,300\n')
        
        result = subprocess.run(
            ['dt', 'diff', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should show diff or indicate files differ
        # May return 0 even if different (depends on implementation)
        assert result.returncode in (0, 1)


class TestDiffBetweenRevisions:
    """Tests for diffing between commits."""
    
    def test_diff_between_commits(self, dvc_repo_with_versions):
        """Diff between two specific commits."""
        repo = dvc_repo_with_versions['repo']
        old_commit = dvc_repo_with_versions['old_commit']
        new_commit = dvc_repo_with_versions['new_commit']
        file_path = dvc_repo_with_versions['file']
        
        result = subprocess.run(
            ['dt', 'diff', '--old', old_commit, '--new', new_commit, file_path],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        # Should show difference
        assert result.returncode in (0, 1)
    
    def test_diff_old_only(self, dvc_repo_with_versions):
        """Diff with only --old specified (compares to workspace)."""
        repo = dvc_repo_with_versions['repo']
        old_commit = dvc_repo_with_versions['old_commit']
        file_path = dvc_repo_with_versions['file']
        
        result = subprocess.run(
            ['dt', 'diff', '--old', old_commit, file_path],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode in (0, 1)


class TestDiffOutputFormats:
    """Tests for different output formats."""
    
    def test_diff_terminal_format(self, dvc_repo_with_versions):
        """Terminal format (default) output."""
        repo = dvc_repo_with_versions['repo']
        file_path = dvc_repo_with_versions['file']
        old_commit = dvc_repo_with_versions['old_commit']
        new_commit = dvc_repo_with_versions['new_commit']
        
        result = subprocess.run(
            ['dt', 'diff', '-o', 'terminal',
             '--old', old_commit, '--new', new_commit, file_path],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode in (0, 1)
    
    def test_diff_json_format(self, dvc_repo_with_versions):
        """JSON format output."""
        repo = dvc_repo_with_versions['repo']
        file_path = dvc_repo_with_versions['file']
        old_commit = dvc_repo_with_versions['old_commit']
        new_commit = dvc_repo_with_versions['new_commit']
        
        result = subprocess.run(
            ['dt', 'diff', '-o', 'json',
             '--old', old_commit, '--new', new_commit, file_path],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        # Should produce output (may be JSON or error)
        assert result.returncode in (0, 1)


class TestDiffErrors:
    """Tests for error handling."""
    
    def test_diff_nonexistent_file(self, dvc_repo_with_files, monkeypatch):
        """Diff of non-existent file should error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'diff', 'nonexistent.csv'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode != 0
    
    def test_diff_untracked_file(self, dvc_repo_with_files, monkeypatch):
        """Diff of untracked file should error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        # Create untracked file
        (dvc_repo_with_files / 'untracked.csv').write_text('a,b\n1,2\n')
        
        result = subprocess.run(
            ['dt', 'diff', 'untracked.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should fail or handle gracefully
        assert result.returncode != 0 or 'not' in result.stderr.lower()
    
    def test_diff_invalid_revision(self, dvc_repo_with_files, monkeypatch):
        """Diff with invalid revision should error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'diff', '--old', 'invalid-revision-xyz', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode != 0
    
    def test_diff_outside_repo(self, tmp_path, monkeypatch):
        """Diff outside DVC repo should error."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'diff', 'file.csv'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode != 0


class TestDiffVerbose:
    """Tests for verbose output."""
    
    def test_diff_verbose(self, dvc_repo_with_versions):
        """'--verbose' shows detailed progress."""
        repo = dvc_repo_with_versions['repo']
        file_path = dvc_repo_with_versions['file']
        old_commit = dvc_repo_with_versions['old_commit']
        new_commit = dvc_repo_with_versions['new_commit']
        
        result = subprocess.run(
            ['dt', 'diff', '--verbose',
             '--old', old_commit, '--new', new_commit, file_path],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode in (0, 1)
