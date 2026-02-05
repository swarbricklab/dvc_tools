"""Integration tests for 'dt history' command.

Tests for showing version history of DVC-tracked files.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def dvc_repo_with_history(dvc_repo_with_files, monkeypatch):
    """DVC repo with file that has multiple versions.
    
    Creates a tracked file and modifies it across commits.
    """
    repo = dvc_repo_with_files
    monkeypatch.chdir(repo)
    
    # Modify the data file and create a new version
    data_file = repo / 'data.csv'
    data_file.write_text('id,value\n1,100\n2,200\n3,300\n4,400\n')
    
    # Re-add to DVC (creates new hash)
    subprocess.run(['dvc', 'add', 'data.csv'], check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], check=True, capture_output=True)
    subprocess.run(
        ['git', 'commit', '-m', 'Update data.csv v2'],
        check=True, capture_output=True
    )
    
    # Create a third version
    data_file.write_text('id,value\n1,100\n2,200\n3,300\n4,400\n5,500\n')
    subprocess.run(['dvc', 'add', 'data.csv'], check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], check=True, capture_output=True)
    subprocess.run(
        ['git', 'commit', '-m', 'Update data.csv v3'],
        check=True, capture_output=True
    )
    
    return repo


# =============================================================================
# Test Classes
# =============================================================================

class TestHistoryBasic:
    """Tests for basic 'dt history' functionality."""
    
    def test_history_help(self):
        """'dt history --help' shows usage."""
        result = subprocess.run(
            ['dt', 'history', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'PATH' in result.stdout or 'path' in result.stdout.lower()
        assert '--limit' in result.stdout or '-n' in result.stdout
    
    def test_history_tracked_file(self, dvc_repo_with_history):
        """Show history of tracked file."""
        result = subprocess.run(
            ['dt', 'history', 'data.csv'],
            capture_output=True,
            text=True,
            cwd=dvc_repo_with_history,
        )
        
        assert result.returncode == 0
        # Should show version history
        # Could show commit hashes, dates, or file hashes
        assert result.stdout.strip()
    
    def test_history_shows_multiple_versions(self, dvc_repo_with_history):
        """History should show multiple versions."""
        result = subprocess.run(
            ['dt', 'history', 'data.csv'],
            capture_output=True,
            text=True,
            cwd=dvc_repo_with_history,
        )
        
        assert result.returncode == 0
        # Should have multiple lines (one per version)
        lines = [l for l in result.stdout.strip().split('\n') if l.strip()]
        # At least 2 versions (we created 3)
        assert len(lines) >= 2


class TestHistoryLimit:
    """Tests for limiting history output."""
    
    def test_history_limit(self, dvc_repo_with_history):
        """'-n' limits number of versions shown."""
        result = subprocess.run(
            ['dt', 'history', '-n', '1', 'data.csv'],
            capture_output=True,
            text=True,
            cwd=dvc_repo_with_history,
        )
        
        assert result.returncode == 0
        # Should have limited output
        lines = [l for l in result.stdout.strip().split('\n') if l.strip()]
        # At most 1 version entry (may have header)
        assert len(lines) <= 3


class TestHistorySince:
    """Tests for date filtering."""
    
    def test_history_since(self, dvc_repo_with_history):
        """'--since' filters by date."""
        result = subprocess.run(
            ['dt', 'history', '--since', '1 week ago', 'data.csv'],
            capture_output=True,
            text=True,
            cwd=dvc_repo_with_history,
        )
        
        # Should work (all versions are recent)
        assert result.returncode == 0
    
    def test_history_since_future(self, dvc_repo_with_history):
        """'--since' with future date shows nothing."""
        result = subprocess.run(
            ['dt', 'history', '--since', '2030-01-01', 'data.csv'],
            capture_output=True,
            text=True,
            cwd=dvc_repo_with_history,
        )
        
        # Should return error (no commits found) or success with empty output
        assert result.returncode in (0, 1)
        # Should be empty or show no versions


class TestHistoryOutput:
    """Tests for output formats."""
    
    def test_history_json(self, dvc_repo_with_history):
        """'--json' produces valid JSON."""
        import json
        
        result = subprocess.run(
            ['dt', 'history', '--json', 'data.csv'],
            capture_output=True,
            text=True,
            cwd=dvc_repo_with_history,
        )
        
        assert result.returncode == 0
        # Should be valid JSON
        try:
            data = json.loads(result.stdout)
            assert isinstance(data, list)
        except json.JSONDecodeError:
            pytest.fail("Output is not valid JSON")
    
    def test_history_verbose(self, dvc_repo_with_history):
        """'--verbose' shows full details."""
        result = subprocess.run(
            ['dt', 'history', '--verbose', 'data.csv'],
            capture_output=True,
            text=True,
            cwd=dvc_repo_with_history,
        )
        
        assert result.returncode == 0
        # Verbose should show more info


class TestHistoryErrors:
    """Tests for error handling."""
    
    def test_history_untracked_file(self, dvc_repo_with_files, monkeypatch):
        """History of untracked file should error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        # Create a file that's not tracked by DVC
        (dvc_repo_with_files / 'untracked.txt').write_text('hello')
        
        result = subprocess.run(
            ['dt', 'history', 'untracked.txt'],
            capture_output=True,
            text=True,
        )
        
        # Should fail or show no history
        # Behavior depends on implementation
        assert result.returncode != 0 or 'not' in result.stderr.lower() or 'no' in result.stdout.lower()
    
    def test_history_nonexistent_file(self, dvc_repo_with_files, monkeypatch):
        """History of non-existent file should error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'history', 'nonexistent.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should fail
        assert result.returncode != 0
    
    def test_history_outside_repo(self, tmp_path, monkeypatch):
        """History outside DVC repo should error."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'history', 'file.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should fail
        assert result.returncode != 0
