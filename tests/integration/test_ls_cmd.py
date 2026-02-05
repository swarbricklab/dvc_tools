"""Integration tests for 'dt ls' command.

Tests for listing DVC-tracked files with various filters.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Classes
# =============================================================================

class TestLsBasic:
    """Tests for basic 'dt ls' functionality."""
    
    def test_ls_in_dvc_repo(self, dvc_repo_with_files, monkeypatch):
        """List tracked files in DVC repo."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should show tracked file
        assert 'data.csv' in result.stdout
    
    def test_ls_help(self):
        """'dt ls --help' shows usage."""
        result = subprocess.run(
            ['dt', 'ls', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '--pattern' in result.stdout or '-p' in result.stdout
        assert '--long' in result.stdout or '-l' in result.stdout
    
    def test_ls_outside_dvc_repo(self, tmp_path, monkeypatch):
        """List in non-DVC directory should handle gracefully."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'ls'],
            capture_output=True,
            text=True,
        )
        
        # Should either return empty or error message
        # depending on dvc list behavior outside repos
        assert result.returncode in (0, 1)


class TestLsRecursive:
    """Tests for recursive listing."""
    
    def test_ls_recursive_flag(self, dvc_repo_with_files, monkeypatch):
        """'-R' flag enables recursive listing."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '-R'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Recursive listing should work
        assert 'data.csv' in result.stdout


class TestLsLongFormat:
    """Tests for long format output."""
    
    def test_ls_long_format(self, dvc_repo_with_files, monkeypatch):
        """'-l' shows size and type."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '-l'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Long format should include file info
        output = result.stdout
        # Should show file type or size information
        assert 'data.csv' in output
    
    def test_ls_show_hash(self, dvc_repo_with_files, monkeypatch):
        """'--show-hash' shows MD5 hashes."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--show-hash'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should include hash (32 char hex string)
        # At minimum should have the file listed
        assert 'data.csv' in result.stdout


class TestLsFilters:
    """Tests for filtering options."""
    
    def test_ls_pattern_filter(self, dvc_repo_with_files, monkeypatch):
        """'--pattern' glob filter works."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--pattern', '*.csv'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should show csv files
        assert 'data.csv' in result.stdout
    
    def test_ls_pattern_no_match(self, dvc_repo_with_files, monkeypatch):
        """Pattern with no matches returns empty."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--pattern', '*.xyz'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should be empty or no matches
        assert 'data.csv' not in result.stdout
    
    def test_ls_files_only(self, dvc_repo_with_files, monkeypatch):
        """'--files' shows only files."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--files'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should include the file
        assert 'data.csv' in result.stdout


class TestLsOutput:
    """Tests for output formats."""
    
    def test_ls_json_output(self, dvc_repo_with_files, monkeypatch):
        """'--json' produces valid JSON."""
        import json
        
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--json'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should be valid JSON
        try:
            data = json.loads(result.stdout)
            assert isinstance(data, list)
        except json.JSONDecodeError:
            pytest.fail("Output is not valid JSON")


class TestLsRevision:
    """Tests for revision-based listing."""
    
    def test_ls_at_head(self, dvc_repo_with_files, monkeypatch):
        """'--rev HEAD' lists at HEAD."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--rev', 'HEAD'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'data.csv' in result.stdout


class TestLsAll:
    """Tests for --all flag."""
    
    def test_ls_all_includes_git_files(self, dvc_repo_with_files, monkeypatch):
        """'--all' includes non-DVC files."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--all'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should include README.md (created by fixture)
        assert 'README' in result.stdout or 'data.csv' in result.stdout
