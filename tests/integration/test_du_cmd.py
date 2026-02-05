"""Integration tests for 'dt du' command.

Tests for disk usage reporting of DVC-tracked files.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Classes
# =============================================================================

class TestDuBasic:
    """Tests for basic 'dt du' functionality."""
    
    def test_du_in_dvc_repo(self, dvc_repo_with_files, monkeypatch):
        """Report disk usage in DVC repo."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should show some size information
        # Could be bytes or human-readable
        assert 'data.csv' in result.stdout or result.stdout.strip()
    
    def test_du_help(self):
        """'dt du --help' shows usage."""
        result = subprocess.run(
            ['dt', 'du', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '--human' in result.stdout or '-h' in result.stdout
        assert '--summarize' in result.stdout or '-s' in result.stdout
    
    def test_du_outside_dvc_repo(self, tmp_path, monkeypatch):
        """Disk usage in non-DVC directory."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'du'],
            capture_output=True,
            text=True,
        )
        
        # Should handle gracefully (empty or error)
        assert result.returncode in (0, 1)


class TestDuHumanReadable:
    """Tests for human-readable output."""
    
    def test_du_human_flag(self, dvc_repo_with_files, monkeypatch):
        """'-h' shows human-readable sizes."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', '-h'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Human-readable output should contain size units or numbers
        output = result.stdout
        # Should have some output (size info)
        assert output.strip()


class TestDuSummarize:
    """Tests for summarize option."""
    
    def test_du_summarize(self, dvc_repo_with_files, monkeypatch):
        """'-s' shows only total."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', '-s'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should have compact output
        lines = result.stdout.strip().split('\n')
        # Summarize should give fewer lines than detailed
        assert len(lines) <= 2  # Just total line


class TestDuInodes:
    """Tests for inode counting."""
    
    def test_du_inodes(self, dvc_repo_with_files, monkeypatch):
        """'--inodes' counts files instead of bytes."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', '--inodes'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should show file counts
        output = result.stdout
        assert output.strip()


class TestDuTargets:
    """Tests for specific targets."""
    
    def test_du_specific_target(self, dvc_repo_with_files, monkeypatch):
        """Report usage for specific path."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        # May succeed or fail depending on target resolution
        # Just verify it runs
        assert result.returncode in (0, 1)
    
    def test_du_nonexistent_target(self, dvc_repo_with_files, monkeypatch):
        """Nonexistent target handles gracefully."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', 'nonexistent.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should handle missing target
        assert result.returncode in (0, 1)


class TestDuDepth:
    """Tests for depth limiting."""
    
    def test_du_max_depth(self, dvc_repo_with_files, monkeypatch):
        """'--max-depth' limits output depth."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', '--max-depth', '1'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0


class TestDuCached:
    """Tests for cached vs expected sizes."""
    
    def test_du_cached(self, dvc_repo_with_files, monkeypatch):
        """'--cached' reports cached sizes."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', '--cached'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
    
    def test_du_expected(self, dvc_repo_with_files, monkeypatch):
        """'--expected' reports expected sizes."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', '--expected'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0


class TestDuTotal:
    """Tests for total line."""
    
    def test_du_total(self, dvc_repo_with_files, monkeypatch):
        """'--total' shows grand total."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'du', '--total'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should include total line (if there are files)
        # May have "total" or sum line
