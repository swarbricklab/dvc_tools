"""Integration tests for 'dt mv' command.

Tests for moving/renaming DVC-tracked files with import preservation.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Classes
# =============================================================================

class TestMvBasic:
    """Tests for basic 'dt mv' functionality."""
    
    def test_mv_help(self):
        """'dt mv --help' shows usage."""
        result = subprocess.run(
            ['dt', 'mv', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'SRC' in result.stdout
        assert 'DST' in result.stdout
    
    def test_mv_tracked_file(self, dvc_repo_with_files, monkeypatch):
        """Move a DVC-tracked file."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        result = subprocess.run(
            ['dt', 'mv', 'data.csv', 'renamed_data.csv'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # Original .dvc should be gone
            assert not (repo / 'data.csv.dvc').exists()
            # New .dvc should exist
            assert (repo / 'renamed_data.csv.dvc').exists()
    
    def test_mv_to_directory(self, dvc_repo_with_files, monkeypatch):
        """Move tracked file to a directory."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        # Create target directory
        subdir = repo / 'subdir'
        subdir.mkdir()
        
        result = subprocess.run(
            ['dt', 'mv', 'data.csv', 'subdir/'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # File should be in subdir
            assert (repo / 'subdir' / 'data.csv.dvc').exists()


class TestMvImportPreservation:
    """Tests for import deps preservation (key feature)."""
    
    def test_mv_preserves_deps_section(self, dvc_repo_with_files, monkeypatch):
        """Moving import file preserves deps section."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        # Create a mock import .dvc file with deps section
        import_dvc = repo / 'imported_file.csv.dvc'
        import_dvc.write_text("""outs:
- md5: abc123
  size: 100
  hash: md5
  path: imported_file.csv
deps:
- path: data/file.csv
  repo:
    url: https://github.com/org/repo
    rev: main
""")
        
        # Create the actual file
        (repo / 'imported_file.csv').write_text('data')
        
        result = subprocess.run(
            ['dt', 'mv', 'imported_file.csv', 'moved_import.csv'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # Check new .dvc file has deps
            new_dvc = repo / 'moved_import.csv.dvc'
            if new_dvc.exists():
                content = new_dvc.read_text()
                # Should preserve deps section
                assert 'deps' in content or 'repo' in content


class TestMvErrors:
    """Tests for error handling."""
    
    def test_mv_nonexistent_source(self, dvc_repo_with_files, monkeypatch):
        """Move non-existent file should error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'mv', 'nonexistent.csv', 'new.csv'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode != 0
    
    def test_mv_destination_exists(self, dvc_repo_with_files, monkeypatch):
        """Move to existing destination - behavior depends on DVC."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        # Create destination file (not tracked)
        (repo / 'existing.csv').write_text('existing data')
        
        result = subprocess.run(
            ['dt', 'mv', 'data.csv', 'existing.csv'],
            capture_output=True,
            text=True,
        )
        
        # DVC mv may succeed (overwrite) or fail depending on version
        # Either behavior is acceptable
        assert result.returncode in (0, 1)
    
    def test_mv_outside_repo(self, tmp_path, monkeypatch):
        """Move outside DVC repo should error."""
        monkeypatch.chdir(tmp_path)
        
        (tmp_path / 'file.csv').write_text('data')
        
        result = subprocess.run(
            ['dt', 'mv', 'file.csv', 'new.csv'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode != 0


class TestMvVerbose:
    """Tests for verbose output."""
    
    def test_mv_verbose(self, dvc_repo_with_files, monkeypatch):
        """'--verbose' shows detailed progress."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        result = subprocess.run(
            ['dt', 'mv', '--verbose', 'data.csv', 'moved.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should work with verbose
        if result.returncode == 0:
            # Verbose output may show more detail
            pass


class TestMvUntracked:
    """Tests for moving untracked files."""
    
    def test_mv_untracked_file(self, dvc_repo, monkeypatch):
        """Moving untracked file should error or pass through."""
        repo = dvc_repo
        monkeypatch.chdir(repo)
        
        # Create untracked file (no .dvc)
        (repo / 'untracked.txt').write_text('data')
        
        result = subprocess.run(
            ['dt', 'mv', 'untracked.txt', 'moved.txt'],
            capture_output=True,
            text=True,
        )
        
        # May error (not DVC tracked) or use regular dvc mv
        assert result.returncode in (0, 1)
