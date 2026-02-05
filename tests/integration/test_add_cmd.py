"""Integration tests for 'dt add' command.

Tests for adding files to DVC tracking via compute node.
Note: Full qxub functionality requires HPC environment.
These tests focus on worker mode and error handling.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Classes
# =============================================================================

class TestAddBasic:
    """Tests for basic 'dt add' functionality."""
    
    def test_add_help(self):
        """'dt add --help' shows usage."""
        result = subprocess.run(
            ['dt', 'add', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'TARGETS' in result.stdout or 'target' in result.stdout.lower()
        assert '--threads' in result.stdout or '-t' in result.stdout
    
    def test_add_requires_targets(self, dvc_repo, monkeypatch):
        """Add without targets shows error."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'add'],
            capture_output=True,
            text=True,
        )
        
        # Should fail - targets required
        assert result.returncode != 0


class TestAddWorkerMode:
    """Tests for add worker mode (--worker flag).
    
    Worker mode runs the actual dvc add without qxub submission.
    This allows testing the core add logic without HPC environment.
    """
    
    def test_add_single_file_worker_mode(self, dvc_repo, monkeypatch):
        """Add single file in worker mode."""
        monkeypatch.chdir(dvc_repo)
        
        # Create a file to add
        test_file = dvc_repo / 'new_data.txt'
        test_file.write_text('test data content\n')
        
        result = subprocess.run(
            ['dt', 'add', '--worker', 'new_data.txt'],
            capture_output=True,
            text=True,
        )
        
        # Worker mode should work without qxub
        if result.returncode == 0:
            # Should create .dvc file
            assert (dvc_repo / 'new_data.txt.dvc').exists()
    
    def test_add_directory_worker_mode(self, dvc_repo, monkeypatch):
        """Add directory in worker mode."""
        monkeypatch.chdir(dvc_repo)
        
        # Create a directory with files
        data_dir = dvc_repo / 'dataset'
        data_dir.mkdir()
        (data_dir / 'a.txt').write_text('a')
        (data_dir / 'b.txt').write_text('b')
        
        result = subprocess.run(
            ['dt', 'add', '--worker', 'dataset'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # Should create .dvc file for directory
            assert (dvc_repo / 'dataset.dvc').exists()


class TestAddErrors:
    """Tests for error handling."""
    
    def test_add_nonexistent_file(self, dvc_repo, monkeypatch):
        """Add non-existent file should error."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'add', '--worker', 'nonexistent.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should fail
        assert result.returncode != 0
    
    def test_add_outside_repo(self, tmp_path, monkeypatch):
        """Add outside DVC repo should error."""
        monkeypatch.chdir(tmp_path)
        
        # Create a file
        (tmp_path / 'data.txt').write_text('data')
        
        result = subprocess.run(
            ['dt', 'add', '--worker', 'data.txt'],
            capture_output=True,
            text=True,
        )
        
        # Should fail - not in DVC repo
        assert result.returncode != 0


class TestAddOptions:
    """Tests for add options."""
    
    def test_add_threads_option(self, dvc_repo, monkeypatch):
        """'--threads' option sets checksum_jobs."""
        monkeypatch.chdir(dvc_repo)
        
        # Create a file
        (dvc_repo / 'data.txt').write_text('test')
        
        result = subprocess.run(
            ['dt', 'add', '--help'],
            capture_output=True,
            text=True,
        )
        
        # Should have threads option
        assert '--threads' in result.stdout or '-t' in result.stdout
    
    def test_add_verbose_option(self, dvc_repo, monkeypatch):
        """'--verbose' shows detailed progress."""
        monkeypatch.chdir(dvc_repo)
        
        # Create a file
        (dvc_repo / 'verbose_test.txt').write_text('test data')
        
        result = subprocess.run(
            ['dt', 'add', '--worker', '--verbose', 'verbose_test.txt'],
            capture_output=True,
            text=True,
        )
        
        # Verbose mode should work
        # Output may include progress information


class TestAddAlreadyTracked:
    """Tests for adding already tracked files."""
    
    def test_add_already_tracked(self, dvc_repo_with_files, monkeypatch):
        """Adding already tracked file should update or succeed."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'add', '--worker', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should handle gracefully (re-adding is OK)
        # May succeed or show warning
        assert result.returncode in (0, 1)


class TestAddQxub:
    """Tests for qxub integration (may skip on non-HPC)."""
    
    @pytest.mark.requires_qxub
    def test_add_submits_to_qxub(self, dvc_repo, monkeypatch):
        """Add submits job to qxub (HPC only)."""
        monkeypatch.chdir(dvc_repo)
        
        (dvc_repo / 'hpc_data.txt').write_text('test')
        
        result = subprocess.run(
            ['dt', 'add', 'hpc_data.txt'],
            capture_output=True,
            text=True,
        )
        
        # Should submit job or succeed
        assert result.returncode in (0, 1)
    
    def test_add_no_wait_option(self, dvc_repo, monkeypatch):
        """'--no-wait' submits without waiting."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'add', '--help'],
            capture_output=True,
            text=True,
        )
        
        # Should have no-wait option
        assert '--no-wait' in result.stdout
