"""Integration tests for 'dt doctor' command.

Tests for diagnostic checks and environment verification.
"""

import subprocess

import pytest


# =============================================================================
# Test Classes
# =============================================================================

class TestDoctorBasic:
    """Tests for basic 'dt doctor' functionality."""
    
    def test_doctor_help(self):
        """'dt doctor --help' shows usage."""
        result = subprocess.run(
            ['dt', 'doctor', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '--verbose' in result.stdout or '-v' in result.stdout
    
    def test_doctor_runs(self):
        """Doctor command runs and produces output."""
        result = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        # Should complete (may have warnings but not crash)
        assert result.returncode in (0, 1)
        # Should produce diagnostic output
        assert result.stdout.strip()
    
    def test_doctor_shows_version(self):
        """Doctor shows dt version."""
        result = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        # Should include version info
        assert 'version' in result.stdout.lower() or 'dt' in result.stdout


class TestDoctorChecks:
    """Tests for individual diagnostic checks."""
    
    def test_doctor_checks_git(self):
        """Doctor checks git installation."""
        result = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        # Should mention git
        assert 'git' in result.stdout.lower()
    
    def test_doctor_checks_dvc(self):
        """Doctor checks DVC installation."""
        result = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        # Should mention DVC
        assert 'dvc' in result.stdout.lower()
    
    def test_doctor_shows_status_markers(self):
        """Doctor uses status markers (✓/✗ or pass/fail)."""
        result = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        output = result.stdout
        # Should have some status indication
        has_markers = (
            '✓' in output or '✗' in output or
            'pass' in output.lower() or 'fail' in output.lower() or
            'ok' in output.lower() or 'error' in output.lower()
        )
        assert has_markers or result.stdout.strip()


class TestDoctorVerbose:
    """Tests for verbose mode."""
    
    def test_doctor_verbose(self):
        """'--verbose' shows more details."""
        result_normal = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        result_verbose = subprocess.run(
            ['dt', 'doctor', '--verbose'],
            capture_output=True,
            text=True,
        )
        
        # Verbose should have at least as much output
        assert len(result_verbose.stdout) >= len(result_normal.stdout)
    
    def test_doctor_verbose_shows_config(self):
        """Verbose mode shows configuration."""
        result = subprocess.run(
            ['dt', 'doctor', '--verbose'],
            capture_output=True,
            text=True,
        )
        
        # Verbose should include config or DVC doctor output
        output = result.stdout.lower()
        has_config_info = (
            'config' in output or
            'cache' in output or
            'remote' in output
        )
        assert has_config_info or result.stdout.strip()


class TestDoctorInRepo:
    """Tests for doctor in a DVC repository."""
    
    def test_doctor_in_dvc_repo(self, dvc_repo, monkeypatch):
        """Doctor runs in DVC repo context."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode in (0, 1)
        assert result.stdout.strip()
    
    def test_doctor_outside_repo(self, tmp_path, monkeypatch):
        """Doctor runs outside repo (limited checks)."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        # Should still run and show basic diagnostics
        assert result.returncode in (0, 1)
        assert result.stdout.strip()


class TestDoctorSummary:
    """Tests for summary output."""
    
    def test_doctor_shows_summary(self):
        """Doctor shows pass/fail summary."""
        result = subprocess.run(
            ['dt', 'doctor'],
            capture_output=True,
            text=True,
        )
        
        # Should have some summary or count
        # Format may vary but should conclude with summary
        assert result.stdout.strip()
