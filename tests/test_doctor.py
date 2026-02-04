"""Tests for dt doctor module."""

import os
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from dt import doctor
from dt import config as cfg


class TestDiagnosticResult:
    """Tests for DiagnosticResult class."""
    
    def test_str_format_for_passed(self):
        """Passed result shows checkmark."""
        result = doctor.DiagnosticResult("test", True, "Test passed")
        output = str(result)
        
        assert "✓" in output
        assert "Test passed" in output
    
    def test_str_format_for_failed(self):
        """Failed result shows cross."""
        result = doctor.DiagnosticResult("test", False, "Test failed")
        output = str(result)
        
        assert "✗" in output
        assert "Test failed" in output
    
    def test_str_includes_help_text_on_failure(self):
        """Failed result includes help text."""
        result = doctor.DiagnosticResult(
            "test", False, "Test failed", 
            "Try this to fix"
        )
        output = str(result)
        
        assert "Try this to fix" in output
    
    def test_str_omits_help_text_on_success(self):
        """Passed result omits help text."""
        result = doctor.DiagnosticResult(
            "test", True, "Test passed",
            "This help should not appear"
        )
        output = str(result)
        
        assert "This help should not appear" not in output


class TestGetDtVersion:
    """Tests for get_dt_version function."""
    
    def test_returns_version_string(self):
        """Returns version string."""
        version = doctor.get_dt_version()
        
        # Should return some string (version or "unknown")
        assert isinstance(version, str)
        assert len(version) > 0
    
    def test_returns_unknown_on_error(self):
        """Returns 'unknown' if version cannot be determined."""
        with patch('importlib.metadata.version', side_effect=Exception("test")):
            version = doctor.get_dt_version()
        
        assert version == "unknown"


class TestCheckCommandVersion:
    """Tests for check_command_version function."""
    
    def test_with_available_command(self):
        """Returns True and version for available command."""
        # Use 'echo' which should be available on all systems
        with patch('shutil.which', return_value='/bin/echo'):
            mock_result = MagicMock()
            mock_result.stdout = 'echo (GNU coreutils) 8.32'
            mock_result.stderr = ''
            
            with patch('subprocess.run', return_value=mock_result):
                available, version = doctor.check_command_version('echo')
        
        assert available is True
        assert 'echo' in version.lower() or version != ""
    
    def test_with_missing_command(self):
        """Returns False for missing command."""
        with patch('shutil.which', return_value=None):
            available, version = doctor.check_command_version('nonexistent_command')
        
        assert available is False
        assert version == ""
    
    def test_handles_subprocess_error(self):
        """Returns True with unknown version on subprocess error."""
        with patch('shutil.which', return_value='/some/path'):
            with patch('subprocess.run', side_effect=Exception("error")):
                available, version = doctor.check_command_version('somecommand')
        
        assert available is True
        assert version == "version unknown"


class TestCheckGit:
    """Tests for check_git function."""
    
    def test_passes_when_git_installed(self):
        """Passes when git is available."""
        with patch.object(doctor, 'check_command_version', return_value=(True, 'git version 2.40.0')):
            result = doctor.check_git()
        
        assert result.passed is True
        assert 'Git installed' in result.message
    
    def test_fails_when_git_missing(self):
        """Fails when git is not available."""
        with patch.object(doctor, 'check_command_version', return_value=(False, '')):
            result = doctor.check_git()
        
        assert result.passed is False
        assert 'not found' in result.message.lower()
        assert result.help_text is not None


class TestCheckDvc:
    """Tests for check_dvc function."""
    
    def test_passes_when_dvc_installed(self):
        """Passes when DVC is available."""
        with patch.object(doctor, 'check_command_version', return_value=(True, 'dvc version 3.42.0')):
            result = doctor.check_dvc()
        
        assert result.passed is True
        assert 'DVC installed' in result.message
    
    def test_fails_when_dvc_missing(self):
        """Fails when DVC is not available."""
        with patch.object(doctor, 'check_command_version', return_value=(False, '')):
            result = doctor.check_dvc()
        
        assert result.passed is False
        assert 'not found' in result.message.lower()


class TestCheckGh:
    """Tests for check_gh function."""
    
    def test_passes_when_gh_installed(self):
        """Passes when GitHub CLI is available."""
        with patch.object(doctor, 'check_command_version', return_value=(True, 'gh version 2.40.0')):
            result = doctor.check_gh()
        
        assert result.passed is True
        assert 'GitHub CLI installed' in result.message
    
    def test_fails_when_gh_missing(self):
        """Fails when GitHub CLI is not available."""
        with patch.object(doctor, 'check_command_version', return_value=(False, '')):
            result = doctor.check_gh()
        
        assert result.passed is False
        assert 'optional' in result.help_text.lower()


class TestCheckSshKey:
    """Tests for check_ssh_key function."""
    
    def test_finds_existing_key(self, tmp_path, monkeypatch):
        """Finds existing SSH key."""
        ssh_dir = tmp_path / '.ssh'
        ssh_dir.mkdir()
        (ssh_dir / 'id_ed25519.pub').write_text('ssh-ed25519 AAAA...')
        
        monkeypatch.setattr(Path, 'home', lambda: tmp_path)
        
        result = doctor.check_ssh_key()
        
        assert result.passed is True
        assert 'SSH key found' in result.message
    
    def test_reports_missing_key(self, tmp_path, monkeypatch):
        """Reports when no SSH key is found."""
        ssh_dir = tmp_path / '.ssh'
        ssh_dir.mkdir()
        # No key files
        
        monkeypatch.setattr(Path, 'home', lambda: tmp_path)
        
        result = doctor.check_ssh_key()
        
        assert result.passed is False
        assert 'No SSH key found' in result.message


class TestCheckGithubSsh:
    """Tests for check_github_ssh function."""
    
    def test_successful_authentication(self):
        """Reports success when GitHub SSH works."""
        mock_result = MagicMock()
        mock_result.returncode = 1  # GitHub returns 1 on success
        mock_result.stdout = ''
        mock_result.stderr = 'Hi testuser! You\'ve successfully authenticated'
        
        with patch('subprocess.run', return_value=mock_result):
            result = doctor.check_github_ssh()
        
        assert result.passed is True
        assert 'testuser' in result.message or 'works' in result.message
    
    def test_failed_authentication(self):
        """Reports failure when GitHub SSH fails."""
        mock_result = MagicMock()
        mock_result.returncode = 255
        mock_result.stdout = ''
        mock_result.stderr = 'Permission denied (publickey)'
        
        with patch('subprocess.run', return_value=mock_result):
            result = doctor.check_github_ssh()
        
        assert result.passed is False
        assert 'failed' in result.message.lower() or 'error' in result.message.lower() or result.help_text is not None
    
    def test_timeout_handling(self):
        """Handles timeout gracefully."""
        import subprocess
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired('ssh', 10)):
            result = doctor.check_github_ssh()
        
        assert result.passed is False
        assert 'timeout' in result.message.lower() or result.help_text is not None


class TestCheckCacheRoot:
    """Tests for check_cache_root function."""
    
    def test_when_configured_and_accessible(self, tmp_path):
        """Reports success when cache root is configured and accessible."""
        cache_root = tmp_path / 'cache'
        cache_root.mkdir()
        
        with patch.object(cfg, 'get_value', return_value=str(cache_root)):
            result = doctor.check_cache_root()
        
        assert result.passed is True
        assert 'Cache root' in result.message or 'cache' in result.message.lower()
    
    def test_when_not_configured(self):
        """Reports failure when cache root not configured."""
        with patch.object(cfg, 'get_value', return_value=None):
            result = doctor.check_cache_root()
        
        assert result.passed is False
        assert 'not configured' in result.message.lower()
