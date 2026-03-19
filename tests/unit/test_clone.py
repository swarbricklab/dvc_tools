"""Tests for dt clone module."""

import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from dt import clone
from dt import config as cfg
from dt.errors import CloneError


class TestResolveRepositoryUrl:
    """Tests for resolve_repository_url function."""
    
    def test_full_url_returned_as_is(self):
        """Full URL is returned unchanged."""
        url = "git@github.com:swarbricklab/dvc_tools.git"
        result = clone.resolve_repository_url(url)
        
        assert result == url
    
    def test_https_url_returned_as_is(self):
        """HTTPS URL is returned unchanged."""
        url = "https://github.com/swarbricklab/dvc_tools.git"
        result = clone.resolve_repository_url(url)
        
        assert result == url
    
    def test_short_name_with_owner_argument(self):
        """Short name with owner argument constructs URL."""
        result = clone.resolve_repository_url("myrepo", owner="myorg")
        
        assert result == "git@github.com:myorg/myrepo.git"
    
    def test_short_name_with_config_owner(self):
        """Short name uses owner from config."""
        with patch.object(cfg, 'get_value', return_value="configowner"):
            result = clone.resolve_repository_url("myrepo")
        
        assert result == "git@github.com:configowner/myrepo.git"
    
    def test_raises_error_when_owner_missing(self):
        """Raises CloneError when owner is needed but not configured."""
        with patch.object(cfg, 'get_value', return_value=None):
            with pytest.raises(CloneError, match="no owner is configured"):
                clone.resolve_repository_url("myrepo")


class TestExtractRepoName:
    """Tests for extract_repo_name function."""
    
    def test_ssh_url(self):
        """Extracts name from SSH URL."""
        url = "git@github.com:swarbricklab/dvc_tools.git"
        result = clone.extract_repo_name(url)
        
        assert result == "dvc_tools"
    
    def test_https_url(self):
        """Extracts name from HTTPS URL."""
        url = "https://github.com/swarbricklab/dvc_tools.git"
        result = clone.extract_repo_name(url)
        
        assert result == "dvc_tools"
    
    def test_removes_git_suffix(self):
        """Removes .git suffix from name."""
        url = "git@github.com:org/repo.git"
        result = clone.extract_repo_name(url)
        
        assert result == "repo"
    
    def test_handles_trailing_slash(self):
        """Handles trailing slash in URL."""
        url = "https://github.com/org/repo/"
        result = clone.extract_repo_name(url)
        
        assert result == "repo"
    
    def test_url_without_git_suffix(self):
        """Handles URL without .git suffix."""
        url = "https://github.com/org/repo"
        result = clone.extract_repo_name(url)
        
        assert result == "repo"


class TestCloneRepository:
    """Tests for clone_repository function."""
    
    def test_basic_clone(self, tmp_path, monkeypatch):
        """Basic clone runs git clone command."""
        monkeypatch.chdir(tmp_path)
        
        mock_subprocess = MagicMock()
        mock_subprocess.returncode = 0
        
        with patch('subprocess.run', return_value=mock_subprocess) as mock_run:
            with patch.object(cfg, 'get_value', return_value='testowner'):
                with patch('dt.clone.cache_mod.init_cache'):
                    result = clone.clone_repository(
                        'git@github.com:org/testrepo.git',
                        verbose=False,
                        no_auth=True,
                    )
        
        # Check git clone was called
        first_call = mock_run.call_args_list[0]
        cmd = first_call[0][0]
        assert cmd[0:2] == ['git', 'clone']
        assert 'git@github.com:org/testrepo.git' in cmd
    
    def test_clone_with_shallow(self, tmp_path, monkeypatch):
        """Shallow clone includes --depth 1."""
        monkeypatch.chdir(tmp_path)
        
        mock_subprocess = MagicMock()
        mock_subprocess.returncode = 0
        
        with patch('subprocess.run', return_value=mock_subprocess) as mock_run:
            with patch.object(cfg, 'get_value', return_value='testowner'):
                with patch('dt.clone.cache_mod.init_cache'):
                    clone.clone_repository(
                        'git@github.com:org/testrepo.git',
                        shallow=True,
                        verbose=False,
                        no_auth=True,
                    )
        
        first_call = mock_run.call_args_list[0]
        cmd = first_call[0][0]
        assert '--depth' in cmd
        assert '1' in cmd
    
    def test_clone_with_no_submodules(self, tmp_path, monkeypatch):
        """no_submodules skips submodule commands."""
        monkeypatch.chdir(tmp_path)
        
        mock_subprocess = MagicMock()
        mock_subprocess.returncode = 0
        
        with patch('subprocess.run', return_value=mock_subprocess) as mock_run:
            with patch.object(cfg, 'get_value', return_value='testowner'):
                with patch('dt.clone.cache_mod.init_cache'):
                    clone.clone_repository(
                        'git@github.com:org/testrepo.git',
                        no_submodules=True,
                        verbose=False,
                        no_auth=True,
                    )
        
        # Should not have --recurse-submodules
        first_call = mock_run.call_args_list[0]
        cmd = first_call[0][0]
        assert '--recurse-submodules' not in cmd
    
    def test_clone_failure_raises_error(self, tmp_path, monkeypatch):
        """Clone failure raises CloneError."""
        monkeypatch.chdir(tmp_path)
        
        mock_subprocess = MagicMock()
        mock_subprocess.returncode = 1
        
        with patch('subprocess.run', return_value=mock_subprocess):
            with patch.object(cfg, 'get_value', return_value='testowner'):
                with pytest.raises(CloneError, match="Git clone failed"):
                    clone.clone_repository(
                        'git@github.com:org/testrepo.git',
                        verbose=False,
                        no_auth=True,
                    )
    
    def test_custom_path(self, tmp_path, monkeypatch):
        """Clone to custom path."""
        monkeypatch.chdir(tmp_path)
        
        mock_subprocess = MagicMock()
        mock_subprocess.returncode = 0
        
        with patch('subprocess.run', return_value=mock_subprocess) as mock_run:
            with patch.object(cfg, 'get_value', return_value='testowner'):
                with patch('dt.clone.cache_mod.init_cache'):
                    result = clone.clone_repository(
                        'git@github.com:org/testrepo.git',
                        path='custom_dir',
                        verbose=False,
                        no_auth=True,
                    )
        
        assert result == Path('custom_dir')
        
        # Check custom path used in git clone
        first_call = mock_run.call_args_list[0]
        cmd = first_call[0][0]
        assert 'custom_dir' in cmd


class TestCloneAuthSetup:
    """Tests for auth setup integration in clone_repository."""

    def test_auth_setup_runs_by_default(self, tmp_path, monkeypatch):
        """auth_setup is called when no_auth is False (default)."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'testrepo').mkdir()

        mock_subprocess = MagicMock()
        mock_subprocess.returncode = 0

        mock_report = MagicMock()
        mock_report.ssh_results = []
        mock_report.credentials_installed = {}
        mock_report.skipped_ssh = True
        mock_report.skipped_credentials = True
        mock_report.errors = []

        with patch('subprocess.run', return_value=mock_subprocess):
            with patch.object(cfg, 'get_value', return_value='testowner'):
                with patch('dt.clone.cache_mod.init_cache'):
                    with patch('dt.clone.auth_setup_mod.auth_setup', return_value=mock_report) as mock_auth:
                        clone.clone_repository(
                            'git@github.com:org/testrepo.git',
                            verbose=False,
                        )

        mock_auth.assert_called_once_with(verbose=False)

    def test_no_auth_skips_setup(self, tmp_path, monkeypatch):
        """auth_setup is not called when no_auth is True."""
        monkeypatch.chdir(tmp_path)

        mock_subprocess = MagicMock()
        mock_subprocess.returncode = 0

        with patch('subprocess.run', return_value=mock_subprocess):
            with patch.object(cfg, 'get_value', return_value='testowner'):
                with patch('dt.clone.cache_mod.init_cache'):
                    with patch('dt.clone.auth_setup_mod.auth_setup') as mock_auth:
                        clone.clone_repository(
                            'git@github.com:org/testrepo.git',
                            no_auth=True,
                            verbose=False,
                        )

        mock_auth.assert_not_called()

    def test_auth_setup_failure_does_not_abort_clone(self, tmp_path, monkeypatch):
        """Clone succeeds even if auth_setup raises an exception."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'testrepo').mkdir()

        mock_subprocess = MagicMock()
        mock_subprocess.returncode = 0

        with patch('subprocess.run', return_value=mock_subprocess):
            with patch.object(cfg, 'get_value', return_value='testowner'):
                with patch('dt.clone.cache_mod.init_cache'):
                    with patch('dt.clone.auth_setup_mod.auth_setup', side_effect=RuntimeError('gcp error')):
                        result = clone.clone_repository(
                            'git@github.com:org/testrepo.git',
                            verbose=False,
                        )

        assert result == Path('testrepo')
