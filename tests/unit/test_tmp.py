"""Tests for dt tmp module."""

import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from dt import tmp
from dt import config as cfg
from dt.errors import TmpError


class TestGetTmpDir:
    """Tests for get_tmp_dir function."""
    
    def test_returns_correct_path(self, tmp_path, monkeypatch):
        """Returns .dt/tmp/clones path."""
        monkeypatch.chdir(tmp_path)
        
        result = tmp.get_tmp_dir()
        
        assert result == tmp_path / '.dt' / 'tmp' / 'clones'


class TestResolveRepositoryUrl:
    """Tests for resolve_repository_url function."""
    
    def test_full_ssh_url_returned_as_is(self):
        """Full SSH URL is returned unchanged."""
        url = "git@github.com:swarbricklab/dt-test-registry.git"
        result = tmp.resolve_repository_url(url)
        
        assert result == url
    
    def test_full_https_url_returned_as_is(self):
        """Full HTTPS URL is returned unchanged."""
        url = "https://github.com/swarbricklab/dt-test-registry.git"
        result = tmp.resolve_repository_url(url)
        
        assert result == url
    
    def test_short_name_with_owner_argument(self):
        """Short name with owner argument constructs URL."""
        result = tmp.resolve_repository_url("dt-test-registry", owner="swarbricklab")
        
        assert result == "git@github.com:swarbricklab/dt-test-registry.git"
    
    def test_short_name_uses_config_owner(self):
        """Short name uses owner from config."""
        with patch.object(cfg, 'get_value', return_value="swarbricklab"):
            result = tmp.resolve_repository_url("dt-test-registry")
        
        assert result == "git@github.com:swarbricklab/dt-test-registry.git"
    
    def test_raises_error_when_owner_missing(self):
        """Raises TmpError when owner is needed but not configured."""
        with patch.object(cfg, 'get_value', return_value=None):
            with pytest.raises(TmpError, match="no owner is configured"):
                tmp.resolve_repository_url("myrepo")


class TestGetRepoId:
    """Tests for get_repo_id function."""
    
    def test_ssh_format(self):
        """Converts SSH URL to repo ID."""
        url = "git@github.com:swarbricklab/dt-test-registry.git"
        result = tmp.get_repo_id(url)
        
        assert result == "github.com/swarbricklab/dt-test-registry"
    
    def test_https_format(self):
        """Converts HTTPS URL to repo ID."""
        url = "https://github.com/swarbricklab/dt-test-registry.git"
        result = tmp.get_repo_id(url)
        
        assert result == "github.com/swarbricklab/dt-test-registry"
    
    def test_short_name_with_owner(self):
        """Converts short name with owner to repo ID."""
        result = tmp.get_repo_id("dt-test-registry", owner="swarbricklab")
        
        assert result == "github.com/swarbricklab/dt-test-registry"
    
    def test_removes_git_suffix(self):
        """Removes .git suffix from repo ID."""
        url = "git@github.com:org/repo.git"
        result = tmp.get_repo_id(url)
        
        assert not result.endswith('.git')
        assert result == "github.com/org/repo"


class TestEnsureGitignore:
    """Tests for ensure_gitignore function."""
    
    def test_adds_pattern(self, tmp_path, monkeypatch):
        """Adds .dt/tmp/ to .gitignore."""
        monkeypatch.chdir(tmp_path)
        
        result = tmp.ensure_gitignore()
        
        assert result is True
        assert (tmp_path / '.gitignore').exists()
        content = (tmp_path / '.gitignore').read_text()
        assert '.dt/tmp/' in content
    
    def test_returns_false_for_existing_pattern(self, tmp_path, monkeypatch):
        """Returns False if pattern already exists."""
        monkeypatch.chdir(tmp_path)
        
        # Create .gitignore with pattern already
        (tmp_path / '.gitignore').write_text('.dt/tmp/\n')
        
        result = tmp.ensure_gitignore()
        
        assert result is False


class TestCloneRepo:
    """Tests for clone_repo function."""
    
    def test_creates_sparse_clone(self, tmp_path, monkeypatch):
        """Creates sparse clone with correct git commands."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').touch()
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ''
        
        calls = []
        def capture_run(cmd, **kwargs):
            calls.append(cmd)
            return mock_result
        
        with patch('subprocess.run', side_effect=capture_run):
            with patch('dt.tmp.utils.check_git'):
                result = tmp.clone_repo(
                    'git@github.com:swarbricklab/dt-test-registry.git',
                    verbose=False
                )
        
        assert result == tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'swarbricklab' / 'dt-test-registry'
        
        # Verify git clone was called with --no-checkout
        clone_cmd = calls[0]
        assert clone_cmd[0:2] == ['git', 'clone']
        assert '--no-checkout' in clone_cmd
        assert '--depth' in clone_cmd
    
    def test_refreshes_existing_clone(self, tmp_path, monkeypatch):
        """Refreshes existing clone when refresh=True."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').touch()
        
        # Create existing clone directory
        repo_path = tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        repo_path.mkdir(parents=True)
        (repo_path / '.git').mkdir()
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ''
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            with patch('dt.tmp.utils.check_git'):
                result = tmp.clone_repo(
                    'git@github.com:org/repo.git',
                    refresh=True,
                    verbose=False
                )
        
        assert result == repo_path
    
    def test_uses_cached_clone(self, tmp_path, monkeypatch):
        """Uses cached clone when refresh=False."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').touch()
        
        # Create existing clone directory
        repo_path = tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        repo_path.mkdir(parents=True)
        
        with patch('subprocess.run') as mock_run:
            with patch('dt.tmp.utils.check_git'):
                result = tmp.clone_repo(
                    'git@github.com:org/repo.git',
                    refresh=False,
                    verbose=False
                )
        
        assert result == repo_path
        # Should not call subprocess.run (no refresh, no clone)
        mock_run.assert_not_called()
    
    def test_raises_error_on_clone_failure(self, tmp_path, monkeypatch):
        """Raises TmpError when git clone fails."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').touch()
        
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = 'clone failed'
        
        with patch('subprocess.run', return_value=mock_result):
            with patch('dt.tmp.utils.check_git'):
                with pytest.raises(TmpError, match="Failed to clone"):
                    tmp.clone_repo(
                        'git@github.com:org/repo.git',
                        verbose=False
                    )
