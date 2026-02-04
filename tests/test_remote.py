"""Unit tests for dt.remote module.

Tests remote storage management functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.remote import (
    resolve_remote_path,
    init_remote_structure,
    configure_dvc_remote,
    init_remote,
    list_remotes,
)
from dt.errors import RemoteError


# =============================================================================
# resolve_remote_path tests
# =============================================================================

class TestResolveRemotePath:
    """Tests for the resolve_remote_path function."""

    def test_uses_remote_path_when_provided(self):
        """Test uses remote_path directly when provided."""
        result = resolve_remote_path(remote_path="/explicit/path")
        
        assert result == Path("/explicit/path")

    def test_constructs_path_from_root_and_name(self):
        """Test constructs path from remote_root and name."""
        with patch("dt.remote.cfg.get_value", return_value=None):
            result = resolve_remote_path(
                name="myproject",
                remote_root="/remote/storage",
            )
            
            assert result == Path("/remote/storage/myproject")

    def test_uses_config_remote_root(self):
        """Test uses remote.root from config."""
        with patch("dt.remote.cfg.get_value", return_value="/config/remote"):
            with patch("dt.remote.utils.get_project_name", return_value="project"):
                result = resolve_remote_path()
                
                assert result == Path("/config/remote/project")

    def test_raises_error_when_no_root_configured(self):
        """Test RemoteError raised when remote root not configured."""
        with patch("dt.remote.cfg.get_value", return_value=None):
            with pytest.raises(RemoteError, match="Remote root not configured"):
                resolve_remote_path(name="project")


# =============================================================================
# init_remote_structure tests
# =============================================================================

class TestInitRemoteStructure:
    """Tests for the init_remote_structure function."""

    def test_creates_remote_directory(self, tmp_path):
        """Test creates remote directory if not exists."""
        remote_dir = tmp_path / "remote"
        
        with patch("dt.remote.utils.set_group_writable"):
            with patch("dt.remote.utils.create_md5_subdirs"):
                init_remote_structure(remote_dir, verbose=False)
                
                assert remote_dir.exists()

    def test_calls_set_group_writable(self, tmp_path):
        """Test calls set_group_writable on remote dir."""
        remote_dir = tmp_path / "remote"
        
        with patch("dt.remote.utils.set_group_writable") as mock_sgw:
            with patch("dt.remote.utils.create_md5_subdirs"):
                init_remote_structure(remote_dir, verbose=False)
                
                mock_sgw.assert_called_once_with(remote_dir)

    def test_creates_md5_subdirs(self, tmp_path):
        """Test creates files/md5 subdirectory structure."""
        remote_dir = tmp_path / "remote"
        
        with patch("dt.remote.utils.set_group_writable"):
            with patch("dt.remote.utils.create_md5_subdirs") as mock_create:
                init_remote_structure(remote_dir, verbose=False)
                
                mock_create.assert_called_once()


# =============================================================================
# configure_dvc_remote tests
# =============================================================================

class TestConfigureDvcRemote:
    """Tests for the configure_dvc_remote function."""

    def test_adds_ssh_remote_when_host_configured(self, tmp_path):
        """Test adds SSH remote when ssh.host is configured."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = Path("/remote/storage/project")
        
        with patch("dt.remote.cfg.get_value", return_value="hostname.example.com"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stderr="")
                
                configure_dvc_remote(repo_path, remote_dir, verbose=False)
                
                # Should have called dvc remote add twice (SSH and local)
                assert mock_run.call_count == 2
                
                # First call should be SSH remote
                first_call = mock_run.call_args_list[0][0][0]
                assert "ssh://hostname.example.com" in " ".join(first_call)

    def test_adds_local_remote(self, tmp_path):
        """Test always adds local remote."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = Path("/remote/storage/project")
        
        with patch("dt.remote.cfg.get_value", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stderr="")
                
                configure_dvc_remote(repo_path, remote_dir, verbose=False)
                
                call_args = mock_run.call_args[0][0]
                assert "--local" in call_args
                assert "local" in call_args

    def test_ignores_already_exists_error(self, tmp_path):
        """Test ignores 'already exists' error."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = Path("/remote/storage/project")
        
        with patch("dt.remote.cfg.get_value", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=1,
                    stderr="remote 'local' already exists",
                )
                
                # Should not raise
                configure_dvc_remote(repo_path, remote_dir, verbose=False)


# =============================================================================
# init_remote tests
# =============================================================================

class TestInitRemote:
    """Tests for the init_remote function."""

    def test_checks_dvc_dependency(self, tmp_path):
        """Test checks DVC dependency first."""
        from dt import utils
        
        with patch("dt.remote.utils.check_dvc") as mock_check:
            mock_check.side_effect = utils.DependencyError("dvc not found")
            
            with pytest.raises(RemoteError, match="dvc not found"):
                init_remote(repo_path=tmp_path)

    def test_creates_remote_if_not_exists(self, tmp_path):
        """Test creates remote directory if not exists."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = tmp_path / "remote"
        
        with patch("dt.remote.utils.check_dvc"):
            with patch("dt.remote.resolve_remote_path", return_value=remote_dir):
                with patch("dt.remote.init_remote_structure") as mock_init:
                    with patch("dt.remote.configure_dvc_remote"):
                        init_remote(repo_path=repo_path, verbose=False)
                        
                        mock_init.assert_called_once()

    def test_skips_creation_if_exists(self, tmp_path):
        """Test skips creation if remote directory exists."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        
        with patch("dt.remote.utils.check_dvc"):
            with patch("dt.remote.resolve_remote_path", return_value=remote_dir):
                with patch("dt.remote.init_remote_structure") as mock_init:
                    with patch("dt.remote.configure_dvc_remote"):
                        init_remote(repo_path=repo_path, verbose=False)
                        
                        mock_init.assert_not_called()

    def test_returns_remote_path(self, tmp_path):
        """Test returns the remote directory path."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        
        with patch("dt.remote.utils.check_dvc"):
            with patch("dt.remote.resolve_remote_path", return_value=remote_dir):
                with patch("dt.remote.configure_dvc_remote"):
                    result = init_remote(repo_path=repo_path, verbose=False)
                    
                    assert result == remote_dir


# =============================================================================
# list_remotes tests
# =============================================================================

class TestListRemotes:
    """Tests for the list_remotes function."""

    def test_returns_empty_list_when_no_remotes(self, tmp_path):
        """Test returns empty list when no remotes configured."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="",
            )
            
            result = list_remotes(tmp_path)
            
            assert result == []

    def test_parses_remote_list_output(self, tmp_path):
        """Test parses dvc remote list output."""
        with patch("subprocess.run") as mock_run:
            # First call: dvc remote list
            # Second call: dvc config core.remote
            mock_run.side_effect = [
                MagicMock(
                    returncode=0,
                    stdout="origin\tssh://host/path\nlocal\t/local/path\n",
                ),
                MagicMock(
                    returncode=0,
                    stdout="origin\n",
                ),
            ]
            
            result = list_remotes(tmp_path)
            
            assert len(result) == 2
            # Check first remote
            assert result[0][0] == "origin"
            assert "ssh://host/path" in result[0][1]

    def test_identifies_default_remote(self, tmp_path):
        """Test correctly identifies default remote."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(
                    returncode=0,
                    stdout="origin\tssh://host/path\nlocal\t/local/path\n",
                ),
                MagicMock(
                    returncode=0,
                    stdout="origin\n",
                ),
            ]
            
            result = list_remotes(tmp_path)
            
            # origin should be default
            origin = [r for r in result if r[0] == "origin"][0]
            assert origin[2] is True  # is_default
            
            # local should not be default
            local = [r for r in result if r[0] == "local"][0]
            assert local[2] is False

    def test_uses_cwd_when_no_path_provided(self):
        """Test uses current directory when no path provided."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="")
            
            with patch("pathlib.Path.cwd", return_value=Path("/current/dir")):
                list_remotes()
                
                call_kwargs = mock_run.call_args[1]
                assert call_kwargs.get("cwd") == Path("/current/dir")
