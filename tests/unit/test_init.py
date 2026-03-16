"""Unit tests for dt.init module.

Tests project initialization functions.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from dt.init import (
    check_dependencies,
    init_git,
    init_dvc,
    check_github_remote,
    install_dvc_hooks,
    get_dvc_autostage,
    init_dt_directory,
    init_project,
)
from dt.errors import InitError


# =============================================================================
# check_dependencies tests
# =============================================================================

class TestCheckDependencies:
    """Tests for the check_dependencies function."""

    def test_checks_both_by_default(self):
        """Test that both git and dvc are checked by default."""
        with patch("dt.init.utils.check_git") as mock_git:
            with patch("dt.init.utils.check_dvc") as mock_dvc:
                check_dependencies()
                
                mock_git.assert_called_once()
                mock_dvc.assert_called_once()

    def test_skips_git_check_when_require_git_false(self):
        """Test git check is skipped when require_git=False."""
        with patch("dt.init.utils.check_git") as mock_git:
            with patch("dt.init.utils.check_dvc") as mock_dvc:
                check_dependencies(require_git=False)
                
                mock_git.assert_not_called()
                mock_dvc.assert_called_once()

    def test_skips_dvc_check_when_require_dvc_false(self):
        """Test dvc check is skipped when require_dvc=False."""
        with patch("dt.init.utils.check_git") as mock_git:
            with patch("dt.init.utils.check_dvc") as mock_dvc:
                check_dependencies(require_dvc=False)
                
                mock_git.assert_called_once()
                mock_dvc.assert_not_called()

    def test_raises_init_error_when_git_missing(self):
        """Test InitError raised when git is not found."""
        from dt import utils
        
        with patch("dt.init.utils.check_git") as mock_git:
            mock_git.side_effect = utils.DependencyError("git not found")
            
            with pytest.raises(InitError, match="git not found"):
                check_dependencies()

    def test_raises_init_error_when_dvc_missing(self):
        """Test InitError raised when dvc is not found."""
        from dt import utils
        
        with patch("dt.init.utils.check_git"):
            with patch("dt.init.utils.check_dvc") as mock_dvc:
                mock_dvc.side_effect = utils.DependencyError("dvc not found")
                
                with pytest.raises(InitError, match="dvc not found"):
                    check_dependencies()


# =============================================================================
# init_git tests
# =============================================================================

class TestInitGit:
    """Tests for the init_git function."""

    def test_returns_false_when_git_exists(self, tmp_path):
        """Test returns False when .git already exists."""
        (tmp_path / ".git").mkdir()
        
        result = init_git(tmp_path, verbose=False)
        
        assert result is False

    def test_runs_git_init_when_not_exists(self, tmp_path):
        """Test runs git init when .git doesn't exist."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            
            result = init_git(tmp_path, verbose=False)
            
            assert result is True
            mock_run.assert_called_once()
            call_args = mock_run.call_args
            assert call_args[0][0] == ["git", "init"]

    def test_raises_init_error_on_failure(self, tmp_path):
        """Test InitError raised when git init fails."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="failed")
            
            with pytest.raises(InitError, match="Failed to initialize git"):
                init_git(tmp_path, verbose=False)


# =============================================================================
# init_dvc tests
# =============================================================================

class TestInitDvc:
    """Tests for the init_dvc function."""

    def test_returns_false_when_dvc_exists(self, tmp_path):
        """Test returns False when .dvc already exists."""
        (tmp_path / ".dvc").mkdir()
        
        result = init_dvc(tmp_path, verbose=False)
        
        assert result is False

    def test_runs_dvc_init_when_not_exists(self, tmp_path):
        """Test runs dvc init when .dvc doesn't exist."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            
            result = init_dvc(tmp_path, verbose=False)
            
            assert result is True
            mock_run.assert_called_once()
            call_args = mock_run.call_args
            assert call_args[0][0] == ["dvc", "init"]

    def test_raises_init_error_on_failure(self, tmp_path):
        """Test InitError raised when dvc init fails."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="failed")
            
            with pytest.raises(InitError, match="Failed to initialize DVC"):
                init_dvc(tmp_path, verbose=False)


# =============================================================================
# check_github_remote tests
# =============================================================================

class TestCheckGithubRemote:
    """Tests for the check_github_remote function."""

    def test_returns_true_when_remote_exists(self, tmp_path):
        """Test returns True when origin remote exists."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="git@github.com:user/repo.git",
            )
            
            result = check_github_remote(tmp_path, "project", verbose=False)
            
            assert result is True

    def test_returns_false_when_no_remote(self, tmp_path):
        """Test returns False when no origin remote."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stderr="No such remote",
            )
            
            result = check_github_remote(tmp_path, "project", verbose=False)
            
            assert result is False


# =============================================================================
# install_dvc_hooks tests
# =============================================================================

class TestInstallDvcHooks:
    """Tests for the install_dvc_hooks function."""

    def test_runs_dt_install(self, tmp_path):
        """Test that dt install is run."""
        with patch("dt.install.install") as mock_install:
            install_dvc_hooks(tmp_path, verbose=False)
            
            mock_install.assert_called_once_with(tmp_path, verbose=False)


# =============================================================================
# get_dvc_autostage tests
# =============================================================================

class TestGetDvcAutostage:
    """Tests for the get_dvc_autostage function."""

    def test_returns_true_when_autostage_enabled(self, tmp_path):
        """Test returns True when core.autostage is true."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="true\n",
            )
            
            result = get_dvc_autostage(tmp_path)
            
            assert result is True

    def test_returns_false_when_autostage_disabled(self, tmp_path):
        """Test returns False when core.autostage is false."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="false\n",
            )
            
            result = get_dvc_autostage(tmp_path)
            
            assert result is False

    def test_returns_false_when_not_configured(self, tmp_path):
        """Test returns False when config not set."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            
            result = get_dvc_autostage(tmp_path)
            
            assert result is False


# =============================================================================
# init_dt_directory tests
# =============================================================================

class TestInitDtDirectory:
    """Tests for the init_dt_directory function."""

    def test_creates_dt_directory(self, tmp_path):
        """Test that .dt directory is created."""
        with patch("dt.init.get_dvc_autostage", return_value=False):
            result = init_dt_directory(tmp_path, verbose=False)
            
            assert result == tmp_path / ".dt"
            assert (tmp_path / ".dt").is_dir()

    def test_creates_gitignore(self, tmp_path):
        """Test that .dt/.gitignore is created."""
        with patch("dt.init.get_dvc_autostage", return_value=False):
            init_dt_directory(tmp_path, verbose=False)
            
            gitignore = tmp_path / ".dt" / ".gitignore"
            assert gitignore.exists()
            content = gitignore.read_text()
            assert "config.local.yaml" in content
            assert "tmp/" in content

    def test_autostages_when_dvc_autostage_enabled(self, tmp_path):
        """Test that .gitignore is auto-staged when DVC autostage is on."""
        with patch("dt.init.get_dvc_autostage", return_value=True):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)
                
                init_dt_directory(tmp_path, verbose=False)
                
                # Should have called git add
                calls = mock_run.call_args_list
                git_add_called = any(
                    "git" in str(c) and "add" in str(c)
                    for c in calls
                )
                assert git_add_called


# =============================================================================
# init_project tests
# =============================================================================

class TestInitProject:
    """Tests for the init_project function."""

    def test_checks_dependencies_first(self, tmp_path):
        """Test that dependencies are checked first."""
        with patch("dt.init.check_dependencies") as mock_check:
            mock_check.side_effect = InitError("git not found")
            
            with pytest.raises(InitError, match="git not found"):
                init_project(repo_path=tmp_path)

    def test_skips_git_when_no_git_true(self, tmp_path):
        """Test git init is skipped when no_git=True."""
        with patch("dt.init.check_dependencies"):
            with patch("dt.init.init_git") as mock_init_git:
                with patch("dt.init.init_dvc", return_value=True):
                    with patch("dt.init.init_dt_directory", return_value=tmp_path / ".dt"):
                        with patch("dt.init.install_dvc_hooks"):
                            with patch("dt.init.utils.get_project_name", return_value="test"):
                                init_project(no_git=True, no_cache=True, no_remote=True, 
                                           repo_path=tmp_path, verbose=False)
                                
                                mock_init_git.assert_not_called()

    def test_skips_dvc_when_no_dvc_true(self, tmp_path):
        """Test DVC init is skipped when no_dvc=True."""
        with patch("dt.init.check_dependencies"):
            with patch("dt.init.init_git", return_value=True):
                with patch("dt.init.init_dvc") as mock_init_dvc:
                    with patch("dt.init.init_dt_directory", return_value=tmp_path / ".dt"):
                        with patch("dt.init.check_github_remote", return_value=True):
                            with patch("dt.init.utils.get_project_name", return_value="test"):
                                init_project(no_dvc=True, no_cache=True, no_remote=True, 
                                           repo_path=tmp_path, verbose=False)
                                
                                mock_init_dvc.assert_not_called()

    def test_returns_result_dict(self, tmp_path):
        """Test that result dict is returned with paths."""
        with patch("dt.init.check_dependencies"):
            with patch("dt.init.init_git", return_value=True):
                with patch("dt.init.init_dvc", return_value=True):
                    with patch("dt.init.init_dt_directory", return_value=tmp_path / ".dt"):
                        with patch("dt.init.install_dvc_hooks"):
                            with patch("dt.init.check_github_remote", return_value=True):
                                with patch("dt.init.utils.get_project_name", return_value="test"):
                                    result = init_project(
                                        no_cache=True, no_remote=True,
                                        repo_path=tmp_path, verbose=False
                                    )
                                    
                                    assert result["name"] == "test"
                                    assert result["path"] == tmp_path
                                    assert result["git"] == tmp_path / ".git"
                                    assert result["dvc"] == tmp_path / ".dvc"

    def test_uses_current_dir_by_default(self):
        """Test that current directory is used by default."""
        with patch("dt.init.check_dependencies"):
            with patch("dt.init.init_git", return_value=True):
                with patch("dt.init.init_dvc", return_value=True):
                    with patch("dt.init.init_dt_directory") as mock_dt:
                        mock_dt.return_value = Path.cwd() / ".dt"
                        with patch("dt.init.install_dvc_hooks"):
                            with patch("dt.init.check_github_remote", return_value=True):
                                with patch("dt.init.utils.get_project_name", return_value="test"):
                                    result = init_project(
                                        no_cache=True, no_remote=True, verbose=False
                                    )
                                    
                                    assert result["path"] == Path.cwd()
