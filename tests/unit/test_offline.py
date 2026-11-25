"""Unit tests for dt.offline module.

Tests offline mode support for redirecting Git URLs to local temp clones.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import yaml

from dt.offline import (
    get_dt_root,
    get_tmp_dir,
    get_local_config_path,
    load_offline_state,
    save_offline_state,
    clear_offline_state,
    list_temp_clones,
    repo_id_to_urls,
    get_config_key,
    get_current_redirects,
    enable,
    disable,
)
from dt.errors import OfflineError


# =============================================================================
# get_dt_root tests
# =============================================================================

class TestGetDtRoot:
    """Tests for the get_dt_root function."""

    def test_returns_dt_directory_when_exists(self, tmp_path, monkeypatch):
        """Test returns .dt directory when it exists."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        assert get_dt_root() == dt_dir

    def test_found_from_subdirectory(self, tmp_path, monkeypatch):
        """.dt is resolved at the project root, not the current directory."""
        (tmp_path / ".dvc").mkdir()
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        subdir = tmp_path / "analysis" / "nested"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)

        assert get_dt_root() == dt_dir

    def test_raises_error_when_dt_not_exists(self, tmp_path, monkeypatch):
        """Test OfflineError raised when .dt doesn't exist."""
        monkeypatch.chdir(tmp_path)

        with pytest.raises(OfflineError, match="Not in a dt-initialized directory"):
            get_dt_root()


# =============================================================================
# get_tmp_dir tests
# =============================================================================

class TestGetTmpDir:
    """Tests for the get_tmp_dir function."""

    def test_returns_tmp_clones_path(self, tmp_path, monkeypatch):
        """Test returns .dt/tmp/clones path."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        assert get_tmp_dir() == dt_dir / "tmp" / "clones"


# =============================================================================
# load_offline_state tests
# =============================================================================

class TestLoadOfflineState:
    """Tests for the load_offline_state function."""

    def test_returns_empty_state_when_config_not_exists(self, tmp_path):
        """Test returns empty state when config file doesn't exist."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        with patch("dt.offline.get_dt_root", return_value=dt_dir):
            state = load_offline_state()
            
            assert state == {"git_redirects": [], "remote_overrides": []}

    def test_loads_state_from_config(self, tmp_path):
        """Test loads state from config file."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        config = {
            "offline": {
                "git_redirects": ["github.com/org/repo"],
                "remote_overrides": [{"remote_name": "origin", "original_url": "ssh://..."}],
            }
        }
        config_path = dt_dir / "config.local.yaml"
        config_path.write_text(yaml.dump(config))
        
        with patch("dt.offline.get_dt_root", return_value=dt_dir):
            state = load_offline_state()
            
            assert state["git_redirects"] == ["github.com/org/repo"]
            assert len(state["remote_overrides"]) == 1

    def test_handles_corrupt_config_gracefully(self, tmp_path):
        """Test handles corrupt config file gracefully."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        config_path = dt_dir / "config.local.yaml"
        config_path.write_text("not: valid: yaml: {")
        
        with patch("dt.offline.get_dt_root", return_value=dt_dir):
            state = load_offline_state()
            
            assert state == {"git_redirects": [], "remote_overrides": []}


# =============================================================================
# save_offline_state tests
# =============================================================================

class TestSaveOfflineState:
    """Tests for the save_offline_state function."""

    def test_saves_state_to_config(self, tmp_path):
        """Test saves state to config file."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        state = {
            "git_redirects": ["github.com/org/repo"],
            "remote_overrides": [{"remote_name": "origin", "original_url": "ssh://..."}],
        }
        
        with patch("dt.offline.get_dt_root", return_value=dt_dir):
            save_offline_state(state)
            
            config_path = dt_dir / "config.local.yaml"
            assert config_path.exists()
            
            loaded = yaml.safe_load(config_path.read_text())
            assert loaded["offline"]["git_redirects"] == ["github.com/org/repo"]

    def test_preserves_existing_config(self, tmp_path):
        """Test preserves other config values."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        existing = {"other_key": "other_value"}
        config_path = dt_dir / "config.local.yaml"
        config_path.write_text(yaml.dump(existing))
        
        with patch("dt.offline.get_dt_root", return_value=dt_dir):
            save_offline_state({"git_redirects": [], "remote_overrides": []})
            
            loaded = yaml.safe_load(config_path.read_text())
            assert loaded["other_key"] == "other_value"
            assert "offline" in loaded


# =============================================================================
# clear_offline_state tests
# =============================================================================

class TestClearOfflineState:
    """Tests for the clear_offline_state function."""

    def test_clears_offline_state(self, tmp_path):
        """Test clears offline state."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        config = {
            "offline": {
                "git_redirects": ["github.com/org/repo"],
                "remote_overrides": [],
            }
        }
        config_path = dt_dir / "config.local.yaml"
        config_path.write_text(yaml.dump(config))
        
        with patch("dt.offline.get_dt_root", return_value=dt_dir):
            clear_offline_state()
            
            loaded = yaml.safe_load(config_path.read_text())
            assert loaded["offline"]["git_redirects"] == []


# =============================================================================
# list_temp_clones tests
# =============================================================================

class TestListTempClones:
    """Tests for the list_temp_clones function."""

    def test_returns_empty_list_when_no_clones(self, tmp_path):
        """Test returns empty list when no temp clones exist."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        with patch("dt.offline.get_tmp_dir", return_value=dt_dir / "tmp" / "clones"):
            result = list_temp_clones()
            
            assert result == []

    def test_finds_git_repos(self, tmp_path):
        """Test finds git repos in temp clone structure."""
        tmp_clones = tmp_path / ".dt" / "tmp" / "clones"
        repo_path = tmp_clones / "github.com" / "org" / "repo"
        for d in ("objects", "refs"):
            (repo_path / ".git" / d).mkdir(parents=True)
        (repo_path / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
        
        with patch("dt.offline.get_tmp_dir", return_value=tmp_clones):
            result = list_temp_clones()
            
            assert len(result) == 1
            assert result[0][0] == "github.com/org/repo"

    def test_ignores_non_git_directories(self, tmp_path):
        """Test ignores directories without .git."""
        tmp_clones = tmp_path / ".dt" / "tmp" / "clones"
        non_repo = tmp_clones / "github.com" / "org" / "notarepo"
        non_repo.mkdir(parents=True)
        # No .git directory
        
        with patch("dt.offline.get_tmp_dir", return_value=tmp_clones):
            result = list_temp_clones()
            
            assert result == []


# =============================================================================
# repo_id_to_urls tests
# =============================================================================

class TestRepoIdToUrls:
    """Tests for the repo_id_to_urls function."""

    def test_returns_empty_for_invalid_repo_id(self):
        """Test returns empty list for invalid repo_id."""
        result = repo_id_to_urls("invalid")
        assert result == []

    def test_generates_github_urls(self):
        """Test generates GitHub URL variants."""
        result = repo_id_to_urls("github.com/org/repo")
        
        assert "git@github.com:org/repo.git" in result
        assert "git@github.com:org/repo" in result
        assert "https://github.com/org/repo.git" in result
        assert "https://github.com/org/repo" in result

    def test_generates_gitlab_urls(self):
        """Test generates GitLab URL variants."""
        result = repo_id_to_urls("gitlab.com/org/project")
        
        assert "git@gitlab.com:org/project.git" in result
        assert "https://gitlab.com/org/project" in result


# =============================================================================
# get_config_key tests
# =============================================================================

class TestGetConfigKey:
    """Tests for the get_config_key function."""

    def test_returns_insteadof_key(self):
        """Test returns correct git config key."""
        result = get_config_key(Path("/path/to/clone"))
        
        assert result == "url./path/to/clone.insteadOf"


# =============================================================================
# get_current_redirects tests
# =============================================================================

class TestGetCurrentRedirects:
    """Tests for the get_current_redirects function."""

    def test_returns_empty_dict_when_no_redirects(self):
        """Test returns empty dict when no redirects configured."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
            
            result = get_current_redirects()
            
            assert result == {}

    def test_parses_git_config_output(self):
        """Test parses git config output correctly."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="url./path/to/clone.insteadof git@github.com:org/repo.git\n",
            )
            
            result = get_current_redirects()
            
            assert "/path/to/clone" in result
            assert "git@github.com:org/repo.git" in result["/path/to/clone"]


# =============================================================================
# enable tests
# =============================================================================

class TestEnable:
    """Tests for the enable function."""

    def test_raises_error_when_no_clones_or_remotes(self):
        """Test OfflineError raised when no temp clones or SSH remotes."""
        with patch("dt.offline.list_temp_clones", return_value=[]):
            with patch("dt.offline.get_ssh_remotes", return_value=[]):
                with pytest.raises(OfflineError, match="No temporary clones"):
                    enable()

    def test_sets_git_config_for_clones(self, tmp_path):
        """Test sets git config insteadOf for temp clones."""
        clone_path = tmp_path / "clone"
        
        with patch("dt.offline.list_temp_clones", return_value=[
            ("github.com/org/repo", clone_path),
        ]):
            with patch("dt.offline.get_ssh_remotes", return_value=[]):
                with patch("dt.offline.save_offline_state"):
                    with patch("subprocess.run") as mock_run, \
                         patch("dt.offline.get_tmp_dir", return_value=tmp_path / "clones"), \
                         patch("dt.utils.dvc_local_config_problem", return_value=None):
                        mock_run.return_value = MagicMock(
                            returncode=0, stdout="git@github.com:org/repo.git\n")
                        
                        repos, remotes = enable(verbose=False)
                        
                        assert repos == ["github.com/org/repo"]
                        # Should have called git config for each URL variant
                        assert mock_run.call_count >= 1


# =============================================================================
# disable tests
# =============================================================================

class TestDisable:
    """Tests for the disable function."""

    def test_removes_git_config_redirects(self, tmp_path):
        """Test removes git config insteadOf entries."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        with patch("dt.offline.get_dt_root", return_value=dt_dir):
            with patch("dt.offline.load_offline_state", return_value={
                "git_redirects": ["github.com/org/repo"],
                "remote_overrides": [],
            }):
                with patch("dt.offline.get_tmp_dir", return_value=dt_dir / "tmp" / "clones"):
                    with patch("dt.offline.clear_offline_state"):
                        with patch("subprocess.run") as mock_run:
                            mock_run.return_value = MagicMock(returncode=0)
                            
                            repos, remotes = disable(verbose=False)
                            
                            # Should have called git config --unset-all
                            assert mock_run.call_count >= 1
                            assert repos == ["github.com/org/repo"]

    def test_removes_dvc_remote_overrides(self, tmp_path):
        """Test removes DVC remote URL overrides."""
        dt_dir = tmp_path / ".dt"
        dt_dir.mkdir()
        
        with patch("dt.offline.get_dt_root", return_value=dt_dir):
            with patch("dt.offline.load_offline_state", return_value={
                "git_redirects": [],
                "remote_overrides": [{"remote_name": "origin", "original_url": "ssh://..."}],
            }):
                with patch("dt.offline.get_tmp_dir", return_value=dt_dir / "tmp" / "clones"):
                    with patch("dt.offline.clear_offline_state"):
                        with patch("subprocess.run") as mock_run:
                            mock_run.return_value = MagicMock(returncode=0)
                            
                            repos, remotes = disable(verbose=False)
                            
                            assert remotes == ["origin"]


# =============================================================================
# redirect safety
# =============================================================================

import os
import subprocess as _sp


def _real_clone(path, origin):
    path.mkdir(parents=True)
    _sp.run(["git", "init", "-q", str(path)], check=True)
    _sp.run(["git", "-C", str(path), "remote", "add", "origin", origin], check=True)
    return path


class TestRedirectSafety:
    @pytest.mark.parametrize("host", ["github.com.evil.example", "evilgithub.com",
                                      "gitlab.com.attacker.io"])
    def test_lookalike_host_gets_no_forge_urls(self, host):
        urls = repo_id_to_urls(f"{host}/org/repo")
        assert urls and all(host in u for u in urls)
        assert not any("@github.com:" in u or "://github.com/" in u or
                       "@gitlab.com:" in u or "://gitlab.com/" in u for u in urls)

    def test_exact_host_is_case_insensitive(self):
        assert "git@github.com:org/repo.git" in repo_id_to_urls("GitHub.com/org/repo")

    def test_malformed_components_give_nothing(self):
        assert repo_id_to_urls("github.com/../repo") == []

    def test_clone_with_mismatched_origin_gets_no_redirect(self, tmp_path):
        clones = tmp_path / ".dt" / "tmp" / "clones"
        good = _real_clone(clones / "github.com" / "org" / "good",
                           "git@github.com:org/good.git")
        _real_clone(clones / "github.com" / "org" / "repo",
                    "https://github.com.evil.example/org/repo")
        calls = []
        real_run = _sp.run

        def run(cmd, *a, **k):
            if cmd[:4] == ["git", "config", "--local", "--add"]:
                calls.append(cmd)
                return MagicMock(returncode=0, stdout="", stderr="")
            return real_run(cmd, *a, **k)

        with patch("dt.offline.get_tmp_dir", return_value=clones), \
             patch("dt.offline.get_ssh_remotes", return_value=[]), \
             patch("dt.offline.save_offline_state"), \
             patch("dt.utils.dvc_local_config_problem", return_value=None), \
             patch("dt.offline.subprocess.run", side_effect=run):
            repos, _ = enable(verbose=False)
        assert repos == ["github.com/org/good"]
        targets = {c[4] for c in calls}
        assert targets == {f"url.{os.path.abspath(good)}.insteadOf"}

    def test_listing_ignores_symlinks_and_bare_layouts(self, tmp_path):
        clones = tmp_path / ".dt" / "tmp" / "clones"
        real = _real_clone(tmp_path / "elsewhere", "git@github.com:org/x.git")
        (clones / "github.com" / "org").mkdir(parents=True)
        (clones / "github.com" / "org" / "x").symlink_to(real)
        bare = clones / "github.com" / "org" / "bare"
        bare.mkdir()
        (bare / "HEAD").write_text("ref: refs/heads/main\n")
        with patch("dt.offline.get_tmp_dir", return_value=clones):
            assert list_temp_clones() == []

    def _enable(self, clones, run=None):
        with patch("dt.offline.get_tmp_dir", return_value=clones), \
             patch("dt.offline.get_ssh_remotes", return_value=[]), \
             patch("dt.offline.save_offline_state"), \
             patch("dt.utils.dvc_local_config_problem", return_value=None):
            if run is None:
                return enable(verbose=False)
            with patch("dt.offline.subprocess.run", side_effect=run):
                return enable(verbose=False)

    def test_unverified_prefix_sibling_blocks_redirect(self, tmp_path):
        clones = tmp_path / ".dt" / "tmp" / "clones"
        _real_clone(clones / "github.com" / "org" / "repo", "git@github.com:org/repo.git")
        # A sibling git's prefix match would reach, with a foreign origin.
        _real_clone(clones / "github.com" / "org" / "repo-x",
                    "https://github.com.evil.example/org/repo-x")
        repos, _ = self._enable(clones, run=self._recording_run([]))
        assert repos == []

    def test_verified_prefix_sibling_is_fine(self, tmp_path):
        clones = tmp_path / ".dt" / "tmp" / "clones"
        _real_clone(clones / "github.com" / "org" / "repo", "git@github.com:org/repo.git")
        _real_clone(clones / "github.com" / "org" / "repo-x", "git@github.com:org/repo-x.git")
        repos, _ = self._enable(clones, run=self._recording_run([]))
        assert sorted(repos) == ["github.com/org/repo", "github.com/org/repo-x"]

    @pytest.mark.parametrize("extra", ["second", "http"])
    def test_multi_valued_or_http_origin_is_refused(self, tmp_path, extra):
        clones = tmp_path / ".dt" / "tmp" / "clones"
        path = clones / "github.com" / "org" / "repo"
        if extra == "http":
            _real_clone(path, "http://github.com/org/repo")
        else:
            _real_clone(path, "https://github.com/org/repo")
            _sp.run(["git", "-C", str(path), "config", "--add", "remote.origin.url",
                     "https://github.com.evil.example/org/repo"], check=True)
        repos, _ = self._enable(clones, run=self._recording_run([]))
        assert repos == []

    def test_stale_redirects_into_clones_are_cleared(self, tmp_path):
        clones = tmp_path / ".dt" / "tmp" / "clones"
        stale = clones / "github.com.evil.example" / "org" / "repo"
        unsets = []
        real_run = _sp.run

        def run(cmd, *a, **k):
            if cmd[:4] == ["git", "config", "--local", "--get-regexp"]:
                return MagicMock(returncode=0, stderr="",
                                 stdout=f"url.{stale}.insteadof https://github.com/org/repo\n"
                                        "url./elsewhere.insteadof https://x/y/z\n")
            if cmd[:4] == ["git", "config", "--local", "--unset-all"]:
                unsets.append(cmd[4])
                return MagicMock(returncode=0, stdout="", stderr="")
            if cmd[:4] == ["git", "config", "--local", "--add"]:
                return MagicMock(returncode=0, stdout="", stderr="")
            return real_run(cmd, *a, **k)

        clones.mkdir(parents=True)
        with pytest.raises(OfflineError):  # no clones/remotes left to enable
            self._enable(clones, run=run)
        _real_clone(clones / "github.com" / "org" / "ok", "git@github.com:org/ok.git")
        self._enable(clones, run=run)
        assert f"url.{stale}.insteadOf" in unsets
        assert "url./elsewhere.insteadOf" not in unsets

    @staticmethod
    def _recording_run(calls):
        real_run = _sp.run

        def run(cmd, *a, **k):
            if cmd[:4] == ["git", "config", "--local", "--add"]:
                calls.append(cmd)
                return MagicMock(returncode=0, stdout="", stderr="")
            if cmd[:4] == ["git", "config", "--local", "--get-regexp"]:
                return MagicMock(returncode=1, stdout="", stderr="")
            return real_run(cmd, *a, **k)
        return run
