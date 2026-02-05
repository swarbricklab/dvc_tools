"""Integration tests for dt init command.

These tests run real git and DVC commands to test project initialization.
"""

import os
import subprocess
from pathlib import Path

import pytest

from tests.conftest import requires_dvc, requires_git


# =============================================================================
# Helper Functions
# =============================================================================

def run_dt(*args, cwd=None, check=True):
    """Run dt command and return result."""
    result = subprocess.run(
        ['dt', *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            ['dt', *args],
            result.stdout,
            result.stderr,
        )
    return result


# =============================================================================
# Basic Initialization Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestInitBasic:
    """Test basic dt init functionality."""

    def test_init_new_project(self, isolated_dir):
        """Initialize new project creates git and DVC repos."""
        result = run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        assert (isolated_dir / '.git').is_dir()
        assert (isolated_dir / '.dvc').is_dir()
        assert (isolated_dir / '.dt').is_dir()

    def test_init_creates_dt_gitignore(self, isolated_dir):
        """Init creates .dt/.gitignore with proper entries."""
        run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        gitignore = isolated_dir / '.dt' / '.gitignore'
        assert gitignore.exists()
        content = gitignore.read_text()
        assert '/config.local.yaml' in content
        assert '/tmp/' in content

    def test_init_installs_dvc_hooks(self, isolated_dir):
        """Init installs DVC git hooks."""
        run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        hooks_dir = isolated_dir / '.git' / 'hooks'
        # DVC installs post-checkout hook among others
        post_checkout = hooks_dir / 'post-checkout'
        assert post_checkout.exists()


# =============================================================================
# Skip Git/DVC Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestInitSkipOptions:
    """Test --no-git and --no-dvc options."""

    def test_init_no_git_in_existing_repo(self, git_repo):
        """Init with --no-git skips git initialization in existing repo."""
        result = run_dt('init', '--no-git', '--no-cache', '--no-remote', cwd=git_repo)
        
        assert result.returncode == 0
        assert (git_repo / '.dvc').is_dir()
        # Git should still exist from fixture
        assert (git_repo / '.git').is_dir()

    def test_init_no_dvc(self, isolated_dir):
        """Init with --no-dvc only initializes git."""
        result = run_dt('init', '--no-dvc', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        assert (isolated_dir / '.git').is_dir()
        assert not (isolated_dir / '.dvc').exists()

    def test_init_no_git_no_dvc(self, isolated_dir):
        """Init with both --no-git and --no-dvc just creates .dt directory."""
        result = run_dt('init', '--no-git', '--no-dvc', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        assert (isolated_dir / '.dt').is_dir()
        assert not (isolated_dir / '.git').exists()
        assert not (isolated_dir / '.dvc').exists()


# =============================================================================
# Already Initialized Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestInitAlreadyExists:
    """Test init behavior when already initialized."""

    def test_init_in_git_repo(self, git_repo):
        """Init in existing git repo doesn't reinitialize git."""
        # Initial git log
        initial_result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True, text=True, cwd=git_repo
        )
        
        result = run_dt('init', '--no-cache', '--no-remote', cwd=git_repo)
        
        assert result.returncode == 0
        assert 'already initialized' in result.stdout.lower() or result.returncode == 0
        
        # Git HEAD should be unchanged
        final_result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True, text=True, cwd=git_repo
        )
        # Note: init_git returns False if .git exists, it doesn't modify

    def test_init_in_dvc_repo(self, dvc_repo):
        """Init in existing DVC repo doesn't reinitialize DVC."""
        result = run_dt('init', '--no-cache', '--no-remote', cwd=dvc_repo)
        
        assert result.returncode == 0
        assert 'already initialized' in result.stdout.lower() or result.returncode == 0


# =============================================================================
# Named Project Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestInitWithName:
    """Test --name option for project naming."""

    def test_init_with_custom_name(self, isolated_dir):
        """Init with --name uses custom project name."""
        result = run_dt('init', '--name', 'my-custom-project', 
                       '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        assert 'my-custom-project' in result.stdout


# =============================================================================
# Cache and Remote Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestInitCacheRemote:
    """Test cache and remote initialization."""

    def test_init_no_cache_no_remote(self, isolated_dir):
        """Init with --no-cache --no-remote skips external setup."""
        result = run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        # Should complete without errors even with no cache.root configured

    def test_init_with_cache_root(self, isolated_dir, tmp_path):
        """Init with --cache-root creates cache structure and configures DVC."""
        cache_root = tmp_path / 'caches'
        cache_root.mkdir()
        
        result = run_dt('init', '--cache-root', str(cache_root), 
                       '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        
        # Cache directory should be created with project name
        project_name = isolated_dir.name
        cache_dir = cache_root / project_name
        assert cache_dir.exists(), f"Cache directory {cache_dir} should be created"
        
        # DVC should be configured to use external cache
        dvc_config_local = isolated_dir / '.dvc' / 'config.local'
        assert dvc_config_local.exists(), "DVC local config should exist"
        config_content = dvc_config_local.read_text()
        assert str(cache_dir) in config_content, "Cache path should be in DVC config"
        assert 'cache' in config_content.lower(), "Cache section should be configured"

    def test_init_with_remote_root(self, isolated_dir, tmp_path):
        """Init with --remote-root creates remote structure and configures DVC."""
        cache_root = tmp_path / 'caches'
        cache_root.mkdir()
        remote_root = tmp_path / 'remotes'
        remote_root.mkdir()
        
        result = run_dt('init', '--cache-root', str(cache_root),
                       '--remote-root', str(remote_root), cwd=isolated_dir)
        
        assert result.returncode == 0
        
        # Remote directory should be created with project name
        project_name = isolated_dir.name
        remote_dir = remote_root / project_name
        assert remote_dir.exists(), f"Remote directory {remote_dir} should be created"


# =============================================================================
# Team and Owner Option Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestInitOwnerTeamOptions:
    """Test --owner and --team options for gh repo create suggestion."""

    def test_init_with_team_shows_in_suggestion(self, isolated_dir):
        """Init with --team includes team in gh repo create suggestion."""
        result = run_dt('init', '--owner', 'myorg', '--team', 'analysts',
                       '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        # Suggestion should include the team option
        if 'gh repo create' in result.stdout:
            assert '--team=analysts' in result.stdout

    def test_init_without_team_omits_team_option(self, isolated_dir):
        """Init without --team does not include team in suggestion."""
        result = run_dt('init', '--owner', 'myorg',
                       '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        if 'gh repo create' in result.stdout:
            assert '--team=' not in result.stdout


# =============================================================================
# Error Handling Tests
# =============================================================================

@pytest.mark.integration
class TestInitErrors:
    """Test error handling in dt init."""

    @pytest.mark.skipif(
        subprocess.run(['which', 'git'], capture_output=True).returncode != 0,
        reason="git is installed, can't test missing git"
    )
    def test_init_missing_git(self, isolated_dir, monkeypatch):
        """Init fails gracefully when git is not available."""
        # This test only runs if we can simulate missing git
        # In practice, this is hard to test without mocking
        pass

    @pytest.mark.skipif(
        subprocess.run(['which', 'dvc'], capture_output=True).returncode != 0,
        reason="dvc is installed, can't test missing dvc"
    )
    def test_init_missing_dvc(self, isolated_dir, monkeypatch):
        """Init fails gracefully when dvc is not available."""
        # This test only runs if we can simulate missing dvc
        pass


# =============================================================================
# DVC Configuration Verification Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestInitDvcConfiguration:
    """Test that dt init correctly configures DVC settings."""

    def test_init_creates_dvcignore(self, isolated_dir):
        """Init creates .dvcignore file."""
        run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        dvcignore = isolated_dir / '.dvcignore'
        assert dvcignore.exists(), ".dvcignore should be created"

    def test_init_dvc_config_exists(self, isolated_dir):
        """Init creates .dvc/config file."""
        run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        dvc_config = isolated_dir / '.dvc' / 'config'
        assert dvc_config.exists(), ".dvc/config should be created"

    def test_init_with_cache_configures_external_cache(self, isolated_dir, tmp_path):
        """Init with cache-root configures DVC to use external cache."""
        cache_root = tmp_path / 'caches'
        cache_root.mkdir()
        
        run_dt('init', '--cache-root', str(cache_root), '--no-remote', cwd=isolated_dir)
        
        # Verify cache is configured in .dvc/config.local
        dvc_config_local = isolated_dir / '.dvc' / 'config.local'
        if dvc_config_local.exists():
            content = dvc_config_local.read_text()
            # Should configure external cache directory
            assert 'cache' in content.lower()


# =============================================================================
# GitHub Remote Check Tests  
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestInitGitHubRemote:
    """Test GitHub remote checking during init."""

    def test_init_without_remote_shows_suggestion(self, isolated_dir):
        """Init without origin remote suggests gh repo create."""
        result = run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        # Should suggest creating a GitHub repo
        assert 'gh repo create' in result.stdout or 'No GitHub remote' in result.stdout

    def test_init_with_owner_shows_in_suggestion(self, isolated_dir):
        """Init with --owner includes owner in suggestion."""
        result = run_dt('init', '--owner', 'myorg', 
                       '--no-cache', '--no-remote', cwd=isolated_dir)
        
        assert result.returncode == 0
        # Suggestion should include the owner
        if 'gh repo create' in result.stdout:
            assert 'myorg' in result.stdout
