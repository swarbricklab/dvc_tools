"""Integration tests for 'dt offline' command.

Tests for enable/disable/status subcommands that manage offline mode
for Git repos and DVC remotes.
"""

import os
import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def dt_initialized_repo(dvc_repo, monkeypatch):
    """Create a DVC repo with dt initialized (.dt directory exists).
    
    This is the minimum required for offline commands to work.
    """
    dt_dir = dvc_repo / '.dt'
    dt_dir.mkdir()
    (dt_dir / 'config.yaml').write_text('# DT config\n')
    
    return dvc_repo


@pytest.fixture
def dt_repo_with_tmp_clone(dt_initialized_repo, tmp_path):
    """Create a dt repo with a temporary clone available.
    
    Creates the structure expected by offline mode:
    .dt/tmp/clones/github.com/org/repo/  (a git repo)
    """
    # Create temp clones directory structure
    clones_dir = dt_initialized_repo / '.dt' / 'tmp' / 'clones'
    clone_path = clones_dir / 'github.com' / 'testorg' / 'testrepo'
    clone_path.mkdir(parents=True)
    
    # Initialize a bare git repo at the clone path
    subprocess.run(
        ['git', 'init', '--bare'],
        cwd=clone_path,
        check=True,
        capture_output=True,
    )
    
    return {
        'repo': dt_initialized_repo,
        'clone_path': clone_path,
        'repo_id': 'github.com/testorg/testrepo',
    }


@pytest.fixture
def run_dt_offline(dt_initialized_repo):
    """Provide a function to run dt offline commands.
    
    Returns a wrapper that runs dt offline commands and captures output.
    """
    def _run(*args, check=True):
        result = subprocess.run(
            ['dt', 'offline', *args],
            capture_output=True,
            text=True,
            cwd=dt_initialized_repo,
        )
        if check and result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode,
                ['dt', 'offline', *args],
                result.stdout,
                result.stderr,
            )
        return result
    
    return _run


# =============================================================================
# Test Classes
# =============================================================================

class TestOfflineStatus:
    """Tests for 'dt offline status' command."""
    
    def test_status_without_dt_init_fails(self, dvc_repo, monkeypatch):
        """Status should fail if .dt directory doesn't exist."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'offline', 'status'],
            capture_output=True,
            text=True,
            cwd=dvc_repo,
        )
        
        assert result.returncode != 0
        assert 'dt-initialized' in result.stderr or 'dt init' in result.stderr.lower()
    
    def test_status_shows_disabled_when_fresh(self, run_dt_offline):
        """Fresh repo should show offline mode as disabled."""
        result = run_dt_offline('status')
        
        assert result.returncode == 0
        assert 'DISABLED' in result.stdout
    
    def test_status_shows_no_clones_when_empty(self, run_dt_offline):
        """Status should show no clones available when none exist."""
        result = run_dt_offline('status')
        
        assert result.returncode == 0
        # Should show no clones available or hint to create them
        assert 'none' in result.stdout.lower() or 'dt tmp clone' in result.stdout
    
    def test_status_shows_available_clone(self, dt_repo_with_tmp_clone):
        """Status should show available temp clones."""
        repo = dt_repo_with_tmp_clone['repo']
        repo_id = dt_repo_with_tmp_clone['repo_id']
        
        result = subprocess.run(
            ['dt', 'offline', 'status'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        # Should list the available clone
        assert repo_id in result.stdout or 'testrepo' in result.stdout


class TestOfflineEnable:
    """Tests for 'dt offline enable' command."""
    
    def test_enable_without_dt_init_fails(self, dvc_repo, monkeypatch):
        """Enable should fail if .dt directory doesn't exist."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'offline', 'enable'],
            capture_output=True,
            text=True,
            cwd=dvc_repo,
        )
        
        assert result.returncode != 0
        assert 'dt-initialized' in result.stderr or 'dt init' in result.stderr.lower()
    
    def test_enable_without_clones_fails(self, run_dt_offline):
        """Enable should fail when no temp clones or remotes exist."""
        result = run_dt_offline('enable', check=False)
        
        assert result.returncode != 0
        assert 'no temp' in result.stderr.lower() or 'clone' in result.stderr.lower()
    
    def test_enable_with_clone_succeeds(self, dt_repo_with_tmp_clone):
        """Enable should work when temp clones exist."""
        repo = dt_repo_with_tmp_clone['repo']
        
        result = subprocess.run(
            ['dt', 'offline', 'enable'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        assert 'enabled' in result.stdout.lower()
    
    def test_enable_sets_git_config(self, dt_repo_with_tmp_clone):
        """Enable should set git config insteadOf entries."""
        repo = dt_repo_with_tmp_clone['repo']
        clone_path = dt_repo_with_tmp_clone['clone_path']
        
        # Enable offline mode
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        
        # Check git config
        result = subprocess.run(
            ['git', 'config', '--local', '--get-regexp', 'url.*insteadOf'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        # Should have insteadOf entries pointing to local clone
        assert result.returncode == 0
        assert str(clone_path) in result.stdout
    
    def test_enable_verbose_shows_details(self, dt_repo_with_tmp_clone):
        """Enable with -v should show detailed output."""
        repo = dt_repo_with_tmp_clone['repo']
        
        result = subprocess.run(
            ['dt', 'offline', 'enable', '-v'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        # Verbose should show the redirect
        assert 'github.com' in result.stdout or 'testorg' in result.stdout
    
    def test_enable_saves_state(self, dt_repo_with_tmp_clone):
        """Enable should save state to .dt/config.local.yaml."""
        repo = dt_repo_with_tmp_clone['repo']
        
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        
        config_local = repo / '.dt' / 'config.local.yaml'
        assert config_local.exists()
        
        content = config_local.read_text()
        assert 'git_redirects' in content


class TestOfflineDisable:
    """Tests for 'dt offline disable' command."""
    
    def test_disable_without_dt_init_fails(self, dvc_repo, monkeypatch):
        """Disable should fail if .dt directory doesn't exist."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'offline', 'disable'],
            capture_output=True,
            text=True,
            cwd=dvc_repo,
        )
        
        assert result.returncode != 0
        assert 'dt-initialized' in result.stderr or 'dt init' in result.stderr.lower()
    
    def test_disable_when_not_enabled(self, run_dt_offline):
        """Disable should report nothing was enabled."""
        result = run_dt_offline('disable')
        
        assert result.returncode == 0
        assert 'not enabled' in result.stdout.lower() or '0' in result.stdout
    
    def test_disable_after_enable_works(self, dt_repo_with_tmp_clone):
        """Disable should remove what enable added."""
        repo = dt_repo_with_tmp_clone['repo']
        
        # Enable first
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        
        # Then disable
        result = subprocess.run(
            ['dt', 'offline', 'disable'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        assert 'disabled' in result.stdout.lower()
    
    def test_disable_removes_git_config(self, dt_repo_with_tmp_clone):
        """Disable should remove git config insteadOf entries."""
        repo = dt_repo_with_tmp_clone['repo']
        
        # Enable then disable
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        subprocess.run(
            ['dt', 'offline', 'disable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        
        # Check git config - should have no insteadOf entries
        result = subprocess.run(
            ['git', 'config', '--local', '--get-regexp', 'url.*insteadOf'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        # No entries means returncode 1 (no matches)
        assert result.returncode == 1 or result.stdout.strip() == ''
    
    def test_disable_verbose_shows_details(self, dt_repo_with_tmp_clone):
        """Disable with -v should show detailed output."""
        repo = dt_repo_with_tmp_clone['repo']
        
        # Enable first
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        
        result = subprocess.run(
            ['dt', 'offline', 'disable', '-v'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0


class TestOfflineStatusAfterEnableDisable:
    """Tests for status reflecting enable/disable changes."""
    
    def test_status_shows_enabled_after_enable(self, dt_repo_with_tmp_clone):
        """Status should show ENABLED after enable."""
        repo = dt_repo_with_tmp_clone['repo']
        
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        
        result = subprocess.run(
            ['dt', 'offline', 'status'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        assert 'ENABLED' in result.stdout
    
    def test_status_shows_disabled_after_disable(self, dt_repo_with_tmp_clone):
        """Status should show DISABLED after disable."""
        repo = dt_repo_with_tmp_clone['repo']
        
        # Enable then disable
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        subprocess.run(
            ['dt', 'offline', 'disable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        
        result = subprocess.run(
            ['dt', 'offline', 'status'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        assert 'DISABLED' in result.stdout


class TestOfflineFromSubdirectory:
    """Tests for running offline commands from subdirectories."""
    
    def test_status_works_from_subdirectory(self, dt_initialized_repo, monkeypatch):
        """Status should work when run from a subdirectory."""
        # Create a subdirectory
        subdir = dt_initialized_repo / 'subdir' / 'deep'
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)
        
        result = subprocess.run(
            ['dt', 'offline', 'status'],
            capture_output=True,
            text=True,
            cwd=subdir,
        )
        
        assert result.returncode == 0
        assert 'DISABLED' in result.stdout
    
    def test_enable_works_from_subdirectory(self, dt_repo_with_tmp_clone, monkeypatch):
        """Enable should work when run from a subdirectory."""
        repo = dt_repo_with_tmp_clone['repo']
        
        # Create and cd to subdirectory
        subdir = repo / 'data' / 'processed'
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)
        
        result = subprocess.run(
            ['dt', 'offline', 'enable'],
            capture_output=True,
            text=True,
            cwd=subdir,
        )
        
        assert result.returncode == 0
        assert 'enabled' in result.stdout.lower()
    
    def test_disable_works_from_subdirectory(self, dt_repo_with_tmp_clone, monkeypatch):
        """Disable should work when run from a subdirectory."""
        repo = dt_repo_with_tmp_clone['repo']
        
        # Enable from root first
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        
        # Create and cd to subdirectory
        subdir = repo / 'src' / 'models'
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)
        
        # Disable from subdirectory
        result = subprocess.run(
            ['dt', 'offline', 'disable'],
            capture_output=True,
            text=True,
            cwd=subdir,
        )
        
        assert result.returncode == 0
        assert 'disabled' in result.stdout.lower()


class TestOfflineHelpAndUsage:
    """Tests for help text and usage."""
    
    def test_offline_help(self):
        """'dt offline --help' should show subcommands."""
        result = subprocess.run(
            ['dt', 'offline', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'enable' in result.stdout.lower()
        assert 'disable' in result.stdout.lower()
        assert 'status' in result.stdout.lower()
    
    def test_offline_enable_help(self):
        """'dt offline enable --help' should show options."""
        result = subprocess.run(
            ['dt', 'offline', 'enable', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '-v' in result.stdout or '--verbose' in result.stdout
    
    def test_offline_disable_help(self):
        """'dt offline disable --help' should show options."""
        result = subprocess.run(
            ['dt', 'offline', 'disable', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '-v' in result.stdout or '--verbose' in result.stdout


class TestOfflineIdempotence:
    """Tests for idempotent behavior of enable/disable."""
    
    def test_enable_twice_is_safe(self, dt_repo_with_tmp_clone):
        """Enabling twice should not cause errors."""
        repo = dt_repo_with_tmp_clone['repo']
        
        # Enable twice
        result1 = subprocess.run(
            ['dt', 'offline', 'enable'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        result2 = subprocess.run(
            ['dt', 'offline', 'enable'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result1.returncode == 0
        assert result2.returncode == 0
    
    def test_disable_twice_is_safe(self, dt_repo_with_tmp_clone):
        """Disabling twice should not cause errors."""
        repo = dt_repo_with_tmp_clone['repo']
        
        # Enable then disable twice
        subprocess.run(
            ['dt', 'offline', 'enable'],
            check=True,
            capture_output=True,
            cwd=repo,
        )
        result1 = subprocess.run(
            ['dt', 'offline', 'disable'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        result2 = subprocess.run(
            ['dt', 'offline', 'disable'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result1.returncode == 0
        assert result2.returncode == 0
