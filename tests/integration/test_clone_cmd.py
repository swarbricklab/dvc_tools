"""Integration tests for dt clone command.

These tests run real git commands to test repository cloning.
Network-dependent tests use the dt-test-fixtures repository.
"""

import os
import subprocess
from pathlib import Path

import pytest

from tests.conftest import requires_dvc, requires_git, requires_network


# =============================================================================
# Helper Functions
# =============================================================================

def run_dt(*args, cwd=None, check=True, env=None):
    """Run dt command and return result.
    
    Args:
        *args: Command arguments to pass to dt
        cwd: Working directory
        check: Raise exception on non-zero exit
        env: Environment variables (defaults to os.environ)
    """
    run_env = env if env is not None else os.environ
    result = subprocess.run(
        ['dt', *args],
        capture_output=True,
        text=True,
        cwd=cwd,
        env=run_env,
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
# Clone with Full URL Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneFullUrl:
    """Test cloning with full repository URLs."""

    def test_clone_with_https_url(self, isolated_dir):
        """Clone with full HTTPS URL."""
        result = run_dt(
            'clone', 
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        repo_path = isolated_dir / 'dt-test-fixtures'
        assert repo_path.is_dir()
        assert (repo_path / '.git').is_dir()

    def test_clone_with_ssh_url(self, isolated_dir):
        """Clone with full SSH URL."""
        result = run_dt(
            'clone',
            'git@github.com:swarbricklab/dt-test-fixtures.git',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        repo_path = isolated_dir / 'dt-test-fixtures'
        assert repo_path.is_dir()
        assert (repo_path / '.git').is_dir()

    def test_clone_creates_dvc_structure(self, isolated_dir):
        """Cloned repo has DVC structure with .dvc files."""
        run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--shallow',
            cwd=isolated_dir
        )
        
        repo_path = isolated_dir / 'dt-test-fixtures'
        assert (repo_path / '.dvc').is_dir()
        # dt-test-fixtures has .dvc files
        dvc_files = list(repo_path.rglob('*.dvc'))
        assert len(dvc_files) > 0, "Expected at least one .dvc file in the cloned repo"
        
        # Verify specific expected .dvc files exist
        expected_dvc_files = ['single_file/data.txt.dvc', 'directory/data.dvc']
        for expected_file in expected_dvc_files:
            expected_path = repo_path / expected_file
            assert expected_path.exists(), f"Expected {expected_file} to exist"


# =============================================================================
# Clone with Short Name Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneShortName:
    """Test cloning with short repository names."""

    def test_clone_with_owner_flag(self, isolated_dir):
        """Clone with short name and --owner flag."""
        result = run_dt(
            'clone',
            'dt-test-fixtures',
            '--owner', 'swarbricklab',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        repo_path = isolated_dir / 'dt-test-fixtures'
        assert repo_path.is_dir()

    def test_clone_short_name_without_owner_fails(self, isolated_dir, tmp_path):
        """Clone with short name and no owner configured fails."""
        # Isolate from system/user config by overriding XDG paths in subprocess env
        empty_config = tmp_path / 'empty_config'
        empty_config.mkdir()
        
        # Create isolated environment for subprocess
        isolated_env = os.environ.copy()
        isolated_env['XDG_CONFIG_HOME'] = str(empty_config)
        isolated_env['XDG_CONFIG_DIRS'] = str(empty_config)
        # Also clear HOME-based fallback
        isolated_env['HOME'] = str(tmp_path / 'fake_home')
        (tmp_path / 'fake_home').mkdir()
        
        result = run_dt(
            'clone',
            'some-nonexistent-repo',
            cwd=isolated_dir,
            check=False,
            env=isolated_env,
        )
        
        # Should fail because no owner is configured
        assert result.returncode != 0
        assert 'owner' in result.stderr.lower() or 'owner' in str(result.stdout).lower()

    def test_clone_short_name_with_config_owner(self, dvc_repo):
        """Clone with short name uses owner from config."""
        # Pre-configure owner
        run_dt('config', 'set', 'owner', 'swarbricklab', cwd=dvc_repo)
        
        # Clone using short name - config owner should be used
        result = run_dt(
            'clone',
            'dt-test-fixtures',
            '--shallow',
            cwd=dvc_repo
        )
        
        assert result.returncode == 0
        repo_path = dvc_repo / 'dt-test-fixtures'
        assert repo_path.is_dir()
        assert (repo_path / '.git').is_dir()


# =============================================================================
# Clone with Custom Path Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneCustomPath:
    """Test cloning to custom directory."""

    def test_clone_to_custom_directory(self, isolated_dir):
        """Clone to specified directory."""
        custom_path = isolated_dir / 'my-custom-dir'
        
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            str(custom_path),
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        assert custom_path.is_dir()
        assert (custom_path / '.git').is_dir()

    def test_clone_extracts_repo_name_correctly(self, isolated_dir):
        """Clone extracts correct repo name from URL."""
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures.git',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        # Should strip .git suffix
        repo_path = isolated_dir / 'dt-test-fixtures'
        assert repo_path.is_dir()


# =============================================================================
# Clone Options Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneOptions:
    """Test clone command options."""

    def test_clone_shallow(self, isolated_dir):
        """Shallow clone has limited history."""
        run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--shallow',
            cwd=isolated_dir
        )
        
        repo_path = isolated_dir / 'dt-test-fixtures'
        
        # Check commit count - shallow should have very few
        result = subprocess.run(
            ['git', 'rev-list', '--count', 'HEAD'],
            capture_output=True, text=True, cwd=repo_path
        )
        commit_count = int(result.stdout.strip())
        assert commit_count == 1, f"Shallow clone should have 1 commit, got {commit_count}"

    def test_clone_no_submodules(self, isolated_dir):
        """Clone with --no-submodules skips submodule init."""
        # dt-test-fixtures may or may not have submodules
        # This test verifies the flag is accepted
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--no-submodules',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0


# =============================================================================
# Clone Cache Configuration Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneCacheConfiguration:
    """Test that clone properly configures external cache."""

    def test_clone_configures_cache_with_repo_name(self, isolated_dir, tmp_path):
        """Clone configures external cache using repository name."""
        # Pre-configure cache root
        cache_root = tmp_path / 'caches'
        cache_root.mkdir()
        
        # We need a DVC repo to set config in first
        run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        run_dt('config', 'set', 'cache.root', str(cache_root), cwd=isolated_dir)
        
        # Now clone - cache should be configured based on repo name
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        repo_path = isolated_dir / 'dt-test-fixtures'
        
        # Check that cache directory was created
        expected_cache = cache_root / 'dt-test-fixtures'
        assert expected_cache.exists(), f"Cache directory {expected_cache} should be created"

    def test_clone_with_custom_cache_name(self, isolated_dir, tmp_path):
        """Clone with --cache-name uses custom cache directory."""
        cache_root = tmp_path / 'caches'
        cache_root.mkdir()
        
        run_dt('init', '--no-cache', '--no-remote', cwd=isolated_dir)
        run_dt('config', 'set', 'cache.root', str(cache_root), cwd=isolated_dir)
        
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--cache-name', 'my-custom-cache',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        
        # Check that custom cache directory was created
        expected_cache = cache_root / 'my-custom-cache'
        assert expected_cache.exists(), f"Custom cache directory {expected_cache} should be created"


# =============================================================================
# Clone Error Handling Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneErrors:
    """Test error handling in dt clone."""

    def test_clone_nonexistent_repo(self, isolated_dir):
        """Clone non-existent repo fails with clear error."""
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/nonexistent-repo-12345',
            cwd=isolated_dir,
            check=False
        )
        
        assert result.returncode != 0
        # Should not have created any directory
        nonexistent_path = isolated_dir / 'nonexistent-repo-12345'
        assert not nonexistent_path.exists(), "Failed clone should not create directory"

    def test_clone_to_existing_directory(self, isolated_dir):
        """Clone to existing directory fails with appropriate error."""
        # First clone
        run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--shallow',
            cwd=isolated_dir
        )
        
        # Second clone to same location should fail
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--shallow',
            cwd=isolated_dir,
            check=False
        )
        
        assert result.returncode != 0
        # Error should mention the directory already exists
        combined_output = result.stdout + result.stderr
        assert 'exist' in combined_output.lower() or 'fatal' in combined_output.lower()


# =============================================================================
# Clone Output Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneOutput:
    """Test clone command output."""

    def test_clone_shows_next_steps(self, isolated_dir):
        """Clone output includes helpful next steps with specific commands."""
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        # Should show directory change instruction
        assert 'cd' in result.stdout, "Output should include cd instruction"
        # Should suggest dvc pull
        assert 'dvc pull' in result.stdout, "Output should suggest dvc pull command"

    def test_clone_shows_resolved_url(self, isolated_dir):
        """Clone with short name shows resolved URL in output."""
        result = run_dt(
            'clone',
            'dt-test-fixtures',
            '--owner', 'swarbricklab',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        # Should show the resolved URL or owner in the output
        assert 'swarbricklab' in result.stdout, "Output should show resolved owner"


# =============================================================================
# Clone No-Init Option Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneNoInit:
    """Test --no-init option skips dt init steps."""

    def test_clone_no_init_skips_cache_setup(self, isolated_dir):
        """Clone with --no-init skips cache initialization."""
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--no-init',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        repo_path = isolated_dir / 'dt-test-fixtures'
        assert repo_path.is_dir()
        assert (repo_path / '.git').is_dir()
        # DVC should still exist from the cloned repo itself
        assert (repo_path / '.dvc').is_dir()


# =============================================================================
# Local Clone Tests (No Network)
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestCloneLocal:
    """Test cloning from local repositories (no network required)."""

    def test_clone_local_path(self, dvc_repo, tmp_path, monkeypatch):
        """Clone from local path."""
        monkeypatch.chdir(tmp_path)
        target = tmp_path / 'cloned'
        
        result = run_dt(
            'clone',
            str(dvc_repo),
            str(target),
            cwd=tmp_path
        )
        
        assert result.returncode == 0
        assert target.is_dir()
        assert (target / '.git').is_dir()
