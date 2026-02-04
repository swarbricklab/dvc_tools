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
        """Cloned repo has DVC structure."""
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
        assert len(dvc_files) > 0


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

    def test_clone_short_name_without_owner_fails(self, isolated_dir):
        """Clone with short name and no owner configured fails."""
        # Ensure no owner is configured
        result = run_dt(
            'clone',
            'some-nonexistent-repo',
            cwd=isolated_dir,
            check=False
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
        assert commit_count == 1  # --depth 1 means 1 commit

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

    def test_clone_to_existing_directory(self, isolated_dir):
        """Clone to existing directory fails."""
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


# =============================================================================
# Clone Output Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_network
class TestCloneOutput:
    """Test clone command output."""

    def test_clone_shows_next_steps(self, isolated_dir):
        """Clone output includes helpful next steps."""
        result = run_dt(
            'clone',
            'https://github.com/swarbricklab/dt-test-fixtures',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        # Should show next steps
        assert 'cd' in result.stdout or 'dvc pull' in result.stdout

    def test_clone_shows_resolved_url(self, isolated_dir):
        """Clone with short name shows resolved URL."""
        result = run_dt(
            'clone',
            'dt-test-fixtures',
            '--owner', 'swarbricklab',
            '--shallow',
            cwd=isolated_dir
        )
        
        assert result.returncode == 0
        # Should show the resolved URL
        assert 'swarbricklab' in result.stdout


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
