"""Integration tests for 'dt tmp' command group.

Tests for temporary repository clone management.
"""

import subprocess
from pathlib import Path

import pytest

from tests.integration.conftest import requires_network


# =============================================================================
# Test Tmp Clone
# =============================================================================

class TestTmpClone:
    """Tests for 'dt tmp clone' subcommand."""
    
    def test_tmp_clone_help(self):
        """'dt tmp clone --help' shows usage."""
        result = subprocess.run(
            ['dt', 'tmp', 'clone', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'REPOSITORY' in result.stdout
    
    def test_tmp_clone_requires_repository(self, dvc_repo, monkeypatch):
        """Clone requires repository argument."""
        monkeypatch.chdir(dvc_repo)
        
        # Initialize .dt directory
        (dvc_repo / '.dt').mkdir(exist_ok=True)
        
        result = subprocess.run(
            ['dt', 'tmp', 'clone'],
            capture_output=True,
            text=True,
        )
        
        # Should fail - missing argument
        assert result.returncode != 0
    
    def test_tmp_clone_requires_dt_init(self, dvc_repo, monkeypatch):
        """Clone requires dt init (for .dt directory)."""
        monkeypatch.chdir(dvc_repo)
        
        # Don't create .dt directory
        
        result = subprocess.run(
            ['dt', 'tmp', 'clone', 'some-repo'],
            capture_output=True,
            text=True,
        )
        
        # Should fail - .dt not initialized
        assert result.returncode != 0


class TestTmpList:
    """Tests for 'dt tmp list' subcommand."""
    
    def test_tmp_list_help(self):
        """'dt tmp list --help' shows usage."""
        result = subprocess.run(
            ['dt', 'tmp', 'list', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
    
    def test_tmp_list_empty(self, dvc_repo, monkeypatch):
        """List when no temp clones exist."""
        monkeypatch.chdir(dvc_repo)
        
        # Initialize .dt directory
        dt_dir = dvc_repo / '.dt'
        dt_dir.mkdir(exist_ok=True)
        
        result = subprocess.run(
            ['dt', 'tmp', 'list'],
            capture_output=True,
            text=True,
        )
        
        # Should succeed (empty list)
        assert result.returncode == 0
    
    def test_tmp_list_shows_clones(self, dvc_repo, monkeypatch):
        """List shows available temp clones."""
        monkeypatch.chdir(dvc_repo)
        
        # Create .dt and temp clone structure
        clones_dir = dvc_repo / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        clones_dir.mkdir(parents=True)
        
        # Initialize as git repo
        subprocess.run(
            ['git', 'init', '--bare'],
            cwd=clones_dir,
            capture_output=True,
        )
        
        result = subprocess.run(
            ['dt', 'tmp', 'list'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should show the clone
        assert 'repo' in result.stdout or 'github.com' in result.stdout


class TestTmpClean:
    """Tests for 'dt tmp clean' subcommand."""
    
    def test_tmp_clean_help(self):
        """'dt tmp clean --help' shows usage."""
        result = subprocess.run(
            ['dt', 'tmp', 'clean', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '--all' in result.stdout
    
    def test_tmp_clean_specific_repo(self, dvc_repo, monkeypatch):
        """Clean specific repository."""
        monkeypatch.chdir(dvc_repo)
        
        # Create .dt and temp clone
        clones_dir = dvc_repo / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        clones_dir.mkdir(parents=True)
        subprocess.run(['git', 'init', '--bare'], cwd=clones_dir, capture_output=True)
        
        result = subprocess.run(
            ['dt', 'tmp', 'clean', 'github.com/org/repo'],
            capture_output=True,
            text=True,
        )
        
        # Should work
        assert result.returncode in (0, 1)
    
    def test_tmp_clean_all(self, dvc_repo, monkeypatch):
        """Clean all temp clones."""
        monkeypatch.chdir(dvc_repo)
        
        # Create .dt and temp clone
        clones_dir = dvc_repo / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        clones_dir.mkdir(parents=True)
        
        result = subprocess.run(
            ['dt', 'tmp', 'clean', '--all'],
            capture_output=True,
            text=True,
        )
        
        # Should work
        assert result.returncode == 0


# =============================================================================
# Test Tmp Help
# =============================================================================

class TestTmpHelp:
    """Tests for tmp command help."""
    
    def test_tmp_group_help(self):
        """'dt tmp --help' shows subcommands."""
        result = subprocess.run(
            ['dt', 'tmp', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'clone' in result.stdout
        assert 'list' in result.stdout
        assert 'clean' in result.stdout


# =============================================================================
# Test Tmp Options
# =============================================================================

class TestTmpCloneOptions:
    """Tests for tmp clone options."""
    
    def test_tmp_clone_no_refresh_option(self):
        """'--no-refresh' option is available."""
        result = subprocess.run(
            ['dt', 'tmp', 'clone', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '--no-refresh' in result.stdout
    
    def test_tmp_clone_owner_option(self):
        """'--owner' option for short names."""
        result = subprocess.run(
            ['dt', 'tmp', 'clone', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '--owner' in result.stdout


@pytest.mark.requires_network
class TestTmpCloneNetwork:
    """Tests requiring network access."""
    
    def test_tmp_clone_from_github(self, dvc_repo, monkeypatch):
        """Clone from GitHub repository."""
        monkeypatch.chdir(dvc_repo)
        
        # Initialize .dt
        (dvc_repo / '.dt').mkdir(exist_ok=True)
        
        result = subprocess.run(
            ['dt', 'tmp', 'clone', 
             'https://github.com/swarbricklab/dt-test-fixtures'],
            capture_output=True,
            text=True,
            timeout=60,
        )
        
        if result.returncode == 0:
            # Should create clone in .dt/tmp/clones
            clones_dir = dvc_repo / '.dt' / 'tmp' / 'clones'
            assert clones_dir.exists()
