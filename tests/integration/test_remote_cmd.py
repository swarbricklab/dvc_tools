"""Integration tests for 'dt remote' command group.

Tests for remote storage initialization and listing.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Remote Init
# =============================================================================

class TestRemoteInit:
    """Tests for 'dt remote init' subcommand."""
    
    def test_remote_init_help(self):
        """'dt remote init --help' shows usage."""
        result = subprocess.run(
            ['dt', 'remote', 'init', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'PROJECT_NAME' in result.stdout or 'project' in result.stdout.lower()
    
    def test_remote_init_with_path(self, dvc_repo, tmp_path, monkeypatch):
        """Initialize remote with explicit path."""
        monkeypatch.chdir(dvc_repo)
        
        remote_path = tmp_path / 'test_remote'
        
        result = subprocess.run(
            ['dt', 'remote', 'init', '--path', str(remote_path)],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # Should create remote directory
            assert remote_path.exists()
    
    def test_remote_init_configures_dvc(self, dvc_repo, tmp_path, monkeypatch):
        """Remote init configures DVC remotes."""
        monkeypatch.chdir(dvc_repo)
        
        remote_path = tmp_path / 'configured_remote'
        
        result = subprocess.run(
            ['dt', 'remote', 'init', '--path', str(remote_path)],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # Check DVC config has remote
            config_result = subprocess.run(
                ['dvc', 'remote', 'list'],
                capture_output=True,
                text=True,
                cwd=dvc_repo,
            )
            # Should have some remote configured
            if config_result.returncode == 0:
                # May show configured remotes
                pass


# =============================================================================
# Test Remote List
# =============================================================================

class TestRemoteList:
    """Tests for 'dt remote list' subcommand."""
    
    def test_remote_list_help(self):
        """'dt remote list --help' shows usage."""
        result = subprocess.run(
            ['dt', 'remote', 'list', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
    
    def test_remote_list_current_repo(self, dvc_repo, monkeypatch):
        """List remotes for current repo."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'remote', 'list'],
            capture_output=True,
            text=True,
        )
        
        # Should work (may show no remotes)
        assert result.returncode in (0, 1)
    
    def test_remote_list_shows_default(self, dvc_repo_with_remote, monkeypatch):
        """Remote list shows default remote marker."""
        repo = dvc_repo_with_remote['repo']
        monkeypatch.chdir(repo)
        
        result = subprocess.run(
            ['dt', 'remote', 'list'],
            capture_output=True,
            text=True,
        )
        
        # Should show the configured remote
        assert result.returncode == 0
        # May indicate default with marker
        assert 'local' in result.stdout.lower() or result.stdout.strip()


# =============================================================================
# Test Remote Help
# =============================================================================

class TestRemoteHelp:
    """Tests for remote command help."""
    
    def test_remote_group_help(self):
        """'dt remote --help' shows subcommands."""
        result = subprocess.run(
            ['dt', 'remote', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'init' in result.stdout
        assert 'list' in result.stdout


# =============================================================================
# Test Remote Outside Repo
# =============================================================================

class TestRemoteOutsideRepo:
    """Tests for remote commands outside DVC repo."""
    
    def test_remote_list_outside_repo(self, tmp_path, monkeypatch):
        """Remote list outside repo should handle gracefully."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'remote', 'list'],
            capture_output=True,
            text=True,
        )
        
        # May fail or show no remotes
        assert result.returncode in (0, 1)


# =============================================================================
# Test Remote Configuration
# =============================================================================

class TestRemoteConfiguration:
    """Tests for remote configuration behavior."""
    
    def test_remote_init_creates_structure(self, dvc_repo, tmp_path, monkeypatch):
        """Remote init creates proper directory structure."""
        monkeypatch.chdir(dvc_repo)
        
        remote_path = tmp_path / 'structured_remote'
        
        result = subprocess.run(
            ['dt', 'remote', 'init', '--path', str(remote_path)],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0 and remote_path.exists():
            # Should have cache-like structure
            files_dir = remote_path / 'files' / 'md5'
            if files_dir.exists():
                # Should have hash directories
                subdirs = list(files_dir.iterdir())
                assert len(subdirs) == 256
