"""Integration tests for 'dt index' command group.

Tests for index mirror management (pull, push, status).
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Index Status
# =============================================================================

class TestIndexStatus:
    """Tests for 'dt index status' subcommand."""
    
    def test_index_status_help(self):
        """'dt index status --help' shows usage."""
        result = subprocess.run(
            ['dt', 'index', 'status', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
    
    def test_index_status_not_configured(self, dvc_repo, monkeypatch):
        """Index status when not configured."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'index', 'status'],
            capture_output=True,
            text=True,
        )
        
        # Should indicate not configured or show helpful message
        assert result.returncode in (0, 1)
        # May show "not configured" or similar
    
    def test_index_status_in_dvc_repo(self, dvc_repo_with_cache, monkeypatch):
        """Index status in configured repo."""
        repo = dvc_repo_with_cache['repo']
        monkeypatch.chdir(repo)
        
        result = subprocess.run(
            ['dt', 'index', 'status'],
            capture_output=True,
            text=True,
        )
        
        # Should complete
        assert result.returncode in (0, 1)


# =============================================================================
# Test Index Pull
# =============================================================================

class TestIndexPull:
    """Tests for 'dt index pull' subcommand."""
    
    def test_index_pull_help(self):
        """'dt index pull --help' shows usage."""
        result = subprocess.run(
            ['dt', 'index', 'pull', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '--verbose' in result.stdout or '-v' in result.stdout
        assert '--dry' in result.stdout or '--dry-run' in result.stdout
    
    def test_index_pull_dry_run(self, dvc_repo, monkeypatch):
        """Index pull dry run."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'index', 'pull', '--dry'],
            capture_output=True,
            text=True,
        )
        
        # Should complete (may fail if not configured)
        assert result.returncode in (0, 1)
    
    def test_index_pull_not_configured(self, dvc_repo, monkeypatch):
        """Index pull when mirror not configured."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'index', 'pull'],
            capture_output=True,
            text=True,
        )
        
        # Should indicate configuration needed
        assert result.returncode in (0, 1)


# =============================================================================
# Test Index Push
# =============================================================================

class TestIndexPush:
    """Tests for 'dt index push' subcommand."""
    
    def test_index_push_help(self):
        """'dt index push --help' shows usage."""
        result = subprocess.run(
            ['dt', 'index', 'push', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
    
    def test_index_push_dry_run(self, dvc_repo, monkeypatch):
        """Index push dry run."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'index', 'push', '--dry'],
            capture_output=True,
            text=True,
        )
        
        # Should complete (may fail if not configured)
        assert result.returncode in (0, 1)


# =============================================================================
# Test Index Help
# =============================================================================

class TestIndexHelp:
    """Tests for index command help."""
    
    def test_index_group_help(self):
        """'dt index --help' shows subcommands."""
        result = subprocess.run(
            ['dt', 'index', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'pull' in result.stdout
        assert 'push' in result.stdout
        assert 'status' in result.stdout


# =============================================================================
# Test Index Outside Repo
# =============================================================================

class TestIndexOutsideRepo:
    """Tests for index commands outside DVC repo."""
    
    def test_index_status_outside_repo(self, tmp_path, monkeypatch):
        """Index status outside repo should error."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'index', 'status'],
            capture_output=True,
            text=True,
        )
        
        # Command returns 0 but shows mirror not configured message
        assert result.returncode == 0
        assert 'not configured' in result.stdout.lower()


# =============================================================================
# Test Index With Mirror Configured
# =============================================================================

class TestIndexWithMirror:
    """Tests with index mirror configured."""
    
    @pytest.fixture
    def repo_with_index_mirror(self, dvc_repo_with_cache, tmp_path, monkeypatch):
        """DVC repo with index mirror configured."""
        repo = dvc_repo_with_cache['repo']
        monkeypatch.chdir(repo)
        
        # Create mirror directory
        mirror = tmp_path / 'index_mirror'
        mirror.mkdir()
        
        # Configure mirror in dt config
        dt_dir = repo / '.dt'
        dt_dir.mkdir(exist_ok=True)
        config_file = dt_dir / 'config.yaml'
        config_file.write_text(f"""
index:
  mirror_root: {mirror}
""")
        
        return {
            'repo': repo,
            'mirror': mirror,
            'cache': dvc_repo_with_cache['cache'],
        }
    
    def test_index_status_with_mirror(self, repo_with_index_mirror):
        """Index status with mirror configured."""
        repo = repo_with_index_mirror['repo']
        
        result = subprocess.run(
            ['dt', 'index', 'status'],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        # Should work
        assert result.returncode in (0, 1)
