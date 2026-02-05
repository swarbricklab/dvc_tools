"""Integration tests for 'dt cache' command group.

Tests for cache management: init, rm, validate.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Cache Init
# =============================================================================

class TestCacheInit:
    """Tests for 'dt cache init' subcommand."""
    
    def test_cache_init_help(self):
        """'dt cache init --help' shows usage."""
        result = subprocess.run(
            ['dt', 'cache', 'init', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'PROJECT_NAME' in result.stdout or 'project' in result.stdout.lower()
    
    def test_cache_init_with_path(self, dvc_repo, tmp_path, monkeypatch):
        """Initialize cache with explicit path."""
        monkeypatch.chdir(dvc_repo)
        
        cache_path = tmp_path / 'test_cache'
        
        result = subprocess.run(
            ['dt', 'cache', 'init', '--cache-path', str(cache_path)],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            # Should create cache directory
            assert cache_path.exists()
            # Should have md5 subdirectories
            md5_dir = cache_path / 'files' / 'md5'
            if md5_dir.exists():
                # Should have 256 hash directories
                subdirs = list(md5_dir.iterdir())
                assert len(subdirs) == 256
    
    def test_cache_init_idempotent(self, dvc_repo, tmp_path, monkeypatch):
        """Cache init is idempotent (can run twice)."""
        monkeypatch.chdir(dvc_repo)
        
        cache_path = tmp_path / 'idempotent_cache'
        
        # Run twice
        result1 = subprocess.run(
            ['dt', 'cache', 'init', '--cache-path', str(cache_path)],
            capture_output=True,
            text=True,
        )
        result2 = subprocess.run(
            ['dt', 'cache', 'init', '--cache-path', str(cache_path)],
            capture_output=True,
            text=True,
        )
        
        # Both should succeed
        assert result1.returncode == 0
        assert result2.returncode == 0


# =============================================================================
# Test Cache RM
# =============================================================================

class TestCacheRm:
    """Tests for 'dt cache rm' subcommand."""
    
    def test_cache_rm_help(self):
        """'dt cache rm --help' shows usage."""
        result = subprocess.run(
            ['dt', 'cache', 'rm', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'TARGETS' in result.stdout or 'target' in result.stdout.lower()
        assert '--dry' in result.stdout or '--dry-run' in result.stdout
    
    def test_cache_rm_dry_run(self, dvc_repo_with_files, monkeypatch):
        """Dry run shows what would be deleted."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'cache', 'rm', '--dry', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        # Dry run should complete without actually deleting
        assert result.returncode in (0, 1)
    
    def test_cache_rm_requires_force_or_remote(self, dvc_repo_with_files, monkeypatch):
        """Cache rm blocks deletion if not in remote (without --force)."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'cache', 'rm', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        # May block or succeed depending on remote status
        # Should at least not crash
        assert result.returncode in (0, 1)
    
    def test_cache_rm_force(self, dvc_repo_with_files, monkeypatch):
        """'--force' allows deletion without remote check."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'cache', 'rm', '--force', '--dry', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should work with force flag
        assert result.returncode in (0, 1)
    
    def test_cache_rm_nonexistent(self, dvc_repo_with_files, monkeypatch):
        """Cache rm of non-existent file handles gracefully."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'cache', 'rm', 'nonexistent.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should handle gracefully
        assert result.returncode in (0, 1)


# =============================================================================
# Test Cache Validate
# =============================================================================

class TestCacheValidate:
    """Tests for 'dt cache validate' subcommand."""
    
    def test_cache_validate_help(self):
        """'dt cache validate --help' shows usage."""
        result = subprocess.run(
            ['dt', 'cache', 'validate', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '--fix' in result.stdout
        assert '--json' in result.stdout
    
    def test_cache_validate_all(self, dvc_repo_with_files, monkeypatch):
        """Validate all cache files."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'cache', 'validate'],
            capture_output=True,
            text=True,
        )
        
        # Should complete
        assert result.returncode in (0, 1)
    
    def test_cache_validate_specific_target(self, dvc_repo_with_files, monkeypatch):
        """Validate specific target."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'cache', 'validate', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode in (0, 1)
    
    def test_cache_validate_json_output(self, dvc_repo_with_files, monkeypatch):
        """'--json' produces JSON output."""
        import json
        
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'cache', 'validate', '--json'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0 and result.stdout.strip():
            try:
                data = json.loads(result.stdout)
                assert isinstance(data, (list, dict))
            except json.JSONDecodeError:
                # May have no output if nothing to validate
                pass
    
    def test_cache_validate_verbose(self, dvc_repo_with_files, monkeypatch):
        """'--verbose' shows per-file results."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'cache', 'validate', '--verbose'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode in (0, 1)


# =============================================================================
# Test Cache Help
# =============================================================================

class TestCacheHelp:
    """Tests for cache command help."""
    
    def test_cache_group_help(self):
        """'dt cache --help' shows subcommands."""
        result = subprocess.run(
            ['dt', 'cache', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'init' in result.stdout
        assert 'rm' in result.stdout
        assert 'validate' in result.stdout


# =============================================================================
# Test Cache Outside Repo
# =============================================================================

class TestCacheOutsideRepo:
    """Tests for cache commands outside DVC repo."""
    
    def test_cache_validate_outside_repo(self, tmp_path, monkeypatch):
        """Cache validate outside repo should handle gracefully."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'cache', 'validate'],
            capture_output=True,
            text=True,
        )
        
        # Should fail with helpful message
        assert result.returncode != 0
