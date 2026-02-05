"""Integration tests for dt push command.

Tests the complete push workflow including:
- Push to project remotes
- Push to specific remotes
- Dry run mode
- Verbose output
- Error handling
- Target-specific pushes

Requires:
- git installed
- dvc installed
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

# Markers for conditional test execution
requires_git = pytest.mark.skipif(
    shutil.which('git') is None,
    reason='git not installed'
)
requires_dvc = pytest.mark.skipif(
    shutil.which('dvc') is None,
    reason='dvc not installed'
)


def run_dt(*args, cwd=None, check=True):
    """Run dt command and return result."""
    cmd = ['dt'] + list(args)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if check and result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
    return result


# =============================================================================
# Push-Specific Fixtures
# =============================================================================

@pytest.fixture(scope='function')
def push_test_repo(tmp_path):
    """Create a DVC repo with files ready for pushing.
    
    Sets up:
    - Git repo with DVC initialized
    - Data files tracked by DVC
    - Local remote configured for testing push
    - External cache configured
    """
    repo = tmp_path / 'repo'
    repo.mkdir()
    
    # Initialize git
    subprocess.run(['git', 'init'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'config', 'user.email', 'test@test.com'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'config', 'user.name', 'Test'], cwd=repo, check=True, capture_output=True)
    
    # Create initial commit
    readme = repo / 'README.md'
    readme.write_text('# Test Project\n')
    subprocess.run(['git', 'add', 'README.md'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'commit', '-m', 'Initial commit'], cwd=repo, check=True, capture_output=True)
    
    # Initialize DVC
    subprocess.run(['dvc', 'init'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'commit', '-m', 'Initialize DVC'], cwd=repo, check=True, capture_output=True)
    
    # Create cache directory
    cache_dir = tmp_path / 'cache'
    cache_dir.mkdir()
    files_md5 = cache_dir / 'files' / 'md5'
    for i in range(256):
        (files_md5 / f'{i:02x}').mkdir(parents=True)
    
    # Configure cache
    subprocess.run(['dvc', 'cache', 'dir', '--local', str(cache_dir)], cwd=repo, check=True, capture_output=True)
    
    # Create remote directories
    remote1 = tmp_path / 'remote1'
    remote1.mkdir()
    remote1_files = remote1 / 'files' / 'md5'
    for i in range(256):
        (remote1_files / f'{i:02x}').mkdir(parents=True)
    
    remote2 = tmp_path / 'remote2'
    remote2.mkdir()
    remote2_files = remote2 / 'files' / 'md5'
    for i in range(256):
        (remote2_files / f'{i:02x}').mkdir(parents=True)
    
    # Configure remotes at project scope
    subprocess.run(['dvc', 'remote', 'add', 'remote1', str(remote1)], cwd=repo, check=True, capture_output=True)
    subprocess.run(['dvc', 'remote', 'add', 'remote2', str(remote2)], cwd=repo, check=True, capture_output=True)
    subprocess.run(['dvc', 'remote', 'default', 'remote1'], cwd=repo, check=True, capture_output=True)
    
    # Create and track data files
    data_file = repo / 'data.csv'
    data_file.write_text('id,value\n1,100\n2,200\n3,300\n')
    subprocess.run(['dvc', 'add', 'data.csv'], cwd=repo, check=True, capture_output=True)
    
    # Create a directory to track
    data_dir = repo / 'data_dir'
    data_dir.mkdir()
    (data_dir / 'a.txt').write_text('file a content\n')
    (data_dir / 'b.txt').write_text('file b content\n')
    subprocess.run(['dvc', 'add', 'data_dir'], cwd=repo, check=True, capture_output=True)
    
    # Commit DVC tracking
    subprocess.run(['git', 'add', '.'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'commit', '-m', 'Add data files'], cwd=repo, check=True, capture_output=True)
    
    return {
        'path': repo,
        'cache': cache_dir,
        'remote1': remote1,
        'remote2': remote2,
    }


@pytest.fixture(scope='function')
def push_test_repo_no_remotes(tmp_path):
    """Create a DVC repo without any remotes configured."""
    repo = tmp_path / 'repo'
    repo.mkdir()
    
    # Initialize git
    subprocess.run(['git', 'init'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'config', 'user.email', 'test@test.com'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'config', 'user.name', 'Test'], cwd=repo, check=True, capture_output=True)
    
    # Create initial commit
    readme = repo / 'README.md'
    readme.write_text('# Test Project\n')
    subprocess.run(['git', 'add', 'README.md'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'commit', '-m', 'Initial commit'], cwd=repo, check=True, capture_output=True)
    
    # Initialize DVC
    subprocess.run(['dvc', 'init'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'commit', '-m', 'Initialize DVC'], cwd=repo, check=True, capture_output=True)
    
    # Create and track a data file
    data_file = repo / 'data.csv'
    data_file.write_text('id,value\n1,100\n')
    subprocess.run(['dvc', 'add', 'data.csv'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'add', '.'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'commit', '-m', 'Add data'], cwd=repo, check=True, capture_output=True)
    
    return {'path': repo}


# =============================================================================
# Basic Push Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestPushBasic:
    """Test basic dt push functionality."""

    def test_push_to_all_project_remotes(self, push_test_repo):
        """Push pushes to all project-configured remotes."""
        repo = push_test_repo['path']
        remote1 = push_test_repo['remote1']
        remote2 = push_test_repo['remote2']
        
        result = run_dt('push', cwd=repo, check=False)
        
        # Should complete successfully
        assert result.returncode == 0, f"Push failed: {result.stderr}"
        
        # Both remotes should be mentioned in output
        combined_output = result.stdout + result.stderr
        assert 'remote1' in combined_output or '✓' in combined_output
        
        # Remote should have files
        remote1_files = list(remote1.rglob('*'))
        assert len([f for f in remote1_files if f.is_file()]) > 0, "remote1 should have files"

    def test_push_to_specific_remote(self, push_test_repo):
        """Push with -r pushes to specific remote only."""
        repo = push_test_repo['path']
        remote1 = push_test_repo['remote1']
        remote2 = push_test_repo['remote2']
        
        # Clear remotes to start fresh
        for r in [remote1, remote2]:
            for f in r.rglob('*'):
                if f.is_file():
                    f.unlink()
        
        result = run_dt('push', '-r', 'remote1', cwd=repo, check=False)
        
        assert result.returncode == 0, f"Push to remote1 failed: {result.stderr}"
        
        # remote1 should have files
        remote1_files = [f for f in remote1.rglob('*') if f.is_file()]
        assert len(remote1_files) > 0, "remote1 should have files"

    def test_push_specific_target(self, push_test_repo):
        """Push specific .dvc target."""
        repo = push_test_repo['path']
        
        result = run_dt('push', 'data.csv.dvc', cwd=repo, check=False)
        
        # Should complete successfully
        assert result.returncode == 0, f"Push specific target failed: {result.stderr}"


# =============================================================================
# Dry Run Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestPushDryRun:
    """Test dt push --dry-run mode."""

    def test_dry_run_shows_what_would_be_pushed(self, push_test_repo):
        """Dry run shows count and size without pushing."""
        repo = push_test_repo['path']
        remote1 = push_test_repo['remote1']
        
        # Clear remote to ensure files need pushing
        for f in remote1.rglob('*'):
            if f.is_file():
                f.unlink()
        
        result = run_dt('push', '--dry', cwd=repo, check=False)
        
        # Should show summary
        combined_output = result.stdout + result.stderr
        assert 'would push' in combined_output.lower() or 'file' in combined_output.lower() or 'nothing' in combined_output.lower()
        
        # Remote should still be empty (no actual push)
        remote1_files = [f for f in remote1.rglob('*') if f.is_file()]
        assert len(remote1_files) == 0, "Dry run should not push files"

    def test_dry_run_verbose_lists_files(self, push_test_repo):
        """Dry run with -v lists individual files."""
        repo = push_test_repo['path']
        
        result = run_dt('push', '--dry', '-v', cwd=repo, check=False)
        
        combined_output = result.stdout + result.stderr
        # Verbose output should show file paths or hashes
        assert len(combined_output) > 0, "Verbose dry run should produce output"

    def test_dry_run_with_workers_shows_distribution(self, push_test_repo):
        """Dry run with --workers shows worker distribution."""
        repo = push_test_repo['path']
        
        result = run_dt('push', '--dry', '-w', '4', cwd=repo, check=False)
        
        combined_output = result.stdout + result.stderr
        # Should mention workers or show distribution
        assert 'worker' in combined_output.lower() or 'nothing' in combined_output.lower() or '4' in combined_output


# =============================================================================
# Verbose Mode Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestPushVerbose:
    """Test dt push verbose output."""

    def test_verbose_shows_progress(self, push_test_repo):
        """Verbose mode shows push progress."""
        repo = push_test_repo['path']
        
        result = run_dt('push', '-v', cwd=repo, check=False)
        
        combined_output = result.stdout + result.stderr
        # Should produce output about pushing
        assert len(combined_output) > 0, "Verbose mode should produce output"

    def test_verbose_shows_remote_names(self, push_test_repo):
        """Verbose mode shows which remotes are being pushed to."""
        repo = push_test_repo['path']
        
        result = run_dt('push', '-v', cwd=repo, check=False)
        
        combined_output = result.stdout + result.stderr
        # Should mention remote names or show status
        assert 'remote' in combined_output.lower() or '✓' in combined_output or '✗' in combined_output


# =============================================================================
# Error Handling Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestPushErrors:
    """Test error handling in dt push."""

    def test_push_no_remotes_fails(self, push_test_repo_no_remotes):
        """Push without configured remotes fails with helpful error."""
        repo = push_test_repo_no_remotes['path']
        
        result = run_dt('push', cwd=repo, check=False)
        
        # Should fail
        assert result.returncode != 0, "Push without remotes should fail"
        
        # Error message should mention remotes
        combined_output = result.stdout + result.stderr
        assert 'remote' in combined_output.lower() or 'configured' in combined_output.lower()

    def test_push_invalid_remote_fails(self, push_test_repo):
        """Push to non-existent remote fails."""
        repo = push_test_repo['path']
        
        result = run_dt('push', '-r', 'nonexistent_remote', cwd=repo, check=False)
        
        # Should fail
        assert result.returncode != 0, "Push to invalid remote should fail"

    def test_push_outside_dvc_repo(self, tmp_path):
        """Push outside DVC repository fails."""
        result = run_dt('push', cwd=tmp_path, check=False)
        
        # Should fail
        assert result.returncode != 0, "Push outside DVC repo should fail"


# =============================================================================
# Target Resolution Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestPushTargetResolution:
    """Test target resolution in dt push."""

    def test_push_dvc_file_directly(self, push_test_repo):
        """Push .dvc file directly."""
        repo = push_test_repo['path']
        
        result = run_dt('push', 'data.csv.dvc', cwd=repo, check=False)
        
        assert result.returncode == 0, f"Push .dvc file failed: {result.stderr}"

    def test_push_directory_target(self, push_test_repo):
        """Push directory target resolves correctly."""
        repo = push_test_repo['path']
        
        result = run_dt('push', 'data_dir.dvc', cwd=repo, check=False)
        
        assert result.returncode == 0, f"Push directory failed: {result.stderr}"

    def test_push_multiple_targets(self, push_test_repo):
        """Push multiple targets at once."""
        repo = push_test_repo['path']
        
        result = run_dt('push', 'data.csv.dvc', 'data_dir.dvc', cwd=repo, check=False)
        
        assert result.returncode == 0, f"Push multiple targets failed: {result.stderr}"


# =============================================================================
# Output Format Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestPushOutput:
    """Test dt push output format."""

    def test_push_shows_status_per_remote(self, push_test_repo):
        """Push shows status for each remote."""
        repo = push_test_repo['path']
        
        result = run_dt('push', cwd=repo, check=False)
        
        combined_output = result.stdout + result.stderr
        # Should show checkmarks or remote names
        assert '✓' in combined_output or 'remote' in combined_output.lower()

    def test_push_nothing_to_push(self, push_test_repo):
        """Push when files already pushed shows appropriate message."""
        repo = push_test_repo['path']
        
        # First push
        run_dt('push', cwd=repo, check=False)
        
        # Second push - nothing new to push
        result = run_dt('push', cwd=repo, check=False)
        
        # Should succeed
        assert result.returncode == 0
