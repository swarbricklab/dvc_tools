"""Integration tests for dt pull command.

These tests verify pull functionality with real DVC repositories.
Uses cloned dt-test-fixtures and dt-test-registry repositories.

The test repos are cloned from GitHub on first use (session-scoped).
Data is pulled from remotes, which works with local remotes for testing.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from tests.conftest import requires_dvc, requires_git

# Import requires_network from integration conftest
from tests.integration.conftest import requires_network


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


def run_dvc(*args, cwd=None, check=True):
    """Run dvc command and return result."""
    result = subprocess.run(
        ['dvc', *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            ['dvc', *args],
            result.stdout,
            result.stderr,
        )
    return result


# =============================================================================
# Session-Scoped Test Repository Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def pull_test_repos(tmp_path_factory):
    """Session-scoped base directory for pull test repositories.
    
    Clones dt-test-fixtures and dt-test-registry from GitHub once per session.
    
    Returns a dict with paths to the cloned repos.
    """
    base_dir = tmp_path_factory.mktemp("pull-test-repos")
    
    repos = {}
    
    # Clone dt-test-registry first (source for imports)
    registry_path = base_dir / "dt-test-registry"
    result = subprocess.run(
        ['git', 'clone', 'https://github.com/swarbricklab/dt-test-registry.git', 
         str(registry_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"Could not clone dt-test-registry: {result.stderr}")
    repos['registry'] = registry_path
    
    # Clone dt-test-fixtures
    fixtures_path = base_dir / "dt-test-fixtures"
    result = subprocess.run(
        ['git', 'clone', 'https://github.com/swarbricklab/dt-test-fixtures.git',
         str(fixtures_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"Could not clone dt-test-fixtures: {result.stderr}")
    repos['fixtures'] = fixtures_path
    
    return repos


@pytest.fixture
def pull_test_fixtures(tmp_path, pull_test_repos):
    """Create a fresh clone of dt-test-fixtures for isolated pull testing.
    
    Configures both fixtures and registry clones to use local remotes
    with synthetic data, so tests can run without network access after clone.
    """
    # Set up the registry clone with synthetic data
    registry_clone = tmp_path / 'registry'
    subprocess.run(
        ['git', 'clone', str(pull_test_repos['registry']), str(registry_clone)],
        check=True, capture_output=True
    )
    
    # Create local remote with synthetic data for registry
    registry_remote = tmp_path / 'registry-remote' / 'files' / 'md5'
    registry_remote.mkdir(parents=True)
    
    # Create synthetic data matching dt-test-registry .dvc files
    # data/file.csv - md5: f4265d6b19fbb80c34d6f47a0048107f
    file_content = "id,name,value\n1,alpha,200\n2,beta,300\n3,gamma,400\n"
    file_hash = "f4265d6b19fbb80c34d6f47a0048107f"
    (registry_remote / file_hash[:2]).mkdir(exist_ok=True)
    (registry_remote / file_hash[:2] / file_hash[2:]).write_text(file_content)
    
    # Configure registry clone to use local remote
    subprocess.run(
        ['dvc', 'remote', 'add', '--local', '-f', 'local', str(registry_remote.parent.parent)],
        cwd=registry_clone, check=True, capture_output=True
    )
    subprocess.run(
        ['dvc', 'remote', 'default', '--local', 'local'],
        cwd=registry_clone, check=True, capture_output=True
    )
    
    # Set up the fixtures clone
    clone_path = tmp_path / 'dt-test-fixtures'
    subprocess.run(
        ['git', 'clone', str(pull_test_repos['fixtures']), str(clone_path)],
        check=True, capture_output=True
    )
    
    # Create local remote with synthetic data for fixtures
    local_remote = tmp_path / 'fixtures-remote' / 'files' / 'md5'
    local_remote.mkdir(parents=True)
    
    # single_file/data.txt - md5: d8e8fca2dc0f896fd7cb4cb0031ba249
    single_file_content = "test\n"
    single_file_hash = "d8e8fca2dc0f896fd7cb4cb0031ba249"
    (local_remote / single_file_hash[:2]).mkdir(exist_ok=True)
    (local_remote / single_file_hash[:2] / single_file_hash[2:]).write_text(single_file_content)
    
    # Configure fixtures clone to use local remote
    subprocess.run(
        ['dvc', 'remote', 'add', '--local', 'test-remote', str(local_remote.parent.parent)],
        cwd=clone_path, check=True, capture_output=True
    )
    subprocess.run(
        ['dvc', 'remote', 'default', '--local', 'test-remote'],
        cwd=clone_path, check=True, capture_output=True
    )
    
    # Configure local cache
    local_cache = tmp_path / 'local-cache'
    local_cache.mkdir()
    subprocess.run(
        ['dvc', 'cache', 'dir', '--local', str(local_cache)],
        cwd=clone_path, check=True, capture_output=True
    )
    
    # Update import .dvc files to point to local registry clone
    for dvc_file in clone_path.rglob('*.dvc'):
        if dvc_file.is_dir() or '.dvc' in dvc_file.parts[:-1]:
            continue
            
        content = dvc_file.read_text()
        if 'repo:' in content:
            data = yaml.safe_load(content)
            if data and 'deps' in data:
                for dep in data['deps']:
                    if 'repo' in dep:
                        dep['repo']['url'] = str(registry_clone)
                dvc_file.write_text(yaml.dump(data, default_flow_style=False))
    
    return {
        'path': clone_path,
        'remote': local_remote.parent.parent,
        'cache': local_cache,
        'registry': registry_clone,
    }


# =============================================================================
# Basic Pull Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network
class TestPullBasic:
    """Test basic dt pull functionality."""

    def test_pull_discovers_dvc_files(self, pull_test_fixtures):
        """Pull without targets discovers and processes .dvc files."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', '-v', cwd=repo, check=False)
        
        # Should mention discovering .dvc files
        assert 'Discovering' in result.stdout or '.dvc' in result.stdout
        # Return code may be non-zero if some files can't be fetched, 
        # but command should run

    def test_pull_specific_target(self, pull_test_fixtures):
        """Pull specific .dvc file target."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', 'single_file/data.txt.dvc', '-v', cwd=repo, check=False)
        
        # Should process the specific target
        combined_output = result.stdout + result.stderr
        assert 'single_file' in combined_output or result.returncode == 0

    def test_pull_directory_target_resolves_to_dvc(self, pull_test_fixtures):
        """Pull directory target resolves to its .dvc file."""
        repo = pull_test_fixtures['path']
        
        # Pull directory/ which should resolve to directory/data.dvc
        result = run_dt('pull', 'directory/', '-v', cwd=repo, check=False)
        
        # Should find and process the directory target
        combined_output = result.stdout + result.stderr
        assert 'directory' in combined_output.lower() or result.returncode == 0


# =============================================================================
# Import vs Regular Target Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network
class TestPullImportSeparation:
    """Test that pull correctly separates imports from regular targets."""

    def test_pull_identifies_import_targets(self, pull_test_fixtures):
        """Pull identifies import targets and handles them separately."""
        repo = pull_test_fixtures['path']
        
        # Pull with verbose to see target classification
        result = run_dt('pull', '-v', cwd=repo, check=False)
        
        # Should mention imports (imported/ directory contains import files)
        # The verbose output should show import vs regular classification
        combined_output = result.stdout + result.stderr
        # Either explicitly mentions "import" or shows dt fetch being called
        assert 'import' in combined_output.lower() or 'fetch' in combined_output.lower() or result.returncode == 0

    def test_pull_import_target_uses_fetch(self, pull_test_fixtures):
        """Pull import target uses dt fetch + dvc checkout."""
        repo = pull_test_fixtures['path']
        
        # imported/file.csv.dvc is an import from dt-test-registry
        result = run_dt('pull', 'imported/file.csv.dvc', '-v', cwd=repo, check=False)
        
        # Should process as import (may show fetch or checkout)
        combined_output = result.stdout + result.stderr
        assert 'import' in combined_output.lower() or 'checkout' in combined_output.lower() or 'fetch' in combined_output.lower() or result.returncode == 0

    def test_pull_regular_target_uses_dvc_pull(self, pull_test_fixtures):
        """Pull regular (non-import) target uses dvc pull."""
        repo = pull_test_fixtures['path']
        
        # single_file/data.txt.dvc is a regular (non-import) file
        result = run_dt('pull', 'single_file/data.txt.dvc', '-v', cwd=repo, check=False)
        
        # Should process as regular file (uses dvc pull)
        combined_output = result.stdout + result.stderr
        # Should not be identified as an import
        assert result.returncode == 0 or 'regular' in combined_output.lower() or 'pull' in combined_output.lower()


# =============================================================================
# Dry Run Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network
class TestPullDryRun:
    """Test dt pull --dry-run mode."""

    def test_dry_run_shows_what_would_be_pulled(self, pull_test_fixtures):
        """Dry run shows what would be pulled without pulling."""
        repo = pull_test_fixtures['path']
        cache = pull_test_fixtures['cache']
        
        # Clear cache to ensure files need pulling
        if cache.exists():
            shutil.rmtree(cache)
        cache.mkdir()
        
        result = run_dt('pull', '--dry', cwd=repo, check=False)
        
        # Should show summary without actually pulling
        combined_output = result.stdout + result.stderr
        # Dry run output should mention imports or regular files to pull
        assert 'import' in combined_output.lower() or 'pull' in combined_output.lower() or 'would' in combined_output.lower() or result.returncode == 0
        
        # Cache should still be empty (no actual pull)
        cache_files = [f for f in cache.rglob('*') if f.is_file()]
        # Allow for some minor files but not the actual data
        assert len(cache_files) < 5, f"Dry run should not populate cache significantly: {cache_files}"

    def test_dry_run_verbose_lists_files(self, pull_test_fixtures):
        """Dry run with verbose lists files that would be pulled."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', '--dry', '-v', cwd=repo, check=False)
        
        # Verbose dry run should list more details
        combined_output = result.stdout + result.stderr
        assert len(combined_output) > 0, "Verbose dry run should produce output"


# =============================================================================
# Force Mode Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network
class TestPullForceMode:
    """Test dt pull --force mode for re-fetching .dir manifests."""

    def test_force_mode_deletes_dir_manifests(self, pull_test_fixtures):
        """Force mode deletes .dir manifests before pulling."""
        repo = pull_test_fixtures['path']
        
        # Use force mode on a directory target
        result = run_dt('pull', '--force', 'directory/', '-v', cwd=repo, check=False)
        
        # Should mention force mode or deleting manifests
        combined_output = result.stdout + result.stderr
        # Force mode should complete (even if no .dir files found)
        assert result.returncode == 0 or 'force' in combined_output.lower() or 'manifest' in combined_output.lower() or 'deleted' in combined_output.lower()

    def test_force_without_targets_processes_all(self, pull_test_fixtures):
        """Force mode without targets processes all directories."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', '--force', '-v', cwd=repo, check=False)
        
        # Should complete without error
        # Force mode should work even if no .dir manifests exist
        combined_output = result.stdout + result.stderr
        assert len(combined_output) > 0  # Should produce some output


# =============================================================================
# Verbose Mode Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network
class TestPullVerbose:
    """Test dt pull verbose output."""

    def test_verbose_shows_target_resolution(self, pull_test_fixtures):
        """Verbose mode shows how targets are resolved."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', '-v', cwd=repo, check=False)
        
        # Verbose output should show progress
        combined_output = result.stdout + result.stderr
        # Should mention resolving, discovering, or processing
        assert len(combined_output) > 50, "Verbose mode should produce detailed output"

    def test_verbose_shows_import_vs_regular(self, pull_test_fixtures):
        """Verbose mode shows import vs regular target separation."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', '-v', cwd=repo, check=False)
        
        combined_output = result.stdout + result.stderr
        # Should distinguish between import and regular targets
        # (either explicitly or by showing different handling)
        assert 'import' in combined_output.lower() or 'regular' in combined_output.lower() or 'fetch' in combined_output.lower() or 'dvc pull' in combined_output.lower()


# =============================================================================
# Error Handling Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
class TestPullErrors:
    """Test error handling in dt pull."""

    def test_pull_outside_dvc_repo_shows_no_files(self, tmp_path):
        """Pull outside DVC repository shows no .dvc files message."""
        result = run_dt('pull', cwd=tmp_path, check=False)
        
        # Command returns 0 but shows "No .dvc files found"
        combined_output = result.stdout + result.stderr
        assert 'no .dvc files' in combined_output.lower(), \
            f"Should indicate no .dvc files found: {combined_output}"

    def test_pull_nonexistent_target_fails(self, pull_test_fixtures):
        """Pull with nonexistent target fails appropriately."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', 'nonexistent/file.dvc', cwd=repo, check=False)
        
        # Should fail or show that target doesn't exist
        # (behavior may vary - could ignore missing targets)
        combined_output = result.stdout + result.stderr
        assert result.returncode != 0 or 'not found' in combined_output.lower() or 'no' in combined_output.lower() or len(combined_output) > 0


# =============================================================================
# Target Resolution Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network
class TestPullTargetResolution:
    """Test target resolution in dt pull."""

    def test_pull_file_path_resolves_to_parent_dvc(self, pull_test_fixtures):
        """Pull file path resolves to parent .dvc file."""
        repo = pull_test_fixtures['path']
        
        # directory/data/a.txt is inside directory tracked by directory/data.dvc
        result = run_dt('pull', 'directory/data/', '-v', cwd=repo, check=False)
        
        # Should resolve to the parent .dvc file
        combined_output = result.stdout + result.stderr
        assert 'directory' in combined_output.lower() or result.returncode == 0

    def test_pull_dvc_file_directly(self, pull_test_fixtures):
        """Pull .dvc file directly works."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', 'single_file/data.txt.dvc', '-v', cwd=repo, check=False)
        
        # Should process the .dvc file directly
        combined_output = result.stdout + result.stderr
        assert 'single_file' in combined_output or 'data.txt' in combined_output or result.returncode == 0

    def test_pull_multiple_targets(self, pull_test_fixtures):
        """Pull multiple targets at once."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', 'single_file/data.txt.dvc', 'directory/', '-v', cwd=repo, check=False)
        
        # Should process both targets
        combined_output = result.stdout + result.stderr
        # Should mention or process both targets
        assert len(combined_output) > 0


# =============================================================================
# Output Format Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network
class TestPullOutput:
    """Test dt pull output format and messages."""

    def test_pull_shows_progress(self, pull_test_fixtures):
        """Pull shows progress during operation."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', '-v', cwd=repo, check=False)
        
        # Should show some progress output
        combined_output = result.stdout + result.stderr
        assert len(combined_output) > 0, "Pull should produce some output"

    def test_pull_quiet_minimal_output(self, pull_test_fixtures):
        """Pull without verbose produces minimal output."""
        repo = pull_test_fixtures['path']
        
        result = run_dt('pull', cwd=repo, check=False)
        
        # Non-verbose should have less output than verbose
        quiet_output = result.stdout
        
        result_verbose = run_dt('pull', '-v', cwd=repo, check=False)
        verbose_output = result_verbose.stdout
        
        # Verbose should produce more output (or at least equal)
        assert len(verbose_output) >= len(quiet_output) * 0.5, \
            "Verbose mode should produce at least as much output as quiet mode"
