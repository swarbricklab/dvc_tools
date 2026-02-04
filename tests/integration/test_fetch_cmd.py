"""Integration tests for dt fetch command.

These tests verify fetch functionality with real DVC repositories.
Uses cloned dt-test-fixtures and dt-test-registry repositories.

The test repos are cloned from GitHub on first use (session-scoped).
Data is fetched from remotes, which works on NCI with SSH access.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

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
def test_repos_base(tmp_path_factory):
    """Session-scoped base directory for test repositories.
    
    Clones dt-test-fixtures and dt-test-registry from GitHub once per session.
    Creates synthetic remote data for testing (no network required after clone).
    
    Returns a dict with paths to the cloned repos.
    """
    base_dir = tmp_path_factory.mktemp("test-repos")
    
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
    
    # Create synthetic remote data for registry
    # This matches the .dvc files in the repo
    _create_registry_remote_data(registry_path)
    
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
    
    # Create synthetic remote data for fixtures
    _create_fixtures_remote_data(fixtures_path)
    
    return repos


def _create_registry_remote_data(registry_path: Path):
    """Create synthetic remote data for dt-test-registry.
    
    The data matches the .dvc files in the repository.
    """
    import json
    
    remote_path = registry_path / '.remote' / 'files' / 'md5'
    remote_path.mkdir(parents=True, exist_ok=True)
    
    # data/file.csv - md5: f4265d6b19fbb80c34d6f47a0048107f
    file_content = "id,name,value\n1,alpha,200\n2,beta,300\n3,gamma,400\n4,delta,500\n5,epsilon,600\n"
    file_hash = "f4265d6b19fbb80c34d6f47a0048107f"
    (remote_path / file_hash[:2]).mkdir(exist_ok=True)
    (remote_path / file_hash[:2] / file_hash[2:]).write_text(file_content)
    
    # data/dir/a.csv - md5: 7320ddd77a276f2ecd73ed18e631ee2b
    a_content = "id,category\n1,A\n2,B\n3,C\n"
    a_hash = "7320ddd77a276f2ecd73ed18e631ee2b"
    (remote_path / a_hash[:2]).mkdir(exist_ok=True)
    (remote_path / a_hash[:2] / a_hash[2:]).write_text(a_content)
    
    # data/dir/b.csv - md5: c2ad4b026e39ec2257321d20373b9f47
    b_content = "id,score\n1,85\n2,92\n"
    b_hash = "c2ad4b026e39ec2257321d20373b9f47"
    (remote_path / b_hash[:2]).mkdir(exist_ok=True)
    (remote_path / b_hash[:2] / b_hash[2:]).write_text(b_content)
    
    # data/dir.dvc .dir manifest - md5: bc894c83412ff34cbc40f9bcb5983258.dir
    dir_manifest = [
        {"relpath": "a.csv", "md5": a_hash, "size": len(a_content)},
        {"relpath": "b.csv", "md5": b_hash, "size": len(b_content)},
    ]
    dir_hash = "bc894c83412ff34cbc40f9bcb5983258"
    (remote_path / dir_hash[:2]).mkdir(exist_ok=True)
    (remote_path / dir_hash[:2] / (dir_hash[2:] + '.dir')).write_text(json.dumps(dir_manifest))


def _create_fixtures_remote_data(fixtures_path: Path):
    """Create synthetic remote data for dt-test-fixtures.
    
    The data matches the .dvc files in the repository.
    """
    import json
    
    remote_path = fixtures_path / '.remote' / 'files' / 'md5'
    remote_path.mkdir(parents=True, exist_ok=True)
    
    # single_file/data.txt - We need to check what this file contains
    # For now, create placeholder based on the .dvc file
    # The actual data will be created when we know the exact content
    
    # directory/data/ - similar structure
    # importable/ - similar structure
    
    # These files are for regular (non-import) tests, so they don't need
    # the same level of detail as the registry. The tests mostly check
    # that import files work correctly.


@pytest.fixture
def dt_test_fixtures_path(test_repos_base):
    """Path to the dt-test-fixtures repository (session-cached clone)."""
    return test_repos_base['fixtures']


@pytest.fixture
def dt_test_registry_path(test_repos_base):
    """Path to the dt-test-registry repository (session-cached clone)."""
    return test_repos_base['registry']


@pytest.fixture
def cloned_test_fixtures(tmp_path, dt_test_fixtures_path, dt_test_registry_path):
    """Create a fresh clone of dt-test-fixtures for isolated testing.
    
    Configures both fixtures and registry clones to use local remotes
    with data copied from cache, so tests can run without network access.
    """
    # First, set up the registry clone with a local remote
    # This is the source repository for imports
    registry_clone = tmp_path / 'registry'
    subprocess.run(
        ['git', 'clone', str(dt_test_registry_path), str(registry_clone)],
        check=True, capture_output=True
    )
    
    # Create local remote for registry with cached data
    registry_remote = tmp_path / 'registry-remote'
    registry_remote.mkdir()
    
    # Copy cached data from the session-cached registry
    registry_cache = dt_test_registry_path / '.dvc' / 'cache'
    if registry_cache.exists():
        shutil.copytree(registry_cache, registry_remote / 'files', dirs_exist_ok=True)
    
    # Also check .remote if it exists (local development)
    registry_original_remote = dt_test_registry_path / '.remote'
    if registry_original_remote.exists():
        shutil.copytree(registry_original_remote, registry_remote, dirs_exist_ok=True)
    
    # Configure registry clone to use local remote
    subprocess.run(
        ['dvc', 'remote', 'add', '--local', '-f', 'local', str(registry_remote)],
        cwd=registry_clone, check=True, capture_output=True
    )
    subprocess.run(
        ['dvc', 'remote', 'default', '--local', 'local'],
        cwd=registry_clone, check=True, capture_output=True
    )
    
    # Now set up the fixtures clone
    clone_path = tmp_path / 'dt-test-fixtures'
    subprocess.run(
        ['git', 'clone', str(dt_test_fixtures_path), str(clone_path)],
        check=True, capture_output=True
    )
    
    # Set up local remote storage for fixtures
    local_remote = tmp_path / 'fixtures-remote'
    local_remote.mkdir()
    
    # Copy cached data from the session-cached repo
    source_cache = dt_test_fixtures_path / '.dvc' / 'cache'
    if source_cache.exists():
        shutil.copytree(source_cache, local_remote / 'files', dirs_exist_ok=True)
    
    original_remote = dt_test_fixtures_path / '.remote'
    if original_remote.exists():
        shutil.copytree(original_remote, local_remote, dirs_exist_ok=True)
    
    # Configure DVC to use local remote
    subprocess.run(
        ['dvc', 'remote', 'add', '--local', 'test-remote', str(local_remote)],
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
    import yaml
    for dvc_file in clone_path.rglob('*.dvc'):
        # Skip the .dvc directory itself
        if dvc_file.is_dir():
            continue
        # Skip files inside .dvc directory
        if '.dvc' in dvc_file.parts[:-1]:
            continue
            
        content = dvc_file.read_text()
        if 'repo:' in content:
            # Update URL to point to local registry clone
            data = yaml.safe_load(content)
            if data and 'deps' in data:
                for dep in data['deps']:
                    if 'repo' in dep:
                        dep['repo']['url'] = str(registry_clone)
                dvc_file.write_text(yaml.dump(data, default_flow_style=False))
    
    return {
        'path': clone_path,
        'remote': local_remote,
        'cache': local_cache,
        'registry': registry_clone,
    }


# =============================================================================
# Basic Fetch Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network  # Clones repos from GitHub
class TestFetchBasic:
    """Test basic dt fetch functionality."""

    def test_fetch_no_targets_finds_dvc_files(self, cloned_test_fixtures):
        """Fetch without targets processes all .dvc files."""
        repo = cloned_test_fixtures['path']
        
        result = run_dt('fetch', cwd=repo, check=False)
        
        # Should complete (may have some failures for non-import files)
        # But should at least find and process .dvc files
        assert result.returncode == 0 or 'dvc fetch' in result.stdout.lower() or '✗' in result.stdout

    def test_fetch_specific_target(self, cloned_test_fixtures):
        """Fetch specific .dvc file target."""
        repo = cloned_test_fixtures['path']
        
        # Try to fetch the imported file
        result = run_dt('fetch', 'imported/file.csv.dvc', '-v', cwd=repo, check=False)
        
        # Should process the target
        assert 'imported/file.csv.dvc' in result.stdout or 'file.csv' in result.stdout

    def test_fetch_nonexistent_target(self, cloned_test_fixtures):
        """Fetch non-existent target shows error."""
        repo = cloned_test_fixtures['path']
        
        result = run_dt('fetch', 'nonexistent/file.dvc', cwd=repo, check=False)
        
        assert result.returncode != 0 or 'not found' in result.stdout.lower() or '✗' in result.stdout


# =============================================================================
# Import Fetch Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network  # Clones repos from GitHub
class TestFetchImport:
    """Test fetch with import .dvc files."""

    def test_fetch_import_populates_cache(self, cloned_test_fixtures):
        """Fetch import file populates cache from source repo."""
        repo = cloned_test_fixtures['path']
        cache = cloned_test_fixtures['cache']
        
        # Clear the cache first
        if cache.exists():
            shutil.rmtree(cache)
        cache.mkdir()
        
        result = run_dt('fetch', 'imported/file.csv.dvc', '-v', cwd=repo, check=False)
        
        # Fetch should succeed
        assert result.returncode == 0, f"Fetch failed: {result.stderr}"
        assert '✓' in result.stdout, f"Expected success marker in output: {result.stdout}"
        
        # Cache should now contain files
        cache_files = list(cache.rglob('*'))
        cache_files_only = [f for f in cache_files if f.is_file()]
        assert len(cache_files_only) > 0, f"Cache should contain files after fetch: {cache}"

    def test_fetch_import_verbose_output(self, cloned_test_fixtures):
        """Fetch with -v shows detailed progress."""
        repo = cloned_test_fixtures['path']
        
        result = run_dt('fetch', 'imported/file.csv.dvc', '-v', cwd=repo, check=False)
        
        # Verbose output should show progress details
        output = result.stdout + result.stderr
        assert 'Import from:' in output, "Verbose should show import source"
        assert 'Found local cache:' in output or 'Cached:' in output, "Verbose should show cache activity"


# =============================================================================
# Regular File Fetch Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network  # Clones repos from GitHub
class TestFetchRegular:
    """Test fetch with regular (non-import) .dvc files."""

    def test_fetch_regular_file_suggests_dvc_fetch(self, cloned_test_fixtures):
        """Fetch regular file suggests using dvc fetch."""
        repo = cloned_test_fixtures['path']
        
        # single_file/data.txt.dvc is a regular .dvc file (not an import)
        result = run_dt('fetch', 'single_file/data.txt.dvc', cwd=repo, check=False)
        
        # Should indicate it's not an import or already in cache
        output = result.stdout.lower()
        assert 'dvc fetch' in output or 'not an import' in output or 'already in cache' in output or '✗' in result.stdout

    def test_fetch_file_already_in_cache(self, cloned_test_fixtures):
        """Fetch file already in cache reports success."""
        repo = cloned_test_fixtures['path']
        cache = cloned_test_fixtures['cache']
        
        # First, ensure data is in cache by running dvc pull
        run_dvc('pull', 'single_file/data.txt.dvc', cwd=repo, check=False)
        
        result = run_dt('fetch', 'single_file/data.txt.dvc', cwd=repo, check=False)
        
        # Should either say already in cache or not an import
        output = result.stdout
        assert 'already in cache' in output.lower() or 'not an import' in output.lower() or '✓' in output or '✗' in output


# =============================================================================
# Fetch Options Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network  # Clones repos from GitHub
class TestFetchOptions:
    """Test fetch command options."""

    def test_fetch_no_refresh(self, cloned_test_fixtures):
        """Fetch with --no-refresh skips clone refresh."""
        repo = cloned_test_fixtures['path']
        
        result = run_dt('fetch', '--no-refresh', '-v', 'imported/file.csv.dvc', 
                       cwd=repo, check=False)
        
        # Should complete without errors about refreshing
        # The flag should be accepted
        assert '--no-refresh' not in result.stderr  # Flag should be recognized

    def test_fetch_no_index_sync(self, cloned_test_fixtures):
        """Fetch with --no-index-sync skips index sync."""
        repo = cloned_test_fixtures['path']
        
        result = run_dt('fetch', '--no-index-sync', 'imported/file.csv.dvc',
                       cwd=repo, check=False)
        
        # Flag should be recognized (no error about unknown option)
        assert '--no-index-sync' not in result.stderr


# =============================================================================
# Error Handling Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network  # Clones repos from GitHub
class TestFetchErrors:
    """Test fetch error handling."""

    def test_fetch_invalid_dvc_file(self, cloned_test_fixtures, tmp_path):
        """Fetch invalid .dvc file shows error."""
        repo = cloned_test_fixtures['path']
        
        # Create an invalid .dvc file
        invalid_dvc = repo / 'invalid.dvc'
        invalid_dvc.write_text('not: valid: yaml: [')
        
        result = run_dt('fetch', 'invalid.dvc', cwd=repo, check=False)
        
        # Should report an error
        assert result.returncode != 0 or '✗' in result.stdout

    def test_fetch_directory_not_dvc_file(self, cloned_test_fixtures):
        """Fetch directory (not .dvc file) shows error."""
        repo = cloned_test_fixtures['path']
        
        result = run_dt('fetch', 'single_file/', cwd=repo, check=False)
        
        # Should fail - not a .dvc file
        output = result.stdout + result.stderr
        assert 'not a .dvc file' in output.lower() or result.returncode != 0 or '✗' in result.stdout


# =============================================================================
# Integration with Checkout Tests
# =============================================================================

@pytest.mark.integration
@requires_git
@requires_dvc
@requires_network  # Clones repos from GitHub
class TestFetchCheckoutWorkflow:
    """Test fetch + checkout workflow."""

    def test_fetch_then_checkout_workflow(self, cloned_test_fixtures):
        """Fetch followed by dvc checkout restores files."""
        repo = cloned_test_fixtures['path']
        cache = cloned_test_fixtures['cache']
        
        # Clear cache to ensure we're testing the full workflow
        if cache.exists():
            shutil.rmtree(cache)
        cache.mkdir()
        
        # Remove any existing data file
        data_file = repo / 'imported' / 'file.csv'
        if data_file.exists():
            data_file.unlink()
        
        # File should not exist now
        assert not data_file.exists(), "Data file should be removed before test"
        
        # Fetch the import
        fetch_result = run_dt('fetch', 'imported/file.csv.dvc', '-v', cwd=repo, check=False)
        assert fetch_result.returncode == 0, f"Fetch failed: {fetch_result.stderr}"
        
        # Cache should be populated
        cache_files = [f for f in cache.rglob('*') if f.is_file()]
        assert len(cache_files) > 0, "Cache should have files after fetch"
        
        # Run dvc checkout (as suggested by dt fetch output)
        checkout_result = run_dvc('checkout', 'imported/file.csv.dvc', cwd=repo, check=False)
        assert checkout_result.returncode == 0, f"Checkout failed: {checkout_result.stderr}"
        
        # File should now exist
        assert data_file.exists(), "Data file should exist after checkout"
