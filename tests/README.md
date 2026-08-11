# Tests

This directory contains all tests for the `dt` package.

## Structure

```
tests/
    conftest.py              # Shared pytest configuration and fixtures
    README.md                # This file
    
    unit/                    # Mostly-mocked tests (~1880 tests)
        conftest.py          # Unit-specific fixtures
        test_*.py            # Unit test modules
        unit_tests.md        # Unit test checklist
    
    integration/             # End-to-end CLI tests (real git/DVC)
        conftest.py          # Integration fixtures (real repos)
        test_*_cmd.py        # Integration test modules
        integration_tests.md # Integration test checklist
    
    fixtures/                # Shared test data and helpers
        __init__.py          # Fixture utilities
```

## Running Tests

### All Tests

```bash
pytest tests/
```

### Unit Tests Only (Fast)

```bash
pytest tests/unit/
```

### Integration Tests Only

```bash
pytest tests/integration/
```

### Using Markers

```bash
# Skip slow tests
pytest -m "not slow"

# Skip integration tests
pytest -m "not integration"

# Only tests that don't require network
pytest -m "not requires_network"

# Same, but via the environment variable the network tests also honour
DT_TEST_OFFLINE=1 pytest tests/
```

The full integration suite takes about an hour here, most of it spawning real
`dvc` subprocesses. Skipping the network tests cuts out the GitHub clones.

## Test Categories

### Unit Tests (`tests/unit/`)

- **Fast**: A few minutes total, versus about an hour for integration
- **Isolated**: Use mocked dependencies
- **No external requirements**: Don't need DVC/git installed
- **High coverage**: Test internal functions in isolation

See [unit/unit_tests.md](unit/unit_tests.md) for the complete checklist.

### Integration Tests (`tests/integration/`)

- **End-to-end**: Test complete CLI commands
- **Real dependencies**: Use actual git/DVC operations
- **Environment-dependent**: May skip if tools not installed
- **Realistic scenarios**: Match actual user workflows

See [integration/integration_tests.md](integration/integration_tests.md) for the complete checklist.

## Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.integration` | End-to-end tests |
| `@pytest.mark.requires_dvc` | Requires DVC to be installed |
| `@pytest.mark.requires_git` | Requires git to be installed |
| `@pytest.mark.requires_network` | Requires network access |
| `@pytest.mark.requires_qxub` | Requires qxub (HPC environment only) |
| `@pytest.mark.slow` | Tests that take > 1 second |

## Fixtures

### Shared Fixtures (`tests/conftest.py`)

- `project_root` - Path to project root
- `cli_runner` - Click CLI test runner
- `isolated_dir` - Isolated temp directory

### Unit Fixtures (`tests/unit/conftest.py`)

- `isolate_from_working_repo` - **autouse**; chdirs every unit test to a neutral
  temp directory. Most of `dt` resolves the project root from the working
  directory, so without this a test that forgets to chdir silently reads the
  real `dvc_tools` checkout — and opens its DVC state database on shared
  storage. Tests that need a project still chdir into their own fixture.
- `mock_dvc_repo` - Mock DVC repo structure
- `sample_dvc_files` - Sample .dvc files
- `cache_structure` - DVC cache directory
- `cache_with_files` - Cache with sample files
- `isolated_config` - Isolated config environment

### Integration Fixtures (`tests/integration/conftest.py`)

- `git_repo` - Real git repository
- `dvc_repo` - Real DVC repository
- `dvc_repo_with_files` - DVC repo with tracked files
- `dvc_repo_with_cache` - DVC repo with external cache
- `dvc_repo_with_remote` - DVC repo with local remote
- `dt_test_fixtures_clone` - Clone of test fixtures repo
- `run_dt` - Function to run dt commands
- `rewrite_tracked(path, text)` - Helper (not a fixture). Use it instead of
  `path.write_text()` on a DVC-tracked file: under `cache.type=hardlink,symlink`
  the workspace file is a read-only link into the cache, so writing through it
  fails — and if it were writable, it would corrupt the shared cache object.

## Test Data Repositories

Two external repositories provide realistic DVC scenarios:

| Repository | Purpose | URL |
|------------|---------|-----|
| dt-test-fixtures | Main test repo with diverse patterns | https://github.com/swarbricklab/dt-test-fixtures |
| dt-test-registry | Source repo for import testing | https://github.com/swarbricklab/dt-test-registry |

### DVC Tracking Patterns Covered

| Pattern | Example | Detection |
|---------|---------|-----------|
| Single file via .dvc | `data.csv.dvc` | Has `outs:` with `path:` |
| Directory via .dvc | `dir.dvc` | Has `outs:` with `.dir` hash |
| Import from repo | `imported/file.csv.dvc` | Has `deps:` with `repo.url` |
| Pipeline output | `pipeline/dvc.lock` | Referenced in `dvc.yaml` |

## Environment Variables

| Variable | Effect |
|----------|--------|
| `DT_TEST_OFFLINE` | Skip tests requiring network access |

## Writing Tests

### Unit Tests

```python
"""Unit test example."""
from unittest.mock import patch, MagicMock

def test_my_function(mock_dvc_repo):
    """Test with mocked DVC repo."""
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        # Test code here
```

### Integration Tests

```python
"""Integration test example."""
import pytest

@pytest.mark.integration
@pytest.mark.requires_dvc
def test_dt_command(run_dt, dvc_repo_with_files):
    """Test dt command end-to-end."""
    result = run_dt('ls')
    assert result.returncode == 0
    assert 'data.csv' in result.stdout
```
