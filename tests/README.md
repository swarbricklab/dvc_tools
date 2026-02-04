# DVC Tools Test Suite

## Test Infrastructure

### Test Repositories

Two external repositories provide realistic DVC tracking scenarios for integration tests:

#### dt-test-registry
**URL:** `https://github.com/swarbricklab/dt-test-registry`

Source repository for import testing. Contains versioned data across three tags:

| Tag | Content Changes |
|-----|-----------------|
| v1.0 | Initial: file.csv, dir/, nested/deep/path/, pipeline/ |
| v2.0 | Modified file.csv, added file to dir/ |
| v3.0 | Further modifications |

**Structure:**
```
data/
  file.csv          # Single tracked file
  dir/              # Tracked directory
    a.csv
    b.csv
nested/deep/path/
  data.csv          # Deep nesting test
pipeline/
  dvc.yaml          # Pipeline with transform stage
  dvc.lock
.cache/             # Local cache (gitignored)
.remote/            # Local remote (gitignored)
```

#### dt-test-fixtures
**URL:** `https://github.com/swarbricklab/dt-test-fixtures`

Main test repository with diverse DVC tracking patterns:

**Structure:**
```
single_file/
  data.csv.dvc      # File tracked via .dvc
importable/
  file.csv.dvc      # Another tracked file
  dir.dvc           # Directory tracked via .dvc
imported/
  file.csv.dvc      # Import from dt-test-registry (has deps.repo)
  dir.dvc           # Directory import
pipeline/
  dvc.yaml          # Pipeline stage
  dvc.lock          # Output tracked via lock file
.cache/             # Local cache (gitignored)
.remote/            # Local remote (gitignored)
```

### DVC Tracking Patterns Covered

| Pattern | Example | Detection |
|---------|---------|-----------|
| Single file via .dvc | `data.csv.dvc` | Has `outs:` with `path:` |
| Directory via .dvc | `dir.dvc` | Has `outs:` with `.dir` hash |
| Import from repo | `imported/file.csv.dvc` | Has `deps:` with `repo.url` |
| Pipeline output | `pipeline/dvc.lock` | Referenced in `dvc.yaml` |

## Test Categories

### Unit Tests

Located in `test_*.py` files. Test internal functions in isolation:

- **test_config.py** - Configuration loading, scopes, CLI
- **test_pull.py** - Path resolution, partitioning, file discovery
- **test_fetch.py** - Cache population, import handling

Run with:
```bash
pytest tests/ -v
```

### Integration Tests (Future)

Will use the test repositories for end-to-end testing:

```bash
# Clone fresh for each test run
git clone https://github.com/swarbricklab/dt-test-fixtures /tmp/test-fixtures
cd /tmp/test-fixtures
dt pull --verbose
```

## Local Testing Setup

Both test repos have `.cache/` and `.remote/` directories (gitignored) for offline testing without network access:

```bash
# In dt-test-registry or dt-test-fixtures
dvc remote add -d local .remote
dvc config cache.dir .cache
dvc push  # Populates local remote
```

## Running Tests

```bash
# All tests
pytest tests/ -v

# Specific module
pytest tests/test_pull.py -v

# With coverage
pytest tests/ --cov=dt --cov-report=term-missing
```
