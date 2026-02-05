# dt pull

Pull DVC-tracked files, automatically handling imports from other repositories.

## Usage

```bash
dt pull [options] [targets...]
```

## What it does

A smart pull that handles both regular DVC files and imports:

1. **Resolve targets** to their tracking `.dvc` files
2. **Separate imports**: Targets tracked by `.dvc` files with `deps.repo` are handled via `dt fetch` + `dvc checkout`
3. **Pull remaining**: Other targets are pulled via `dvc pull` (or parallel workers)

This enables pulling data from repositories that were imported with `dvc import`, even when you don't have direct access to their remote storage.

## Options

| Option | Description |
|--------|-------------|
| `--dry`, `--dry-run` | Show what would be pulled without actually pulling |
| `-v`, `--verbose` | Show detailed progress (with `--dry`, lists all files) |
| `-w N`, `--workers N` | Distribute pull across N compute nodes via qxub |
| `-r NAME`, `--remote NAME` | Pull from specific remote |
| `-f`, `--force` | Delete `.dir` manifests before pulling to force re-fetch |
| `--no-wait` | Submit worker jobs and exit without waiting for completion |

All other options are passed through to `dvc pull`. See `dvc pull --help` for available options.

## Examples

### Basic usage

```bash
# Pull all tracked files (imports handled automatically)
dt pull

# Pull specific targets
dt pull data/imported.dvc data/local.dvc

# Pull a directory (resolves to its .dvc file)
dt pull data/

# Pull with verbose output
dt pull -v

# Force re-fetch (after fixing corrupted cache files)
dt pull --force data/
```

### Dry run

Preview what would be pulled without actually transferring:

```bash
# Summary view - shows imports and regular files separately
dt pull --dry
# Output:
# Imports to fetch (2):
#   data/external.dvc → dt fetch data/external.dvc
#   models/pretrained.dvc → dt fetch models/pretrained.dvc
# Would pull 15 regular file(s), 850.0 MB

# Detailed list with file paths
dt pull --dry -v
# Output:
# Imports to fetch (2):
#   ...
# Regular files to pull (15 files, 850.0 MB):
#   data/processed.csv  (abc123...)
#   models/output.pkl   (def456...)
#   ...

# Preview worker distribution
dt pull --dry -w 8
# Output:
# Imports to fetch (2):
#   ...
# Would pull 15 regular file(s), 850.0 MB
# 
# With 8 workers:
#   Worker 0: 2 file(s)
#   Worker 3: 3 file(s)
#   ...
```

### Parallel pull with qxub

For large datasets, distribute the pull across multiple compute nodes:

```bash
# Pull using 16 parallel workers
dt pull --workers 16

# Pull from specific remote with workers
dt pull -w 8 -r myremote

# Submit jobs and exit without waiting
dt pull -w 16 --no-wait
```

> **Note:** Imports are always handled first (via `dt fetch` + `dvc checkout`) before parallel workers are submitted for regular files.

## Target Resolution

Each target is resolved to its tracking `.dvc` file:

| Target | Resolves to |
|--------|-------------|
| `data.dvc` | `data.dvc` |
| `data/` | `data.dvc` (if exists) |
| `data/subdir/file.txt` | `data.dvc` (parent dir tracking) |
| `models/output.pkl` | None (if tracked by `dvc.yaml`) |

If the resolved `.dvc` file has a `deps.repo` section (indicating an import), the target is handled via `dt fetch` + `dvc checkout`. Otherwise, it's passed to `dvc pull`.

## How it works

### Step 1: Resolve targets

Without targets, scans for all `.dvc` files. With targets, resolves each to its tracking `.dvc` file (if any).

### Step 2: Separate imports from regular files

Each `.dvc` file is checked for a `deps` section with a `repo` key:

```yaml
# Import .dvc file (has deps.repo)
deps:
- path: data/shared
  repo:
    url: git@github.com:myorg/otherproject.git
outs:
- md5: abc123...
  path: shared_data
```

### Step 3: Handle imports

For targets tracked by import `.dvc` files, runs `dt fetch` which:
- Clones the source repository (sparsely)
- Finds a locally-accessible cache
- Creates symlinks in the primary cache

Then runs `dvc checkout` to link files from cache to workspace.

### Step 4: Pull remaining data

Runs `dvc pull` for targets not tracked by imports, fetching from configured remotes.

With `--workers N`, this step uses parallel workers via qxub instead of a single `dvc pull`.

## Parallel mode details

When using `--workers N`, regular files (not imports) are pulled using distributed workers:

1. **Handle imports first**: All imports are fetched via `dt fetch` and checked out
2. **Build manifest**: Enumerate regular files to pull using DVC internals
3. **Partition by hash**: Files are assigned to workers based on their MD5 hash prefix
4. **Submit jobs**: Each worker is submitted via `qxub exec`
5. **Monitor**: Wait for all jobs to complete (unless `--no-wait`)

### qxub configuration

The parallel mode uses these configuration options (set via `dt config`):

| Option | Default | Description |
|--------|---------|-------------|
| `qxub.env` | `dt` | Conda environment name for workers |
| `qxub.queue` | `copyq` | PBS queue for job submission |
| `qxub.walltime` | `10:00:00` | Maximum job runtime |
| `qxub.mem` | `4GB` | Memory allocation per worker |

See [Configuration Options](config_options.md) for details.

## Comparison with dvc pull

| Feature | `dvc pull` | `dt pull` |
|---------|-----------|-----------|
| Regular files | ✓ | ✓ |
| Import files | Requires source remote access | Uses local cache via `dt fetch` |
| Parallel workers | `--jobs` (threads) | `--workers` (distributed nodes) |
| dvc.yaml outputs | ✓ | ✓ (passed through) |
| Network access | Required for imports | Not required if cache accessible |

## Typical workflow

After cloning a project that has imports:

```bash
# Clone the project
dt clone myproject
cd myproject

# Preview what will be pulled
dt pull --dry -v

# Pull all data including imports
dt pull -v

# For large datasets, use parallel workers
dt pull -w 16
```

## Force mode

The `--force` option deletes `.dir` manifest files from the cache before pulling. This forces DVC to re-fetch the entire directory contents from the remote.

### When to use

Use `--force` after running `dt cache validate --fix` to repair corrupted directories:

```bash
# Step 1: Find and delete corrupted files
dt cache validate --fix
# Output: Deleted 2 corrupted file(s)
#         Affected .dir manifests: 1
#         Run 'dt pull --force' to re-fetch affected directories

# Step 2: Force re-fetch the directory
dt pull --force data/
```

### How it works

1. Finds all `.dir` manifest files for the specified targets
2. Deletes them from the cache
3. Runs normal pull, which triggers a fresh fetch of all directory contents

Without `--force`, DVC would see the `.dir` manifest and assume all files are present, even if some were deleted by `--fix`.

## See also

- [dt cache validate](cache.md#dt-cache-validate) - Validate cache integrity
- [dt fetch](fetch.md) - Fetch imports from local caches
- [dt import](import.md) - Import data from other repositories
- [dt push](push.md) - Push to all remotes (with parallel support)
- [Configuration Options](config_options.md) - qxub settings
