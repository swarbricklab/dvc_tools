# dt cache

Cache management commands for working with DVC's external shared caches in HPC environments.

## dt cache init

Sets up an external shared cache with proper permissions for group collaboration.

### Usage

```bash
dt cache init [options] [project_name]
```

### Options

- `--name <project_name>`: Override project name (defaults to current directory name)
- `--cache-root <path>`: Override cache root directory (defaults to `cache.root` config value)
- `--cache-path <path>`: Override complete cache path (absolute or relative to current directory)

### What it does

- Creates the external cache directory structure
- Sets group read/write permissions to allow team collaboration
- Configures DVC to use the external cache via `dvc cache dir --local`
- Keeps configuration local to maintain repository portability

### Examples

```bash
# Set up cache with default settings
# Uses: ${cache.root config}/${current directory name}
dt cache init

# Set up cache with custom name and root
# Uses: /scratch/a56/dvc/cache/my-project
dt cache init --name my-project --cache-root /scratch/a56/dvc/cache

# Set up cache with complete custom path (absolute)
dt cache init --cache-path /completely/different/location/shared-cache

# Set up cache with relative path
dt cache init --cache-path ../shared-cache
```

### Path Resolution

The cache location is determined by (in order of precedence):

1. **`--cache-path`** - Complete path override (absolute or relative to current directory)
2. **Constructed path** - `${cache_root}/${project_name}` where:
   - **cache_root**: `--cache-root` argument OR `cache.root` config value
   - **project_name**: `--name` argument OR current directory name

**Default behavior** (no options): Uses `${cache.root config}/${current directory name}`

## dt cache rm

Remove cached files for specified targets from the local cache. This deletes the cache files while leaving the workspace unchanged.

### Usage

```bash
dt cache rm [options] <target> [<target> ...]
```

### Arguments

- `<target>`: One or more paths, `.dvc` files, or directories to remove from cache.

### Options

- `--dry`: Show what would be deleted without actually deleting anything.
- `--size`: Report file sizes for each file (works with or without `--dry`).
- `-v, --verbose`: Print detailed progress information.
- `-f, --force`: Delete even if files are not in the remote (dangerous - may cause data loss).

### What it does

1. Resolves the specified targets to find all associated DVC-tracked files
2. Checks if each file exists in the remote (safety check)
3. Locates the corresponding cache files (using the MD5 hashes from `.dvc` files)
4. Deletes the cache files from the local cache directory

The workspace files are **not** affected. Users can manipulate workspace files directly using standard OS commands like `rm`.

### Safety Check

By default, `dt cache rm` **refuses to delete files that are not in the remote**. This prevents accidental data loss for files that haven't been pushed yet.

If you want to delete such files anyway (e.g., cleaning up a mistaken `dvc add`), use `--force`:

```bash
# This will fail if data/uncommitted.csv is not in remote
dt cache rm data/uncommitted.csv

# Force deletion even if not in remote
dt cache rm --force data/uncommitted.csv
```

### Examples

```bash
# Remove cache for a single file
dt cache rm data/large_dataset.csv

# Remove cache for an entire directory
dt cache rm data/processed/

# Dry run - show what would be deleted
dt cache rm --dry data/

# Show sizes of files that would be deleted
dt cache rm --dry --size data/

# Remove cache for multiple targets with size reporting
dt cache rm --size data/train.csv data/test.csv models/

# Force delete files not yet pushed to remote
dt cache rm --force data/temp_experiment/

# Verbose output
dt cache rm -v data/
```

### Use Cases

- **Reclaiming disk space**: Remove cached files for data you no longer need locally.
- **Cleaning up after experiments**: Delete cache for intermediate results.
- **Selective cache management**: Keep important data cached while removing less-used files.
- **Undoing a mistaken `dvc add`**: Use `--force` to remove cache for files you added by accident.

### Notes

- This command only affects the **primary cache**. Remote storage is not modified.
- If the workspace files are still present, they can be re-cached using `dvc add` or restored from remote using `dvc pull`.
- Directory targets are processed recursively to find all contained DVC-tracked files.
- Use `dvc data status` to check if your cache is consistent after removing files.

## dt cache validate

Validate cache integrity by verifying MD5 checksums. Detects corrupted files (e.g., from interrupted transfers) by comparing the actual file hash against the expected hash encoded in the filename.

### Usage

```bash
dt cache validate [options] [targets...]
```

### Arguments

- `[targets]`: Optional paths, `.dvc` files, or directories to validate. Without targets, validates all tracked files.

### Options

| Option | Description |
|--------|-------------|
| `--fix` | Delete corrupted files (they can be re-fetched with `dt pull`) |
| `-v`, `--verbose` | Show detailed progress for each file |
| `--json` | Output results as JSON |
| `--no-progress` | Suppress progress bar |

### What it does

1. Resolves targets to find all DVC-tracked files
2. For each file in the cache:
   - Computes the actual MD5 hash
   - Compares against the expected hash (from the cache filename)
   - Reports mismatches as corrupted files
3. With `--fix`, deletes corrupted files

### Examples

```bash
# Validate entire cache
dt cache validate

# Validate specific target
dt cache validate data/

# Validate with verbose output
dt cache validate -v

# Fix corrupted files (delete them)
dt cache validate --fix

# Output as JSON
dt cache validate --json
```

### Fixing Corrupted Directories

When corruption is found inside a DVC-tracked directory, the workflow is:

```bash
# Step 1: Find and delete corrupted files
dt cache validate --fix

# Step 2: Force re-fetch directory contents
dt pull --force data/
```

The `--fix` option only deletes the corrupted individual files. You must use `dt pull --force` to delete the `.dir` manifest and trigger a fresh pull of the entire directory.

### Understanding the Output

```
Validating 150 cache files...
  ✓ 148 valid
  ✗ 2 corrupted

Corrupted files:
  abc123... (expected: abc123, actual: def456)
  789xyz... (expected: 789xyz, actual: 000000)

Affected .dir manifests: 1
  Run 'dt pull --force' to re-fetch affected directories
```

### Use Cases

- **After interrupted transfers**: Network issues during `dvc pull` can leave partial files
- **Storage verification**: Periodic checks for data integrity
- **Debugging pull failures**: Identify why `dvc checkout` might fail
- **Before important runs**: Ensure cache is healthy before long computations

## dt cache perms

Check that a shared cache's directories stay group-writable.

```bash
dt cache perms                    # Report
dt cache perms --fix              # Repair what you own
dt cache perms --fix --sticky     # ...and restrict deletion to file owners
```

| Option | Description |
|--------|-------------|
| `--path PATH` | Check a specific cache directory |
| `--fix` | Apply the policy. Without this, only reports. |
| `--sticky` / `--no-sticky` | Require the sticky bit (default: `perms.sticky` config, else off) |
| `--allow-other` / `--no-other` | World read/execute, `2775` vs `2770` |
| `-j, --jobs N` | Concurrent stat workers (default: 8) |
| `--json` / `-v` | Machine-readable output / list every directory |

Same policy and mechanics as [`dt remote perms`](remote.md#dt-remote-perms) —
see there for the full explanation of the mode table, the sticky bit, and why
repair is inherently per-owner.

One difference: a cache also has a `runs/` run-cache tree, which
[`dt cache init`](#dt-cache-init) pre-creates with the same 256 prefixes. `dt
cache perms` expects that full set; `dt remote perms` does not, since a remote
may carry an empty `runs/` with nothing owed.

## dt cache clean

Remove abandoned `.tmp` files left behind by interrupted transfers.

DVC writes each blob to a random temporary name and renames it into place. A
transfer killed before the rename leaves the partial file behind permanently,
and since these are dotfiles they never show up in a casual `ls`.

```bash
dt cache clean                    # Report what could be removed
dt cache clean --delete           # Remove them
dt cache clean --min-age 30       # Only files older than 30 days
dt cache clean --path /path/to/cache --json
```

| Option | Description |
|--------|-------------|
| `--path PATH` | Clean a specific cache directory |
| `--min-age DAYS` | Only remove files older than this (default: 7) |
| `--delete` | Actually remove the files. Without this, only reports. |
| `-j, --jobs N` | Concurrent prefix scanners (default: 8) |
| `--json` / `-v` | Machine-readable output / list every file |

This is the same sweep as [`dt remote clean`](remote.md#dt-remote-clean),
pointed at a cache instead of a remote — see there for the full explanation of
why it is safe, how permissions work, and why empty prefix directories are
deliberately left in place.

Worth running on a shared cache, where one user's interrupted pulls consume
quota everyone shares.

## Related Commands

- [`dt init`](init.md) - Initialize projects with cache setup
- [`dt fetch`](fetch.md) - Fetch imports from local caches
- [`dt import`](import.md) - Import data from other repositories
- [`dt remote init`](remote.md#init) - Set up remote storage
- [`dt tmp`](tmp.md) - Manage temporary repository clones
- [`dt config`](config.md) - Configure cache settings