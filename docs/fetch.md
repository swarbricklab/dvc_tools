# dt fetch

Fetch DVC-tracked files into the primary cache from local sources.

## Usage

```bash
dt fetch [targets...] [options]
```

## What it does

Populates the primary cache with symlinks to files from source caches. This is the `dt` equivalent of `dvc fetch`, but works with local caches (other projects' remotes that are accessible on the same filesystem).

For **import `.dvc` files** (created by `dvc import`), `dt fetch` automatically:
1. Clones the source repository to access its DVC configuration
2. Finds a locally-accessible cache from the source repo's remotes
3. Creates symlinks in the primary cache pointing to the source files

For **URL import `.dvc` files** (created by `dvc import-url`), `dt fetch`:
1. Uses `dvc update` to re-download data from the source URL
2. If the source has changed, the .dvc file is updated with the new hash
3. Data is cached locally after download

For **regular `.dvc` files** (created by `dvc add`), `dt fetch`:
1. Checks if there's a locally-accessible remote (same filesystem)
2. If found, creates symlinks from the remote to the primary cache
3. If no local remote available and `--network` is specified, falls back to `dvc fetch`

After `dt fetch`, run `dvc checkout` to link files from cache to workspace.

## Options

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Show detailed progress messages |
| `--update` | Recover from .dir failures by rebuilding manifests with `dt update` |
| `--network` | Fall back to `dvc fetch` (network) if local remote not available |
| `--dry` | Show stage categorization without fetching (for troubleshooting) |
| `--imports` | Only fetch repo imports (from `dvc import`) |
| `--urls` | Only fetch URL imports (from `dvc import-url`) |
| `--regular` | Only fetch regular stages (non-imports) |
| `--no-index-sync` | Skip automatic index mirror sync |
| `--source PATH` | Explicit source cache path (overrides auto-discovery) |
| `--destination PATH` | Explicit destination cache path (overrides primary cache) |
| `--cache-type TYPE` | Link type: `reflink`, `hardlink`, `symlink`, or `copy` |

### Stage Type Filters

The `--imports`, `--urls`, and `--regular` flags can be combined to fetch specific types of stages:

```bash
# Only fetch repo imports
dt fetch --imports

# Only fetch URL imports
dt fetch --urls

# Fetch both imports and URL imports (skip regular stages)
dt fetch --imports --urls

# If no filter specified, all stage types are fetched
dt fetch
```

### Explicit Cache Paths

The `--source` and `--destination` options allow you to specify explicit cache paths instead of using auto-discovery:

```bash
# Fetch from an explicit source cache
dt fetch --source /path/to/source/cache

# Fetch into an explicit destination cache (instead of primary cache)
dt fetch --destination /path/to/dest/cache

# Combine both for full control over source and destination
dt fetch --source /shared/project-cache --destination /local/cache
```

This is useful when:
- The source cache is not configured as a DVC remote
- You want to populate a different cache than the primary one
- You're copying data between caches on shared filesystems

### Cache Link Type

The `--cache-type` option controls how files are linked from source to destination cache. By default, `dt fetch` tries methods in order until one succeeds: reflink → hardlink → symlink → copy.

```bash
# Only use symlinks (fail if symlink not possible)
dt fetch --cache-type symlink

# Force copy (useful when source may change)
dt fetch --cache-type copy

# Use hardlinks (same filesystem, no extra space)
dt fetch --cache-type hardlink
```

| Type | Description |
|------|-------------|
| `reflink` | Copy-on-write (instant, zero space, safe to modify). Requires filesystem support (e.g., APFS, Btrfs, XFS). |
| `hardlink` | Same inode, no extra space. Only works within the same filesystem. |
| `symlink` | Pointer to source file. Works across filesystems but source must remain accessible. |
| `copy` | Full copy. Slower but universally compatible. |

## Examples

```bash
# Fetch all .dvc files from local sources
dt fetch

# Fetch specific targets
dt fetch data/external.dvc

# Show detailed progress
dt fetch -v

# Fall back to network fetch if local remote not available
dt fetch --network

# Fetch with verbose output and network fallback
dt fetch -v --network data/
```

## Handling Import Files

When you run `dt fetch` on a `.dvc` file created by `dvc import`, it automatically finds the source cache:

```bash
# This .dvc file was created by: dvc import git@github.com:org/repo.git data/file
dt fetch -v imported_data.dvc

# Output:
# Fetching import: imported_data.dvc
# Import from: git@github.com:org/repo.git
# Cloning source repository...
# Found local cache: /g/data/a56/dvc/repo (from remote 'nci')
# Populating primary cache...
# Fetched 5 files from /g/data/a56/dvc/repo

# Now checkout with standard DVC
dvc checkout imported_data.dvc
```

## Handling Regular DVC Files

For regular `.dvc` files (created by `dvc add`), `dt fetch` checks your configured remotes for one that's on the local filesystem:

```bash
# If you have a local remote configured (e.g., /shared/dvc/remote)
dt fetch -v data.txt.dvc

# Output:
# ✓ data.txt.dvc: Fetched 1 files from local remote 'nci'

# If no local remote is available
dt fetch data.txt.dvc

# Output:
# ✗ data.txt.dvc: No local remote available (use --network to fetch)

# Use --network to fall back to dvc fetch
dt fetch --network data.txt.dvc

# Output:
# ✓ data.txt.dvc: Fetched via dvc fetch (network)
```

## Handling URL Imports

For `.dvc` files created by `dvc import-url` (external URLs like S3, HTTP, local paths), `dt fetch` re-downloads from the source:

```bash
# This .dvc file was created by: dvc import-url s3://bucket/data.csv
dt fetch -v data.csv.dvc

# Output:
# Fetching URL import: data.csv.dvc
#   URL import from: s3://bucket/data.csv
#   Running: dvc update data.csv.dvc
# ✓ data.csv.dvc: Fetched from s3://bucket/data.csv

# If the source has changed, the .dvc file is updated
dt fetch -v data.csv.dvc

# Output:
# ✓ data.csv.dvc: Updated from s3://bucket/data.csv
```

**Note**: URL imports are typically not pushed to remote storage, so `dvc fetch` wouldn't find them. `dt fetch` automatically detects these and uses `dvc update` to re-download from the source URL.

## Recovering from .dir Failures

When fetching directory imports, the `.dir` manifest file may be missing from the source remote (a common issue when data was pushed with older DVC versions or `dvc update --no-download` was used). Use `--update` to automatically recover:

```bash
# Fetch fails because .dir file is missing
dt fetch imported/dir.dvc

# Output:
# nci (3 files)
#   Failed .dir manifests (1):
#     abc123.dir (imported/dir.dvc: imported/dir): not found in source
#   Hint: .dir files may need rebuilding. Try: dt fetch --update

# Recover by rebuilding the .dir manifest
dt fetch --update imported/dir.dvc

# Output:
# Rebuilding 1 missing .dir manifests...
# imported/dir.dvc:
#   Source: /projects/source-repo
#   Path: data/dir
#   Locked rev: abc123...
#   Created .dir file with 5 entries
# ✓ 1 stages processed
```

The `--update` flag calls `dt update` with the locked revision to rebuild the `.dir` manifest, then retries the fetch.

## How it works

1. **For repo import files** (from `dvc import`): `dt fetch` clones the source repository (cached in `.dt/tmp/`) to access its DVC configuration, finds a locally-accessible remote, and creates symlinks in the primary cache.

2. **For URL import files** (from `dvc import-url`): `dt fetch` runs `dvc update` to re-download from the source URL (S3, HTTP, local path, etc.). If the source has changed, the .dvc file is updated.

3. **For regular files** (from `dvc add`): `dt fetch` checks if any configured remote is accessible on the local filesystem (either a local path or SSH to the current host). If found, it creates symlinks. Otherwise, it suggests using `--network`.

The fetch creates **symlinks** (or reflinks if supported) rather than copies, so files appear in your cache without using additional disk space.

## Workflow: Fetch + Checkout

The DVC model separates "fetch" (remote/cache → cache) from "checkout" (cache → workspace):

```bash
# Step 1: Populate the cache with files from source caches
dt fetch data/imported/

# Step 2: Link files from cache to workspace
dvc checkout data/imported/

# Or use dt pull which does both
dt pull data/imported/
```

## Example: Working with imports

```bash
# Project B imports data from Project A
dvc import git@github.com:org/projectA.git data/dataset.csv

# Later, on a shared HPC filesystem, fetch the data locally:
dt fetch -v data/dataset.csv.dvc

# Then checkout to workspace:
dvc checkout data/dataset.csv.dvc
```

## See also

- [dt pull](pull.md) - Pull files (fetch + checkout in one step)
- [dt update](update.md) - Rebuild .dir manifests for imports
- [dt import](import.md) - Import data from other repositories
- [dt cache](cache.md) - Manage the cache
- [dt tmp](tmp.md) - Manage temporary repository clones
