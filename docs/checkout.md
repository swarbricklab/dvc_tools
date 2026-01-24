# dt checkout

Checkout DVC-tracked files, searching across multiple cache directories.

## Usage

```bash
dt checkout [targets...] [options]
```

## What it does

Runs `dvc checkout` but searches for cached files across:

1. The primary DVC cache (from `.dvc/config` or `.dvc/config.local`)
2. All alternate caches configured via `dt cache add`

This enables checking out files that exist in another project's cache or remote storage without copying them to the local cache first.

For **import `.dvc` files** (created by `dvc import`), `dt checkout` automatically:
- Clones the source repository to access its DVC configuration
- Finds a locally-accessible cache from the source repo's remotes
- Adds that cache and checks out the files
- Populates the primary cache so standard DVC commands work

## Options

- `-v, --verbose`: Show which cache is being checked
- `-c, --cache <name>`: Use only this cache (by name or path). Checkout fails if files are not found.

All other options are passed through to `dvc checkout`. See `dvc checkout --help` for available options.

## Examples

```bash
# Checkout all tracked files
dt checkout

# Checkout specific targets
dt checkout data/processed.dvc

# Force checkout (overwrite modified files)
dt checkout --force

# Show cache search progress
dt checkout -v

# Checkout from a specific cache only
dt checkout --cache neochemo
dt checkout --cache /g/data/a56/dvc/projectA
```

## Handling Import Files

When you run `dt checkout` on a `.dvc` file created by `dvc import`, it automatically handles the import:

```bash
# This .dvc file was created by: dvc import git@github.com:org/repo.git data/file
dt checkout -v imported_data.dvc

# Output:
# Detected import: imported_data.dvc
# Import from: git@github.com:org/repo.git
# Cloning source repository...
# Found local cache: /g/data/a56/dvc/repo (from remote 'nci')
# ✓ /g/data/a56/dvc/repo
```

This eliminates the need to manually configure caches for imported data.

## How it works

For each alternate cache, `dt checkout` temporarily sets it as the cache directory and runs `dvc checkout --allow-missing`. Files found in any cache are checked out; missing files are skipped until the next cache is tried.

When using `--cache`, only that specific cache is used and `--allow-missing` is NOT passed, so checkout will fail if files are missing.

The checkout respects the `cache.type` DVC setting (copy, symlink, hardlink, etc.).

After successful checkout, `dt checkout` populates the primary cache with hardlinks (or symlinks for cross-device) so that standard DVC commands (`dvc status`, `dvc push`) work correctly.

## Configuration

Configure alternate caches with `dt cache add`:

```bash
# Add another project's remote as an alternate cache
dt cache add /g/data/a56/dvc/otherproject

# Discover and add caches from a repository
dt cache add-from git@github.com:org/repo.git

# List configured caches
dt cache list
```

## Example workflow

A common pattern on HPC systems where projects share a filesystem:

```bash
# Project A has data in its remote at /g/data/a56/dvc/projectA
# Project B wants to import that data

# In Project B:
dt cache add /g/data/a56/dvc/projectA

# Now checkout will find files from Project A's remote
dt checkout data/shared.dvc
```

## See also

- [dt cache](cache.md) - Manage alternate caches
- [dt import](import.md) - Import data from other repositories
- [dt tmp](tmp.md) - Manage temporary repository clones
