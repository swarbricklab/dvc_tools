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

## Options

All options are passed through to `dvc checkout`. See `dvc checkout --help` for available options.

## Examples

```bash
# Checkout all tracked files
dt checkout

# Checkout specific targets
dt checkout data/processed.dvc

# Force checkout (overwrite modified files)
dt checkout --force
```

## How it works

For each alternate cache, `dt checkout` temporarily sets it as the cache directory and runs `dvc checkout --allow-missing`. Files found in any cache are checked out; missing files are skipped until the next cache is tried.

The checkout respects the `cache.type` DVC setting (copy, symlink, hardlink, etc.).

## Configuration

Configure alternate caches with `dt cache add`:

```bash
# Add another project's remote as an alternate cache
dt cache add /g/data/a56/dvc/otherproject

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
