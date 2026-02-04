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

After `dt fetch`, run `dvc checkout` to link files from cache to workspace.

For **regular `.dvc` files** (created by `dvc add`), use `dvc fetch` to download from remotes.

## Options

- `-v, --verbose`: Show detailed progress messages
- `--no-refresh`: Skip refreshing temp clones (useful for offline use)

## Examples

```bash
# Fetch all import files
dt fetch

# Fetch specific targets
dt fetch data/external.dvc

# Show detailed progress
dt fetch -v

# Skip refreshing temp clones (for offline use)
dt fetch --no-refresh
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

## How it works

1. **For import files**: `dt fetch` clones the source repository (cached in `.dt/tmp/`) to access its DVC configuration, finds a locally-accessible remote, and creates symlinks in the primary cache.

2. **For regular files**: Reports that the file is not an import and suggests using `dvc fetch`.

The fetch creates **symlinks** rather than copies, so files appear in your cache without using additional disk space.

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
- [dt import](import.md) - Import data from other repositories
- [dt cache](cache.md) - Manage the cache
- [dt tmp](tmp.md) - Manage temporary repository clones
