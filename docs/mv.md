# dt mv

Move or rename DVC-tracked files, preserving import metadata.

## Usage

```bash
dt mv <src> <dst> [options]
```

## What it does

Runs `dvc mv` but fixes a long-standing bug where import `.dvc` files lose their `deps` section when moved. This section contains critical metadata about the import source (repository URL, revision, path).

For **regular DVC-tracked files** (created by `dvc add`):
- Simply calls `dvc mv` as a pass-through

For **import `.dvc` files** (created by `dvc import`):
1. Saves the `deps` section from the original `.dvc` file
2. Runs `dvc mv`
3. Restores the `deps` section to the new `.dvc` file

## Options

- `-v, --verbose`: Show detailed progress messages

## Examples

```bash
# Move a DVC-tracked file
dt mv data/raw.csv data/processed.csv

# Rename a file
dt mv results/output_v1.parquet results/output_final.parquet

# Move into a directory
dt mv data/file.csv archive/

# With verbose output
dt mv -v data/imported_data.csv data/renamed_data.csv
```

## The DVC Bug

When you use `dvc mv` on a file that was imported with `dvc import`, the resulting `.dvc` file is missing the `deps` section:

**Before move** (`data/original.dvc`):
```yaml
deps:
- path: source_data.csv
  repo:
    url: git@github.com:org/source-repo.git
    rev: abc123
outs:
- md5: d41d8cd98f00b204e9800998ecf8427e
  path: original.csv
```

**After `dvc mv`** (`data/renamed.dvc`) — **BUG**:
```yaml
outs:
- md5: d41d8cd98f00b204e9800998ecf8427e
  path: renamed.csv
```

The `deps` section is completely lost, which means:
- `dvc update` no longer works on this file
- You can't tell where the data originally came from
- Re-importing requires finding the source manually

## How dt mv fixes this

`dt mv` detects import `.dvc` files (using `is_repo_import()`) and preserves the `deps` metadata:

**After `dt mv`** (`data/renamed.dvc`) — **CORRECT**:
```yaml
deps:
- path: source_data.csv
  repo:
    url: git@github.com:org/source-repo.git
    rev: abc123
outs:
- md5: d41d8cd98f00b204e9800998ecf8427e
  path: renamed.csv
```

## See also

- [dt import](import.md) — Import data from other repositories
- [dt fetch](fetch.md) — Fetch DVC-tracked files including imports
- [dt pull](pull.md) — Pull DVC-tracked files
