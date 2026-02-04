# dt history

Show version history of DVC-tracked files across git commits.

## Usage

```bash
dt history <path> [options]
```

## Description

Lists the different versions (checksums) of a DVC-tracked file across git history, showing when each version was introduced. This helps you understand how a file evolved over time.

Use with `dt diff` to examine the actual content changes between versions.

## Options

| Option | Description |
|--------|-------------|
| `-n, --limit N` | Maximum number of versions to show |
| `--since DATE` | Only show versions since date (e.g., "2025-01-01", "1 month ago") |
| `--json` | Output as JSON |
| `-v, --verbose` | Show full hashes and author information |

## Examples

### Show all versions of a file

```bash
$ dt history data.csv
COMMIT     DATE          HASH              MESSAGE
a1b2c3d    2026-01-15    d41d8cd98f00b204  Add initial dataset
e5f6g7h    2026-01-20    098f6bcd4621d373  Update with Q4 data
i9j0k1l    2026-02-01    5d41402abc4b2a76  Fix missing values
```

### Show last 5 versions

```bash
$ dt history data.csv -n 5
```

### Show versions from the last month

```bash
$ dt history data.csv --since "1 month ago"
```

### Verbose output with full hashes

```bash
$ dt history data.csv -v
COMMIT        DATE          AUTHOR              HASH                                MESSAGE
a1b2c3d       2026-01-15    John Doe            d41d8cd98f00b204e9800998ecf8427e    Add initial dataset
e5f6g7h       2026-01-20    Jane Smith          098f6bcd4621d373cade4e832627b4f6    Update with Q4 data
```

### JSON output for scripting

```bash
$ dt history data.csv --json
[
  {
    "commit": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "short_commit": "a1b2c3d",
    "date": "2026-01-15",
    "message": "Add initial dataset",
    "author": "John Doe",
    "hash": "d41d8cd98f00b204e9800998ecf8427e"
  }
]
```

## Workflow

### Understanding file evolution

1. See when a file changed:
   ```bash
   dt history data.csv
   ```

2. Drill down into specific changes:
   ```bash
   dt diff data.csv --old a1b2c3d --new e5f6g7h
   ```

### Tracking down a regression

```bash
# Find recent versions
dt history model_weights.h5 -n 10

# Compare suspected versions
dt diff model_weights.h5 --old <good_commit> --new <bad_commit>
```

## How It Works

1. **Find candidate commits**: Queries git for commits that touched `.dvc` files or `dvc.lock`
2. **Check each commit**: Uses DVC internals to get the file hash at each revision
3. **Filter changes**: Only shows commits where the hash actually changed
4. **Format output**: Displays results with commit info and checksums

### Performance

Uses DVC's `_collect_indexes` with a reused Repo object for fast hash lookups (~0.05s per revision vs 0.32s for CLI calls).

For repositories with imports, automatically enables offline mode to avoid slow network operations.

## Supported Tracking Mechanisms

Works with all DVC tracking methods:

- **Direct `.dvc` files** (`data.csv.dvc`)
- **Directory tracking** (files inside a tracked directory)
- **Pipeline outputs** (`dvc.lock`)

## See Also

- [dt diff](diff.md) - Show content differences between versions
- [dt fetch](fetch.md) - Fetch files into cache for viewing
