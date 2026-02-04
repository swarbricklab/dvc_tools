# dt diff

Show content differences between versions of DVC-tracked files.

## Usage

```bash
dt diff <path> [options]
```

## Description

Compares the actual content of DVC-tracked files between git revisions. Unlike `dvc diff` which only shows *which* files changed, `dt diff` shows *what* changed inside the files.

Uses format-specific handlers for smart diffing (e.g., tabular diff for CSV files).

## Options

| Option | Description |
|--------|-------------|
| `--old REV` | The older revision to compare (default: HEAD) |
| `--new REV` | The newer revision to compare (default: workspace) |
| `-o, --output FORMAT` | Output format: `terminal`, `json`, `html`, `md` |
| `-v, --verbose` | Show detailed progress |

## Examples

### Compare HEAD to workspace

```bash
$ dt diff data.csv
```

Shows what changed since the last commit.

### Compare specific revisions

```bash
$ dt diff data.csv --old HEAD~1
$ dt diff data.csv --old v1.0 --new v2.0
$ dt diff data.csv --old abc123 --new def456
```

### HTML output for sharing

```bash
$ dt diff data.csv --old v1.0 --new v2.0 -o html > changes.html
```

### JSON output for scripting

```bash
$ dt diff data.csv -o json
```

## Supported Formats

### CSV/TSV Files

Uses [daff](https://github.com/paulfitz/daff) for tabular diffing:

```bash
$ dt diff samples.csv --old HEAD~1
@@,sample_id,value,status
  ,S001,42,active
+ ,S002,38,active
- ,S003,45,inactive
→ ,S004,50→52,pending→active
```

Legend:
- `+` Added row
- `-` Deleted row  
- `→` Modified value (shows old→new)

Install daff: `pip install daff`

### Other Formats

For unsupported formats, shows metadata comparison:

```
Binary/unsupported format: size changed from 1,234,567 to 1,345,678 bytes (+111,111)
```

## Workflow

### Reviewing recent changes

```bash
# What changed since last commit?
dt diff data.csv

# What changed in the last 3 commits?
dt diff data.csv --old HEAD~3
```

### Comparing releases

```bash
# Compare two tagged versions
dt diff results.csv --old v1.0.0 --new v2.0.0

# Save as HTML report
dt diff results.csv --old v1.0.0 --new v2.0.0 -o html > release_diff.html
```

### Combined with dt history

```bash
# First, see version history
$ dt history data.csv
COMMIT     DATE          HASH              MESSAGE
a1b2c3d    2026-01-15    d41d8cd98f00b204  Add initial dataset
e5f6g7h    2026-01-20    098f6bcd4621d373  Update with Q4 data

# Then examine specific changes
$ dt diff data.csv --old a1b2c3d --new e5f6g7h
```

## How It Works

1. **Fetch old version**: Uses `dvc.api.open()` to get file content at the old revision
2. **Fetch new version**: Gets workspace file or uses `dvc.api.open()` for specific revision
3. **Select handler**: Chooses format-specific handler based on file extension
4. **Compute diff**: Handler generates the appropriate diff format

### Requirements

- Files must be in the DVC cache (run `dvc pull` or `dt fetch` first)
- For CSV/TSV: `daff` must be installed (`pip install daff`)

## Handler Architecture

`dt diff` uses a plugin system for format-specific diffing:

| Handler | Extensions | Tool |
|---------|------------|------|
| CSVHandler | `.csv`, `.tsv`, `.txt` | daff |
| FallbackHandler | (all others) | Size comparison |

### Future Handlers

Planned for future releases:
- **AnnData** (`.h5ad`) - Compare obs/var/X matrices
- **VCF** - Variant comparison
- **Parquet** - Schema and data diff
- **Directories** - Recursive file comparison

## Error Handling

### File not in cache

```
Error: Failed to get 'data.csv' at revision 'HEAD': ...
```

**Solution**: Run `dvc pull` or `dt fetch` to populate the cache.

### daff not installed

```
Error: daff not found. Install with: pip install daff
```

**Solution**: `pip install daff`

### Unsupported format

Falls back to metadata comparison (size change).

## See Also

- [dt history](history.md) - Show version history of files
- [dt fetch](fetch.md) - Fetch files into cache
- [daff documentation](https://github.com/paulfitz/daff) - Tabular diff tool
