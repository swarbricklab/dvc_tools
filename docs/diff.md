# dt diff

Show differences between versions of DVC-tracked data.

## Usage

```bash
# Tree view (default) - which files changed?
dt diff [paths...] [options]

# Content view - what changed inside a file?
dt diff <path> --content [options]
```

## Description

`dt diff` wraps `dvc diff` with friendlier output formats:

- **Tree view** (default): Shows which files changed in a collapsible tree structure
- **Content view** (`--content`): Shows what changed *inside* a specific file

The tree view is designed for large diffs (thousands of files) and automatically collapses to fit in a GitHub PR comment (~60k chars).

## Options

| Option | Description |
|--------|-------------|
| `--old REV` | The older revision to compare (default: HEAD) |
| `--new REV` | The newer revision to compare (default: workspace) |
| `--content` | Show content diff of a single file (requires one path) |
| `--level N` | Tree depth: number or "auto" to fit GH comment (default: auto) |
| `-o, --output FORMAT` | Output format for `--content` mode: `terminal`, `json`, `html`, `md` |
| `-v, --verbose` | Show detailed progress |

## Tree View (Default)

Shows which files changed, organized as a tree with counts at each level.

### Examples

```bash
# All changes HEAD → workspace
dt diff

# Filter to specific paths
dt diff data/
dt diff data/ models/

# Compare to tag
dt diff --old v1.0

# Between revisions
dt diff --old v1.0 --new v2.0

# Limit tree depth
dt diff --level 3
```

### Example Output

```
Changes (HEAD → workspace): 4238 added, 12 modified, 3 deleted

├── data/ (+4238, ~12, -3)
│   ├── sc/cellranger/ (+4123)
│   │   ├── annotation/ (+36 files)
│   │   ├── bam/count/captures/ (+24 files)
│   │   ├── count/
│   │   │   ├── filtered/ (+33 files)
│   │   │   └── predemux/ (+33 files)
│   │   └── ... (+4012)
│   └── processed/ (~12, -3)
│       ├── [~] samples.csv
│       └── ... (~11, -3)
└── models/ (+115)
    └── ... (+115)
```

Legend:
- `+` Added
- `~` Modified
- `-` Deleted
- `→` Renamed

### Auto-Level

By default, `--level auto` automatically collapses the tree to fit within ~60k characters (suitable for GitHub PR comments). Use `--level N` to set a specific depth.

## Content View (`--content`)

Shows what changed *inside* a specific file. Requires exactly one path.

### Examples

```bash
# What changed inside this file?
dt diff data.csv --content

# Compare to older revision
dt diff data.csv --content --old HEAD~1

# HTML output for sharing
dt diff data.csv --content --old v1.0 --new v2.0 -o html > changes.html
```

### Supported Formats

#### CSV/TSV Files

Uses [daff](https://github.com/paulfitz/daff) for tabular diffing:

```bash
$ dt diff samples.csv --content --old HEAD~1
@@,sample_id,value,status
  ,S001,42,active
+ ,S002,38,active
- ,S003,45,inactive
→ ,S004,50→52,pending→active
```

Install daff: `pip install daff`

#### Other Formats

For unsupported formats, shows metadata comparison:

```
Binary/unsupported format: size changed from 1,234,567 to 1,345,678 bytes (+111,111)
```

## CI/GitHub Integration

The tree view is designed for CI workflows:

```yaml
# .github/workflows/dvc-diff.yml
- name: Show DVC changes
  run: |
    dt diff --old ${{ github.event.before }} --new ${{ github.sha }} > diff.md
    echo "::notice::$(cat diff.md)"
```

The auto-level feature ensures output fits in GitHub PR comments.

## Error Handling

### dvc diff fails

```
Error: dvc diff failed: Not a DVC repository
```

**Solution**: Ensure you're in a DVC repository.

### File not in cache (content mode)

```
Error: Failed to get 'data.csv' at revision 'HEAD': ...
```

**Solution**: Run `dvc pull` or `dt fetch` first.

### daff not installed (content mode for CSV)

```
Error: daff not found. Install with: pip install daff
```

**Solution**: `pip install daff`

## See Also

- [dt history](history.md) - Show version history of files
- [dt fetch](fetch.md) - Fetch files into cache
