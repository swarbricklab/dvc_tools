# dt diff

Show differences between versions of DVC-tracked data.

## Usage

```bash
# Tree view (default) - which files changed?
dt diff [paths...] [options]

# Content view - what changed inside a file?
dt diff <path> --content [options]

# Direct comparison - what differs between two workspace files?
dt diff <path-a> <path-b> --content [options]
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
| `--content` | Show a content diff (one path = revision mode, two paths = direct comparison) |
| `--level N` | Tree depth: number or "auto" to fit GH comment (default: auto) |
| `-o, --output FORMAT` | Output format: `terminal`, `json`, `html`, `md`, `table`, `csv` |
| `-v, --verbose` | Show detailed progress |
| `--list-handlers` | List the registered content-diff handlers and exit |
| `--summary` | (`--content` only) Brief statistics / key changes only |
| `--granular` | (`--content` only) Exhaustive diff with full detail |

`--summary` and `--granular` are mutually exclusive; omit both for the standard
content diff.

## Output Formats

| Format | Description |
|--------|-------------|
| `terminal` | (default) Tree view for terminal display |
| `json` | Raw JSON from `dvc diff --json` |
| `table` | Markdown table from `dvc diff --md` |
| `md` | Tree view in diff code block (with colors on GitHub) |
| `csv` | CSV with columns: change, path, old_hash, new_hash |
| `html` | Interactive collapsible HTML tree |

### Format Examples

```bash
# Raw JSON output
dt diff -o json

# Markdown table (like dvc diff --md)
dt diff -o table

# Markdown with diff syntax highlighting (green/red on GitHub)
dt diff -o md > changes.md

# CSV for data processing
dt diff -o csv > changes.csv

# Interactive HTML (collapsible tree)
dt diff -o html > changes.html
```

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
$ dt diff --level 2
Changes (HEAD → workspace): 4238 added, 12 modified, 3 deleted

├── data/ (+4123, ~12, -3)
│   ├── [~] samples.csv
│   ├── processed/ (~11, -3)
│   │   ... (~11, -3)
│   └── sc/ (+4123)
│       ... (+4123)
└── models/ (+115)
    └── checkpoints/ (+115)
        ... (+115)
```

Every path component is its own level in the tree, and a directory deeper than
`--level` is replaced wholesale by a `...` line carrying its counts. Changed
files are shown as `[<symbol>] name`, and are listed before subdirectories.

Legend:
- `+` Added
- `~` Modified
- `-` Deleted
- `→` Renamed

### Auto-Level

By default, `--level auto` automatically collapses the tree to fit within ~60k characters (suitable for GitHub PR comments). Use `--level N` to set a specific depth.

## Content View (`--content`)

Shows what changed *inside* a file. Takes either one path (compare that path
across revisions) or two paths (compare two workspace files directly).

### Examples

```bash
# What changed inside this file?
dt diff data.csv --content

# Compare to older revision
dt diff data.csv --content --old HEAD~1

# Counts only / exhaustive detail
dt diff data.csv --content --summary
dt diff data.csv --content --granular

# HTML output for sharing
dt diff data.csv --content --old v1.0 --new v2.0 -o html > changes.html

# Compare two workspace files directly (--old/--new are not allowed here)
dt diff old.csv new.csv --content
```

### Handlers

Content diffing is handled by format-specific plugins, with a fallback for
anything unrecognised. To see what is registered:

```bash
dt diff --list-handlers
```

### Supported Formats

#### CSV/TSV Files

Uses [daff](https://github.com/paulfitz/daff) for tabular diffing:

The output is daff's own diff format, whose first column is a row marker:
`@@` header, blank for unchanged, `+++` added, `---` deleted, `->` modified
(with modified cells written as `old->new`).

```bash
$ dt diff samples.csv --content --old HEAD~1
@@,sample_id,value,status
,S001,42,active
+++,S002,38,active
---,S003,45,inactive
->,S004,50->52,pending->active
```

With `--summary` you get counts instead:

```bash
$ dt diff samples.csv --content --old HEAD~1 --summary
CSV diff summary: 1 row(s) added, 1 row(s) deleted, 1 row(s) modified
```

Install daff: `pip install daff`. Handled extensions: `.csv`, `.tsv`, `.tab`,
`.txt`.

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
    dt diff -o md --old ${{ github.event.before }} --new ${{ github.sha }} > diff.md
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
- [dt find](find.md) - Reverse lookup: find the path for a hash
