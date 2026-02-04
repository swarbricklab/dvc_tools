# dt ls

List and filter DVC-tracked files. Wraps `dvc list` with powerful filtering capabilities.

## Usage

```bash
dt ls [URL] [PATH] [options]
```

By default, lists DVC outputs (tracked data) in the current repository. Can also list remote repositories by URL.

## Options

### DVC list options (pass-through)

| Option | Description |
|--------|-------------|
| `--rev REV` | Git revision (SHA, branch, tag) |
| `-R, --recursive` | List recursively into directories |
| `--all` | Include non-DVC files (git-tracked files like `.dvc`, `.gitignore`) |

### Filter options

| Option | Description |
|--------|-------------|
| `-p, --pattern GLOB` | Filter by glob pattern (e.g., `*.csv`, `data/**`) |
| `-e, --regex PATTERN` | Filter by regex pattern |
| `--min-size SIZE` | Minimum file size (e.g., `100K`, `1M`, `1G`) |
| `--max-size SIZE` | Maximum file size |
| `--files` | Show only files (exclude directories) |
| `--dirs` | Show only directories |
| `--exec` | Show only executable files |
| `--hash PREFIX` | Filter by hash prefix |

### Output options

| Option | Description |
|--------|-------------|
| `-l, --long` | Long format showing type and size |
| `--show-hash` | Show MD5 hash for each item |
| `--json` | Output as JSON (for piping to `jq` etc.) |

## Output format

**Default**: One path per line (pipe-friendly)
```
test_file.txt
data/results.csv
```

**Long format** (`-l`): Type indicator and size
```
-        26  test_file.txt
d       1.5M  data_dir
```

Type indicators:
- `-` = file
- `d` = directory

**With hash** (`--show-hash`):
```
-        26  1a7086969032ca102f45a74c9fac2fa3  test_file.txt
```

## Examples

### Basic listing

```bash
dt ls                    # List DVC outputs in current repo
dt ls -R                 # List recursively
dt ls . data/            # List specific directory
dt ls --all              # Include git files too
```

### Filtering by path

```bash
dt ls --pattern "*.csv"           # All CSV files
dt ls --pattern "data/**"         # Everything under data/
dt ls --regex "train|test"        # Paths containing train or test
dt ls --regex "\.parquet$"        # Files ending in .parquet
```

### Filtering by size

```bash
dt ls --min-size 1M              # Files >= 1MB
dt ls --max-size 100K            # Files <= 100KB
dt ls --min-size 1G --max-size 10G   # Between 1GB and 10GB
```

Size units: `K` (kilobytes), `M` (megabytes), `G` (gigabytes), `T` (terabytes)

### Filtering by type

```bash
dt ls --files             # Files only
dt ls --dirs              # Directories only
dt ls --exec              # Executable files only
```

### Filtering by hash

```bash
dt ls --hash abc123       # Items with hash starting with abc123
dt ls --hash 1a7086 -l    # Show details for matching hash
```

### Historical versions

```bash
dt ls --rev HEAD~5        # List at 5 commits ago
dt ls --rev v1.0          # List at tag v1.0
dt ls --rev main          # List at branch main
```

### Combining filters

```bash
# Large CSV files
dt ls --pattern "*.csv" --min-size 1M -l

# Small files in data directory
dt ls -R --pattern "data/**" --max-size 100K --files

# Find specific hash with details
dt ls --hash abc123 -l --show-hash
```

### Piping output

```bash
# Count files
dt ls --files | wc -l

# Filter with grep
dt ls -R | grep train

# Process with xargs
dt ls --pattern "*.csv" | xargs -I {} dt summary {}

# JSON to jq
dt ls --json | jq '.[] | select(.size > 1000000) | .path'
```

### Remote repositories

```bash
# List from GitHub
dt ls git@github.com:org/repo.git

# List specific path in remote repo
dt ls https://github.com/org/repo data/

# List at specific revision
dt ls git@github.com:org/repo.git --rev v1.0
```

## JSON output

The `--json` flag outputs the full item data from `dvc list`:

```json
[
  {
    "isout": true,
    "isdir": false,
    "isexec": false,
    "size": 26,
    "md5": "1a7086969032ca102f45a74c9fac2fa3",
    "path": "test_file.txt"
  }
]
```

Fields:
- `isout`: `true` if DVC output (tracked data), `false` if git-tracked
- `isdir`: `true` if directory
- `isexec`: `true` if executable
- `size`: Size in bytes
- `md5`: MD5 hash (null for git-tracked files)
- `path`: Relative path

## Related commands

- [`dt find`](find.md) - Reverse lookup: find path by hash
- [`dt du`](du.md) - Disk usage of DVC-tracked files
- [`dt summary`](summary.md) - Summarize a .dvc file
