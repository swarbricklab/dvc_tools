# dt find

Reverse hash lookup: given an MD5 hash, find the workspace path(s) it corresponds to.

## Usage

```bash
dt find <hash> [options]
```

## Options

| Option | Description |
|--------|-------------|
| `--dvc-file` | Show the .dvc file that tracks each match |
| `--dir-file` | Show the hash of the parent .dir manifest for files inside directories |
| `--cache-path` | Show the cache path for each match |
| `--no-expand` | Don't search inside directories (only match top-level entries) |
| `--json` | Output results as JSON |
| `-v, --verbose` | Show additional details (equivalent to `--dvc-file --dir-file --cache-path`) |

## Examples

### Basic lookup

```bash
$ dt find a1b2c3d4e5f6
data/processed/results.csv
```

### Partial hash matching

Hashes can be partial (minimum 4 characters):

```bash
$ dt find a1b2
data/processed/results.csv
data/raw/input.csv
```

### With verbose output

```bash
$ dt find a1b2c3d4 -v
data/processed/results.csv (dvc: data/processed/results.csv.dvc) 
  cache: /path/to/cache/files/md5/a1/b2c3d4e5f6...
```

### JSON output

The JSON objects always carry `path` and `hash`; the other keys appear only
when the corresponding flag (or `-v`) is given.

```bash
$ dt find a1b2c3d4 --json --dvc-file
[
  {
    "path": "data/processed/results.csv",
    "hash": "a1b2c3d4e5f6...",
    "dvc_file": "data/processed/results.csv.dvc"
  }
]
```

### Finding files inside directories

By default, `dt find` searches inside DVC-tracked directories. If a directory is tracked as a single `.dvc` file, the command will expand it and search the contents:

```bash
$ dt find 9f8e7d6c
data/images/photo_001.jpg

# --dir-file (or -v) also reports the enclosing .dir manifest
$ dt find 9f8e7d6c -v
data/images/photo_001.jpg (dvc: data/images.dvc) (dir: 4c2f1ab90dd3e77e...) 
  cache: /path/to/cache/files/md5/9f/8e7d6c...
```

Use `--no-expand` to only match top-level entries:

```bash
$ dt find 9f8e7d6c --no-expand
No matches found for hash: 9f8e7d6c
```

When there are no matches the command exits with status 1 (with `--json` it
prints an empty list instead).

## How it works

`dt find` uses DVC's internal index to perform fast lookups:

1. Iterates through all tracked outputs in `repo.index.outs`
2. For each output, checks if the hash matches (prefix matching supported)
3. For directory outputs, expands the manifest using `out.get_obj().iteritems()` to search nested files
4. Returns all matching paths with optional metadata

This approach is much faster than invoking DVC CLI commands or parsing `.dvc` files manually.

## Use cases

- **Debugging cache issues**: Find which file a cached object belongs to
- **Investigating history**: Combined with `dt history`, trace a specific version
- **Cross-referencing**: Match hashes from logs or other tools to workspace paths

## Related commands

- [`dt history`](history.md) - Show version history of a file
- [`dt diff`](diff.md) - Show content differences between versions
- [`dt ls`](ls.md) - List and filter DVC-tracked files (forward lookup by path)
