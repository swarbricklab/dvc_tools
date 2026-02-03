# dt du

Report disk usage for DVC-tracked files, similar to the standard `du` command.

## Usage

```bash
dt du [options] [target...]
```

### Arguments

- `[target...]`: Optional paths, `.dvc` files, or directories. Defaults to all tracked files.

### Options

**Output control:**
- `-h, --human-readable`: Print sizes in human-readable format (e.g., 1K, 234M, 2G)
- `-d, --max-depth N`: Limit output to N levels of depth (aggregate subdirectories beyond this)
- `-s, --summarize`: Show only the total (equivalent to `-d 0`)
- `--inodes`: Count number of files instead of bytes
- `-c, --total`: Show a grand total line at the end

**What to measure:**
- `--cached`: Show only sizes of files actually in local cache (default)
- `--expected`: Show expected sizes from DVC metadata (what you'd have if fully pulled)

## Examples

### Basic usage

```bash
# Show disk usage for all tracked files (sorted by size)
dt du

# Human-readable output
dt du -h

# Specific target
dt du data/
```

### Depth control

```bash
# Summary only (total size)
dt du -s -h

# Show up to 2 levels deep
dt du -d 2 -h

# All files (no depth limit, default)
dt du -h
```

### File counts

```bash
# Count files instead of bytes
dt du --inodes

# File count summary
dt du --inodes -s
```

### Cache vs expected

```bash
# What's actually cached locally (default)
dt du --cached -h

# What would be needed if fully checked out
dt du --expected -h
```

## Output Format

Output mimics standard `du`, sorted by size (largest last):

```
   45056   models/v1.pkl
12845056   data/train.csv
 5368709120   data/images/
```

With `-h`:

```
   44K    models/v1.pkl
   12M    data/train.csv
  5.0G    data/images/
```

With `--inodes`:

```
       1   models/v1.pkl
       1   data/train.csv
    1247   data/images/
```

## Notes

- Sizes are read from DVC metadata (`.dvc` files and `.dir` manifests)
- `--cached` mode checks which files are actually present in the local cache
- `--expected` mode reports the full size regardless of cache state
- Output is sorted by size ascending (like `du | sort -n`)
- Uses the same size formatting as DVC itself

## Related Commands

- [`dt cache rm`](cache.md#dt-cache-rm) - Remove files from cache
- [`dt push`](push.md) - Push files to remote
- [`dt pull`](pull.md) - Pull files from remote
