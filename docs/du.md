# dt du

Report disk usage for DVC-tracked files, similar to the standard `du` command.

## Usage

```bash
dt du [options] [target...]
```

### Arguments

- `[target...]`: Optional paths, `.dvc` files, directories, or path prefixes. Defaults to all tracked files. When a path prefix is specified, all DVC targets under that path are included.

### Options

**Output control:**
- `-h, --human-readable`: Print sizes in human-readable format (e.g., 1K, 234M, 2G)
- `-d, --max-depth N`: Limit output to N levels of depth (aggregate subdirectories beyond this)
- `-s, --summarize`: Show only the total (equivalent to `-d 0`)
- `--inodes`: Count number of files instead of bytes
- `-c, --total`: Show a grand total line at the end

**What to measure:**
- `--cached`: Show only sizes of files actually in local cache
- `--expected`: Show only expected sizes from DVC metadata
- (default): Show both cached and expected sizes in two columns

## Examples

### Basic usage (shows both cached & expected)

```bash
# Show disk usage for all tracked files (sorted by size)
dt du

# Human-readable output
dt du -h

# Output (two columns by default):
# CACHED  EXPECTED   PATH
#   44K      44K    models/v1.pkl
#   12M      12M    data/train.csv
#    0       5.0G    data/images/
```

### Path prefixes

```bash
# Show sizes for all targets under a directory path
dt du data/images/he/whole_slide_images -h

# Summarize total size of all targets under a path
dt du -s -h data/images/

# Multiple path prefixes
dt du data/images/ data/annotations/ -h
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

### Single column output

```bash
# Only cached sizes (what's locally available)
dt du --cached -h

# Only expected sizes (from metadata)
dt du --expected -h
```

## Output Format

By default, shows both cached and expected sizes with a header:

```
CACHED  EXPECTED   PATH
  44K      44K    models/v1.pkl
  12M      12M    data/train.csv
   0       5.0G    data/images/
```

With `--cached` or `--expected`, shows single column (sorted by size, largest last):

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

## Warnings

When files are missing size metadata (showing 0 expected size), a warning is displayed:

```
Warning: 3 file(s) have no size metadata. Run 'dt update' to populate.
```

Run `dt update` on the relevant `.dvc` files to populate the size metadata from the source repository.

## Notes

- By default, shows both cached and expected sizes side-by-side
- Use `--cached` or `--expected` to limit output to a single column
- Sizes are read from DVC metadata (`.dvc` files and `.dir` manifests)
- `--cached` mode checks which files are actually present in the local cache
- `--expected` mode reports the full size regardless of cache state
- Output is sorted by size ascending (like `du | sort -n`)
- Uses the same size formatting as DVC itself

## Related Commands

- [`dt cache rm`](cache.md#dt-cache-rm) - Remove files from cache
- [`dt push`](push.md) - Push files to remote
- [`dt pull`](pull.md) - Pull files from remote
- [`dt update`](update.md) - Update .dvc files and populate metadata
