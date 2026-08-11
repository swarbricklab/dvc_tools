# dt push

Push DVC-tracked files to all project-configured remotes.

## Usage

```bash
dt push [options] [targets...]
```

## What it does

Runs `dvc push` for each remote configured at **project** or **local** scope, skipping remotes inherited from user or system config.

This ensures data is pushed to all remotes that are part of the repository configuration, without pushing to personal or team-wide default remotes.

## Options

| Option | Description |
|--------|-------------|
| `--dry`, `--dry-run` | Show what would be pushed without actually pushing |
| `-v`, `--verbose` | Show detailed progress (with `--dry`, lists all files) |
| `-w N`, `--workers N` | Distribute push across N compute nodes via qxub |
| `-r NAME`, `--remote NAME` | Push to specific remote instead of all project remotes |
| `--no-wait` | Submit worker jobs and exit without waiting for completion |

All other options are passed through to `dvc push`. See `dvc push --help` for available options.

## Examples

### Basic usage

```bash
# Push all tracked files to all project remotes
dt push

# Push specific targets
dt push data/processed.csv.dvc

# Push to a specific remote only
dt push -r myremote
```

### Dry run

Preview what would be pushed without actually transferring:

```bash
# Summary view
dt push --dry
# Output: Would push 42 file(s), 1.2 GB

# Detailed list with file paths
dt push --dry -v
# Output:
# Files to push (42 files, 1.2 GB):
#   data/processed.csv  (abc123...)
#   models/output.pkl   (def456...)
#   ...

# Preview worker distribution
dt push --dry -w 8
# Output:
# Would push 42 file(s), 1.2 GB
# 
# With 8 workers:
#   Worker 0: 6 file(s), 180.0 MB
#   Worker 1: 5 file(s), 150.0 MB
#   ...
```

> **Note:** the per-worker breakdown is only printed for a plain `--dry -w N`.
> Adding `-v` switches to the full file listing instead.

### Parallel push with qxub

For large datasets, distribute the push across multiple compute nodes:

```bash
# Push using 16 parallel workers
dt push --workers 16

# Push to specific remote with workers
dt push -w 8 -r myremote

# Submit jobs and exit without waiting
dt push -w 16 --no-wait
# Output:
# Submitted 16 job(s):
#   12345678.gadi-pbs
#   12345679.gadi-pbs
#   ...
# Manifest: /path/to/manifest/dir
#
# Monitor with: qxub monitor 12345678.gadi-pbs ...
```

## Parallel mode details

When using `--workers N`, the push is distributed across multiple compute nodes:

1. **Build manifest**: Enumerate all files to push using DVC internals
2. **Partition by size**: Files are sorted largest-first and each is assigned to the currently least-loaded worker (greedy LPT bin-packing), so total bytes are balanced across workers. Each file goes to exactly one worker, so partitions are disjoint and workers never contend.
3. **Submit jobs**: Each worker is submitted via `qxub exec`
4. **Monitor**: Wait for all jobs to complete (unless `--no-wait`)

The assignment is deterministic given the file sizes, so a `--dry -w N` preview matches the real run.

### qxub configuration

The parallel mode uses these configuration options (set via `dt config`):

| Option | Default | Description |
|--------|---------|-------------|
| `qxub.env` | `dt` | Conda environment name for workers |
| `qxub.queue` | `copyq` | PBS queue for job submission |
| `qxub.walltime` | `10:00:00` | Maximum job runtime |
| `qxub.mem` | `4GB` | Memory allocation per worker |

Example configuration:

```bash
# Set the conda environment for workers
dt config set qxub.env myenv

# Use a different queue with more time
dt config set qxub.queue normal
dt config set qxub.walltime 24:00:00
```

### Why parallel push?

For large datasets with thousands of files, a single `dvc push` can be slow due to:
- Network latency for each file
- Single-threaded checksum verification
- Remote storage rate limits

Parallel push distributes the work across multiple nodes, with each worker handling a disjoint, byte-balanced partition of the files. This avoids conflicts, and because no single worker is left holding all the large files, wall-clock time scales with the number of workers rather than with the biggest partition.

## Example

Suppose a project has remotes for cloud storage and HPC as follows:

```bash
$ dvc remote list
gs     gs://myproject
nci    ssh://myhost.com.au/myproject
```

Running `dt push` pushes to both, ensuring data is available on both GCS and SSH storage.

## See Also

- [dt pull](pull.md) - Pull files (with parallel support)
- [dt clone](clone.md) - Clone repositories
- [dt remote init](remote.md) - Set up remote storage
- [Configuration Options](config_options.md) - qxub settings
