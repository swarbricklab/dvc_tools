# dt add

Add files or directories to DVC tracking via compute node.

## Overview

`dt add` submits a `dvc add` command to a compute node via qxub, enabling
parallel checksum computation for large files. All targets are processed
in a single job to avoid DVC lock contention.

For local execution (e.g., small files on a login node), use `dvc add` directly.

## Usage

```bash
dt add [OPTIONS] TARGETS...
```

### Arguments

- `TARGETS`: One or more files or directories to add to DVC tracking (required).
  All targets are processed in a single job.

### Options

| Option | Description |
|--------|-------------|
| `-t, --threads N` | Number of threads for checksum computation (default: 192) |
| `--no-wait` | Submit job and exit without waiting for completion |
| `-v, --verbose` | Show detailed progress |

All other options are passed through to `dvc add`. Run `dvc add --help` for
additional options.

## Examples

### Basic Usage

```bash
# Add a directory (uses up to 192 threads)
dt add data/

# Add a single file
dt add large_dataset.csv

# Add multiple targets (all in one job)
dt add data/ results/ models/
```

### Controlling Threads

```bash
# Use up to 24 threads
dt add -t 24 data/

# Use maximum threads for very large directories
dt add -t 96 huge_dataset/
```

### Async Submission

```bash
# Submit and exit immediately (don't wait for completion)
dt add --no-wait data/
# Output: Submitted job: 12345678.gadi-pbs
#         Monitor with: qxub monitor 12345678.gadi-pbs
```

### Passing DVC Options

```bash
# Add with a custom .dvc filename
dt add --file custom.dvc data/

# Add to a specific target location  
dt add --to-remote data/
```

## Configuration

The following options can be set in `.dt/config`:

| Option | Default | Description |
|--------|---------|-------------|
| `add.max_threads` | 192 | Maximum threads allowed |
| `add.mem_per_thread` | 1 | GB of RAM allocated per thread |

### Setting Configuration

```bash
# Set maximum threads
dt config set add.max_threads 64

# Set memory per thread (for memory-intensive operations)
dt config set add.mem_per_thread 8
```

### Resource Allocation

Resources are requested from qxub based on thread count:
- **Threads**: Capped to total file count across all targets
- **CPUs**: 1 CPU per 4 threads (rounded up), minimum 1
- **Memory**: `threads × mem_per_thread` GB

Example: With 192 threads and 1 GB per thread, the job requests 48 CPUs and 192 GB RAM.

## How It Works

1. **Submit Phase** (login node):
   - Counts files across all targets
   - Caps threads to file count
   - Calculates CPU and memory requirements
   - Submits single job to compute node via qxub

2. **Execution Phase** (compute node):
   - Sets `core.checksum_jobs` to thread count (local scope)
   - Runs `dvc add` on all targets
   - Unsets the config after completion

3. **Result**:
   - `.dvc` files are created for each target
   - Files are added to `.gitignore`
   - Original data remains in place

## Comparison with dvc add

| Feature | `dt add` | `dvc add` |
|---------|----------|-----------|
| Runs on | Compute node (via qxub) | Current node |
| Parallel checksums | Yes (configurable) | Limited |
| Best for | Large files, HPC | Small files, local |
| Resource management | Automatic | Manual |

## Error Handling

Common errors and solutions:

| Error | Cause | Solution |
|-------|-------|----------|
| "qxub is not available" | qxub not in PATH | Load appropriate module |
| "Not in a DVC repository" | No .dvc directory | Run `dvc init` first |
| Target not found | Invalid path | Check file/directory exists |

## Related Commands

- [dt push](push.md) - Push data to remote storage
- [dt init](init.md) - Initialize a DVC repository
- [DVC add documentation](https://dvc.org/doc/command-reference/add)
