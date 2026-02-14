# dt data status

Show changes between the last git commit, DVC files and the workspace.

## Overview

`dt data status` wraps `dvc data status` with parallel checksum computation
and optional delegation to a compute node via qxub.  It uses the same
resource allocations as `dt add` (thread count, CPU and memory scaling).

For quick local checks (e.g. small projects on a login node), use
`dvc data status` directly.

## Usage

```bash
dt data status [OPTIONS] [-- DVC_OPTIONS...]
```

### Options

| Option | Description |
|--------|-------------|
| `-t, --threads N` | Number of threads for checksum computation |
| `--no-wait` | Submit job and exit without waiting for completion |
| `-v, --verbose` | Show detailed progress |

All other options are passed through to `dvc data status`.

### DVC pass-through options

| Option | Description |
|--------|-------------|
| `--json` | Show output in JSON format |
| `--granular` | Show granular file-level info for DVC-tracked directories |
| `--unchanged` | Show unmodified DVC-tracked files |
| `--untracked-files [no\|all]` | Show untracked files |
| `--not-in-remote` | Show files missing from remote |
| `--no-remote-refresh` | Use cached remote index (skip remote check) |

## Examples

### Basic usage

```bash
# Run via compute node (default threads)
dt data status

# File-level detail for directories
dt data status --granular

# JSON output
dt data status --json
```

### Controlling threads

```bash
# Use 48 threads
dt data status -t 48

# Use 24 threads with granular output
dt data status -t 24 --granular
```

### Async submission

```bash
# Submit and exit immediately
dt data status --no-wait
# Output: Submitted job: 12345678.gadi-pbs
#         Monitor with: qxub monitor 12345678.gadi-pbs
```

## Configuration

`dt data status` uses the same resource configuration as `dt add`:

| Option | Default | Description |
|--------|---------|-------------|
| `add.max_threads` | 192 | Maximum threads allowed |
| `add.mem_per_thread` | 1 | GB of RAM allocated per thread |

### Resource allocation

Resources are requested from qxub based on thread count:
- **Threads**: Number of parallel checksum jobs
- **CPUs**: 1 CPU per 4 threads (rounded up), minimum 1
- **Memory**: `threads × mem_per_thread` GB

## How it works

1. **Submit phase** (login node):
   - Submits a single job to a compute node via qxub
   - Calculates CPU and memory from thread count

2. **Execution phase** (compute node):
   - Temporarily sets `core.checksum_jobs` to the thread count (local scope)
   - Runs `dvc data status` with any pass-through options
   - Restores the original `core.checksum_jobs` value afterwards

## Comparison with dvc data status

| Feature | `dt data status` | `dvc data status` |
|---------|-------------------|-------------------|
| Runs on | Compute node (via qxub) | Current node |
| Parallel checksums | Yes (configurable) | Limited |
| Best for | Large projects on HPC | Small projects, local |
| Resource management | Automatic | Manual |

## Related commands

- [dt add](add.md) — Add files to DVC tracking (same resource config)
- [DVC data status documentation](https://dvc.org/doc/command-reference/data/status)
