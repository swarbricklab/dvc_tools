# Configuration Options

Reference for all `dt` configuration options. 

See [dt config](config.md) for command usage and [Configuration Scopes](config_scopes.md) for understanding scope hierarchy.

## All Options

| Option | Description | Example |
|--------|-------------|---------|
| `owner` | Default GitHub owner (user or org) for [short repository names](clone.md#short-name-feature) | `myorg` |
| `team` | GitHub team for [`gh repo create --team`](https://cli.github.com/manual/gh_repo_create) | `analysts` |
| `cache.root` | Root directory for [shared external caches](cache.md) | `/g/data/a56/dvc_cache` |
| `remote.root` | Root directory for [DVC remote storage](remote.md) | `/g/data/a56/dvc_remote` |
| `ssh.host` | SSH hostname for remote access | `gadi-dm.nci.org.au` |
| `qxub.env` | Conda environment for parallel workers | `dt` |
| `qxub.queue` | PBS queue for parallel jobs | `copyq` |
| `qxub.walltime` | Maximum runtime for parallel jobs | `10:00:00` |
| `qxub.mem` | Memory allocation for parallel jobs | `4GB` |

## Option Details

### `owner`

The GitHub owner (user or organization) used for short repository names with `dt clone`. When set, `dt clone myproject` expands to `git@github.com:myorg/myproject.git`.

This can be a personal GitHub username or an organization name—GitHub uses "owner" as the generic term for repository ownership.

### `team`

Used by `dt init` when suggesting the `gh repo create` command. If set, adds `--team=<value>` to the suggested command.

> **Note:** The `--team` option only applies when `owner` is a GitHub organization. If `owner` is a personal GitHub account, the team setting is ignored.

### `cache.root`

Base directory for project caches. Each project gets a subdirectory: `{cache.root}/{project_name}/`

The cache stores DVC file content locally, enabling multiple clones of the same repository to share downloaded data.

### `remote.root`

Base directory for DVC remotes. Each project gets a subdirectory: `{remote.root}/{project_name}/`

The remote is the authoritative store for DVC-tracked files, accessed via SSH from external systems or directly on the local filesystem.

### `ssh.host`

Hostname used when configuring SSH remotes. This allows DVC to push/pull data from external machines.

## qxub Options

These options configure parallel push/pull operations via [qxub](https://github.com/swarbricklab/qxub).

### `qxub.env`

**Default:** `dt`

The conda environment to activate on worker nodes. This environment must have `dt` installed.

```bash
dt config set qxub.env myenv
```

### `qxub.queue`

**Default:** `copyq`

The PBS queue for submitting parallel jobs. Use a queue with network access to cloud storage if pushing/pulling to S3, GCS, etc.

```bash
# Use the copy queue (has network access)
dt config set qxub.queue copyq

# Use a normal compute queue
dt config set qxub.queue normal
```

### `qxub.walltime`

**Default:** `10:00:00`

Maximum runtime for each worker job in HH:MM:SS format.

```bash
# Allow 24 hours for large transfers
dt config set qxub.walltime 24:00:00
```

### `qxub.mem`

**Default:** `4GB`

Memory allocation per worker. Increase for large files that require significant memory for checksum computation.

```bash
# Allocate 8GB per worker
dt config set qxub.mem 8GB
```

## Example: Setting up parallel operations

```bash
# Configure qxub settings at user scope (applies to all projects)
dt config set --user qxub.env dt
dt config set --user qxub.queue copyq
dt config set --user qxub.walltime 10:00:00
dt config set --user qxub.mem 4GB

# Now parallel push/pull will use these settings
dt push -w 16
dt pull -w 16
```

## See also

- [dt config](config.md) - Set and get configuration values
- [Configuration Scopes](config_scopes.md) - Understanding scope hierarchy
- [dt push](push.md) - Push with parallel support
- [dt pull](pull.md) - Pull with parallel support

