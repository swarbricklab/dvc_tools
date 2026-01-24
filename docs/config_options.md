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

