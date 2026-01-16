# Configuration Options

Reference for all `dt` configuration options. See [dt config](config.md) for how to view and set these values.

## All Options

| Option | Description | Example |
|--------|-------------|---------|
| `org` | Default GitHub organization for [short repository names](clone.md#short-name-feature) | `swarbricklab` |
| `team` | GitHub team for [`gh repo create --team`](https://cli.github.com/manual/gh_repo_create) | `analysts` |
| `cache.root` | Root directory for [shared external caches](cache.md) | `/g/data/a56/dvc_cache` |
| `remote.root` | Root directory for [DVC remote storage](remote.md) | `/g/data/a56/dvc_remote` |
| `ssh.host` | SSH hostname for remote access | `gadi-dm.nci.org.au` |

## Option Details

### `org`

Enables short repository names with `dt clone`. When set, `dt clone neochemo` expands to `git@github.com:swarbricklab/neochemo.git`.

### `team`

Used by `dt init` when suggesting the `gh repo create` command. If set, adds `--team=<value>` to the suggested command.

### `cache.root`

Base directory for project caches. Each project gets a subdirectory: `{cache.root}/{project_name}/`

The cache stores DVC file content locally, enabling multiple clones of the same repository to share downloaded data.

### `remote.root`

Base directory for DVC remotes. Each project gets a subdirectory: `{remote.root}/{project_name}/`

The remote is the authoritative store for DVC-tracked files, accessed via SSH from external systems or directly on the local filesystem.

### `ssh.host`

Hostname used when configuring SSH remotes. This allows DVC to push/pull data from external machines.

## Recommended Setup (NCI Gadi)

```yaml
# ~/.config/dt/config.yaml
org: swarbricklab
team: analysts
cache:
  root: /g/data/a56/dvc_cache
remote:
  root: /g/data/a56/dvc_remote
ssh:
  host: gadi-dm.nci.org.au
```
