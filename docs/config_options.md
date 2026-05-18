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
| `site_cache.root` | Root directory for shared DVC [`site_cache_dir`](index.md) | `/g/data/a56/dvc/site` |
| `site_cache.enabled` | Whether `dt init`/`dt clone` configure `core.site_cache_dir` | `true` |
| `index.mirror_root` | Root directory for the [index archive](index.md) | `/g/data/a56/dvc/index-archive` |
| `index.lock_timeout` | Seconds to wait for `local.lock` / `mirror.lock` | `120` |
| `index.retry_interval` | Initial retry interval for locks | `5` |
| `add.max_threads` | Maximum threads for checksum computation | `192` |
| `add.mem_per_thread` | GB of RAM per thread for `dt add` | `1` |
| `qxub.env` | Conda environment for parallel workers | `dt` |
| `qxub.queue` | PBS queue for parallel jobs | `copyq` |
| `qxub.walltime` | Maximum runtime for parallel jobs | `10:00:00` |
| `qxub.mem` | Memory allocation for parallel jobs | `4GB` |
| `auth.github_user` | GitHub username for [`dt auth whoami`](auth.md#dt-auth-whoami) | `alice-smith` |
| `auth.github_teams` | GitHub team slugs (comma-separated) | `data-team, ops` |
| `auth.gcp_email` | GCP IAM email address | `alice@proj.iam.gserviceaccount.com` |
| `auth.aws_identity` | AWS IAM ARN | `arn:aws:iam::123:user/alice` |
| `auth.slack_webhook` | Slack incoming-webhook URL for [`dt auth request --send`](auth.md#dt-auth-request) | `https://hooks.slack.com/services/...` |
| `auth.admin_email` | Admin email address for [`dt auth request --send email`](auth.md#dt-auth-request) | `admin@example.com` |
| `summary.output_dir` | Output directory for [summary files](summary.md) | `docs` |

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

## Index Options

These options configure the [index archive](index.md) used by `dt index pull|push` and the shared DVC `site_cache_dir`.

### `site_cache.root`

Root directory for the shared DVC `site_cache_dir`. When set, `dt init` and `dt clone` write `core.site_cache_dir = {site_cache.root}/{project_name}` to `.dvc/config.local`, so every node mounting the workspace shares one live index.

```bash
dt config set site_cache.root /g/data/<project>/dvc/site
```

Leave unset to fall back to DVC's per-node default (typically `/var/tmp/dvc`).

### `site_cache.enabled`

**Default:** `true`

Master switch. Set to `false` to make `dt init` and `dt clone` skip `core.site_cache_dir` configuration even when `site_cache.root` is set. Per-invocation `--no-site-cache` always takes precedence.

### `index.mirror_root`

Root directory for the shared index archive. The actual archive path is `{mirror_root}/{repo_hash}/`. Must be a local or networked filesystem path — `gs://` / `s3://` are not supported.

```bash
dt config set index.mirror_root /g/data/<project>/dvc/index-archive
```

### `index.lock_timeout`

**Default:** `120`

Maximum seconds to wait for `local.lock` or `mirror.lock` (held during `dt index pull|push`) before giving up.

```bash
# Wait up to 5 minutes for locks
dt config set index.lock_timeout 300
```

### `index.retry_interval`

**Default:** `5`

Initial retry interval in seconds when waiting for a lock. Uses exponential backoff up to 30 seconds.

```bash
dt config set index.retry_interval 10
```

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

## add Options

These options configure the `dt add` command for parallel checksum computation.

### `add.max_threads`

**Default:** `192`

Maximum number of threads for checksum computation when adding files. This controls the `core.checksum_jobs` DVC setting. A standard node has 48 CPUs × 4 threads = 192 max threads.

```bash
# Limit to 96 threads
dt config set add.max_threads 96
```

### `add.mem_per_thread`

**Default:** `1`

Gigabytes of RAM to allocate per thread when submitting `dt add` jobs via qxub. Total memory = threads × mem_per_thread. A standard node has 192 GB RAM / 192 threads = 1 GB per thread.

```bash
# Allocate 2 GB per thread for memory-intensive operations
dt config set add.mem_per_thread 2
```

**Example:** With 192 threads and 1 GB per thread, the job requests 48 CPUs and 192 GB RAM.

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

## auth Options

These options configure user identities and delivery methods for [`dt auth`](auth.md).

### `auth.github_user`

GitHub username. Used by `dt auth whoami` and included in access request templates.

Can be auto-detected with `dt auth whoami --detect` (uses `gh api user`).

```bash
dt config set --user auth.github_user alice-smith
```

### `auth.github_teams`

Comma-separated list of GitHub team slugs. Useful for access requests since repo access is typically managed via teams.

Can be auto-detected with `dt auth whoami --detect` (uses `gh api user/teams`).

```bash
dt config set --user auth.github_teams 'data-team, ops'
```

### `auth.gcp_email`

GCP IAM email address (user or service account). Included in access request templates for GCS-related resources.

Can be auto-detected with `dt auth whoami --detect` (uses `gcloud auth list`).

```bash
dt config set --user auth.gcp_email alice@proj.iam.gserviceaccount.com
```

### `auth.aws_identity`

AWS IAM ARN. Included in access request templates for S3-related resources.

Can be auto-detected with `dt auth whoami --detect` (uses `aws sts get-caller-identity`).

```bash
dt config set --user auth.aws_identity 'arn:aws:iam::123456:user/alice'
```

### `auth.slack_webhook`

Slack incoming-webhook URL for posting access requests. Obtain one by creating an [Incoming Webhook](https://api.slack.com/messaging/webhooks) in your Slack workspace.

Recommended at **system** scope so all users on the platform share the same channel.

```bash
dt config set --system auth.slack_webhook 'https://hooks.slack.com/services/T.../B.../xxx'
```

### `auth.admin_email`

Email address of the administrator who handles access requests. Used by `dt auth request --send email`, which pipes the request text to the local `mail` command.

```bash
dt config set --system auth.admin_email 'data-admin@example.com'
```

## summary Options

### `summary.output_dir`

**Default:** `docs`

Output directory for files generated by [`dt summary`](summary.md). Used as the default location for tree.txt (DVC file listing) and dag.md (pipeline DAG).

```bash
# Use current directory for summary files
dt config set summary.output_dir .

# Use a custom documentation directory
dt config set summary.output_dir project_docs
```

The `--out` flag on `dt summary` overrides this setting.

## See also

- [dt config](config.md) - Set and get configuration values
- [Configuration Scopes](config_scopes.md) - Understanding scope hierarchy
- [dt add](add.md) - Add files with parallel checksums
- [dt push](push.md) - Push with parallel support
- [dt pull](pull.md) - Pull with parallel support
- [dt summary](summary.md) - Generate project documentation

