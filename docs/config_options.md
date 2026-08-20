# Configuration Options

Reference for all `dt` configuration options. 

See [dt config](config.md) for command usage and [Configuration Scopes](config_scopes.md) for understanding scope hierarchy.

## All Options

| Option | Description | Example |
|--------|-------------|---------|
| `owner` | Default GitHub owner (user or org) for [short repository names](clone.md#short-name-feature) | `myorg` |
| `team` | GitHub team for [`gh repo create --team`](https://cli.github.com/manual/gh_repo_create) | `analysts` |
| `username` | Default SSH username for remote hosts, used by [`dt clone`](clone.md) and [`dt auth setup`](auth.md) | `jr9959` |
| `cache.root` | Root director**y** for [shared external caches](cache.md). May also be a list, in which case the **first** entry is the primary cache and the rest are reported by `dt auth list`. Manage with `dt config add/remove cache.root <path>` | `/g/data/a56/dvc_cache` |
| `remote.root` | Root director**ies** for [DVC remote storage](remote.md). A single path, or a list. The **first** entry is where new remotes are created; the rest are also scanned by `--all`. Manage with `dt config add/remove remote.root <path>` | `/g/data/a56/dvc/analysis` |
| `ssh.host` | SSH hostname for remote access | `gadi-dm.nci.org.au` |
| `site_cache.root` | Root directory for shared DVC [`site_cache_dir`](index.md) | `/g/data/a56/dvc/site` |
| `site_cache.enabled` | Whether `dt init`/`dt clone` configure `core.site_cache_dir` | `true` |
| `index.mirror_root` | Root directory for the [index archive](index.md) | `/g/data/a56/dvc/index-archive` |
| `index.auto_sync` | Sync the index archive from `dt status` and the pre-commit hook | `false` |
| `index.lock_timeout` | Seconds to wait for `local.lock` / `mirror.lock` | `120` |
| `index.retry_interval` | Initial retry interval for locks | `5` |
| `add.max_threads` | Maximum threads for checksum computation | `192` |
| `add.mem_per_thread` | GB of RAM per thread for `dt add` | `1` |
| `qxub.env` | Conda environment for parallel workers | `dt` |
| `qxub.queue` | PBS queue for parallel jobs (default depends on the caller — see below) | `copyq` |
| `qxub.walltime` | Maximum runtime for parallel jobs | `10:00:00` |
| `qxub.mem` | Memory allocation for parallel jobs | `4GB` |
| `deps.cache_dir` | Directory for the [org-wide import index](deps.md#dt-deps-index); set to a shared path so a whole lab reuses one scan. Defaults to a per-user cache dir (`~/.cache/dvc-tools/repo-deps` on Linux) | `/g/data/a56/dvc-tools/repo-deps` |
| `hooks.verbosity` | Output level for [git hooks](install.md#configuration): `quiet`, `normal`, `verbose` (default `normal`) | `quiet` |
| `hooks.<hook>.checks.<check>.*` | Per-check overrides for git hooks (`enabled`, `mode`, `max_size`, …). See [dt install](install.md#configuration) | `hooks.pre-commit.checks.large-files.max_size` |
| `perms.sticky` | Whether shared dirs get the sticky bit, restricting deletion to file owners (creation is unaffected). Default `true`. Used by `dt remote/cache init` and `perms` | `false` |
| `perms.allow_other` | Whether shared dirs permit world read/execute (`3775` vs `3770`). Default `false` | `true` |
| `clean.min_age_days` | Age threshold for [`dt remote clean`](remote.md#dt-remote-clean) / [`dt cache clean`](cache.md#dt-cache-clean) (default: 7) | `14` |
| `auth.github_user` | GitHub username for [`dt auth whoami`](auth.md#dt-auth-whoami) | `alice-smith` |
| `auth.github_teams` | GitHub team slugs (comma-separated) | `data-team, ops` |
| `auth.gcp_email` | GCP IAM email address | `alice@proj.iam.gserviceaccount.com` |
| `auth.aws_identity` | AWS IAM ARN | `arn:aws:iam::123:user/alice` |
| `auth.slack_webhook` | Slack incoming-webhook URL for [`dt auth request --send`](auth.md#dt-auth-request) | `https://hooks.slack.com/services/...` |
| `auth.admin_email` | Admin email address for [`dt auth request --send email`](auth.md#dt-auth-request) | `admin@example.com` |
| `secrets.backend` | Secret-manager backend for [`dt auth credentials`](auth.md#dt-auth-credentials); only `gcp` is implemented | `gcp` |
| `secrets.gcp.project` | GCP project holding the secrets (required when `secrets.backend` is `gcp`) | `my-gcp-project` |
| `secrets.prefix` | Prefix for secret names (default `dvc-remote-`) | `dvc-remote-` |
| `secrets.gcp.locations` | Region(s) for new GCP secrets; unset means GCP automatic (global) replication | `australia-southeast1` |
| `secrets.default_endpointurl` | Fallback S3 endpoint for [`dt auth credentials configure-remotes`](auth.md#dt-auth-credentials-configure-remotes) when `--endpoint` is omitted | `https://<account>.r2.cloudflarestorage.com` |
| `summary.output_dir` | Output directory for [summary files](summary.md) | `docs` |
| `archive.staging_dir` | Local staging directory for [`dt remote archive`](archive.md#configuration) inner tarballs (no default — required) | `/scratch/a56/jr9959/dt-archive` |
| `archive.backend_root` | Base path on the archive backend (default `dt-archive`) | `dt-archive` |
| `archive.registry_path` | Central archive register directory; unset disables registration | `/g/data/a56/dt-archives/registry` |
| `archive.stage_jobs` | Parallel workers for `archive stage` (default `min(PBS_NCPUS or nproc, 8)`) | `8` |
| `archive.deposit_jobs` | Parallel upload workers for `archive deposit` (default `4`) | `4` |
| `archive.scan_jobs` | Threads for the preflight remote scan (default `32`) | `32` |
| `archive.compress` | Compression for inner tarballs: `none` (default), `gzip`, `zstd` | `zstd` |
| `archive.qxub_env` | Conda env for `--via-qxub` archive workers; falls back to `qxub.env` | `dt` |
| `archive.qxub_queue` | PBS queue for archive workers (default `normal` — *not* `copyq`); falls back to `qxub.queue` | `normal` |
| `archive.qxub_walltime` | Walltime per archive worker (default `04:00:00`); falls back to `qxub.walltime` | `04:00:00` |
| `archive.qxub_mem` | Memory per archive worker (default `4GB`); falls back to `qxub.mem` | `4GB` |

## Option Details

### `owner`

The GitHub owner (user or organization) used for short repository names with `dt clone`. When set, `dt clone myproject` expands to `git@github.com:myorg/myproject.git`.

This can be a personal GitHub username or an organization name—GitHub uses "owner" as the generic term for repository ownership.

### `team`

Used by `dt init` when suggesting the `gh repo create` command. If set, adds `--team=<value>` to the suggested command.

> **Note:** The `--team` option only applies when `owner` is a GitHub organization. If `owner` is a personal GitHub account, the team setting is ignored.

### `username`

Default SSH username used when setting up access to remote hosts (e.g. your NCI login for `gadi-dm.nci.org.au`). Both `dt clone` and `dt auth setup` use it as the default for `--username`, so setting it once avoids being prompted for your username on every run.

```bash
dt config set --user username jr9959
```

Resolution order during auth setup: a per-host `--config` YAML file, then `--username` (which defaults to this config value), then a username embedded in the remote URL, then an interactive prompt. SSH/forge hosts like `github.com` always use `git` and ignore this setting.

### `cache.root`

Base directory for project caches. Each project gets a subdirectory: `{cache.root}/{project_name}/`

The cache stores DVC file content locally, enabling multiple clones of the same repository to share downloaded data.

### `remote.root`

Base director**ies** for DVC remotes. Each project gets a subdirectory:
`{remote.root}/{project_name}/`

The remote is the authoritative store for DVC-tracked files, accessed via SSH
from external systems or directly on the local filesystem.

May be a single path or a **list**. The first entry is the default — it is where
`dt init` and `dt remote init` create a new remote. Every entry is scanned by
`dt remote perms --all` and `dt remote clean --all`.

```yaml
remote:
  root:
    - /g/data/a56/dvc/analysis      # default: new remotes go here
    - /g/data/a56/dvc/datasets
    - /g/data/a56/dvc/registries
    - /g/data/px14/dvc/analysis
```

Manage the list with [`dt config add` / `dt config remove`](config.md#list-valued-settings).

Scopes override rather than merge: the highest-precedence scope defining
`remote.root` supplies the whole list.

**Do not list cache roots here.** `/g/data/a56/dvc/cache` and
`/g/data/px14/dvc/cache` hold caches, not remotes; listing them would have
`perms` apply remote policy to a cache and `clean` sweep it as one.

Store names are not unique across roots — one site has 18 names appearing under
more than one root — so `--all` labels each store with just enough of its path
to stay unambiguous.

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

Root directory for the shared index archive. The actual archive path is `{mirror_root}/repo/{repo_hash}/`, where `repo_hash` is the basename of the repo's DVC `site_cache_dir`. Must be a local or networked filesystem path — `gs://` / `s3://` are not supported.

```bash
dt config set index.mirror_root /g/data/<project>/dvc/index-archive
```

### `index.auto_sync`

**Default:** `false`

When `true`, `dt status` pulls the index archive before running, and the
`index-sync` hook check pulls then pushes it. (That check is not one of the
built-in defaults — enable it too, e.g.
`dt config set hooks.pre-commit.checks.index-sync.enabled true`.) Off by default: the
implicit sync raced concurrent `dvc` invocations and added latency to commands
that did not need it. Run `dt index pull|push` explicitly instead.

```bash
dt config set index.auto_sync true
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

These options configure the jobs `dt` submits via [qxub](https://github.com/swarbricklab/qxub): `dt push --workers`, `dt add`, and (via the `archive.qxub_*` fallbacks) `dt remote archive stage --via-qxub`.

### `qxub.env`

**Default:** `dt`

The conda environment to activate on worker nodes. This environment must have `dt` installed.

```bash
dt config set qxub.env myenv
```

### `qxub.queue`

**Default:** `copyq` for `dt push` workers; `normal` for `dt add` jobs

The PBS queue for submitting parallel jobs. Transfer work defaults to `copyq`
(a data-mover queue with network access to cloud storage); checksum work
defaults to `normal`, which has the CPUs. Setting this key overrides both.
`dt remote archive` has its own `archive.qxub_queue` key, also defaulting to
`normal`.

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

# Now distributed push will use these settings
dt push -w 16
```

`dt pull` has no `--workers` flag; it pulls in-process. The `qxub.*` settings
apply to `dt push -w` and to `dt add` jobs submitted via qxub.

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

## secrets Options

These configure the secret manager backing [`dt auth credentials`](auth.md#dt-auth-credentials).

### `secrets.backend`

**Default:** _unset_ (credential commands error out until it is set)

Which secret manager to use. Only `gcp` (Google Secret Manager) is
implemented; `aws` is planned.

```bash
dt config set secrets.backend gcp
dt config set secrets.gcp.project my-gcp-project
```

### `secrets.gcp.project`

**Default:** _unset_ (required when `secrets.backend` is `gcp`)

The GCP project that holds the secrets.

### `secrets.prefix`

**Default:** `dvc-remote-`

Prefix applied to secret names, so one project can host secrets for several
tools without collisions.

### `secrets.default_endpointurl`

**Default:** _unset_

Fallback S3 endpoint URL used by `dt auth credentials configure-remotes` when
`--endpoint` is not passed. With neither set, the command errors.

```bash
dt config set secrets.default_endpointurl https://<account>.r2.cloudflarestorage.com
```

### `secrets.gcp.locations`

**Default:** _unset_ (GCP automatic/global replication)

Region(s) in which `dt auth credentials set` creates GCP Secret Manager
secrets. When unset, secrets are created with GCP's default automatic
(global) replication. When set, secrets are created with **user-managed**
replication in the given region(s) — required in projects governed by a
`constraints/gcp.resourceLocations` org policy, which forbids creating
secrets in `global`.

Accepts a single region or a comma-separated list.

```bash
# Single region
dt config set secrets.gcp.locations australia-southeast1

# Multiple regions
dt config set secrets.gcp.locations australia-southeast1,australia-southeast2
```

## archive Options

The `archive.*` keys configure [`dt remote archive`](archive.md). They are
listed in the table above; see [Configuration](archive.md#configuration) in the
archive reference for the full discussion of each.

## hooks Options

`hooks.verbosity` sets git-hook output to `quiet`, `normal` (default), or
`verbose`; `dt hook run -v` forces verbose regardless.

Individual checks are configured under
`hooks.<hook-name>.checks.<check-name>.<setting>` — for example:

```bash
dt config set --project hooks.pre-commit.checks.large-files.max_size 10MB
dt config set --project hooks.pre-push.checks.dvc-push.mode prompt
```

Hook settings usually belong to the repository rather than the person, so these
examples pass `--project`; a bare `dt config set` writes user scope and would
apply the same limit everywhere.

Built-in defaults live in the code, so hooks work without any config file;
configured scopes override the defaults per leaf. See
[dt install](install.md#configuration) for the check names and settings.

## See also

- [dt config](config.md) - Set and get configuration values
- [Configuration Scopes](config_scopes.md) - Understanding scope hierarchy
- [dt add](add.md) - Add files with parallel checksums
- [dt push](push.md) - Push with parallel support
- [dt install](install.md) - Git hooks and their checks
- [dt remote archive](archive.md) - Cold-storage archiving
- [dt summary](summary.md) - Generate project documentation

