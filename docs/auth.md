# dt auth

Verify and diagnose access to all storage backends used by a DVC project.

## Overview

A DVC project may depend on several storage backends simultaneously — a shared filesystem cache, an SSH remote, S3-compatible object storage (e.g. CloudFlare R2), GCS buckets, and git-hosted source repositories for imports. `dt auth` discovers every backend the current project touches and helps you verify, diagnose, and request access to each one.

> **Relationship to `dt doctor`:** `dt doctor` checks that *tools* are installed and *configuration* is sane. `dt auth` checks that you can actually *reach and use* the storage those tools point at.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| [`dt auth list`](#dt-auth-list) | Discover every storage endpoint the project uses |
| [`dt auth whoami`](#dt-auth-whoami) | Show current user identities across systems |
| [`dt auth check`](#dt-auth-check) | Test access to each endpoint |
| [`dt auth request`](#dt-auth-request) | Generate an access-request template from failures |
| [`dt auth teams`](#dt-auth-teams) | Manage GitHub team access for repositories |
| [`dt auth credentials`](#dt-auth-credentials) | Install S3 credentials from secret managers |
| [`dt auth grant`](#dt-auth-grant) | Grant a user access to a resource *(planned)* |

---

## dt auth list

Discover every storage endpoint the current project relies on.

### Usage

```bash
dt auth list [--type TYPE] [--repo URL] [--json]
```

### Options

| Option | Description |
|--------|-------------|
| `--type TYPE` | Filter to a specific endpoint type: `filesystem`, `ssh`, `s3`, `gs`, `http`, `git` |
| `--repo URL` | Discover endpoints for a remote repository (cloned to a temp dir). Accepts a full URL or a short name (resolved via the `owner` config key, same as `dt clone`). |
| `--json` | Output as JSON array |

The `--type` flag can be repeated to include multiple types:

```bash
# Only show filesystem and SSH endpoints
dt auth list --type filesystem --type ssh

# Only show cloud storage
dt auth list --type s3 --type gs

# Discover endpoints for a repo you haven't cloned
dt auth list --repo git@github.com:org/data-repo.git

# Use a short name (requires: dt config set owner <github-org>)
dt auth list --repo neochemo
```

### Sources scanned

| Source | What is discovered |
|--------|--------------------|
| DVC config | Configured remotes (SSH, local, S3, GCS, …) |
| `.dvc` files (`deps.repo.url`) | Source repositories used by `dvc import` |
| Import source remotes | Remotes of each source repo (via tmp clone) |
| dt config | `cache.root`, `remote.root`, `ssh.host` |
| Git config | `origin` and other git remote URLs |

### How import remotes are discovered

For each unique `repo.url` found in `.dvc` import files, `dt auth list` uses the existing tmp-clone infrastructure (`tmp_mod.clone_repo`) to obtain a shallow clone, then runs `dvc remote list` inside it. This is the same mechanism `dt fetch` uses to find locally-accessible caches for imports.

### Output

Each discovered endpoint is classified by type:

| Type | Examples |
|------|----------|
| `filesystem` | `/g/data/a56/dvc_cache`, `/scratch/dvc/remote` |
| `ssh` | `ssh://gadi.nci.org.au/g/data/a56/dvc_remote` |
| `s3` | `s3://my-bucket/dvc-remote` (AWS S3, CloudFlare R2, MinIO, …) |
| `gs` | `gs://my-bucket/dvc-remote` |
| `http` | `https://example.com/data` |
| `git` | `git@github.com:org/repo.git` |

```
$ dt auth list
Endpoints for project 'my-project':

  filesystem
    /g/data/a56/dvc_cache                       (cache.root)
    /g/data/a56/dvc_remote/my-project           (remote.root)

  ssh
    ssh://gadi.nci.org.au/g/data/a56/dvc_remote (DVC remote 'origin')
      → local equivalent: /g/data/a56/dvc_remote (host is local)

  s3
    s3://my-r2-bucket/dvc                       (DVC remote 'cloud')

  git
    git@github.com:org/data-repo.git            (import source)
      → remote: ssh://gadi.nci.org.au/g/data/a56/dvc_remote/data-repo
```

### JSON output

```bash
dt auth list --json
```

Returns a JSON array of endpoint objects, each with `type`, `url`, `source` (where it was discovered), and optional `local_path` (for SSH remotes on the local host).

---

## dt auth whoami

Show current user identities across all relevant systems.

### Usage

```bash
dt auth whoami [--detect] [--save] [--json]
```

| Option | Description |
|--------|-------------|
| `--detect` | Probe external tools (`gh`, `gcloud`, `aws`) to auto-detect active accounts |
| `--save` | Detect + save results to user config (implies `--detect`) |
| `--json` | Output as JSON |

Without flags, displays the local username and any identities stored in dt config.

### Examples

```
$ dt auth whoami
  NCI username:   alice           (detected)
  GitHub user:    alice-smith     (config)
  GitHub teams:   data-team, ops  (config)
```

```
$ dt auth whoami --detect
Detecting identities...
  ✓ NCI username:   alice                          (detected)             matches config
  ✓ GitHub user:    alice-smith                     (detected via gh api)  matches config
  ✗ GitHub teams:   data-team, ops, new-team        (detected via gh api)
      config has: data-team, ops
  ● GCP email:      alice@proj.iam.gserviceaccount.com  (detected via gcloud)  not in config
```

```
$ dt auth whoami --save
Detecting identities...
  ...
✓ Saved 2 identity value(s) to user config.
```

### Configuration

```bash
# Store identities manually
dt config set --user auth.github_user alice-smith
dt config set --user auth.github_teams 'data-team, ops'
dt config set --user auth.gcp_email alice@proj.iam.gserviceaccount.com
```

See [Configuration Options](config_options.md#auth-options) for all identity keys.

---

## dt auth check

Test whether the current user can actually access each discovered endpoint.

### Usage

```bash
dt auth check [--type TYPE] [--repo URL] [--verbose] [--user USERNAME] [--json]
```

### Options

| Option | Description |
|--------|-------------|
| `--type TYPE` | Only check specific endpoint type(s): `filesystem`, `ssh`, `s3`, `gs`, `http`, `git` |
| `--repo URL` | Check endpoints for a remote repository (cloned to a temp dir). Accepts a full URL or a short name. |
| `--verbose` | Show per-subdirectory detail for filesystem checks |
| `--user USERNAME` | Check access from another user's perspective (admin use) |
| `--json` | Output as JSON |

```bash
# Only check filesystem access (cache and remote directories)
dt auth check --type filesystem

# Only check S3/R2 credentials
dt auth check --type s3

# Check everything except git repos
dt auth check --type filesystem --type ssh --type s3 --type gs

# Check endpoints for a repo you haven't cloned
dt auth check --repo git@github.com:org/data-repo.git

# Use a short name
dt auth check --repo neochemo
```

### Ownership info for failed subdirectories

When filesystem subdirectories fail the access check, `dt auth check` shows the **owner and group** of each failing directory. This tells you who to ask to run `setfacl` to grant access:

```
  ✗ /g/data/a56/dvc_cache
      files  not writable  (owner: ab1234, group: a56)
    Fix permissions: setfacl -R -m u:jsmith:rwx /g/data/a56/dvc_cache/files
    Ask ab1234 to run the setfacl command(s) above
```

### Checks by endpoint type

#### Filesystem (`/path/to/...`)

- Verify the root path exists
- **Walk all immediate subdirectories** and check read/write permissions on each
- Report any subdirectory that is not readable or not writable
- With `--verbose`, list every subdirectory and its permission status

This catches the common case where the cache root is accessible but individual project subdirectories have restrictive group ownership.

#### SSH remotes

- **Local-host equivalence**: if the SSH host is on the same platform (using the same `is_local_host` logic as `dt fetch`), check the *local filesystem path* directly — including the subdirectory walk described above
- **Remote SSH**: attempt `ssh -T -o BatchMode=yes -o ConnectTimeout=5 <host>` to verify the connection works. If it fails, suggest checking SSH key forwarding (`ssh-add -l`) rather than key existence, since keys are often forwarded from a laptop to HPC

#### S3-compatible storage (`s3://...`)

CloudFlare R2 buckets (and other S3-compatible stores) use the S3 protocol but are not AWS. The check uses the `aws` CLI with the endpoint URL from DVC config if available:

1. `aws sts get-caller-identity --endpoint-url <endpoint>` — verify credentials are configured
2. `aws s3 ls <bucket-prefix> --endpoint-url <endpoint>` — verify bucket access

If no `endpointurl` is configured in DVC, falls back to the default AWS endpoint. The endpoint URL is read from the DVC remote config (`dvc remote modify <name> endpointurl <url>`).

> **Note**: the `aws` CLI must be installed and configured with appropriate credentials. For R2, this means an Access Key ID and Secret Access Key in `~/.aws/credentials` or environment variables, with the correct `--endpoint-url`.

#### GCS (`gs://...`)

- `gcloud auth list` — check if any account is authenticated
- `gsutil ls <bucket-prefix>` — verify bucket access
- **Soft failure**: GCS checks report warnings rather than errors, since the team is transitioning from service-account auth to IAM

#### Git repositories

- `git ls-remote --exit-code <url>` — verify the repo is reachable
- If this fails and the URL is SSH-based, suggest checking SSH agent forwarding (`ssh-add -l`)

#### HTTP(S) URLs

- `curl -sf --head <url>` — verify the URL is reachable

### Output

```
$ dt auth check
Checking access for project 'my-project'...

  ✓ /g/data/a56/dvc_cache                       read/write (12/12 subdirs OK)
  ✓ /g/data/a56/dvc_remote/my-project           read/write (12/12 subdirs OK)
  ✓ ssh://gadi.nci.org.au/...                    → checked as local path
  ✓ s3://my-r2-bucket/dvc                        credentials OK, bucket accessible
  ⚠ gs://my-gcs-bucket/dvc                       no gcloud auth configured (warning)
  ✓ git@github.com:org/data-repo.git             reachable

5 passed, 0 failed, 1 warning.
```

With `--verbose`:

```
$ dt auth check --verbose
Checking access for project 'my-project'...

  ✓ /g/data/a56/dvc_cache
      /g/data/a56/dvc_cache/00  ✓ r/w
      /g/data/a56/dvc_cache/01  ✓ r/w
      ...
      /g/data/a56/dvc_cache/ff  ✓ r/w
      /g/data/a56/dvc_cache/files/md5  ✓ r/w
    12 subdirectories, all accessible

  ✗ /scratch/dvc/project-x
      /scratch/dvc/project-x        ✓ r/w
      /scratch/dvc/project-x/files  ✗ not readable (group: other-project)
    1 of 2 subdirectories failed
    Hint: ask the owner to run: chmod -R g+rw /scratch/dvc/project-x/files
```

### SSH key forwarding

When an SSH access check fails, `dt auth check` will **not** look for local SSH keys (they may not exist on HPC — keys are typically forwarded from a laptop via `ssh -A`). Instead it will suggest:

```
  ✗ ssh://gadi.nci.org.au/...   connection failed
    Hint: check your SSH agent has keys loaded: ssh-add -l
    Hint: ensure you connected with agent forwarding: ssh -A <host>
```

### Checking access for another user (`--user`)

Admins can check whether a specific user has access to project
endpoints **without needing sudo**. The `--user` flag simulates
permission checks from that user's perspective:

```bash
# Check if user 'ab1234' can access all endpoints
dt auth check --user ab1234

# Only check filesystem access for that user
dt auth check --user ab1234 --type filesystem
```

#### How it works

| Endpoint type | Method |
|---------------|--------|
| **filesystem** | Resolves the user's uid/gid/supplementary groups via `pwd` and `grp`, then checks file mode bits from `os.stat()` plus POSIX ACLs via `getfacl` |
| **git** (GitHub) | Queries `gh api repos/{owner}/{repo}/collaborators/{user}/permission` to get the user's permission level |
| **ssh** (local path) | Same as filesystem — checks the local path |
| s3, gs, http, ssh (remote) | Skipped — cannot determine another user's credentials |

Example output:

```
$ dt auth check --user ab1234
Checking access for user: ab1234

  ✓ /g/data/a56/dvc_cache                       read/write for ab1234
  ✗ /g/data/a56/dvc_remote/my-project           not readable by ab1234
    Hint: Grant access: setfacl -R -m u:ab1234:rwx /g/data/a56/dvc_remote/my-project
  ✓ git@github.com:org/data-repo.git             write access for ab1234
  – s3://my-r2-bucket/dvc                        cannot check s3 access for another user
```

---

## dt auth request

Generate an access-request message from the results of `dt auth check`,
and optionally deliver it via Slack or email.

### Usage

```bash
dt auth request [--type TYPE] [--repo URL] [--format text|markdown|json] [--send [slack|email]]
```

| Option | Description |
|--------|-------------|
| `--type TYPE` | Only include failures for specific endpoint type(s) |
| `--repo URL` | Generate request for a remote repository. Accepts a full URL or short name. |
| `--format` | Output format: `text` (default), `markdown`, or `json` |
| `--send` | Send the request. Omit the value to auto-detect (Slack → email), or specify `slack` or `email` explicitly. |

Runs `dt auth check` internally (respecting `--type` filters), collects failures, and produces a template that can be sent to an administrator or pasted into a support ticket.

The request automatically includes the user's **identities** (NCI username, GitHub user, GitHub teams, GCP email, AWS identity) so admins know which accounts to grant access to. Identities are gathered from config and auto-detection (same as `dt auth whoami`).

With `--send`, the request is delivered directly:

- **Slack** — Posts to an incoming-webhook URL configured via `auth.slack_webhook`.
- **Email** — Pipes the text-format request to the local `mail` command, addressed to `auth.admin_email`.

Auto-detect (`--send` with no argument) tries Slack first, then email.

### Configuration for --send

```bash
# Slack webhook (system scope recommended)
dt config set --system auth.slack_webhook 'https://hooks.slack.com/services/T.../B.../xxx'

# Admin email (system or user scope)
dt config set --system auth.admin_email 'admin@example.com'
```

### Examples

```
$ dt auth request
Access request for user 'jsmith' on project 'my-project'

The following resources are not accessible:

  1. Filesystem: /scratch/dvc/project-x/files
     Status: not readable
     Required: read/write access
     Suggested fix: chmod -R g+rw /scratch/dvc/project-x/files
                    (or add user 'jsmith' to the owning group)

  2. S3: s3://my-r2-bucket/dvc
     Status: credentials not configured
     Required: read access (at minimum)
     Suggested fix: configure aws credentials for the R2 endpoint

Identities:
  NCI username: jsmith
  GitHub user: jsmith-gh
  GitHub teams: org/data-team

Platform: gadi-dm.nci.org.au
dt version: 0.2.0
Date: 2026-02-12
```

```bash
# Send directly to Slack
$ dt auth request --send slack

# Auto-detect delivery method
$ dt auth request --send

# Send via email
$ dt auth request --send email
```

---

## dt auth teams

Manage GitHub team access for repositories. Wraps the `gh` CLI to provide quick commands for the most common team/repo access operations.

### Subcommands

| Subcommand | Description |
|------------|-------------|
| `dt auth teams repo <URL>` | List teams with access to a repository |
| `dt auth teams user <USERNAME> --org <ORG>` | List teams a user belongs to |
| `dt auth teams add-to-repo <TEAM> <URL>` | Add a team to a repository |
| `dt auth teams add-user <USER> <TEAM> --org <ORG>` | Add a user to a team |

All subcommands support `--json` output where applicable.

Repository arguments accept a **full URL** or a **short name** (resolved via the `owner` config key, same as `dt clone`).

### Examples

```bash
# List teams with access to a repo
dt auth teams repo git@github.com:org/data-repo.git

# Same, using a short name
dt auth teams repo neochemo

# List teams that alice belongs to in the org
dt auth teams user alice --org myorg

# Grant a team push access to a repo (short name or full URL)
dt auth teams add-to-repo data-team neochemo

# Grant read-only access
dt auth teams add-to-repo data-readers git@github.com:org/repo.git --permission pull

# Preview without making changes
dt auth teams add-to-repo data-team git@github.com:org/repo.git --dry

# Add a user to a team
dt auth teams add-user alice data-team --org myorg
```

> **Note**: These commands require the `gh` CLI to be authenticated with appropriate permissions (org admin or team maintainer for write operations).

---

## dt auth credentials

Manage S3 remote credentials by fetching them from a secret manager and installing them into `.dvc/config.local`.

### Subcommands

| Subcommand | Description |
|------------|-------------|
| `dt auth credentials install` | Fetch and install credentials from secret manager |
| `dt auth credentials uninstall` | Remove credentials from `.dvc/config.local` |
| `dt auth credentials status` | Show which remotes have credentials installed |

### Configuration

Add to `.dt/config.yaml` or user config:

```yaml
secrets:
  backend: gcp
  prefix: dvc-remote-    # Optional, default "dvc-remote-"
  gcp:
    project: my-gcp-project
```

### Secret format

Secrets are named by **repository** (e.g., `dvc-remote-neochemo` for the `neochemo` repo) and contain **raw DVC INI config** that gets appended directly to `.dvc/config.local`:

```ini
['remote "cloud"']
    access_key_id = AKIAXXXXXXXX
    secret_access_key = xxxxx
    endpointurl = https://xxx.r2.cloudflarestorage.com

['remote "backup"']
    access_key_id = AKIAYYYYYYYY
    secret_access_key = yyyyy
    endpointurl = https://yyy.r2.cloudflarestorage.com
```

This is the same format as `.dvc/config.local`. The secret content is appended directly to the file.

### Examples

```bash
# Install credentials
dt auth credentials install

# Install with verbose output
dt auth credentials install -v

# See status of all S3 remotes
dt auth credentials status

# Remove installed credentials
dt auth credentials uninstall
```

### Security

- Credentials are written to `.dvc/config.local`, which is gitignored by default
- File permissions are set to `600` (owner read/write only)
- Requires GCP authentication via `gcloud auth login` or service account

### Supported backends

| Backend | Status | Configuration |
|---------|--------|---------------|
| GCP Secret Manager | ✅ Available | `secrets.backend: gcp` |
| AWS Secrets Manager | 🔜 Planned | `secrets.backend: aws` |

---

## dt auth grant

> **Status**: Not yet implemented.

Planned command to grant a user access to a resource.

### Planned capabilities

| Resource type | Mechanism | Notes |
|---------------|-----------|-------|
| Filesystem | `setfacl` / group management | Deferred — the admin typically does not own the cache files and lacks sudo. Use `dt auth check --user` + ownership info to identify who to ask. |
| GitHub repo | `gh api` collaborator invite or `dt auth teams add-to-repo` | Team-based access is available now via `dt auth teams`. |
| S3/R2 bucket | Policy suggestion (manual) | Planned |
| GCS bucket | IAM binding suggestion (manual) | Planned |

For now, use `dt auth teams` to manage GitHub-level access and `dt auth check` with ownership info to identify the right person to grant filesystem access.

---

## Build order

This command is being built incrementally:

1. **`dt auth list`** — pure discovery, no side effects ✅
2. **`dt auth whoami`** — identity management and detection ✅
3. **`dt auth check`** — read-only access tests ✅
4. **`dt auth request`** — template generation from check results ✅
5. **`dt auth teams`** — GitHub team management ✅
6. **`dt auth credentials`** — secret manager integration for S3 credentials ✅
7. **`dt auth grant`** — admin actions (not yet implemented; use `dt auth teams` for GitHub access)

---

## Design notes

### Separation from `dt doctor`

`dt doctor` verifies that tools are installed and configuration is syntactically correct. `dt auth` verifies that the configured resources are actually *accessible*. The two commands are complementary but serve different audiences:

- `dt doctor` answers: "Is my environment set up correctly?"
- `dt auth check` answers: "Can I reach the data I need?"

### S3-compatible storage (CloudFlare R2, MinIO, etc.)

DVC's S3 remotes work with any S3-compatible endpoint, not just AWS. `dt auth` reads the `endpointurl` from DVC remote config to ensure checks target the correct service. The `aws` CLI works with R2 and other S3-compatible stores when given the right `--endpoint-url`.

### SSH key forwarding vs local keys

In HPC environments, SSH keys are typically on a user's laptop and forwarded to the HPC login node via `ssh -A`. `dt auth check` does not require local SSH keys to exist — it tests the SSH connection directly and, on failure, suggests checking the SSH agent (`ssh-add -l`) rather than generating new keys.

### Local-host equivalence for SSH remotes

When an SSH remote's host matches the current platform (same hostname or same domain, per `remote.is_local_host`), `dt auth check` tests the *local filesystem path* directly instead of attempting an SSH connection. This is the same logic used by `dt fetch` to avoid unnecessary network round-trips.

### GCS transition

The team currently uses GCS via service accounts. A transition to IAM-based auth is planned. For now, `dt auth check` reports GCS authentication status as a warning rather than an error, to avoid blocking workflows during the transition.
