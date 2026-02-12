# dt auth

Verify and diagnose access to all storage backends used by a DVC project.

## Overview

A DVC project may depend on several storage backends simultaneously — a shared filesystem cache, an SSH remote, S3-compatible object storage (e.g. CloudFlare R2), GCS buckets, and git-hosted source repositories for imports. `dt auth` discovers every backend the current project touches and helps you verify, diagnose, and request access to each one.

> **Relationship to `dt doctor`:** `dt doctor` checks that *tools* are installed and *configuration* is sane. `dt auth` checks that you can actually *reach and use* the storage those tools point at.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| [`dt auth list`](#dt-auth-list) | Discover every storage endpoint the project uses |
| [`dt auth check`](#dt-auth-check) | Test access to each endpoint |
| [`dt auth request`](#dt-auth-request) | Generate an access-request template from failures |
| [`dt auth grant`](#dt-auth-grant) | Grant a user access to a resource (admin) |

---

## dt auth list

Discover every storage endpoint the current project relies on.

### Usage

```bash
dt auth list [--type TYPE] [--json]
```

### Options

| Option | Description |
|--------|-------------|
| `--type TYPE` | Filter to a specific endpoint type: `filesystem`, `ssh`, `s3`, `gs`, `http`, `git` |
| `--json` | Output as JSON array |

The `--type` flag can be repeated to include multiple types:

```bash
# Only show filesystem and SSH endpoints
dt auth list --type filesystem --type ssh

# Only show cloud storage
dt auth list --type s3 --type gs
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

## dt auth check

Test whether the current user can actually access each discovered endpoint.

### Usage

```bash
dt auth check [--type TYPE] [--verbose]
```

### Options

| Option | Description |
|--------|-------------|
| `--type TYPE` | Only check specific endpoint type(s): `filesystem`, `ssh`, `s3`, `gs`, `http`, `git` |
| `--verbose` | Show per-subdirectory detail for filesystem checks |

```bash
# Only check filesystem access (cache and remote directories)
dt auth check --type filesystem

# Only check S3/R2 credentials
dt auth check --type s3

# Check everything except git repos
dt auth check --type filesystem --type ssh --type s3 --type gs
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

---

## dt auth request

Generate an access-request message from the results of `dt auth check`.

### Usage

```bash
dt auth request [--type TYPE] [--format text|markdown|json]
```

| Option | Description |
|--------|-------------|
| `--type TYPE` | Only include failures for specific endpoint type(s) |
| `--format` | Output format: `text` (default), `markdown`, or `json` |

Runs `dt auth check` internally (respecting `--type` filters), collects failures, and produces a template that can be sent to an administrator or pasted into a support ticket.

### Example

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

Platform: gadi-dm.nci.org.au
dt version: 0.2.0
Date: 2026-02-12
```

---

## dt auth grant

Grant a user access to a resource. Built gradually — initial support for POSIX filesystem permissions.

### Usage

```bash
dt auth grant <user> <resource> [--level read|write] [--dry]
```

### Planned capabilities

| Resource type | Mechanism | Status |
|---------------|-----------|--------|
| Filesystem | `setfacl` / group management | First to build |
| GitHub repo | `gh api` collaborator invite | Planned |
| S3/R2 bucket | Policy suggestion (manual) | Planned |
| GCS bucket | IAM binding suggestion (manual) | Planned |

### Example

```bash
# Grant write access to a cache directory
dt auth grant jsmith /g/data/a56/dvc_cache --level write --dry

# Would run:
#   setfacl -R -m u:jsmith:rwx /g/data/a56/dvc_cache
```

The `--dry` flag shows what commands would be run without executing them.

> **Note**: `dt auth grant` requires appropriate admin permissions. It is designed to be extended over time as new resource types are encountered.

---

## Build order

This command is being built incrementally:

1. **`dt auth list`** — pure discovery, no side effects
2. **`dt auth check`** — read-only access tests
3. **`dt auth request`** — template generation from check results
4. **`dt auth grant`** — admin actions, built last

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
