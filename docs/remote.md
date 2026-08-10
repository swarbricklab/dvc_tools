# dt remote

Remote storage management commands for configuring and working with DVC remotes in HPC environments.

## dt remote init

Sets up remote storage for the project with both SSH and local access methods.

### Usage

```bash
dt remote init [options] [project_name]
```

### Options

- `--name <project_name>`: Override project name (defaults to current directory name)
- `--remote-root <path>`: Override remote root directory (defaults to `remote.root` config value)
- `--remote-path <path>`: Override complete remote path (absolute or relative to current directory)

### What it does

- Creates the remote directory structure with proper group permissions
- Sets up SSH remote accessible from external platforms via `dvc remote add -d`
- Creates a local remote override for efficient transfers within the same system
- Maintains portability by keeping local remote configuration workspace-specific

### Examples

```bash
# Set up remote with default settings
# Uses: ${remote.root config}/${current directory name}
dt remote init

# Set up remote with custom root
# Uses: /g/data/a56/my-dvc-storage/my-project  
dt remote init --name my-project --remote-root /g/data/a56/my-dvc-storage

# Set up remote with complete custom path (absolute)
dt remote init --remote-path /g/data/a56/special-project/custom-remote

# Set up remote with relative path
dt remote init --remote-path ../shared-remote
```

### Path Resolution

The remote location is determined by (in order of precedence):

1. **`--remote-path`** - Complete path override (absolute or relative to current directory)
2. **Constructed path** - `${remote_root}/${project_name}` where:
   - **remote_root**: `--remote-root` argument OR `remote.root` config value
   - **project_name**: `--name` argument OR current directory name

**Default behavior** (no options): Uses `${remote.root config}/${current directory name}`

### Remote Configuration

Two remotes are configured:

- **Official remote**: Named after the platform (e.g., "nci"), accessible via SSH from anywhere
- **Local remote**: Named "local", provides direct filesystem access within the same platform
- SSH host determined by `ssh.host` config item (typically `gadi-dm.nci.org.au` on NCI)

## dt remote list

List DVC remotes from a repository (local or remote).

### Usage

```bash
dt remote list [repository] [--owner <owner>]
```

### Examples

```bash
# List remotes from current repository
dt remote list

# List remotes from a remote repository
dt remote list git@github.com:myorg/otherproject.git

# Using short name
dt remote list otherproject --owner myorg
```

### Output

```
storage    ssh://gadi-dm.nci.org.au/g/data/a56/dvc/neochemo (default)
local      /g/data/a56/dvc/neochemo [local]
```

The `[local]` marker indicates paths accessible on the local filesystem.

## dt remote verify

Exhaustively verify that every blob in a locally-accessible remote hashes to
the md5 implied by its path. This catches silent corruption and the
truncated/partial blobs left behind when a transfer is interrupted (e.g. a push
job hitting a walltime limit).

```bash
dt remote verify [remote]              # verify (incremental via ledger)
dt remote verify --full                # ignore ledger, re-hash everything
dt remote verify --report r.json       # also write a JSON report
dt remote verify --workers 32          # distribute over 32 compute nodes
dt remote verify --status              # instant status, no hashing (see below)
dt remote verify --recheck             # re-check only the last bad blobs
dt remote verify --recheck-report r.json  # ...from a specific report file
```

Verified blobs are recorded in a per-prefix ledger under `<remote>/.dt-verify/`
so later runs skip anything unchanged since it last passed; `--full` forces a
full re-hash and `--no-ledger` disables the ledger entirely.

### Persisted bad-blob report

Every scan writes its bad/incomplete findings to `<remote>/.dt-verify/bad.json`,
so the last scan's results survive between runs and feed `--recheck` and
`dt remote quarantine`. Overwriting on each scan naturally prunes blobs that now
pass.

### `--status` — instant status, no hashing

Returns immediately without hashing, reporting from the ledger + a stat-only
enumeration: how many blobs passed the previous scan (and when), how many were
bad at the last scan (from `bad.json`), the total blob count, and how many are
**unscanned or changed** since the ledger (and would be re-hashed on the next
incremental run).

```
Remote: bcarc_xenium  (dvc-v3)
  Verified OK: 12,500 (last scan 2026-07-01T09:14:00+00:00)
  Bad:         2 (as of 2026-07-01T09:14:00+00:00)
  Total blobs: 12,840 (1.2 TB)
  Unscanned/changed since ledger: 340 (would be re-hashed next run)
```

### `--recheck` — targeted re-check after a fix

Hashes **only** the previously-bad blobs (from `bad.json`, or a report file
passed with `--recheck-report`) to confirm a fix, instead of re-scanning the
whole store. Blobs that now pass are promoted into the ledger and pruned from
`bad.json`; a blob that has vanished (e.g. quarantined but not yet re-pushed) is
reported as `missing`.

## dt remote quarantine

Act on `dt remote verify` findings: **move** (not delete) each corrupt/incomplete
blob aside so that `dvc push`, run from a repo whose cache still holds a good
copy, re-uploads it. Move-not-delete keeps the corrupt bytes for forensics and
is reversible.

```bash
dt remote verify [remote]              # find bad blobs -> bad.json
dt remote quarantine [remote]          # move them (+ enclosing .dir) aside
dvc push                               # re-upload good copies
dt remote verify --recheck             # confirm just those
dt remote quarantine --list            # list quarantine batches
dt remote quarantine --restore <ts>    # move a batch back (reversible)
```

Options: `--from-report <path>` (default `<remote>/.dt-verify/bad.json`),
`--verify` (run a fresh scan first), `--dry-run`, `--restore <timestamp>`,
`--list`, `--json`.

**Why the enclosing `.dir` is quarantined too.** By default `dvc push` does not
examine a directory's member files if the directory's `.dir` object is already
present in the remote — it treats "`.dir` present" as "directory complete". So
quarantining a bad *member* blob alone would not force a re-push; the enclosing
`.dir` object must also be moved aside (even though it is intact). `.dir` blobs
are tiny, so re-pushing them is cheap. `dt remote quarantine` builds a
`md5 → [.dir]` reverse map by scanning the remote's `.dir` objects and
quarantines each enclosing `.dir` alongside its bad members. It handles v2/v3/
mixed layouts and the case where the bad blob *is* a `.dir`.

Quarantined blobs land in `<remote>/.dt-verify/quarantine/<timestamp>/<rel-path>`
with a `manifest.json` restore map.

## dt remote clean

Remove abandoned `.tmp` files left behind by interrupted transfers.

DVC uploads a blob by writing it under a random temporary name in the
destination's prefix directory, then renaming it into place:

```python
def tmp_fname(prefix: str = "") -> str:      # dvc_objects/fs/utils.py
    return f"{prefix}.{token_urlsafe(16)}.tmp"
```

A push killed before the rename — a cancelled job, a walltime kill, a dropped
connection — leaves the partial file behind permanently. Nothing in DVC ever
cleans these up, and because they are dotfiles they never appear in a casual
`ls`, so on a shared lab remote they quietly accumulate. A first sweep of one
lab's remote root found **703 files totalling 381 GiB**, most of it a year old.

```bash
dt remote clean                     # Report on the default remote
dt remote clean --delete            # Remove them
dt remote clean --all               # Report across every remote under remote.root
dt remote clean --all --delete      # Clean the whole remote root
dt remote clean --min-age 30        # Only files older than 30 days
dt remote clean --path /g/data/.../remote --json
```

| Option | Description |
|--------|-------------|
| `REMOTE_NAME` | Clean a named remote instead of the default |
| `--path PATH` | Clean a specific remote directory |
| `--all` | Every remote under `remote.root` |
| `--root PATH` | Remote root for `--all` (default: the `remote.root` config) |
| `--min-age DAYS` | Only remove files older than this (default: 7) |
| `--delete` | Actually remove the files. Without this, only reports. |
| `-j, --jobs N` | Concurrent prefix scanners (default: 8) |
| `--json` / `-v` | Machine-readable output / list every file |

**Reports by default.** Like `dt remote fsck --repair` and `dt cache validate
--fix`, this only tells you what it found unless you ask it to act. The report
alone is useful — the per-owner breakdown tells you whose interrupted pushes are
costing what.

### Why it is safe

**The name shape is matched exactly** — `^\.[A-Za-z0-9_-]{22}\.tmp$`, the
precise output of `tmp_fname()`. A loose `*.tmp` glob would happily delete
someone's `notes.tmp`.

**Only files older than `--min-age` are touched**, so a transfer still in flight
is never considered. Each file is also re-checked immediately before removal and
skipped if its mtime moved since the scan, which closes the window where a
stalled transfer resumed mid-sweep.

**Deleting a live temp file cannot corrupt the remote anyway.** The writer keeps
writing to the now-unlinked inode and its final rename fails with `ENOENT`, so
the worst case is a failed transfer that gets rerun — never a damaged blob.

**Empty prefix directories are always left in place.** DVC recreates a missing
prefix directory using the writing user's umask, which may not grant group
write, silently locking every other group member out of that prefix. Only files
are ever removed.

### Permissions

Deleting a file you do not own is normal and usually works. Removal depends on
write and execute permission on the **containing directory**, not on ownership
of the file — so on a group-writable remote, any group member can clear another
user's abandoned uploads.

Two things do block it: a directory that is not group-writable (only its owner
can clean it, regardless of who owns the files inside), and the sticky bit
(which restricts removal to each file's own owner).

Rather than guessing in advance — a pre-flight check would both race and misread
ACLs — the command attempts each removal and reports what failed, grouped by the
directory that blocked it, since that is what has to change:

```
  3 could not be removed (12.1G)
      /g/data/px14/dvc/datasets/foo/files/md5/3a  drwxr-sr-x  dir owner: ab1234
        3 file(s), 12.1G -- permission denied
      files owned by: cd5678 2, ef9012 1
```

The exit status is non-zero if any removal failed.

### Scope

Local paths and `ssh://` URLs pointing at a local host. Cloud remotes are
refused with an explanation rather than silently doing nothing: on `gs://` and
`s3://` the equivalent waste is abandoned *multipart uploads*, which are not
stray files and are cleared with a bucket lifecycle rule.

## dt remote archive

Archive a DVC remote to cold storage (e.g. NCI MDSS), verify it,
restore from it, and prune the on-disk copy once verified. See
[archive.md](archive.md) for the full reference.

```bash
dt remote archive create  <name>   # stage + deposit in one go
dt remote archive stage   <name>   # parallel inner tars on compute node
dt remote archive deposit <name>   # parallel uploads on data mover
dt remote archive list             # list archives recorded in .dt/archives/
dt remote archive verify  <name>   # sidecar + per-file existence/size
dt remote archive restore <name>   # full / per-prefix / single-object restore
dt remote archive prune   <name>   # delete on-disk remote after verify
dt remote archive destroy <name>   # delete an archive copy from the backend (does NOT touch source)
```

## Related Commands

- [`dt init`](init.md) - Initialize projects with remote setup
- [`dt cache init`](cache.md#init) - Set up local cache
- [`dt fetch`](fetch.md) - Fetch imports from local caches
- [`dt config`](config.md) - Configure remote settings
- [`dt remote archive`](archive.md) - Archive a remote to cold storage
- [`dt push`](push.md) - Push cache to remotes (re-uploads quarantined blobs)
- [`dt tmp`](tmp.md) - Manage temporary repository clones