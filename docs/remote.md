# dt remote

Remote storage management commands for configuring and working with DVC remotes in HPC environments.

## dt remote init

Sets up remote storage for the project with both SSH and local access methods.

### Usage

```bash
dt remote init [options] [project_name]
```

### Arguments

- `[project_name]`: Optional project name, equivalent to `--name` (`--name` wins if both are given)

### Options

- `--name <project_name>`: Override project name (defaults to current directory name)
- `--remote-root <path>`: Override remote root directory (defaults to the first `remote.root` config entry)
- `--remote-path <path>`: Override complete remote path (absolute or relative to current directory)

### What it does

- Creates the remote directory structure with proper group permissions —
  all 256 blob prefix directories up front, so DVC never creates one itself
  under the writing user's umask
- Applies the shared-directory policy: setgid, group-writable, **sticky**, and
  no access for users outside the group (`3770`). Sticky means everyone can
  still push, but only a file's owner can delete it. Override with the
  `perms.sticky` / `perms.allow_other` config keys, and audit later with
  [`dt remote perms`](#dt-remote-perms)
- Sets up an SSH remote accessible from external platforms via `dvc remote add -d`
  (only when `ssh.host` is configured)
- Creates a local remote override for efficient transfers within the same system
- Maintains portability by keeping local remote configuration workspace-specific

If the remote directory already exists, it is reused as-is: the structure is
**not** re-created and permissions are **not** re-applied. Use
[`dt remote perms --fix`](#dt-remote-perms) to bring an existing remote back to
policy.

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
   - **remote_root**: `--remote-root` argument OR the first `remote.root` config entry
   - **project_name**: `--name` argument OR the positional argument OR current directory name

**Default behavior** (no options): Uses `${remote.root config}/${current directory name}`

### Remote Configuration

Up to two remotes are configured:

- **SSH remote**: named after the project (the `--name` value, or the remote
  directory's basename), written to `.dvc/config` as the default remote and
  reachable from anywhere. The host comes from the `ssh.host` config item
  (typically `gadi-dm.nci.org.au` on NCI). **If `ssh.host` is not set, this
  remote is not created at all** and only the local remote is configured.
- **Local remote**: named `local`, written to `.dvc/config.local` (so it stays
  out of git) and made the default there, giving direct filesystem access
  within the same platform.

An existing remote name is left as-is rather than being overwritten.

## dt remote list

List DVC remotes from a repository (local or remote).

### Usage

```bash
dt remote list [repository] [--owner <owner>] [--all]
```

### Options

- `--owner <owner>`: Override the GitHub owner for short names
- `--all`: Show remotes from all config scopes, including local overrides

By default only remotes defined in the shared project config (`.dvc/config`)
are listed. The `local` remote written by [`dt remote init`](#dt-remote-init)
lives in `.dvc/config.local`, so it appears only with `--all`.

### Examples

```bash
# List remotes from current repository (project scope)
dt remote list

# Include local overrides from .dvc/config.local
dt remote list --all

# List remotes from a remote repository
dt remote list git@github.com:myorg/otherproject.git

# Using short name
dt remote list otherproject --owner myorg
```

### Output

```
$ dt remote list
storage (default): ssh://gadi-dm.nci.org.au/g/data/a56/dvc/neochemo

$ dt remote list --all
storage: ssh://gadi-dm.nci.org.au/g/data/a56/dvc/neochemo
local (default): /g/data/a56/dvc/neochemo
```

The `(default)` marker is `core.remote` **as read in the same scopes as the
listing**. That is why it moves: in project scope the SSH remote is the default,
but once `.dvc/config.local` is included, its `local` override wins.

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
dt remote verify --no-egress           # cloud: size check, no download (see below)
dt remote verify --status              # instant status, no hashing (see below)
dt remote verify --recheck             # re-check only the last bad blobs
dt remote verify --recheck-report r.json  # ...from a specific report file
```

Verified blobs are recorded in a per-prefix ledger under `<remote>/.dt-verify/`
so later runs skip anything unchanged since it last passed; `--full` forces a
full re-hash and `--no-ledger` disables the ledger entirely.

### `--no-egress` / `--size-only` — cloud remotes, zero download

The full re-hash above reads every blob, so it only works on a
locally-accessible remote. On an **object-store remote (`s3://`, Cloudflare
R2)** re-hashing means **downloading every blob** — egressing the whole remote,
which is cost- and time-prohibitive at TB scale.

`--no-egress` verifies a cloud remote **without downloading any blob content**.
It lists the remote's objects and their sizes from store metadata (a single
recursive `LIST`, giving each object's `ContentLength`) and compares each to the
**expected size** DVC recorded for that blob (derived from the repo's tracked
`.dir` manifests / `.dvc` outs). A size that differs from expected is a
definitive corruption signal at zero egress.

```bash
dt remote verify wgs --no-egress               # size check the s3://wgs remote
dt remote verify wgs --no-egress --report r.json  # feed quarantine via a report
```

**What it catches:** truncation / interrupted-transfer corruption — a partial
object sitting at the intact file's md5 path (`remote size != expected size`).
This is exactly the failure that made `dt remote verify` necessary, and on R2 it
is now detectable for ~free instead of egressing the whole remote.

**What it does *not* catch:** same-size content corruption (wrong bytes at the
right length). Detecting that still requires a full re-hash verify, because the
only way to confirm content on an object store is to download it. Objects with
no locally-known expected size (`.dir` manifests, or blobs not tracked in the
current checkout) are reported as *unchecked*, not bad.

Credentials, endpoint and R2 settings are read from `.dvc/config` via DVC's own
cloud plumbing — the same path `dvc push`/`pull` use — so no extra configuration
is needed. The report is the same shape as a full verify (and so feeds
`dt remote quarantine` via `--report`), with `size_mismatch` as the bad-blob
status.

### Persisted bad-blob report

Every scan writes its bad/incomplete findings to `<remote>/.dt-verify/bad.json`,
so the last scan's results survive between runs and feed `--recheck` and
`dt remote quarantine`. Overwriting on each scan naturally prunes blobs that now
pass. The write is best-effort: on a remote where `.dt-verify/` cannot be
created it is skipped, and `--recheck` / `dt remote quarantine` then need an
explicit `--recheck-report` / `--from-report`.

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
`--from-list <path>` (externally-known bad set, see below), `--verify` (run a
fresh scan first), `--dry-run`, `--restore <timestamp>`, `--list`, `--json`.
`--verify`, `--from-report` and `--from-list` are mutually exclusive.

**Acting on an externally-known bad set (`--from-list`).** Sometimes the bad
blobs are already known from a source of truth other than `dt remote verify` —
e.g. on `bcarc_wgs` 310 bad R2 blobs were identified by comparing remote blob
sizes against the intact upstream S3 objects. Running a full `dt remote verify`
just to regenerate that list could egress terabytes. `--from-list` feeds the
known set straight into the same quarantine machinery:

```bash
dt remote quarantine [remote] --from-list bad_md5s.txt
dvc push                                # re-upload good copies
dt remote verify --recheck              # confirm just those
```

The list is plain text, **one entry per line**, where each entry is either:

- a bare **md5** (32 lowercase hex), optionally with a `.dir` suffix for a
  directory manifest object (e.g. `a1b2…`), or
- a **blob path** relative to the remote root (e.g. `files/md5/a1/b2…`, or the
  v2 form `a1/b2…`), or an absolute path under the remote.

Blank lines and `#` comments are ignored, and only the first whitespace-delimited
field on each line is read — so `<md5> <size>` columns (or a trailing
`# note`) from a size-comparison tool feed in directly. Each md5 is resolved to
its blob path under the remote's detected layout (v2/v3/mixed), normalised into
the same bad-entry record `dt remote verify` writes (`{path, expected_md5,
status: mismatch}`), and then driven through the identical move-aside logic —
including pulling in each enclosing `.dir` object (essential, see below).
Lines that are neither a valid md5 nor a blob path reconstructing a valid md5
are reported and skipped.

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

## dt remote diff

Compare the blob sets of two locally-accessible remotes.

```
dt remote diff REMOTE_A REMOTE_B [--list] [--json] [-j N]
```

Each argument is either a configured remote **name** or a filesystem **path**.
A bare token that matches a configured remote resolves to that remote; anything
containing a `/` (or any non-remote token) is treated as a path — so the *old*
location, which is usually not a configured remote, can be compared directly.

The report gives, for each side, the object count and total size, plus how many
objects are **only in A**, **only in B**, and **common**:

```
A: /g/data/a56/dvc/analysis/brca_mega_atlas  (dvc-v2)
   79,783 object(s), 2.1T
B: nci  (dvc-v3)
   240,612 object(s), 5.4T
  Common:    79,779 (2.1T)
  Only in A: 4 (199G)
  Only in B: 160,833 (3.3T)
  B is a subset of A (top-up B with: dt remote merge A B).
```

Blob identity is **content-addressed and layout-independent**: the same object
stored under a v2 layout (`ab/cdef…`) in one remote and a v3 layout
(`files/md5/ab/cdef…`) in the other is counted as *common*, not differing. This
is what diagnoses a half-finished migration — the case where blobs are split
across an old and a new location with neither a superset of the other, so
`dvc pull` against the configured remote fails for the handful that only ever
reached the old location and they look simply "missing". `--list` prints the
differing blob keys; `--json` emits the full machine-readable structure.

## dt remote merge

Top-up one remote from another: copy the blobs present in `SRC` but missing from
`DST`, so `DST` becomes a superset.

```
dt remote merge SRC DST [--dry-run] [--verify] [--group-writable] [-j N]
```

This is the fix for the half-migrated remote diagnosed by `dt remote diff`.
Unlike `dt remote move` — which relocates a remote and **deletes the source**,
repointing `.dvc/config` — `merge` touches neither the config nor the source, so
other repos still pointing at `SRC` are unaffected. Unlike `dt remote copy`,
which refuses a non-empty destination, `merge` is designed to add into a
destination that already holds most of the data.

Properties:

- **Additive** — never deletes or overwrites a blob already in `DST`.
- **Atomic** — each blob is written to a `.tmp` sibling and `os.replace()`d onto
  its final name, so a concurrent reader never sees a partial blob.
- **Verifying** (`--verify`) — re-hash each copied blob against its
  content-addressed filename *before* publishing it; a mismatch is reported and
  the blob is **not** copied, so corruption is never propagated.
- **Idempotent / resumable** — a second run, or a run after an interruption,
  only copies what is still missing.
- Works on a `SRC`/`DST` **path** that is not a configured remote.

`--dry-run` reports how many objects and bytes *would* transfer and writes
nothing — the recommended first step on shared, multi-TB storage:

```
dt remote diff /g/data/a56/dvc/analysis/brca_mega_atlas nci   # see the gap
dt remote merge /g/data/a56/dvc/analysis/brca_mega_atlas nci --dry-run
dt remote merge /g/data/a56/dvc/analysis/brca_mega_atlas nci --verify
dt remote verify nci --recheck     # optional: confirm the newly-landed blobs
```

If `DST` does not exist yet it is created and every `SRC` blob is treated as
missing (a plain full copy); `--group-writable` makes any newly-created prefix
directories group-writable + setgid for shared access. On the same filesystem
the copy is a real byte copy (content-addressed stores are not reflinked here to
keep the destination independent of the soon-to-be-retired source).

### Deferred (issue #166 follow-ups)

Two asks from the originating issue are intentionally **not** in this command and
are tracked as follow-ups:

- **Pull-safety / availability diagnosis.** `dt pull --dry` classifies a remote
  by reachability, not object presence, so a target whose blobs are entirely
  absent can still read as "✓ local". And a `dt pull` on a partially-missing
  target can remove the workspace copy before a failed fetch. Making pull check
  object presence (and fetch-then-checkout only what verified) is a separate
  change to the pull path.
- **Cloud (non-local) remotes.** `diff`/`merge` require both endpoints on a
  locally-accessible filesystem; an S3/GCS source or destination would need the
  object-store transfer path.

## dt remote perms

Check and repair directory permissions on a shared remote.

A shared remote only works if its blob directories stay group-writable and
setgid. [`dt remote init`](#dt-remote-init) pre-creates all 256 prefix
directories precisely so that DVC never has to — where that hasn't run, DVC
creates each prefix on demand under the writing user's umask, frequently
leaving it unwritable by everyone else. The damage is silent until somebody
else's push fails.

```bash
dt remote perms                     # Report on the default remote
dt remote perms --all               # Report across every remote
dt remote perms --fix               # Repair what you own
dt remote perms --all --fix         # ...across the whole remote root
```

| Option | Description |
|--------|-------------|
| `REMOTE_NAME` | Check a named remote instead of the default |
| `--path PATH` | Check a specific remote directory |
| `--all` | Every remote under `remote.root` |
| `--root PATH` | Remote root for `--all`; repeatable, replaces the configured list |
| `--fix` | Apply the policy. Without this, only reports. |
| `--sticky` / `--no-sticky` | Require the sticky bit (default: on, or the `perms.sticky` config) |
| `--allow-other` / `--no-other` | Permit world read/execute (default: off, or the `perms.allow_other` config) |
| `-j, --jobs N` | Concurrent stat workers (default: 8) |
| `--json` / `-v` | Machine-readable output / list every directory |

### The policy

| Mode | Meaning |
|------|---------|
| **`3770`** | **setgid + group write + sticky, no access for others (default)** |
| `2770` | as above without sticky (`--no-sticky`) |
| `3775` | sticky, world read/execute permitted (`--allow-other`) |
| `2775` | neither |

setgid keeps the group on newly created entries.

Only *missing* bits count as deviations. A directory that is **more restrictive**
than the policy is left alone — so `--allow-other` tolerates world-readable
directories, it never grants world access to one that lacks it. Loosening
permissions is not something a repair tool should do silently.

`dt remote init` and `dt cache init` create directories under this same policy,
resolved from the same config keys, so a store cannot be created under one
policy and audited under another.

### The sticky bit

Sticky is **on by default**. It adds `+t`, which on a directory means a file may be removed or
renamed only by **the file's owner, the directory's owner, or root**. Everyone
with group write can still create files and push — only deletion is restricted.
This is the mechanism `/tmp` uses, and it works on Lustre.

Two consequences worth knowing:

**Each remote's owner keeps full control of it.** Because the directory owner is
also exempt, whoever initialised a remote can still delete and move anything
inside it — including for recovery flows like
[`dt remote quarantine`](#dt-remote-quarantine). Sticky protects you from *other
people's* accidents in your remote without taking away your own authority.

**Sticky is not inherited.** A new subdirectory gets setgid but not `+t` (and
under a typical umask, not group write either), so a remote that loses a prefix
directory will drift. Because `dt remote init` pre-creates all 256, this is rare
in practice — but it's why `perms` exists as a re-runnable check rather than a
one-off.

### Several roots

`remote.root` may list several roots; `--all` sweeps every one of them. The
first entry stays the default for creating new remotes — adding search roots
never moves where `dt remote init` puts things.

```bash
dt remote perms --all                                   # every configured root
dt remote perms --all --root /g/data/px14/dvc/analysis  # ad-hoc, repeatable
```

Store names are not unique across roots, so each is labelled with just enough of
its path to stay unambiguous — a bare name where that is unique, otherwise
`registries/chromium`, and more of the path if two roots share a basename.

A root that is missing or empty contributes nothing rather than aborting: with
several configured, one stale entry should not stop the rest being swept.

### Only the owner can repair

There is an asymmetry that shapes this whole command:

- **`unlink` is governed by write permission on the containing directory** — so
  a group member *can* delete another user's blob.
- **`chmod` is governed by ownership** — so a group member *cannot* repair
  another user's directory, even with group write.

Repair is therefore inherently per-owner. The report is grouped that way, and
its main output is the worklist telling each person what only they can run:

```
1603 directories deviate from policy, 344 prefix directories missing

Needs the owner to run it:
  hz7248         772 directories
  jr9959         357 directories  (you)
  sl6147         266 directories
  hk8797         196 directories

Each owner: dt remote perms --all --fix
```

Creating a *missing* prefix directory only needs write permission on its parent,
so `--fix` often succeeds at that part even on someone else's remote — and since
you created it, you own it and the mode sticks.

With `--fix`, the exit status is non-zero if any deviating directory could not
be repaired. A plain report always exits zero, however much it found.

### Layouts

Both DVC layouts are handled, detected from what is actually present rather
than from a declared version, so a store partway through a migration needs no
special-casing:

- **v3** — prefixes under `files/md5/`. Expected to hold all 256, since that is
  what `dt remote init` pre-creates.
- **v2** — prefixes at the store root, alongside `files/`, `runs/` and the
  verify ledger. Checked, but *not* expected to be complete: a v2 root
  legitimately holds only the prefixes in use. This also means v2 stores do not
  get the pre-creation protection, so their prefixes are created by DVC under
  whoever's umask.
- **Mixed** — both, checked together.

### Two kinds of gap

A store holding **none** of the 256 prefixes was simply never pre-created —
usually a remote nobody has pushed to yet. That's reported as
`prefix directories not pre-created`, distinct from a store that has **some**
and reports `N of 256 prefix directories missing`, which means it has drifted.
Only the second is a sign that something went wrong.

`--fix` treats both the same way: it creates the whole 256 for a never-pre-created
store, exactly as `dt remote init` would have. Creating directories is the one
repair that does not need ownership, so this generally succeeds — and it is
additive, never destructive.

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
| `--root PATH` | Remote root for `--all`; repeatable, replaces the configured list |
| `--min-age DAYS` | Only remove files older than this (default: 7, or the `clean.min_age_days` config) |
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
can clean it, regardless of who owns the files inside), and [the sticky
bit](#the-sticky-bit) (which restricts removal to the file's owner, the
directory's owner, or root). Since `dt remote init` sets sticky by default,
expect to be able to clear only your own abandoned uploads on a remote you do
not own.

Rather than guessing in advance — a pre-flight check would both race and misread
ACLs — the command attempts each removal and reports what failed, grouped by the
directory that blocked it, since that is what has to change:

```
  3 could not be removed (12.1G)
      /g/data/px14/dvc/datasets/foo/files/md5/3a  drwxr-sr-x  dir owner: ab1234
        3 file(s), 12.1G -- permission denied
      files owned by: cd5678 2, ef9012 1
```

With `--delete`, the exit status is non-zero if any removal failed. A plain
report always exits zero.

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
- [`dt cache init`](cache.md#dt-cache-init) - Set up local cache
- [`dt fetch`](fetch.md) - Fetch imports from local caches
- [`dt config`](config.md) - Configure remote settings
- [`dt remote archive`](archive.md) - Archive a remote to cold storage
- [`dt push`](push.md) - Push cache to remotes (re-uploads quarantined blobs)
- [`dt tmp`](tmp.md) - Manage temporary repository clones