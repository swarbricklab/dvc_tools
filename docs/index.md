# dt index

Manage the DVC per-repo SQLite index (`core.site_cache_dir`) and its
optional shared archive mirror.

## Overview

DVC keeps a per-repo SQLite index (object DB, link tracking, file-state
cache) under `core.site_cache_dir`. By default DVC places this under
`/var/tmp/dvc`, which is **local to each compute node**. On HPC clusters
that means every node has to rebuild the index the first time it touches
the workspace.

`dt` provides two complementary mechanisms:

1. **Shared `site_cache_dir`** — point DVC's per-repo index at shared
   storage (e.g. `/scratch/<project>/dvc/site/<repo>`) so every node
   mounting the same workspace sees the same index live. This is the
   recommended default and is configured automatically by
   [`dt init`](init.md) / [`dt clone`](clone.md). See
   [`dt index set`](#dt-index-set) to configure an existing repo.

2. **Archive mirror** — `dt index push|pull` snapshot the live index to
   a long-lived archive directory via the SQLite online-backup API and
   merge entries back in with `INSERT OR IGNORE`. Use this when you
   want a durable, off-node copy of the index (e.g. for backup, or to
   seed a new shared `site_cache_dir`). Archiving is **explicit only** —
   it is never triggered automatically by other `dt` commands.

### How the mirror transport works

Earlier versions of `dt` mirrored the index with rsync (and an
experimental fsspec/cloud path). That was racy under concurrent writes
because SQLite database files are not safe to copy byte-for-byte while
DVC has them open.

The current transport for each SQLite database file in the index is:

1. `sqlite3.Connection.backup()` produces a **consistent snapshot** of
   the source DB, even while DVC is writing to it.
2. If no destination DB exists, the snapshot is atomically renamed into
   place (`os.replace`).
3. Otherwise the snapshot is `ATTACH`-ed to the destination and each
   shared user table is merged with `INSERT OR IGNORE`. Tables that
   only exist in the source are copied across verbatim.
4. The snapshot file is then unlinked.

This means `dt index push` and `dt index pull` are safe to run while
DVC operations are in flight, do not require a global lock across
nodes, and never lose existing rows in the destination.

> **Cloud mirrors are no longer supported.** Use a shared local
> filesystem (Lustre, NFS, GPFS, etc.) for `index.mirror_root`. The
> `dvc-tools[gcs]` / `dvc-tools[s3]` extras have been removed.

## Configuration

```bash
# Shared site_cache_dir root (recommended) — combined with the project name.
# dt init / dt clone read this to set core.site_cache_dir per repo.
dt config set site_cache.root /scratch/<project>/dvc/site

# Opt out of dt managing core.site_cache_dir entirely
dt config set site_cache.enabled false

# Archive mirror root (only needed if you use dt index push|pull)
dt config set index.mirror_root /g/data/<project>/dvc/index-archive

# Lock tuning for the archive mirror
dt config set index.lock_timeout 120      # seconds (default: 120)
dt config set index.retry_interval 5      # seconds (default: 5)
```

### Lustre / parallel filesystems

SQLite's WAL journal mode is incompatible with most parallel file
systems. `dt` forces `journal_mode=delete` on every DB it opens (live
and mirror), so a shared `site_cache_dir` on Lustre / GPFS / NFS works
correctly.

### Shared filesystem permissions

A shared `site_cache_dir` is read **and written** by every user that
touches the workspace, so the root directory needs to be group-writable
with the setgid bit (so per-project subdirs inherit the group) and
ideally a default ACL (so files DVC creates inside are also
group-writable regardless of each user's umask).

Set it up once, as the directory owner:

```bash
SC_ROOT=/scratch/<project>/dvc/site
mkdir -p $SC_ROOT
chgrp <project-group> $SC_ROOT
chmod 2775 $SC_ROOT
setfacl    -m u::rwx,g::rwx,o::rx $SC_ROOT
setfacl -d -m u::rwx,g::rwx,o::rx $SC_ROOT
```

Every user that runs `dt`/`dvc` against the shared cache should also
have `umask 0002` in effect so newly created SQLite files come out
group-writable.

`dt` creates per-repo subdirectories with mode `2775` and warns at
`dt init` / `dt clone` / `dt index set` / `dt index migrate` time if
the root is missing the setgid bit, group-write, or a default ACL.

## Commands

### dt index set

Configure `core.site_cache_dir` for the current repo (writes
`.dvc/config.local`). Does **not** copy any existing index — use
`dt index migrate` for that.

```bash
dt index set --root /scratch/<project>/dvc/site       # root + project name
dt index set --path /scratch/<project>/dvc/site/myrepo
dt index set --root /scratch/<project>/dvc/site --name otherrepo
```

### dt index migrate

Move `core.site_cache_dir` to a new location, copying the current
contents first, then switching DVC over. The old location is left
in place (delete it manually if you wish).

```bash
dt index migrate --root /scratch/<project>/dvc/site
dt index migrate --path /scratch/<project>/dvc/site/myrepo
```

### dt index pull

Snapshot + merge each SQLite DB from the archive mirror into the live
`site_cache_dir`. New DBs in the mirror are renamed into place; existing
DBs are merged with `INSERT OR IGNORE`.

```bash
dt index pull           # Pull latest archive into the live index
dt index pull -v        # Verbose: show each DB merged
dt index pull -q        # Quiet (no output)
dt index pull --dry     # Show what would be pulled
```

### dt index push

The mirror counterpart of `dt index pull`: snapshot + merge from the
live `site_cache_dir` into the archive mirror.

```bash
dt index push           # Push current index to the archive
dt index push -v        # Verbose: show each DB merged
dt index push -q        # Quiet
dt index push --dry     # Show what would be pushed
```

### dt index build

Build the ODB index by walking the cache and trusting filenames, avoiding expensive hash computation.

```bash
dt index build              # Build from current repo's cache
dt index build -v           # Show each file being indexed
dt index build --dry        # Preview what would be indexed
dt index build --cache /path/to/cache  # Use specific cache
```

**Why this is faster:**

DVC's default index-building process reads and hashes every file in the cache, which can take hours for large datasets. `dt index build` bypasses this by trusting that cache files are named correctly (files are stored as `{hash[0:2]}/{hash[2:]}`).

**When to use:**

- After symlinking a large remote cache to a local location
- When setting up a new compute node with access to a shared cache
- When the index is missing or corrupted
- When DVC's automatic index building is taking too long

**Validation:**

Use `dt cache validate` separately if you need to verify checksum integrity. The build command assumes cache filenames are correct.

### dt index status

Show site_cache_dir and mirror configuration.

```bash
dt index status
# Output:
# Index configuration:
#   site_cache_dir: /scratch/a56/jr9959/dvc/site/my-repo
#   Local index:    /scratch/a56/jr9959/dvc/site/my-repo/repo/<sha>/...
#   Mirror:         /g/data/a56/dvc/index-archive/<sha>/...
#
# Status:
#   Local exists:   yes
#   Mirror exists:  yes
```

## Locking

`dt index push|pull` use file-based locks to serialise archive
operations across users:

- `local.lock` — held during pull while writing into `site_cache_dir`.
- `mirror.lock` — held during push while writing into the archive.

If a lock is held, commands wait and retry with exponential backoff up
to `index.lock_timeout`. If a process was interrupted, locks may be
left behind; `dt index status` shows lock owner and age. Remove a
stale lock by deleting the lock file manually:

```bash
rm /g/data/<project>/dvc/index-archive/<hash>/mirror.lock
```

For the **live** `site_cache_dir` we rely on SQLite's own
concurrency: snapshots are taken via the online-backup API, so DVC and
`dt` may both be reading or writing while the snapshot proceeds.

## Failure handling

`dt index push|pull` treat per-DB failures as warnings — a single
corrupt or in-use DB does not abort the whole sync. Commands also work
fine when the mirror is unreachable; you just get a warning and
nothing is archived/pulled.

## Migration from the old transport

If you were previously using `index.mirror_root` on cloud storage
(`gs://` or `s3://`), or relying on the implicit index sync that ran
inside `dt add`/`dt fetch`/`dt pull`:

- Move your mirror to a shared local filesystem and update
  `index.mirror_root`.
- Run `dt index push|pull` explicitly when you want to archive or
  restore. There is no longer a `--no-index-sync` flag because no
  command syncs the index implicitly.
- Consider switching to a shared `site_cache_dir` (`dt index set`) so
  every node sees the same live index and the archive becomes a
  pure backup, not the primary sharing mechanism.

## See also

- [dt init](init.md) / [dt clone](clone.md) — set `site_cache_dir` at project setup
- [dt pull](pull.md) — pull DVC-tracked files
- [dt fetch](fetch.md) — fetch imports into cache
- [Configuration Options](config_options.md) — `site_cache.*` and `index.*` keys
