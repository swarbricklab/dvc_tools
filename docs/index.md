# dt index

Manage the DVC per-repo SQLite index (`core.site_cache_dir`) and dt's
cache index.

## Overview

DVC keeps a per-repo SQLite index (object DB, link tracking, file-state
cache) under `core.site_cache_dir`. By default DVC places this under
`/var/tmp/dvc`, which is **local to each compute node**. On HPC clusters
that means every node has to rebuild the index the first time it touches
the workspace.

**Shared `site_cache_dir`** — point DVC's per-repo index at shared
storage (e.g. `/scratch/<project>/dvc/site/<user>/<repo>`) so every node
mounting the same workspace sees the same index live. This is the
recommended setup, and [`dt init`](init.md) / [`dt clone`](clone.md)
configure it for you **once `site_cache.root` is set** (without it
they silently leave DVC's default in place). See
[`dt index set`](#dt-index-set) to configure an existing repo.

Separately, `dt` keeps a small **cache index** (a different thing: a
per-cache list of which OIDs exist, used to make `dt fetch` skip already
cached objects). It is managed by [`dt index cache`](#dt-index-cache).

## Configuration

```bash
# Shared site_cache_dir root (recommended) — combined with the project name.
# dt init / dt clone read this to set core.site_cache_dir per repo.
dt config set site_cache.root /scratch/<project>/dvc/site

# Opt out of dt managing core.site_cache_dir entirely
dt config set site_cache.enabled false
```

### Shared filesystem permissions

Each user's index state is kept private, even when the root is shared.
Under a shared root, `dt` uses `{root}/{user}/{project}`:

- `{root}/{user}` and `{root}/{user}/{project}` are created owned by
  you, mode `0700`.
- An existing directory is only used if you own it and everything in it
  is yours and not writable by others; its permissions are then
  tightened. Otherwise it is refused (remove it — DVC rebuilds the
  index). Symlinks owned by other users are refused too.
- Parent directories up to the first system-owned directory must not be
  writable by others unless they have the sticky bit. If a system
  directory (e.g. an HPC project directory) lacks the sticky bit, `dt`
  prints a note suggesting you ask the system administrators to set it.
- `dt index migrate` only copies the old index if it was private to you;
  otherwise it starts empty and DVC rebuilds it.
- `dt index status` warns if the configured directory is not private.

`site_cache.root` and `site_cache.group_shared` are only read from user
or system dt config (or the command line), never from a repo's tracked
`.dt/config.yaml` or the workspace's `.dt/config.local.yaml`.

Prefer an admin or service account as the root's owner. A root that
`dt` has to create itself is made private (`0700`).

Set the root up once, as its owner — group-writable so users can create
their own subdirectory, with the **sticky bit**, and **no** default ACL
granting group write:

```bash
SC_ROOT=/scratch/<project>/dvc/site
mkdir -p $SC_ROOT
chgrp <project-group> $SC_ROOT
chmod 3775 $SC_ROOT     # setgid + sticky
setfacl -k $SC_ROOT     # remove any default ACL
```

A workspace deliberately used by several users can opt in to a
group-writable index (`2770`, never other-writable):

```bash
dt config set site_cache.group_shared true
```

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

### dt index cache

Manage the **cache index** — a lightweight SQLite database recording which
OIDs exist in the DVC cache, so `dt fetch` can skip already-cached objects
without a `stat()` per file (expensive on network filesystems).

It is **per user**: each user's index lives in their own cache directory,
`${XDG_CACHE_HOME:-~/.cache}/dt/oid-index/<hash>.sqlite` (keyed by the DVC
cache's path, owner-only), never inside the DVC cache. Each user's index
fills as they fetch
(self-healing), or immediately with `dt index cache rebuild`. An
unreadable or unexpected index file is ignored with a warning — the index
is advisory. Older versions kept a shared index under
`<cache_root>/.dt/`; it is no longer read and can be deleted.

This is a separate thing from the `site_cache_dir` index above.

```bash
dt index cache status          # Path, existence, entry count
dt index cache rebuild         # Clear and rescan the cache tree (prompts)
dt index cache rebuild --yes   # Skip the confirmation prompt
dt index cache rebuild -v      # Verbose
dt index cache rebuild -q      # Quiet
```

Rebuild after modifying the cache by hand or running `dvc gc`, since the
index is advisory and can go stale in either direction.

### dt index status

Show where DVC's site cache index lives, and warn if it is not private.

```bash
dt index status
# Output:
# Site cache (core.site_cache_dir):
#   Configured: /scratch/a56/jr9959/dvc/site/my-repo
#   Exists:     yes
```

## Removed: archive mirror

Earlier versions had `dt index push|pull`, which archived the
site_cache_dir databases to `index.mirror_root` (optionally on every
`dt status` via `index.auto_sync`). They have been removed: the shared
`site_cache_dir` already persists the index across nodes and `/tmp`
clearing. The `index.mirror_root`, `index.auto_sync`,
`index.lock_timeout` and `index.retry_interval` settings are ignored,
the `index-sync` hook check does nothing, and an existing archive
directory can be deleted.

## See also

- [dt init](init.md) / [dt clone](clone.md) — set `site_cache_dir` at project setup
- [dt pull](pull.md) — pull DVC-tracked files
- [dt fetch](fetch.md) — fetch imports into cache
- [Configuration Options](config_options.md) — `site_cache.*` keys
