# dt remote archive

Archive DVC remotes to cold storage (e.g. NCI MDSS tape), verify them,
restore from them, and prune the on-disk remote once an archive is
verified.

## When to use it

A DVC remote on `/g/data` or similar that is no longer being actively
touched is a candidate for archival. `dt remote archive` tars its
contents in parallel, ships each inner tar to a pluggable backend (MDSS
by default), and writes a manifest under `.dt/archives/` so verify and
restore can work without contacting the backend.

## Quick start

```bash
# One-time: tell dt where to stage tarballs.
dt config set archive.staging_dir /scratch/${PROJECT}/${USER}/dt-archive

# Single-node archive (small archives that finish in one walltime).
dt remote archive create neochemo-2026-05

# Split workflow (recommended for multi-TB archives).
dt remote archive stage   neochemo-2026-05   # compute node (parallel CPUs)
dt remote archive deposit neochemo-2026-05   # data mover (mdss access)

# Verify it.
dt remote archive verify neochemo-2026-05

# Once you're confident, delete the local copy.
dt remote archive prune  neochemo-2026-05
```

## Where to run each subcommand

Most subcommands need MDSS access, which on NCI Gadi means running
inside a `copyq` PBS job with `massdata/<proj>` mounted. A couple are
purely local. Quick reference:

| Subcommand | Queue | `-l storage=` includes | Notes |
| --- | --- | --- | --- |
| `create` | `copyq` ⚠️ | `gdata/<proj>+scratch/<proj>+massdata/<proj>` (+ source-remote flag) | Combines stage + deposit; OK for small archives, but limits stage to data-mover CPUs |
| `stage` | any compute (`normal`, `normalbw`, …) | `gdata/<proj>+scratch/<proj>` (+ source-remote flag) | No MDSS; pure tar/IO |
| `stage --via-qxub` | the orchestrator can run anywhere with `qxub`; per-prefix workers go to `archive.qxub_queue` (default `normal`) | per-worker: `gdata/<proj>+scratch/<proj>` (+ source-remote flag) | Each prefix is its own qsub job |
| `deposit` | `copyq` | `gdata/<proj>+scratch/<proj>+massdata/<proj>` | Only data movers can talk to MDSS |
| `verify` | `copyq` | `gdata/<proj>+massdata/<proj>` (+ `scratch/<proj>` for `--deep`) | Sidecar + per-file `stat`; `--deep` downloads every inner tar |
| `restore` | `copyq` | `gdata/<proj>+massdata/<proj>` (+ destination flag for `--to`) | Reads from MDSS |
| `prune` | `copyq` | `gdata/<proj>+massdata/<proj>` (+ source-remote flag) | Re-verifies first (so needs MDSS), then deletes the on-disk source |
| `destroy` | `copyq` | `gdata/<proj>+massdata/<proj>` | Deletes inner tars + sidecar from MDSS |
| `list` | login node | — | Reads local `.dt/archives/` (and `.dvc/archives/` for legacy) |
| `registry list` / `registry sync` | login node | path holding the register dir | Reads YAML files |

Notes:

- `gdata/<proj>` is in nearly every row because the conda env that
  provides `dt` typically lives under `/g/data/<proj>/conda/`. If your
  install lives elsewhere, swap accordingly.
- "source-remote flag" means the storage flag for wherever the DVC
  remote sits (usually `gdata/<proj>`).
- The orchestrator side of `stage --via-qxub` only submits + monitors
  jobs, so it can run on a login node — `qxub` does the rest.

## Source DVC layouts

`dt remote archive` auto-detects which DVC layout your remote uses and
records it in the manifest under `source_layout`:

| Layout | Recognised by | Manifest keys |
| --- | --- | --- |
| `dvc-v3` | `<remote>/files/md5/<XX>/<hash>` | bare `00` … `ff` |
| `dvc-v2` | `<remote>/<XX>/<hash>` (no `files/md5/` wrapper) | bare `00` … `ff` |
| `dvc-mixed` | Both v2 and v3 trees co-exist in the same remote | `v3-XX` and `v2-XX` |

Mixed remotes happen when a v2 client and a v3 client both pushed
blobs to the same DVC remote at different times. `archive` keeps the
two halves in separate inner tarballs (`v3-00.tar`, `v2-00.tar`, …)
and restores each tree back to its original on-disk location.

Override the detection with `--source-layout dvc-v2 / dvc-v3 /
dvc-mixed`; rarely needed in practice.

## Archive layout

For an archive named `<NAME>`, the backend is a **folder** holding one
file per md5 prefix plus a manifest sidecar uploaded last as a
completion sentinel:

```
<backend>:<backend-dir>/
  ├── 00.tar.zst         ← one inner tar per md5 prefix
  ├── 01.tar.zst
  ├── ...
  ├── ff.tar.zst
  └── <NAME>.manifest.yaml   ← completion sentinel
```

This replaces the previous "one giant outer tar" design. Per-file
uploads can run in parallel, partial uploads survive walltime
boundaries (deposit is resumable), MDSS doesn't have to hold a
multi-TB monolith, and partial restore can fetch a single inner tar
directly instead of streaming the whole archive past it.

## Subcommands

### `dt remote archive create [NAME]`

> **Runs on:** `copyq` (data mover) with `gdata/<proj>+scratch/<proj>+massdata/<proj>` and the source-remote storage flag.
> Combines `stage` + `deposit` in one process — fine for archives that fit in a single walltime, but stage workers are limited to data-mover CPUs.

Convenience: stage + deposit inline. Suitable for small archives that
finish in one walltime. For multi-TB archives, prefer
`stage` + `deposit` as two separate jobs.

**What is `NAME`?** An identifier you choose for *this particular archive
instance* — not the DVC remote name, not the project name. It becomes
the manifest filename (`.dt/archives/<NAME>.yaml`), part of the
default backend folder (`<archive.backend_root>/<remote-dir>/<NAME>/`),
and the handle you pass to `verify`, `restore`, and `prune` later.

If `NAME` is omitted, it defaults to `<remote-dir-name>-<YYYY-MM-DD>`.

| Option | Default | What it does |
| --- | --- | --- |
| `--source` | first local-accessible remote in `.dvc/config` (same as `dt fetch`); falls back to `remote.root/<project>` | The DVC remote path to archive. |
| `--backend` | `mdss` | Backend name (`mdss`, `local`). |
| `--backend-dir` | `<archive.backend_root>/<remote-name>/<NAME>/` | Folder path on the backend. |
| `--staging-dir` | `archive.staging_dir` config | Where the inner tarballs land. |
| `--jobs` | `archive.stage_jobs` or `min(PBS_NCPUS, 8)` | Parallel inner-tar workers (stage phase). |
| `--deposit-jobs` | `archive.deposit_jobs` (default `4`) | Parallel upload workers (deposit phase, capped for MDSS politeness). |
| `--compress` | `archive.compress` or `none` | `none`, `gzip`, or `zstd`. |
| `--source-layout` | `auto` | `auto` / `dvc-v2` / `dvc-v3` / `dvc-mixed`. Default inspects the remote. |
| `--url` | `git remote get-url origin` of the project | Git URL to record in the manifest. |
| `--dry-run` | — | Plan and report sizes without uploading. |
| `--force` | — | Overwrite existing manifest/staging, ignore low-disk warnings. |
| `--resume` | — | Reuse staging, skipping prefixes/files with valid sentinels. |
| `--keep-staging` | — | Keep the staging directory after upload. |

`create` warns about — but does **not** archive — files in the source
remote that live outside `files/md5/` (e.g. a stray `config` or
`README.txt`). Those are recorded in the manifest under
`extras_at_archive_time` for forensics. To prune the on-disk remote
later, the extras must be resolved (deleted, moved, or accepted with
`--force`).

### `dt remote archive stage [NAME]`

> **Runs on:** any compute queue (`normal`, `normalbw`, …) with `gdata/<proj>+scratch/<proj>` and the source-remote storage flag.
> No MDSS involvement — workers just `tar` files. With `--via-qxub`, the orchestrator can run anywhere `qxub` is available (login node fine); each per-prefix worker job goes to `archive.qxub_queue` (default `normal`).

Build the inner tarballs in `<staging-dir>/<NAME>/` and write the
manifest locally. No backend interaction happens. Run this on a
compute node with many CPUs.

Each successfully tarred prefix writes a `<prefix>.tar[.ext].done.json`
sentinel alongside its tarball. A re-run with `--resume` skips
prefixes whose sentinels are still valid (size matches), so walltime
hits don't lose Phase 1 work.

For sources too large to tar within a single node's walltime,
`--via-qxub` dispatches one qxub job per md5 prefix instead of
running inline:

```bash
dt remote archive stage neochemo-2026-05 --via-qxub
```

Each worker is a single-CPU job invoking `dt remote archive
_build-prefix <NAME> <PREFIX>` on a compute node; the orchestrator
waits via `qxub monitor`, then assembles the manifest from the
sentinels every worker leaves in staging. The 256 prefixes give you
natural multi-node parallelism, bounded only by your PBS queue's
concurrency limit. qxub config (queue, walltime, mem) is read from
`qxub.*` keys — see [config.md](config.md).

### `dt remote archive deposit <NAME>`

> **Runs on:** `copyq` (data mover) with `gdata/<proj>+scratch/<proj>+massdata/<proj>`.
> The only subcommand that *must* be on `copyq` for write access to MDSS.

Read `.dt/archives/<NAME>.yaml`, upload every inner tar in the
manifest to `<backend-dir>/<filename>`, then upload a copy of the
manifest to `<backend-dir>/<NAME>.manifest.yaml` last (the
completion sentinel). Run this on a data mover.

Each successful upload writes a `<filename>.deposited.json` sentinel in
staging. A re-run with `--resume` skips files whose deposit sentinels
are present, so a killed `deposit` only re-uploads the in-flight ones.

| Option | Default | What it does |
| --- | --- | --- |
| `--staging-dir` | `archive.staging_dir` config | Where the staged inner tarballs live. |
| `--jobs` | `archive.deposit_jobs` (default `4`) | Parallel upload workers. |
| `--dry-run` | — | Report what would be uploaded without contacting the backend. |
| `--resume` | — | Skip files whose `.deposited.json` sentinels are valid. |
| `--keep-staging` | — | Keep the staging directory after upload. |

### `dt remote archive list`

> **Runs on:** login node. No queue, no storage flags. Reads `.dt/archives/` (falling back to legacy `.dvc/archives/`).

Print every archive recorded under `.dt/archives/`. Does not contact
the backend.

### `dt remote archive verify <name>`

> **Runs on:** `copyq` with `gdata/<proj>+massdata/<proj>`. Add `scratch/<proj>` if you use `--deep` so it can stage downloaded tars there.

Default ("quick"):

- Check the manifest sidecar exists at
  `<backend-dir>/<name>.manifest.yaml` (the completion sentinel).
- For each inner tar in the manifest, check it exists at
  `<backend-dir>/<filename>` and that its size matches.

`--deep` additionally downloads every inner tar to a temp file and
recomputes its sha256. Expensive on tape.

### `dt remote archive restore <name> [--to <path>]`

> **Runs on:** `copyq` with `gdata/<proj>+massdata/<proj>` and the storage flag for wherever the destination lives (the source remote by default, or `--to` if given).

`--to` is **optional**. If omitted, restore puts the data back at
`manifest.source_remote` — i.e. wherever it was archived from. The
typical post-prune workflow is just:

```bash
dt remote archive restore <name>
```

…and the source remote pops back into existence. A full restore also
clears the `ARCHIVED.yaml` signpost since the data is no longer
archived. Partial restores (`--prefix` / `--object`) leave the
signpost in place because most of the data is still on the backend.

Modes:

- `--object <md5>` — fetch the matching inner tar to a temp dir,
  extract a single entry.
- `--prefix <hex>` — fetch one inner tar, extract everything in it.
- (neither) — full restore. Fetches every inner tar in turn and
  extracts each into `--to`.

**Mixed-layout convenience.** When the archive's `source_layout` is
`dvc-mixed`, `--prefix XX` (bare hex) restores **both** halves
(`v3-XX` and `v2-XX` if both exist) — you don't need to specify the
namespace. Use the namespaced form (`--prefix v3-XX` or `--prefix
v2-XX`) to restore only one half explicitly. Likewise `--object
<hash>` tries the v3 candidate first, then the v2 candidate, and
returns whichever inner tar actually contains the object. Pure
layouts (v2 or v3 only) are unaffected — `--prefix XX` means just
that one prefix.

### `dt remote archive registry list`

> **Runs on:** login node. No queue. Reads `.yaml` files from `archive.registry_path`.

List every archive recorded in the central register
(``archive.registry_path``). Each row shows project, archive name,
backend, size, creation timestamp, and lifecycle status
(verified / pruned).

If the register is unconfigured, this prints a hint and exits non-zero.

### `dt remote archive registry sync --root <PATH> [--root <PATH>...]`

> **Runs on:** login node. No queue, no MDSS. Walks each `<root>/.dt/archives/` (and legacy `<root>/.dvc/archives/`) and writes entries to the register dir.

Rebuild register entries from the manifests under each listed root.
Useful when bootstrapping the register across an existing fleet of
projects, or after manual edits / deletes in the register dir.

### `dt remote archive destroy <name>`

> **Runs on:** `copyq` with `gdata/<proj>+massdata/<proj>`. No source-remote flag needed — `destroy` never touches the source.

Delete the **archive copy** from the backend. Does NOT touch the source
remote — that's what `prune` is for.

Use this when:
- You archived the wrong directory.
- You archived an empty / meaningless tree (e.g. a missing `files/md5/`).
- You want to retry a deposit cleanly from scratch.

Deletes happen sidecar-first, so an interrupted destroy leaves the
archive marked incomplete on the backend (no sidecar present) rather
than falsely complete with missing inner tars.

| Option | What it does |
| --- | --- |
| `--yes` | Skip the interactive confirmation prompt. |
| `--keep-manifest` | Wipe the backend copy but keep `.dt/archives/<name>.yaml` and the registry entry. Use this when you want to retry deposit (e.g. after destroying a partial upload). |

### `dt remote archive prune <name>`

> **Runs on:** `copyq` with `gdata/<proj>+massdata/<proj>` and the source-remote storage flag.
> Re-verifies the archive on MDSS before deleting anything; needs MDSS read access and write access to wherever the source DVC remote lives.

After successful verify, `prune` drops an
[`ARCHIVED.yaml` signpost](#signposts) at the root of the source
remote *before* deleting the blob data, so anyone arriving later via
`ls` / `dt fetch` / `dt pull` / `dt status` / `dt doctor` sees a clear
explanation of where the data went and how to bring it back.

Refuses to run unless:

1. The archive verifies (sidecar present + every inner tar present at
   expected size). `--force` never bypasses this.
2. There are no files in the source remote outside `files/md5/`
   (extras). `--force` skips this check.

When both conditions are met, deletes `<source-remote>/files/md5/` and
reports the bytes freed. `--yes` skips the interactive confirmation.

## Parallelism

Inner-tar creation (`stage`) is process parallelism on a single
node — one `tar` subprocess per prefix. Default is
`archive.stage_jobs` or `min(PBS_NCPUS or nproc, 8)`. Past 8, Lustre
OST contention on `/g/data` erases the gains.

Upload (`deposit`) is thread parallelism — multiple concurrent `mdss
put` calls. Default is `archive.deposit_jobs=4`. The ceiling here is
MDSS politeness, not data-mover cores; very wide fan-out can stall the
tape robot.

Stage and deposit run on different node types:

| Phase | Where | Why |
| --- | --- | --- |
| `stage`   | normal compute (many CPUs) | Pure tar/IO — wants cores. |
| `deposit` | data mover (`copyq`)        | Only data movers can talk to MDSS. |

| Resource | Per `--jobs` | Notes |
| --- | --- | --- |
| CPUs | ~1 | Tar is mostly IO-bound; +1 if `--compress zstd`. |
| Memory | ~0.5 GB | Tar's read buffers. |
| Staging disk | ~1× remote size total | Inner tarballs land here before upload. |

### Sample PBS submissions

Stage on a normal compute node:

```bash
#!/bin/bash
#PBS -P a56
#PBS -q normal
#PBS -l ncpus=8
#PBS -l mem=32GB
#PBS -l jobfs=400GB
#PBS -l walltime=24:00:00
#PBS -l storage=gdata/a56+scratch/a56

cd /path/to/repo
dt remote archive stage neochemo-2026-05 \
    --staging-dir /scratch/a56/${USER}/dt-archive \
    -v
```

Deposit on a data mover:

```bash
#!/bin/bash
#PBS -P a56
#PBS -q copyq
#PBS -l ncpus=1
#PBS -l mem=4GB
#PBS -l walltime=10:00:00
#PBS -l storage=gdata/a56+scratch/a56+massdata/a56

cd /path/to/repo
dt remote archive deposit neochemo-2026-05 \
    --staging-dir /scratch/a56/${USER}/dt-archive \
    -v
```

If `deposit` runs out of walltime, rerun with `--resume` — only the
in-flight uploads have to repeat.

MDSS-side jobs (`verify`, `restore`, `destroy`, `prune`) all share the
same general shape — `copyq`, `gdata/<proj>+massdata/<proj>`, optional
extra storage flag for the data they touch:

```bash
#!/bin/bash
#PBS -P a56
#PBS -q copyq
#PBS -l ncpus=1
#PBS -l mem=4GB
#PBS -l walltime=01:00:00
#PBS -l storage=gdata/a56+massdata/a56   # + scratch/a56 for restore --to / verify --deep
                                          # + gdata/<other> for the source remote (prune only)

cd /path/to/repo
dt remote archive verify  neochemo-2026-05
# or restore: dt remote archive restore neochemo-2026-05 --to /scratch/a56/$USER/restored
# or destroy: dt remote archive destroy neochemo-2026-05 --yes
# or prune:   dt remote archive prune   neochemo-2026-05 --yes
```

Or wrap any of them in a one-liner via `qxub`:

```bash
qxub exec --env dt --queue copyq --time 01:00:00 --mem 4GB \
    --storage 'gdata/a56+massdata/a56' \
    -N dt-verify-neochemo \
    -- dt remote archive verify neochemo-2026-05
```

## Backends

The first PR ships:

- `mdss` — NCI tape, via the `mdss` CLI. Only works on data-mover
  nodes (`gadi-dm.nci.org.au`, `copyq`).
- `local` — copies files to a local directory. Intended for tests
  and local dev.

Adding a backend means subclassing the `ArchiveBackend` protocol in
[dt/archive/backends.py](../dt/archive/backends.py) and calling
`register_backend('<name>', <Cls>)`. The protocol is small: `put_file`,
`get_file`, `exists`, `stat`, `list_dir`.

## Manifest

`.dt/archives/<name>.yaml` is a small YAML document (schema version 2)
recording:

- `backend_dir` — folder path on the backend.
- `layout: folder-per-prefix` — one inner tar per md5 prefix.
- `contents.inner_tars` — one row per inner tar with filename, size,
  sha256 and object count.
- Provenance: git ref + git url, dt version, who created it, when.
- The list of extras present at archive time (informational).

Commit it alongside the rest of the project so `list`, `verify`, and
`restore` work without backend access. A copy of the same manifest is
also uploaded to the backend as `<NAME>.manifest.yaml` — both the
completion sentinel and a belt-and-braces restore key if the project
repo is ever lost.

## Signposts

After `prune` deletes the on-disk blob data, it leaves an
`ARCHIVED.yaml` file at the root of the source remote directory. This
is the "signpost" — a small subset of the manifest that:

- **Explains to humans** (top-of-file comment block) what happened
  and how to restore.
- **Lets dt commands detect** an archived remote by the
  `dt_archive_signpost: 1` marker key and route the user to
  `dt remote archive restore` rather than failing with a "no such
  blob" error.

```yaml
# This DVC remote was archived to cold storage by `dt remote archive prune`.
# ... full comment block ...
dt_archive_signpost: 1
archive_name: neochemo-2026-05
backend: mdss
backend_dir: dt-archive/neochemo/neochemo-2026-05/
source_layout: dvc-v3
source_remote: /g/data/<proj>/dvc/neochemo
git_url: git@github.com:<org>/<repo>.git
git_ref: <sha>
manifest_in_repo: .dt/archives/neochemo-2026-05.yaml
pruned_at: 2026-05-30T12:34:56+00:00
pruned_by: <user>
```

Commands that notice the signpost:

| Command | Behaviour when signpost present |
| --- | --- |
| `dt fetch` | **Refuses** with a friendly error explaining how to restore. |
| `dt pull` | **Refuses** with the same message — keeps `dvc pull` from timing out against an empty remote. |
| `dt status` | Prints the signpost message before delegating to `dvc status`, but doesn't refuse. |
| `dt doctor` | Reports an `archived_remotes` check failure with a suggested `dt remote archive restore` command. |
| `dt remote archive restore` | **Removes** the signpost on full restore (since the data is back). Partial restores leave it. |

The signpost is the *only* on-disk artefact of an archived remote
besides the in-repo manifest at `.dt/archives/`. Removing it manually
(or via full restore) re-enables `dvc fetch` / `dvc pull` against
that remote.

## Central register

For team-shared visibility into "what archives exist across all our
projects", point a shared directory at the register:

```bash
dt config set archive.registry_path /g/data/<proj>/dt-archives/registry
```

After that, every successful `create` / `deposit` writes a YAML entry
to that directory; `verify` and `prune` update the entry's lifecycle
status. Per-project manifests under `.dt/archives/` remain the
canonical source of truth — the register is a derived index.

```bash
dt remote archive registry list                  # browse all archives
dt remote archive registry sync --root /scratch/<proj>/myproject \
                                --root /scratch/<proj>/other     # bootstrap
```

If `archive.registry_path` is unset, register hooks are silent no-ops.

## Configuration

| Key | Default | What it sets |
| --- | --- | --- |
| `archive.staging_dir` | — (required) | Local directory for inner tarballs. |
| `archive.backend_root` | `dt-archive` | Base path on the backend. |
| `archive.stage_jobs` | `min(PBS_NCPUS or nproc, 8)` | Parallel workers for `stage`. |
| `archive.deposit_jobs` | `4` | Parallel workers for `deposit`. MDSS-politeness ceiling. |
| `archive.compress` | `none` | Default compression for inner tars. DVC blobs are usually already-compressed; gzip saves ~10% at hours of CPU cost. Set to `zstd` for genuinely compressible data. |
| `archive.registry_path` | — (off) | Central register directory (team-shared OK). |
| `archive.qxub_queue` | `normal` (fallback `qxub.queue`) | PBS queue for `--via-qxub` stage workers. *Not* `copyq` — workers need CPU. |
| `archive.qxub_walltime` | `04:00:00` | Per-prefix worker walltime. One inner tar fits well inside this. |
| `archive.qxub_mem` | `4GB` | Per-prefix worker memory. |
| `archive.qxub_env` | `dt` | Conda env name for `--via-qxub` workers. |

Set via `dt config set archive.X Y` at any scope.

The `archive.qxub_*` keys fall back to the generic `qxub.*` keys if
unset, so an existing qxub config still works — but the *defaults*
differ: the generic default is `copyq` (right for `dt push` / `mdss`
work), the archive-specific default is `normal` (right for tar work).

## Related commands

- [`dt remote init`](remote.md) — set up a DVC remote that you might
  later archive.
- [`dt config`](config.md) — set `archive.*` keys.
- [`dt du`](du.md) — figure out how big a remote is before archival.
