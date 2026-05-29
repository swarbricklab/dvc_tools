# dt remote archive

Archive DVC remotes to cold storage (e.g. NCI MDSS tape), verify them,
restore from them, and prune the on-disk remote once an archive is
verified.

## When to use it

A DVC remote on `/g/data` or similar that is no longer being actively
touched is a candidate for archival. `dt remote archive` tars its
contents in parallel, ships each inner tar to a pluggable backend (MDSS
by default), and writes a manifest under `.dvc/archives/` so verify and
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

Convenience: stage + deposit inline. Suitable for small archives that
finish in one walltime. For multi-TB archives, prefer
`stage` + `deposit` as two separate jobs.

**What is `NAME`?** An identifier you choose for *this particular archive
instance* — not the DVC remote name, not the project name. It becomes
the manifest filename (`.dvc/archives/<NAME>.yaml`), part of the
default backend folder (`<archive.backend_root>/<remote-dir>/<NAME>/`),
and the handle you pass to `verify`, `restore`, and `prune` later.

If `NAME` is omitted, it defaults to `<remote-dir-name>-<YYYY-MM-DD>`.

| Option | Default | What it does |
| --- | --- | --- |
| `--source` | project remote | Override the DVC remote path to archive. |
| `--backend` | `mdss` | Backend name (`mdss`, `local`). |
| `--backend-dir` | `<archive.backend_root>/<remote-name>/<NAME>/` | Folder path on the backend. |
| `--staging-dir` | `archive.staging_dir` config | Where the inner tarballs land. |
| `--jobs` | `archive.stage_jobs` or `min(PBS_NCPUS, 8)` | Parallel inner-tar workers (stage phase). |
| `--deposit-jobs` | `archive.deposit_jobs` (default `4`) | Parallel upload workers (deposit phase, capped for MDSS politeness). |
| `--compress` | `archive.compress` or `zstd` | `none`, `gzip`, or `zstd`. |
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

Read `.dvc/archives/<NAME>.yaml`, upload every inner tar in the
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

Print every archive recorded under `.dvc/archives/`. Does not contact
the backend.

### `dt remote archive verify <name>`

Default ("quick"):

- Check the manifest sidecar exists at
  `<backend-dir>/<name>.manifest.yaml` (the completion sentinel).
- For each inner tar in the manifest, check it exists at
  `<backend-dir>/<filename>` and that its size matches.

`--deep` additionally downloads every inner tar to a temp file and
recomputes its sha256. Expensive on tape.

### `dt remote archive restore <name> --to <path>`

Modes:

- `--object <md5>` — fetch the matching inner tar to a temp dir,
  extract a single entry.
- `--prefix <hex>` — fetch one inner tar, extract everything in it.
- (neither) — full restore. Fetches every inner tar in turn and
  extracts each into `--to`.

### `dt remote archive prune <name>`

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

`.dvc/archives/<name>.yaml` is a small YAML document (schema version 2)
recording:

- `backend_dir` — folder path on the backend.
- `layout: folder-per-prefix` — one inner tar per md5 prefix.
- `contents.inner_tars` — one row per inner tar with filename, size,
  sha256 and object count.
- Provenance: git ref, dt version, who created it, when.
- The list of extras present at archive time (informational).

Commit it alongside the rest of the project so `list`, `verify`, and
`restore` work without backend access. A copy of the same manifest is
also uploaded to the backend as `<NAME>.manifest.yaml` — both the
completion sentinel and a belt-and-braces restore key if the project
repo is ever lost.

## Configuration

| Key | Default | What it sets |
| --- | --- | --- |
| `archive.staging_dir` | — (required) | Local directory for inner tarballs. |
| `archive.backend_root` | `dt-archive` | Base path on the backend. |
| `archive.stage_jobs` | `min(PBS_NCPUS or nproc, 8)` | Parallel workers for `stage`. |
| `archive.deposit_jobs` | `4` | Parallel workers for `deposit`. MDSS-politeness ceiling. |
| `archive.compress` | `zstd` | Default compression for inner tars. |

Set via `dt config set archive.X Y` at any scope.

## Related commands

- [`dt remote init`](remote.md) — set up a DVC remote that you might
  later archive.
- [`dt config`](config.md) — set `archive.*` keys.
- [`dt du`](du.md) — figure out how big a remote is before archival.
