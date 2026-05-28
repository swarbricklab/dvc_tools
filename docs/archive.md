# dt remote archive

Archive DVC remotes to cold storage (e.g. NCI MDSS tape), verify them,
restore from them, and prune the on-disk remote once an archive is
verified.

## When to use it

A DVC remote on `/g/data` or similar that is no longer being actively
touched is a candidate for archival. `dt remote archive` tars its
contents in parallel, ships the result to a pluggable backend (MDSS by
default), and writes a manifest under `.dvc/archives/` so verify and
restore can work without contacting the backend.

## Quick start

```bash
# One-time: tell dt where to stage tarballs.
dt config set archive.staging_dir /scratch/${PROJECT}/${USER}/dt-archive

# Create an archive on MDSS.
dt remote archive create neochemo-2026-05

# Verify it.
dt remote archive verify neochemo-2026-05

# Once you're confident, delete the local copy.
dt remote archive prune  neochemo-2026-05
```

## Archive layout

For an archive named `<NAME>`:

```
<backend>:<backend-path>/<NAME>.tar          ← outer tar (always uncompressed)
  ├── 00.tar       ← one inner tar per md5 prefix
  ├── 01.tar       ← inner tars compressed only if --compress is set
  ├── ...
  └── ff.tar
```

The inner tars are built **in parallel** by independent worker
processes (one per prefix). The outer tar is a thin wrapper so a tape
operation can move "the whole archive" as one object.

Selective restore is the inverse: stream the outer tar from the
backend, ask `tar` for one inner, then ask `tar` again for one entry
inside that inner. The cost on tape is one stream-through.

## Subcommands

### `dt remote archive create [NAME]`

Build the parallel inner tarballs, wrap them into one outer tar, ship
it to the backend, and write `.dvc/archives/<NAME>.yaml`.

**What is `NAME`?** An identifier you choose for *this particular archive
instance* — not the DVC remote name, not the project name. It becomes
the manifest filename (`.dvc/archives/<NAME>.yaml`), part of the
default backend object path
(`dt-archive/<remote-dir>/<NAME>.tar`), and the handle you pass to
`verify`, `restore`, and `prune` later. If you re-archive the same
remote three times over a year, you'd have three distinct names —
typically date-stamped, e.g. `neochemo-2026-05`, `neochemo-2026-11`,
`neochemo-2027-03` — and three manifests sitting beside each other in
git history.

If `NAME` is omitted, it defaults to `<remote-dir-name>-<YYYY-MM-DD>`
so re-running on the same remote on different days produces
distinguishable archives without you having to think.

| Option | Default | What it does |
| --- | --- | --- |
| `--source` | project remote | Override the DVC remote path to archive. |
| `--backend` | `mdss` | Backend name (`mdss`, `local`). |
| `--backend-path` | `dt-archive/<remote-name>/<NAME>.tar` | Object path on the backend. |
| `--staging-dir` | `archive.staging_dir` config | Where the inner tarballs land. |
| `--jobs` | `min(PBS_NCPUS or nproc, 8)` | Parallel inner-tar workers. |
| `--compress` | `none` | `none`, `gzip`, or `zstd` for inner tars. |
| `--dry-run` | — | Plan and report sizes without uploading. |
| `--force` | — | Overwrite existing manifest/staging, ignore low-disk warnings. |
| `--keep-staging` | — | Keep the staging directory after upload. |

`create` warns about — but does **not** archive — files in the source
remote that live outside `files/md5/` (e.g. a stray `config` or
`README.txt`). Those are recorded in the manifest under
`extras_at_archive_time` for forensics. To prune the on-disk remote
later, the extras must be resolved (deleted, moved, or accepted with
`--force`).

### `dt remote archive list`

Print every archive recorded under `.dvc/archives/`. Does not contact
the backend.

### `dt remote archive verify <name>`

Default ("quick"):

- `stat` the backend object and confirm its size matches the manifest.
- Stream the backend object through `sha256` and compare to the manifest.

`--deep` additionally streams the outer tar through `tar -tvf -` to
enumerate the inner tarballs and confirm their sizes match the
manifest. This is slow on tape.

### `dt remote archive restore <name> --to <path>`

Modes:

- `--object <md5>` — extract a single md5 object. Streams the outer
  tar; `tar -xO` the matching inner tar; `tar -x` the single entry.
- `--prefix <hex>` — extract all objects under a single md5 prefix.
- (neither) — full restore. Downloads the outer tar to `--to` and
  extracts everything.

### `dt remote archive prune <name>`

Refuses to run unless:

1. The archive verifies (size + sha256 against the manifest). `--force`
   never bypasses this.
2. There are no files in the source remote outside `files/md5/`
   (extras). `--force` skips this check.

When both conditions are met, deletes `<source-remote>/files/md5/` and
reports the bytes freed. `--yes` skips the interactive confirmation.

## Parallelism

Inner-tar creation is process parallelism on a **single node**. Each
worker is a `tar` subprocess writing one prefix's tarball to staging.
You need to size the PBS job (or interactive `gadi-dm` session) to
match `--jobs`:

| Resource | Per `--jobs` | Notes |
| --- | --- | --- |
| CPUs | ~1 | Mostly IO-bound, more if `--compress zstd`. |
| Memory | ~0.5 GB | Tar's read buffers. |
| Staging disk | ~1× remote size total | Inner tarballs land here before upload. |

The MDSS upload itself is serial regardless. The total wall-clock is
roughly `max(parallel-inner-time, mdss-upload-time)`; for TB-class
remotes, the upload dominates and heroic `--jobs` values bring little
return.

Past `--jobs=8`, Lustre OST contention on `/g/data` usually erases any
remaining benefit, so the default is capped accordingly.

### Sample PBS submission

```bash
#!/bin/bash
#PBS -P a56
#PBS -q copyq
#PBS -l ncpus=8
#PBS -l mem=32GB
#PBS -l jobfs=400GB
#PBS -l walltime=08:00:00
#PBS -l storage=gdata/a56+scratch/a56

cd /path/to/repo
dt remote archive create neochemo-2026-05 \
    --jobs 8 \
    --staging-dir $PBS_JOBFS \
    -v
```

Single-node is the v1; the per-prefix work is structured as a
side-effect-free function (`dt.archive.operations.build_prefix_tarball`)
so a future multi-node mode can dispatch it as the unit of work in a
`qxub monitor` job graph without rewriting the inner loop.

## Backends

The first PR ships:

- `mdss` — NCI tape, via the `mdss` CLI. Only works on data-mover
  nodes (`gadi-dm.nci.org.au`).
- `local` — copies tarballs to a local directory. Intended for tests
  and local dev.

Adding a backend means subclassing the `ArchiveBackend` protocol in
[dt/archive/backends.py](../dt/archive/backends.py) and calling
`register_backend('<name>', <Cls>)`. The protocol is small: `put_stream`,
`put_file`, `get_file`, `get_stream`, `exists`, `stat`.

## Manifest

`.dvc/archives/<name>.yaml` is a small YAML document recording:

- Outer tar size and sha256.
- One row per inner tar with its filename, size, sha256 and object count.
- Provenance: git ref, dt version, who created it, when.
- The list of extras present at archive time (informational).

Commit it alongside the rest of the project so `list`, `verify`, and
`restore` work without backend access.

## Related commands

- [`dt remote init`](remote.md) — set up a DVC remote that you might
  later archive.
- [`dt config`](config.md) — set `archive.staging_dir`.
- [`dt du`](du.md) — figure out how big a remote is before archival.
