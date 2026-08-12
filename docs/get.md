# dt get

Download DVC-tracked data without creating tracking files.

## Usage

```bash
dt get <repository> <path> [options]
dt get <repository> --csv <file> [options]
```

## What it does

`dt get` is to `dvc get` what `dt import` is to `dvc import`: it materialises data from a source repository and writes **no `.dvc` file**. The result is plain data, with no provenance and no way to `dvc update` it later.

That is the point. It exists for handing a subset of a dataset to someone outside the group who is running their own pipeline and does not want our tracking files.

1. Clones the source repository once to read its DVC configuration
2. Looks for a cache reachable on this filesystem; if there is none, downloads from the source's remote instead (see [Two paths](#two-paths-local-cache-or-the-network))
3. Resolves each path with `dvc list`, which works for a subpath *inside* a tracked directory
4. Places the objects at their destinations, in parallel

## dt get vs dt import

| | `dt import` | `dt get` |
|---|---|---|
| Verb analogue | `dvc import` | `dvc get` |
| Writes `.dvc` tracking | yes | **no** |
| Updatable later (`dvc update`) | yes | no |
| Intended user | our repos, keep provenance | external collaborator, plain files |

Use `dt import` when you want to keep the link to the source. Use `dt get` when you want the bytes.

## Subpaths inside a tracked directory

A path may name a subdirectory *inside* a single large directory output. If `data/fq` is one tracked directory of 898 files, you can take one sample from it:

```bash
dt get my-registry data/fq/AF013-A -o fastqs/
```

This is what makes it possible to hand over 82 of 412 samples without transferring the other 330.

## Batch mode

`--csv` fetches every path listed in a CSV file. The source repository is cloned **once** and every row is resolved against that clone — unlike a loop over `dvc get`, which re-clones the source's git metadata on every invocation.

The CSV contract is shared with [`dt import --csv`](import.md): a column holding the source path (`path` by default), and an optional `output` column that overrides the destination for that row.

```bash
# Every row's path column, collected under fastqs/
dt get my-registry --csv samples.csv -o fastqs/

# The path lives in a differently-named column
dt get my-registry --csv samples.csv --path-col fq_dir -o fastqs/

# Only rows where wts_lib is empty
dt get my-registry --csv samples.csv --filter 'wts_lib=' -o fastqs/
```

Each row reports `✓`/`✗` with a summary, and the command exits non-zero if any row failed.

### How `--jobs` is spent

This describes the local-cache path; on the download path see [Two paths](#two-paths-local-cache-or-the-network).

Rows are resolved concurrently, then **every file from every row is placed through a single pool** of `--jobs` workers. The budget covers the transfer as a whole rather than being re-divided per row.

This matters when rows are small. A sample directory of two fastqs would, under per-row batching, leave six of eight workers idle for the whole row; flattening keeps them all fed across the 164 files of an 82-sample manifest. Reporting is still per row and still in CSV order.

### Filters

`--filter` is repeatable and the expressions are ANDed:

- `COL=VALUE` — the cell equals VALUE
- `COL!=VALUE` — the cell does not equal VALUE
- `COL=` — the cell is **empty**, which is how you select on "this column wasn't filled in"

Naming a column the CSV does not have is an error rather than a silent zero-row result.

## Link types

By default `dt get` honours DVC's `cache.type` when you are inside a repo that sets it, so on NCI it behaves like everything else here. `cache.type` is a preference *list* (ours is `hardlink,symlink`) and every entry is tried in order.

With two deliberate departures from the configured value:

**Symlink is never used unless you ask for it.** A symlink is right for a workspace, which is meant to stay attached to the cache, and wrong for a hand-off, which has to stand alone. It is also the most dangerous entry to inherit: linking from `/scratch` to `/g/data` fails `EXDEV` at hardlink and lands on symlink, which *succeeds having moved no bytes*. You get a confident `N fetched, 0 failed` over a directory of pointers into a cache the recipient cannot reach — and `rsync` will faithfully copy those pointers rather than the data.

**Copy always terminates the chain.** A `cache.type` of just `hardlink` would otherwise fail outright across filesystems.

So `hardlink,symlink` becomes `hardlink → copy`. With no configuration at all — the normal case outside our setup — it is **reflink → hardlink → copy**.

If you genuinely want symlinks, `--link symlink` still does it: an explicit flag is a decision, an inherited config value is not.

Override with `--link`:

```bash
dt get my-registry data/ref.fa --link copy -o ./
dt get my-registry data/ref.fa --link hardlink,copy -o ./
```

**On file permissions.** Copied and reflinked output is left writable (`0644`) — the recipient owns this data. Hardlinked output shares an inode with the cache object, so it keeps the cache's read-only mode; widening it would make the shared cache object writable too.

## Options

- `-o, --out <path>`: Destination. With `--csv`, the directory rows are collected under (default: basename of each path)
- `--owner <owner>`: Override the GitHub owner for short repository names
- `--rev <rev>`: Fetch from a specific git revision (branch, tag, or commit)
- `--csv <file>`: Fetch every path listed in a CSV file
- `--path-col <name>`: CSV column holding the source path (default: `path`)
- `--filter <expr>`: Select rows, e.g. `--filter 'wts_lib='`. Repeatable, ANDed.
- `-j, --jobs <n>`: Parallel workers (default: 8). In `--csv` mode the budget spans the whole transfer, not each row — see below.
- `--link <types>`: Link type(s), comma-separated. Defaults to DVC `cache.type` if set.
- `--no-refresh`: Skip refreshing the temp clone (for offline use)
- `--no-remote-fallback`: Fail instead of downloading when no local cache is reachable
- `--resume`: Skip files already present, continuing an interrupted transfer instead of restarting
- `--check`: Verify existing files against their recorded checksum
- `-f, --force`: Overwrite existing output files
- `-v, --verbose`: Show detailed progress

## Examples

```bash
# One subpath of a tracked directory
dt get my-registry data/fq/AF013-A -o fastqs/

# A whole tracked output
dt get my-registry data/reference -o ./

# From a specific revision
dt get my-registry data/fq/AF013-A --rev v1.2.0 -o fastqs/

# Batch, selecting SETUP samples straight from the source manifest
dt get bcarc-wts --csv paths.csv --path-col fq_dir --filter 'wts_lib=' -o fastqs/

# Force a real copy regardless of cache.type
dt get my-registry data/fq/AF013-A --link copy -o fastqs/
```

## Two paths: local cache, or the network

`dt get` picks automatically.

**If a cache is reachable on this filesystem** — the normal case inside NCI — it links or copies straight out of it. Fast, and no network involved.

**If not** — the normal case for anyone on a different system — it downloads from whichever remote the source repository configures (object storage, typically). This is what lets someone outside the group run `dt get` at all: they need credentials for that remote, but no shared filesystem and no DVC tracking in their own project.

The download path still clones the source only once: the local clone is handed to `dvc get` as the repository, so DVC has no reason to re-fetch the git metadata per row. Subpaths inside a tracked directory work the same way on both paths.

Rows download one at a time rather than fanned out, because the network is the bottleneck and `--jobs` already parallelises within a row.

Use `--no-remote-fallback` to make a missing local cache a hard error instead — useful when you expect the fast path and want to know if you didn't get it.

## Resuming and verifying

A 358 GiB transfer will not always survive to the end, so `dt get` can pick up where it stopped.

```bash
# Continue an interrupted transfer: fetch only what is missing
dt get my-registry --csv samples.csv -o fastqs/ --resume

# Verify a finished download against DVC's recorded checksums
dt get my-registry --csv samples.csv -o fastqs/ --check

# Resume, re-fetching anything that fails its checksum
dt get my-registry --csv samples.csv -o fastqs/ --resume --check
```

**Use `--resume` and `--check` together.** This is the important part. An interruption mid-file leaves a *truncated* file, and by existence — or even by size, if the writer preallocated — it is indistinguishable from a complete one. `--resume` alone will keep it forever, because it has no way to tell. `--check` hashes what is already there, so the corruption is found and replaced.

The three behave differently on purpose:

| Flags | Existing file that is correct | Existing file that is corrupt | Missing file |
|---|---|---|---|
| `--resume` | skipped | **kept, silently** | fetched |
| `--check` | verified | reported, exit non-zero | fetched |
| `--resume --check` | verified | re-fetched | fetched |

`--check` on its own is a validation pass: it does not repair anything, it tells you whether what you have is sound and exits non-zero if not. That is what you want after handing data to someone, or after receiving it.

`--force` and `--resume` are mutually exclusive — one re-fetches everything, the other skips what is present — and passing both is an error rather than a silent precedence rule.

On the download path, resume works **per file**, not per directory: a sample directory that stopped halfway through resumes at the file it stopped on rather than starting the sample again.

## See also

- [`dt import`](import.md) — same batch shape, but keeps provenance
- [`dt fetch`](fetch.md) — populate the primary cache
- [`dt pull`](pull.md) — fetch plus checkout in your own project
