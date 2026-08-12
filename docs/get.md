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
2. Finds a locally-accessible cache from the source repo's remotes
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

Rows are resolved concurrently, then **every file from every row is placed through a single pool** of `--jobs` workers. The budget covers the transfer as a whole rather than being re-divided per row.

This matters when rows are small. A sample directory of two fastqs would, under per-row batching, leave six of eight workers idle for the whole row; flattening keeps them all fed across the 164 files of an 82-sample manifest. Reporting is still per row and still in CSV order.

### Filters

`--filter` is repeatable and the expressions are ANDed:

- `COL=VALUE` — the cell equals VALUE
- `COL!=VALUE` — the cell does not equal VALUE
- `COL=` — the cell is **empty**, which is how you select on "this column wasn't filled in"

Naming a column the CSV does not have is an error rather than a silent zero-row result.

## Link types

By default `dt get` honours DVC's `cache.type` when you are inside a repo that sets it, so on NCI it behaves like everything else here. Note `cache.type` is a preference *list* (ours is `hardlink,symlink`) and every entry is tried in order.

With no configuration — the normal case for someone outside our setup — it tries **reflink → hardlink → copy**. Symlink is deliberately excluded from that default: a symlink points into a cache the recipient may not be able to read, so it would appear to succeed while producing an unusable file.

Off NCI the chain falls through to a real copy by itself, because hardlink fails with `EXDEV` across filesystems.

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

## Requirements

`dt get` copies from storage this machine can already reach. If the source data lives only in a remote you have no mount for, it will say so — use `dvc get` for that case.

## See also

- [`dt import`](import.md) — same batch shape, but keeps provenance
- [`dt fetch`](fetch.md) — populate the primary cache
- [`dt pull`](pull.md) — fetch plus checkout in your own project
