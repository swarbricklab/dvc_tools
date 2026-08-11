# dt update

Rebuild `.dir` manifests and update imported DVC data.

## Synopsis

```bash
dt update [OPTIONS] [TARGETS]...
```

## Description

`dt update` serves two primary purposes:

1. **Rebuild `.dir` manifests** for directory imports when the manifest is missing from the source remote
2. **Update imports** to a different revision of the source repository

This is particularly useful for recovering from `.dir` fetch failures, which can occur when data was pushed with older DVC versions or when `dvc update --no-download` was used without re-pushing.

Rebuilt `.dir` files are automatically pushed to the source remote so others don't encounter the same missing manifest issues.

## Smart Revision Detection

When `--rev` is not specified, `dt update` compares the locked revision
(`deps.repo.rev_lock`) against the source repository's HEAD and decides what is
safe to do:

- **HEAD equals the locked rev** — refreshes the `.dir` at that revision.
- **HEAD moved, but the imported data is unchanged** — advances `rev_lock` to
  HEAD. This is a metadata-only upgrade; no data is re-downloaded.
- **HEAD moved and the data changed** — stops without touching anything and
  prints your options (`--force`, `--rebuild`, or `--rev <rev>`).
- **The imported path moved upstream** — the new path is detected by hash and
  the `.dvc` file is repointed at it.

So `dt update` never silently pulls *different data*, but it will roll the lock
forward when the content is provably identical. Use `--rebuild` if you want to
stay pinned to the locked revision without consulting HEAD at all.

## Arguments

| Argument | Description |
|----------|-------------|
| `TARGETS` | .dvc files to update. If omitted, finds and updates all import files. |

## Options

| Option | Description |
|--------|-------------|
| `--rev TEXT` | Git revision (commit, branch, tag) to update to. If omitted, see [Smart Revision Detection](#smart-revision-detection). |
| `--rebuild` | Rebuild .dir at the locked rev, skipping change detection entirely. |
| `--force` | Update to HEAD even when the upstream data has changed. |
| `--no-download` | Rebuild the .dir file only; skip the follow-up `dt fetch`. |
| `--dry-run`, `--dry` | Show what would be done without making changes. |
| `--status` | Print a status summary for all imports (implies `--dry-run`). |
| `-v, --verbose` | Show detailed progress. |

## Use Cases

### 1. Fix missing .dir manifests (most common)

When `dt fetch` fails because a `.dir` manifest is missing from the remote:

```bash
dt update --rebuild data/external.dvc
```

`--rebuild` rebuilds the `.dir` at the currently locked revision and never
consults upstream HEAD, so the version you are pinned to cannot change.

### 2. Check import status

See what would be updated across all imports without making changes:

```bash
dt update --status
# or
dt update --dry-run
```

### 3. Update to new data (like dvc update)

When you want the latest version from upstream:

```bash
dt update --rev HEAD data/shared.csv.dvc
```

Or update to a specific tag/commit:

```bash
dt update --rev v1.2.0
dt update --rev abc1234
```

If you just want to accept whatever is at HEAD after `dt update` has stopped to
warn you that the data changed:

```bash
dt update --force data/shared.csv.dvc
```

### 4. Fix .dir when upstream has changed

When the `.dir` is missing AND upstream data has changed, but you want to stay at your current version:

```bash
dt update --rebuild data/external.dvc
```

`--rebuild` skips change detection altogether, so it rebuilds the `.dir` at the
locked revision regardless of what has happened upstream. Use `--status` first
if you want to see what changed.

### 5. Update without downloading

For CI/CD or when you only want to update the reference:

```bash
dt update --no-download --rev main
```

This updates references without downloading data - useful for CI/CD.

### 6. Verbose output

```bash
dt update -v data/external.dvc
```

## Workflow

### Recovering from .dir failures

When `dt fetch` fails because a `.dir` manifest is missing:

```bash
# Fetch fails with hint about .dir files
dt fetch imported/dir.dvc
# Output: Failed .dir manifests (1): abc123.dir ...
# Hint: .dir files may need rebuilding. Try: dt fetch --update

# Option 1: Use dt fetch --update (automatic recovery)
dt fetch --update imported/dir.dvc

# Option 2: Rebuild at the locked rev, then fetch
dt update --rebuild imported/dir.dvc
dt fetch imported/dir.dvc

# Option 3: Let dt update decide (rolls the lock forward if data is unchanged)
dt update imported/dir.dvc
```

## How it works

1. **Find import files**: If no targets specified, finds all .dvc files with a `deps.repo` section (imports)
2. **Determine revision**: Uses `--rev` if specified, otherwise applies [Smart Revision Detection](#smart-revision-detection) (or the locked revision with `--rebuild`)
3. **Clone source repo**: Clones/refreshes the source repository (cached in `.dt/tmp/clones/`)
4. **Query source**: Gets the file listing, hashes, and sizes from the source repository using `dvc list --json --show-hash --size --recursive`
5. **Rebuild .dir**: For directories, rebuilds the `.dir` manifest file from the file listing
6. **Update .dvc file**: Updates the .dvc file with the new hash, `size`, and `nfiles` metadata
7. **Push .dir**: Pushes the `.dir` file to the source remote so others don't have this issue
8. **Fetch data**: Runs `dt fetch` on the updated targets to populate the local cache (skipped with `--no-download`)

`dt update` does not check files out into the workspace — run `dvc checkout`
afterwards if you need the workspace copies linked.

## Metadata population

`dt update` produces first-class `.dvc` files with complete metadata:

```yaml
outs:
- hash: md5
  path: images/
  md5: abc123def456.dir
  size: 1073741824    # Total size of all files (1 GiB)
  nfiles: 42          # Number of files in directory
```

For single file imports:

```yaml
outs:
- hash: md5
  path: data.csv
  md5: abc123def456
  size: 52428800      # File size (50 MiB)
```

This enables `dt du` to report accurate sizes and file counts. Note that size information is only available if the source repository's `.dvc` files also contain size metadata.

## Import detection

A .dvc file is considered an import if it has a `deps` section with a `repo` key:

```yaml
# This is an import .dvc file
md5: abc123
deps:
- path: data/file.csv
  repo:
    url: git@github.com:org/project.git
    rev_lock: def456
outs:
- path: file.csv
  md5: ghi789
```

For directory imports, DVC creates a `.dir` manifest file that lists all files in the directory. This manifest is stored in the cache with a `.dir` extension. If the `.dir` file is missing from the remote (e.g., because it wasn't pushed), `dt update` can rebuild it by checking out the source data.

Regular .dvc files (without `deps.repo`) cannot be updated with `dt update`.

## See also

- [dt fetch](fetch.md) - Fetch imported data to cache (with `--update` for auto-recovery)
- [dt import](import.md) - Import data from remote repository
- [dt pull](pull.md) - Pull data to workspace
