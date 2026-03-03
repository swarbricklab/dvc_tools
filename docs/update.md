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

When `--rev` is not specified, `dt update` intelligently determines the best revision to use:

- If the .dvc file has a `rev_lock`, uses that (rebuilds at the currently locked revision)
- If the .dvc file has a `rev` field (branch/tag), updates to the latest HEAD of that ref
- Otherwise, updates to the default branch HEAD

This means running `dt update` without `--rev` will **not** accidentally update to the latest version—it respects the locked revision.

## Arguments

| Argument | Description |
|----------|-------------|
| `TARGETS` | .dvc files to update. If omitted, finds and updates all import files. |

## Options

| Option | Description |
|--------|-------------|
| `--rev TEXT` | Git revision (commit, branch, tag) to update to. |
| `--no-download` | Rebuild .dir file only, do not download data. |
| `--dry-run` | Show what would be updated without making changes. |
| `-v, --verbose` | Show detailed progress. |
| `--no-index-sync` | Skip automatic index mirror sync. |

## Examples

### Rebuild .dir manifests at locked revision

The most common use case—rebuild missing `.dir` files without changing the locked revision:

```bash
dt update data/external.dvc
```

### Preview what would be updated

```bash
dt update --dry-run
```

### Update to specific tag

```bash
dt update --rev v1.2.0
```

### Update to branch HEAD

```bash
dt update --rev main
```

### Update to specific commit

```bash
dt update --rev abc1234
```

### Update without downloading data

Rebuild the `.dir` manifest without downloading the actual files:

```bash
dt update --no-download data/external.dvc
```

### Update with verbose output

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

# Option 2: Run dt update manually
dt update imported/dir.dvc
dt fetch imported/dir.dvc
```

### Updating to a new version

When you want the latest version from upstream:

```bash
dt update --rev main data/shared.csv.dvc
```

This updates the .dvc file, fetches the data to cache, and checks out the files to your workspace—all in one command.

### Update without downloading

For CI/CD or when you only want to update the reference:

```bash
dt update --no-download --rev main
```

This updates the .dvc file to point to the new revision and rebuilds the `.dir` manifest, but doesn't download the actual data files.

## How it works

1. **Find import files**: If no targets specified, finds all .dvc files with a `deps.repo` section (imports)
2. **Determine revision**: Uses `--rev` if specified, otherwise uses the locked revision (`rev_lock`) from the .dvc file
3. **Clone source repo**: Clones the source repository at the target revision (cached in `.dt/tmp/`)
4. **Query source**: Gets the file listing, hashes, and sizes from the source repository using `dvc list --json --show-hash --size --recursive`
5. **Rebuild .dir**: For directories, rebuilds the `.dir` manifest file from the file listing
6. **Update .dvc file**: Updates the .dvc file with the new hash, `size`, and `nfiles` metadata
7. **Push .dir**: Pushes the `.dir` file to the source remote so others don't have this issue
8. **Fetch data**: Downloads the data files to the local cache
9. **Checkout**: Checks out files from cache to the workspace
10. **Sync index**: If index mirror is configured, syncs after update

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
