# dt update

Update imported DVC data to a specific revision.

## Synopsis

```bash
dt update [OPTIONS] [TARGETS]...
```

## Description

Updates .dvc files created by `dvc import` or `dt import` to reference a different revision of the source repository. By default, updates to the latest HEAD of the source repo.

This is the dt equivalent of `dvc update` with better defaults and integration with dt's index sync features.

## Arguments

| Argument | Description |
|----------|-------------|
| `TARGETS` | .dvc files to update. If omitted, finds and updates all import files. |

## Options

| Option | Description |
|--------|-------------|
| `--rev TEXT` | Git revision (commit, branch, tag) to update to. Defaults to HEAD. |
| `-R, --recursive` | Update all stages in specified directory. |
| `--no-download` | Update .dvc file only, do not download data. |
| `--to-remote` | Update data directly on the remote. |
| `-r, --remote TEXT` | Remote storage to perform updates to. |
| `-j, --jobs INTEGER` | Number of parallel jobs. |
| `-v, --verbose` | Show detailed progress. |
| `--no-index-sync` | Skip automatic index mirror sync. |

## Examples

### Update all imports to HEAD

```bash
dt update
```

### Update specific import file

```bash
dt update data/external.dvc
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

### Update .dvc file only (no data download)

```bash
dt update --no-download
```

### Update with verbose output

```bash
dt update -v data/external.dvc
```

## Workflow

### Typical usage

1. **Import data** from another repository:
   ```bash
   dt import other-project data/shared.csv
   ```

2. **Later, update** to get the latest version:
   ```bash
   dt update data/shared.csv.dvc
   ```

3. **Or update to specific version**:
   ```bash
   dt update --rev v2.0.0 data/shared.csv.dvc
   ```

### Update without downloading

For CI/CD or when you only want to update the reference:

```bash
dt update --no-download --rev main
```

This updates the .dvc file to point to the new revision but doesn't download the actual data.

## How it works

1. **Find import files**: If no targets specified, finds all .dvc files with a `deps.repo` section (imports)
2. **Run dvc update**: Calls `dvc update` with the specified options
3. **Sync index**: If index mirror is configured, syncs before and after update

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

Regular .dvc files (without `deps.repo`) cannot be updated with `dt update`.

## See also

- [dt import](import.md) - Import data from remote repository
- [dt fetch](fetch.md) - Fetch imported data to cache
- [dt pull](pull.md) - Pull data to workspace
