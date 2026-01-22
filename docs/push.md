# dt push

Push DVC-tracked files to all project-configured remotes.

## Usage

```bash
dt push [options] [targets...]
```

## What it does

Runs `dvc push` for each remote configured at **project** or **local** scope, skipping remotes inherited from user or system config.

This ensures data is pushed to all remotes that are part of the repository configuration, without pushing to personal or team-wide default remotes.

## Options

All options are passed through to `dvc push`. See `dvc push --help` for available options.

## Examples

```bash
# Push all tracked files to all project remotes
dt push

# Push specific targets
dt push data/processed.csv.dvc
```

## Example

Suppose a project has remotes for cloud storage and HPC as follows:

```bash
$ dvc remote list
gs     gs://myproject
nci    ssh://myhost.com.au/myproject
```

Running `dt push` pushes to both, ensuring data is available on both GCS and SSH storage.

## See Also

- [dt clone](clone.md) - Clone repositories
- [dt remote init](remote.md) - Set up remote storage
