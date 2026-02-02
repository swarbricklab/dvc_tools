# dt import

Import DVC-tracked data from remote repositories using locally-accessible caches.

## Usage

```bash
dt import <repository> <path> [options]
```

## What it does

Imports files or directories from another DVC repository by:

1. Cloning the source repository (sparsely) to access its DVC configuration
2. Finding a locally-accessible cache from the source repo's remotes
3. Creating `.dvc` and `.dir` files locally
4. Checking out the files using the discovered cache
5. Populating your primary cache so standard DVC commands work

Unlike `dvc import`, this does **not** require network access to the remote storage. Instead, it uses locally-accessible cache paths (e.g., shared filesystems on HPC).

## Options

- `--out, -o <path>`: Destination path for imported files (default: basename of source path)
- `--owner <owner>`: Override the GitHub owner for short repository names
- `--no-checkout`: Create `.dvc` file without checking out the data
- `-v, --verbose`: Show detailed progress

## Examples

```bash
# Import a directory from another repository
dt import git@github.com:myorg/otherproject.git data/processed

# Import with custom output name
dt import otherproject data/samples --out my_samples

# Import without checking out (just create .dvc file)
dt import neochemo data/large_dataset --no-checkout
```

## How it works

### Step 1: Clone Source Repository

A sparse clone is created in `.dt/tmp/clones/` containing only the `.dvc/` directory and `*.dvc` files. This provides access to the source repo's DVC configuration.

### Step 2: Find Local Cache

The source repository's remotes are examined to find one accessible locally:
- Direct filesystem paths: `/g/data/a56/dvc/project`
- SSH URLs matching `ssh.host`: `ssh://gadi-dm.nci.org.au/g/data/...`

### Step 3: Create DVC Files

For directories, a `.dir` file is created in the cache containing the manifest of all files. A `.dvc` file is created in your workspace pointing to this content.

### Step 4: Checkout

Files are checked out from the discovered cache using symlinks (respecting your `cache.type` setting).

### Step 5: Populate Primary Cache

Hardlinks (or symlinks for cross-device) are created in your primary cache pointing to the same content. This allows standard DVC commands to work:

```bash
dvc status        # Shows files are up to date
dvc push          # Can push to your remote
dvc checkout      # Standard checkout works
```

## Comparison with dvc import

| Feature | `dvc import` | `dt import` |
|---------|-------------|-------------|
| Network access required | Yes | No |
| Uses remote storage | Yes | Uses local cache |
| Creates `.dvc` file | Yes | Yes |
| Works offline | No | Yes (if cache accessible) |
| Tracks source repo | Yes (frozen) | No (standalone) |

## Requirements

- Source repository must have a DVC remote accessible as a local path
- The `ssh.host` config should be set if using SSH URLs
- Sufficient permissions to read the source cache

## Troubleshooting

### "No locally-accessible cache found"

The source repository doesn't have a remote that resolves to a local path. Options:
1. Manually add the cache: `dt cache add /path/to/cache`
2. Use standard `dvc import` with network access
3. Copy the cache to a local filesystem

### Files not found in cache

The source data may not have been pushed to the remote. Check with:
```bash
cd /path/to/source/repo
dvc push
```

## See also

- [dt checkout](checkout.md) - Checkout with import handling
- [dt cache add-from](cache.md#dt-cache-add-from) - Add cache from repository
- [dt tmp](tmp.md) - Manage temporary clones
- [dt remote list](remote.md#dt-remote-list) - List repository remotes
