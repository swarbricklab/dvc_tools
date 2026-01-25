# dt pull

Pull DVC-tracked files, automatically handling imports from other repositories.

## Usage

```bash
dt pull [options] [targets...]
```

## What it does

A smart pull that handles both regular DVC files and imports:

1. **Resolve targets** to their tracking `.dvc` files
2. **Separate imports**: Targets tracked by `.dvc` files with `deps.repo` are handled via `dt checkout`
3. **Pull remaining**: Other targets are pulled via `dvc pull`

This enables pulling data from repositories that were imported with `dvc import`, even when you don't have direct access to their remote storage.

## Options

- `-v, --verbose`: Show detailed progress

All other options are passed through to `dvc pull`. See `dvc pull --help` for available options.

## Examples

```bash
# Pull all tracked files (imports handled automatically)
dt pull

# Pull specific targets
dt pull data/imported.dvc data/local.dvc

# Pull a directory (resolves to its .dvc file)
dt pull data/

# Pull a file within a tracked directory
dt pull data/subdir/file.txt

# Pull with verbose output
dt pull -v
```

## Target Resolution

Each target is resolved to its tracking `.dvc` file:

| Target | Resolves to |
|--------|-------------|
| `data.dvc` | `data.dvc` |
| `data/` | `data.dvc` (if exists) |
| `data/subdir/file.txt` | `data.dvc` (parent dir tracking) |
| `models/output.pkl` | None (if tracked by `dvc.yaml`) |

If the resolved `.dvc` file has a `deps.repo` section (indicating an import), the target is handled via `dt checkout`. Otherwise, it's passed to `dvc pull`.

## How it works

### Step 1: Resolve targets

Without targets, scans for all `.dvc` files. With targets, resolves each to its tracking `.dvc` file (if any).

### Step 2: Separate imports from regular files

Each `.dvc` file is checked for a `deps` section with a `repo` key:

```yaml
# Import .dvc file (has deps.repo)
deps:
- path: data/shared
  repo:
    url: git@github.com:myorg/otherproject.git
outs:
- md5: abc123...
  path: shared_data
```

### Step 3: Handle imports

For targets tracked by import `.dvc` files, runs `dt checkout` which:
- Clones the source repository (sparsely)
- Finds a locally-accessible cache
- Checks out the files
- Populates the primary cache

### Step 4: Pull remaining data

Runs `dvc pull` for targets not tracked by imports, fetching from configured remotes.

## Comparison with dvc pull

| Feature | `dvc pull` | `dt pull` |
|---------|-----------|-----------|
| Regular files | ✓ | ✓ |
| Import files | Requires source remote access | Uses local cache from source repo |
| dvc.yaml outputs | ✓ | ✓ (passed through) |
| Network access | Required for imports | Not required if cache accessible |

## Typical workflow

After cloning a project that has imports:

```bash
# Clone the project
dt clone myproject
cd myproject

# Pull all data including imports
dt pull -v

# Output:
# Handling imports...
#   data/external.dvc → dt checkout
# Pulling remaining data...
#   dvc pull data/local.dvc models/
```

## See also

- [dt checkout](checkout.md) - Checkout with import handling
- [dt import](import.md) - Import data from other repositories
- [dt push](push.md) - Push to all remotes
