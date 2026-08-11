# dt pull

Pull DVC-tracked files, automatically handling imports from other repositories.

## Usage

```bash
dt pull [options] [targets...]
```

## What it does

`dt pull` is simply **`dt fetch` + `dvc checkout`**:

1. **Fetch phase**: runs [`dt fetch`](fetch.md) over the targets, which categorises
   every stage (repo import / URL import / regular) and populates the primary
   cache — via local symlinks where a source cache or local remote is
   accessible, and via the network otherwise.
2. **Checkout phase**: runs `dvc checkout` to link the cached objects into the
   workspace.

This enables pulling data from repositories that were imported with `dvc import`, even when you don't have direct access to their remote storage.

Unlike `dt fetch` (where `--network` is opt-in), `dt pull` has network access
**enabled by default**; use `--no-network` to restrict it to locally-accessible
sources.

## Options

| Option | Description |
|--------|-------------|
| `-f`, `--force` | Delete `.dir` manifests before pulling to force re-fetch (also passes `--force` to `dvc checkout`) |
| `--dry`, `--dry-run` | Show the stage categorisation without fetching or checking out |
| `-v`, `--verbose` | Show detailed progress (with `--dry`, lists every stage) |
| `--update` | Permit the mutating import re-resolution path (`dvc update`, which rewrites the `.dvc` file) and `.dir` manifest rebuild for imports that cannot be pulled otherwise |
| `--network` / `--no-network` | Enable/disable network access for fetching. Default: enabled |

`dt pull` does not forward unrecognised options to `dvc pull` — the flags above
are the complete set.

## Examples

### Basic usage

```bash
# Pull all tracked files (imports handled automatically)
dt pull

# Pull specific targets
dt pull data/imported.dvc data/local.dvc

# Pull a directory (resolves to its .dvc file)
dt pull data/

# Pull with verbose output
dt pull -v

# Force re-fetch (after fixing corrupted cache files)
dt pull --force data/
```

### Dry run

Preview what would be pulled without actually transferring. `--dry` prints the
stage categorisation produced by the fetch phase and skips the checkout:

```bash
# Summary view - stages grouped by category and source
dt pull --dry
# Output:
# Stage categorization (17 total):
# URL imports: 1
# Repo imports: 2
#     projectA: 2 (✓ local)
# Regular stages: 14 (✓ local via 'nci')

# Detailed list, naming every stage
dt pull --dry -v
# Output:
# Stage categorization (17 total):
# URL imports: 1
#     data/external.csv.dvc
# Repo imports: 2
#     projectA: 2 (✓ local)
#         data/dataset.dvc
#         models/pretrained.dvc
# Regular stages: 14 (✓ local via 'nci')
#     data/processed.csv.dvc
#     ...
```

## Target Resolution

Targets are resolved by DVC's own target view, the same mechanism `dvc pull`
uses. Any of these forms work:

| Target | Meaning |
|--------|---------|
| `data.dvc` | The stage in that `.dvc` file |
| `data/` | The `.dvc` file tracking that directory |
| `data/subdir/file.txt` | The stage whose output covers that path |
| `transform` | A pipeline stage name from `dvc.yaml` |
| `models/output.pkl` | A pipeline output (resolved to its `dvc.yaml` stage) |

With no targets at all, every stage in the repo is collected.

Stages are then categorised by the fetch phase: `.dvc` files with a `deps.repo`
section are repo imports (grouped by source repository), other imports — those
from `dvc import-url` — are URL imports, and everything else is a regular stage.

## How it works

### Step 1: Collect stages

Without targets, collects every stage in the repo. With targets, resolves each
one through DVC's target view (see [Target Resolution](#target-resolution)).

### Step 2: Categorise stages

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

For targets tracked by import `.dvc` files, `dt fetch`:
- Clones the source repository into `.dt/tmp/clones/`
- Finds a locally-accessible cache
- Creates symlinks in the primary cache

For a **pinned import** (e.g. one created with `dt import --rev`) whose data is
not in this repo's own remote, `dt pull` recovers it **non-mutatingly** from the
import's source: it checks out the source repo's clone at the locked revision,
points that clone's cache at your primary cache, and runs `dvc fetch` there, so
the pinned objects are pulled from the source's own remote without rewriting
your `.dvc` file. Only if that also fails does `dt pull` report an error and
suggest `--update` (which re-resolves and rewrites the `.dvc`).

### Step 4: Fetch regular stages

Regular stages are populated from a locally-accessible remote by symlink where
possible. When that is not possible and network access is enabled (the default),
`dt fetch` falls back to `dvc fetch` for those stages.

### Step 5: Checkout

Finally `dvc checkout` links the cached objects into the workspace (with
`--force` when `dt pull --force` was used). A checkout failure is reported but
does not undo the fetch — you can re-run `dvc checkout` manually to see details.

## Comparison with dvc pull

| Feature | `dvc pull` | `dt pull` |
|---------|-----------|-----------|
| Regular files | ✓ | ✓ |
| Import files | Requires source remote access | Uses local cache via `dt fetch` |
| Pinned imports missing from this remote | Fails | Recovered non-mutatingly from the source repo's remote |
| dvc.yaml outputs | ✓ | ✓ |
| Network access | Required for imports | Not required if a local cache/remote is accessible |

## Typical workflow

After cloning a project that has imports:

```bash
# Clone the project
dt clone myproject
cd myproject

# Preview what will be pulled
dt pull --dry -v

# Pull all data including imports
dt pull -v

# Only use locally-accessible sources (no network)
dt pull --no-network
```

## Force mode

The `--force` option deletes `.dir` manifest files from the cache before pulling. This forces DVC to re-fetch the entire directory contents from the remote.

### When to use

Use `--force` after running `dt cache validate --fix` to repair corrupted directories:

```bash
# Step 1: Find and delete corrupted files
dt cache validate --fix
# Output: Deleted 2 corrupted file(s)
#         Affected .dir manifests: 1
#         Run 'dt pull --force' to re-fetch affected directories

# Step 2: Force re-fetch the directory
dt pull --force data/
```

### How it works

1. Finds all `.dir` manifest files for the specified targets
2. Deletes them from the cache
3. Runs normal pull, which triggers a fresh fetch of all directory contents

Without `--force`, DVC would see the `.dir` manifest and assume all files are present, even if some were deleted by `--fix`.

## See also

- [dt cache validate](cache.md#dt-cache-validate) - Validate cache integrity
- [dt fetch](fetch.md) - Fetch imports from local caches
- [dt import](import.md) - Import data from other repositories
- [dt offline](offline.md) - Make imports resolvable on compute nodes without internet
- [dt push](push.md) - Push to all remotes (with parallel support)
- [Configuration Options](config_options.md) - `cache.*`, `remote.*` and `index.*` settings
