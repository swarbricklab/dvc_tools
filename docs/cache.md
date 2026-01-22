# dt cache

Cache management commands for working with DVC's external shared caches in HPC environments.

## dt cache init

Sets up an external shared cache with proper permissions for group collaboration.

### Usage

```bash
dt cache init [options] [project_name]
```

### Options

- `--name <project_name>`: Override project name (defaults to current directory name)
- `--cache-root <path>`: Override cache root directory (defaults to `cache.root` config value)
- `--cache-path <path>`: Override complete cache path (absolute or relative to current directory)

### What it does

- Creates the external cache directory structure
- Sets group read/write permissions to allow team collaboration
- Configures DVC to use the external cache via `dvc cache dir --local`
- Keeps configuration local to maintain repository portability

### Examples

```bash
# Set up cache with default settings
# Uses: ${cache.root config}/${current directory name}
dt cache init

# Set up cache with custom name and root
# Uses: /scratch/a56/dvc/cache/my-project
dt cache init --name my-project --cache-root /scratch/a56/dvc/cache

# Set up cache with complete custom path (absolute)
dt cache init --cache-path /completely/different/location/shared-cache

# Set up cache with relative path
dt cache init --cache-path ../shared-cache
```

### Path Resolution

The cache location is determined by (in order of precedence):

1. **`--cache-path`** - Complete path override (absolute or relative to current directory)
2. **Constructed path** - `${cache_root}/${project_name}` where:
   - **cache_root**: `--cache-root` argument OR `cache.root` config value
   - **project_name**: `--name` argument OR current directory name

**Default behavior** (no options): Uses `${cache.root config}/${current directory name}`

## dt cache list

List the primary DVC cache and all configured alternate caches.

```bash
dt cache list
```

Example output:

```
Primary: /scratch/a56/jr9959/.dvc/cache

Alternate caches:
  /g/data/a56/dvc/neochemo      (local)
  /g/data/a56/dvc/projectA      (user)
```

## dt cache add

Add an alternate cache path for multi-cache checkout.

```bash
dt cache add <path> [--local|--project|--user|--system]
```

Default scope is **local** (stored in `.dt/config.local`).

```bash
# Add to local config (default)
dt cache add /g/data/a56/dvc/neochemo

# Add to user config
dt cache add /g/data/a56/dvc/shared --user
```

## dt cache remove

Remove an alternate cache path.

```bash
dt cache remove <path> [--local|--project|--user|--system]
```

```bash
dt cache remove /g/data/a56/dvc/neochemo
```

## Alternate cache configuration

Alternate caches are stored in dt config under `cache.alt`:

```yaml
# .dt/config.local
cache:
  alt:
    - /g/data/a56/dvc/neochemo
    - /g/data/a56/dvc/projectA
```

Paths from all scopes are merged, with duplicates removed.

Alternate caches allow `dt checkout` to find files across multiple cache locations. This is useful when:

- Importing data from other projects on the same filesystem
- Sharing caches across related projects
- Accessing data from a project's remote storage directly (when mounted locally)

## Related Commands

- [`dt init`](init.md) - Initialize projects with cache setup
- [`dt checkout`](checkout.md) - Checkout using multiple caches
- [`dt remote init`](remote.md#init) - Set up remote storage
- [`dt config`](config.md) - Configure cache settings