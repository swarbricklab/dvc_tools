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


## Related Commands

- [`dt init`](init.md) - Initialize projects with cache setup
- [`dt remote init`](remote.md#init) - Set up remote storage
- [`dt config`](config.md) - Configure cache settings