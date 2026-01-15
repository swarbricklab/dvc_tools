# dt clone

The `clone` command creates a new local instance of an existing DVC project from a GitHub repository. This command handles the complete setup process to get a working DVC environment that's properly configured for the local platform.

## Usage

```bash
dt clone [options] <repository_url> [path]
```

## Arguments

- `<repository_url>`: GitHub repository URL (HTTPS or SSH format -- prefer SSH)
- `[path]`: Optional path to the directory name for the clone (defaults to repository name in current directory)

## Options

- `--no-init`: Skip running `dt init` after cloning
- `--no-submodules`: Skip cloning git submodules
- `--cache-name <name>`: Override cache directory name (defaults to repository name)
- `--remote-name <name>`: Override remote directory name (defaults to repository name)
- `--shallow`: Perform a shallow clone (only recent history)

## What it does

This operation includes the following steps:

1. **Git Clone**: Clones the repository using `git clone`
2. **Submodule Initialization**: Recursively clones all git submodules and their submodules
3. **DVC Environment Setup**: Runs `dt init` to configure local DVC environment (unless `--no-init` is specified)
4. **Cache Configuration**: Links to the appropriate shared cache directory
5. **Remote Configuration**: Sets up both SSH and local remotes for the platform

## Examples

```bash
# Clone a repository with default settings
dt clone git@github.com/swarbricklab/my-analysis.git

# Clone to a specific directory
dt clone git@github.com:swarbricklab/my-analysis.git my-local-copy

# Clone without automatic initialization (manual setup later)
dt clone --no-init git@github.com/swarbricklab/my-analysis.git

# Clone with custom cache name
dt clone --cache-name shared-analysis git@github.com/swarbricklab/my-analysis.git

# Quick clone without submodules for inspection
dt clone --no-submodules --shallow git@github.com/swarbricklab/my-analysis.git
```

## Typical Workflow

```bash
# Navigate to your workspace
cd /scratch/a56/$USER/

# Clone an existing analysis
dt clone git@github.com/swarbricklab/single-cell-analysis.git

# Start working
cd single-cell-analysis
dvc pull  # Download data files
```

## Cache Sharing

The clone command automatically configures the local workspace to use a shared cache based on the repository name. This means:

- Multiple clones of the same repository share the same cache
- Data files are only stored once on the filesystem
- First clone downloads files, subsequent clones link to existing cached files
- Cache location follows the pattern: `${cache.root}/${repository_name}`

### Cache Benefits

```bash
# First clone downloads all data
dt clone git@github.com/swarbricklab/large-dataset.git
cd large-dataset
dvc pull  # Downloads 10GB of data to shared cache

# Second clone reuses cached data
cd ..
dt clone git@github.com/swarbricklab/large-dataset.git analysis-copy
cd analysis-copy
dvc pull  # Instant - links to existing cache
```

## Platform Integration

After cloning, the workspace is automatically configured for the current platform:

- **Cache**: Points to shared external cache directory
- **SSH Remote**: Configured for access from external systems
- **Local Remote**: Optimized for transfers within the same platform
- **Git Hooks**: DVC hooks installed for seamless git/dvc integration

This ensures that the cloned repository works immediately without manual configuration while maintaining portability to other platforms.

## Manual Setup After Clone

If you used `--no-init`, you can set up the DVC environment manually:

```bash
# Clone without initialization
dt clone --no-init https://github.com/swarbricklab/my-project.git
cd my-project

# Set up DVC environment manually
dt cache init
dt remote init

# Or run full initialization
dt init
```

## Related Commands

- [`dt init`](init.md) - Initialize new DVC projects
- [`dt cache init`](cache.md#init) - Cache setup
- [`dt remote init`](remote.md#init) - Remote storage setup
- [`dt config`](config.md) - Configuration management
