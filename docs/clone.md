# dt clone

The `clone` command creates a new local instance of an existing DVC project from a GitHub repository. This command handles the complete setup process to get a working DVC environment that's properly configured for the local platform.

## Usage

```bash
dt clone [options] <repository> [path]
```

## Arguments

- `<repository>`: Either a full GitHub URL or a short repository name
  - Full URL: `git@github.com:swarbricklab/neochemo.git`
  - Short name: `neochemo` (requires `org` to be configured)
- `[path]`: Optional path to the directory name for the clone (defaults to repository name in current directory)

## Options

- `--org <name>`: Override the GitHub organization for short names
- `--no-init`: Skip running `dt init` after cloning
- `--no-submodules`: Skip cloning git submodules
- `--cache-name <name>`: Override cache directory name (defaults to repository name)
- `--remote-name <name>`: Override remote directory name (defaults to repository name)
- `--shallow`: Perform a shallow clone (only recent history)

## Short Name Feature

When the `org` configuration is set, you can use repository short names instead of full URLs:

```bash
# Set your default organization once
dt config set org swarbricklab

# Then clone using just the repository name
dt clone neochemo

# This is equivalent to:
dt clone git@github.com:swarbricklab/neochemo.git
```

You can also override the organization for a single clone:

```bash
# Clone from a different organization
dt clone --org other-org some-repo
```

The command automatically detects whether you've provided a full URL or a short name:
- If the argument contains `:` or `/`, it's treated as a full URL
- Otherwise, it's treated as a short name and combined with the configured `org`

## What it does

This operation includes the following steps:

1. **Git Clone**: Clones the repository using `git clone`
2. **Submodule Initialization**: Recursively clones all git submodules and their submodules
3. **Cache Configuration**: Sets up the shared external cache directory

## Examples

### Using short names (recommended)

```bash
# First, set your default organization
dt config set org swarbricklab

# Clone using just the repository name
dt clone neochemo

# Clone to a specific directory
dt clone neochemo my-local-copy

# Clone from a different organization
dt clone --org other-lab their-analysis
```

### Using full URLs

```bash
# Clone a repository with default settings
dt clone git@github.com:swarbricklab/my-analysis.git

# Clone to a specific directory
dt clone git@github.com:swarbricklab/my-analysis.git my-local-copy

# Clone without automatic initialization (manual setup later)
dt clone --no-init git@github.com:swarbricklab/my-analysis.git

# Clone with custom cache name
dt clone --cache-name shared-analysis git@github.com:swarbricklab/my-analysis.git

# Quick clone without submodules for inspection
dt clone --no-submodules --shallow git@github.com:swarbricklab/my-analysis.git
```

## Typical Workflow

```bash
# Navigate to your workspace
cd /scratch/a56/$USER/

# Set up your organization (one-time)
dt config set org swarbricklab

# Clone an existing analysis using short name
dt clone single-cell-analysis

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

The cloned repository's existing DVC remote configuration is preserved. Use `dt remote init` if you need to set up local remote overrides.

## Manual Setup After Clone

If you need additional setup after cloning:

```bash
# Clone the repository
dt clone neochemo
cd neochemo

# Set up local remote override for faster transfers
dt remote init

# Download data
dvc pull
```

## Related Commands

- [`dt init`](init.md) - Initialize new DVC projects
- [`dt cache init`](cache.md#init) - Cache setup
- [`dt remote init`](remote.md#init) - Remote storage setup
- [`dt config`](config.md) - Configuration management
