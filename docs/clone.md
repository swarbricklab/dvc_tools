# dt clone

The `clone` command creates a new local instance of an existing DVC project from a GitHub repository. This command handles the complete setup process to get a working DVC environment that's properly configured for the local platform.

## Usage

```bash
dt clone [options] <repository> [path]
```

## Arguments

- `<repository>`: Either a full GitHub URL or a short repository name
  - Full URL: `git@github.com:myorg/myproject.git`
  - Short name: `myproject` (requires `owner` to be configured)
- `[path]`: Optional path to the directory name for the clone (defaults to repository name in current directory)
  - `.` is supported only when the current directory is empty

## Options

- `--owner <name>`: Override the GitHub owner for short names
- `--no-init`: Skip running `dt init` after cloning
- `--no-submodules`: Skip cloning git submodules
- `--cache-name <name>`: Override cache directory name (defaults to repository name)
- `--remote-name <name>`: Override remote directory name (defaults to repository name)
- `--shallow`: Perform a shallow clone (only recent history)
- `--pull`: Run `dt pull` after cloning to fetch all data files
- `--no-auth`: Skip running `dt auth setup` after cloning
- `--no-hooks`: Skip installing git hooks and DVC merge driver
- `--rev <revision>`: Check out a specific branch, tag, or commit after cloning
- `--overwrite`: Remove the target directory if it already exists before cloning

## Short Name Feature

When the `owner` configuration is set, you can use repository short names instead of full URLs:

```bash
# Set your default owner once (can be user or org)
dt config set owner myorg

# Then clone using just the repository name
dt clone myproject

# This is equivalent to:
dt clone git@github.com:myorg/myproject.git
```

You can also override the owner for a single clone:

```bash
# Clone from a different owner
dt clone --owner other-user some-repo
```

The command automatically detects whether you've provided a full URL or a short name:
- If the argument contains `:` or `/`, it's treated as a full URL
- Otherwise, it's treated as a short name and combined with the configured `owner`

## What it does

This operation includes the following steps:

1. **Git Clone**: Clones the repository using `git clone`
2. **Submodule Initialization**: Recursively clones all git submodules and their submodules
3. **Cache Configuration**: Sets up the shared external cache directory
4. **Remote Setup**: Configures a local filesystem remote for efficient HPC shared access
5. **Git Hooks**: Installs pre-commit, post-checkout, and pre-push hooks plus the DVC merge driver (use `--no-hooks` to skip)
6. **Auth Setup**: Runs `dt auth setup` to configure SSH keys and S3 credentials (use `--no-auth` to skip)

## Examples

### Using short names (recommended)

```bash
# First, set your default owner
dt config set owner myorg

# Clone and pull in one step - ready to work immediately
dt clone --pull myproject

# Clone using just the repository name
dt clone myproject

# Clone to a specific directory
dt clone myproject my-local-copy

# Clone from a different owner
dt clone --owner other-lab their-analysis
```

### Using full URLs

```bash
# Clone a repository with default settings
dt clone git@github.com:myorg/my-analysis.git

# Clone to a specific directory
dt clone git@github.com:myorg/my-analysis.git my-local-copy

# Clone without automatic initialization (manual setup later)
dt clone --no-init git@github.com:myorg/my-analysis.git

# Clone with custom cache name
dt clone --cache-name shared-analysis git@github.com:myorg/my-analysis.git

# Quick clone without submodules for inspection
dt clone --no-submodules --shallow git@github.com:myorg/my-analysis.git
```

### Cloning a specific revision

Use `--rev` to clone a repository and immediately check out a branch, tag, or commit:

```bash
# Clone and check out a specific tag
dt clone --rev v1.2.0 myproject

# Clone and check out a branch
dt clone --rev feature/new-pipeline myproject

# Clone and check out a specific commit
dt clone --rev a3f9c12 myproject
```

### Overwriting an existing clone

Use `--overwrite` to replace an existing directory with a fresh clone:

```bash
# Remove the existing directory and clone again
dt clone --overwrite myproject

# Combine with --rev to reset to a known revision
dt clone --overwrite --rev v1.2.0 myproject my-local-copy
```

`--overwrite` cannot be used with destination `.`.

### Cloning into the current directory

You can clone directly into the current directory by passing `.` as the path,
but only when the directory is empty:

```bash
dt clone git@github.com:myorg/my-analysis.git .
```

If the current directory is not empty, choose a new destination directory instead.

## Typical Workflow

```bash
# Navigate to your workspace
cd /scratch/$PROJECT/$USER/

# Set up your owner (one-time)
dt config set owner myorg

# Clone and pull data in one step (recommended)
dt clone --pull single-cell-analysis
cd single-cell-analysis
# Ready to work - data files already checked out

# Or clone first, then pull manually
dt clone single-cell-analysis
cd single-cell-analysis
dt pull  # Download data files
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
dt clone git@github.com:myorg/large-dataset.git
cd large-dataset
dvc pull  # Downloads 10GB of data to shared cache

# Second clone reuses cached data
cd ..
dt clone git@github.com:myorg/large-dataset.git analysis-copy
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
dt pull
```

## Related Commands

- [`dt init`](init.md) - Initialize new DVC projects
- [`dt cache init`](cache.md#init) - Cache setup
- [`dt remote init`](remote.md#init) - Remote storage setup
- [`dt config`](config.md) - Configuration management
