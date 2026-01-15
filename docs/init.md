# dt init

The `init` command creates a new DVC project, ready to work with the patterns described in the main [Architecture](../README.md#architecture) section. This command provides a streamlined way to set up a complete DVC project with proper external caching and remote storage configuration.

## Project Structure After Initialization

After running `dt init`, your project will have:

```
my-project/
├── .git/                 # Git repository
├── .dvc/                 # DVC configuration
│   ├── config            # DVC settings
│   └── ...
├── .dt/                  # dt tool configuration (git-ignored)
|    └── config.local     # Local dt settings
├── .gitignore            # Updated with DVC patterns
── README.md             # Basic project README
```

## Usage

```bash
# Initialize complete DVC project (all components)
dt init [options]
```

## Options

- `--name <name>`: Override the project name (defaults to current directory name)
- `--org <organization>`: Override the GitHub organization (defaults to config value)
- `--cache-root <path>`: Override the cache root directory (defaults to config value)
- `--remote-root <path>`: Override the remote root directory (defaults to config value)
- `--no-git`: Skip git initialization
- `--no-dvc`: Skip DVC initialization
- `--no-cache`: Skip cache setup
- `--no-remote`: Skip remote setup

## What it does

The `dt init` command orchestrates the following initialization steps:

1. **Git Setup**: Runs `dt git init` to initialize git repository and link to GitHub
2. **DVC Setup**: Runs `dt dvc init` to initialize DVC in the repository
3. **Cache Setup**: Runs `dt cache init` to configure shared external cache
4. **Remote Setup**: Runs `dt remote init` to set up remote storage
5. **Git Hooks**: Runs `dvc install` to set up git hooks

Each of these steps can also be run independently for testing or incremental setup.

## Complete Initialization Example

```bash
# Create and initialize a new project
mkdir my-analysis && cd my-analysis

# Initialize everything at once
dt init

# This is equivalent to running:
# dt git init
# dt dvc init
# dt cache init 
# dt remote init
```

## Incremental Setup

For testing or troubleshooting, you can run initialization steps independently:

```bash
# Set up git and GitHub integration first
dt git init my-project

# Add DVC to existing git repo
dt dvc init

# Set up shared cache
dt cache init my-project

# Set up remote storage
dt remote init my-project
```

## Configuration

The project will be configured with:

- **Git remote**: Linked to GitHub repository
- **DVC cache**: Points to shared external cache directory
- **DVC remotes**: 
  - SSH remote for external access
  - Local remote for efficient internal transfers
- **Permissions**: Proper group permissions for team collaboration
- **Hooks**: git commands will be linked to dvc commands via git hooks

## Related Commands

- [`dt git init`](git.md) - Git and GitHub setup
- [`dt dvc init`](dvc.md) - DVC initialization  
- [`dt cache init`](cache.md#init) - Cache setup
- [`dt remote init`](remote.md#init) - Remote storage setup
- [`dt config`](config.md) - Configuration management
