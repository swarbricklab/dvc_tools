# DVC Tools

This package provides convenient tools for working with DVC in a HPC environment with shared external caches and SSH remotes on the same HPC.

These tools are currently used by Swarbrick Lab on NCI.

## Installation

Install the package using pip:

```bash
# Install from source (development)
pip install -e .

# Or install from PyPI (when published)
pip install dvc-tools
```

## Quick Start

```bash
# Create a new DVC project
mkdir my-analysis && cd my-analysis
dt init my-analysis

# Or clone an existing project  
dt clone git@github.com/swarbricklab/existing-project.git

# Check configuration
dt config
```

## Background Resources

For background information on DVC, see:
- [DVC Basics](docs/DVC_basics.md)
- [Official docs](https://dvc.org/doc)
- [Official YouTube channel](https://www.youtube.com/playlist?list=PL7WG7YrwYcnDa_V8jmtnm_CTh4ny0Mm8M)
- [Discord Server](https://discordapp.com/invite/dvwXA2N)

## Architecture

On NCI, the Swarbrick Lab typically uses the following pattern:
- workspaces are on scratch (usually `/scratch/${PROJECT}/${USER}/`)
- external shared caches are on scratch (usually `/scratch/)${PROJECT}/dvc/cache/${REPO}`)
- remotes are on gdata (usually `/g/data/${PROJECT}/dvc/${REPO_TYPE}/${REPO})
- remotes are defined as SSH hosts via `gadi-dm.nci.org.au`

## Commands

This package provides the `dt` command with subcommands for working with DVC projects in HPC environments:

### Core Commands
- **[config](docs/config.md)** - View and modify configuration settings with hierarchical scopes
- **[init](docs/init.md)** - Initialize new DVC projects with proper cache and remote setup  
- **[clone](docs/clone.md)** - Clone existing DVC projects from GitHub with automatic configuration

### Cache & Remote Management
- **[cache](docs/cache.md)** - Set up and manage external shared caches
- **[remote](docs/remote.md)** - Configure remote storage with SSH and local access

Each command includes help documentation accessible via `dt <command> --help`.

## Quick Command Reference

**config** - Manage configuration settings with hierarchical scopes (local > project > user > system). See [docs/config.md](docs/config.md).

**init** - Initialize new DVC projects with complete setup including git, DVC, cache, and remote configuration. See [docs/init.md](docs/init.md).

**clone** - Clone existing DVC projects from GitHub with automatic platform-specific configuration and shared cache setup. See [docs/clone.md](docs/clone.md).

**cache** - Set up external shared caches with proper group permissions for team collaboration. See [docs/cache.md](docs/cache.md).

**remote** - Configure remote storage with both SSH (for external access) and local (for efficiency) remotes. See [docs/remote.md](docs/remote.md).
