# DVC Tools

Convenient tools for working with DVC in HPC environments with shared external caches and SSH remotes.

## Installation

```bash
# Install from GitHub
pip install git+ssh://git@github.com/swarbricklab/dvc_tools.git
```

## Quick Start

```bash
# Create a new DVC project
mkdir my-analysis && cd my-analysis
dt init my-analysis

# Or clone an existing project  
dt clone git@github.com:myorg/existing-project.git

# Check configuration
dt doctor
```

## Commands

This package provides the `dt` command with subcommands for working with DVC projects:

| Command | Description |
|---------|-------------|
| `dt init` | Initialize a new DVC project with cache and remote |
| `dt clone` | Clone an existing DVC project with local configuration |
| `dt add` | Add files to DVC tracking via compute node |
| `dt checkout` | Checkout files, searching across multiple caches |
| `dt pull` | Pull DVC-tracked files, handling imports automatically |
| `dt push` | Push files to all configured remotes |
| `dt import` | Import data from other repositories using local caches |
| `dt cache` | Manage external shared caches |
| `dt remote` | Manage remote storage |
| `dt tmp` | Manage temporary repository clones |
| `dt config` | View and modify configuration settings |
| `dt doctor` | Diagnose common setup issues |

See the [Command Reference](docs/commands.md) for full documentation.

Each command includes help via `dt <command> --help`.

## Architecture

On HPC systems, `dt` supports the following pattern:

- **Workspaces** on fast scratch storage (e.g., `/scratch/${PROJECT}/${USER}/`)
- **Shared caches** on scratch for team collaboration (e.g., `/scratch/${PROJECT}/dvc/cache/`)
- **Remotes** on persistent storage (e.g., `/g/data/${PROJECT}/dvc/`)
- **SSH access** to remotes from external systems

## Documentation

- [Command Reference](docs/commands.md) - All commands and options
- [Configuration](docs/config.md) - Configuration system and scopes
- [DVC Basics](docs/DVC_basics.md) - Background on DVC concepts

## External Resources

- [DVC Official Docs](https://dvc.org/doc)
- [DVC YouTube Channel](https://www.youtube.com/playlist?list=PL7WG7YrwYcnDa_V8jmtnm_CTh4ny0Mm8M)
- [DVC Discord](https://discordapp.com/invite/dvwXA2N)

