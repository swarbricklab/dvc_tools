# dt config

View and modify configuration settings for the `dt` tool.

## Usage

```bash
dt config list              # View all configuration
dt config get <key>         # View specific setting
dt config set <key> <value> # Set configuration (user scope by default)
dt config unset <key>       # Remove configuration
dt config path              # Show config file locations
```

### Scope Flags

```bash
dt config set --local <key> <value>   # Current workspace only
dt config set --project <key> <value> # All clones of this repo (tracked)
dt config set --user <key> <value>    # All repos for current user (default)
dt config set --system <key> <value>  # All users (team defaults)
```

## Configuration Scopes

| Scope | Location | Tracked | Use case |
|-------|----------|---------|----------|
| **local** | `.dt/config.local.yaml` | No | Workspace overrides |
| **project** | `.dt/config.yaml` | Yes | Shared repo settings |
| **user** | `~/.config/dt/config.yaml` | N/A | Personal defaults |
| **system** | `$XDG_CONFIG_DIRS/dt/config.yaml` | N/A | Team defaults |

Precedence: local > project > user > system

## Quick Start

```bash
# Set your organization (enables short clone names)
dt config set org swarbricklab

# Set cache and remote roots
dt config set cache.root /g/data/a56/dvc_cache
dt config set remote.root /g/data/a56/dvc_remote

# Set SSH host for remote access
dt config set ssh.host gadi-dm.nci.org.au

# View current configuration
dt config list
dt config list --show-origin  # Show which scope each value comes from
```

## Team Configuration

For team-wide defaults, use system scope via `XDG_CONFIG_DIRS`:

```bash
export XDG_CONFIG_DIRS="/path/to/team/xdg:${XDG_CONFIG_DIRS:-/etc/xdg}"
```

The config at `/path/to/team/xdg/dt/config.yaml` will be found automatically.

## See Also

- [Configuration Options](config_options.md) - Full list of available options
- [dt init](init.md) - Project initialization
- [dt clone](clone.md) - Clone repositories
