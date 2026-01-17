# Configuration Scopes

The `dt` configuration system uses hierarchical scopes, similar to git and DVC.

## Scope Hierarchy

| Scope | Location | Tracked | Use case |
|-------|----------|---------|----------|
| **local** | `.dt/config.local.yaml` | No | Workspace overrides |
| **project** | `.dt/config.yaml` | Yes | Shared repo settings (default) |
| **user** | `~/.config/dt/config.yaml` | N/A | Personal defaults |
| **system** | `$XDG_CONFIG_DIRS/dt/config.yaml` | N/A | Team defaults |

**Precedence:** local > project > user > system

When a key exists in multiple scopes, the more specific scope wins. Local overrides project, which overrides user, which overrides system.

## Choosing a Scope

### Project Scope (default)

Settings that should be shared with all collaborators on a repository. The `.dt/config.yaml` file is tracked by git.

```bash
dt config set cache.root /g/data/a56/dvc_cache
```

Use for: cache locations, remote roots, project-specific settings.

### Local Scope

Settings for your current workspace only. Not tracked by git (`.dt/config.local.yaml` is gitignored).

```bash
dt config set --local ssh.host alternate-host.example.org
```

Use for: temporary overrides, testing, machine-specific paths.

### User Scope

Personal settings that apply to all repositories for the current user.

```bash
dt config set --user owner swarbricklab
dt config set --user team analysts
```

Use for: GitHub owner (user or organization), team membership, personal preferences.

### System Scope

Team-wide defaults shared by all users. Read-only via CLI (edit file directly).

Use for: organization defaults, shared infrastructure paths.

## Specifying Scope

Use scope flags with `set`, `unset`, and `list`:

```bash
dt config set --local key value    # Local scope
dt config set --project key value  # Project scope (default)
dt config set --user key value     # User scope
dt config set --system key value   # System scope

dt config list --project           # List project config only
dt config list --show-origin       # Show scope for each value
```

## System Scope Setup

System configuration is found via `XDG_CONFIG_DIRS`. To share team defaults:

```bash
# Add to module file or .bashrc
export XDG_CONFIG_DIRS="/g/data/a56/config/xdg:${XDG_CONFIG_DIRS:-/etc/xdg}"
```

The config at `/g/data/a56/config/xdg/dt/config.yaml` will be loaded automatically.

## Viewing Configuration

```bash
dt config list                 # All effective values
dt config list --show-origin   # Show which scope each value comes from
dt config list --project       # Only project scope
dt config path                 # Show config file paths
```

## See Also

- [dt config](config.md) - Command reference
- [Configuration Options](config_options.md) - Available options
