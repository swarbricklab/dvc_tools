# Configuration Scopes

The `dt` configuration system uses hierarchical scopes, similar to git and DVC.

## Scope Hierarchy

| Scope | Location | Tracked | Use case |
|-------|----------|---------|----------|
| **local** | `.dt/config.local.yaml` | No | Workspace overrides |
| **project** | `.dt/config.yaml` | Yes | Shared repo settings (default) |
| **user** | `$XDG_CONFIG_HOME/dt/config.yaml`, else `~/.config/dt/config.yaml` | N/A | Personal defaults |
| **system** | `<dir>/dt/config.yaml` for a `<dir>` in `$XDG_CONFIG_DIRS` | N/A | Team defaults |

The local and project paths are relative to the project root — the git root if
there is one, else the DVC root, else the current directory.

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
dt config set --user owner myorg
dt config set --user team analysts
```

Use for: GitHub owner (user or organization), team membership, personal preferences.

### System Scope

Team-wide defaults shared by all users. `dt config set --system` writes to it
like any other scope, but the file usually lives in a shared, write-protected
directory, so in practice an administrator edits it.

Use for: organization defaults, shared infrastructure paths.

## Specifying Scope

The same four flags work on `set`, `add`, `remove`, `unset`, `list`, and
`path`. Only one may be given at a time.

```bash
dt config set --local key value    # Local scope
dt config set --project key value  # Project scope (default)
dt config set --user key value     # User scope
dt config set --system key value   # System scope

dt config list --project           # List project config only
dt config list --show-origin       # Show scope for each value
```

## System Scope Setup

System configuration is found via `XDG_CONFIG_DIRS` (default `/etc/xdg`). Each
colon-separated directory is checked in turn and the first one containing
`dt/config.yaml` wins. To share team defaults:

```bash
# Add to module file or .bashrc
export XDG_CONFIG_DIRS="/g/data/a56/config/xdg:${XDG_CONFIG_DIRS:-/etc/xdg}"
```

The config at `/g/data/a56/config/xdg/dt/config.yaml` will be loaded automatically.

If no directory in the list contains the file, `dt` reports the first
directory as the system path — that is where `--system` writes.

## Viewing Configuration

```bash
dt config                      # All effective values, each tagged with its scope
dt config list                 # All effective values
dt config list --show-origin   # Show which scope each value comes from
dt config list --project       # Only project scope
dt config path                 # Show all four paths, with ✓/✗ for existence
dt config path --user          # Show just the user config path
```

## See Also

- [dt config](config.md) - Command reference
- [Configuration Options](config_options.md) - Available options
