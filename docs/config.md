# dt config

The `config` command is used to view and modify configuration settings that control the behavior of the `dt` tool. Configuration follows a hierarchical scope system similar to git and dvc, allowing for flexible management of settings across different levels.

## Configuration Format

Configuration files use **YAML** format for consistency with DVC (`dvc.yaml`) and Snakemake. Files are named `config.yaml` at each scope level.

Example configuration file:

```yaml
# dt config.yaml
org: "myorg"
platform: "hpc"

cache:
  root: "/shared/cache"
  permissions: "ug+rw"

remote:
  root: "/shared/remote"
  permissions: "ug+rw"

ssh:
  host: "login.example.org"
```

> **Note**: Always quote strings that could be misinterpreted by YAML (e.g., `"on"`, `"yes"`, `"1.10"`).

## Usage

```bash
# View all configuration
dt config

# View specific configuration item
dt config <key>

# Set configuration at default scope (user)
dt config <key> <value>

# Set configuration at specific scope
dt config --local <key> <value>
dt config --project <key> <value>
dt config --user <key> <value>
dt config --system <key> <value>

# Unset configuration
dt config --unset <key>
dt config --unset --local <key>
```

## Configuration Scopes

There are four levels of config, with scopes mirroring the levels used by git and dvc:

| Scope | Location | Tracked by git | Use case |
|-------|----------|----------------|----------|
| **local** | `.dt/config.local.yaml` | No (gitignored) | Current workspace only |
| **project** | `.dt/config.yaml` | Yes | All clones of this repo |
| **user** | `~/.config/dt/config.yaml` | N/A | All repos for current user |
| **system** | `$XDG_CONFIG_DIRS/dt/config.yaml` | N/A | All users (team defaults) |

Configuration values are resolved in order of precedence: local > project > user > system.

### System Configuration Location

The system-level configuration is found by searching `XDG_CONFIG_DIRS` (colon-separated list of directories, defaulting to `/etc/xdg`). To use a shared team configuration:

```bash
# Add to your environment (e.g., module file or .bashrc)
export XDG_CONFIG_DIRS="/path/to/team/xdg:${XDG_CONFIG_DIRS:-/etc/xdg}"
```

This allows the team config at `/path/to/team/xdg/dt/config.yaml` to be found automatically.

## Examples

```bash
# Set your organization for GitHub integration
dt config --user org myorg

# Set project-specific cache root that applies to all clones
dt config --project cache.root /shared/cache

# Set local SSH host for current workspace only
dt config --local ssh.host login.example.org

# View current effective configuration
dt config

# Check specific setting
dt config cache.root
```

## Configuration Options

### GitHub Integration
- `org`: Default GitHub organization for repository operations
- `team`: GitHub team to grant access when creating new repositories
- `github.token`: GitHub personal access token for API operations

### Cache Settings
- `cache.root`: Root directory for shared external caches
- `cache.permissions`: Default permissions for cache directories (default: `ug+rw`)

### Remote Settings
- `remote.root`: Root directory for remote storage
- `remote.permissions`: Default permissions for remote directories (default: `ug+rw`)

### SSH Settings
- `ssh.host`: SSH hostname for remote access (e.g., `gadi-dm.nci.org.au`)
- `ssh.user`: Default SSH username (defaults to current user)

### Platform Settings
- `platform`: Platform identifier (e.g., `nci`, `local`) - affects remote naming

## Team Configuration

For team-wide defaults, use the **system** scope via `XDG_CONFIG_DIRS`. See [swarbricklab/team_config](https://github.com/swarbricklab/team_config) for an example of managing shared team configuration.

## Best Practices

1. **Use user scope for personal settings**: GitHub tokens, preferred organizations
2. **Use project scope for repository-specific settings**: Custom cache locations, specific remote configurations
3. **Use local scope sparingly**: Only for workspace-specific overrides that shouldn't be shared
4. **Check effective configuration**: Run `dt config` regularly to see what settings are active
5. **Quote ambiguous YAML values**: Strings like `"yes"`, `"no"`, `"on"`, `"off"`, or version numbers like `"1.10"` should be quoted
