# dt config

The `config` command is used to view and modify configuration settings that control the behavior of the `dt` tool. Configuration follows a hierarchical scope system similar to git and dvc, allowing for flexible management of settings across different levels.

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

- **local**: only affects the current instance of the current repo (stored in `.dt` directory within repo but ignored by git)
- **project**: affects all clones of the current repo (stored in `.dt` directory within repo and tracked by git)
- **user**: affects all clones of all repos used by the current user (stored outside repo in `~/.config/dt`)
- **system**: affects all users in all repos (stored outside repo in central location determined by `XDG_CONFIG_DIRS`)

Configuration values are resolved in order of precedence: local > project > user > system.

## Examples

```bash
# Set your organization for GitHub integration
dt config --user org swarbricklab

# Set project-specific cache root that applies to all clones
dt config --project cache.root /scratch/a56/dvc/cache

# Set local SSH host for current workspace only
dt config --local ssh.host gadi-dm.nci.org.au

# View current effective configuration
dt config

# Check specific setting
dt config cache.root
```

## Configuration Options

### GitHub Integration
- `org`: Default GitHub organization for repository operations
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

## Swarbrick Lab Defaults

For the Swarbrick Lab on NCI, most users can fall back on sensible defaults specified at the "system" level:

```bash
# System-level defaults (pre-configured)
dt config --system org swarbricklab
dt config --system cache.root /scratch/a56/dvc/cache
dt config --system remote.root /g/data/a56/dvc/analysis
dt config --system ssh.host gadi-dm.nci.org.au
dt config --system platform nci
```

These defaults can be overridden at user or project level as needed.

## Best Practices

1. **Use user scope for personal settings**: GitHub tokens, preferred organizations
2. **Use project scope for repository-specific settings**: Custom cache locations, specific remote configurations
3. **Use local scope sparingly**: Only for workspace-specific overrides that shouldn't be shared
4. **Check effective configuration**: Run `dt config` regularly to see what settings are active