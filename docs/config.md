# dt config

View and modify configuration settings for the `dt` tool.

## Usage

```bash
dt config list              # View all configuration
dt config get <key>         # View specific setting
dt config set <key> <value> # Set configuration (project scope by default)
dt config unset <key>       # Remove configuration
dt config path              # Show config file locations
```

### Scope Flags

```bash
dt config set --local <key> <value>   # Current workspace only
dt config set --project <key> <value> # Shared repo settings (default)
dt config set --user <key> <value>    # Personal defaults
dt config set --system <key> <value>  # Team defaults
```

## Quick Start

```bash
# Set cache and remote roots (saved to project config)
dt config set cache.root /g/data/a56/dvc_cache
dt config set remote.root /g/data/a56/dvc_remote

# Set personal settings (user scope)
dt config set --user owner swarbricklab
dt config set --user team analysts

# View current configuration
dt config list
dt config list --show-origin  # Show which scope each value comes from
```

## See Also

- [Configuration Scopes](config_scopes.md) - Understanding local, project, user, system scopes
- [Configuration Options](config_options.md) - Full list of available options
- [dt init](init.md) - Project initialization
- [dt clone](clone.md) - Clone repositories
