# dt config

View and modify configuration settings for the `dt` tool.

## Usage

```bash
dt config list              # View all configuration
dt config get <key>         # View specific setting
dt config set <key> <value> # Set configuration (project scope by default)
dt config add <key> <value> # Append to a list-valued setting
dt config remove <key> <value>  # Remove one entry from a list
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
dt config set --user owner myorg
dt config set --user team analysts

# View current configuration
dt config list
dt config list --show-origin  # Show which scope each value comes from
```

## List-valued settings

Some settings hold several values. `remote.root` is the main one: the **first**
entry is where new remotes are created, and the rest are additional places that
`dt remote perms --all` and `dt remote clean --all` sweep.

```bash
dt config add remote.root /g/data/px14/dvc/analysis
dt config remove remote.root /g/data/px14/dvc/analysis
dt config get remote.root
```

`add` promotes a plain scalar to a list on first use, so a setting can start as
one path and grow. It appends, so it never changes which root is the default.

Scopes **override** rather than merge, so `add` writes the whole effective list
into the chosen scope — otherwise adding one entry at user scope would silently
discard the entries inherited from system scope.

```
$ dt config add remote.root /g/data/px14/dvc/analysis --user
Added to remote.root in user config (6 values):
  /g/data/a56/dvc/analysis   <- default
  /g/data/a56/dvc/datasets
  /g/data/a56/dvc/registries
  /g/data/a56/dvc/remotes
  /g/data/a56/dvc/workflows
  /g/data/px14/dvc/analysis
```

A list can also be written directly, though quoting makes it error-prone:

```bash
dt config set remote.root '[/g/data/a56/dvc/analysis, /g/data/a56/dvc/datasets]'
```

## See Also

- [Configuration Scopes](config_scopes.md) - Understanding local, project, user, system scopes
- [Configuration Options](config_options.md) - Full list of available options
- [dt init](init.md) - Project initialization
- [dt clone](clone.md) - Clone repositories
