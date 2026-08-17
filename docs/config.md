# dt config

View and modify configuration settings for the `dt` tool.

## Usage

```bash
dt config                   # List all effective values, each tagged with its scope
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

The same flags work on `add`, `remove`, `unset`, `list`, and `path`. Only one
scope flag may be given at a time.

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

Only some settings hold several values:

| Key | Meaning of the list |
|-----|---------------------|
| `remote.root` | The **first** entry is where new remotes are created; the rest are also swept by `dt remote perms --all` and `dt remote clean --all` |
| `cache.root` | The **first** entry is the primary cache; the rest are reported by `dt auth list` |
| `secrets.gcp.locations` | Regions for user-managed secret replication |

`dt config add` refuses any other key. That is deliberate, and the reason is
worth knowing: a config outlives the code that reads it. Every other setting is
read as a single value, so turning one into a list does not fail where the
mistake was made — the reader hands the list on quite happily, and it surfaces
later as `Path(['/a', '/b'])` somewhere else entirely.

This is not hypothetical. `remote.root` gained extra entries before every
consumer had been taught to expect them, which retroactively broke 26 already
published `dt` container images: each aborts on startup with a bare
`TypeError`, and being immutable, none of them can be fixed.

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

`set` does **not** apply the restriction above — it will write a list to any
key, since it cannot tell a YAML list from a string that looks like one. If you
need a genuinely new list-valued setting, add it to `LIST_VALUED_KEYS` in
`dt/config.py` and make sure every consumer reads it through `get_str_list`.

## See Also

- [Configuration Scopes](config_scopes.md) - Understanding local, project, user, system scopes
- [Configuration Options](config_options.md) - Full list of available options
- [dt init](init.md) - Project initialization
- [dt clone](clone.md) - Clone repositories
