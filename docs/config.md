# dt config

View and modify configuration settings for the `dt` tool.

## Usage

```bash
dt config                   # List all effective values, each tagged with its scope
dt config list              # View all configuration
dt config get <key>         # View specific setting
dt config set <key> <value> # Set configuration (user scope by default)
dt config add <key> <value> # Append to a list-valued setting
dt config remove <key> <value>  # Remove one entry from a list
dt config unset <key>       # Remove configuration
dt config import <file>     # Merge a config file someone sent you
dt config path              # Show config file locations
```

### Scope Flags

```bash
dt config set <key> <value>           # Personal defaults (user scope, default)
dt config set --local <key> <value>   # Current workspace only
dt config set --project <key> <value> # Shared repo settings, tracked by git
dt config set --user <key> <value>    # Personal defaults (explicit)
dt config set --system <key> <value>  # Team defaults
```

The same flags work on `add`, `remove`, `unset`, `list`, and `path`. Only one
scope flag may be given at a time.

Writes land in **user** scope unless told otherwise, so a setting applies in
every repository rather than only the one you happened to be standing in. Use
`--project` for the few settings that belong to the repository itself;
`.dt/config.yaml` is tracked by git, so that value is committed and inherited
by every collaborator.

Because project and local scope both outrank user scope, a user-scope write can
leave the effective value unchanged. When that happens `set` says so on stderr
and names the scope to use instead — see
[Configuration Scopes](config_scopes.md#project-scope).

## Quick Start

```bash
# Personal defaults, applied in every repository (user scope)
dt config set owner myorg
dt config set team analysts
dt config set cache.root /g/data/a56/dvc_cache
dt config set remote.root /g/data/a56/dvc_remote

# Settings that belong to this repository and its collaborators
dt config set --project hooks.pre-commit.checks.large-files.max_size 10MB

# View current configuration
dt config list
dt config list --show-origin  # Show which scope each value comes from
```

## Handing a configuration to someone else

Offsite collaborators are the awkward case: they need the same registry,
secret-manager and remote settings as everyone else, but their filesystem
paths are their own. `dt config import` installs a config file without them
having to know where config lives on their machine:

```bash
# You: write the settings that travel, and send the file
cat > lab-defaults.yaml <<'YAML'
owner: swarbricklab
secrets:
  backend: gcp
  gcp:
    project: swarbrick-secrets
YAML

# Them:
dt config import lab-defaults.yaml
```

The file is an ordinary `dt` config file — the same shape as the one
`dt config path --user` points at — so there is no separate format, and it can
equally be copied into place by hand. What the command adds over `cp`:

- **It merges.** Keys the file does not mention are left alone, so importing
  lab defaults does not wipe the cache root they set for their own machine.
  Copying the file over the top would.
- **It finds the path.** No instructions about `~/.config/dt/`, `XDG_CONFIG_HOME`,
  or creating the directory first.
- **It shows the diff and asks.** Additions apply straight away; anything that
  would *replace* an existing value is listed and confirmed first (`--yes` to
  skip, `--dry-run` to look without writing).
- **It validates.** A list given to a key that takes a single value is refused
  up front rather than breaking a tool later — see
  [List-valued settings](#list-valued-settings) for why that matters.
- **It flags paths that do not exist here**, which is the usual symptom of a
  file that came from a different filesystem.

```
$ dt config import lab-defaults.yaml
lab-defaults.yaml -> user config (/home/alice/.config/dt/config.yaml)
  + secrets.backend      gcp
  + secrets.gcp.project  swarbrick-secrets
  ~ owner                mine -> swarbricklab

Replace 1 existing value in user config? [y/N]: y
Wrote 3 settings to user config (/home/alice/.config/dt/config.yaml).
```

Re-importing the same file is a no-op, so it is safe to send an updated copy.
Scope flags work as they do on `set`, so `--project` imports settings that
belong to a repository and its collaborators rather than to a person.

For a whole team on the same filesystem, a shared system-scope file is usually
better than handing out copies — see
[System Scope Setup](config_scopes.md#system-scope-setup).

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
