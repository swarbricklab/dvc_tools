# Configuration Scopes

The `dt` configuration system uses hierarchical scopes, similar to git and DVC.

## Scope Hierarchy

| Scope | Location | Tracked | Use case |
|-------|----------|---------|----------|
| **local** | `.dt/config.local.yaml` | No | Workspace overrides |
| **project** | `.dt/config.yaml` | Yes | Shared repo settings |
| **user** | `$XDG_CONFIG_HOME/dt/config.yaml`, else `~/.config/dt/config.yaml` | N/A | Personal defaults (default for writes) |
| **system** | `<dir>/dt/config.yaml` for a `<dir>` in `$XDG_CONFIG_DIRS` | N/A | Team defaults |

The local and project paths are relative to the project root — the git root if
there is one, else the DVC root, else the current directory.

**Precedence:** local > project > user > system

When a key exists in multiple scopes, the more specific scope wins. Local overrides project, which overrides user, which overrides system.

## Choosing a Scope

`dt config set` writes **user** scope unless a flag says otherwise. Any other
scope has to be named.

### User Scope (default for writes)

Personal settings that apply in every repository you work in.

```bash
dt config set owner myorg
dt config set username jr9959
dt config set cache.root /g/data/a56/dvc_cache
```

Use for: almost everything — GitHub owner, SSH username, cache and remote
roots, staging directories. These describe you and the machine, not one
repository, so writing them once is enough.

This is the default because the alternative surprised people twice over: a
setting made in one repo did not apply in the next one, and `.dt/config.yaml`
is tracked by git, so a personal preference arrived in every collaborator's
checkout on the next pull.

### Project Scope

Settings that genuinely belong to the repository and *should* be shared with
everyone working on it. `.dt/config.yaml` is tracked by git, so a value written
here is committed and inherited.

```bash
dt config set --project hooks.pre-commit.checks.large-files.max_size 10MB
```

Use for: settings a collaborator would get wrong if left to their own defaults
— hook thresholds for a repo of unusually large files, a remote every
collaborator must use.

Because project scope outranks user scope, a value committed here overrides
what a collaborator set for themselves. `dt config set` says so when it happens:

```
$ dt config set cache.root /scratch/a56/jr9959/cache
Set cache.root=/scratch/a56/jr9959/cache in user config (/home/jr9959/.config/dt/config.yaml).
Note: cache.root is also set in project config (/path/to/repo/.dt/config.yaml), which takes
      precedence over user — the effective value is unchanged ('/g/data/a56/dvc_cache').
      Precedence is local > project > user > system. To change the value that applies,
      target that scope: --project.
```

### Local Scope

Settings for your current workspace only. Not tracked by git (`.dt/config.local.yaml` is gitignored).

```bash
dt config set --local ssh.host alternate-host.example.org
```

Use for: temporary overrides, testing, machine-specific paths.

### System Scope

Team-wide defaults shared by all users. `dt config set --system` writes to it
like any other scope, but the file usually lives in a shared, write-protected
directory, so in practice an administrator edits it.

Use for: organization defaults, shared infrastructure paths.

## Specifying Scope

The same four flags work on `set`, `add`, `remove`, `unset`, `list`, and
`path`. Only one may be given at a time.

```bash
dt config set key value            # User scope (default)
dt config set --local key value    # Local scope
dt config set --project key value  # Project scope
dt config set --user key value     # User scope (explicit)
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

## Removing a Value

`unset` also defaults to user scope, so removing a project-scoped value needs
`--project`. Rather than a bare "not found", the error names the scopes that do
hold the key:

```
$ dt config unset cache.root
Error: Key 'cache.root' is not set in user configuration, but is set in project:
  --project	/path/to/repo/.dt/config.yaml
Re-run with the scope you meant, e.g. 'dt config unset --project cache.root'.
```

## Viewing Configuration

```bash
dt config                      # All effective values, each tagged with its scope
dt config list                 # All effective values
dt config list --show-origin   # Show which scope each value comes from
dt config list --project       # Only project scope
dt config path                 # Show all four paths, with ✓/✗ for existence
dt config path --user          # Show just the user config path
```

## Sharing a Configuration

For a team on one filesystem, point `XDG_CONFIG_DIRS` at a shared system-scope
file (above) — everyone then tracks it live. For someone offsite, on their own
filesystem, send them a config file instead and have them run
`dt config import <file>`, which merges it into their user scope without
clobbering their own paths. See
[Handing a configuration to someone else](config.md#handing-a-configuration-to-someone-else).

## See Also

- [dt config](config.md) - Command reference
- [Configuration Options](config_options.md) - Available options
