# dt tmp

Manage temporary repository clones for accessing DVC configuration from remote repositories.

## Overview

The `dt tmp` commands manage sparse clones stored in `.dt/tmp/clones/`. These clones contain only the `.dvc/` directory and `*.dvc` files, providing access to a repository's DVC configuration without downloading any data.

Temporary clones are used by other commands like `dt import`, `dt cache add-from`, and `dt checkout` (for import files).

## dt tmp clone

Clone a repository into `.dt/tmp/clones/`.

### Usage

```bash
dt tmp clone <repository> [options]
```

### Options

- `--owner <owner>`: Override the GitHub owner for short names
- `--no-refresh`: Use cached clone without refreshing

### Examples

```bash
# Clone using full URL
dt tmp clone git@github.com:myorg/otherproject.git

# Clone using short name (requires git.owner config)
dt tmp clone otherproject

# Clone with owner override
dt tmp clone otherproject --owner myorg

# Use existing clone without fetching updates
dt tmp clone otherproject --no-refresh
```

### What it creates

```
.dt/
└── tmp/
    └── clones/
        └── github.com/
            └── myorg/
                └── otherproject/
                    ├── .dvc/
                    │   ├── config
                    │   └── ...
                    └── *.dvc files
```

## dt tmp list

List all temporary clones.

### Usage

```bash
dt tmp list
```

### Example output

```
Temporary clones in .dt/tmp/clones/:
  github.com/myorg/project-a
  github.com/myorg/project-b
```

## dt tmp remove

Remove a temporary clone.

### Usage

```bash
dt tmp remove <repository> [--owner <owner>]
```

### Examples

```bash
# Remove by full URL
dt tmp remove git@github.com:myorg/otherproject.git

# Remove by short name
dt tmp remove otherproject --owner myorg
```

## Repository Resolution

Repository arguments can be:

1. **Full URL**: `git@github.com:owner/repo.git` or `https://github.com/owner/repo`
2. **Short name**: `repo` (requires `git.owner` config or `--owner` option)

Short names are resolved using the `git.owner` configuration:

```bash
# Set default owner
dt config set git.owner myorg

# Now short names work
dt tmp clone otherproject  # → git@github.com:myorg/otherproject.git
```

## Sparse Checkout

Clones use Git's sparse checkout feature to minimize disk usage:

```bash
git clone --no-checkout --depth 1 --single-branch <url>
git sparse-checkout set --no-cone '/.dvc/' '*.dvc'
```

This checks out only:
- The `.dvc/` configuration directory
- All `*.dvc` files (tracking files)

## Automatic Usage

You typically don't need to use `dt tmp` directly. These commands use it automatically:

- `dt import` - Clones source repo to find cache
- `dt cache add-from` - Clones repo to discover remotes  
- `dt checkout` - Clones source for import `.dvc` files

## Gitignore

The `.dt/tmp/` directory is automatically added to `.dt/.gitignore` to prevent accidental commits.

## See also

- [dt import](import.md) - Import data from repositories
- [dt cache add-from](cache.md#dt-cache-add-from) - Add cache from repository
- [dt remote list](remote.md#dt-remote-list) - List repository remotes
