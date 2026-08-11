# dt tmp

Manage temporary repository clones for accessing DVC configuration from remote repositories.

## Overview

The `dt tmp` commands manage repository clones stored in `.dt/tmp/clones/`. They are ordinary git clones, so they carry the repository's `.dvc/` directory, its `*.dvc` files and its `dvc.yaml`/`dvc.lock` — everything needed to resolve a DVC target — but no DVC-tracked *data* is downloaded.

Temporary clones are used by other commands like `dt import` and `dt fetch` (for import files).

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

# Clone using short name (requires the owner config)
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
                    ├── .git/
                    ├── .dvc/
                    │   ├── config
                    │   └── ...
                    ├── dvc.yaml, dvc.lock
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
Cached repositories in .dt/tmp/clones/:
  github.com/myorg/project-a
  github.com/myorg/project-b
```

## dt tmp clean

Remove cached repository clones.

### Usage

```bash
dt tmp clean [repository] [--owner <owner>] [--all]
```

With neither a repository nor `--all`, the command errors rather than guessing.

### Options

- `--owner <owner>`: Override the GitHub owner for short names
- `--all`: Remove all cached clones

### Examples

```bash
# Remove by full URL
dt tmp clean git@github.com:myorg/otherproject.git

# Remove by short name
dt tmp clean otherproject --owner myorg

# Remove every cached clone
dt tmp clean --all
```

## Repository Resolution

Repository arguments can be:

1. **Full URL**: `git@github.com:owner/repo.git` or `https://github.com/owner/repo`
2. **Short name**: `repo` (requires `owner` config or `--owner` option)

Short names are resolved using the `owner` configuration:

```bash
# Set default owner
dt config set owner myorg

# Now short names work
dt tmp clone otherproject  # → git@github.com:myorg/otherproject.git
```

## Checkout

Clones are plain, full git clones:

```bash
git clone <url> .dt/tmp/clones/github.com/<owner>/<repo>
```

Earlier versions used a shallow sparse checkout of just `/.dvc/` and `*.dvc`.
That was dropped because it left out `dvc.yaml`/`dvc.lock`, so DVC could not
resolve pipeline outputs, and because a full history is needed to check out an
arbitrary revision (`dt import --rev`, `dt diff`). Only git objects are
transferred either way — no DVC-tracked data is downloaded.

## Automatic Usage

You typically don't need to use `dt tmp` directly. These commands use it automatically:

- `dt import` - Clones source repo to find cache
- `dt fetch` - Clones source for import `.dvc` files

## Gitignore

Cloning writes a `/tmp/` entry into `.dt/.gitignore` (creating the file if
needed) so clones are never accidentally committed. Existing entries are left
alone.

## See also

- [dt import](import.md) - Import data from repositories
- [dt fetch](fetch.md) - Fetch imports from local caches
- [dt remote list](remote.md#dt-remote-list) - List repository remotes
