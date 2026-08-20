# dt init

The `init` command creates a new DVC project, ready to work with the patterns described in the main [Architecture](../README.md#architecture) section. This command provides a streamlined way to set up a complete DVC project with proper external caching and remote storage configuration.

## Project Structure After Initialization

After running `dt init`, your project will have:

```
my-project/
├── .git/                 # Git repository
├── .dt/                  # DVC Tools configuration
│   └── .gitignore        # Ignores config.local.yaml, tmp/ and hook-results/
├── .dvc/                 # DVC configuration
│   ├── config            # DVC settings (tracked)
│   ├── config.local      # Local DVC settings (not tracked)
│   └── ...
├── .dvcignore            # DVC ignore patterns, seeded with `.gitignore`
└── .gitignore            # Updated with DVC patterns
```

### Why `.dvcignore` starts with `.gitignore`

DVC writes a `.gitignore` next to everything it tracks. Its repo filesystem
excludes only `*.dvc`, `dvc.yaml`, `dvc.lock` and `.dvcignore` when another repo
reads through it — so `dvc import <this repo> <some/dir>`, where `some/dir` is
not itself an out, hashes those generated ignore files and folds them into the
importer's payload, `nfiles` and `size`. One pattern stops this repo's
bookkeeping from becoming someone else's data.

DVC still creates and maintains those `.gitignore` files; it just stops treating
them as content. `dt doctor` flags repos that predate this, and adding the
pattern later is safe — but note it changes the hash, `nfiles` and `size` of any
directory import of a path that contains one.

## Usage

```bash
# Initialize complete DVC project (all components)
dt init [options]
```

## Options

- `--name <name>`: Override the project name (defaults to current directory name)
- `--owner <owner>`: Override the GitHub owner (user or organization)
- `--team <team>`: GitHub team for access, used in the suggested `gh repo create` command (only valid if the owner is an org)
- `--cache-root <path>`: Override the cache root directory (defaults to config value)
- `--remote-root <path>`: Override the remote root directory (defaults to config value)
- `--site-cache-root <path>`: Override the DVC `site_cache_dir` root (combined with the project name). Falls back to the `site_cache.root` config value.
- `--site-cache-path <path>`: Override the full DVC `site_cache_dir` path (skips the `root/name` derivation).
- `--no-site-cache`: Skip `core.site_cache_dir` setup entirely; DVC's built-in default (typically `/var/tmp/dvc`, per-node) is used.
- `--no-git`: Skip git initialization
- `--no-dvc`: Skip DVC initialization
- `--no-cache`: Skip cache setup
- `--no-remote`: Skip remote setup

### `core.site_cache_dir`

DVC stores per-repo SQLite state (object index, link tracking, file
state cache) under `core.site_cache_dir`. By default that lives on
each node's local `/var/tmp/dvc`, which means every compute node
rebuilds the index the first time it touches the workspace.

When `site_cache.root` is configured (or `--site-cache-root` is given),
`dt init` sets `core.site_cache_dir` to `<root>/<project-name>` in
`.dvc/config.local`, so every node mounting the same workspace shares
one live index. See [`dt index`](index.md) for the full model and for
how to configure or migrate the path after the fact.

Resolution order is `--site-cache-path` (full path), then
`--site-cache-root`/`site_cache.root` combined with the project name.
If none of these is set the step is skipped and DVC's default applies.
`site_cache.enabled: false` in any config scope is a master switch that
disables the step regardless of the other settings.

## What it does

The `dt init` command orchestrates the following initialization steps:

1. **Git Setup**: Initializes git repository with `git init`
2. **DVC Setup**: Initializes DVC with `dvc init`
3. **Site Cache**: Sets `core.site_cache_dir` to `<site_cache.root>/<project-name>` (skipped with `--no-site-cache`, with `--no-dvc`, when `site_cache.enabled` is `false`, or when `site_cache.root` is unset and no `--site-cache-*` flag is given)
4. **DVC Tools Directory**: Creates `.dt/.gitignore` to ignore `config.local.yaml`, `tmp/` and `hook-results/`
5. **Cache Setup**: Runs `dt cache init` to configure shared external cache
6. **Remote Setup**: Runs `dt remote init` to set up remote storage
7. **Git Hooks**: Runs `dt install` to set up git hooks and the DVC merge driver (skipped with `--no-dvc`)
8. **GitHub Check**: Checks for a GitHub remote and suggests `gh repo create` if missing (skipped with `--no-git`)

Each cache and remote step can also be run independently for testing or incremental setup.

The `.dt/.gitignore` is auto-staged if DVC's `core.autostage` is enabled.

## Complete Initialization Example

```bash
# Create and initialize a new project
mkdir my-analysis && cd my-analysis

# Initialize everything at once
dt init

# This is roughly equivalent to running:
# git init
# dvc init
# dt cache init
# dt remote init
# dt install
```

## Incremental Setup

For testing or troubleshooting, you can run initialization steps independently:

```bash
# Initialize git first
git init

# Initialize DVC
dvc init

# Set up shared cache
dt cache init

# Set up remote storage
dt remote init
```

## Configuration

The project will be configured with:

- **Git remote**: `init` does not create one; it checks for an `origin` remote and prints a `gh repo create` command if there isn't one
- **DVC cache**: Points to shared external cache directory
- **DVC remotes**: 
  - SSH remote for external access (only when `ssh.host` is configured)
  - Local remote for efficient internal transfers
- **Permissions**: Proper group permissions for team collaboration
- **Hooks**: git commands will be linked to dvc commands via git hooks

## Related Commands

- [`dt add`](add.md) - Add files to DVC tracking
- [`dt cache init`](cache.md#dt-cache-init) - Cache setup
- [`dt remote init`](remote.md#dt-remote-init) - Remote storage setup
- [`dt index`](index.md) - Manage `core.site_cache_dir` and the optional archive mirror
- [`dt config`](config.md) - Configuration management
