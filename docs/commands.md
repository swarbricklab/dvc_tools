# dt Command Reference

`dt` (DVC Tools) provides commands for working with DVC in HPC environments with shared external caches and SSH remotes.

## Commands Overview

| Command | Description |
|---------|-------------|
| [`dt init`](#dt-init) | Initialize a new DVC project with external cache and remote |
| [`dt clone`](#dt-clone) | Clone an existing DVC project and set up local configuration |
| [`dt add`](#dt-add) | Add files to DVC tracking via compute node |
| [`dt checkout`](#dt-checkout) | Checkout DVC-tracked files, searching across multiple caches |
| [`dt mv`](#dt-mv) | Move or rename DVC-tracked files, preserving import metadata |
| [`dt pull`](#dt-pull) | Pull DVC-tracked files, handling imports automatically |
| [`dt push`](#dt-push) | Push DVC-tracked files to all configured remotes |
| [`dt import`](#dt-import) | Import DVC-tracked data from other repositories |
| [`dt summary`](#dt-summary) | Generate project documentation (tree.txt, dag.md) |
| [`dt cache`](#dt-cache) | Manage external shared caches |
| [`dt remote`](#dt-remote) | Manage remote storage |
| [`dt tmp`](#dt-tmp) | Manage temporary repository clones |
| [`dt config`](#dt-config) | View and modify configuration settings |
| [`dt du`](#dt-du) | Report disk usage for DVC-tracked files |
| [`dt doctor`](#dt-doctor) | Diagnose common setup issues |

---

## dt init

Initialize a new DVC project with external cache and remote storage.

```bash
dt init [options]
```

Sets up a complete DVC project with shared cache and SSH-accessible remote. [Full documentation →](init.md)

---

## dt clone

Clone an existing DVC project and set up local configuration.

```bash
dt clone <repository> [directory]
```

Clones a Git repository and configures local DVC settings for the current environment. [Full documentation →](clone.md)

---

## dt add

Add files or directories to DVC tracking via compute node.

```bash
dt add [targets...] [-t <threads>] [--no-wait] [-v]
```

Submits `dvc add` to a compute node via qxub with parallel checksum computation. Threads are capped to file count, and CPUs are allocated at 1 per 4 threads. [Full documentation →](add.md)

---

## dt checkout

Checkout DVC-tracked files, searching across multiple caches.

```bash
dt checkout [targets...] [-v] [-c <cache>]
```

Searches primary and alternate caches for files. Automatically handles import `.dvc` files by cloning source repos. [Full documentation →](checkout.md)

---

## dt mv

Move or rename DVC-tracked files, preserving import metadata.

```bash
dt mv <src> <dst> [-v]
```

Wraps `dvc mv` to fix a bug where import `.dvc` files lose their `deps` section. [Full documentation →](mv.md)

---

## dt pull

Pull DVC-tracked files, handling imports automatically.

```bash
dt pull [targets...] [-v]
```

Resolves targets to their tracking `.dvc` files. For imports (`.dvc` with `deps.repo`), uses `dt checkout`. For regular files, uses `dvc pull`. [Full documentation →](pull.md)

---

## dt push

Push DVC-tracked files to all configured remotes.

```bash
dt push [targets...] [options]
```

Pushes to both the default remote and local remote for redundancy. [Full documentation →](push.md)

---

## dt import

Import DVC-tracked data from other repositories using local caches.

```bash
dt import <repository> <path> [-o <output>] [--no-checkout]
```

Imports files without network storage access by using locally-accessible caches. [Full documentation →](import.md)

---

## dt summary

Generate project documentation files.

```bash
dt summary [--out <dir>] [--tree-only] [--dag-only]
```

Creates tree.txt (DVC-tracked file listing) and dag.md (pipeline DAG in mermaid format). [Full documentation →](summary.md)

---

## dt cache

Manage external shared caches.

| Subcommand | Description |
|------------|-------------|
| `dt cache init` | Set up an external shared cache with proper permissions |
| `dt cache list` | List the primary DVC cache and all alternate caches |
| `dt cache add <path>` | Add an alternate cache path for multi-cache checkout |
| `dt cache add-from <repo>` | Discover and add a cache from a repository's remotes |
| `dt cache remove <path>` | Remove an alternate cache path |

[Full documentation →](cache.md)

---

## dt remote

Manage remote storage.

| Subcommand | Description |
|------------|-------------|
| `dt remote init` | Set up remote storage with SSH and local access methods |
| `dt remote list [repo]` | List DVC remotes (optionally from a remote repository) |

[Full documentation →](remote.md)

---

## dt tmp

Manage temporary repository clones.

| Subcommand | Description |
|------------|-------------|
| `dt tmp clone <repo>` | Clone a repository into `.dt/tmp/clones/` (sparse checkout) |
| `dt tmp list` | List cached repository clones |
| `dt tmp clean` | Remove cached repository clones |

Temporary clones provide access to DVC configuration from remote repositories. [Full documentation →](tmp.md)

---

## dt config

View and modify configuration settings.

| Subcommand | Description |
|------------|-------------|
| `dt config list` | List all effective configuration values |
| `dt config get <key>` | Get a specific configuration value |
| `dt config set <key> <value>` | Set a configuration value |
| `dt config unset <key>` | Remove a configuration value |
| `dt config path` | Show configuration file paths |

Configuration uses hierarchical scopes: local > project > user > system. [Full documentation →](config.md)

---

## dt du

Report disk usage for DVC-tracked files.

```bash
dt du [targets...] [-h] [-d N] [-s] [--inodes] [-c] [--cached|--expected]
```

| Option | Description |
|--------|-------------|
| `-h, --human-readable` | Print sizes in human-readable format (K, M, G) |
| `-d, --max-depth N` | Limit output to N levels of depth |
| `-s, --summarize` | Show only the grand total |
| `--inodes` | Count number of files instead of bytes |
| `-c, --total` | Show a grand total line at the end |
| `--cached/--expected` | Show cached sizes (default) or expected sizes from metadata |

Output is sorted by size ascending (largest last). [Full documentation →](du.md)

---

## dt doctor

Diagnose common setup issues and verify configuration.

```bash
dt doctor
```

Checks DVC installation, cache configuration, remote setup, and permissions. [Full documentation →](doctor.md)

---

## See Also

- [DVC Basics](DVC_basics.md) - Introduction to DVC concepts
- [Configuration Options](config_options.md) - Available configuration settings
- [Configuration Scopes](config_scopes.md) - How configuration hierarchy works
