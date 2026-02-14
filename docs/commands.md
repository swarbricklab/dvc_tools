# dt Command Reference

`dt` (DVC Tools) provides commands for working with DVC in HPC environments with shared external caches and SSH remotes.

## Commands Overview

| Command | Description |
|---------|-------------|
| [`dt init`](#dt-init) | Initialize a new DVC project with external cache and remote |
| [`dt clone`](#dt-clone) | Clone an existing DVC project and set up local configuration |
| [`dt add`](#dt-add) | Add files to DVC tracking via compute node |
| [`dt data status`](#dt-data-status) | Show DVC data status via compute node |
| [`dt diff`](#dt-diff) | Show content differences between file versions |
| [`dt fetch`](#dt-fetch) | Fetch import files into primary cache from local sources |
| [`dt find`](#dt-find) | Find workspace path(s) for a given hash |
| [`dt history`](#dt-history) | Show version history of DVC-tracked files |
| [`dt ls`](#dt-ls) | List and filter DVC-tracked files |
| [`dt mv`](#dt-mv) | Move or rename DVC-tracked files, preserving import metadata |
| [`dt pull`](#dt-pull) | Pull DVC-tracked files, handling imports automatically |
| [`dt push`](#dt-push) | Push DVC-tracked files to all configured remotes |
| [`dt import`](#dt-import) | Import DVC-tracked data from other repositories |
| [`dt summary`](#dt-summary) | Generate project documentation (tree.txt, dag.md) |
| [`dt cache`](#dt-cache) | Manage external shared caches |
| [`dt remote`](#dt-remote) | Manage remote storage |
| [`dt tmp`](#dt-tmp) | Manage temporary repository clones |
| [`dt worktree`](#dt-worktree) | Manage git worktrees with DVC cache configured |
| [`dt config`](#dt-config) | View and modify configuration settings |
| [`dt du`](#dt-du) | Report disk usage for DVC-tracked files |
| [`dt auth`](#dt-auth) | Verify and diagnose access to storage backends |
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

## dt data status

Show changes between the last git commit, DVC files and the workspace.

```bash
dt data status [-t <threads>] [--no-wait] [-v] [-- DVC_OPTIONS...]
```

Wraps `dvc data status` with parallel checksum computation and optional compute-node delegation. Uses the same resource allocations as `dt add`. [Full documentation →](data_status.md)

---

## dt diff

Show content differences between versions of DVC-tracked files.

```bash
dt diff <path> [--old REV] [--new REV] [-o FORMAT] [-v]
```

Compares actual file content (not just checksums) using format-specific handlers. [Full documentation →](diff.md)

---

## dt fetch

Fetch import files into the primary cache from local sources.

```bash
dt fetch [targets...] [-v] [--no-refresh]
```

Populates the primary cache with symlinks from source caches. For imports, automatically clones source repos to find local caches. [Full documentation →](fetch.md)

---

## dt find

Find workspace path(s) for a given hash.

```bash
dt find <hash> [--dvc-file] [--dir-file] [--cache-path] [-v] [--json]
```

Reverse lookup: given an MD5 hash, find which DVC-tracked file(s) it corresponds to. Searches both top-level files and files within directories. [Full documentation →](find.md)

---

## dt history

Show version history of DVC-tracked files.

```bash
dt history <path> [-n LIMIT] [--since DATE] [--json] [-v]
```

Lists different versions (checksums) across git history, showing when each version was introduced. [Full documentation →](history.md)

---

## dt ls

List and filter DVC-tracked files.

```bash
dt ls [URL] [PATH] [-R] [--pattern GLOB] [--min-size SIZE] [--files] [--json]
```

Wraps `dvc list` with filtering by path pattern, size, type, and hash. Pipe-friendly output. [Full documentation →](ls.md)

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

Resolves targets to their tracking `.dvc` files. For imports (`.dvc` with `deps.repo`), uses `dt fetch`. For regular files, uses `dvc pull`. [Full documentation →](pull.md)

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
| `dt cache rm` | Remove cached files for specified targets |

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

## dt worktree

Manage git worktrees with DVC cache configured.

| Subcommand | Description |
|------------|-------------|
| `dt worktree add <path>` | Create a worktree with DVC cache configured |
| `dt worktree list` | List all worktrees |
| `dt worktree remove <path>` | Remove a worktree |

Ensures DVC cache is shared between worktrees. [Full documentation →](worktree.md)

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

## dt auth

Verify and diagnose access to all storage backends used by a DVC project.

| Subcommand | Description |
|------------|-------------|
| `dt auth list` | Discover every storage endpoint the project uses |
| `dt auth whoami` | Show current user identities across systems |
| `dt auth check` | Test access to each endpoint |
| `dt auth request` | Generate an access-request template from failures |
| `dt auth teams` | Manage GitHub team access for repositories |
| `dt auth grant` | Grant a user access to a resource *(planned)* |

Discovers endpoints from DVC remotes, `.dvc` import files, dt config, and git remotes. Supports filesystem, SSH, S3-compatible (including CloudFlare R2), GCS, and git endpoints. [Full documentation →](auth.md)

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
