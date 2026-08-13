# dt Command Reference

`dt` (DVC Tools) provides commands for working with DVC in HPC environments with shared external caches and SSH remotes.

## Commands Overview

| Command | Description |
|---------|-------------|
| [`dt init`](#dt-init) | Initialize a new DVC project with external cache and remote |
| [`dt clone`](#dt-clone) | Clone an existing DVC project and set up local configuration |
| [`dt add`](#dt-add) | Add files to DVC tracking via compute node |
| [`dt data status`](#dt-data-status) | Show DVC data status via compute node |
| [`dt diff`](#dt-diff) | Show differences between versions of DVC-tracked data |
| [`dt fetch`](#dt-fetch) | Fetch DVC-tracked files into the primary cache from local sources |
| [`dt find`](#dt-find) | Find workspace path(s) for a given hash |
| [`dt history`](#dt-history) | Show version history of DVC-tracked files |
| [`dt ls`](#dt-ls) | List and filter DVC-tracked files |
| [`dt mv`](#dt-mv) | Move or rename DVC-tracked files, preserving import metadata |
| [`dt pull`](#dt-pull) | Pull DVC-tracked files, handling imports automatically |
| [`dt push`](#dt-push) | Push DVC-tracked files to all configured remotes |
| [`dt import`](#dt-import) | Import DVC-tracked data from other repositories |
| [`dt get`](#dt-get) | Download DVC-tracked data without creating tracking files |
| [`dt update`](#dt-update) | Update imported data by rebuilding `.dir` manifests |
| [`dt summary`](#dt-summary) | Generate project documentation (tree.txt, dag.md) |
| [`dt deps`](#dt-deps) | Inspect dependencies between repositories |
| [`dt cache`](#dt-cache) | Manage external shared caches |
| [`dt remote`](#dt-remote) | Manage remote storage |
| [`dt index`](#dt-index) | Manage the DVC site cache index mirror |
| [`dt tmp`](#dt-tmp) | Manage temporary repository clones |
| [`dt offline`](#dt-offline) | Manage offline mode for compute nodes without internet |
| [`dt worktree`](#dt-worktree) | Manage git worktrees with DVC cache configured |
| [`dt config`](#dt-config) | View and modify configuration settings |
| [`dt du`](#dt-du) | Report disk usage for DVC-tracked files |
| [`dt install`](#dt-install) | Install git hooks and DVC merge driver |
| [`dt uninstall`](#dt-uninstall) | Remove git hooks installed by dt |
| [`dt hook`](#dt-hook) | Manage and run git hook checks |
| [`dt migrate`](#dt-migrate) | Migrate `.dvc` files from v2 to v3 format |
| [`dt status`](#dt-status) | Show DVC pipeline and stage status |
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
dt add <targets...> [-t <threads>] [--no-wait] [-v]
```

Submits `dvc add` to a compute node via qxub with parallel checksum computation. Threads are capped to file count, and CPUs are allocated at 1 per 4 threads. [Full documentation →](add.md)

---

## dt data status

Show changes between the last git commit, DVC files and the workspace.

```bash
dt data status [-t <threads>] [--no-wait] [-v] [DVC_OPTIONS...]
```

Wraps `dvc data status` with parallel checksum computation and optional compute-node delegation. Uses the same resource allocations as `dt add` (`add.max_threads`, `add.mem_per_thread`). Unrecognised options are passed through to `dvc data status`. [Full documentation →](data_status.md)

---

## dt diff

Show differences between versions of DVC-tracked data.

```bash
dt diff [paths...] [--old REV] [--new REV] [--content] [-o FORMAT] [-v]
```

By default shows *which* files changed as a tree (wrapping `dvc diff`). With `--content` and exactly one path, compares actual file content (not just checksums) using format-specific handlers; `--summary` and `--granular` adjust the level of detail. [Full documentation →](diff.md)

---

## dt fetch

Fetch DVC-tracked files into the primary cache from local sources.

```bash
dt fetch [targets...] [-v] [--imports|--urls|--regular] [--network]
dt fetch --csv <file> [--path-col COL]
```

Populates the primary cache with links to files in source caches (reflink → hardlink → symlink → copy, or a single method with `--cache-type`). For repo imports, clones the source repo to find a locally-accessible cache. [Full documentation →](fetch.md)

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
dt pull [targets...] [-v] [--update] [--no-network]
dt pull --csv <file> [--path-col COL]
```

Equivalent to `dt fetch` followed by `dvc checkout`. Imports and local-remote data are fetched through `dt fetch`; data needing network access falls back to the non-mutating `dvc fetch`. Use `--update` to opt in to the mutating import re-resolution (`dvc update`, which rewrites the `.dvc` file). [Full documentation →](pull.md)

---

## dt push

Push DVC-tracked files to all configured remotes.

```bash
dt push [targets...] [options]
```

Pushes to every remote configured at project or local scope, sequentially, so copies stay redundant. With `-w/--workers N` the push is partitioned by hash prefix and distributed across N compute nodes via qxub. [Full documentation →](push.md)

---

## dt import

Import DVC-tracked data from other repositories using local caches.

```bash
dt import <repository> <path> [-o <output>] [--no-checkout]
```

Imports files without network storage access by using locally-accessible caches. If no locally-accessible source cache is found, it falls back to `dvc import` (disable with `--no-dvc-import-fallback`). [Full documentation →](import.md)

---

## dt get

Download DVC-tracked data without creating tracking files.

```bash
dt get <repository> <path> [-o <output>] [--rev REV] [--link TYPES] [-j N] [-f] [-v]
dt get <repository> --csv <file> [--path-col COL] [-o <dir>]
```

The `dvc get` counterpart to `dt import`: materialises data and writes no `.dvc` file, for handing a subset of a dataset to someone outside the group. A path may name a subdirectory *inside* a tracked directory output, so you can take part of a large dataset without transferring all of it. With `--csv` the source is cloned once and every row resolved against that clone, rather than re-cloning per path. [Full documentation →](get.md)

---

## dt update

Update imported data by rebuilding `.dir` manifests.

```bash
dt update [targets...] [--rev REV] [--rebuild] [--force] [--status] [--dry] [-v]
```

Rebuilds `.dir` manifests for repo imports whose directory manifest is missing or stale, so `dt fetch` can populate the cache correctly. With no options it upgrades to HEAD only when the upstream data is unchanged; `--rebuild` fixes the `.dir` at the locked rev, `--force` accepts upstream changes, and `--rev` pins to a specific revision. [Full documentation →](update.md)

---

## dt summary

Generate project documentation files.

```bash
dt summary [-o <dir>] [--tree-only] [--dag-only] [--repo-dag]
```

Creates tree.txt (DVC-tracked file listing) and dag.md (pipeline DAG in mermaid format) in `docs/` by default. `--repo-dag` additionally writes repo-dag.md, the graph of imports between repos; it is opt-in because it clones every upstream repo. [Full documentation →](summary.md)

---

## dt deps

Inspect dependencies between repositories.

| Subcommand | Description |
|------------|-------------|
| `dt deps list` | List the repositories this repo imports from |
| `dt deps graph` | Build the recursive dependency graph between repos |
| `dt deps index` | Build/refresh the org-wide import index |
| `dt deps downstream` | Show which repos import FROM this one |
| `dt deps gaps` | Report repos the graph could not resolve |

Where `dvc dag` shows the graph of stages inside one repo, `dt deps` shows the graph of imports between repos, collapsing thousands of individual imports into one line per source repo. The graph may contain cycles, which are reported rather than treated as an error. [Full documentation →](deps.md)

---

## dt cache

Manage external shared caches.

| Subcommand | Description |
|------------|-------------|
| `dt cache init` | Set up an external shared cache with proper permissions |
| `dt cache rm` | Remove cached files for specified targets |
| `dt cache clean` | Remove abandoned .tmp files from interrupted transfers |
| `dt cache perms` | Check/repair group-writable permissions on a shared cache |
| `dt cache validate` | Verify cached blobs against the MD5 implied by their path |

[Full documentation →](cache.md)

---

## dt remote

Manage remote storage.

| Subcommand | Description |
|------------|-------------|
| `dt remote init` | Set up remote storage with SSH and local access methods |
| `dt remote list [repo]` | List DVC remotes (optionally from a remote repository) |
| `dt remote status` | Report location, accessibility, layout and archive state of remotes |
| `dt remote clean` | Remove abandoned .tmp files from interrupted transfers |
| `dt remote perms` | Check/repair permissions (sticky bit on by default) |
| `dt remote verify` | Re-hash every blob and report corrupt/incomplete objects |
| `dt remote quarantine` | Move bad blobs (and their `.dir`) aside so `dvc push` re-uploads them |
| `dt remote fsck` | Find symlinked blobs in a remote and optionally repair them |
| `dt remote copy` | Duplicate a remote's blob tree to a new path |
| `dt remote move` | Relocate a remote and repoint `.dvc/config` |
| `dt remote archive create <name>` | Archive a remote (stage + deposit in one go) |
| `dt remote archive stage <name>` | Build inner tarballs in staging (compute-node phase) |
| `dt remote archive deposit <name>` | Upload staged tarballs to backend (data-mover phase) |
| `dt remote archive list` | List archives recorded under `.dt/archives/` |
| `dt remote archive verify <name>` | Verify an archive against its manifest |
| `dt remote archive restore <name>` | Restore content from an archive (full / prefix / single object) |
| `dt remote archive prune <name>` | Delete the on-disk remote once its archive is verified |
| `dt remote archive destroy <name>` | Delete an archive copy from the backend (does NOT touch source) |
| `dt remote archive registry list` | List archives across all projects (central register) |
| `dt remote archive registry sync` | Refresh the central register from project manifests |

[Full documentation →](remote.md) · [Archive reference →](archive.md)

---

## dt index

Manage the DVC site cache index mirror.

| Subcommand | Description |
|------------|-------------|
| `dt index status` | Show local index and mirror status |
| `dt index pull` | Pull the index from the shared mirror to local |
| `dt index push` | Push the local index to the shared mirror |
| `dt index build` | Build the index from cache filenames (no re-hashing) |
| `dt index set` | Set `core.site_cache_dir` for the current repo |
| `dt index migrate` | Move `core.site_cache_dir`, copying its current contents |
| `dt index cache status` | Show status of the local cache index (SQLite OID index) |
| `dt index cache rebuild` | Rebuild the cache index by scanning the filesystem |

The mirror location is set with `dt config set index.mirror_root <path>`. [Full documentation →](index.md)

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

## dt offline

Manage offline mode for compute nodes without internet.

| Subcommand | Description |
|------------|-------------|
| `dt offline enable` | Redirect git URLs to local temporary clones and SSH remotes to local paths |
| `dt offline disable` | Remove those redirects and restore the original URLs |
| `dt offline status` | Show which clones and remotes can be served locally |

Enable this on a login node (which has internet) after running `dt tmp clone`, so DVC operations submitted to a compute node resolve locally. [Full documentation →](offline.md)

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
| `dt config add <key> <value>` | Append a value to a list-valued setting (e.g. `remote.root`) |
| `dt config remove <key> <value>` | Remove a value from a list-valued setting |
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
| `-s, --summarize` | Show only the total (equivalent to `-d 0`) |
| `--inodes` | Count number of files instead of bytes |
| `-c, --total` | Show a grand total line at the end |
| `--cached` | Show only cached sizes (default is both cached and expected) |
| `--expected` | Show only expected sizes, taken from `.dvc` metadata |

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
| `dt auth setup` | Set up SSH keys, SSH config, and credentials in one step |
| `dt auth credentials` | Manage DVC remote credentials in the secret manager |
| `dt auth teams` | Manage GitHub team access for repositories |
| `dt auth grant` | Grant a user access to a resource *(planned, not implemented)* |

Discovers endpoints from DVC remotes, `.dvc` import files, dt config, and git remotes. Supports filesystem, SSH, S3-compatible (including CloudFlare R2), GCS, HTTP, and git endpoints. [Full documentation →](auth.md)

---

## dt install

Install git hooks and DVC merge driver.

```bash
dt install [--force] [-v]
```

Writes hook scripts to `.git/hooks/` that delegate to `dt hook run` and registers the DVC merge driver. No config file is written — the default checks are built in, and you override them with `dt config set hooks.*`. [Full documentation →](install.md)

---

## dt uninstall

Remove git hooks installed by dt.

```bash
dt uninstall [-v]
```

Only removes hooks that were installed by `dt install`. Foreign hooks are left untouched. [Full documentation →](install.md)

---

## dt hook

Manage and run git hook checks.

| Subcommand | Description |
|------------|-------------|
| `dt hook list` | Show configured checks for each hook |
| `dt hook run <name>` | Run all enabled checks for a hook |
| `dt hook run-check <hook> <check>` | Run or submit a single check |
| `dt hook results` | Show or clear async check results |
| `dt hook check large-files` | Check staged files against size limit |

Checks are configured via `dt config hooks.*` keys and can run synchronously (blocking git) or asynchronously via qxub on HPC systems. [Full documentation →](install.md)

---

## dt migrate

Migrate `.dvc` files from v2 to v3 format.

```bash
dt migrate [targets...] [--dry] [--find-v2] [--cache-root PATH] [-v]
```

Rewrites `.dvc` files in place to the v3 format (explicit `hash` field, plain md5 instead of md5-dos2unix), including the imports that `dvc cache migrate --dvc-files` trips over. Run `dvc cache migrate` first to relocate the cache data itself. [Full documentation →](migrate.md)

---

## dt status

Show DVC pipeline and stage status.

```bash
dt status [--imports] [-v] [DVC_OPTIONS...]
```

Wraps `dvc status`. Pulls the index archive first only if `index.auto_sync` is on (off by default). Pass `--imports` to also check import freshness via `dt update --status`. Unrecognised options are passed through to `dvc status`.

---

## dt doctor

Diagnose common setup issues and verify configuration.

```bash
dt doctor [-v]
```

Checks Git and DVC installation, GitHub CLI availability, SSH key and GitHub authentication, and that the configured `cache.root` and `remote.root` exist and are accessible. `-v` adds network, local-remote and endpoint access checks plus `dvc doctor` output. [Full documentation →](doctor.md)

---

## See Also

- [DVC Basics](DVC_basics.md) - Introduction to DVC concepts
- [Configuration Options](config_options.md) - Available configuration settings
- [Configuration Scopes](config_scopes.md) - How configuration hierarchy works
