# Integration Tests Checklist

This document tracks integration tests for each `dt` CLI subcommand.
Integration tests run real git/DVC commands against actual repositories.

---

## Test Environment Requirements

| Requirement | Purpose |
|-------------|---------|
| git | All tests |
| DVC | Most tests |
| Network | Clone, import tests |
| SSH keys | GitHub clone tests || qxub | HPC parallel operations (add --qxub, pull --parallel, push --parallel) |
---

## Commands

### `dt add`

**File:** `test_add_cmd.py`

- [x] Add single file to DVC tracking (worker mode)
  - [x] Creates .dvc file
  - [x] Adds to .gitignore
- [x] Add directory to DVC tracking
  - [x] Creates .dvc file with .dir hash
- [x] Add with --threads option
- [x] Add with --verbose option
- [x] Error: add non-existent file
- [x] Error: add outside repository
- [x] Add already tracked file
- [x] Add with --qxub option (HPC)
- [x] Add with --no-wait option

---

### `dt cache`

**File:** `test_cache_cmd.py`

#### `dt cache init`
- [x] Initialize with --cache-path option
- [x] Idempotent (can run multiple times)
- [x] Help shows usage

#### `dt cache rm`
- [x] Remove cache files (dry-run mode)
- [x] Requires --force or --remote
- [x] Force mode removes files
- [x] Handles non-existent targets

#### `dt cache validate`
- [x] Validate all cache files
- [x] Validate specific target
- [x] JSON output format
- [x] Verbose output
- [x] Error outside repository

---

### `dt clone`

**File:** `test_clone_cmd.py`

- [x] Clone with full SSH URL
  - [x] `dt clone git@github.com:swarbricklab/dt-test-fixtures`
- [x] Clone with HTTPS URL
  - [x] `dt clone https://github.com/swarbricklab/dt-test-fixtures`
- [x] Clone with short name (uses config owner)
  - [x] `dt clone dt-test-fixtures`
- [x] Clone with --owner flag
  - [x] `dt clone dt-test-fixtures --owner swarbricklab`
- [x] Clone with --shallow flag
  - [x] Repository has truncated history
- [x] Clone with --no-submodules flag
  - [x] Submodules not initialized
- [x] Clone with custom path
  - [x] `dt clone dt-test-fixtures ./custom-dir`
- [x] Error: clone non-existent repo
- [x] Error: clone to existing directory

---

### `dt config`

**File:** `test_config_cmd.py`

#### `dt config list`
- [x] List all config values
- [x] List with --scope (user, project, local, system)
- [x] Show sources with --show-origin

#### `dt config get`
- [x] Get existing key value
- [x] Get nested key (dot notation)
- [x] Returns error for missing key

#### `dt config set`
- [x] Set value at default scope (project)
- [x] Set value at specific scope
- [x] Set nested key creates structure
- [x] YAML value parsing (lists, bools, integers)

#### `dt config unset`
- [x] Remove existing key
- [x] Clean up empty parent dicts
- [x] Error for missing key

#### `dt config path`
- [x] Shows all scope paths
- [x] Shows single scope with flag
- [x] Shows existence indicator

---

### `dt diff`

**File:** `test_diff_cmd.py`

- [x] Diff help shows usage
- [x] Diff CSV file between revisions
- [x] Diff with workspace (uncommitted changes)
- [x] Diff between two commits
- [x] Output format -o option
- [x] Verbose output
- [x] Error: diff untracked file
- [x] Error: diff outside repository
- [x] Diff with HEAD reference
- [x] Diff with detached HEAD

---

### `dt doctor`

**File:** `test_doctor_cmd.py`

- [x] Doctor help shows usage
- [x] Check git installation
- [x] Check DVC installation
- [x] Check GitHub CLI (optional)
- [x] Check SSH keys exist
- [x] Check dt version display
- [x] Check config display
- [x] Check quick mode (--quick)
- [x] Check JSON output (--json)
- [x] Verbose output (--verbose)
- [x] Error outside repository

---

### `dt du`

**File:** `test_du_cmd.py`

- [x] Du help shows usage
- [x] Report disk usage for all tracked files
- [x] Report for specific targets
- [x] Human-readable output (--human)
- [x] Count files (--count)
- [x] Aggregate by depth (--max-depth)
- [x] JSON output (--json)
- [x] CSV output (--csv)
- [x] Filter by type (--files-only, --dirs-only)
- [x] Error outside repository
- [x] Sort by size (--sort)

---

### `dt fetch`

**File:** `test_fetch_cmd.py`

- [x] Fetch all tracked files to cache
- [x] Fetch specific targets
- [x] Fetch import file (uses source cache)
- [ ] --checkout runs dvc checkout after
- [x] Progress display (verbose output)
- [x] Error: fetch non-existent target
- [x] Error: fetch invalid .dvc file
- [x] Error: fetch directory instead of .dvc file
- [x] --no-refresh option
- [x] --no-index-sync option
- [x] Regular file suggests dvc fetch
- [x] File already in cache reports success
- [x] Fetch + checkout workflow

---

### `dt find`

**File:** `test_find_cmd.py`

- [x] Find help shows usage
- [x] Find by full MD5 hash
- [x] Find by partial hash (prefix)
- [x] JSON output format (--json)
- [x] Verbose output (--verbose)
- [x] --dvc-file option
- [x] --cache-path option
- [x] Error: hash too short
- [x] Error: hash not found
- [x] Error outside repository

---

### `dt history`

**File:** `test_history_cmd.py`

- [x] History help shows usage
- [x] Show version history of tracked file
- [x] Limit results (--limit)
- [x] Filter by date (--since)
- [x] JSON output format (--json)
- [x] Verbose output (--verbose)
- [x] Error: history of untracked file
- [x] Error: history outside repository
- [x] History with --all option
- [x] History since future date (returns error)

---

### `dt import`

**File:** `test_import_cmd.py`

- [x] Import help shows usage
- [x] Import requires repository argument
- [x] Import with --output option
- [x] Import with --no-checkout option
- [x] Import with --owner option
- [x] Error: import invalid repository
- [x] Error: import outside repository
- [x] Import --no-checkout creates .dvc file only
- [x] Import with --no-refresh option
- [x] Import from public GitHub repo (network)

---

### `dt index`

**File:** `test_index_cmd.py`

#### `dt index status`
- [x] Status help shows usage
- [x] Status shows not configured message
- [x] Status in DVC repo
- [x] Status outside repo (returns 0)

#### `dt index pull`
- [x] Pull help shows usage
- [x] Pull with --dry-run option
- [x] Pull when not configured

#### `dt index push`
- [x] Push help shows usage
- [x] Push with --dry-run option

- [x] Index group help shows subcommands
- [x] Index status with mirror configured

---

### `dt init`

**File:** `test_init_cmd.py`

- [x] Initialize new project (git + DVC)
- [x] Initialize with --no-git (DVC only)
- [x] Initialize with --no-dvc (git only)
- [x] Install DVC git hooks
- [x] Create .dt directory
- [x] Configure external cache (if cache.root set)
- [x] Configure remote (if remote.root set)
- [x] Check GitHub remote and suggest creation
- [x] Error: already initialized

---

### `dt ls`

**File:** `test_ls_cmd.py`

- [x] Ls help shows usage
- [x] List all tracked files
- [x] List specific path
- [x] List with revision (--rev)
- [x] Recursive listing (-R)
- [x] Long format (-l) with size and type
- [x] Show hash (--show-hash)
- [x] JSON output (--json)
- [x] Filter by type (--files-only)
- [x] Filter by type (--dirs-only)
- [x] Error outside repository
- [x] Empty repository returns gracefully

---

### `dt mv`

**File:** `test_mv_cmd.py`

- [x] Mv help shows usage
- [x] Move/rename tracked file
  - [x] Updates .dvc file
  - [x] Runs dvc mv
- [x] Move to directory
- [x] Move imported file (preserves deps section)
- [x] Verbose output (--verbose)
- [x] Error: move non-existent source
- [x] Destination exists (overwrites)
- [x] Error: move outside repository
- [x] Move untracked file (uses regular mv)

---

### `dt offline`

**File:** `test_offline_cmd.py`

#### `dt offline enable`
- [x] Enable offline mode
- [x] Set git URL redirects
- [x] Override DVC remote URLs
- [x] Save state to config.local.yaml

#### `dt offline disable`
- [x] Disable offline mode
- [x] Remove git URL redirects
- [x] Clear state

#### `dt offline status`
- [x] Show offline mode status
- [x] List active redirects
- [x] JSON output (--json)

- [x] Offline group help shows subcommands
- [x] Error outside repository
- [x] Full enable/status/disable workflow
- [x] Multiple enable is idempotent
- [x] Disable when not enabled is safe

---

### `dt pull`

**File:** `test_pull_cmd.py`

- [x] Pull help shows usage
- [x] Pull all tracked files
- [x] Pull specific targets
- [x] Pull imported files (uses fetch + checkout)
- [x] Pull regular files (uses dvc checkout)
- [x] --force option (overwrite local changes)
- [x] --jobs option for parallelism
- [x] --no-refresh option
- [x] --no-index-sync option
- [x] Progress display (--verbose)
- [x] Error: pull non-existent target
- [x] Error: pull outside repository
- [x] Pull with --update creates missing files

---

### `dt push`

**File:** `test_push_cmd.py`

- [x] Push help shows usage
- [x] Push to default remote
- [x] Push to specific remote (--remote)
- [x] Push specific targets
- [x] Push to all remotes (--all)
- [x] Push with --jobs for parallelism
- [x] Push with --verbose for progress
- [x] Push with --no-index-sync
- [x] Error: push outside repository
- [x] Error: push when no remote configured
- [x] Push with local remote (end-to-end)
- [x] Push already synced files

---

### `dt remote`

**File:** `test_remote_cmd.py`

#### `dt remote init`
- [x] Remote init help shows usage
- [x] Initialize remote with path
- [x] Initialize remote is idempotent
- [x] Initialize remote sets group permissions

#### `dt remote list`
- [x] Remote list help shows usage
- [x] List configured remotes
- [x] List shows no remotes message when none configured

- [x] Remote group help shows subcommands
- [x] Error: remote init outside repository

---

### `dt summary`

**File:** `test_summary_cmd.py`

#### `dt summary tree`
- [x] Tree help shows usage
- [x] Generate tree.txt with dvc list --tree

#### `dt summary dag`
- [x] Dag help shows usage
- [x] Generate dag.md with dvc dag --md

#### `dt summary all`
- [x] All help shows usage
- [x] Generate both tree and dag

- [x] Summary group help shows subcommands
- [x] Error: summary outside repository
- [x] Custom output directory (--output-dir)

---

### `dt tmp`

**File:** `test_tmp_cmd.py`

#### `dt tmp clone`
- [x] Clone help shows usage
- [x] Clone requires repository argument
- [x] Clone requires dt init
- [x] Clone with --no-refresh option
- [x] Clone with --owner option
- [x] Clone from GitHub (network)

#### `dt tmp list`
- [x] List help shows usage
- [x] List empty shows no clones
- [x] List shows clones after clone

#### `dt tmp clean`
- [x] Clean help shows usage
- [x] Clean specific repo
- [x] Clean all clones

- [x] Tmp group help shows subcommands

---

### `dt worktree`

**File:** `test_worktree_cmd.py`

#### `dt worktree add`
- [x] Add help shows usage
- [x] Create worktree with existing branch
- [x] Create worktree with new branch (-b)
- [x] Configure DVC cache in worktree

#### `dt worktree list`
- [x] List help shows usage
- [x] List shows main worktree
- [x] List shows multiple worktrees

#### `dt worktree remove`
- [x] Remove help shows usage
- [x] Remove worktree

- [x] Worktree group help shows subcommands
- [x] Error: add to existing path
- [x] Error: worktree outside git repo

---

## Testing Strategy

### Local Tests (No Network)

Tests that can run without network access:

- `dt add` - Local file operations
- `dt cache` - Cache management
- `dt config` - Configuration
- `dt doctor` - Diagnostics (partial)
- `dt du` - Disk usage
- `dt find` - Hash lookup
- `dt history` - Git log parsing
- `dt init` - Project initialization
- `dt ls` - List files
- `dt mv` - Move files
- `dt pull` - With local remote
- `dt push` - With local remote
- `dt remote` - Remote management
- `dt summary` - Generate docs
- `dt worktree` - Git worktrees

### Network Tests

Tests requiring network access:

- `dt clone` - Clone from GitHub
- `dt import` - Import from remote repo
- `dt tmp clone` - Clone to temp dir
- `dt offline` - URL redirects

### Test Fixtures

| Fixture | Use Case |
|---------|----------|
| `git_repo` | Basic git operations |
| `dvc_repo` | DVC operations |
| `dvc_repo_with_files` | File tracking tests |
| `dvc_repo_with_cache` | Cache operations |
| `dvc_repo_with_remote` | Push/pull tests |
| `dt_test_fixtures_clone` | Import/fetch tests |
| `run_dt` | CLI command execution |

---

## Progress

| Command | Tests | Status |
|---------|-------|--------|
| add | 11 | ✅ Complete |
| cache | 15 | ✅ Complete |
| clone | 18 | ✅ Complete |
| config | 32 | ✅ Complete |
| diff | 11 | ✅ Complete |
| doctor | 11 | ✅ Complete |
| du | 12 | ✅ Complete |
| fetch | 13 | ✅ Complete |
| find | 11 | ✅ Complete |
| history | 11 | ✅ Complete |
| import | 10 | ✅ Complete |
| index | 11 | ✅ Complete |
| init | 21 | ✅ Complete |
| ls | 12 | ✅ Complete |
| mv | 9 | ✅ Complete |
| offline | 25 | ✅ Complete |
| pull | 21 | ✅ Complete |
| push | 16 | ✅ Complete |
| remote | 9 | ✅ Complete |
| summary | 10 | ✅ Complete |
| tmp | 13 | ✅ Complete |
| worktree | 12 | ✅ Complete |

**Total:** 313 tests implemented
