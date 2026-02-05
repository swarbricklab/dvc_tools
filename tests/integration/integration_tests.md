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

- [ ] Add single file to DVC tracking
  - [ ] Creates .dvc file
  - [ ] Adds to .gitignore
  - [ ] File moves to cache (symlink left)
- [ ] Add directory to DVC tracking
  - [ ] Creates .dvc file with .dir hash
  - [ ] All files cached
- [ ] Add with --threads option
  - [ ] Sets checksum_jobs temporarily
  - [ ] Restores original value after
- [ ] Add with custom DVC args (--to-remote, etc.)
- [ ] Error: add non-existent file
- [ ] Error: add file already tracked

---

### `dt cache`

**File:** `test_cache_cmd.py`

#### `dt cache init`
- [ ] Initialize new external cache
  - [ ] Creates files/md5/XX directory structure (256 dirs)
  - [ ] Sets group permissions
- [ ] Configure repo to use external cache
  - [ ] Sets cache.dir in local config
- [ ] Skip if cache already exists

#### `dt cache info`
- [ ] Display cache location and size
- [ ] Show file count

#### `dt cache validate`
- [ ] Validate cache files against stored hashes
- [ ] Report corrupted files
- [ ] --fix mode removes corrupted files

#### `dt cache remove`
- [ ] Remove cache files for targets
  - [ ] Dry-run mode
  - [ ] Blocks if not in remote (without --force)
  - [ ] Force mode

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

- [ ] Diff CSV file between revisions
  - [ ] Uses daff when available
  - [ ] Shows row-level changes
- [ ] Diff with workspace (uncommitted changes)
- [ ] Diff between two commits
- [ ] Output formats: terminal, html, json
- [ ] Error: diff untracked file
- [ ] Error: unsupported file format (fallback handler)

---

### `dt doctor`

**File:** `test_doctor_cmd.py`

- [ ] Check git installation
- [ ] Check DVC installation
- [ ] Check GitHub CLI (optional)
- [ ] Check SSH keys exist
- [ ] Check GitHub SSH authentication
- [ ] Check cache.root configuration
- [ ] Check remote.root configuration
- [ ] Run `dvc doctor` integration
- [ ] Display dt version
- [ ] Display config with sources

---

### `dt du`

**File:** `test_du_cmd.py`

- [ ] Report disk usage for all tracked files
- [ ] Report for specific targets
- [ ] Show cached vs total size
- [ ] Count files (--count)
- [ ] Aggregate by depth (--max-depth)
- [ ] Human-readable output (--human)
- [ ] Machine output (--bytes)

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

- [ ] Find by full MD5 hash
- [ ] Find by partial hash (prefix)
- [ ] Find with --expand-dirs (show contents)
- [ ] JSON output format
- [ ] Verbose output (show file info)
- [ ] Error: hash too short

---

### `dt history`

**File:** `test_history_cmd.py`

- [ ] Show version history of tracked file
- [ ] Limit results (--limit)
- [ ] Filter by date (--since)
- [ ] Show hash changes
- [ ] JSON output format
- [ ] Error: history of untracked file

---

### `dt import`

**File:** `test_import_cmd.py`

- [ ] Import file from remote repo
  - [ ] `dt import https://github.com/swarbricklab/dt-test-registry data/file.csv`
- [ ] Import directory from remote repo
- [ ] Import with --rev (specific revision/tag)
- [ ] Import with --checkout (checkout after import)
- [ ] Creates .dvc file with deps.repo section
- [ ] Populates cache from source
- [ ] Error: import non-existent path

---

### `dt index`

**File:** `test_index_cmd.py`

#### `dt index pull`
- [ ] Sync index from mirror
- [ ] Handle lock contention

#### `dt index push`
- [ ] Sync index to mirror
- [ ] Create mirror if missing

#### `dt index status`
- [ ] Show sync status
- [ ] Show lock status

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

- [ ] List all tracked files
- [ ] List specific path
- [ ] List with revision (--rev)
- [ ] Recursive listing (-R)
- [ ] Filter by pattern (--pattern)
- [ ] Filter by regex (--regex)
- [ ] Filter by size (--min-size, --max-size)
- [ ] Filter by type (--files-only, --dirs-only)
- [ ] Filter by hash prefix (--hash)
- [ ] Long format (-l) with size and type
- [ ] Show hash (--show-hash)
- [ ] JSON output

---

### `dt mv`

**File:** `test_mv_cmd.py`

- [ ] Move/rename tracked file
  - [ ] Updates .dvc file
  - [ ] Runs dvc mv
- [ ] Move imported file
  - [ ] Preserves deps section
  - [ ] Updates path in deps
- [ ] Move to directory
- [ ] Verbose output
- [ ] Error: move non-tracked file
- [ ] Error: destination exists

---

### `dt offline`

**File:** `test_offline_cmd.py`

#### `dt offline enable`
- [ ] Enable offline mode
- [ ] Set git URL redirects to temp clones
- [ ] Override DVC remote URLs
- [ ] Save state to config.local.yaml

#### `dt offline disable`
- [ ] Disable offline mode
- [ ] Remove git URL redirects
- [ ] Remove DVC remote overrides
- [ ] Clear state

#### `dt offline status`
- [ ] Show offline mode status
- [ ] List active redirects
- [ ] List temp clones

---

### `dt pull`

**File:** `test_pull_cmd.py`

- [ ] Pull all tracked files
- [ ] Pull specific targets
- [ ] Pull imported files (uses fetch + checkout)
- [ ] Pull regular files (uses dvc checkout)
- [ ] --force option (overwrite local changes)
- [ ] Progress display
- [ ] Error: pull non-existent target

---

### `dt push`

**File:** `test_push_cmd.py`

- [ ] Push to default remote
- [ ] Push to specific remote (--remote)
- [ ] Push specific targets
- [ ] Push to all remotes (--all)
- [ ] Progress display
- [ ] Error: no remotes configured

---

### `dt remote`

**File:** `test_remote_cmd.py`

#### `dt remote init`
- [ ] Initialize remote storage
- [ ] Create directory structure
- [ ] Configure DVC remotes (SSH + local)

#### `dt remote list`
- [ ] List configured remotes
- [ ] Show default remote
- [ ] Parse remote URLs

---

### `dt summary`

**File:** `test_summary_cmd.py`

#### `dt summary tree`
- [ ] Generate tree.txt with dvc list --tree

#### `dt summary dag`
- [ ] Generate dag.md with dvc dag --md

#### `dt summary all`
- [ ] Generate both tree and dag

- [ ] Custom output directory (--output-dir)
- [ ] Custom filenames

---

### `dt tmp`

**File:** `test_tmp_cmd.py`

#### `dt tmp clone`
- [ ] Clone repo to .dt/tmp/clones/
- [ ] Sparse clone (no checkout)
- [ ] Refresh existing clone
- [ ] Use cached clone

#### `dt tmp list`
- [ ] List cached clones

#### `dt tmp clean`
- [ ] Remove specific clone
- [ ] Remove all clones

---

### `dt worktree`

**File:** `test_worktree_cmd.py`

#### `dt worktree add`
- [ ] Create worktree with existing branch
- [ ] Create worktree with new branch (-b)
- [ ] Configure DVC cache in worktree
- [ ] Initialize submodules

#### `dt worktree list`
- [ ] List worktrees
- [ ] Show branch and head

#### `dt worktree remove`
- [ ] Remove worktree
- [ ] Force removal

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
| add | 0 | ⬜ Not started |
| cache | 0 | ⬜ Not started |
| clone | 15 | ✅ Complete |
| config | 24 | ✅ Complete |
| diff | 0 | ⬜ Not started |
| doctor | 0 | ⬜ Not started |
| du | 0 | ⬜ Not started |
| fetch | 12 | ✅ Complete |
| find | 0 | ⬜ Not started |
| history | 0 | ⬜ Not started |
| import | 0 | ⬜ Not started |
| index | 0 | ⬜ Not started |
| init | 16 | ✅ Complete |
| ls | 0 | ⬜ Not started |
| mv | 0 | ⬜ Not started |
| offline | 0 | ⬜ Not started |
| pull | 0 | ⬜ Not started |
| push | 0 | ⬜ Not started |
| remote | 0 | ⬜ Not started |
| summary | 0 | ⬜ Not started |
| tmp | 0 | ⬜ Not started |
| worktree | 0 | ⬜ Not started |

**Total:** 67 tests implemented
