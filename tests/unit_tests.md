# Unit Tests Checklist

This document tracks the unit tests needed for the `dt` package. Tests are grouped by module.

---

## `dt.utils`

✅ **Implemented in `tests/test_utils.py`**

### Formatting Utilities
- [x] `format_size` - format bytes as human-readable string
  - [x] Test with human_readable=True (various sizes: B, KB, MB, GB, TB)
  - [x] Test with human_readable=False (returns raw bytes string)
  - [x] Test edge cases: 0 bytes

### DVC Cache Utilities
- [x] `get_cache_dir` - get primary DVC cache directory
  - [x] Test returns Path when in DVC repo
  - [x] Test returns None when not in DVC repo
- [x] `hash_to_cache_path` - convert hash to cache file path
  - [x] Test with regular hash
  - [x] Test with .dir suffix hash
- [ ] `oid_to_path` - convert hash using DVC internals
  - [ ] Test with valid hash
  - [ ] Test returns None when cache unavailable
- [ ] `collect_tracked_entries` - collect tracked file entries
  - [ ] Test with no targets (all files)
  - [ ] Test with specific targets
  - [ ] Test with push=True vs push=False

### DVC File Utilities
- [ ] `load_dvc_file` - load .dvc file using DVC parser
  - [ ] Test with valid .dvc file (use `sample_dvc_files` fixture)
  - [ ] Test raises DVCFileError for invalid file
- [x] `parse_dvc_file` - parse .dvc file as YAML
  - [x] Test with valid .dvc file
  - [x] Test raises DVCFileError for invalid file
  - [x] Test with import .dvc file
- [ ] `is_repo_import` - check if .dvc file is an import
  - [ ] Test with import .dvc file (`imported/file.csv.dvc` pattern from dt-test-fixtures)
  - [ ] Test with regular .dvc file (`single_file/data.csv.dvc` pattern)
- [ ] `get_import_info` - extract import information
  - [ ] Test returns dict with url, rev, path for imports (url: dt-test-registry)
  - [ ] Test returns None for non-imports

### Project Utilities
- [x] `get_project_name` - get current directory name
  - [x] Test returns correct directory name
- [x] `check_command` - check if command is available
  - [x] Test with existing command
  - [x] Test raises DependencyError for missing command
  - [x] Test install hint in error message
- [x] `check_dvc` - check DVC availability
  - [x] Test passes when DVC installed
  - [x] Test raises DependencyError when DVC missing
- [x] `check_git` - check git availability
  - [x] Test passes when git installed
  - [x] Test raises DependencyError when git missing
- [x] `update_gitignore` - add pattern to .gitignore
  - [x] Test adds new pattern
  - [x] Test returns False for existing pattern
  - [x] Test creates .gitignore if missing
  - [x] Test appends to existing .gitignore
  - [x] Test custom gitignore path
- [x] `set_group_writable` - set group write permissions
  - [x] Test sets correct permissions
  - [x] Test sets setgid by default
- [x] `create_md5_subdirs` - create files/md5 subdirectory structure
  - [x] Test creates 256 subdirectories (00-ff)

### Project Root Discovery
- [x] `find_dvc_root` - find DVC project root
  - [x] Test finds root when in DVC project
  - [x] Test returns None when not in DVC project
- [x] `find_git_root` - find git repository root
  - [x] Test finds root when in git repo
  - [x] Test returns None when not in git repo
- [x] `find_project_root` - find project root (git > DVC > cwd)
  - [x] Test returns cwd as fallback
  - [x] Test uses start_path parameter

### Git Revision Utilities
- [ ] `get_hash_at_rev` - get DVC hash at specific revision
  - [ ] Test with valid path and revision
  - [ ] Test returns None for non-tracked path
- [x] `get_candidate_commits` - get commits that modified DVC metadata
  - [x] Test returns list of commit hashes in git repo
  - [x] Test returns empty list outside git repo
  - [x] Test with limit parameter
- [x] `get_commit_info` - get metadata for git commit
  - [x] Test returns dict with hash, date, message, author
  - [x] Test with invalid commit returns partial info

---

## `dt.config`

✅ **Implemented in `tests/test_config.py`** (CLI-level tests covering underlying functions)

**Fixtures:** Use `isolated_config` and `temp_dirs` from `test_config.py`

- [x] `get_config_paths` - get config file paths for each scope
  - [x] Test returns dict with all scopes (use `isolated_config` fixture)
  - [x] Test respects XDG environment variables (monkeypatch XDG_CONFIG_HOME)
- [x] `load_config` - load merged configuration
  - [x] Test merges configs in precedence order (use `isolated_config`)
- [x] `load_scope_config` - load config from single scope
  - [x] Test returns dict for existing config
  - [x] Test returns empty dict for missing config
- [x] `get_value` - get config value by key
  - [x] Test with existing key
  - [x] Test returns default for missing key
  - [x] Test with nested keys (dot-separated)
- [x] `set_value` - set config value at scope
  - [x] Test creates nested structure
  - [x] Test parses YAML values correctly
  - [x] Test at different scopes (user, project, local, system)
- [x] `unset_value` - remove config value
  - [x] Test removes existing key
  - [x] Test cleans up empty parent dicts
  - [x] Test returns False for missing key
- [ ] `flatten_dict` - flatten nested dict to dot-separated keys
  - [ ] Test with nested structure
  - [ ] Test with flat structure
- [x] `list_config` - list configuration values
  - [x] Test with specific scope
  - [x] Test with merged config
- [x] `list_config_with_sources` - list values with source scope
  - [x] Test returns list of (key, value, scope) tuples
- [ ] `get_list_value` - get list config value merged across scopes
  - [ ] Test merges lists correctly
  - [ ] Test removes duplicates
- [ ] `add_list_value` - add value to list config
  - [ ] Test adds new value
  - [ ] Test returns False for duplicate
- [ ] `remove_list_value` - remove value from list config
  - [ ] Test removes existing value
  - [ ] Test returns False for missing value

---

## `dt.errors`

✅ **Implemented in `tests/test_errors.py`**

- [x] Test all exception classes exist and inherit from DTError
  - [x] `DTError`
  - [x] `CacheError`
  - [x] `CheckoutError`
  - [x] `CloneError`
  - [x] `DependencyError`
  - [x] `DiffError`
  - [x] `DuError`
  - [x] `DVCFileError`
  - [x] `FetchError`
  - [x] `FindError`
  - [x] `HistoryError`
  - [x] `LsError`
  - [x] `HPCError`
  - [x] `ImportError_`
  - [x] `InitError`
  - [x] `MvError`
  - [x] `OfflineError`
  - [x] `PullError`
  - [x] `PushError`
  - [x] `RemoteError`
  - [x] `SummaryError`
  - [x] `TmpError`
  - [x] `WorktreeError`
  - [x] `AddError`
- [x] Test exceptions can be raised and caught
- [x] Test exceptions are caught by base DTError
- [x] Test exception with context (chaining)
- [x] Test specific exception messages

---

## `dt.add`

✅ **Implemented in `tests/test_add.py`**

- [x] `check_qxub` - check if qxub is available
  - [x] Test returns True when available
  - [x] Test returns False when not available
- [x] `count_files` - count files in path
  - [x] Test with single file
  - [x] Test with directory (recursive)
  - [x] Test with empty directory
  - [x] Test with nonexistent path (fallback)
- [x] `get_checksum_jobs` - get core.checksum_jobs setting
  - [x] Test returns int when set
  - [x] Test returns None when not set
  - [x] Test returns None on empty output
  - [x] Test returns None on subprocess error
- [x] `set_checksum_jobs` - set checksum_jobs at local scope
  - [x] Test sets value correctly
- [x] `unset_checksum_jobs` - unset checksum_jobs at local scope
  - [x] Test unsets value
- [x] `add` - add files to DVC tracking
  - [x] Test raises error for no targets
  - [x] Test raises error for invalid thread count
  - [x] Test raises error for excessive thread count
  - [x] Test basic add operation
  - [x] Test with threads sets checksum_jobs
  - [x] Test restores original checksum_jobs
  - [x] Test with dvc_args
- [x] `add_via_qxub` - add via compute node
  - [x] Test raises error when qxub missing
  - [x] Test raises error for no targets
  - [x] Test raises error for invalid thread count
  - [x] Test submits job correctly
  - [x] Test caps threads to file count

---

## `dt.cache`

✅ **Implemented in `tests/test_cache.py`**

**Fixtures:** Use `cache_dirs` from `test_fetch.py` for cache structure tests

- [x] `resolve_cache_path` - resolve cache directory path
  - [x] Test with cache_path override
  - [x] Test constructs from cache_root + name
  - [x] Test uses config cache.root
  - [x] Test raises CacheError when not configured
- [x] `init_cache_structure` - initialize cache directory structure
  - [x] Test creates required directories (files/md5/00-ff)
  - [x] Test creates 256 subdirectories
  - [x] Test sets group permissions
- [x] `configure_dvc_cache` - configure DVC to use cache
  - [x] Test sets cache directory
  - [x] Test raises error on failure
- [x] `init_cache` - initialize external shared cache
  - [x] Test creates new cache
  - [x] Test uses existing cache
- [x] `get_cache_dir` - get primary cache directory
  - [x] Test returns Path when available
  - [x] Test raises CacheError when not available
- [x] `expand_dir_hashes` - expand .dir hashes to file hashes
  - [x] Test expands directory contents
  - [x] Test handles missing .dir file
  - [x] Test preserves non-.dir hashes
- [x] `get_hash_for_path_in_dir` - get hash for file in directory
  - [x] Test finds correct hash
  - [x] Test finds nested file
  - [x] Test returns None when not found
  - [x] Test returns None for missing .dir file
- [x] `check_hashes_in_remote` - check which hashes exist in remote
  - [x] Test returns empty for empty input
  - [x] Test returns all not in remote on import error
- [ ] `collect_hashes_for_targets` - collect hashes for targets
  - [ ] Test returns files, paths, repo_root
- [x] `get_cache_file_info` - get cache file paths and sizes
  - [x] Test returns (hash, path, size) tuples
  - [x] Test returns None size for missing
- [x] `hash_to_cache_path` - convert hash to cache path
  - [x] Test regular hash
  - [x] Test .dir hash
- [ ] `remove_cache_files` - remove cache files for targets
  - [ ] Test dry_run mode
  - [ ] Test actual deletion
  - [ ] Test blocked when files not in remote
  - [ ] Test force mode
- [ ] `compute_file_hash` - compute MD5 hash of file
  - [ ] Test returns correct hash
- [ ] `expected_hash_from_path` - extract expected hash from cache path
  - [ ] Test with regular file
  - [ ] Test with .dir file
- [ ] `validate_cache_file` - validate single cache file
  - [ ] Test returns (True, expected, actual) for valid
  - [ ] Test returns (False, expected, actual) for corrupted
- [ ] `get_parent_dir_hash` - find parent .dir for file hash
  - [ ] Test finds parent
  - [ ] Test returns None when not found
- [ ] `validate_cache` - validate cache files
  - [ ] Test validates all files
  - [ ] Test validates specific targets
  - [ ] Test fix mode deletes corrupted files

---

## `dt.clone`

✅ **Implemented in `tests/test_clone.py`**

- [x] `resolve_repository_url` - resolve repo spec to URL
  - [x] Test with full URL returns as-is
  - [x] Test with HTTPS URL returns as-is
  - [x] Test with short name and owner argument
  - [x] Test with short name and config owner
  - [x] Test raises CloneError when owner missing
- [x] `extract_repo_name` - extract repo name from URL
  - [x] Test with SSH URL format
  - [x] Test with HTTPS URL format
  - [x] Test removes .git suffix
  - [x] Test handles trailing slash
  - [x] Test URL without .git suffix
- [x] `clone_repository` - clone DVC repository
  - [x] Test basic clone
  - [x] Test with shallow option
  - [x] Test with no_submodules
  - [x] Test clone failure raises error
  - [x] Test custom path

---

## `dt.diff`

### Handler Registration
- [ ] `register_handler` - decorator to register diff handler
  - [ ] Test adds handler to registry
- [ ] `get_handler` - get handler for file path
  - [ ] Test returns correct handler for extension
  - [ ] Test returns None for unsupported extension
- [ ] `list_handlers` - list registered handlers
  - [ ] Test returns list of handler info

### Built-in Handlers
- [ ] `CSVHandler.diff` - diff CSV files
  - [ ] Test with daff available
  - [ ] Test raises DiffError when daff missing
  - [ ] Test different output formats
- [ ] `FallbackHandler.diff` - fallback for unsupported formats
  - [ ] Test returns metadata comparison
  - [ ] Test JSON output format

### Main Functions
- [ ] `diff` - compute diff between versions
  - [ ] Test with old_rev and workspace
  - [ ] Test with two revisions
  - [ ] Test raises DiffError for missing file
- [ ] `get_supported_formats` - get formatted string of supported formats
  - [ ] Test returns formatted string

---

## `dt.doctor`

✅ **Implemented in `tests/test_doctor.py`**

- [x] `DiagnosticResult` - diagnostic result class
  - [x] Test __str__ format for passed (shows checkmark)
  - [x] Test __str__ format for failed (shows cross)
  - [x] Test includes help_text on failure
  - [x] Test omits help_text on success
- [x] `get_dt_version` - get dt package version
  - [x] Test returns version string
  - [x] Test returns "unknown" on error
- [x] `check_command_version` - check command availability and version
  - [x] Test with available command
  - [x] Test with missing command
  - [x] Test handles subprocess error
- [x] `check_git` - check git installation
  - [x] Test passes when installed
  - [x] Test fails when missing
- [x] `check_dvc` - check DVC installation
  - [x] Test passes when installed
  - [x] Test fails when missing
- [x] `check_gh` - check GitHub CLI installation
  - [x] Test passes when installed
  - [x] Test fails when missing (optional)
- [x] `check_ssh_key` - check SSH keys exist
  - [x] Test finds existing key
  - [x] Test reports missing key
- [x] `check_github_ssh` - check GitHub SSH connection
  - [x] Test successful authentication
  - [x] Test failed authentication
  - [x] Test timeout handling
- [x] `check_cache_root` - check cache root configuration
  - [x] Test when configured and accessible
  - [x] Test when not configured
- [ ] `check_remote_root` - check remote root configuration
  - [ ] Test when configured and accessible
  - [ ] Test when not configured
- [ ] `run_diagnostics` - run all diagnostic checks
  - [ ] Test returns list of DiagnosticResult
- [ ] `run_dvc_doctor` - run dvc doctor
  - [ ] Test returns output string
- [ ] `get_config_with_sources` - get config values with sources
  - [ ] Test returns list of tuples

---

## `dt.du`

- [ ] `collect_tracked_files` - collect DVC-tracked files
  - [ ] Test with no targets
  - [ ] Test with specific targets
- [ ] `get_dir_file_count` - get file count in tracked directory
  - [ ] Test returns correct count
- [ ] `get_cached_size` - get actual cached size
  - [ ] Test with cached file
  - [ ] Test with cached directory
  - [ ] Test returns 0 when not cached
- [ ] `get_cached_file_count` - get count of cached files
  - [ ] Test with cached files
  - [ ] Test returns 0 when not cached
- [ ] `aggregate_by_depth` - aggregate by directory depth
  - [ ] Test aggregation at different depths
  - [ ] Test with unlimited depth
- [ ] `calculate_du` - calculate disk usage
  - [ ] Test with cached=True
  - [ ] Test with cached=False
  - [ ] Test with count_inodes=True
  - [ ] Test with max_depth

---

## `dt.fetch`

✅ **Implemented in `tests/test_fetch.py`**

**Fixtures:** Use `fetch_setup` and `cache_dirs` from `test_fetch.py`

**Test data:** Import pattern from `imported/file.csv.dvc` in dt-test-fixtures

- [x] `_populate_cache_from_source` - populate cache from source cache
  - [x] Test single file fetch
  - [x] Test directory fetch with .dir manifest
  - [x] Test no cache returns zero
  - [x] Test no outs returns zero
- [x] `fetch` - fetch DVC-tracked files into primary cache
  - [x] Test with specific targets
  - [x] Test nonexistent target fails
  - [x] Test import file calls fetch_import
- [ ] `fetch_import` - fetch import .dvc file from source cache
  - [ ] Test successful fetch (use `fetch_setup` with .dvc containing deps.repo)
  - [ ] Test raises FetchError for non-import (use regular .dvc file pattern)
  - [ ] Test raises FetchError when no local cache
- [ ] `smart_checkout` - deprecated alias (backwards compat)
  - [ ] Test runs fetch and dvc checkout

---

## `dt.find`

- [ ] `find_by_hash` - find workspace path for hash
  - [ ] Test with full hash
  - [ ] Test with partial hash
  - [ ] Test with expand_dirs=True
  - [ ] Test returns multiple matches
  - [ ] Test raises FindError for short hash
- [ ] `format_results` - format find results
  - [ ] Test terminal output
  - [ ] Test JSON output
  - [ ] Test verbose mode

---

## `dt.history`

- [ ] `history` - get version history for DVC-tracked file
  - [ ] Test returns list of version entries
  - [ ] Test with limit
  - [ ] Test with since filter
  - [ ] Test raises HistoryError for untracked file
- [ ] `format_history` - format history entries
  - [ ] Test terminal output
  - [ ] Test JSON output
  - [ ] Test verbose mode

---

## `dt.hpc`

- [ ] `check_qxub` - check if qxub available
  - [ ] Test returns True/False
- [ ] `require_qxub` - ensure qxub available
  - [ ] Test raises HPCError when missing
- [ ] `get_qxub_config` - get qxub configuration
  - [ ] Test returns config dict
- [ ] `build_qxub_command` - build qxub exec command
  - [ ] Test builds correct command
- [ ] `submit_workers` - submit worker jobs
  - [ ] Test submits jobs
  - [ ] Test skips empty partitions
- [ ] `monitor_jobs` - monitor jobs until completion
  - [ ] Test monitors successfully
- [ ] `get_transfer_dir` - get transfer directory
  - [ ] Test creates directory
- [ ] `get_prefixes_for_worker` - get hash prefixes for worker
  - [ ] Test partitions correctly
- [ ] `save_manifest` - save manifest and partitions
  - [ ] Test saves files correctly
- [ ] `load_worker_partition` - load worker partition
  - [ ] Test loads correctly

---

## `dt.import_data`

✅ **Partially implemented in `tests/test_fetch.py`**

**Fixtures:** Use `cache_dirs` and `fetch_setup` from `test_fetch.py`

- [ ] `populate_primary_cache` - hardlink workspace symlinks to cache
  - [ ] Test creates hardlinks (use `cache_dirs`)
  - [ ] Test falls back to symlinks on cross-device
- [x] `populate_cache_file` - copy/link single file to cache
  - [x] Test single file cached via hardlink
  - [x] Test .dir file cached
  - [x] Test already exists returns False
  - [x] Test source not found returns False
- [ ] `configure_clone_cache` - configure clone to use cache
  - [ ] Test sets cache directory
- [ ] `list_files` - list files at path in DVC repo
  - [ ] Test returns file list
  - [ ] Test recursive listing
- [ ] `get_file_size_from_cache` - get file size from cache
  - [ ] Test returns size
  - [ ] Test returns None when missing
- [ ] `compute_dir_hash` - compute MD5 for .dir content
  - [ ] Test returns correct hash
- [ ] `create_dir_file` - create .dir file in cache
  - [ ] Test creates file with correct hash
- [ ] `create_dvc_file` - create .dvc file
  - [ ] Test creates file for single file
  - [ ] Test creates file for directory
- [ ] `import_data` - import DVC-tracked data from remote repo
  - [ ] Test single file import
  - [ ] Test directory import
  - [ ] Test with checkout=True

---

## `dt.index`

- [ ] `get_index_paths` - get local and mirror paths
  - [ ] Test returns tuple of paths
  - [ ] Test raises IndexNotConfigured when not configured
- [ ] `get_lock_timeout` - get lock timeout from config
  - [ ] Test returns configured value or default
- [ ] `get_retry_interval` - get retry interval from config
  - [ ] Test returns configured value or default
- [ ] `is_auto_sync_enabled` - check if auto sync enabled
  - [ ] Test returns config value
- [ ] `get_lock_owner` - get owner of lock file
  - [ ] Test returns username
- [ ] `get_lock_age` - get age of lock file
  - [ ] Test returns seconds
- [ ] `wait_for_lock` - wait for lock release
  - [ ] Test returns True when released
  - [ ] Test returns False on timeout
- [ ] `acquire_lock` - acquire lock
  - [ ] Test creates lock file
  - [ ] Test raises IndexLockTimeout
- [ ] `release_lock` - release lock
  - [ ] Test removes lock file
- [ ] `pull` - pull index from mirror
  - [ ] Test successful sync
  - [ ] Test handles missing mirror
- [ ] `push` - push index to mirror
  - [ ] Test successful sync
  - [ ] Test creates mirror if missing
- [ ] `status` - get index status
  - [ ] Test returns status dict
- [ ] `with_index_sync` - decorator for automatic sync
  - [ ] Test pulls before and pushes after

---

## `dt.init`

- [ ] `check_dependencies` - check required tools
  - [ ] Test passes when available
  - [ ] Test raises InitError when missing
- [ ] `init_git` - initialize git repository
  - [ ] Test creates .git directory
  - [ ] Test returns False if exists
- [ ] `check_github_remote` - check if GitHub remote exists
  - [ ] Test returns True when exists
  - [ ] Test returns False and prints suggestion
- [ ] `init_dvc` - initialize DVC
  - [ ] Test creates .dvc directory
  - [ ] Test returns False if exists
- [ ] `install_dvc_hooks` - install DVC git hooks
  - [ ] Test runs dvc install
- [ ] `get_dvc_autostage` - check if autostage enabled
  - [ ] Test returns True/False
- [ ] `init_dt_directory` - initialize .dt directory
  - [ ] Test creates .dt/.gitignore
  - [ ] Test auto-stages when autostage enabled
- [ ] `init_project` - initialize complete DVC project
  - [ ] Test runs all initialization steps
  - [ ] Test respects no_* flags
  - [ ] Test returns result dict

---

## `dt.ls`

- [ ] `parse_size` - parse human-readable size to bytes
  - [ ] Test with various units (K, M, G, T)
  - [ ] Test raises LsError for invalid
- [ ] `format_size` - format bytes as human-readable
  - [ ] Test various sizes
  - [ ] Test None handling
- [ ] `run_dvc_list` - run dvc list and parse output
  - [ ] Test returns list of items
  - [ ] Test raises LsError on failure
- [ ] `filter_items` - filter list items
  - [ ] Test pattern filtering (glob)
  - [ ] Test regex filtering
  - [ ] Test size filtering (min/max)
  - [ ] Test type filtering (files/dirs/exec)
  - [ ] Test hash_prefix filtering
- [ ] `format_output` - format filtered items
  - [ ] Test simple output
  - [ ] Test long format
  - [ ] Test with hash
  - [ ] Test JSON output
- [ ] `list_files` - main entry point
  - [ ] Test combines list, filter, format

---

## `dt.mv`

- [ ] `mv` - move/rename DVC-tracked file
  - [ ] Test basic move (non-import)
  - [ ] Test import move preserves deps
  - [ ] Test raises MvError on failure

---

## `dt.offline`

- [ ] `get_dt_root` - get .dt directory
  - [ ] Test returns Path
  - [ ] Test raises OfflineError when not initialized
- [ ] `get_tmp_dir` - get temp clones directory
  - [ ] Test returns correct path
- [ ] `get_local_config_path` - get config.local.yaml path
  - [ ] Test returns correct path
- [ ] `load_offline_state` - load offline state
  - [ ] Test loads existing state
  - [ ] Test returns empty state when missing
- [ ] `save_offline_state` - save offline state
  - [ ] Test saves correctly
- [ ] `clear_offline_state` - clear offline state
  - [ ] Test clears state
- [ ] `list_temp_clones` - list temp clones
  - [ ] Test returns list of (repo_id, path)
- [ ] `repo_id_to_urls` - convert repo_id to URLs
  - [ ] Test with github.com
  - [ ] Test with other hosts
- [ ] `get_config_key` - get git config key for path
  - [ ] Test returns correct key format
- [ ] `get_current_redirects` - get current URL redirects
  - [ ] Test returns dict of redirects
- [ ] `enable` - enable offline mode
  - [ ] Test sets git config
  - [ ] Test sets DVC remote overrides
  - [ ] Test saves state
- [ ] `disable` - disable offline mode
  - [ ] Test removes git config
  - [ ] Test removes DVC remote overrides
  - [ ] Test clears state
- [ ] `status` - get offline mode status
  - [ ] Test returns status dict
- [ ] `get_ssh_remotes` - get SSH-based DVC remotes
  - [ ] Test returns list of remotes
- [ ] `get_remote_overrides` - get current remote overrides
  - [ ] Test returns dict
- [ ] `enable_remote_overrides` - enable local overrides
  - [ ] Test sets overrides
- [ ] `disable_remote_overrides` - disable local overrides
  - [ ] Test removes overrides
- [ ] `get_remote_override_status` - get override status
  - [ ] Test returns status dict

---

## `dt.pull`

✅ **Partially implemented in `tests/test_pull.py`**

**Fixtures:** Use `dvc_project` and `dvc_project_tree` from `test_pull.py`

- [ ] `delete_dir_manifests` - delete .dir manifests from cache
  - [ ] Test deletes manifests (create .dir file in `cache_dirs`)
  - [ ] Test handles specific targets
- [ ] `get_remote_files_size` - estimate total size from remote
  - [ ] Test returns size estimate (use `cache_dirs` with files)
- [ ] `build_pull_manifest` - build manifest of files to pull
  - [ ] Test returns manifest dict (requires DVC, use @requires_dvc)
- [x] `partition_manifest` - partition manifest across workers
  - [x] Test empty manifest returns empty partitions
  - [x] Test single file single worker
  - [x] Test multiple files distributed
  - [x] Test partition by hex prefix
  - [x] Test more workers than files
- [ ] `pull_partition` - pull partition of files
  - [ ] Test transfers files (requires DVC, use @requires_dvc)
  - [ ] Test returns (pulled, failed) counts
- [ ] `parallel_pull` - execute parallel pull via qxub
  - [ ] Test submits workers
  - [ ] Test waits for completion
- [ ] `worker_pull` - execute worker pull
  - [ ] Test processes partition
- [x] `resolve_to_dvc_file` - resolve target to .dvc file
  - [x] Test with .dvc file that exists
  - [x] Test with .dvc file that doesn't exist
  - [x] Test target with .dvc suffix exists
  - [x] Test target in subdirectory
  - [x] Test directory target
  - [x] Test no .dvc file exists
  - [x] Test nested file in tracked dir
- [x] `is_import_target` - check if target is import
  - [x] Test regular file not import
  - [x] Test nonexistent target
- [x] `find_all_dvc_files` - find all .dvc files
  - [x] Test finds all .dvc files
  - [x] Test excludes .dvc directory
  - [x] Test excludes .dt directory
  - [x] Test results are sorted
- [x] `separate_targets` - separate import and regular targets
  - [x] Test separate empty list
  - [x] Test all regular targets
  - [x] Test nonexistent targets
- [ ] `pull` - pull DVC-tracked files
  - [ ] Test handles imports
  - [ ] Test handles regular files
  - [ ] Test force mode

---

## `dt.push`

- [ ] `get_files_size` - get total size of files in cache
  - [ ] Test returns size
- [ ] `get_project_remotes` - get project-configured remotes
  - [ ] Test returns list of (name, url)
- [ ] `push_to_remote` - push to single remote
  - [ ] Test returns (success, output)
- [ ] `push_all` - push to all project remotes
  - [ ] Test pushes to all remotes
  - [ ] Test raises PushError when no remotes
- [ ] `build_manifest` - build manifest of files to push
  - [ ] Test returns manifest dict
- [ ] `partition_manifest` - partition manifest across workers
  - [ ] Test partitions by hash prefix
- [ ] `push_partition` - push partition of files
  - [ ] Test transfers files
  - [ ] Test returns (pushed, failed) counts
- [ ] `parallel_push` - execute parallel push via qxub
  - [ ] Test submits workers
  - [ ] Test waits for completion
- [ ] `worker_push` - execute worker push
  - [ ] Test processes partition

---

## `dt.remote`

- [ ] `resolve_remote_path` - resolve remote directory path
  - [ ] Test with remote_path override
  - [ ] Test constructs from remote_root + name
  - [ ] Test raises RemoteError when not configured
- [ ] `init_remote_structure` - initialize remote directory structure
  - [ ] Test creates directories
  - [ ] Test sets permissions
- [ ] `configure_dvc_remote` - configure DVC remotes
  - [ ] Test sets SSH remote
  - [ ] Test sets local remote
- [ ] `init_remote` - initialize remote storage
  - [ ] Test creates new remote
  - [ ] Test uses existing remote
- [ ] `list_remotes` - list remotes for repo
  - [ ] Test returns list of (name, url, is_default)
- [ ] `list_remotes_from_repo` - list remotes from remote repo
  - [ ] Test uses tmp clone
- [ ] `parse_remote_url` - parse URL into host and path
  - [ ] Test local paths
  - [ ] Test file:// URLs
  - [ ] Test SSH URLs
  - [ ] Test SCP-style
  - [ ] Test cloud storage (returns None path)
- [ ] `get_local_hosts` - get list of local hostnames
  - [ ] Test includes hostname and configured ssh.host
- [ ] `is_local_host` - check if host is local
  - [ ] Test with local host
  - [ ] Test with remote host
- [ ] `extract_local_path` - extract local path from URL
  - [ ] Test with local path
  - [ ] Test with SSH to local host
  - [ ] Test returns None for remote
- [ ] `find_local_remote` - find locally-accessible remote
  - [ ] Test finds first accessible
  - [ ] Test with check_exists
- [ ] `find_local_remote_from_repo` - find local remote from remote repo
  - [ ] Test uses tmp clone

---

## `dt.summary`

- [ ] `get_output_dir` - get output directory for summaries
  - [ ] Test with explicit argument
  - [ ] Test with config value
  - [ ] Test default
- [ ] `generate_tree` - generate tree.txt
  - [ ] Test creates file
  - [ ] Test uses dvc list --tree
- [ ] `generate_dag` - generate dag.md
  - [ ] Test creates file
  - [ ] Test uses dvc dag --md
- [ ] `generate_all` - generate all summary files
  - [ ] Test creates both files

---

## `dt.tmp`

✅ **Implemented in `tests/test_tmp.py`**

**Test data:** Can use dt-test-registry URL for clone tests

- [x] `get_tmp_dir` - get tmp directory path
  - [x] Test returns `.dt/tmp/clones` path
- [x] `resolve_repository_url` - resolve repo spec to URL
  - [x] Test with full SSH URL
  - [x] Test with full HTTPS URL
  - [x] Test with short name and owner argument
  - [x] Test with short name uses config owner
  - [x] Test raises TmpError when owner missing
- [x] `get_repo_id` - convert URL to path-like directory structure
  - [x] Test SSH format → `github.com/swarbricklab/dt-test-registry`
  - [x] Test HTTPS format → `github.com/swarbricklab/dt-test-registry`
  - [x] Test short name with owner
  - [x] Test removes .git suffix
- [x] `ensure_gitignore` - ensure .dt/tmp in .gitignore
  - [x] Test adds pattern
  - [x] Test returns False for existing pattern
- [x] `clone_repo` - clone repository to tmp
  - [x] Test creates sparse clone
  - [x] Test refreshes existing clone
  - [x] Test uses cached clone when refresh=False
  - [x] Test raises error on clone failure
- [ ] `list_repos` - list cached repository clones
  - [ ] Test returns list of (repo_id, path)
- [ ] `clean_repos` - remove cached repository clones
  - [ ] Test removes specific repo
  - [ ] Test removes all repos

---

## `dt.worktree`

- [ ] `add` - create git worktree with DVC cache
  - [ ] Test creates worktree
  - [ ] Test configures DVC cache
  - [ ] Test initializes submodules
  - [ ] Test with new branch
  - [ ] Test raises WorktreeError on failure
- [ ] `list_worktrees` - list git worktrees
  - [ ] Test returns list of worktree info
- [ ] `remove` - remove git worktree
  - [ ] Test removes worktree
  - [ ] Test force removal

---

## Testing Infrastructure

### Available Test Repositories

Two external repositories provide realistic DVC tracking scenarios:

#### dt-test-registry
**URL:** `https://github.com/swarbricklab/dt-test-registry`

Source repository for import testing with versioned data across three tags (v1.0, v2.0, v3.0).

**Structure:**
```
data/
  file.csv          # Single tracked file
  dir/              # Tracked directory (a.csv, b.csv)
nested/deep/path/
  data.csv          # Deep nesting test
pipeline/           # Pipeline with transform stage
.cache/             # Local cache (gitignored)
.remote/            # Local remote (gitignored)
```

#### dt-test-fixtures
**URL:** `https://github.com/swarbricklab/dt-test-fixtures`

Main test repository with diverse DVC tracking patterns:

**Structure:**
```
single_file/
  data.csv.dvc      # File tracked via .dvc
importable/
  file.csv.dvc      # Another tracked file
  dir.dvc           # Directory tracked via .dvc
imported/
  file.csv.dvc      # Import from dt-test-registry (has deps.repo)
  dir.dvc           # Directory import
pipeline/           # Pipeline stage (dvc.yaml, dvc.lock)
.cache/             # Local cache (gitignored)
.remote/            # Local remote (gitignored)
```

### DVC Tracking Patterns Covered

| Pattern | Example in dt-test-fixtures | Detection |
|---------|-----------|-----------|
| Single file via .dvc | `single_file/data.csv.dvc` | Has `outs:` with `path:` |
| Directory via .dvc | `importable/dir.dvc` | Has `outs:` with `.dir` hash |
| Import from repo | `imported/file.csv.dvc` | Has `deps:` with `repo.url` |
| Pipeline output | `pipeline/dvc.lock` | Referenced in `dvc.yaml` |

### Existing Pytest Fixtures

From `test_config.py`:
- **`temp_dirs`** - Creates temporary directories for config scopes (home, project, system)
- **`isolated_config`** - Sets up isolated config environment with XDG overrides
- **`runner`** - Click CliRunner for CLI testing

From `test_pull.py`:
- **`dvc_project`** - Creates minimal DVC project with sample .dvc files
- **`dvc_project_tree`** - Creates DVC project with .dvc files at various directory levels

From `test_fetch.py`:
- **`cache_dirs`** - Creates source and destination cache directory structures
- **`fetch_setup`** - Creates DVC project with cache and source cache for fetch testing

### Recommended New Fixtures

For comprehensive unit testing, add these fixtures to `tests/conftest.py`:

```python
@pytest.fixture
def dt_test_fixtures_clone(tmp_path):
    """Clone dt-test-fixtures for integration tests.
    
    Use sparingly - prefer mocked fixtures for unit tests.
    """
    import subprocess
    repo_path = tmp_path / 'dt-test-fixtures'
    subprocess.run([
        'git', 'clone', '--depth', '1',
        'https://github.com/swarbricklab/dt-test-fixtures',
        str(repo_path)
    ], check=True, capture_output=True)
    return repo_path

@pytest.fixture
def mock_dvc_repo(tmp_path, monkeypatch):
    """Mock DVC repository without requiring DVC.
    
    Creates .dvc directory and sample .dvc files.
    """
    (tmp_path / '.dvc').mkdir()
    (tmp_path / '.git').mkdir()
    monkeypatch.chdir(tmp_path)
    return tmp_path

@pytest.fixture  
def sample_dvc_files(mock_dvc_repo):
    """Add sample .dvc files to mock repo.
    
    Includes: regular file, directory, and import patterns.
    """
    # Regular file
    (mock_dvc_repo / 'data.csv.dvc').write_text(
        'outs:\n  - md5: abcdef1234567890abcdef1234567890\n'
        '    size: 1024\n    path: data.csv\n'
    )
    # Directory
    (mock_dvc_repo / 'dir.dvc').write_text(
        'outs:\n  - md5: 1234567890abcdef1234567890abcdef.dir\n'
        '    size: 2048\n    nfiles: 5\n    path: dir\n'
    )
    # Import (with deps.repo)
    (mock_dvc_repo / 'imported.dvc').write_text(
        'deps:\n  - path: data/file.csv\n'
        '    repo:\n      url: https://github.com/swarbricklab/dt-test-registry\n'
        '      rev_lock: abc123\n'
        'outs:\n  - md5: fedcba0987654321fedcba0987654321\n'
        '    size: 512\n    path: imported.csv\n'
    )
    return mock_dvc_repo
```

### Test Categories

- **Unit Tests**: Test internal functions in isolation using mocked fixtures
  - Use `mock_dvc_repo`, `sample_dvc_files`, `cache_dirs` fixtures
  - Mock subprocess calls for external commands (dvc, git)
  
- **Integration Tests**: Test with real DVC/git using test repositories
  - Use `dt_test_fixtures_clone` fixture
  - Require DVC installed (skip if unavailable)

### Environment Considerations

```python
import pytest
import shutil

# Skip if DVC not installed
requires_dvc = pytest.mark.skipif(
    shutil.which('dvc') is None,
    reason="DVC not installed"
)

# Skip if qxub not available (HPC tests)
requires_qxub = pytest.mark.skipif(
    shutil.which('qxub') is None,
    reason="qxub not available (HPC environment only)"
)

# Skip network-dependent tests
requires_network = pytest.mark.skipif(
    os.environ.get('DT_TEST_OFFLINE'),
    reason="Network tests disabled"
)
```
