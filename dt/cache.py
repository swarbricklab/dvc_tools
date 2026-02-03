"""Cache management for DVC Tools.

Handles external shared cache setup and configuration for HPC environments.
"""

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import config as cfg
from . import utils


class CacheError(Exception):
    """Raised when cache operations fail."""
    pass


def format_size(size_bytes: int) -> str:
    """Format byte size as human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    elif size_bytes < 1024 * 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024 * 1024):.1f} TB"


def resolve_cache_path(
    name: Optional[str] = None,
    cache_root: Optional[str] = None,
    cache_path: Optional[str] = None,
) -> Path:
    """Resolve the cache directory path.
    
    Path resolution order:
    1. cache_path - Complete path override
    2. Constructed: {cache_root}/{name}
    
    Args:
        name: Project name (defaults to current directory name)
        cache_root: Root directory for caches
        cache_path: Complete path override
        
    Returns:
        Resolved cache directory path
        
    Raises:
        CacheError: If cache location cannot be determined
    """
    if cache_path:
        return Path(cache_path).resolve()
    
    # Get cache root from argument or config
    root = cache_root or cfg.get_value('cache.root')
    if not root:
        raise CacheError(
            "Cache root not configured.\n"
            "Either specify --cache-root or set cache.root:\n"
            "  dt config set cache.root /path/to/cache"
        )
    
    # Get project name
    project_name = name or utils.get_project_name()
    
    return Path(root) / project_name


def init_cache_structure(cache_dir: Path, verbose: bool = True) -> None:
    """Initialize the cache directory structure with proper permissions.
    
    Creates the files/md5 subdirectories (00-ff) and runs directory
    with group write permissions for shared access in HPC environments.
    
    Args:
        cache_dir: Path to the cache directory
        verbose: Print progress messages
    """
    if verbose:
        print(f"Initializing cache structure at {cache_dir}")
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    utils.set_group_writable(cache_dir)
    
    # Create runs directory for DVC run cache
    runs_dir = cache_dir / "runs"
    runs_dir.mkdir(exist_ok=True)
    utils.set_group_writable(runs_dir)
    
    # Create files/md5 structure with 00-ff subdirectories
    utils.create_md5_subdirs(cache_dir, verbose=verbose)


def configure_dvc_cache(repo_path: Path, cache_dir: Path, verbose: bool = True) -> None:
    """Configure DVC to use the specified cache directory.
    
    Uses --local flag to keep configuration workspace-specific.
    
    Args:
        repo_path: Path to the DVC repository
        cache_dir: Path to the cache directory
        verbose: Print progress messages
    """
    if verbose:
        print(f"Configuring DVC cache: {cache_dir}")
    
    result = subprocess.run(
        ['dvc', 'cache', 'dir', '--local', str(cache_dir)],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise CacheError(f"Failed to configure DVC cache: {result.stderr}")


def init_cache(
    name: Optional[str] = None,
    cache_root: Optional[str] = None,
    cache_path: Optional[str] = None,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
) -> Path:
    """Initialize an external shared cache for a DVC project.
    
    Creates the cache directory structure with proper permissions
    and configures DVC to use it.
    
    Args:
        name: Project name (defaults to current directory name)
        cache_root: Root directory for caches
        cache_path: Complete path override
        repo_path: Path to the DVC repository (defaults to cwd)
        verbose: Print progress messages
        
    Returns:
        Path to the initialized cache directory
        
    Raises:
        CacheError: If cache initialization fails
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise CacheError(str(e))
    
    repo_path = repo_path or Path.cwd()
    cache_dir = resolve_cache_path(name, cache_root, cache_path)
    
    if cache_dir.exists():
        if verbose:
            print(f"Using existing cache at {cache_dir}")
    else:
        if verbose:
            print(f"Creating cache at {cache_dir}")
        init_cache_structure(cache_dir, verbose=verbose)
    
    configure_dvc_cache(repo_path, cache_dir, verbose=verbose)
    
    return cache_dir


# =============================================================================
# Cache rm functionality
# =============================================================================

def get_cache_dir() -> Path:
    """Get the primary DVC cache directory.
    
    Returns:
        Path to the cache files/md5 directory
        
    Raises:
        CacheError: If cache is not configured or DVC not available
    """
    try:
        from dvc.repo import Repo
        repo = Repo()
        return Path(repo.cache.local.path)
    except ImportError:
        raise CacheError("DVC internals not available")
    except Exception as e:
        raise CacheError(f"Failed to get cache directory: {e}")


def hash_to_cache_path(cache_dir: Path, file_hash: str) -> Path:
    """Convert a file hash to its cache file path.
    
    Args:
        cache_dir: Path to the cache files/md5 directory
        file_hash: MD5 hash (possibly with .dir suffix)
        
    Returns:
        Path to the cache file
    """
    hash_clean = file_hash.replace('.dir', '')
    prefix = hash_clean[:2]
    suffix = hash_clean[2:]
    if file_hash.endswith('.dir'):
        suffix += '.dir'
    
    return cache_dir / prefix / suffix


def expand_dir_hashes(
    cache_dir: Path,
    file_hashes: List[str],
    hash_to_path: Optional[Dict[str, str]] = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Expand .dir hashes to include the files they reference.
    
    For each hash ending in .dir, reads the manifest and adds the
    individual file hashes to the result.
    
    Args:
        cache_dir: Path to the cache files/md5 directory
        file_hashes: List of file hashes (some may be .dir files)
        hash_to_path: Optional existing hash->path mapping to extend
        
    Returns:
        Tuple of (expanded hash list, updated hash->path mapping)
    """
    import json
    
    expanded = []
    paths = dict(hash_to_path) if hash_to_path else {}
    
    for file_hash in file_hashes:
        expanded.append(file_hash)
        
        if file_hash.endswith('.dir'):
            # Read the .dir file to get contained file hashes
            cache_path = hash_to_cache_path(cache_dir, file_hash)
            dir_path = paths.get(file_hash, '')
            if cache_path.exists():
                try:
                    with open(cache_path, 'r') as f:
                        dir_contents = json.load(f)
                    # Each entry has 'md5' key with the file hash and 'relpath'
                    for entry in dir_contents:
                        if 'md5' in entry:
                            child_hash = entry['md5']
                            if child_hash not in expanded:
                                expanded.append(child_hash)
                            # Build path for child
                            if dir_path and 'relpath' in entry:
                                child_path = f"{dir_path}/{entry['relpath']}"
                                paths[child_hash] = child_path
                except (json.JSONDecodeError, OSError, KeyError):
                    # If we can't read the .dir file, just skip expansion
                    pass
    
    return expanded, paths


def get_hash_for_path_in_dir(
    cache_dir: Path,
    dir_hash: str,
    relative_path: str,
) -> Optional[str]:
    """Get the hash for a specific file within a tracked directory.
    
    Args:
        cache_dir: Path to the cache files/md5 directory
        dir_hash: The .dir hash for the tracked directory
        relative_path: Path of the file relative to the directory root
        
    Returns:
        The file's hash if found, None otherwise
    """
    import json
    
    cache_path = hash_to_cache_path(cache_dir, dir_hash)
    if not cache_path.exists():
        return None
    
    try:
        with open(cache_path, 'r') as f:
            dir_contents = json.load(f)
        
        for entry in dir_contents:
            if entry.get('relpath') == relative_path:
                return entry.get('md5')
    except (json.JSONDecodeError, OSError, KeyError):
        pass
    
    return None


def check_hashes_in_remote(
    file_hashes: List[str],
    remote: Optional[str] = None,
) -> Tuple[List[str], List[str]]:
    """Check which hashes exist in the remote.
    
    Args:
        file_hashes: List of file hashes to check
        remote: Optional remote name (uses default if not specified)
        
    Returns:
        Tuple of (hashes_in_remote, hashes_not_in_remote)
    """
    try:
        from dvc.repo import Repo
        from dvc_data.hashfile.hash_info import HashInfo
        from dvc_data.hashfile.status import compare_status
    except ImportError:
        # Can't access DVC internals -> conservative: treat as NOT in remote
        return [], file_hashes

    if not file_hashes:
        return [], []

    try:
        repo = Repo()
        cache_odb = repo.cache.local
        remote_obj = repo.cloud.get_remote(name=remote)
        remote_odb = remote_obj.odb

        # Create HashInfo objects (strip possible .dir suffix)
        obj_ids = [HashInfo('md5', h.replace('.dir', '')) for h in file_hashes]

        # Compare with remote
        status = compare_status(
            cache_odb,
            remote_odb,
            obj_ids,
            check_deleted=False,
            shallow=True,
        )

        # status.new = hashes NOT on remote
        not_in_remote = {hi.value for hi in status.new}

        in_remote = []
        not_found = []
        for h in file_hashes:
            h_clean = h.replace('.dir', '')
            if h_clean in not_in_remote:
                not_found.append(h)
            else:
                in_remote.append(h)

        return in_remote, not_found

    except Exception:
        # Any failure to contact/check remote -> conservative: treat as NOT in remote
        return [], file_hashes


def collect_hashes_for_targets(
    targets: List[str],
    verbose: bool = False,
) -> Dict[str, Any]:
    """Collect all file hashes for the specified targets.
    
    Uses DVC internals to enumerate all files tracked by the targets.
    
    Args:
        targets: List of targets (.dvc files, paths, stages)
        verbose: Print progress messages
        
    Returns:
        Dict with 'files' (list of hash strings), 'paths' (hash->path mapping),
        and 'repo_root'
    """
    try:
        from dvc.repo import Repo
        from dvc.repo.fetch import _collect_indexes
    except ImportError as e:
        raise CacheError(f"DVC internals not available: {e}")
    
    repo = Repo()
    
    if verbose:
        print(f"Collecting files for targets...")
    
    # Collect indexes for targets
    indexes = _collect_indexes(
        repo,
        targets=targets,
        remote=None,
        all_branches=False,
        with_deps=False,
        all_tags=False,
        recursive=False,
        all_commits=False,
        revs=None,
        workspace=True,
        push=False,  # We want all files, not just pushable ones
    )
    
    if not indexes:
        return {'files': [], 'paths': {}, 'repo_root': str(repo.root_dir)}
    
    # Build hash-to-path mapping and collect all hashes
    hash_to_path: Dict[str, str] = {}
    files: List[str] = []
    
    for idx in indexes.values():
        repo_data = idx.data.get('repo')
        if repo_data:
            for key, entry in repo_data.items():
                if entry.hash_info and entry.hash_info.value:
                    path = '/'.join(key)
                    file_hash = entry.hash_info.value
                    hash_to_path[file_hash] = path
                    if file_hash not in files:
                        files.append(file_hash)
    
    if verbose:
        print(f"Found {len(files)} file(s)")
    
    return {
        'files': files,
        'paths': hash_to_path,
        'repo_root': str(repo.root_dir),
    }


def get_cache_file_info(
    cache_dir: Path,
    file_hashes: List[str],
) -> List[Tuple[str, Path, Optional[int]]]:
    """Get cache file paths and sizes for the given hashes.
    
    Args:
        cache_dir: Path to the cache files/md5 directory
        file_hashes: List of file hashes
        
    Returns:
        List of (hash, cache_path, size) tuples. Size is None if file doesn't exist.
    """
    results = []
    for file_hash in file_hashes:
        cache_path = hash_to_cache_path(cache_dir, file_hash)
        if cache_path.exists():
            try:
                size = cache_path.stat().st_size
            except OSError:
                size = None
            results.append((file_hash, cache_path, size))
        else:
            results.append((file_hash, cache_path, None))
    
    return results


def remove_cache_files(
    targets: List[str],
    dry_run: bool = False,
    show_size: bool = False,
    verbose: bool = False,
    force: bool = False,
) -> Dict[str, Any]:
    """Remove cache files for the specified targets.
    
    Only affects the primary cache. Alternate caches are never modified.
    
    Args:
        targets: List of targets (.dvc files, paths, stages)
        dry_run: If True, only report what would be deleted
        show_size: If True, include file sizes in the output
        verbose: Print detailed progress
        force: If True, delete even if files are not in remote
        
    Returns:
        Dict with results:
            - 'deleted': List of (path, hash, size) for deleted files
            - 'missing': List of (path, hash) for files not in cache
            - 'failed': List of (path, hash, error) for deletion failures
            - 'not_in_remote': List of (path, hash) for files not in remote
            - 'total_size': Total size of deleted (or would-be-deleted) files
            - 'blocked': True if deletion was blocked due to files not in remote
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise CacheError(str(e))
    
    # Get primary cache directory
    cache_dir = get_cache_dir()
    
    if verbose:
        print(f"Primary cache: {cache_dir}")
    
    # Collect hashes for targets
    manifest = collect_hashes_for_targets(targets, verbose=verbose)
    
    if not manifest['files']:
        return {
            'deleted': [],
            'missing': [],
            'failed': [],
            'not_in_remote': [],
            'total_size': 0,
            'blocked': False,
        }
    
    # Expand .dir hashes to include contained files
    all_hashes, hash_to_path = expand_dir_hashes(
        cache_dir, manifest['files'], manifest['paths']
    )
    
    if verbose and len(all_hashes) > len(manifest['files']):
        print(f"Expanded to {len(all_hashes)} file(s) (including directory contents)")
    
    # Get cache file info (filter to files that exist)
    file_info = get_cache_file_info(cache_dir, all_hashes)
    existing_hashes = [h for h, _, size in file_info if size is not None]
    
    # Check which files are in remote
    not_in_remote = []
    if existing_hashes and not force:
        if verbose:
            print("Checking remote status...")
        in_remote, not_in_remote_hashes = check_hashes_in_remote(existing_hashes)
        not_in_remote = [
            (hash_to_path.get(h, h), h) 
            for h in not_in_remote_hashes
        ]
    
    deleted = []
    missing = []
    failed = []
    total_size = 0
    blocked = False
    
    # If there are files not in remote and not forcing, block deletion
    if not_in_remote and not force:
        blocked = True
        # Still calculate what would be deleted for reporting
        for file_hash, cache_path, size in file_info:
            workspace_path = hash_to_path.get(file_hash, file_hash)
            if size is None:
                missing.append((workspace_path, file_hash))
            else:
                total_size += size
                deleted.append((workspace_path, file_hash, size))
        
        return {
            'deleted': deleted,
            'missing': missing,
            'failed': failed,
            'not_in_remote': not_in_remote,
            'total_size': total_size,
            'blocked': blocked,
        }
    
    for file_hash, cache_path, size in file_info:
        workspace_path = hash_to_path.get(file_hash, file_hash)
        
        if size is None:
            # File not in cache
            missing.append((workspace_path, file_hash))
            continue
        
        total_size += size
        
        if dry_run:
            # Just record what would be deleted
            deleted.append((workspace_path, file_hash, size))
        else:
            # Actually delete the file
            try:
                cache_path.unlink()
                deleted.append((workspace_path, file_hash, size))
                if verbose:
                    size_str = f" ({format_size(size)})" if show_size else ""
                    print(f"Deleted: {workspace_path}{size_str}")
            except OSError as e:
                failed.append((workspace_path, file_hash, str(e)))
                if verbose:
                    print(f"Failed to delete {workspace_path}: {e}")
    
    return {
        'deleted': deleted,
        'missing': missing,
        'failed': failed,
        'not_in_remote': not_in_remote,
        'total_size': total_size,
        'blocked': False,
    }
