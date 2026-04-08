"""Cache management for DVC Tools.

Handles external shared cache setup and configuration for HPC environments.
"""

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dvc.repo import Repo

from . import config as cfg
from . import utils
from .errors import CacheError


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
    
    # Create runs directory for DVC run cache with 00-ff subdirectories
    runs_dir = cache_dir / "runs"
    runs_dir.mkdir(exist_ok=True)
    utils.set_group_writable(runs_dir)
    for i in range(256):
        subdir = runs_dir / f"{i:02x}"
        subdir.mkdir(exist_ok=True)
        utils.set_group_writable(subdir)
    
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
    cache_dir = utils.get_cache_dir()
    if cache_dir is None:
        raise CacheError("Cache not configured or DVC not available")
    return cache_dir


# Re-export hash_to_cache_path from utils
hash_to_cache_path = utils.hash_to_cache_path


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
    if verbose:
        print(f"Collecting files for targets...")
    
    try:
        result = utils.collect_tracked_entries(targets=targets, push=False)
    except utils.DependencyError as e:
        raise CacheError(str(e))
    
    # Extract hashes from entries
    files = [entry['hash'] for entry in result['entries']]
    
    if verbose:
        print(f"Found {len(files)} file(s)")
    
    return {
        'files': files,
        'paths': result['hash_to_path'],
        'repo_root': str(result['repo'].root_dir),
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
    
    # _collect_indexes already returns both directory manifests (.dir files)
    # and all contained files, so no expansion is needed
    all_hashes = manifest['files']
    hash_to_path = manifest['paths']
    
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
                    size_str = f" ({utils.format_size(size)})" if show_size else ""
                    print(f"Deleted: {workspace_path}{size_str}")
            except PermissionError as e:
                # Cache files are often read-only (mode 0444)
                failed.append((workspace_path, file_hash, f"Permission denied (file is read-only)"))
                if verbose:
                    print(f"Failed to delete {workspace_path}: permission denied (read-only)")
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


# =============================================================================
# Cache validate functionality
# =============================================================================

def compute_file_hash(file_path: Path) -> str:
    """Compute MD5 hash of a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        MD5 hash as hex string
    """
    import hashlib
    
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        # Read in chunks to handle large files
        for chunk in iter(lambda: f.read(8192), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def expected_hash_from_path(cache_file: Path) -> str:
    """Extract expected hash from a cache file path.
    
    Cache files are stored as: cache_dir/XX/YYYYYY...
    where XX is the first 2 chars of the hash and YYYYYY... is the rest.
    
    Args:
        cache_file: Path to a cache file
        
    Returns:
        Expected hash (without .dir suffix)
    """
    # Parent directory name is first 2 chars
    prefix = cache_file.parent.name
    # File name is rest of hash (possibly with .dir suffix)
    suffix = cache_file.name.replace('.dir', '')
    return prefix + suffix


def validate_cache_file(cache_file: Path) -> Tuple[bool, str, str]:
    """Validate a single cache file by checking its hash.
    
    Args:
        cache_file: Path to the cache file
        
    Returns:
        Tuple of (is_valid, expected_hash, actual_hash)
    """
    expected = expected_hash_from_path(cache_file)
    actual = compute_file_hash(cache_file)
    return expected == actual, expected, actual


def get_parent_dir_hash(
    cache_dir: Path,
    file_hash: str,
) -> Optional[str]:
    """Find the .dir manifest that contains a given file hash.
    
    Searches all .dir files in the cache to find which directory
    contains the specified file.
    
    Args:
        cache_dir: Path to the cache files/md5 directory
        file_hash: Hash of the file to find parent for
        
    Returns:
        The .dir hash if found, None otherwise
    """
    import json
    
    # Search all .dir files
    for subdir in cache_dir.iterdir():
        if not subdir.is_dir():
            continue
        for cache_file in subdir.iterdir():
            if not cache_file.name.endswith('.dir'):
                continue
            try:
                with open(cache_file, 'r') as f:
                    entries = json.load(f)
                for entry in entries:
                    if entry.get('md5') == file_hash:
                        # Found it - return the full .dir hash
                        return subdir.name + cache_file.name
            except (json.JSONDecodeError, OSError):
                pass
    return None


def validate_cache(
    targets: Optional[List[str]] = None,
    fix: bool = False,
    verbose: bool = False,
    progress: bool = True,
) -> Dict[str, Any]:
    """Validate cache files by checking MD5 checksums.
    
    Args:
        targets: Optional list of workspace paths to validate.
                 If None, validates all files in cache.
        fix: If True, delete corrupted files (and their parent .dir)
        verbose: Print progress for each file
        progress: Show progress counter
        
    Returns:
        Dictionary with:
            - valid: List of (workspace_path, hash) for valid files
            - corrupted: List of (workspace_path, hash, expected, actual) for corrupted
            - missing: List of (workspace_path, hash) for files not in cache
            - fixed: List of (workspace_path, hash) for files that were deleted
            - dir_fixed: List of dir_hash for .dir manifests that were deleted
            - errors: List of (path, error) for files that couldn't be checked
    """
    cache_dir = get_cache_dir()
    
    valid = []
    corrupted = []
    missing = []
    fixed = []
    dir_fixed = []
    errors = []
    
    # Build list of files to check
    if targets:
        # Get hashes for specific targets
        file_hashes, hash_to_path = collect_hashes_for_targets(targets)
        files_to_check = []
        for file_hash in file_hashes:
            cache_path = hash_to_cache_path(cache_dir, file_hash)
            workspace_path = hash_to_path.get(file_hash, file_hash)
            if cache_path.exists():
                files_to_check.append((cache_path, file_hash, workspace_path))
            else:
                missing.append((workspace_path, file_hash))
    else:
        # Validate entire cache
        files_to_check = []
        for subdir in cache_dir.iterdir():
            if not subdir.is_dir():
                continue
            for cache_file in subdir.iterdir():
                if cache_file.name.endswith('.tmp'):
                    continue  # Skip temp files
                file_hash = subdir.name + cache_file.name
                files_to_check.append((cache_file, file_hash, file_hash))
    
    total = len(files_to_check)
    deleted_paths = set()  # Track files deleted during --fix
    
    for i, (cache_path, file_hash, workspace_path) in enumerate(files_to_check):
        if progress and not verbose:
            print(f"\rValidating {i+1}/{total}...", end='', flush=True)
        
        # Skip if already deleted (e.g., .dir manifest deleted when fixing child)
        if str(cache_path) in deleted_paths:
            continue
        
        # Check if file still exists (may have been deleted)
        if not cache_path.exists():
            continue
        
        try:
            is_valid, expected, actual = validate_cache_file(cache_path)
            
            if is_valid:
                valid.append((workspace_path, file_hash))
                if verbose:
                    print(f"✓ {workspace_path}")
            else:
                corrupted.append((workspace_path, file_hash, expected, actual))
                if verbose:
                    print(f"✗ {workspace_path}")
                    print(f"  Expected: {expected}")
                    print(f"  Actual:   {actual}")
                
                if fix:
                    # Delete the corrupted file
                    try:
                        cache_path.unlink()
                        fixed.append((workspace_path, file_hash))
                        deleted_paths.add(str(cache_path))
                        if verbose:
                            print(f"  Deleted corrupted file")
                        
                        # Track parent .dir for informational purposes
                        clean_hash = file_hash.replace('.dir', '')
                        parent_dir = get_parent_dir_hash(cache_dir, clean_hash)
                        if parent_dir and parent_dir not in dir_fixed:
                            dir_fixed.append(parent_dir)
                    except OSError as e:
                        errors.append((str(cache_path), f"Failed to delete: {e}"))
                        
        except OSError as e:
            errors.append((str(cache_path), str(e)))
            if verbose:
                print(f"? {workspace_path}: {e}")
    
    if progress and not verbose:
        print()  # Newline after progress
    
    return {
        'valid': valid,
        'corrupted': corrupted,
        'missing': missing,
        'fixed': fixed,
        'dir_fixed': dir_fixed,
        'errors': errors,
    }


def collect_hashes_for_targets(
    targets: List[str],
) -> Tuple[List[str], Dict[str, str]]:
    """Collect file hashes for the given targets.
    
    Uses DVC internals to resolve targets to their hashes,
    expanding directories to include all contained files.
    
    Args:
        targets: List of workspace paths
        
    Returns:
        Tuple of (list of hashes, hash->path mapping)
    """
    try:
        repo = Repo()
    except Exception as e:
        raise CacheError(f"Not in a DVC repository: {e}")
    
    cache_dir = get_cache_dir()
    file_hashes = []
    hash_to_path = {}
    
    # Resolve targets to absolute paths
    repo_root = Path(repo.root_dir)
    target_paths = set()
    for target in targets:
        target_path = Path(target).resolve()
        if target_path.exists():
            target_paths.add(target_path)
        else:
            # Try relative to repo root
            rel_path = repo_root / target
            if rel_path.exists():
                target_paths.add(rel_path)
            else:
                target_paths.add(target_path)  # Add anyway, might be in .dvc
    
    # Find matching outputs
    for out in repo.index.outs:
        if not out.hash_info or not out.hash_info.value:
            continue
        
        out_path = Path(out.fs_path)
        matches = False
        
        for target_path in target_paths:
            try:
                # Check if output matches or is under target
                if out_path == target_path:
                    matches = True
                    break
                out_path.relative_to(target_path)
                matches = True
                break
            except ValueError:
                # Not relative - check if target is under output (for dirs)
                try:
                    target_path.relative_to(out_path)
                    matches = True
                    break
                except ValueError:
                    pass
        
        if not matches:
            continue
        
        file_hash = out.hash_info.value
        workspace_path = str(out.fs_path)
        
        file_hashes.append(file_hash)
        hash_to_path[file_hash] = workspace_path
        
        # Expand directories
        if out.hash_info.isdir:
            expanded, hash_to_path = expand_dir_hashes(
                cache_dir, [file_hash], hash_to_path
            )
            for h in expanded:
                if h not in file_hashes:
                    file_hashes.append(h)
    
    return file_hashes, hash_to_path
