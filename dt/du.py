"""Disk usage reporting for DVC-tracked files.

Reports sizes of DVC-tracked files, similar to the standard `du` command.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import utils


class DuError(Exception):
    """Error during disk usage calculation."""
    pass


def format_size(size_bytes: int, human_readable: bool = False) -> str:
    """Format byte size for output.
    
    Args:
        size_bytes: Size in bytes
        human_readable: Use human-readable format (K, M, G)
        
    Returns:
        Formatted size string
    """
    if human_readable:
        try:
            from dvc.utils.humanize import naturalsize
            return naturalsize(size_bytes)
        except ImportError:
            # Fallback if DVC internals unavailable
            if size_bytes < 1024:
                return f"{size_bytes}B"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f}K"
            elif size_bytes < 1024 * 1024 * 1024:
                return f"{size_bytes / (1024 * 1024):.1f}M"
            elif size_bytes < 1024 * 1024 * 1024 * 1024:
                return f"{size_bytes / (1024 * 1024 * 1024):.1f}G"
            else:
                return f"{size_bytes / (1024 * 1024 * 1024 * 1024):.1f}T"
    else:
        return str(size_bytes)


def get_cache_dir() -> Optional[Path]:
    """Get the primary DVC cache directory.
    
    Returns:
        Path to the cache files/md5 directory, or None if not available
    """
    try:
        from dvc.repo import Repo
        repo = Repo()
        return Path(repo.cache.local.path)
    except Exception:
        return None


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


def collect_tracked_files(
    targets: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Collect all DVC-tracked files with their metadata.
    
    Args:
        targets: Optional list of targets. If None, collects all tracked files.
        
    Returns:
        List of dicts with 'path', 'hash', 'size', 'nfiles' (for dirs), 'is_dir'
    """
    try:
        from dvc.repo import Repo
        from dvc.repo.fetch import _collect_indexes
    except ImportError as e:
        raise DuError(f"DVC internals not available: {e}")
    
    repo = Repo()
    
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
        push=False,
    )
    
    if not indexes:
        return []
    
    files = []
    seen_hashes = set()
    
    for idx in indexes.values():
        repo_data = idx.data.get('repo')
        if repo_data:
            for key, entry in repo_data.items():
                if entry.hash_info and entry.hash_info.value:
                    file_hash = entry.hash_info.value
                    if file_hash in seen_hashes:
                        continue
                    seen_hashes.add(file_hash)
                    
                    path = '/'.join(key)
                    is_dir = file_hash.endswith('.dir')
                    
                    # Get size and nfiles from entry.meta
                    meta = entry.meta
                    size = meta.size if meta and meta.size else 0
                    nfiles = meta.nfiles if meta and meta.nfiles else 1
                    
                    files.append({
                        'path': path,
                        'hash': file_hash,
                        'size': size,
                        'nfiles': nfiles,
                        'is_dir': is_dir,
                    })
    
    return files


def get_dir_file_count(repo, dir_hash: str) -> int:
    """Get the number of files in a tracked directory.
    
    Args:
        repo: DVC Repo object
        dir_hash: The .dir hash
        
    Returns:
        Number of files in the directory
    """
    import json
    
    cache_dir = Path(repo.cache.local.path)
    cache_path = hash_to_cache_path(cache_dir, dir_hash)
    
    if cache_path.exists():
        try:
            with open(cache_path, 'r') as f:
                contents = json.load(f)
            return len(contents)
        except (json.JSONDecodeError, OSError):
            pass
    
    return 1


def get_cached_size(cache_dir: Path, file_info: Dict[str, Any]) -> int:
    """Get the actual cached size for a file.
    
    Args:
        cache_dir: Path to cache directory
        file_info: File info dict with 'hash', 'is_dir'
        
    Returns:
        Size in bytes of cached data (0 if not cached)
    """
    import json
    
    file_hash = file_info['hash']
    cache_path = hash_to_cache_path(cache_dir, file_hash)
    
    if not cache_path.exists():
        return 0
    
    if file_info['is_dir']:
        # For directories, sum up sizes of all contained files
        total = 0
        try:
            dir_size = cache_path.stat().st_size
            total += dir_size  # The .dir file itself
            
            with open(cache_path, 'r') as f:
                contents = json.load(f)
            
            for entry in contents:
                if 'md5' in entry:
                    child_path = hash_to_cache_path(cache_dir, entry['md5'])
                    if child_path.exists():
                        try:
                            total += child_path.stat().st_size
                        except OSError:
                            pass
        except (json.JSONDecodeError, OSError):
            pass
        
        return total
    else:
        try:
            return cache_path.stat().st_size
        except OSError:
            return 0


def get_cached_file_count(cache_dir: Path, file_info: Dict[str, Any]) -> int:
    """Get the count of cached files for a tracked entry.
    
    Args:
        cache_dir: Path to cache directory
        file_info: File info dict with 'hash', 'is_dir'
        
    Returns:
        Number of cached files (0 if not cached)
    """
    import json
    
    file_hash = file_info['hash']
    cache_path = hash_to_cache_path(cache_dir, file_hash)
    
    if not cache_path.exists():
        return 0
    
    if file_info['is_dir']:
        count = 0
        try:
            with open(cache_path, 'r') as f:
                contents = json.load(f)
            
            for entry in contents:
                if 'md5' in entry:
                    child_path = hash_to_cache_path(cache_dir, entry['md5'])
                    if child_path.exists():
                        count += 1
        except (json.JSONDecodeError, OSError):
            pass
        
        return count
    else:
        return 1


def aggregate_by_depth(
    files: List[Dict[str, Any]],
    max_depth: Optional[int],
) -> List[Dict[str, Any]]:
    """Aggregate file sizes by directory depth.
    
    Args:
        files: List of file info dicts
        max_depth: Maximum depth to show (None for unlimited)
        
    Returns:
        Aggregated list of entries
    """
    if max_depth is None:
        return files
    
    # Group by truncated path
    aggregated: Dict[str, Dict[str, Any]] = {}
    
    for f in files:
        path = f['path']
        parts = path.split('/')
        
        if len(parts) <= max_depth + 1:
            # Within depth limit, keep as-is
            key = path
        else:
            # Beyond depth limit, aggregate to parent
            key = '/'.join(parts[:max_depth + 1])
        
        if key not in aggregated:
            aggregated[key] = {
                'path': key,
                'size': 0,
                'cached_size': 0,
                'nfiles': 0,
                'cached_nfiles': 0,
            }
        
        aggregated[key]['size'] += f.get('size', 0)
        aggregated[key]['cached_size'] += f.get('cached_size', 0)
        aggregated[key]['nfiles'] += f.get('nfiles', 1)
        aggregated[key]['cached_nfiles'] += f.get('cached_nfiles', 0)
    
    return list(aggregated.values())


def calculate_du(
    targets: Optional[List[str]] = None,
    cached: bool = True,
    max_depth: Optional[int] = None,
    count_inodes: bool = False,
) -> List[Tuple[int, str]]:
    """Calculate disk usage for DVC-tracked files.
    
    Args:
        targets: Optional targets to report on
        cached: If True, report cached sizes; if False, report expected sizes
        max_depth: Maximum directory depth (None for unlimited)
        count_inodes: If True, count files instead of bytes
        
    Returns:
        List of (size_or_count, path) tuples, sorted by size ascending
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise DuError(str(e))
    
    # Collect all tracked files
    files = collect_tracked_files(targets)
    
    if not files:
        return []
    
    # Get cache directory for cached size lookup
    cache_dir = get_cache_dir()
    
    # Add cached sizes/counts to file info
    for f in files:
        if cache_dir:
            f['cached_size'] = get_cached_size(cache_dir, f)
            f['cached_nfiles'] = get_cached_file_count(cache_dir, f)
        else:
            f['cached_size'] = 0
            f['cached_nfiles'] = 0
    
    # Aggregate by depth if needed
    if max_depth is not None:
        files = aggregate_by_depth(files, max_depth)
    
    # Build result
    results = []
    for f in files:
        if count_inodes:
            if cached:
                value = f.get('cached_nfiles', 0)
            else:
                value = f.get('nfiles', 1)
        else:
            if cached:
                value = f.get('cached_size', 0)
            else:
                value = f.get('size', 0)
        
        results.append((value, f['path']))
    
    # Sort by size ascending
    results.sort(key=lambda x: x[0])
    
    return results
