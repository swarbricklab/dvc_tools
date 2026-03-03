"""Disk usage reporting for DVC-tracked files.

Reports sizes of DVC-tracked files, similar to the standard `du` command.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import utils
from .errors import DuError


# Re-export from utils for internal use
get_cache_dir = utils.get_cache_dir
hash_to_cache_path = utils.hash_to_cache_path


def _normalize_path(path: str) -> str:
    """Normalize a path for consistent matching.
    
    Removes trailing slashes and leading ./ for consistent comparison.
    """
    path = path.rstrip('/')
    if path.startswith('./'):
        path = path[2:]
    return path


def _path_matches_prefix(entry_path: str, prefix: str) -> bool:
    """Check if an entry path matches a target prefix.
    
    Args:
        entry_path: The path of the tracked entry (e.g., 'data/images/foo.dvc')
        prefix: The target prefix to match (e.g., 'data/images')
        
    Returns:
        True if entry_path equals prefix or is under prefix directory.
    """
    entry_path = _normalize_path(entry_path)
    prefix = _normalize_path(prefix)
    
    if not prefix:
        return True
    
    # Exact match
    if entry_path == prefix:
        return True
    
    # Entry is under the prefix directory
    if entry_path.startswith(prefix + '/'):
        return True
    
    return False


def collect_tracked_files(
    targets: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Collect all DVC-tracked files with their metadata.
    
    If targets are specified, this function supports both:
    - Exact matches (e.g., 'data.csv.dvc' or 'data/')
    - Path prefixes (e.g., 'data/images' to match all targets under that path)
    
    Args:
        targets: Optional list of targets. If None, collects all tracked files.
                 Targets can be exact paths or path prefixes.
        
    Returns:
        List of dicts with 'path', 'hash', 'size', 'nfiles' (for dirs), 'is_dir'
    """
    try:
        # First, try to collect with exact targets (for .dvc files and direct matches)
        try:
            result = utils.collect_tracked_entries(targets=targets, push=False)
            entries = result['entries']
            
            # If we got entries with exact targets, return them
            if entries or targets is None:
                return entries
        except Exception:
            # DVC raises NoOutputOrStageError when target doesn't match
            # Fall through to path prefix matching
            pass
        
        # No entries found with exact targets - try path prefix matching
        # Collect all entries and filter by path prefixes
        all_result = utils.collect_tracked_entries(targets=None, push=False)
        all_entries = all_result['entries']
        
        if not all_entries:
            return []
        
        # Filter entries by path prefix
        filtered = []
        for entry in all_entries:
            entry_path = entry['path']
            for target in targets:
                if _path_matches_prefix(entry_path, target):
                    filtered.append(entry)
                    break
        
        return filtered
        
    except utils.DependencyError as e:
        raise DuError(str(e))


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
    mode: str = 'both',
    max_depth: Optional[int] = None,
    count_inodes: bool = False,
) -> Tuple[List[Tuple], List[str]]:
    """Calculate disk usage for DVC-tracked files.
    
    Args:
        targets: Optional targets to report on
        mode: 'both' for cached & expected, 'cached' for cached only,
              'expected' for expected only
        max_depth: Maximum directory depth (None for unlimited)
        count_inodes: If True, count files instead of bytes
        
    Returns:
        Tuple of (results, warnings) where:
        - results: List of tuples (format depends on mode):
          - 'both': (cached_value, expected_value, path)
          - 'cached'/'expected': (value, path)
        - warnings: List of warning messages
    """
    warnings = []
    
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise DuError(str(e))
    
    # Collect all tracked files
    files = collect_tracked_files(targets)
    
    if not files:
        return [], warnings
    
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
    
    # Check for missing metadata and add warning if needed
    if mode in ('both', 'expected'):
        missing_metadata_count = sum(1 for f in files if f.get('size', 0) == 0)
        if missing_metadata_count > 0:
            warnings.append(
                f"{missing_metadata_count} file(s) have no size metadata. "
                f"Run 'dt update' to populate."
            )
    
    # Aggregate by depth if needed
    if max_depth is not None:
        files = aggregate_by_depth(files, max_depth)
    
    # Build result based on mode
    results = []
    for f in files:
        if mode == 'both':
            # Return 3-tuple: (cached, expected, path)
            if count_inodes:
                cached_val = f.get('cached_nfiles', 0)
                expected_val = f.get('nfiles', 1)
            else:
                cached_val = f.get('cached_size', 0)
                expected_val = f.get('size', 0)
            results.append((cached_val, expected_val, f['path']))
        else:
            # Return 2-tuple: (value, path)
            if count_inodes:
                if mode == 'cached':
                    value = f.get('cached_nfiles', 0)
                else:
                    value = f.get('nfiles', 1)
            else:
                if mode == 'cached':
                    value = f.get('cached_size', 0)
                else:
                    value = f.get('size', 0)
            results.append((value, f['path']))
    
    # Sort by size ascending (use first value for both and single modes)
    results.sort(key=lambda x: x[0])
    
    return results, warnings
