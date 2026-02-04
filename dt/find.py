"""Find DVC-tracked files by hash.

Reverse lookup: given a hash, find which workspace path(s) it corresponds to.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from .errors import FindError
from .utils import get_cache_dir, hash_to_cache_path


def find_by_hash(
    file_hash: str,
    expand_dirs: bool = True,
    show_dvc_file: bool = False,
    show_dir_file: bool = False,
    show_cache_path: bool = False,
    rev: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Find workspace path(s) for a given hash.
    
    Searches both top-level tracked files and files within tracked directories.
    
    Args:
        file_hash: MD5 hash to search for (can be partial, minimum 4 chars)
        expand_dirs: If True, search inside .dir manifests for individual files
        show_dvc_file: If True, include the .dvc file path in results
        show_dir_file: If True, include the .dir file hash if file is in a directory
        show_cache_path: If True, include the full cache path
        rev: Git revision to search (default: current workspace)
        
    Returns:
        List of matches, each containing:
            - path: Workspace path to the file
            - hash: Full MD5 hash
            - dvc_file: Path to .dvc file (if show_dvc_file)
            - dir_hash: Parent .dir hash (if show_dir_file and file is nested)
            - cache_path: Full path in cache (if show_cache_path)
            
    Raises:
        FindError: If hash is invalid or search fails
    """
    if len(file_hash) < 4:
        raise FindError("Hash must be at least 4 characters")
    
    # Normalize hash (remove .dir suffix for matching)
    search_hash = file_hash.replace('.dir', '').lower()
    
    try:
        from dvc.repo import Repo
    except ImportError as e:
        raise FindError(f"DVC not available: {e}")
    
    try:
        repo = Repo()
    except Exception as e:
        raise FindError(f"Not in a DVC repository: {e}")
    
    results = []
    cache_dir = get_cache_dir()
    
    for out in repo.index.outs:
        if not out.hash_info or not out.hash_info.value:
            continue
        
        out_hash = out.hash_info.value.replace('.dir', '').lower()
        workspace_path = str(out.fs_path)
        dvc_file = str(out.stage.path) if out.stage else None
        
        # Check if this output matches
        if out_hash.startswith(search_hash) or search_hash.startswith(out_hash):
            result = {
                'path': workspace_path,
                'hash': out.hash_info.value,
            }
            if show_dvc_file and dvc_file:
                result['dvc_file'] = dvc_file
            if show_cache_path and cache_dir:
                result['cache_path'] = str(hash_to_cache_path(cache_dir, out.hash_info.value))
            results.append(result)
        
        # If it's a directory and we want to expand, search inside
        elif expand_dirs and out.hash_info.isdir:
            try:
                obj = out.get_obj()
                if obj:
                    dir_hash = out.hash_info.value
                    for key, (meta, hash_info) in obj.iteritems():
                        if not hash_info or not hash_info.value:
                            continue
                        
                        item_hash = hash_info.value.lower()
                        if item_hash.startswith(search_hash) or search_hash.startswith(item_hash):
                            relpath = '/'.join(key)
                            full_path = str(Path(out.fs_path) / relpath)
                            
                            result = {
                                'path': full_path,
                                'hash': hash_info.value,
                            }
                            if show_dvc_file and dvc_file:
                                result['dvc_file'] = dvc_file
                            if show_dir_file:
                                result['dir_hash'] = dir_hash
                            if show_cache_path and cache_dir:
                                result['cache_path'] = str(hash_to_cache_path(cache_dir, hash_info.value))
                            results.append(result)
            except Exception:
                # Directory tree not available (not in cache)
                pass
    
    return results


def format_results(
    results: List[Dict[str, Any]],
    verbose: bool = False,
    json_output: bool = False,
) -> str:
    """Format find results for display.
    
    Args:
        results: List of find results
        verbose: Show additional details
        json_output: Output as JSON
        
    Returns:
        Formatted string
    """
    import json
    
    if json_output:
        return json.dumps(results, indent=2)
    
    if not results:
        return "No matches found"
    
    lines = []
    for r in results:
        if verbose:
            parts = [r['path']]
            if 'dvc_file' in r:
                parts.append(f"(dvc: {r['dvc_file']})")
            if 'dir_hash' in r:
                parts.append(f"(dir: {r['dir_hash'][:16]}...)")
            if 'cache_path' in r:
                parts.append(f"\n  cache: {r['cache_path']}")
            lines.append(' '.join(parts))
        else:
            lines.append(r['path'])
    
    return '\n'.join(lines)
