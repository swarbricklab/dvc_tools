"""Find DVC-tracked files by hash.

Reverse lookup: given a hash, find which workspace path(s) it corresponds to.
"""

import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dvc.repo import Repo

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


def find_hash_in_repo(
    target_hash: str,
    repo: Union[str, Path],
    revision: Optional[str] = None,
    exact_match: bool = True,
    verbose: bool = False,
) -> Optional[str]:
    """Find the path for a given hash in any DVC repository.
    
    Uses `dvc list --show-hash` to search for a file with the given hash.
    Works with local paths or remote URLs at any git revision.
    
    Args:
        target_hash: Hash to search for (with or without .dir suffix).
        repo: Path to local repo directory or remote URL.
        revision: Git revision to search at (default: HEAD).
        exact_match: If True, require exact hash match; if False, allow prefix.
        verbose: Print progress messages.
        
    Returns:
        Path within the repo if found, None otherwise.
    """
    # Normalize hash (remove .dir suffix for comparison)
    search_hash = target_hash.replace('.dir', '').lower()
    
    if verbose:
        rev_str = revision[:12] if revision else "HEAD"
        print(f"  Searching for hash {search_hash[:12]}... at {rev_str}...")
    
    # Build command
    cmd = ['dvc', 'list', '--json', '--show-hash', '--recursive', str(repo), '.']
    if revision:
        cmd.extend(['--rev', revision])
    
    # If repo is a local path, run from that directory
    cwd = str(repo) if Path(repo).is_dir() else None
    if cwd:
        # For local repos, use '.' as the repo arg
        cmd = ['dvc', 'list', '--json', '--show-hash', '--recursive', '.']
        if revision:
            cmd.extend(['--rev', revision])
    
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        if verbose:
            print(f"  dvc list failed: {result.stderr.strip()}")
        return None
    
    try:
        items = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    
    for item in items:
        item_hash = (item.get('md5') or '').replace('.dir', '').lower()
        if exact_match:
            if item_hash == search_hash:
                return item.get('path')
        else:
            # Prefix matching
            if item_hash.startswith(search_hash) or search_hash.startswith(item_hash):
                return item.get('path')
    
    return None


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
