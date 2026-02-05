"""Fetch DVC-tracked files into the primary cache.

Populates the primary cache with links/symlinks to files from source caches,
mirroring DVC's fetch concept (remote → cache). After fetch, regular
`dvc checkout` can link files from cache → workspace.

For import .dvc files (those with a deps section), automatically clones the
source repository to find a locally-accessible cache.

This is the "dt" equivalent of `dvc fetch`, but works with local caches
(other projects' remotes that are accessible on the same filesystem).
"""

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import utils
from .errors import FetchError


def _populate_cache_from_source(
    dvc_path: Path,
    source_cache: str,
    verbose: bool = False,
) -> Tuple[int, int]:
    """Populate the primary cache from a source cache.
    
    Creates symlinks in the primary cache pointing to files in the source cache.
    Respects the .dvc file format (v2 vs v3) when determining cache layout.
    
    Args:
        dvc_path: Path to the .dvc file.
        source_cache: Path to the source cache.
        verbose: Print progress messages.
        
    Returns:
        Tuple of (files_added, files_failed) counts.
    """
    from . import import_data as import_mod
    
    primary_cache = utils.get_cache_dir()
    if not primary_cache:
        return 0, 0
    
    # Parse the .dvc file to get output info
    dvc_data = utils.parse_dvc_file(dvc_path)
    if not dvc_data:
        return 0, 0
    
    outs = dvc_data.get('outs', [])
    if not outs:
        return 0, 0
    
    out = outs[0]
    md5 = out.get('md5', '')
    
    # Detect v2 vs v3 format: v3 has explicit 'hash' field, v2 doesn't
    # This determines where dvc checkout will look for files
    use_v3_layout = import_mod.is_v3_dvc_file(dvc_data)
    
    # Get base cache directory (without files/md5 suffix)
    # repo.cache.local.path returns .../files/md5, we need the parent
    cache_base = str(primary_cache)
    if cache_base.endswith('/files/md5') or cache_base.endswith('\\files\\md5'):
        cache_base = str(Path(cache_base).parent.parent)
    
    if verbose:
        layout = "v3 (files/md5/)" if use_v3_layout else "v2 (legacy)"
        print(f"  Using {layout} cache layout")
    
    count = 0
    failed = 0
    
    # Handle the root hash (.dir file or single file)
    if md5:
        result = import_mod.populate_cache_file(
            md5=md5,
            source_cache=source_cache,
            dest_cache=cache_base,
            verbose=verbose,
            use_v3_layout=use_v3_layout,
        )
        if result:
            count += 1
        elif result is False:
            # populate_cache_file returns False for "not found" or "already exists"
            # Check if it's actually missing vs already cached
            hash_clean = md5.replace('.dir', '')
            suffix = '.dir' if md5.endswith('.dir') else ''
            if use_v3_layout:
                dest_file = Path(cache_base) / 'files' / 'md5' / hash_clean[:2] / (hash_clean[2:] + suffix)
            else:
                dest_file = Path(cache_base) / hash_clean[:2] / (hash_clean[2:] + suffix)
            if not dest_file.exists():
                failed += 1
    
    # For directories, also populate individual files
    if md5.endswith('.dir'):
        dir_hash = md5[:-4]  # Remove .dir suffix
        # Try DVC v3 path first, then v2 path
        dir_file_v3 = Path(source_cache) / 'files' / 'md5' / dir_hash[:2] / f"{dir_hash[2:]}.dir"
        dir_file_v2 = Path(source_cache) / dir_hash[:2] / f"{dir_hash[2:]}.dir"
        
        if dir_file_v3.exists():
            dir_file = dir_file_v3
        elif dir_file_v2.exists():
            dir_file = dir_file_v2
        else:
            dir_file = None
        
        if dir_file:
            try:
                import json
                entries = json.loads(dir_file.read_text())
                
                for entry in entries:
                    file_md5 = entry.get('md5', '')
                    if file_md5:
                        result = import_mod.populate_cache_file(
                            md5=file_md5,
                            source_cache=source_cache,
                            dest_cache=cache_base,
                            verbose=verbose,
                            use_v3_layout=use_v3_layout,
                        )
                        if result:
                            count += 1
                        elif result is False:
                            # Check if missing vs already cached
                            if use_v3_layout:
                                dest_file = Path(cache_base) / 'files' / 'md5' / file_md5[:2] / file_md5[2:]
                            else:
                                dest_file = Path(cache_base) / file_md5[:2] / file_md5[2:]
                            if not dest_file.exists():
                                failed += 1
                            
            except (json.JSONDecodeError, KeyError) as e:
                if verbose:
                    print(f"Warning: Could not parse .dir file: {e}")
    
    return count, failed


def fetch_import(
    dvc_path: Path,
    verbose: bool = False,
    refresh: bool = True,
) -> Tuple[str, int, int]:
    """Fetch an import .dvc file by finding and linking from the source cache.
    
    This handles .dvc files created by `dvc import`. It finds a locally-accessible
    cache from the source repository and populates the primary cache with symlinks.
    
    Args:
        dvc_path: Path to the .dvc file.
        verbose: Print progress messages.
        refresh: Whether to refresh the temp clone (default True).
        
    Returns:
        Tuple of (source_cache_path, files_added_count, files_failed_count).
        
    Raises:
        FetchError: If fetch fails.
    """
    from . import remote as remote_mod
    from . import tmp as tmp_mod
    
    import_info = utils.get_import_info(dvc_path)
    if not import_info:
        raise FetchError(f"Not an import .dvc file: {dvc_path}")
    
    source_url = import_info['url']
    if not source_url:
        raise FetchError(f"No source URL in import: {dvc_path}")
    
    if verbose:
        print(f"Import from: {source_url}")
        if import_info.get('path'):
            print(f"  Path: {import_info['path']}")
    
    # Step 1: Clone the source repo to access its DVC config
    if verbose:
        print(f"Cloning source repository...")
    
    try:
        clone_path = tmp_mod.clone_repo(source_url, refresh=refresh, verbose=verbose)
    except tmp_mod.TmpError as e:
        raise FetchError(f"Failed to clone source repository: {e}")
    
    # Step 2: Find a local remote from the source repo
    if verbose:
        print(f"Looking for local cache...")
    
    result = remote_mod.find_local_remote_from_repo(repo_spec=source_url)
    
    if not result:
        raise FetchError(
            f"No locally-accessible cache found for {source_url}.\n"
            f"The source repository's remote may not be on this filesystem.\n"
            f"Options:\n"
            f"  1. Use 'dt import {source_url} <path>' to set up proper tracking\n"
            f"  2. Use 'dvc pull' to fetch from the remote directly"
        )
    
    remote_name, cache_path = result
    
    if verbose:
        print(f"Found local cache: {cache_path} (from remote '{remote_name}')")
    
    # Step 3: Populate primary cache with symlinks
    if verbose:
        print(f"Populating primary cache...")
    
    count, failed = _populate_cache_from_source(dvc_path, cache_path, verbose)
    
    return cache_path, count, failed


def fetch(
    targets: Optional[List[str]] = None,
    verbose: bool = False,
    refresh: bool = True,
) -> List[Tuple[str, bool, str]]:
    """Fetch DVC-tracked files into the primary cache.
    
    Populates the primary cache with symlinks to files from source caches.
    This is the equivalent of `dvc fetch` but for local caches.
    
    For import .dvc files, automatically discovers the source repository's
    local cache and creates symlinks.
    
    After fetch, run `dvc checkout` to link files to the workspace.
    
    Args:
        targets: DVC targets to fetch (None for all .dvc files).
        verbose: Print progress messages.
        refresh: Whether to refresh temp clones (default True).
        
    Returns:
        List of (target, success, message) tuples.
        
    Raises:
        FetchError: If fetch fails.
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise FetchError(str(e))
    
    # If no targets specified, find all .dvc files
    if not targets:
        try:
            targets = [str(p) for p in Path('.').rglob('*.dvc') if not str(p).startswith('.dvc/')]
        except Exception as e:
            raise FetchError(f"Failed to find .dvc files: {e}")
    
    results = []
    
    for target in targets:
        target_path = Path(target)
        
        # Only process .dvc files
        if target_path.suffix != '.dvc':
            # Try adding .dvc suffix
            dvc_path = Path(str(target) + '.dvc')
            if dvc_path.exists():
                target_path = dvc_path
            else:
                results.append((target, False, f"Not a .dvc file: {target}"))
                continue
        
        if not target_path.exists():
            results.append((target, False, f"File not found: {target}"))
            continue
        
        # Check if it's an import file
        if utils.is_repo_import(target_path):
            if verbose:
                print(f"Fetching import: {target_path}")
            
            try:
                cache_path, count, failed = fetch_import(
                    dvc_path=target_path,
                    verbose=verbose,
                    refresh=refresh,
                )
                if failed > 0:
                    # Critical failure - files were expected but not found in source cache
                    results.append((str(target_path), False, 
                        f"FAILED: {failed} file(s) not found in source cache at {cache_path}"))
                elif count == 0:
                    # All files already in cache
                    results.append((str(target_path), True, f"Already in cache (from {cache_path})"))
                else:
                    results.append((str(target_path), True, f"Fetched {count} files from {cache_path}"))
            except FetchError as e:
                results.append((str(target_path), False, str(e)))
        else:
            # Regular .dvc file - check if it's already in cache
            dvc_data = utils.parse_dvc_file(target_path)
            if dvc_data:
                outs = dvc_data.get('outs', [])
                if outs:
                    md5 = outs[0].get('md5', '')
                    primary_cache = utils.get_cache_dir()
                    if primary_cache and md5:
                        hash_clean = md5.replace('.dir', '')
                        suffix = '.dir' if md5.endswith('.dir') else ''
                        cache_file = primary_cache / 'files' / 'md5' / hash_clean[:2] / (hash_clean[2:] + suffix)
                        if cache_file.exists():
                            results.append((str(target_path), True, "Already in cache"))
                        else:
                            results.append((str(target_path), False, "Not an import - use 'dvc fetch' for remote data"))
                    else:
                        results.append((str(target_path), False, "No cache configured"))
                else:
                    results.append((str(target_path), False, "No outputs in .dvc file"))
            else:
                results.append((str(target_path), False, "Could not parse .dvc file"))
    
    return results


# Keep smart_checkout as alias for backwards compatibility during transition
def smart_checkout(
    targets: Optional[List[str]] = None,
    extra_args: Optional[List[str]] = None,
    verbose: bool = False,
    cache: Optional[str] = None,
    refresh: bool = True,
) -> List[Tuple[str, bool, str]]:
    """Deprecated: Use fetch() + dvc checkout instead.
    
    This function is kept for backwards compatibility during the transition.
    It runs fetch() and then dvc checkout.
    """
    # Run fetch first
    results = fetch(
        targets=targets,
        verbose=verbose,
        refresh=refresh,
    )
    
    # Then run dvc checkout
    cmd = ['dvc', 'checkout']
    if targets:
        cmd.extend(targets)
    if extra_args:
        cmd.extend(extra_args)
    
    try:
        checkout_result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        if checkout_result.returncode != 0:
            results.append(('dvc checkout', False, checkout_result.stderr.strip()))
        elif verbose:
            if checkout_result.stdout.strip():
                print(checkout_result.stdout.strip())
    except Exception as e:
        results.append(('dvc checkout', False, str(e)))
    
    return results
