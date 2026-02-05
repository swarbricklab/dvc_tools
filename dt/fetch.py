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


def _is_ignored(path: Path) -> bool:
    """Check if a path is ignored by git or dvc.
    
    Args:
        path: Path to check.
        
    Returns:
        True if the path is ignored, False otherwise.
    """
    # Check git ignore
    try:
        result = subprocess.run(
            ['git', 'check-ignore', '-q', str(path)],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True
    except (OSError, FileNotFoundError):
        pass
    
    # Check dvc ignore (.dvcignore)
    # DVC doesn't have a dedicated command, so check common patterns
    dvcignore = Path('.dvcignore')
    if dvcignore.exists():
        try:
            patterns = dvcignore.read_text().splitlines()
            path_str = str(path)
            for pattern in patterns:
                pattern = pattern.strip()
                if not pattern or pattern.startswith('#'):
                    continue
                # Simple glob matching
                import fnmatch
                if fnmatch.fnmatch(path_str, pattern) or fnmatch.fnmatch(path_str, f'*/{pattern}'):
                    return True
        except OSError:
            pass
    
    return False


def _update_dvc_hash(dvc_path: Path, old_hash: str, new_hash: str, verbose: bool = False) -> None:
    """Update the MD5 hash in a .dvc file.
    
    For import files, the hash is in outs with a .dir suffix (e.g., "abc123.dir").
    
    Args:
        dvc_path: Path to the .dvc file.
        old_hash: The old hash to replace (without .dir suffix).
        new_hash: The new hash to use.
        verbose: Print progress messages.
    """
    import yaml
    
    try:
        content = dvc_path.read_text()
        data = yaml.safe_load(content)
        
        # Update the outs section - check for both exact hash and hash.dir format
        modified = False
        for out in data.get('outs', []):
            out_md5 = out.get('md5', '')
            # Handle both "hash" and "hash.dir" formats
            if out_md5 == old_hash:
                out['md5'] = new_hash
                modified = True
            elif out_md5 == f"{old_hash}.dir":
                out['md5'] = f"{new_hash}.dir"
                modified = True
        
        if modified:
            # Write back with same formatting
            with open(dvc_path, 'w') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            if verbose:
                print(f"  Updated .dvc file hash: {old_hash[:12]}... -> {new_hash[:12]}...")
        elif verbose:
            print(f"  Warning: Could not find hash {old_hash[:12]}... in .dvc file to update")
            
    except Exception as e:
        if verbose:
            print(f"  Warning: Could not update .dvc file: {e}")


def _populate_cache_from_source(
    dvc_path: Path,
    source_cache: str,
    verbose: bool = False,
    rev_lock: Optional[str] = None,
    source_url: Optional[str] = None,
    update: bool = False,
) -> Tuple[int, int]:
    """Populate the primary cache from a source cache.
    
    Creates symlinks in the primary cache pointing to files in the source cache.
    Respects the .dvc file format (v2 vs v3) when determining cache layout.
    
    For directory imports, if the .dir file doesn't exist in the source cache,
    it will be constructed using 'dvc list' to query the source repository.
    
    Args:
        dvc_path: Path to the .dvc file.
        source_cache: Path to the source cache.
        verbose: Print progress messages.
        rev_lock: Git revision for constructing .dir files via dvc list.
        source_url: URL of the source repository (for dvc list).
        update: If True, create .dir file with computed hash and update .dvc file.
        
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
    
    # Show cache destination path in verbose mode
    if use_v3_layout:
        cache_dest_path = Path(cache_base) / 'files' / 'md5'
    else:
        cache_dest_path = Path(cache_base)
    
    if verbose:
        layout = "v3 (files/md5/)" if use_v3_layout else "v2 (legacy)"
        print(f"  DVC file format: {layout}")
        print(f"  Cache destination: {cache_dest_path}")
    
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
        
        # First check if .dir file already exists in destination cache
        # (it may have been created by the original dvc import)
        if use_v3_layout:
            dest_dir_file = Path(cache_base) / 'files' / 'md5' / dir_hash[:2] / f"{dir_hash[2:]}.dir"
        else:
            dest_dir_file = Path(cache_base) / dir_hash[:2] / f"{dir_hash[2:]}.dir"
        
        dir_file = None
        entries = None
        
        if dest_dir_file.exists():
            if verbose:
                print(f"  .dir file already in primary cache: {dest_dir_file}")
            dir_file = dest_dir_file
        else:
            # Try DVC v3 path first, then v2 path in source cache
            dir_file_v3 = Path(source_cache) / 'files' / 'md5' / dir_hash[:2] / f"{dir_hash[2:]}.dir"
            dir_file_v2 = Path(source_cache) / dir_hash[:2] / f"{dir_hash[2:]}.dir"
            
            if dir_file_v3.exists():
                dir_file = dir_file_v3
            elif dir_file_v2.exists():
                dir_file = dir_file_v2
        
        if dir_file is None:
            # .dir file not in source cache or dest cache - construct it using dvc list
            # This works for both regular directories and nested DVC imports
            if source_url:
                deps = dvc_data.get('deps', [])
                if deps:
                    dep_path = deps[0].get('path', '')
                    if dep_path:
                        if verbose:
                            print(f"  .dir file not in cache, using dvc list to build manifest...")
                        
                        result = import_mod.construct_dir_from_dvc_list(
                            repo_url=source_url,
                            path=dep_path,
                            revision=rev_lock,
                            expected_hash=dir_hash,
                            dest_cache=cache_base,
                            use_v3_layout=use_v3_layout,
                            verbose=verbose,
                            update=update,
                        )
                        
                        if result is None:
                            if verbose:
                                print(f"  ERROR: Could not construct .dir file from dvc list")
                            failed += 1
                        else:
                            entries, new_hash = result
                            # If hash changed, update the .dvc file
                            if new_hash and new_hash != dir_hash:
                                _update_dvc_hash(dvc_path, dir_hash, new_hash, verbose)
                                dir_hash = new_hash
            else:
                if verbose:
                    print(f"  .dir file not found in cache and no source URL available")
                failed += 1
        
        # Read entries from existing .dir file
        if dir_file and entries is None:
            try:
                import json
                entries = json.loads(dir_file.read_text())
            except (json.JSONDecodeError, KeyError) as e:
                if verbose:
                    print(f"Warning: Could not parse .dir file: {e}")
                entries = None
        
        # Populate individual files from entries
        if entries:
            for entry in entries:
                file_md5 = entry.get('md5', '')
                relpath = entry.get('relpath', file_md5[:12])  # Use relpath if available
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
                            cache_file = Path(cache_base) / 'files' / 'md5' / file_md5[:2] / file_md5[2:]
                        else:
                            cache_file = Path(cache_base) / file_md5[:2] / file_md5[2:]
                        if not cache_file.exists():
                            if verbose:
                                print(f"  ERROR: File not found in source cache: {relpath} ({file_md5[:12]}...)")
                            failed += 1
    
    return count, failed


def fetch_import(
    dvc_path: Path,
    verbose: bool = False,
    refresh: bool = True,  # Currently unused, kept for API compatibility
    update: bool = False,
) -> Tuple[str, int, int]:
    """Fetch an import .dvc file by finding and linking from the source cache.
    
    This handles .dvc files created by `dvc import`. It finds a locally-accessible
    cache from the source repository and populates the primary cache with symlinks.
    
    Args:
        dvc_path: Path to the .dvc file.
        verbose: Print progress messages.
        refresh: Whether to refresh the temp clone (currently unused).
        update: If True, create .dir file with computed hash and update .dvc file.
        
    Returns:
        Tuple of (source_cache_path, files_added_count, files_failed_count).
        
    Raises:
        FetchError: If fetch fails.
    """
    from . import remote as remote_mod
    
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
    
    # Step 1: Find a local remote from the source repo (clones if needed)
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
    
    # Step 2: Populate primary cache with symlinks
    if verbose:
        print(f"Populating primary cache...")
    
    count, failed = _populate_cache_from_source(
        dvc_path, cache_path, verbose,
        rev_lock=import_info.get('rev'),
        source_url=source_url,
        update=update,
    )
    
    return cache_path, count, failed


def fetch(
    targets: Optional[List[str]] = None,
    verbose: bool = False,
    refresh: bool = True,
    update: bool = False,
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
        update: If True, create .dir file with computed hash and update .dvc file.
        
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
            all_dvc_files = [p for p in Path('.').rglob('*.dvc') if not str(p).startswith('.dvc/')]
            # Filter out ignored files
            targets = []
            for p in all_dvc_files:
                if _is_ignored(p):
                    if verbose:
                        print(f"Skipping ignored file: {p}")
                else:
                    targets.append(str(p))
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
                    update=update,
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
