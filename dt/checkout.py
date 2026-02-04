"""Checkout DVC-tracked files, searching across multiple cache directories.

Enables checking out files from alternate caches (other projects' remotes)
without copying them to the local cache first.

Strategy: Temporarily swap the local cache config for each cache directory,
run dvc checkout with --allow-missing, then restore the original config.

Also handles import .dvc files (those with a deps section) by cloning the
source repository and finding a locally-accessible cache.
"""

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import config as cfg
from . import utils


class CheckoutError(Exception):
    """Raised when checkout operations fail."""
    pass


# Use shared DVC file utilities from utils
parse_dvc_file = utils.parse_dvc_file
load_dvc_file = utils.load_dvc_file


def is_import_dvc(dvc_data: Dict[str, Any]) -> bool:
    """Check if a .dvc file represents an import (has deps section).
    
    DEPRECATED: Use utils.is_repo_import(path) for new code.
    This function is kept for compatibility with code that already
    has parsed dvc_data.
    
    Args:
        dvc_data: Parsed .dvc file contents.
        
    Returns:
        True if this is an import .dvc file.
    """
    deps = dvc_data.get('deps', [])
    if not deps:
        return False
    
    # Check for repo section in deps
    for dep in deps:
        if 'repo' in dep:
            return True
    
    return False


def get_import_info(dvc_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract import information from a .dvc file.
    
    DEPRECATED: Use utils.get_import_info(path) for new code.
    This function is kept for compatibility with code that already
    has parsed dvc_data.
    
    Args:
        dvc_data: Parsed .dvc file contents.
        
    Returns:
        Dictionary with 'url', 'rev', and 'path' keys, or None if not an import.
    """
    deps = dvc_data.get('deps', [])
    for dep in deps:
        repo = dep.get('repo', {})
        if repo:
            return {
                'url': repo.get('url'),
                'rev': repo.get('rev_lock') or repo.get('rev'),
                'path': dep.get('path'),
            }
    return None


def get_primary_cache() -> Optional[str]:
    """Get the primary DVC cache directory.
    
    Returns:
        Path to the primary cache, or None if not configured.
    """
    try:
        result = subprocess.run(
            ['dvc', 'cache', 'dir'],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return None


def get_cache_by_name(name: str) -> Optional[str]:
    """Get a cache directory by name or path.
    
    Searches:
    1. Exact path match in primary or alt caches
    2. Basename match (e.g., 'neochemo' matches '/g/data/.../neochemo')
    
    Args:
        name: Cache name or path to search for.
        
    Returns:
        Full path to the cache, or None if not found.
    """
    all_caches = get_all_caches()
    
    # Exact match first
    for cache in all_caches:
        if cache == name:
            return cache
    
    # Basename match
    for cache in all_caches:
        if Path(cache).name == name:
            return cache
    
    return None


def get_all_caches() -> List[str]:
    """Get all cache directories to search.
    
    Returns caches in order:
    1. Primary DVC cache
    2. Alternate caches from config (in precedence order)
    
    Returns:
        List of cache directory paths.
    """
    caches = []
    
    # Primary cache first
    primary = get_primary_cache()
    if primary:
        caches.append(primary)
    
    # Alternate caches from config
    alt_caches = cfg.get_list_value('cache.alt')
    for path, _scope in alt_caches:
        if path not in caches:
            caches.append(path)
    
    return caches


def _set_cache_dir(cache_dir: str) -> Tuple[bool, str]:
    """Set the DVC cache directory in local config.
    
    Args:
        cache_dir: Path to the cache directory.
        
    Returns:
        Tuple of (success, error_message).
    """
    try:
        result = subprocess.run(
            ['dvc', 'cache', 'dir', '--local', cache_dir],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return False, result.stderr.strip()
        return True, ""
    except Exception as e:
        return False, str(e)


def _unset_cache_dir() -> Tuple[bool, str]:
    """Unset the DVC cache directory in local config.
    
    Returns:
        Tuple of (success, error_message).
    """
    try:
        result = subprocess.run(
            ['dvc', 'cache', 'dir', '--local', '--unset'],
            capture_output=True,
            text=True,
        )
        # --unset may fail if not set, which is fine
        return True, ""
    except Exception as e:
        return False, str(e)


def _get_local_cache_config() -> Optional[str]:
    """Get the current local cache.dir config value.
    
    Returns:
        The local cache.dir value, or None if not set.
    """
    try:
        result = subprocess.run(
            ['dvc', 'config', '--local', 'cache.dir'],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return None


def checkout_with_cache(
    cache_dir: str,
    targets: List[str],
    extra_args: List[str],
    allow_missing: bool = True,
) -> Tuple[bool, str]:
    """Run dvc checkout with a specific cache directory.
    
    Temporarily sets the local cache config, runs checkout, then restores.
    
    Args:
        cache_dir: Path to the cache directory to use.
        targets: DVC targets to checkout (empty for all).
        extra_args: Additional arguments to pass to dvc checkout.
        allow_missing: Add --allow-missing flag (default True for multi-cache).
        
    Returns:
        Tuple of (success, output).
    """
    # Save original local cache config
    original_cache = _get_local_cache_config()
    
    try:
        # Set the cache directory temporarily
        success, err = _set_cache_dir(cache_dir)
        if not success:
            return False, f"Failed to set cache dir: {err}"
        
        # Build the checkout command
        cmd = ['dvc', 'checkout']
        
        if allow_missing:
            cmd.append('--allow-missing')
        
        cmd.extend(extra_args)
        cmd.extend(targets)
        
        # Run checkout
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output.strip()
        
    except Exception as e:
        return False, str(e)
        
    finally:
        # Restore original cache config
        if original_cache:
            _set_cache_dir(original_cache)
        else:
            _unset_cache_dir()


def checkout(
    targets: Optional[List[str]] = None,
    extra_args: Optional[List[str]] = None,
    verbose: bool = False,
    cache: Optional[str] = None,
) -> List[Tuple[str, bool, str]]:
    """Checkout DVC-tracked files, searching across caches.
    
    If `cache` is specified, only that cache is used and --allow-missing
    is NOT passed (checkout will fail if files are missing).
    
    Otherwise, iterates through all caches with --allow-missing.
    
    Args:
        targets: DVC targets to checkout (None for all).
        extra_args: Additional arguments to pass to dvc checkout.
        verbose: Print progress messages.
        cache: Specific cache name/path to use (exclusive mode).
        
    Returns:
        List of (cache_path, success, output) tuples.
        
    Raises:
        CheckoutError: If no caches are configured or named cache not found.
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise CheckoutError(str(e))
    
    # Single cache mode (no --allow-missing)
    if cache:
        cache_path = get_cache_by_name(cache)
        if not cache_path:
            # Try as literal path
            if Path(cache).exists():
                cache_path = str(Path(cache).resolve())
            else:
                raise CheckoutError(
                    f"Cache not found: {cache}\n"
                    f"Available caches: {', '.join(get_all_caches()) or '(none)'}"
                )
        caches = [cache_path]
        allow_missing = False
    else:
        caches = get_all_caches()
        allow_missing = True
    
    if not caches:
        raise CheckoutError(
            "No cache directories configured.\n"
            "Set up a primary cache with: dvc cache dir /path/to/cache\n"
            "Or add alternate caches with: dt cache add /path/to/cache"
        )
    
    targets = targets or []
    extra_args = extra_args or []
    
    results = []
    
    for cache_dir in caches:
        if verbose:
            print(f"Checking cache: {cache_dir}")
        
        success, output = checkout_with_cache(
            cache_dir=cache_dir,
            targets=targets,
            extra_args=extra_args,
            allow_missing=allow_missing,
        )
        
        results.append((cache_dir, success, output))
    
    return results


def smart_checkout(
    targets: Optional[List[str]] = None,
    extra_args: Optional[List[str]] = None,
    verbose: bool = False,
    cache: Optional[str] = None,
    refresh: bool = True,
) -> List[Tuple[str, bool, str]]:
    """Checkout DVC-tracked files, automatically handling imports.
    
    This is the main entry point for dt checkout. It:
    1. Detects if any targets are import .dvc files (have deps section)
    2. For imports, clones the source repo and finds a local cache
    3. For regular files, uses the standard multi-cache checkout
    
    Args:
        targets: DVC targets to checkout (None for all).
        extra_args: Additional arguments to pass to dvc checkout.
        verbose: Print progress messages.
        cache: Specific cache name/path to use (exclusive mode).
        refresh: Whether to refresh temp clones (default True).
        
    Returns:
        List of (cache_path, success, output) tuples.
        
    Raises:
        CheckoutError: If checkout fails.
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise CheckoutError(str(e))
    
    targets = targets or []
    extra_args = extra_args or []
    
    # Separate import targets from regular targets
    import_targets = []
    regular_targets = []
    
    for target in targets:
        target_path = Path(target)
        
        # Only check .dvc files that exist
        if target_path.suffix == '.dvc' and target_path.exists():
            try:
                dvc_data = parse_dvc_file(target_path)
                if is_import_dvc(dvc_data):
                    import_targets.append((target_path, dvc_data))
                    continue
            except CheckoutError:
                pass  # Fall through to regular checkout
        
        regular_targets.append(target)
    
    all_results = []
    
    # Handle import targets
    for dvc_path, dvc_data in import_targets:
        if verbose:
            print(f"Detected import: {dvc_path}")
        
        try:
            results = checkout_import(
                dvc_path=dvc_path,
                dvc_data=dvc_data,
                extra_args=extra_args,
                verbose=verbose,
                refresh=refresh,
            )
            all_results.extend(results)
        except CheckoutError as e:
            # Report as failure but continue with other targets
            all_results.append((str(dvc_path), False, str(e)))
    
    # Handle regular targets (or all if no specific targets)
    if regular_targets or not targets:
        results = checkout(
            targets=regular_targets if regular_targets else None,
            extra_args=extra_args,
            verbose=verbose,
            cache=cache,
        )
        all_results.extend(results)
    
    return all_results


def checkout_import(
    dvc_path: Path,
    dvc_data: Dict[str, Any],
    extra_args: Optional[List[str]] = None,
    verbose: bool = False,
    refresh: bool = True,
) -> List[Tuple[str, bool, str]]:
    """Checkout an import .dvc file by finding the source cache.
    
    This handles .dvc files created by `dvc import` without assuming
    dt import setup (no pre-existing alt cache or temporary clone).
    
    Args:
        dvc_path: Path to the .dvc file.
        dvc_data: Parsed .dvc file contents.
        extra_args: Additional arguments to pass to dvc checkout.
        verbose: Print progress messages.
        refresh: Whether to refresh the temp clone (default True).
    
    The process:
    1. Parse the deps section to get the source repo URL and path
    2. Clone the source repo (sparsely) to find its DVC config
    3. Find a locally-accessible remote from the source repo
    4. Add that cache to cache.alt and run checkout
    5. Optionally populate the primary cache
    
    Args:
        dvc_path: Path to the .dvc file.
        dvc_data: Parsed contents of the .dvc file.
        extra_args: Additional arguments to pass to dvc checkout.
        verbose: Print progress messages.
        
    Returns:
        List of (cache_path, success, output) tuples.
        
    Raises:
        CheckoutError: If checkout fails.
    """
    # Import here to avoid circular imports
    from . import remote as remote_mod
    from . import tmp as tmp_mod
    
    import_info = get_import_info(dvc_data)
    if not import_info:
        raise CheckoutError(f"Not an import .dvc file: {dvc_path}")
    
    source_url = import_info['url']
    if not source_url:
        raise CheckoutError(f"No source URL in import: {dvc_path}")
    
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
        raise CheckoutError(f"Failed to clone source repository: {e}")
    
    # Step 2: Find a local remote from the source repo
    if verbose:
        print(f"Looking for local cache...")
    
    result = remote_mod.find_local_remote_from_repo(repo_spec=source_url)
    
    if not result:
        # Also check if any existing alt cache might work
        # (e.g., user manually configured it)
        alt_caches = cfg.get_list_value('cache.alt')
        available_caches = [c for c, _ in alt_caches if Path(c).exists()]
        
        if available_caches:
            if verbose:
                print(f"No local remote found, trying existing alt caches...")
            
            # Try checkout with existing caches
            return checkout(
                targets=[str(dvc_path)],
                extra_args=extra_args,
                verbose=verbose,
            )
        
        raise CheckoutError(
            f"No locally-accessible cache found for {source_url}.\n"
            f"Options:\n"
            f"  1. Run: dt cache add-from <repo>  (if source has local remote)\n"
            f"  2. Run: dt cache add /path/to/cache  (if you know the cache path)\n"
            f"  3. Run: dt import {source_url} <path>  (to re-import with dt)"
        )
    
    remote_name, cache_path = result
    
    if verbose:
        print(f"Found local cache: {cache_path} (from remote '{remote_name}')")
    
    # Step 3: Add to alt caches if not already there
    cfg.add_list_value('cache.alt', cache_path, 'local')
    
    # Step 4: Run checkout with this cache
    results = checkout(
        targets=[str(dvc_path)],
        extra_args=extra_args,
        verbose=verbose,
    )
    
    # Step 5: Populate primary cache if checkout succeeded
    any_success = any(success for _, success, _ in results)
    if any_success:
        _populate_cache_from_dvc(dvc_path, dvc_data, cache_path, verbose)
    
    return results


def _populate_cache_from_dvc(
    dvc_path: Path,
    dvc_data: Dict[str, Any],
    source_cache: str,
    verbose: bool = False,
) -> None:
    """Populate the primary cache after checking out an import.
    
    Args:
        dvc_path: Path to the .dvc file.
        dvc_data: Parsed contents of the .dvc file.
        source_cache: Path to the cache used for checkout.
        verbose: Print progress messages.
    """
    # Import here to avoid circular imports
    from . import import_data as import_mod
    
    primary_cache = get_primary_cache()
    if not primary_cache:
        return
    
    if verbose:
        print(f"Populating primary cache...")
    
    outs = dvc_data.get('outs', [])
    if not outs:
        return
    
    out = outs[0]
    md5 = out.get('md5', '')
    out_path = dvc_path.parent / out.get('path', '')
    
    # Handle the root hash (.dir file or single file)
    if md5:
        import_mod.populate_cache_file(
            md5=md5,
            source_cache=source_cache,
            dest_cache=primary_cache,
            verbose=verbose,
        )
    
    # For directories, also populate individual files
    if md5.endswith('.dir') and out_path.exists():
        # Read the .dir file to get individual file hashes
        dir_hash = md5[:-4]  # Remove .dir suffix
        dir_file = Path(source_cache) / 'files' / 'md5' / dir_hash[:2] / f"{dir_hash[2:]}.dir"
        
        if dir_file.exists():
            try:
                import json
                entries = json.loads(dir_file.read_text())
                
                files = [
                    {'md5': e['md5'], 'path': e['relpath']}
                    for e in entries
                ]
                
                count = import_mod.populate_primary_cache(
                    files=files,
                    workspace_path=out_path,
                    primary_cache=primary_cache,
                    verbose=verbose,
                )
                
                if verbose and count > 0:
                    print(f"Added {count} file(s) to primary cache")
                    
            except (json.JSONDecodeError, KeyError) as e:
                if verbose:
                    print(f"Warning: Could not parse .dir file: {e}")
