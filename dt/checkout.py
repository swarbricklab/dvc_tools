"""Checkout DVC-tracked files, searching across multiple cache directories.

Enables checking out files from alternate caches (other projects' remotes)
without copying them to the local cache first.

Strategy: Temporarily swap the local cache config for each cache directory,
run dvc checkout with --allow-missing, then restore the original config.
"""

import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from . import config as cfg
from . import utils


class CheckoutError(Exception):
    """Raised when checkout operations fail."""
    pass


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
