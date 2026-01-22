"""Checkout DVC-tracked files, searching across multiple cache directories.

Enables checking out files from alternate caches (other projects' remotes)
without copying them to the local cache first.
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


def checkout_with_cache(
    cache_dir: str,
    targets: List[str],
    extra_args: List[str],
    allow_missing: bool = True,
) -> Tuple[bool, str]:
    """Run dvc checkout with a specific cache directory.
    
    Args:
        cache_dir: Path to the cache directory to use.
        targets: DVC targets to checkout (empty for all).
        extra_args: Additional arguments to pass to dvc checkout.
        allow_missing: Add --allow-missing flag (default True for multi-cache).
        
    Returns:
        Tuple of (success, output).
    """
    # Build the command
    cmd = ['dvc', 'checkout']
    
    if allow_missing:
        cmd.append('--allow-missing')
    
    cmd.extend(extra_args)
    cmd.extend(targets)
    
    # Set cache dir via environment variable for this run
    # This is cleaner than modifying config
    env = None
    if cache_dir:
        import os
        env = os.environ.copy()
        # DVC_CACHE_DIR environment variable overrides config
        env['DVC_CACHE_DIR'] = cache_dir
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output.strip()
    except Exception as e:
        return False, str(e)


def checkout(
    targets: Optional[List[str]] = None,
    extra_args: Optional[List[str]] = None,
    verbose: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Checkout DVC-tracked files, searching across all caches.
    
    Iterates through the primary cache and all alternate caches,
    running dvc checkout with --allow-missing for each.
    
    Args:
        targets: DVC targets to checkout (None for all).
        extra_args: Additional arguments to pass to dvc checkout.
        verbose: Print progress messages.
        
    Returns:
        List of (cache_path, success, output) tuples.
        
    Raises:
        CheckoutError: If no caches are configured.
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise CheckoutError(str(e))
    
    caches = get_all_caches()
    
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
            allow_missing=True,
        )
        
        results.append((cache_dir, success, output))
    
    return results
