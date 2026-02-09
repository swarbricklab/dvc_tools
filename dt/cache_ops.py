"""Cache population operations for DVC Tools.

Shared utilities for copying/linking files into the DVC cache.
Uses the DVC-preferred order: reflink → hardlink → symlink → copy.
"""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Literal, Optional, Tuple

LinkType = Literal['reflink', 'hardlink', 'symlink', 'copy', 'skipped', 'failed']
CacheType = Literal['reflink', 'hardlink', 'symlink', 'copy']

# Valid cache types (matches DVC's cache.type config)
VALID_CACHE_TYPES = ('reflink', 'hardlink', 'symlink', 'copy')


def _try_reflink(source: Path, dest: Path) -> bool:
    """Try to create a reflink (copy-on-write)."""
    try:
        result = subprocess.run(
            ['cp', '--reflink=only', str(source), str(dest)],
            capture_output=True,
        )
        return result.returncode == 0
    except (OSError, FileNotFoundError):
        return False


def _try_hardlink(source: Path, dest: Path) -> bool:
    """Try to create a hardlink."""
    try:
        os.link(source, dest)
        return True
    except OSError:
        return False


def _try_symlink(source: Path, dest: Path) -> bool:
    """Try to create a symlink."""
    try:
        os.symlink(source, dest)
        return True
    except OSError:
        return False


def _try_copy(source: Path, dest: Path) -> bool:
    """Try to copy the file."""
    try:
        shutil.copy2(source, dest)
        return True
    except OSError:
        return False


def link_file(
    source: Path,
    dest: Path,
    verbose: bool = False,
    label: str = '',
    cache_type: Optional[CacheType] = None,
) -> Tuple[bool, LinkType]:
    """Copy or link a file from source to destination.
    
    By default, uses DVC's preferred order: reflink → hardlink → symlink → copy.
    If cache_type is specified, only that method is attempted.
    
    Creates parent directories as needed. Skips if destination already exists.
    
    Args:
        source: Source file path.
        dest: Destination file path.
        verbose: Print progress messages.
        label: Optional label for verbose output (e.g., hash or filename).
        cache_type: If specified, only use this link type (reflink, hardlink,
            symlink, or copy). Fails if the specified type doesn't work.
        
    Returns:
        Tuple of (success, link_type).
        success is True if file was linked/copied, False if skipped or failed.
        link_type indicates how the file was handled.
    """
    # Skip explicit dest.exists() check - it's slow on network filesystems.
    # Instead, just try the operation and catch FileExistsError.
    # This is faster on Lustre/NFS because the filesystem checks existence
    # as part of the link syscall anyway.
    
    if not source.exists():
        if verbose:
            print(f"  ERROR: Source not found: {source}")
        return False, 'failed'
    
    # Ensure parent directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    display = label or source.name
    
    # If specific cache_type requested, only try that one
    if cache_type:
        methods = {
            'reflink': (_try_reflink, 'reflink'),
            'hardlink': (_try_hardlink, 'hardlink'),
            'symlink': (_try_symlink, 'symlink'),
            'copy': (_try_copy, 'copy'),
        }
        
        try_func, type_name = methods[cache_type]
        if try_func(source, dest):
            if verbose:
                print(f"  Cached ({type_name}): {display}")
            return True, type_name
        else:
            # Check if failure was because dest already exists
            if dest.exists():
                return False, 'skipped'
            if verbose:
                print(f"  ERROR: {type_name} failed for {display}")
            return False, 'failed'
    
    # Default: try all methods in order
    # 1. Try reflink (copy-on-write) - best option: instant, zero space, safe to modify
    if _try_reflink(source, dest):
        if verbose:
            print(f"  Cached (reflink): {display}")
        return True, 'reflink'
    
    # 2. Try hardlink - same inode, no extra space, works within same filesystem
    if _try_hardlink(source, dest):
        if verbose:
            print(f"  Cached (hardlink): {display}")
        return True, 'hardlink'
    
    # 3. Try symlink - pointer to source, no extra space, works across filesystems
    if _try_symlink(source, dest):
        if verbose:
            print(f"  Cached (symlink): {display}")
        return True, 'symlink'
    
    # 4. Fall back to regular copy - slower but universally compatible
    if _try_copy(source, dest):
        if verbose:
            print(f"  Cached (copy): {display}")
        return True, 'copy'
    
    # All methods failed. Check if it's because dest already exists.
    # This check happens only once per file (after failure) rather than
    # once per file upfront, which is much faster when most files are new.
    if dest.exists():
        return False, 'skipped'
    
    if verbose:
        print(f"  ERROR: All link methods failed for {display}")
    
    return False, 'failed'


def get_cache_file_path(
    md5: str,
    cache_root: Path,
    use_v3_layout: bool = True,
) -> Path:
    """Get the cache file path for a given MD5 hash.
    
    Args:
        md5: The MD5 hash (with optional .dir suffix).
        cache_root: Path to the cache root directory.
        use_v3_layout: If True, use v3 layout (files/md5/XX/hash).
            If False, use v2 layout (XX/hash).
            
    Returns:
        Path to the cache file.
    """
    # Handle .dir suffix
    if md5.endswith('.dir'):
        hash_only = md5[:-4]
        filename = hash_only[2:] + '.dir'
    else:
        hash_only = md5
        filename = hash_only[2:]
    
    if use_v3_layout:
        return cache_root / 'files' / 'md5' / hash_only[:2] / filename
    else:
        return cache_root / hash_only[:2] / filename


def find_source_file(
    md5: str,
    source_cache: Path,
) -> Optional[Path]:
    """Find a file in a source cache, checking both v3 and v2 layouts.
    
    Args:
        md5: The MD5 hash (with optional .dir suffix).
        source_cache: Path to the source cache root.
        
    Returns:
        Path to the source file if found, None otherwise.
    """
    # Handle .dir suffix
    if md5.endswith('.dir'):
        hash_only = md5[:-4]
        filename = hash_only[2:] + '.dir'
    else:
        hash_only = md5
        filename = hash_only[2:]
    
    # Try v3 path first
    v3_path = source_cache / 'files' / 'md5' / hash_only[:2] / filename
    if v3_path.exists():
        return v3_path
    
    # Fall back to v2 path
    v2_path = source_cache / hash_only[:2] / filename
    if v2_path.exists():
        return v2_path
    
    return None


def populate_cache_file(
    md5: str,
    source_cache: str,
    dest_cache: str,
    verbose: bool = False,
    use_v3_layout: bool = True,
    cache_type: Optional[CacheType] = None,
) -> Optional[bool]:
    """Copy or link a single file from source cache to destination cache.
    
    Used for .dir files and single file imports where we need to ensure
    the file exists in the primary cache.
    
    Supports both DVC v3 cache layout (files/md5/XX/hash) and legacy 
    DVC v2 layout (XX/hash directly in remote root).
    
    Args:
        md5: The MD5 hash (with optional .dir suffix).
        source_cache: Path to the source cache/remote root.
        dest_cache: Path to the destination cache base directory.
        verbose: Print progress messages.
        use_v3_layout: If True, use v3 layout for destination.
        cache_type: If specified, only use this link type (reflink, hardlink,
            symlink, or copy). Fails if the specified type doesn't work.
            
    Returns:
        True if file was added to cache.
        False if file already exists in cache.
        None if source file not found (error case).
    """
    source_path = find_source_file(md5, Path(source_cache))
    if source_path is None:
        return None
    
    dest_path = get_cache_file_path(md5, Path(dest_cache), use_v3_layout)
    
    # Note: We don't check dest_path.exists() here - it's slow on network
    # filesystems. link_file() handles existing files efficiently by catching
    # the failure and checking existence only when needed.
    
    label = f"{md5[:12]}..."
    success, link_type = link_file(source_path, dest_path, verbose=verbose, label=label, cache_type=cache_type)
    
    if link_type == 'skipped':
        return False  # Already existed
    if link_type == 'failed':
        return None
    return success


def is_v2_hash_name(hash_name: str) -> bool:
    """Check if a hash name indicates v2/legacy format.
    
    Args:
        hash_name: The hash algorithm name (e.g., 'md5', 'md5-dos2unix').
        
    Returns:
        True if v2/legacy format, False if v3.
    """
    return hash_name in ('md5-dos2unix', 'params')

