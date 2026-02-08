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


def link_file(
    source: Path,
    dest: Path,
    verbose: bool = False,
    label: str = '',
) -> Tuple[bool, LinkType]:
    """Copy or link a file from source to destination.
    
    Uses DVC's preferred order: reflink → hardlink → symlink → copy.
    Creates parent directories as needed. Skips if destination already exists.
    
    Args:
        source: Source file path.
        dest: Destination file path.
        verbose: Print progress messages.
        label: Optional label for verbose output (e.g., hash or filename).
        
    Returns:
        Tuple of (success, link_type).
        success is True if file was linked/copied, False if skipped or failed.
        link_type indicates how the file was handled.
    """
    if dest.exists():
        return False, 'skipped'
    
    if not source.exists():
        if verbose:
            print(f"  ERROR: Source not found: {source}")
        return False, 'failed'
    
    # Ensure parent directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    display = label or source.name
    
    # 1. Try reflink (copy-on-write) - best option: instant, zero space, safe to modify
    try:
        result = subprocess.run(
            ['cp', '--reflink=only', str(source), str(dest)],
            capture_output=True,
        )
        if result.returncode == 0:
            if verbose:
                print(f"  Cached (reflink): {display}")
            return True, 'reflink'
    except (OSError, FileNotFoundError):
        pass  # cp not available or reflink not supported
    
    # 2. Try hardlink - same inode, no extra space, works within same filesystem
    try:
        os.link(source, dest)
        if verbose:
            print(f"  Cached (hardlink): {display}")
        return True, 'hardlink'
    except OSError as e:
        # EXDEV (18): Cross-device link (different filesystems)
        # EPERM (1): Operation not permitted (common on HPC with quota restrictions)
        # EACCES (13): Permission denied
        if e.errno not in (1, 13, 18):
            if verbose:
                print(f"  ERROR: Failed to hardlink {display}: {e}")
            return False, 'failed'
    
    # 3. Try symlink - pointer to source, no extra space, works across filesystems
    try:
        os.symlink(source, dest)
        if verbose:
            print(f"  Cached (symlink): {display}")
        return True, 'symlink'
    except OSError as e:
        if verbose:
            print(f"  Warning: symlink failed for {display}: {e}")
    
    # 4. Fall back to regular copy - slower but universally compatible
    try:
        shutil.copy2(source, dest)
        if verbose:
            print(f"  Cached (copy): {display}")
        return True, 'copy'
    except OSError as e:
        if verbose:
            print(f"  ERROR: Failed to copy {display}: {e}")
    
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
            
    Returns:
        True if file was added to cache.
        False if file already exists in cache.
        None if source file not found (error case).
    """
    source_path = find_source_file(md5, Path(source_cache))
    if source_path is None:
        return None
    
    dest_path = get_cache_file_path(md5, Path(dest_cache), use_v3_layout)
    
    if dest_path.exists():
        return False
    
    label = f"{md5[:12]}..."
    success, link_type = link_file(source_path, dest_path, verbose=verbose, label=label)
    
    if link_type == 'failed':
        return None
    return success
