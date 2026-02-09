"""Manage DVC site cache index mirror.

The site cache index allows DVC to quickly look up files across multiple
caches. This module syncs the local index with a shared mirror so all
users benefit from the same index without rebuilding it.

Key concepts:
- Local index: DVC's site_cache_dir (usually in /tmp)
- Mirror: Shared network location for index persistence
- Locking: File-based locks prevent concurrent modifications
"""

import os
import subprocess
import time
from functools import wraps
from pathlib import Path
from typing import Callable, Optional, Tuple

from . import config as cfg
from .errors import DTError


class IndexError(DTError):
    """Error during index operations."""
    pass


class IndexLockTimeout(IndexError):
    """Timeout waiting for index lock."""
    pass


class IndexNotConfigured(IndexError):
    """Index mirror not configured."""
    pass


# =============================================================================
# Configuration
# =============================================================================


def get_index_paths() -> Tuple[Path, Path]:
    """Get local index and mirror paths.
    
    Returns:
        Tuple of (local_index_path, mirror_path)
        
    Raises:
        IndexNotConfigured: If mirror root not configured or not in DVC repo.
    """
    # Get mirror root from config
    mirror_root = cfg.get_value('index.mirror_root')
    
    if not mirror_root:
        raise IndexNotConfigured(
            "Index mirror not configured. Set 'index.mirror_root' in dt config."
        )
    
    mirror_root = Path(mirror_root)
    
    # Get local index from dvc doctor
    try:
        result = subprocess.run(
            ['dvc', 'doctor'],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        raise IndexNotConfigured("Not in a DVC repository")
    except FileNotFoundError:
        raise IndexNotConfigured("DVC not found")
    
    # Parse site_cache_dir from output
    local_index = None
    for line in result.stdout.splitlines():
        if 'site_cache_dir' in line.lower():
            parts = line.split()
            if len(parts) >= 2:
                local_index = Path(parts[-1])
                break
    
    if not local_index:
        raise IndexNotConfigured(
            "Could not determine site_cache_dir from dvc doctor"
        )
    
    # Mirror path is based on the repo hash (last component of local index)
    repo_hash = local_index.name
    mirror_path = mirror_root / 'repo' / repo_hash
    
    return local_index, mirror_path


def get_lock_timeout() -> int:
    """Get lock timeout in seconds from config."""
    return int(cfg.get_value('index.lock_timeout', 120))


def get_retry_interval() -> int:
    """Get initial retry interval in seconds from config."""
    return int(cfg.get_value('index.retry_interval', 5))


def is_auto_sync_enabled() -> bool:
    """Check if automatic index sync is enabled."""
    return cfg.get_value('index.auto_sync', True)


# =============================================================================
# Locking
# =============================================================================


def get_lock_owner(lock_path: Path) -> str:
    """Get the owner of a lock file."""
    try:
        import pwd
        stat = lock_path.stat()
        return pwd.getpwuid(stat.st_uid).pw_name
    except Exception:
        return "unknown"


def get_lock_age(lock_path: Path) -> float:
    """Get the age of a lock file in seconds."""
    try:
        return time.time() - lock_path.stat().st_mtime
    except Exception:
        return 0


def wait_for_lock(
    lock_path: Path,
    timeout: Optional[int] = None,
    retry_interval: Optional[int] = None,
    verbose: bool = False,
) -> bool:
    """Wait for a lock file to be released.
    
    Args:
        lock_path: Path to the lock file.
        timeout: Maximum time to wait in seconds.
        retry_interval: Initial retry interval (uses exponential backoff).
        verbose: Print waiting messages.
        
    Returns:
        True if lock was released, False if timeout reached.
    """
    if timeout is None:
        timeout = get_lock_timeout()
    if retry_interval is None:
        retry_interval = get_retry_interval()
    
    if not lock_path.exists():
        return True
    
    elapsed = 0
    interval = retry_interval
    max_interval = 30
    
    while lock_path.exists() and elapsed < timeout:
        owner = get_lock_owner(lock_path)
        age = get_lock_age(lock_path)
        
        if verbose:
            print(f"  Index locked by {owner} ({age:.0f}s ago), waiting {interval}s...")
        
        time.sleep(interval)
        elapsed += interval
        interval = min(interval * 1.5, max_interval)
    
    return not lock_path.exists()


def acquire_lock(lock_path: Path, timeout: Optional[int] = None) -> bool:
    """Acquire a lock, waiting if necessary.
    
    Args:
        lock_path: Path to the lock file.
        timeout: Maximum time to wait for existing lock.
        
    Returns:
        True if lock acquired.
        
    Raises:
        IndexLockTimeout: If timeout reached waiting for lock.
    """
    if not wait_for_lock(lock_path, timeout=timeout):
        owner = get_lock_owner(lock_path)
        raise IndexLockTimeout(
            f"Timeout waiting for index lock (held by {owner}). "
            f"If stale, delete: {lock_path}"
        )
    
    # Create lock file
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.touch()
    
    return True


def release_lock(lock_path: Path) -> None:
    """Release a lock file."""
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass


# =============================================================================
# Core operations
# =============================================================================


def pull(
    verbose: bool = False,
    dry: bool = False,
    quiet: bool = False,
) -> bool:
    """Pull index from mirror to local.
    
    Args:
        verbose: Show detailed rsync output.
        dry: Show what would be synced without syncing.
        quiet: Suppress all output except errors.
        
    Returns:
        True if successful.
    """
    try:
        local_index, mirror_path = get_index_paths()
    except IndexNotConfigured as e:
        if not quiet:
            print(f"Warning: {e}")
        return False
    
    # Check mirror exists and has content
    if not mirror_path.exists():
        if verbose and not quiet:
            print(f"  Mirror does not exist yet: {mirror_path}")
        return True
    
    if not any(mirror_path.iterdir()):
        if verbose and not quiet:
            print(f"  Mirror is empty: {mirror_path}")
        return True
    
    if not quiet:
        if verbose:
            print(f"Syncing index from mirror...")
            print(f"  Mirror: {mirror_path}")
            print(f"  Local:  {local_index}")
        else:
            print(f"Syncing index...")
    
    # Create local index directory if needed
    local_index.mkdir(parents=True, exist_ok=True)
    
    # Acquire lock
    local_lock = local_index / 'local.lock'
    try:
        acquire_lock(local_lock)
    except IndexLockTimeout as e:
        if not quiet:
            print(f"Warning: {e}")
        return False
    
    try:
        # Build rsync command
        cmd = [
            'rsync', '-ah',
            '--perms', '--chmod=ug+rw',
            '--checksum',
            '--omit-dir-times',
        ]
        
        if dry:
            cmd.append('--dry-run')
            cmd.append('-v')
        elif verbose:
            cmd.append('-v')
        
        cmd.extend([f"{mirror_path}/", f"{local_index}/"])
        
        if verbose and not quiet:
            print(f"  Running: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=not verbose,
            text=True,
        )
        
        if result.returncode != 0:
            if not quiet:
                print(f"Warning: rsync failed: {result.stderr}")
            return False
        
        if verbose and not quiet and not dry:
            print("  Index updated from mirror")
        
        return True
        
    finally:
        release_lock(local_lock)


def push(
    verbose: bool = False,
    dry: bool = False,
    quiet: bool = False,
) -> bool:
    """Push index from local to mirror.
    
    Args:
        verbose: Show detailed rsync output.
        dry: Show what would be synced without syncing.
        quiet: Suppress all output except errors.
        
    Returns:
        True if successful.
    """
    try:
        local_index, mirror_path = get_index_paths()
    except IndexNotConfigured as e:
        if not quiet:
            print(f"Warning: {e}")
        return False
    
    # Check local index exists and has content
    if not local_index.exists():
        if verbose and not quiet:
            print(f"  Local index does not exist: {local_index}")
        return True
    
    if not any(local_index.iterdir()):
        if verbose and not quiet:
            print(f"  Local index is empty: {local_index}")
        return True
    
    if not quiet:
        if verbose:
            print(f"Syncing index to mirror...")
            print(f"  Local:  {local_index}")
            print(f"  Mirror: {mirror_path}")
        else:
            print(f"Syncing index...")
    
    # Create mirror directory if needed
    mirror_path.mkdir(parents=True, exist_ok=True)
    
    # Set group permissions on mirror
    try:
        os.chmod(mirror_path, 0o775)
    except OSError:
        pass  # May not own the directory
    
    # Acquire lock
    mirror_lock = mirror_path / 'mirror.lock'
    try:
        acquire_lock(mirror_lock)
    except IndexLockTimeout as e:
        if not quiet:
            print(f"Warning: {e}")
        return False
    
    try:
        # Build rsync command
        cmd = [
            'rsync', '-ah',
            '--perms', '--chmod=ug+rw',
            '--checksum',
            '--omit-dir-times',
        ]
        
        if dry:
            cmd.append('--dry-run')
            cmd.append('-v')
        elif verbose:
            cmd.append('-v')
        
        cmd.extend([f"{local_index}/", f"{mirror_path}/"])
        
        if verbose and not quiet:
            print(f"  Running: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=not verbose,
            text=True,
        )
        
        if result.returncode != 0:
            if not quiet:
                print(f"Warning: rsync failed: {result.stderr}")
            return False
        
        if verbose and not quiet and not dry:
            print("  Index pushed to mirror")
        
        return True
        
    finally:
        release_lock(mirror_lock)


def status(verbose: bool = False) -> dict:
    """Get index status information.
    
    Returns:
        Dict with status info including paths, sizes, lock status.
    """
    result = {
        'configured': False,
        'local_index': None,
        'mirror_path': None,
        'local_exists': False,
        'mirror_exists': False,
        'local_locked': False,
        'mirror_locked': False,
    }
    
    try:
        local_index, mirror_path = get_index_paths()
        result['configured'] = True
        result['local_index'] = str(local_index)
        result['mirror_path'] = str(mirror_path)
        result['local_exists'] = local_index.exists()
        result['mirror_exists'] = mirror_path.exists()
        
        if local_index.exists():
            local_lock = local_index / 'local.lock'
            result['local_locked'] = local_lock.exists()
            if result['local_locked']:
                result['local_lock_owner'] = get_lock_owner(local_lock)
                result['local_lock_age'] = get_lock_age(local_lock)
        
        if mirror_path.exists():
            mirror_lock = mirror_path / 'mirror.lock'
            result['mirror_locked'] = mirror_lock.exists()
            if result['mirror_locked']:
                result['mirror_lock_owner'] = get_lock_owner(mirror_lock)
                result['mirror_lock_age'] = get_lock_age(mirror_lock)
                
    except IndexNotConfigured as e:
        result['error'] = str(e)
    
    return result


# =============================================================================
# Decorator for automatic sync
# =============================================================================


def with_index_sync(
    pull_before: bool = True,
    push_after: bool = True,
):
    """Decorator to add automatic index sync to a function.
    
    Args:
        pull_before: Pull index before function execution.
        push_after: Push index after function execution.
        
    The decorated function can accept these keyword arguments:
        - no_index_sync: Skip all index sync
        - verbose: Enable verbose output
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Check for skip flag
            no_sync = kwargs.pop('no_index_sync', False)
            verbose = kwargs.get('verbose', False)
            
            # Check if auto sync is enabled in config
            if no_sync or not is_auto_sync_enabled():
                return func(*args, **kwargs)
            
            # Pull before
            if pull_before:
                try:
                    pull(quiet=not verbose, verbose=verbose)
                except Exception as e:
                    if verbose:
                        print(f"Warning: index pull failed: {e}")
            
            # Execute the function
            result = func(*args, **kwargs)
            
            # Push after
            if push_after:
                try:
                    push(quiet=not verbose, verbose=verbose)
                except Exception as e:
                    if verbose:
                        print(f"Warning: index push failed: {e}")
            
            return result
        
        return wrapper
    return decorator


# =============================================================================
# Index Building (trusting cache filenames)
# =============================================================================


def get_cache_path() -> Path:
    """Get the DVC cache path for the current repo.
    
    Returns:
        Path to the cache files/md5 directory.
        
    Raises:
        IndexError: If not in a DVC repo or cache not configured.
    """
    try:
        result = subprocess.run(
            ['dvc', 'cache', 'dir'],
            capture_output=True,
            text=True,
            check=True,
        )
        cache_root = Path(result.stdout.strip())
        # Cache dir returns root, we need files/md5
        cache_path = cache_root / 'files' / 'md5'
        if not cache_path.exists():
            # Maybe it's already the md5 directory
            cache_path = cache_root
        return cache_path
    except subprocess.CalledProcessError as e:
        raise IndexError(f"Could not get cache directory: {e.stderr}")
    except FileNotFoundError:
        raise IndexError("DVC not found")


def get_site_cache_dir() -> Path:
    """Get the DVC site_cache_dir for the current repo.
    
    Returns:
        Path to the site_cache_dir.
        
    Raises:
        IndexError: If not in a DVC repo.
    """
    try:
        result = subprocess.run(
            ['dvc', 'doctor'],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        raise IndexError("Not in a DVC repository")
    except FileNotFoundError:
        raise IndexError("DVC not found")
    
    # Parse site_cache_dir from output
    for line in result.stdout.splitlines():
        if 'site_cache_dir' in line.lower():
            parts = line.split()
            if len(parts) >= 2:
                return Path(parts[-1])
    
    raise IndexError("Could not determine site_cache_dir from dvc doctor")


def get_odb_index_name(cache_path: str) -> str:
    """Generate ODB index name from cache path (matches DVC's approach).
    
    DVC uses SHA256 of the cache path to create a unique index name.
    
    Args:
        cache_path: Path to the cache directory.
        
    Returns:
        SHA256 hash of the path.
    """
    import hashlib
    return hashlib.sha256(cache_path.encode('utf-8')).hexdigest()


def walk_cache_directory(
    cache_path: Path,
    verbose: bool = False,
    progress: bool = True,
) -> tuple[list[str], list[str]]:
    """Walk a cache directory and extract hashes from filenames.
    
    Cache files are named {hash[0:2]}/{hash[2:]}, so we can reconstruct
    the full hash from the directory structure without reading file contents.
    
    Args:
        cache_path: Path to the cache files/md5 directory.
        verbose: Print each file found.
        progress: Show progress counter.
        
    Returns:
        Tuple of (dir_hashes, file_hashes).
    """
    dir_hashes = []
    file_hashes = []
    
    # Count files first for progress
    if progress:
        total = sum(1 for d in cache_path.iterdir() if d.is_dir() 
                    for _ in d.iterdir())
        count = 0
    
    for prefix_dir in sorted(cache_path.iterdir()):
        if not prefix_dir.is_dir():
            continue
        
        # Prefix should be 2 hex chars
        prefix = prefix_dir.name
        if len(prefix) != 2:
            continue
        
        for cache_file in prefix_dir.iterdir():
            suffix = cache_file.name
            
            # Skip temp files and unpacked directories
            if suffix.endswith('.tmp') or suffix.endswith('.unpacked'):
                continue
            
            # Reconstruct full hash from path
            full_hash = prefix + suffix
            
            if progress:
                count += 1
                print(f"\rScanning cache: {count}/{total}", end='', flush=True)
            
            # Classify as dir or file hash
            if full_hash.endswith('.dir'):
                dir_hashes.append(full_hash)
                if verbose:
                    print(f"  DIR: {full_hash}")
            else:
                file_hashes.append(full_hash)
                if verbose:
                    print(f"  FILE: {full_hash}")
    
    if progress:
        print()  # Newline after progress
    
    return dir_hashes, file_hashes


def build(
    cache_path: Optional[str] = None,
    verbose: bool = False,
    dry: bool = False,
    quiet: bool = False,
) -> dict:
    """Build ODB index by walking cache and trusting filenames.
    
    This avoids the expensive hash computation by assuming cache files
    are named correctly (hash[0:2]/hash[2:]). Use `dt cache validate`
    if you need to verify checksum integrity.
    
    Args:
        cache_path: Path to cache directory. If None, uses current repo's cache.
        verbose: Show each file being indexed.
        dry: Show what would be indexed without writing.
        quiet: Suppress all output except errors.
        
    Returns:
        Dictionary with:
            - dir_count: Number of directory hashes indexed
            - file_count: Number of file hashes indexed
            - cache_path: Path that was scanned
            - index_path: Path to the index directory
    """
    from dvc_data.hashfile.db.index import ObjectDBIndex
    
    # Get cache path
    if cache_path:
        cache_dir = Path(cache_path)
        # Handle if user provides cache root instead of files/md5
        if (cache_dir / 'files' / 'md5').exists():
            cache_dir = cache_dir / 'files' / 'md5'
    else:
        cache_dir = get_cache_path()
    
    if not cache_dir.exists():
        raise IndexError(f"Cache directory does not exist: {cache_dir}")
    
    # Get site_cache_dir for this repo
    site_cache = get_site_cache_dir()
    
    # Compute ODB index name from cache path
    odb_name = get_odb_index_name(str(cache_dir))
    
    if not quiet:
        print(f"Building index from cache...")
        if verbose:
            print(f"  Cache: {cache_dir}")
            print(f"  Site cache: {site_cache}")
            print(f"  ODB name: {odb_name[:16]}...")
    
    # Walk cache directory
    dir_hashes, file_hashes = walk_cache_directory(
        cache_dir,
        verbose=verbose,
        progress=not quiet,
    )
    
    if not quiet:
        print(f"Found {len(dir_hashes)} directories, {len(file_hashes)} files")
    
    if dry:
        if not quiet:
            print("Dry run - index not modified")
        return {
            'dir_count': len(dir_hashes),
            'file_count': len(file_hashes),
            'cache_path': str(cache_dir),
            'index_path': str(site_cache / 'index' / odb_name),
            'dry_run': True,
        }
    
    # Create/update the ODB index
    if not quiet:
        print("Updating index...")
    
    index = ObjectDBIndex(str(site_cache), odb_name)
    index.update(dir_hashes, file_hashes)
    
    if not quiet:
        print(f"Index built successfully: {site_cache / 'index' / odb_name}")
    
    return {
        'dir_count': len(dir_hashes),
        'file_count': len(file_hashes),
        'cache_path': str(cache_dir),
        'index_path': str(site_cache / 'index' / odb_name),
    }
