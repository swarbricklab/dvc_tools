"""Build DVC's site cache index from cache filenames.

``dt index build`` populates DVC's object-database index (under
``core.site_cache_dir``) by trusting cache filenames instead of hashing
every file.

The former ``dt index push|pull`` archive mirror has been removed; the
site_cache_dir itself now persists on shared storage (see
``dt/site_cache.py``), which covers what the mirror was for.
"""

import subprocess
from pathlib import Path
from typing import Optional

from .errors import DTError


class IndexError(DTError):
    """Error during index operations."""
    pass


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
