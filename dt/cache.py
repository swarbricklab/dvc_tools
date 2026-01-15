"""Cache management for DVC Tools.

Handles external shared cache setup and configuration for HPC environments.
"""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from . import config as cfg


class CacheError(Exception):
    """Raised when cache operations fail."""
    pass


def check_dvc() -> None:
    """Check that DVC is available.
    
    Raises:
        CacheError: If DVC is not found
    """
    if not shutil.which('dvc'):
        raise CacheError(
            "dvc command not found.\n"
            "Please ensure DVC is installed and in your PATH.\n"
            "  pip install dvc"
        )


def get_project_name() -> str:
    """Get the project name from the current directory.
    
    Returns:
        Name of the current directory
    """
    return Path.cwd().name


def resolve_cache_path(
    name: Optional[str] = None,
    cache_root: Optional[str] = None,
    cache_path: Optional[str] = None,
) -> Path:
    """Resolve the cache directory path.
    
    Path resolution order:
    1. cache_path - Complete path override
    2. Constructed: {cache_root}/{name}
    
    Args:
        name: Project name (defaults to current directory name)
        cache_root: Root directory for caches
        cache_path: Complete path override
        
    Returns:
        Resolved cache directory path
        
    Raises:
        CacheError: If cache location cannot be determined
    """
    if cache_path:
        return Path(cache_path).resolve()
    
    # Get cache root from argument or config
    root = cache_root or cfg.get_value('cache.root')
    if not root:
        raise CacheError(
            "Cache root not configured.\n"
            "Either specify --cache-root or set cache.root:\n"
            "  dt config set cache.root /path/to/cache"
        )
    
    # Get project name
    project_name = name or get_project_name()
    
    return Path(root) / project_name


def init_cache_structure(cache_dir: Path, verbose: bool = True) -> None:
    """Initialize the cache directory structure with proper permissions.
    
    Creates the files/md5 subdirectories (00-ff) and runs directory
    with group write permissions for shared access in HPC environments.
    
    Args:
        cache_dir: Path to the cache directory
        verbose: Print progress messages
    """
    if verbose:
        print(f"Initializing cache structure at {cache_dir}")
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    # Create runs directory for DVC run cache
    runs_dir = cache_dir / "runs"
    runs_dir.mkdir(exist_ok=True)
    
    # Create files/md5 structure for DVC v3
    files_md5 = cache_dir / "files" / "md5"
    files_md5.mkdir(parents=True, exist_ok=True)
    
    # Set permissions on main directories
    for d in [cache_dir, runs_dir, files_md5]:
        try:
            os.chmod(d, 0o2775)
        except PermissionError:
            pass
    
    # Create subdirectories 00-ff under files/md5 with proper permissions
    for i in range(256):
        subdir = files_md5 / f"{i:02x}"
        subdir.mkdir(exist_ok=True)
        try:
            os.chmod(subdir, 0o2775)
        except PermissionError:
            pass


def configure_dvc_cache(repo_path: Path, cache_dir: Path, verbose: bool = True) -> None:
    """Configure DVC to use the specified cache directory.
    
    Uses --local flag to keep configuration workspace-specific.
    
    Args:
        repo_path: Path to the DVC repository
        cache_dir: Path to the cache directory
        verbose: Print progress messages
    """
    if verbose:
        print(f"Configuring DVC cache: {cache_dir}")
    
    result = subprocess.run(
        ['dvc', 'cache', 'dir', '--local', str(cache_dir)],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise CacheError(f"Failed to configure DVC cache: {result.stderr}")


def init_cache(
    name: Optional[str] = None,
    cache_root: Optional[str] = None,
    cache_path: Optional[str] = None,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
) -> Path:
    """Initialize an external shared cache for a DVC project.
    
    Creates the cache directory structure with proper permissions
    and configures DVC to use it.
    
    Args:
        name: Project name (defaults to current directory name)
        cache_root: Root directory for caches
        cache_path: Complete path override
        repo_path: Path to the DVC repository (defaults to cwd)
        verbose: Print progress messages
        
    Returns:
        Path to the initialized cache directory
        
    Raises:
        CacheError: If cache initialization fails
    """
    check_dvc()
    
    repo_path = repo_path or Path.cwd()
    cache_dir = resolve_cache_path(name, cache_root, cache_path)
    
    if cache_dir.exists():
        if verbose:
            print(f"Using existing cache at {cache_dir}")
    else:
        if verbose:
            print(f"Creating cache at {cache_dir}")
        init_cache_structure(cache_dir, verbose=verbose)
    
    configure_dvc_cache(repo_path, cache_dir, verbose=verbose)
    
    return cache_dir
