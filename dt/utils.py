"""Shared utilities for DVC Tools.

Common functions used across multiple modules.
"""

import os
import shutil
from pathlib import Path
from typing import Optional


class DependencyError(Exception):
    """Raised when required external tools are not available."""
    pass


# =============================================================================
# Formatting utilities
# =============================================================================

def format_size(size_bytes: int, human_readable: bool = True) -> str:
    """Format byte size as human-readable string.
    
    Uses DVC's naturalsize for consistent formatting with DVC output.
    Falls back to a simple implementation if DVC is not available.
    
    Args:
        size_bytes: Size in bytes
        human_readable: If True, format as K, M, G etc. If False, return raw bytes.
        
    Returns:
        Formatted size string
    """
    if not human_readable:
        return str(size_bytes)
    
    try:
        from dvc.utils.humanize import naturalsize
        return naturalsize(size_bytes)
    except ImportError:
        # Fallback if DVC internals unavailable
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        elif size_bytes < 1024 * 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024 * 1024):.1f} TB"


# =============================================================================
# DVC cache utilities
# =============================================================================

def get_cache_dir() -> Optional[Path]:
    """Get the primary DVC cache directory.
    
    Returns the path to the cache files/md5 directory where DVC stores
    content-addressed files.
    
    Returns:
        Path to the cache files/md5 directory, or None if not in a DVC repo
        or cache not configured.
    """
    try:
        from dvc.repo import Repo
        repo = Repo()
        return Path(repo.cache.local.path)
    except Exception:
        return None


def hash_to_cache_path(cache_dir: Path, file_hash: str) -> Path:
    """Convert a file hash to its cache file path.
    
    Uses DVC's standard layout: cache_dir/XX/XXXXXX...
    where XX is the first two characters of the hash.
    
    Args:
        cache_dir: Path to the cache files/md5 directory
        file_hash: MD5 hash (possibly with .dir suffix)
        
    Returns:
        Path to the cache file
    """
    # Handle .dir suffix
    hash_clean = file_hash.replace('.dir', '')
    prefix = hash_clean[:2]
    suffix = hash_clean[2:]
    if file_hash.endswith('.dir'):
        suffix += '.dir'
    
    return cache_dir / prefix / suffix


def oid_to_path(file_hash: str) -> Optional[Path]:
    """Convert a file hash to its cache path using DVC's cache object.
    
    This uses DVC's internal oid_to_path method for guaranteed compatibility.
    
    Args:
        file_hash: MD5 hash (possibly with .dir suffix)
        
    Returns:
        Path to the cache file, or None if cache not available
    """
    try:
        from dvc.repo import Repo
        repo = Repo()
        return Path(repo.cache.local.oid_to_path(file_hash))
    except Exception:
        return None


# =============================================================================
# Project utilities
# =============================================================================

def get_project_name() -> str:
    """Get the project name from the current directory.
    
    Returns:
        Name of the current directory
    """
    return Path.cwd().name


def check_command(command: str, install_hint: Optional[str] = None) -> None:
    """Check that a command is available in PATH.
    
    Args:
        command: Name of the command to check
        install_hint: Optional hint for how to install the command
        
    Raises:
        DependencyError: If the command is not found
    """
    if not shutil.which(command):
        msg = f"{command} command not found.\nPlease ensure {command} is installed and in your PATH."
        if install_hint:
            msg += f"\n  {install_hint}"
        raise DependencyError(msg)


def check_dvc() -> None:
    """Check that DVC is available.
    
    Raises:
        DependencyError: If DVC is not found
    """
    check_command('dvc', install_hint='pip install dvc')


def check_git() -> None:
    """Check that git is available.
    
    Raises:
        DependencyError: If git is not found
    """
    check_command('git')


def update_gitignore(pattern: str, gitignore_path: Optional[Path] = None) -> bool:
    """Add a pattern to .gitignore if not already present.
    
    Appends the pattern to .gitignore, matching DVC's behavior for
    dvc add and dvc import. Creates .gitignore if it doesn't exist.
    
    Args:
        pattern: The pattern to add (e.g., '/data.txt' or '.dt/tmp/').
        gitignore_path: Path to .gitignore file. Defaults to .gitignore
            in the current directory.
    
    Returns:
        True if .gitignore was modified, False if pattern already present.
    """
    if gitignore_path is None:
        gitignore_path = Path.cwd() / ".gitignore"
    
    # Normalize pattern for comparison
    pattern_normalized = pattern.rstrip('/')
    
    # Check if already present
    if gitignore_path.exists():
        content = gitignore_path.read_text()
        for line in content.splitlines():
            line_normalized = line.strip().rstrip('/')
            if line_normalized == pattern_normalized:
                return False
    else:
        content = ""
    
    # Append pattern
    if content and not content.endswith('\n'):
        content += '\n'
    content += f"{pattern}\n"
    
    gitignore_path.write_text(content)
    return True


def set_group_writable(path: Path, setgid: bool = True) -> None:
    """Set group write permissions on a path.
    
    Args:
        path: Path to set permissions on
        setgid: Also set the setgid bit (default True for shared directories)
    """
    mode = 0o2775 if setgid else 0o0775
    try:
        os.chmod(path, mode)
    except PermissionError:
        pass


def create_md5_subdirs(parent_dir: Path, verbose: bool = False) -> None:
    """Create the files/md5 subdirectory structure for DVC.
    
    Creates 256 subdirectories (00-ff) under files/md5 with proper
    group write permissions for shared access in HPC environments.
    
    Args:
        parent_dir: Parent directory (cache or remote root)
        verbose: Print progress messages
    """
    files_md5 = parent_dir / "files" / "md5"
    files_md5.mkdir(parents=True, exist_ok=True)
    set_group_writable(files_md5)
    
    if verbose:
        print(f"Creating files/md5 subdirectories under {parent_dir}")
    
    for i in range(256):
        subdir = files_md5 / f"{i:02x}"
        subdir.mkdir(exist_ok=True)
        set_group_writable(subdir)
