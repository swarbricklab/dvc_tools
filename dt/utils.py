"""Shared utilities for DVC Tools.

Common functions used across multiple modules.
"""

import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dvc.repo import Repo

from .errors import DependencyError, DVCFileError


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
        repo = Repo()
        return Path(repo.cache.local.oid_to_path(file_hash))
    except Exception:
        return None


def collect_tracked_entries(
    targets: Optional[List[str]] = None,
    remote: Optional[str] = None,
    push: bool = False,
) -> Dict[str, Any]:
    """Collect tracked file entries for targets using DVC internals.
    
    This is a shared wrapper around DVC's _collect_indexes that provides
    a consistent interface for enumerating tracked files.
    
    Args:
        targets: Optional list of targets (.dvc files, paths, stages).
                 If None, collects all tracked files.
        remote: Optional remote name (used for push/pull filtering).
        push: If True, use push mode; if False, use fetch mode.
        
    Returns:
        Dict with:
            - 'entries': List of dicts with 'path', 'hash', 'size', 'nfiles', 'is_dir', 'meta'
            - 'hash_to_path': Dict mapping hash -> workspace path
            - 'repo': The DVC Repo object (for further operations)
            - 'indexes': The raw indexes (for advanced use cases)
            
    Raises:
        DependencyError: If DVC internals are not available
    """
    try:
        from dvc.repo.fetch import _collect_indexes
    except ImportError as e:
        raise DependencyError(f"DVC internals not available: {e}")
    
    repo = Repo()
    
    indexes = _collect_indexes(
        repo,
        targets=targets,
        remote=remote,
        all_branches=False,
        with_deps=False,
        all_tags=False,
        recursive=False,
        all_commits=False,
        revs=None,
        workspace=True,
        push=push,
    )
    
    if not indexes:
        return {
            'entries': [],
            'hash_to_path': {},
            'repo': repo,
            'indexes': indexes,
        }
    
    entries = []
    hash_to_path: Dict[str, str] = {}
    seen_hashes = set()
    
    for idx in indexes.values():
        repo_data = idx.data.get('repo')
        if repo_data:
            for key, entry in repo_data.items():
                if entry.hash_info and entry.hash_info.value:
                    file_hash = entry.hash_info.value
                    path = '/'.join(key)
                    
                    # Always update hash_to_path mapping
                    hash_to_path[file_hash] = path
                    
                    # Skip duplicates for entries list
                    if file_hash in seen_hashes:
                        continue
                    seen_hashes.add(file_hash)
                    
                    is_dir = file_hash.endswith('.dir')
                    meta = entry.meta
                    
                    entries.append({
                        'path': path,
                        'hash': file_hash,
                        'size': meta.size if meta and meta.size else 0,
                        'nfiles': meta.nfiles if meta and meta.nfiles else 1,
                        'is_dir': is_dir,
                        'meta': meta,
                    })
    
    return {
        'entries': entries,
        'hash_to_path': hash_to_path,
        'repo': repo,
        'indexes': indexes,
    }


# =============================================================================
# DVC file utilities
# =============================================================================

def load_dvc_file(dvc_path: Path, repo: Optional[Any] = None) -> Any:
    """Load a .dvc file using DVC's internal parser.
    
    Returns a SingleStageFile object with typed access to stage data.
    
    Args:
        dvc_path: Path to the .dvc file.
        repo: Optional DVC Repo object. If not provided, one will be created.
        
    Returns:
        SingleStageFile object with .stage attribute containing:
        - stage.outs: List of Output objects
        - stage.deps: List of Dependency objects  
        - stage.is_repo_import: True if this is an import from another repo
        - stage.is_import: True if this is any kind of import
        
    Raises:
        DVCFileError: If the file cannot be parsed.
    """
    from dvc.dvcfile import load_file
    
    try:
        if repo is None:
            repo = Repo()
        return load_file(repo, str(dvc_path))
    except Exception as e:
        raise DVCFileError(f"Failed to parse {dvc_path}: {e}")


def parse_dvc_file(dvc_path: Path) -> Dict[str, Any]:
    """Parse a .dvc file and return its contents as a dictionary.
    
    This is a compatibility wrapper - prefer load_dvc_file() for new code.
    
    Args:
        dvc_path: Path to the .dvc file.
        
    Returns:
        Dictionary with the .dvc file contents.
        
    Raises:
        DVCFileError: If the file cannot be parsed.
    """
    import yaml
    
    try:
        with open(dvc_path) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        raise DVCFileError(f"Failed to parse {dvc_path}: {e}")


def is_repo_import(dvc_path: Path, repo: Optional[Any] = None) -> bool:
    """Check if a .dvc file is an import from another repository.
    
    Uses DVC's internal stage.is_repo_import property.
    
    Args:
        dvc_path: Path to the .dvc file.
        repo: Optional DVC Repo object.
        
    Returns:
        True if this .dvc file was created by `dvc import`.
    """
    try:
        dvc_file = load_dvc_file(dvc_path, repo)
        return dvc_file.stage.is_repo_import
    except DVCFileError:
        return False


def get_import_info(dvc_path: Path, repo: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    """Extract import information from a .dvc file.
    
    Uses DVC's internal RepoDependency to get source repo details.
    
    Args:
        dvc_path: Path to the .dvc file.
        repo: Optional DVC Repo object.
        
    Returns:
        Dictionary with 'url', 'rev', and 'path' keys, or None if not an import.
    """
    try:
        dvc_file = load_dvc_file(dvc_path, repo)
        stage = dvc_file.stage
        
        if not stage.is_repo_import:
            return None
        
        # Get the first repo dependency
        for dep in stage.deps:
            if hasattr(dep, 'def_repo') and dep.def_repo:
                return {
                    'url': dep.def_repo.get('url'),
                    'rev': dep.def_repo.get('rev_lock') or dep.def_repo.get('rev'),
                    'path': dep.def_path,
                }
        return None
    except DVCFileError:
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


# =============================================================================
# Project root discovery
# =============================================================================

def find_dvc_root(start: Optional[Path] = None) -> Optional[Path]:
    """Find the DVC project root using DVC internals.
    
    Uses Repo.find_root() which searches for a .dvc directory.
    
    Args:
        start: Starting path for the search. Defaults to cwd.
        
    Returns:
        Path to the DVC project root, or None if not in a DVC project.
    """
    try:
        root = Repo.find_root(root=str(start) if start else None)
        return Path(root)
    except Exception:
        return None


def find_git_root(start: Optional[Path] = None) -> Optional[Path]:
    """Find the git repository root using DVC internals.
    
    Args:
        start: Starting path for the search. Defaults to cwd.
        
    Returns:
        Path to the git root, or None if not in a git repository.
    """
    try:
        repo = Repo(root_dir=str(start) if start else None)
        return Path(repo.scm.root_dir)
    except Exception:
        return None


def find_project_root(start: Optional[Path] = None) -> Path:
    """Find the project root (git root preferred, then DVC root, then cwd).
    
    Args:
        start: Starting path for the search. Defaults to cwd.
        
    Returns:
        Path to the project root (never None, falls back to cwd).
    """
    # Try git root first (more common case)
    git_root = find_git_root(start)
    if git_root:
        return git_root
    
    # Try DVC root
    dvc_root = find_dvc_root(start)
    if dvc_root:
        return dvc_root
    
    # Fallback to cwd
    return start or Path.cwd()


# =============================================================================
# Git revision utilities
# =============================================================================

def get_hash_at_rev(path: str, rev: str, repo: Optional[Any] = None) -> Optional[str]:
    """Get DVC hash for a path at a specific git revision.
    
    Uses DVC internals for speed and to handle all tracking mechanisms
    (direct .dvc files, directories, dvc.lock).
    
    Args:
        path: Path to the DVC-tracked file or directory
        rev: Git revision (commit hash, tag, branch, HEAD~1, etc.)
        repo: Optional DVC Repo object for reuse across calls
        
    Returns:
        MD5 hash of the file at that revision, or None if not found/tracked
    """
    import logging
    
    try:
        from dvc.repo.fetch import _collect_indexes
    except ImportError:
        return None
    
    if repo is None:
        repo = Repo()
    
    # Suppress DVC warnings about missing files (expected when checking history)
    dvc_logger = logging.getLogger('dvc')
    old_level = dvc_logger.level
    dvc_logger.setLevel(logging.ERROR)
    
    try:
        indexes = _collect_indexes(
            repo,
            targets=[path],
            revs=[rev],
            workspace=False,
        )
        for idx in indexes.values():
            repo_data = idx.data.get('repo')
            if repo_data:
                for _k, entry in repo_data.items():
                    if entry.hash_info and entry.hash_info.value:
                        return entry.hash_info.value
    except Exception:
        return None
    finally:
        dvc_logger.setLevel(old_level)
    
    return None


def get_candidate_commits(
    paths: Optional[List[str]] = None,
    since: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[str]:
    """Get commits that modified DVC metadata files.
    
    These are the candidate commits where tracked files might have changed.
    
    Args:
        paths: Optional list of paths to filter by (looks for corresponding .dvc files)
        since: Optional date filter (e.g., "2025-01-01", "1 month ago")
        limit: Optional maximum number of commits to return
        
    Returns:
        List of commit hashes (newest first)
    """
    import subprocess
    
    cmd = ["git", "log", "--format=%H"]
    
    if since:
        cmd.append(f"--since={since}")
    
    # Add path filters for DVC metadata files
    cmd.append("--")
    cmd.extend(["*.dvc", "dvc.lock"])
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return []
    
    commits = [c.strip() for c in result.stdout.strip().split('\n') if c.strip()]
    
    if limit:
        commits = commits[:limit]
    
    return commits


def get_commit_info(commit: str) -> Dict[str, str]:
    """Get metadata for a git commit.
    
    Args:
        commit: Git commit hash
        
    Returns:
        Dict with 'hash', 'short_hash', 'date', 'message', 'author'
    """
    import subprocess
    
    result = subprocess.run(
        ["git", "log", "-1", "--format=%H|%h|%ai|%s|%an", commit],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        return {
            'hash': commit,
            'short_hash': commit[:7],
            'date': '',
            'message': '',
            'author': '',
        }
    
    parts = result.stdout.strip().split('|', 4)
    if len(parts) >= 5:
        return {
            'hash': parts[0],
            'short_hash': parts[1],
            'date': parts[2].split()[0],  # Just the date, not time
            'message': parts[3],
            'author': parts[4],
        }
    
    return {
        'hash': commit,
        'short_hash': commit[:7],
        'date': '',
        'message': '',
        'author': '',
    }
