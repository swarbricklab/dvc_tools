"""Clone functionality for DVC Tools.

Handles cloning DVC repositories with proper cache and remote setup.
"""

import subprocess
from pathlib import Path
from typing import Optional

from . import config as cfg
from . import cache as cache_mod


class CloneError(Exception):
    """Raised when clone operations fail."""
    pass


def resolve_repository_url(repo: str, owner: Optional[str] = None) -> str:
    """Resolve a repository specification to a full URL.
    
    If repo looks like a URL (contains : or /), return as-is.
    Otherwise, treat as a short name and construct URL using owner.
    
    Args:
        repo: Repository URL or short name
        owner: GitHub owner (user or organization)
        
    Returns:
        Full repository URL
        
    Raises:
        CloneError: If owner is needed but not configured
    """
    # Check if it looks like a full URL
    if ':' in repo or '/' in repo:
        return repo
    
    # It's a short name - need owner
    if not owner:
        owner = cfg.get_value('owner')
    
    if not owner:
        raise CloneError(
            f"Repository '{repo}' looks like a short name, but no owner is configured.\n"
            f"Either use a full URL (e.g., git@github.com:owner/{repo}.git) or set the owner:\n"
            f"  dt config set owner <github-user-or-org>"
        )
    
    return f"git@github.com:{owner}/{repo}.git"


def extract_repo_name(repository_url: str) -> str:
    """Extract the repository name from a URL.
    
    Args:
        repository_url: Full repository URL
        
    Returns:
        Repository name without .git suffix
    """
    name = repository_url.rstrip('/')
    name = name.rsplit('/', 1)[-1]  # Get last path component
    name = name.rsplit(':', 1)[-1]  # Handle git@github.com:org/repo format
    if name.endswith('.git'):
        name = name[:-4]
    return name


def clone_repository(
    repository: str,
    path: Optional[str] = None,
    owner: Optional[str] = None,
    no_submodules: bool = False,
    cache_name: Optional[str] = None,
    remote_name: Optional[str] = None,
    shallow: bool = False,
    verbose: bool = True,
) -> Path:
    """Clone a DVC repository with proper cache setup.
    
    Args:
        repository: Repository URL or short name
        path: Target directory (defaults to repo name)
        owner: GitHub owner for short names
        no_submodules: Skip cloning submodules
        cache_name: Override cache directory name
        remote_name: Override remote directory name
        shallow: Perform shallow clone
        verbose: Print progress messages
        
    Returns:
        Path to the cloned repository
        
    Raises:
        CloneError: If cloning fails
    """
    # Resolve repository URL
    repository_url = resolve_repository_url(repository, owner)
    repo_name = extract_repo_name(repository_url)
    
    # Determine target directory
    target_dir = Path(path if path else repo_name)
    
    # Use repo name for cache/remote if not specified
    cache_name = cache_name or repo_name
    remote_name = remote_name or repo_name
    
    if verbose:
        print(f"Cloning {repository_url}")
        if repository != repository_url:
            print(f"  (resolved from '{repository}')")
    
    # Build git clone command
    git_cmd = ['git', 'clone']
    if shallow:
        git_cmd.extend(['--depth', '1'])
    if not no_submodules:
        git_cmd.append('--recurse-submodules')
    git_cmd.extend([repository_url, str(target_dir)])
    
    # Execute git clone
    result = subprocess.run(git_cmd)
    if result.returncode != 0:
        raise CloneError("Git clone failed.")
    
    # Initialize submodules if needed
    if not no_submodules:
        if verbose:
            print("Updating submodules...")
        subprocess.run(
            ['git', 'submodule', 'update', '--init', '--recursive'],
            cwd=target_dir
        )
    
    # Set up cache using the cache module
    try:
        cache_mod.init_cache(
            name=cache_name,
            repo_path=target_dir,
            verbose=verbose,
        )
    except cache_mod.CacheError as e:
        if verbose:
            print(f"Warning: {e}")
    
    if verbose:
        print(f"\nCloned to {target_dir}/")
        print(f"\nNext steps:")
        print(f"  cd {target_dir}")
        print(f"  dvc pull          # Download all data files")
        print(f"  dvc pull <target> # Download selected files (faster)")
    
    return target_dir
