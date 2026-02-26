"""Temporary repository clone management for DVC Tools.

Manages sparse clones of remote repositories in .dt/tmp/clones/ for accessing
their DVC configuration without a full checkout.
"""

import re
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from . import config as cfg
from . import utils
from .errors import TmpError


def get_tmp_dir() -> Path:
    """Get the .dt/tmp/clones directory path for the current repo.
    
    The .dt directory is always at the project root (alongside .git/.dvc),
    regardless of the current working directory.
    
    Returns:
        Path to the .dt/tmp/clones directory.
    """
    return utils.get_dt_dir() / "tmp" / "clones"


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
        TmpError: If owner is needed but not configured
    """
    # Check if it looks like a full URL
    if ':' in repo or '/' in repo:
        return repo
    
    # It's a short name - need owner
    if not owner:
        owner = cfg.get_value('owner')
    
    if not owner:
        raise TmpError(
            f"Repository '{repo}' looks like a short name, but no owner is configured.\n"
            f"Either use a full URL (e.g., git@github.com:owner/{repo}.git) or set the owner:\n"
            f"  dt config set owner <github-user-or-org>"
        )
    
    return f"git@github.com:{owner}/{repo}.git"


def get_repo_id(repo_spec: str, owner: Optional[str] = None) -> str:
    """Convert a repository URL or name to a path-like directory structure.
    
    Examples:
        git@github.com:myorg/myproject.git -> github.com/myorg/myproject
        https://github.com/myorg/myproject -> github.com/myorg/myproject
        myproject (with owner=myorg) -> github.com/myorg/myproject
    
    Args:
        repo_spec: Repository URL, SSH path, or short name
        owner: Optional owner for short names
        
    Returns:
        Path-like identifier string (e.g., "github.com/owner/repo")
    """
    url = resolve_repository_url(repo_spec, owner)
    
    # Handle SSH format: git@github.com:owner/repo.git
    ssh_match = re.match(r'git@([^:]+):(.+?)(?:\.git)?$', url)
    if ssh_match:
        host = ssh_match.group(1)
        path = ssh_match.group(2)
        return f"{host}/{path}"
    
    # Handle HTTPS format: https://github.com/owner/repo
    https_match = re.match(r'https?://([^/]+)/(.+?)(?:\.git)?$', url)
    if https_match:
        host = https_match.group(1)
        path = https_match.group(2)
        return f"{host}/{path}"
    
    # Fallback: sanitize the whole URL into a flat name
    safe = re.sub(r'[^\w\-]', '-', url)
    safe = re.sub(r'-+', '-', safe)
    return safe.strip('-')


def ensure_gitignore() -> bool:
    """Ensure .dt/tmp is in .gitignore.
    
    Creates or updates .gitignore to include .dt/tmp/ pattern.
    Note: The .dt/.gitignore already ignores /tmp/, but this adds
    .dt/tmp/ to the root .gitignore for backward compatibility.
    
    Returns:
        True if .gitignore was modified, False if already contains pattern.
    """
    return utils.update_gitignore(".dt/tmp/")


def ensure_dvcignore() -> bool:
    """Ensure .dt/tmp is in .dvcignore.
    
    Prevents DVC from scanning temp clones (which may contain their own
    .dvc directories and tracked files).
    
    Returns:
        True if .dvcignore was modified, False if already contains pattern.
    """
    dvcignore_path = Path.cwd() / ".dvcignore"
    pattern = ".dt/tmp/"
    pattern_normalized = pattern.rstrip('/')
    
    # Check if already present
    if dvcignore_path.exists():
        content = dvcignore_path.read_text()
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
    
    dvcignore_path.write_text(content)
    return True


def clone_repo(
    repo_spec: str,
    owner: Optional[str] = None,
    refresh: bool = True,
    verbose: bool = True,
) -> Path:
    """Clone or refresh a repository in .dt/tmp/clones/.
    
    Creates a shallow clone with full checkout (not sparse) so that
    dvc.yaml, dvc.lock, and all .dvc files are available for DVC
    commands like `dvc list`.
    
    Args:
        repo_spec: Repository URL or short name
        owner: Optional owner for short names
        refresh: If True, update existing clone (default True)
        verbose: Print progress messages
        
    Returns:
        Path to the cloned repository
        
    Raises:
        TmpError: If cloning fails
    """
    try:
        utils.check_git()
    except utils.DependencyError as e:
        raise TmpError(str(e))
    
    url = resolve_repository_url(repo_spec, owner)
    repo_id = get_repo_id(repo_spec, owner)
    
    tmp_dir = get_tmp_dir()
    repo_path = tmp_dir / repo_id
    
    # Ensure .dt/tmp is gitignored and dvcignored
    ensure_gitignore()
    ensure_dvcignore()
    
    if repo_path.exists():
        if refresh:
            if verbose:
                print(f"Refreshing {repo_id}...")
            return _refresh_clone(repo_path, verbose=verbose)
        else:
            if verbose:
                print(f"Using cached {repo_id}")
            return repo_path
    
    # Create parent directory
    tmp_dir.mkdir(parents=True, exist_ok=True)
    
    if verbose:
        print(f"Cloning {url} to .dt/tmp/clones/{repo_id}...")

    # Full shallow clone (depth 1 for speed, but full checkout for dvc.yaml etc)
    result = subprocess.run(
        ['git', 'clone', '--depth', '1', '--single-branch', url, str(repo_path)],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise TmpError(f"Failed to clone repository: {result.stderr}")
    
    if verbose:
        print(f"Cloned to .dt/tmp/clones/{repo_id}")
    
    return repo_path


def _refresh_clone(repo_path: Path, verbose: bool = True) -> Path:
    """Refresh an existing clone with latest changes.
    
    Args:
        repo_path: Path to the repository
        verbose: Print progress messages
        
    Returns:
        Path to the repository
        
    Raises:
        TmpError: If refresh fails
    """
    # Fetch latest
    result = subprocess.run(
        ['git', 'fetch', '--depth', '1', 'origin'],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise TmpError(f"Failed to fetch: {result.stderr}")
    
    # Reset to origin/HEAD
    result = subprocess.run(
        ['git', 'reset', '--hard', 'origin/HEAD'],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise TmpError(f"Failed to reset: {result.stderr}")
    
    if verbose:
        # Show relative path from tmp_dir
        tmp_dir = get_tmp_dir()
        try:
            rel_path = repo_path.relative_to(tmp_dir)
            print(f"Refreshed {rel_path}")
        except ValueError:
            print(f"Refreshed {repo_path.name}")
    
    return repo_path


def list_repos() -> List[Tuple[str, Path]]:
    """List all cached repository clones.
    
    Returns:
        List of (repo_id, path) tuples where repo_id is the path
        relative to .dt/tmp/clones/ (e.g., "github.com/owner/repo")
    """
    tmp_dir = get_tmp_dir()
    
    if not tmp_dir.exists():
        return []
    
    repos = []
    # Recursively find all directories containing .git
    for git_dir in tmp_dir.rglob(".git"):
        if git_dir.is_dir():
            repo_path = git_dir.parent
            # Get path relative to tmp_dir
            try:
                repo_id = str(repo_path.relative_to(tmp_dir))
                repos.append((repo_id, repo_path))
            except ValueError:
                pass
    
    return sorted(repos)


def clean_repos(repo_spec: Optional[str] = None, owner: Optional[str] = None) -> List[str]:
    """Remove cached repository clones.
    
    Args:
        repo_spec: Specific repo to clean, or None for all
        owner: Optional owner for short names
        
    Returns:
        List of removed repository identifiers
    """
    import shutil
    
    tmp_dir = get_tmp_dir()
    
    if not tmp_dir.exists():
        return []
    
    removed = []
    
    if repo_spec:
        # Remove specific repo
        repo_id = get_repo_id(repo_spec, owner)
        repo_path = tmp_dir / repo_id
        if repo_path.exists():
            shutil.rmtree(repo_path)
            removed.append(repo_id)
    else:
        # Remove all
        for path in tmp_dir.iterdir():
            if path.is_dir() and (path / ".git").exists():
                shutil.rmtree(path)
                removed.append(path.name)
        
        # Also remove tmp_dir if empty
        try:
            tmp_dir.rmdir()
        except OSError:
            pass  # Not empty or doesn't exist
    
    return removed


def get_repo_dvc_config(repo_spec: str, owner: Optional[str] = None) -> Optional[Path]:
    """Get path to a repository's .dvc/config file.
    
    Clones/refreshes the repo if needed.
    
    Args:
        repo_spec: Repository URL or short name
        owner: Optional owner for short names
        
    Returns:
        Path to .dvc/config, or None if not found
    """
    repo_path = clone_repo(repo_spec, owner=owner, refresh=True, verbose=False)
    config_path = repo_path / ".dvc" / "config"
    
    if config_path.exists():
        return config_path
    
    return None


def checkout_path_at_revision(
    repo_path: Path,
    path: str,
    revision: str,
    verbose: bool = False,
) -> bool:
    """Checkout a specific path at a specific revision in a clone.
    
    This is used when we need to access source files at the exact revision
    that was used for an import (rev_lock). The clone may be sparse and shallow,
    so we need to fetch the specific commit first.
    
    Args:
        repo_path: Path to the git repository clone.
        path: Path within the repo to checkout.
        revision: Git revision (commit hash) to checkout.
        verbose: Print progress messages.
        
    Returns:
        True if successful, False otherwise.
    """
    # First, fetch the specific commit (it may not be in our shallow clone)
    if verbose:
        print(f"  Fetching revision {revision[:12]}...")
    
    result = subprocess.run(
        ['git', 'fetch', '--depth', '1', 'origin', revision],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        if verbose:
            print(f"  Warning: Could not fetch revision: {result.stderr.strip()}")
        # Try without depth in case it's already available
        result = subprocess.run(
            ['git', 'fetch', 'origin', revision],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            if verbose:
                print(f"  Error: Fetch failed: {result.stderr.strip()}")
            return False
    
    # Disable sparse checkout to get the full tree at this revision
    if verbose:
        print(f"  Disabling sparse checkout to access full tree...")
    
    result = subprocess.run(
        ['git', 'sparse-checkout', 'disable'],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    # Ignore errors - sparse checkout may not be enabled
    
    # Checkout the specific revision for the whole repo
    if verbose:
        print(f"  Checking out revision {revision[:12]}...")
    
    result = subprocess.run(
        ['git', 'checkout', revision],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        # Try checkout with FETCH_HEAD
        if verbose:
            print(f"  Trying checkout of FETCH_HEAD...")
        result = subprocess.run(
            ['git', 'checkout', 'FETCH_HEAD'],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            if verbose:
                print(f"  Error: Could not checkout: {result.stderr.strip()}")
            return False
    
    # Verify the path exists
    target_path = repo_path / path
    if not target_path.exists():
        if verbose:
            print(f"  Warning: Path does not exist after checkout: {path}")
        return False
    
    return True
