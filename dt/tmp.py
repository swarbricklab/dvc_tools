"""Temporary repository clone management for DVC Tools.

Manages clones of remote repositories in .dt/tmp/clones/ for accessing their
DVC configuration and history without checking out any data.

These were sparse clones once, limited to .dvc/ and *.dvc files. They are now
full clones: dvc.yaml resolution and `dvc diff` against an arbitrary revision
both need more of the tree than a sparse checkout carries. No DVC-tracked data
is downloaded either way.
"""

import hashlib
import os
import re
import stat
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




_ID_COMPONENT = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]*$')


def clone_path_for(repo_id: str, tmp_dir: Optional[Path] = None) -> Path:
    """Directory under ``.dt/tmp/clones`` for ``repo_id``.

    ``host/owner/repo`` ids map to that nested path; anything else (other
    depths, unusual characters) maps to a flat, hashed name under ``_other/``.
    The result is always strictly inside the clones directory and never
    inside another clone.

    Raises:
        TmpError: If the path would pass through a symlink.
    """
    tmp_dir = Path(tmp_dir) if tmp_dir is not None else get_tmp_dir()
    # .dt, .dt/tmp and .dt/tmp/clones themselves must be real directories.
    for comp in (tmp_dir, tmp_dir.parent, tmp_dir.parent.parent):
        if os.path.islink(comp):
            raise TmpError(f"Refusing clone dir {tmp_dir}: {comp} is a symlink")
    parts = str(repo_id).split('/')
    if len(parts) == 3 and all(len(p) <= 100 and _ID_COMPONENT.fullmatch(p) for p in parts):
        rel = Path(*parts)
    else:
        flat = re.sub(r'[^A-Za-z0-9._-]+', '-', str(repo_id)).strip('-.')[:80] or 'repo'
        digest = hashlib.sha256(str(repo_id).encode()).hexdigest()[:12]
        rel = Path('_other') / f'{flat}-{digest}'
    path = tmp_dir / rel
    # No symlinks between the clones dir and the clone.
    for comp in [path, *path.parents]:
        if comp == tmp_dir:
            break
        if os.path.islink(comp):
            raise TmpError(f"Refusing clone path {path}: {comp} is a symlink")
    return path


def _require_clone_root(repo_path: Path) -> None:
    """Require ``repo_path`` to be a clone root created by this user: a real
    directory with its own real ``.git`` directory (HEAD and objects present),
    both owned by the caller."""
    repo_path = Path(repo_path)
    uid = os.geteuid()
    try:
        root_st = os.lstat(repo_path)
        git_st = os.lstat(repo_path / '.git')
        head_st = os.lstat(repo_path / '.git' / 'HEAD')
        obj_st = os.lstat(repo_path / '.git' / 'objects')
        refs_st = os.lstat(repo_path / '.git' / 'refs')
    except FileNotFoundError:
        raise TmpError(f"{repo_path} is not a dt clone (no git directory)")
    if not (stat.S_ISDIR(root_st.st_mode) and stat.S_ISDIR(git_st.st_mode)
            and stat.S_ISREG(head_st.st_mode) and stat.S_ISDIR(obj_st.st_mode)
            and stat.S_ISDIR(refs_st.st_mode)):
        raise TmpError(f"{repo_path} is not a dt clone (not a git directory)")
    if root_st.st_uid != uid or git_st.st_uid != uid:
        raise TmpError(
            f"{repo_path} is not a dt clone owned by you; remove it "
            f"(rm -rf {repo_path}) and retry"
        )


def require_dvc_root(repo_path: Path) -> None:
    """Require ``repo_path`` to have its own real ``.dvc`` directory, so DVC
    run there acts on the clone and not on an enclosing repo."""
    try:
        st = os.lstat(Path(repo_path) / '.dvc')
    except FileNotFoundError:
        raise TmpError(f"{repo_path} is not a DVC repository")
    if not stat.S_ISDIR(st.st_mode):
        raise TmpError(f"{repo_path} is not a DVC repository (.dvc is not a directory)")


def _git(repo_path: Path, *args: str) -> list:
    """git command bound explicitly to the clone at ``repo_path``."""
    repo_path = Path(repo_path)
    return ['git', f'--git-dir={repo_path / ".git"}', f'--work-tree={repo_path}', *args]


def clone_repo(
    repo_spec: str,
    owner: Optional[str] = None,
    refresh: bool = True,
    verbose: bool = True,
    rev: Optional[str] = None,
) -> Path:
    """Clone or refresh a repository in .dt/tmp/clones/.

    Creates a full clone so that dvc.yaml, dvc.lock, and all .dvc files
    are available for DVC commands like `dvc list` and `dvc diff` can
    compare against any revision.

    Args:
        repo_spec: Repository URL or short name
        owner: Optional owner for short names
        refresh: If True, update existing clone (default True)
        verbose: Print progress messages
        rev: Optional git revision (branch, tag, or commit) to check out.
            When given, the clone is left at that revision instead of the
            default branch HEAD. Overrides the origin/HEAD reset done by a
            refresh.

    Returns:
        Path to the cloned repository

    Raises:
        TmpError: If cloning fails
    """
    try:
        utils.check_git()
    except utils.DependencyError as e:
        raise TmpError(str(e))

    if rev is not None and not utils.is_valid_git_rev(rev):
        raise TmpError(f"Invalid git revision: {rev!r}")

    url = resolve_repository_url(repo_spec, owner)
    repo_id = get_repo_id(repo_spec, owner)

    tmp_dir = get_tmp_dir()
    repo_path = clone_path_for(repo_id, tmp_dir)

    # Ensure .dt/.gitignore is up to date (covers /tmp/ and other entries)
    utils.ensure_dt_gitignore()

    if os.path.lexists(repo_path):
        _require_clone_root(repo_path)
        if refresh:
            if verbose:
                print(f"Refreshing {repo_id}...")
            _refresh_clone(repo_path, verbose=verbose)
        else:
            if verbose:
                print(f"Using cached {repo_id}")
    else:
        # Create parent directory
        repo_path.parent.mkdir(parents=True, exist_ok=True)

        if verbose:
            print(f"Cloning {url} to .dt/tmp/clones/{repo_id}...")

        # Full clone for dvc.yaml etc and to support dvc diff against any revision
        result = subprocess.run(
            ['git', 'clone', '--', url, str(repo_path)],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise TmpError(f"Failed to clone repository: {result.stderr}")

        if verbose:
            print(f"Cloned to .dt/tmp/clones/{repo_id}")

    # Check out an explicit revision if requested (after any refresh reset).
    if rev:
        _checkout_rev(repo_path, rev, verbose=verbose)

    return repo_path


def _checkout_rev(repo_path: Path, rev: str, verbose: bool = True) -> Path:
    """Fetch and check out a specific git revision in an existing clone.

    Handles branches, tags, and commit hashes. The clone may be shallow, so
    the revision is fetched explicitly before checkout.

    Args:
        repo_path: Path to the git repository clone.
        rev: Git revision (branch, tag, or commit hash) to check out.
        verbose: Print progress messages.

    Returns:
        Path to the repository.

    Raises:
        TmpError: If the revision cannot be fetched or checked out.
    """
    if not utils.is_valid_git_rev(rev):
        raise TmpError(f"Invalid git revision: {rev!r}")
    _require_clone_root(repo_path)

    if verbose:
        print(f"Checking out revision {rev}...")

    # Fetch the specific rev (may not be present in a shallow clone). Try a
    # shallow fetch first, then a full fetch as a fallback.
    fetched = subprocess.run(
        _git(repo_path, 'fetch', '--depth', '1', '--end-of-options', 'origin', rev),
        cwd=repo_path, capture_output=True, text=True,
    )
    if fetched.returncode != 0:
        fetched = subprocess.run(
            _git(repo_path, 'fetch', '--end-of-options', 'origin', rev),
            cwd=repo_path, capture_output=True, text=True,
        )

    # Prefer checking out the named rev; fall back to FETCH_HEAD (covers the
    # case where the rev was only reachable via the explicit fetch above).
    checkout = subprocess.run(
        _git(repo_path, 'checkout', '--end-of-options', rev),
        cwd=repo_path, capture_output=True, text=True,
    )
    if checkout.returncode != 0:
        checkout = subprocess.run(
            _git(repo_path, 'checkout', 'FETCH_HEAD'),
            cwd=repo_path, capture_output=True, text=True,
        )
        if checkout.returncode != 0:
            detail = checkout.stderr.strip() or fetched.stderr.strip() or 'unknown error'
            raise TmpError(f"Could not check out revision '{rev}': {detail}")

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
    _require_clone_root(repo_path)

    # Fetch latest
    result = subprocess.run(
        _git(repo_path, 'fetch', '--depth', '1', 'origin'),
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise TmpError(f"Failed to fetch: {result.stderr}")
    
    # Reset to origin/HEAD
    result = subprocess.run(
        _git(repo_path, 'reset', '--hard', 'origin/HEAD'),
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
        repo_path = clone_path_for(repo_id, tmp_dir)
        try:
            _require_clone_root(repo_path)
        except TmpError:
            repo_path = None
        if repo_path is not None:
            shutil.rmtree(repo_path)
            removed.append(repo_id)
    else:
        # Remove all
        for path in tmp_dir.iterdir():
            if path.is_dir() and not path.is_symlink() and (path / ".git").exists():
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
    if not utils.is_valid_git_rev(revision):
        if verbose:
            print(f"  Error: invalid revision {revision!r}")
        return False
    try:
        _require_clone_root(repo_path)
    except TmpError as e:
        if verbose:
            print(f"  Error: {e}")
        return False

    # First, fetch the specific commit (it may not be in our shallow clone)
    if verbose:
        print(f"  Fetching revision {revision[:12]}...")
    
    result = subprocess.run(
        _git(repo_path, 'fetch', '--depth', '1', '--end-of-options', 'origin', revision),
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        if verbose:
            print(f"  Warning: Could not fetch revision: {result.stderr.strip()}")
        # Try without depth in case it's already available
        result = subprocess.run(
            _git(repo_path, 'fetch', '--end-of-options', 'origin', revision),
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
        _git(repo_path, 'sparse-checkout', 'disable'),
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    # Ignore errors - sparse checkout may not be enabled
    
    # Checkout the specific revision for the whole repo
    if verbose:
        print(f"  Checking out revision {revision[:12]}...")
    
    result = subprocess.run(
        _git(repo_path, 'checkout', '--end-of-options', revision),
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        # Try checkout with FETCH_HEAD
        if verbose:
            print(f"  Trying checkout of FETCH_HEAD...")
        result = subprocess.run(
            _git(repo_path, 'checkout', 'FETCH_HEAD'),
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
