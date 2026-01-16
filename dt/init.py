"""Project initialization for DVC Tools.

Handles complete DVC project setup including git, DVC, cache, and remote configuration.
"""

import subprocess
from pathlib import Path
from typing import Optional

from . import config as cfg
from . import cache as cache_mod
from . import remote as remote_mod
from . import utils


class InitError(Exception):
    """Raised when initialization operations fail."""
    pass


def check_dependencies(require_dvc: bool = True, require_git: bool = True) -> None:
    """Check that required external tools are available.
    
    Args:
        require_dvc: Check for dvc command
        require_git: Check for git command
        
    Raises:
        InitError: If required tools are not found
    """
    try:
        if require_git:
            utils.check_git()
        if require_dvc:
            utils.check_dvc()
    except utils.DependencyError as e:
        raise InitError(str(e))


def init_git(repo_path: Path, verbose: bool = True) -> bool:
    """Initialize git repository if not already initialized.
    
    Args:
        repo_path: Path to the repository
        verbose: Print progress messages
        
    Returns:
        True if git was initialized, False if already existed
    """
    git_dir = repo_path / '.git'
    
    if git_dir.exists():
        if verbose:
            print("Git repository already initialized.")
        return False
    
    if verbose:
        print("Initializing git repository...")
    
    result = subprocess.run(
        ['git', 'init'],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise InitError(f"Failed to initialize git: {result.stderr}")
    
    return True


def check_github_remote(repo_path: Path, project_name: str, org: str = None, team: str = None, verbose: bool = True) -> bool:
    """Check if a GitHub remote exists for the repository.
    
    Args:
        repo_path: Path to the repository
        project_name: Name of the project
        org: GitHub organization
        team: GitHub team to grant access
        verbose: Print suggestion if remote doesn't exist
        
    Returns:
        True if a remote named 'origin' exists, False otherwise
    """
    result = subprocess.run(
        ['git', 'remote', 'get-url', 'origin'],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        if verbose:
            print("\nNo GitHub remote configured.")
            org_part = org if org else "<org>"
            team_opt = f" --team={team}" if team else ""
            print(f"Create one with:")
            print(f"  gh repo create {org_part}/{project_name} --source=. --remote=origin --private{team_opt}")
        return False
    
    return True


def init_dvc(repo_path: Path, verbose: bool = True) -> bool:
    """Initialize DVC in the repository if not already initialized.
    
    Args:
        repo_path: Path to the repository
        verbose: Print progress messages
        
    Returns:
        True if DVC was initialized, False if already existed
    """
    dvc_dir = repo_path / '.dvc'
    
    if dvc_dir.exists():
        if verbose:
            print("DVC already initialized.")
        return False
    
    if verbose:
        print("Initializing DVC...")
    
    result = subprocess.run(
        ['dvc', 'init'],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise InitError(f"Failed to initialize DVC: {result.stderr}")
    
    if verbose:
        print("\nDVC initialized. Commit the changes with:")
        print('  git add .dvc .dvcignore && git commit -m "dvc: init"')
    
    return True


def install_dvc_hooks(repo_path: Path, verbose: bool = True) -> None:
    """Install DVC git hooks.
    
    Args:
        repo_path: Path to the repository
        verbose: Print progress messages
    """
    if verbose:
        print("Installing DVC git hooks...")
    
    subprocess.run(
        ['dvc', 'install'],
        cwd=repo_path,
        capture_output=True,
    )


def init_project(
    name: Optional[str] = None,
    org: Optional[str] = None,
    cache_root: Optional[str] = None,
    remote_root: Optional[str] = None,
    no_git: bool = False,
    no_dvc: bool = False,
    no_cache: bool = False,
    no_remote: bool = False,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
) -> dict:
    """Initialize a complete DVC project.
    
    Orchestrates all initialization steps:
    1. Git repository setup
    2. DVC initialization
    3. Cache configuration
    4. Remote storage setup
    5. Git hooks installation
    
    Args:
        name: Project name (defaults to current directory name)
        org: GitHub organization
        cache_root: Root directory for caches
        remote_root: Root directory for remotes
        no_git: Skip git initialization
        no_dvc: Skip DVC initialization
        no_cache: Skip cache setup
        no_remote: Skip remote setup
        repo_path: Path to the repository (defaults to cwd)
        verbose: Print progress messages
        
    Returns:
        Dict with paths to initialized components
        
    Raises:
        InitError: If initialization fails
    """
    # Check dependencies first
    check_dependencies(require_dvc=not no_dvc, require_git=not no_git)
    
    repo_path = repo_path or Path.cwd()
    project_name = name or utils.get_project_name()
    
    result = {
        'name': project_name,
        'path': repo_path,
        'git': None,
        'dvc': None,
        'cache': None,
        'remote': None,
    }
    
    if verbose:
        print(f"Initializing DVC project: {project_name}")
        print(f"  Path: {repo_path}")
        print()
    
    # Step 1: Git
    if not no_git:
        init_git(repo_path, verbose=verbose)
        result['git'] = repo_path / '.git'
    
    # Step 2: DVC
    if not no_dvc:
        init_dvc(repo_path, verbose=verbose)
        result['dvc'] = repo_path / '.dvc'
    
    # Step 3: Cache
    if not no_cache:
        try:
            cache_dir = cache_mod.init_cache(
                name=project_name,
                cache_root=cache_root,
                repo_path=repo_path,
                verbose=verbose,
            )
            result['cache'] = cache_dir
        except cache_mod.CacheError as e:
            if verbose:
                print(f"Warning: {e}")
    
    # Step 4: Remote
    if not no_remote:
        try:
            remote_dir = remote_mod.init_remote(
                name=project_name,
                remote_root=remote_root,
                repo_path=repo_path,
                verbose=verbose,
            )
            result['remote'] = remote_dir
        except remote_mod.RemoteError as e:
            if verbose:
                print(f"Warning: {e}")
    
    # Step 5: Git hooks
    if not no_dvc:
        install_dvc_hooks(repo_path, verbose=verbose)
    
    # Step 6: Check for GitHub remote
    if not no_git:
        # Get org and team from argument or config
        effective_org = org or cfg.get_value('org')
        effective_team = cfg.get_value('team')
        check_github_remote(repo_path, project_name, org=effective_org, team=effective_team, verbose=verbose)
    
    if verbose:
        print()
        print("Initialization complete!")
        if result['cache']:
            print(f"  Cache: {result['cache']}")
        if result['remote']:
            print(f"  Remote: {result['remote']}")
    
    return result
