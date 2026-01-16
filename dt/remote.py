"""Remote storage management for DVC Tools.

Handles DVC remote setup with SSH and local access methods for HPC environments.
"""

import subprocess
from pathlib import Path
from typing import Optional

from . import config as cfg
from . import utils


class RemoteError(Exception):
    """Raised when remote operations fail."""
    pass


def resolve_remote_path(
    name: Optional[str] = None,
    remote_root: Optional[str] = None,
    remote_path: Optional[str] = None,
) -> Path:
    """Resolve the remote directory path.
    
    Path resolution order:
    1. remote_path - Complete path override
    2. Constructed: {remote_root}/{name}
    
    Args:
        name: Project name (defaults to current directory name)
        remote_root: Root directory for remotes
        remote_path: Complete path override
        
    Returns:
        Resolved remote directory path
        
    Raises:
        RemoteError: If remote location cannot be determined
    """
    if remote_path:
        return Path(remote_path).resolve()
    
    # Get remote root from argument or config
    root = remote_root or cfg.get_value('remote.root')
    if not root:
        raise RemoteError(
            "Remote root not configured.\n"
            "Either specify --remote-root or set remote.root:\n"
            "  dt config set remote.root /path/to/remote"
        )
    
    # Get project name
    project_name = name or utils.get_project_name()
    
    return Path(root) / project_name


def init_remote_structure(remote_dir: Path, verbose: bool = True) -> None:
    """Initialize the remote directory structure with proper permissions.
    
    Creates the files/md5 subdirectories (00-ff) required by DVC
    with group write permissions for shared access.
    
    Args:
        remote_dir: Path to the remote directory
        verbose: Print progress messages
    """
    if verbose:
        print(f"Initializing remote structure at {remote_dir}")
    
    remote_dir.mkdir(parents=True, exist_ok=True)
    utils.set_group_writable(remote_dir)
    
    # Create files/md5 structure with 00-ff subdirectories
    utils.create_md5_subdirs(remote_dir, verbose=verbose)


def configure_dvc_remote(
    repo_path: Path,
    remote_dir: Path,
    remote_name: Optional[str] = None,
    verbose: bool = True,
) -> None:
    """Configure DVC remotes for SSH and local access.
    
    Sets up:
    - Default SSH remote for external access
    - Local remote override for efficient internal transfers
    
    Args:
        repo_path: Path to the DVC repository
        remote_dir: Path to the remote directory
        remote_name: Name for the remote (defaults to project name)
        verbose: Print progress messages
    """
    project_name = remote_name or remote_dir.name
    
    # Get SSH host from config
    ssh_host = cfg.get_value('ssh.host')
    
    if ssh_host:
        # Set up SSH remote as default
        ssh_url = f"ssh://{ssh_host}{remote_dir}"
        if verbose:
            print(f"Configuring SSH remote '{project_name}': {ssh_url}")
        
        result = subprocess.run(
            ['dvc', 'remote', 'add', '-d', project_name, ssh_url],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        
        # Ignore error if remote already exists
        if result.returncode != 0 and 'already exists' not in result.stderr:
            raise RemoteError(f"Failed to add SSH remote: {result.stderr}")
    
    # Set up local remote for efficient internal transfers
    if verbose:
        print(f"Configuring local remote: {remote_dir}")
    
    result = subprocess.run(
        ['dvc', 'remote', 'add', '--local', 'local', str(remote_dir)],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    # Ignore error if remote already exists
    if result.returncode != 0 and 'already exists' not in result.stderr:
        raise RemoteError(f"Failed to add local remote: {result.stderr}")


def init_remote(
    name: Optional[str] = None,
    remote_root: Optional[str] = None,
    remote_path: Optional[str] = None,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
) -> Path:
    """Initialize remote storage for a DVC project.
    
    Creates the remote directory structure with proper permissions
    and configures DVC remotes for SSH and local access.
    
    Args:
        name: Project name (defaults to current directory name)
        remote_root: Root directory for remotes
        remote_path: Complete path override
        repo_path: Path to the DVC repository (defaults to cwd)
        verbose: Print progress messages
        
    Returns:
        Path to the initialized remote directory
        
    Raises:
        RemoteError: If remote initialization fails
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise RemoteError(str(e))
    
    repo_path = repo_path or Path.cwd()
    remote_dir = resolve_remote_path(name, remote_root, remote_path)
    
    if remote_dir.exists():
        if verbose:
            print(f"Using existing remote at {remote_dir}")
    else:
        if verbose:
            print(f"Creating remote at {remote_dir}")
        init_remote_structure(remote_dir, verbose=verbose)
    
    configure_dvc_remote(repo_path, remote_dir, name, verbose=verbose)
    
    return remote_dir
