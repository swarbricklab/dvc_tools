"""Remote storage management for DVC Tools.

Handles DVC remote setup with SSH and local access methods for HPC environments.
"""

import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

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


def _run_dvc_remote_list(repo_path: Path) -> List[Tuple[str, str, bool]]:
    """Run dvc remote list in a repository and parse output.
    
    Args:
        repo_path: Path to the repository
        
    Returns:
        List of (remote_name, url, is_default) tuples
    """
    result = subprocess.run(
        ['dvc', 'remote', 'list'],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        return []
    
    # Get default remote
    default_result = subprocess.run(
        ['dvc', 'config', 'core.remote'],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    default_remote = default_result.stdout.strip() if default_result.returncode == 0 else None
    
    # Parse output - handle wrapped lines where URL may be on next line
    # DVC outputs: "name\turl" but wraps long lines
    remotes = []
    lines = result.stdout.strip().split('\n')
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue
        
        # Remove (default) marker if present
        line = line.replace('(default)', '').strip()
        
        parts = line.split(None, 1)
        if not parts:
            i += 1
            continue
        
        name = parts[0]
        
        if len(parts) > 1:
            # URL on same line
            url = parts[1].strip()
        else:
            # URL might be on next line (wrapped)
            url = ''
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip().replace('(default)', '').strip()
                # If next line starts with a protocol, it's the URL
                if next_line.startswith(('ssh://', 'http://', 'https://', 's3://', '/', 'gs://')):
                    url = next_line
                    i += 1
        
        is_default = name == default_remote
        remotes.append((name, url, is_default))
        i += 1
    
    return remotes


def list_remotes(repo_path: Optional[Path] = None) -> List[Tuple[str, str, bool]]:
    """List remotes for a DVC repository.
    
    Uses `dvc remote list` to get remotes.
    
    Args:
        repo_path: Path to the repository (defaults to cwd)
        
    Returns:
        List of (remote_name, url, is_default) tuples
    """
    repo_path = repo_path or Path.cwd()
    return _run_dvc_remote_list(repo_path)


def list_remotes_from_repo(
    repo_spec: str,
    owner: Optional[str] = None,
) -> List[Tuple[str, str, bool]]:
    """List remotes for a remote repository.
    
    Uses tmp clone infrastructure to clone the repo and run
    `dvc remote list` within it.
    
    Args:
        repo_spec: Repository URL or short name
        owner: Optional owner for short names
        
    Returns:
        List of (remote_name, url, is_default) tuples
    """
    from . import tmp as tmp_mod
    
    repo_path = tmp_mod.clone_repo(repo_spec, owner=owner, refresh=True, verbose=False)
    return _run_dvc_remote_list(repo_path)


def extract_local_path(url: str) -> Optional[str]:
    """Extract the local filesystem path from a remote URL.
    
    Handles:
    - Local paths: /path/to/remote -> /path/to/remote
    - SSH URLs: ssh://host/path/to/remote -> /path/to/remote
    - SSH URLs with user: ssh://user@host/path -> /path
    
    Args:
        url: Remote URL
        
    Returns:
        Local path if extractable, None otherwise
    """
    import re
    
    url = url.strip()
    
    # Already a local path
    if url.startswith('/'):
        return url
    
    # SSH URL format: ssh://[user@]host/path
    ssh_match = re.match(r'ssh://(?:[^@]+@)?[^/]+(/.*)', url)
    if ssh_match:
        return ssh_match.group(1)
    
    # Not a local-accessible format (s3://, gs://, https://, etc.)
    return None


def find_local_remote(
    remotes: List[Tuple[str, str, bool]],
    check_exists: bool = True,
) -> Optional[Tuple[str, str]]:
    """Find a remote that is accessible on the local filesystem.
    
    Checks remotes in order, returning the first one whose path
    exists on the local filesystem.
    
    Args:
        remotes: List of (name, url, is_default) tuples
        check_exists: If True, verify the path exists (default True)
        
    Returns:
        Tuple of (remote_name, local_path) if found, None otherwise
    """
    for name, url, is_default in remotes:
        local_path = extract_local_path(url)
        if local_path:
            if check_exists:
                if Path(local_path).exists():
                    return (name, local_path)
            else:
                return (name, local_path)
    
    return None


def find_local_remote_from_repo(
    repo_spec: str,
    owner: Optional[str] = None,
    check_exists: bool = True,
) -> Optional[Tuple[str, str]]:
    """Find a locally-accessible remote from a remote repository.
    
    Args:
        repo_spec: Repository URL or short name
        owner: Optional owner for short names
        check_exists: If True, verify the path exists (default True)
        
    Returns:
        Tuple of (remote_name, local_path) if found, None otherwise
    """
    remotes = list_remotes_from_repo(repo_spec, owner=owner)
    return find_local_remote(remotes, check_exists=check_exists)
