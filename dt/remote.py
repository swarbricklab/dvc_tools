"""Remote storage management for DVC Tools.

Handles DVC remote setup with SSH and local access methods for HPC environments.
"""

import os
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from . import config as cfg
from . import utils
from .errors import RemoteError


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
        ['dvc', 'remote', 'add', '--local', '-d', 'local', str(remote_dir)],
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


def _run_dvc_remote_list(
    repo_path: Path,
    project_only: bool = False,
) -> List[Tuple[str, str, bool]]:
    """Run dvc remote list in a repository and parse output.
    
    Args:
        repo_path: Path to the repository
        project_only: If True, only list remotes defined in project scope
            (``.dvc/config``), excluding local overrides.
        
    Returns:
        List of (remote_name, url, is_default) tuples
    """
    # Set COLUMNS to prevent DVC from wrapping output
    env = os.environ.copy()
    env['COLUMNS'] = '1000'
    
    cmd = ['dvc', 'remote', 'list']
    if project_only:
        cmd.append('--project')
    
    result = subprocess.run(
        cmd,
        cwd=repo_path,
        capture_output=True,
        text=True,
        env=env,
    )
    
    if result.returncode != 0:
        return []
    
    # Get default remote (from same scope)
    config_cmd = ['dvc', 'config', 'core.remote']
    if project_only:
        config_cmd.append('--project')
    
    default_result = subprocess.run(
        config_cmd,
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    default_remote = default_result.stdout.strip() if default_result.returncode == 0 else None
    
    # DVC outputs one line per remote: "name<whitespace>url"
    # Default remote has "(default)" suffix
    remotes = []
    
    for line in result.stdout.strip().split('\n'):
        if not line.strip():
            continue
        
        # Check for and remove (default) marker
        is_default_marker = '(default)' in line
        line = line.replace('(default)', '').strip()
        
        # Split on whitespace: name<whitespace>url
        parts = line.split(None, 1)
        if len(parts) >= 2:
            name = parts[0]
            url = parts[1].strip()
            is_default = (name == default_remote) or is_default_marker
            remotes.append((name, url, is_default))
        elif len(parts) == 1:
            # Name only, no URL (shouldn't happen but handle gracefully)
            remotes.append((parts[0], '', parts[0] == default_remote))
    
    return remotes


def list_remotes(
    repo_path: Optional[Path] = None,
    project_only: bool = False,
) -> List[Tuple[str, str, bool]]:
    """List remotes for a DVC repository.
    
    Uses `dvc remote list` to get remotes.
    
    Args:
        repo_path: Path to the repository (defaults to cwd)
        project_only: If True, only list remotes defined in project scope
            (``.dvc/config``), excluding local overrides.
        
    Returns:
        List of (remote_name, url, is_default) tuples
    """
    repo_path = repo_path or Path.cwd()
    return _run_dvc_remote_list(repo_path, project_only=project_only)


def list_remotes_from_repo(
    repo_spec: str,
    owner: Optional[str] = None,
    project_only: bool = False,
) -> List[Tuple[str, str, bool]]:
    """List remotes for a remote repository.
    
    If repo_spec is a local path, runs `dvc remote list` directly.
    Otherwise, uses tmp clone infrastructure to clone the repo first.
    
    Args:
        repo_spec: Repository URL, local path, or short name
        owner: Optional owner for short names
        project_only: If True, only list remotes defined in project scope.
        
    Returns:
        List of (remote_name, url, is_default) tuples
    """
    # Check if repo_spec is a local path that already exists
    local_repo = Path(repo_spec)
    if local_repo.exists() and (local_repo / '.dvc').is_dir():
        # Local repo - run dvc remote list directly to get correct resolved paths
        return _run_dvc_remote_list(local_repo, project_only=project_only)
    
    # Remote repo - clone it first
    # Use refresh=False to avoid network operations when clone already exists
    from . import tmp as tmp_mod
    
    repo_path = tmp_mod.clone_repo(repo_spec, owner=owner, refresh=False, verbose=False)
    return _run_dvc_remote_list(repo_path, project_only=project_only)


def parse_remote_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse a remote URL into host and path components.
    
    Handles:
    - Local paths: /path/to/remote -> (None, /path/to/remote)
    - file:// URLs: file:///path -> (None, /path)
    - SSH URLs: ssh://host/path -> (host, /path)
    - SSH URLs with user: ssh://user@host/path -> (host, /path)
    - SCP-style: user@host:/path -> (host, /path)
    - S3, GCS, etc: s3://bucket/path -> ('s3', None) - not local
    
    Args:
        url: Remote URL
        
    Returns:
        Tuple of (host, path). host is None for local paths,
        path is None for cloud storage (not locally accessible).
    """
    import re
    
    url = url.strip()
    
    # Already a local path
    if url.startswith('/'):
        return (None, url)
    
    # file:// URL
    if url.startswith('file://'):
        return (None, url[7:])
    
    # SSH URL format: ssh://[user@]host/path
    ssh_match = re.match(r'ssh://(?:[^@]+@)?([^/]+)(/.*)', url)
    if ssh_match:
        return (ssh_match.group(1), ssh_match.group(2))
    
    # SCP-style: [user@]host:/path
    scp_match = re.match(r'(?:[^@]+@)?([^:]+):(/.+)', url)
    if scp_match:
        return (scp_match.group(1), scp_match.group(2))
    
    # Cloud storage (s3://, gs://, https://, etc.) - not locally accessible
    return (url.split('://')[0] if '://' in url else None, None)


def get_local_hosts() -> List[str]:
    """Get list of hostnames that should be considered 'local'.
    
    Returns current hostname plus any configured SSH host.
    
    Returns:
        List of hostnames considered local.
    """
    import socket
    import signal
    
    hosts = []
    
    # Current hostname (short and FQDN)
    hostname = socket.gethostname()
    hosts.append(hostname)
    
    # Try to get FQDN with timeout (can hang on nodes without DNS)
    def _timeout_handler(signum, frame):
        raise TimeoutError("FQDN lookup timed out")
    
    try:
        # Set a 2-second timeout for FQDN lookup
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(2)
        try:
            fqdn = socket.getfqdn()
            if fqdn and fqdn != hostname:
                hosts.append(fqdn)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    except (TimeoutError, Exception):
        # FQDN lookup failed or timed out - continue without it
        pass
    
    # Configured SSH host (used for remote URLs)
    ssh_host = cfg.get_value('ssh.host')
    if ssh_host:
        hosts.append(ssh_host)
    
    return hosts


def _get_domain(hostname: str) -> Optional[str]:
    """Extract the domain from a hostname.
    
    Args:
        hostname: A hostname like 'gadi-dm-0001.nci.org.au'
        
    Returns:
        The domain suffix (e.g., 'nci.org.au') or None if no domain.
    """
    parts = hostname.split('.')
    if len(parts) >= 3:
        # Return last 2-3 parts as domain (e.g., 'nci.org.au')
        return '.'.join(parts[-3:]) if len(parts) >= 3 else '.'.join(parts[-2:])
    elif len(parts) == 2:
        return '.'.join(parts)
    return None


def is_local_host(host: str) -> bool:
    """Check if a hostname should be considered 'local'.
    
    A host is considered local if:
    - It matches the current hostname exactly
    - It shares the same domain suffix (e.g., both end with '.nci.org.au')
    - It matches the configured ssh.host
    
    Args:
        host: Hostname to check
        
    Returns:
        True if the host is considered local.
    """
    if not host:
        return False
    
    local_hosts = get_local_hosts()
    
    # Check for exact match
    if host in local_hosts:
        return True
    
    # Check for same short hostname (e.g., 'gadi-dm' matches 'gadi-dm.nci.org.au')
    host_short = host.split('.')[0]
    for local in local_hosts:
        if host_short == local.split('.')[0]:
            return True
    
    # Check for same domain suffix
    # If remote is 'gadi-dm.nci.org.au' and we're on 'gadi-dm-0001.nci.org.au',
    # both share the domain 'nci.org.au' so consider it local
    host_domain = _get_domain(host)
    if host_domain:
        for local in local_hosts:
            local_domain = _get_domain(local)
            if local_domain and host_domain == local_domain:
                return True
    
    return False


def extract_local_path(url: str, check_host: bool = True) -> Optional[str]:
    """Extract the local filesystem path from a remote URL.
    
    Handles:
    - Local paths: /path/to/remote -> /path/to/remote
    - file:// URLs: file:///path -> /path
    - SSH URLs: ssh://host/path -> /path (if host is local)
    - SCP-style: user@host:/path -> /path (if host is local)
    
    Args:
        url: Remote URL
        check_host: If True (default), verify SSH hosts are local
        
    Returns:
        Local path if extractable, None otherwise
    """
    host, path = parse_remote_url(url)
    
    if path is None:
        # Cloud storage or unparseable
        return None
    
    if host is None:
        # Already a local path
        return path
    
    # SSH remote - check if host is local
    if check_host and not is_local_host(host):
        return None
    
    return path


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


def check_remote_access(
    remotes: List[Tuple[str, str, bool]],
) -> Tuple[Optional[Tuple[str, str]], Optional[str]]:
    """Check remote access and return detailed error if not accessible.
    
    Similar to find_local_remote but provides detailed error messages
    when remotes look like they should be local but aren't accessible.
    
    Args:
        remotes: List of (name, url, is_default) tuples
        
    Returns:
        Tuple of:
        - (remote_name, local_path) if found and accessible, None otherwise
        - Error message if remote looks local but not accessible, None otherwise
    """
    local_but_inaccessible = []
    
    for name, url, is_default in remotes:
        local_path = extract_local_path(url)
        if local_path:
            if Path(local_path).exists():
                return ((name, local_path), None)
            else:
                local_but_inaccessible.append((name, local_path, url))
    
    if local_but_inaccessible:
        # Remote looks local but path doesn't exist
        name, path, url = local_but_inaccessible[0]
        return (None, f"Remote '{name}' path not accessible: {path} (from {url})")
    
    return (None, None)


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


def check_remote_access_from_repo(
    repo_spec: str,
    owner: Optional[str] = None,
) -> Tuple[Optional[Tuple[str, str]], Optional[str]]:
    """Check remote access for a remote repository with detailed error messages.
    
    Like find_local_remote_from_repo, but provides detailed error messages
    when remotes look like they should be local but aren't accessible
    (e.g., when a volume is not mounted).
    
    Args:
        repo_spec: Repository URL or short name
        owner: Optional owner for short names
        
    Returns:
        Tuple of:
        - (remote_name, local_path) if found and accessible, None otherwise
        - Error message if remote looks local but not accessible, None otherwise
    """
    remotes = list_remotes_from_repo(repo_spec, owner=owner)
    return check_remote_access(remotes)
