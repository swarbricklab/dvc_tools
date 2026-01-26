"""Offline mode support - redirect Git URLs to local temp clones.

Uses Git's url.<base>.insteadOf configuration to redirect remote
repository URLs to local temporary clones, enabling DVC operations
on compute nodes without internet access.
"""

import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import tmp as tmp_mod


class OfflineError(Exception):
    """Raised when offline operations fail."""
    pass


def get_dt_root() -> Path:
    """Get the .dt directory root.
    
    Returns:
        Path to .dt directory.
        
    Raises:
        OfflineError: If not in a dt-initialized directory.
    """
    cwd = Path.cwd()
    dt_dir = cwd / '.dt'
    if not dt_dir.exists():
        raise OfflineError(
            "Not in a dt-initialized directory. Run 'dt init' first."
        )
    return dt_dir


def get_tmp_dir() -> Path:
    """Get the temporary clones directory.
    
    Returns:
        Path to .dt/tmp directory.
    """
    return get_dt_root() / 'tmp'


def list_temp_clones() -> List[Tuple[str, Path]]:
    """List all temporary clones and their local paths.
    
    Returns:
        List of (repo_id, local_path) tuples.
        For example: ('github.com/org/repo', Path('.dt/tmp/github.com/org/repo'))
    """
    tmp_dir = get_tmp_dir()
    if not tmp_dir.exists():
        return []
    
    clones = []
    
    # Look for repos: .dt/tmp/<host>/<owner>/<repo>
    for host_dir in tmp_dir.iterdir():
        if not host_dir.is_dir():
            continue
        for owner_dir in host_dir.iterdir():
            if not owner_dir.is_dir():
                continue
            for repo_dir in owner_dir.iterdir():
                if not repo_dir.is_dir():
                    continue
                # Check if it's a git repo
                if (repo_dir / '.git').exists() or (repo_dir / 'HEAD').exists():
                    repo_id = f"{host_dir.name}/{owner_dir.name}/{repo_dir.name}"
                    clones.append((repo_id, repo_dir.resolve()))
    
    return sorted(clones)


def repo_id_to_urls(repo_id: str) -> List[str]:
    """Convert a repo_id to possible Git URLs.
    
    Args:
        repo_id: Repository identifier like 'github.com/org/repo'
        
    Returns:
        List of URL patterns that should be redirected.
    """
    # Parse: host/owner/repo
    parts = repo_id.split('/')
    if len(parts) != 3:
        return []
    
    host, owner, repo = parts
    
    urls = []
    if 'github.com' in host:
        urls.extend([
            f"git@github.com:{owner}/{repo}.git",
            f"git@github.com:{owner}/{repo}",
            f"https://github.com/{owner}/{repo}.git",
            f"https://github.com/{owner}/{repo}",
        ])
    elif 'gitlab.com' in host:
        urls.extend([
            f"git@gitlab.com:{owner}/{repo}.git",
            f"git@gitlab.com:{owner}/{repo}",
            f"https://gitlab.com/{owner}/{repo}.git",
            f"https://gitlab.com/{owner}/{repo}",
        ])
    else:
        # Generic host
        urls.extend([
            f"git@{host}:{owner}/{repo}.git",
            f"git@{host}:{owner}/{repo}",
            f"https://{host}/{owner}/{repo}.git",
            f"https://{host}/{owner}/{repo}",
        ])
    
    return urls


def get_config_key(local_path: Path) -> str:
    """Get the git config key for a local path.
    
    Args:
        local_path: Path to the local clone.
        
    Returns:
        Git config key like 'url./path/to/clone.insteadOf'
    """
    return f"url.{local_path}.insteadOf"


def get_current_redirects() -> Dict[str, List[str]]:
    """Get current URL redirects from git config.
    
    Returns:
        Dictionary mapping local paths to lists of redirected URLs.
    """
    result = subprocess.run(
        ['git', 'config', '--local', '--get-regexp', r'^url\..*\.insteadOf$'],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        return {}
    
    redirects: Dict[str, List[str]] = {}
    
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        # Format: url./path/to/clone.insteadof git@github.com:org/repo.git
        # Note: git config outputs lowercase 'insteadof'
        parts = line.split(' ', 1)
        if len(parts) != 2:
            continue
        
        key, url = parts
        # Extract path from key: url./path/to/clone.insteadof -> /path/to/clone
        key_lower = key.lower()
        if key_lower.startswith('url.') and key_lower.endswith('.insteadof'):
            local_path = key[4:-10]  # Remove 'url.' and '.insteadof'
            local_path = key[4:-10]  # Remove 'url.' and '.insteadOf'
            if local_path not in redirects:
                redirects[local_path] = []
            redirects[local_path].append(url)
    
    return redirects


def enable(verbose: bool = False) -> List[str]:
    """Enable offline mode by setting up URL redirects.
    
    Sets git config to redirect remote URLs to local temp clones.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        List of repo_ids that were enabled.
        
    Raises:
        OfflineError: If no temp clones exist.
    """
    clones = list_temp_clones()
    
    if not clones:
        raise OfflineError(
            "No temporary clones found. Use 'dt tmp clone <repo>' first."
        )
    
    enabled = []
    
    for repo_id, local_path in clones:
        urls = repo_id_to_urls(repo_id)
        
        if verbose:
            print(f"Enabling offline for {repo_id}")
        
        for url in urls:
            result = subprocess.run(
                ['git', 'config', '--local', '--add', 
                 get_config_key(local_path), url],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise OfflineError(
                    f"Failed to set git config: {result.stderr}"
                )
            
            if verbose:
                print(f"  {url} -> {local_path}")
        
        enabled.append(repo_id)
    
    return enabled


def disable(verbose: bool = False) -> List[str]:
    """Disable offline mode by removing URL redirects.
    
    Removes git config entries that redirect to temp clones.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        List of local paths that were disabled.
    """
    redirects = get_current_redirects()
    
    if not redirects:
        return []
    
    tmp_dir = get_tmp_dir()
    disabled = []
    
    for local_path, urls in redirects.items():
        # Only remove redirects pointing to our .dt/tmp directory
        if str(tmp_dir) not in local_path:
            continue
        
        if verbose:
            print(f"Disabling offline for {local_path}")
        
        # Remove all insteadOf entries for this path
        result = subprocess.run(
            ['git', 'config', '--local', '--unset-all',
             get_config_key(Path(local_path))],
            capture_output=True,
            text=True,
        )
        
        # --unset-all returns 5 if key doesn't exist, which is fine
        if result.returncode not in (0, 5):
            if verbose:
                print(f"  Warning: {result.stderr}")
        
        disabled.append(local_path)
    
    return disabled


def status() -> Dict[str, dict]:
    """Get the status of offline mode.
    
    Returns:
        Dictionary with status information:
        - 'enabled': bool - Whether any redirects are active
        - 'clones': List of available temp clones
        - 'active': List of active redirects
        - 'missing': List of clones without active redirects
    """
    clones = list_temp_clones()
    redirects = get_current_redirects()
    
    # Find which clones have active redirects
    active = []
    clone_paths = {str(path): repo_id for repo_id, path in clones}
    
    for local_path in redirects:
        if local_path in clone_paths:
            active.append(clone_paths[local_path])
    
    # Find clones without redirects
    active_set = set(active)
    missing = [repo_id for repo_id, _ in clones if repo_id not in active_set]
    
    return {
        'enabled': len(active) > 0,
        'clones': [repo_id for repo_id, _ in clones],
        'active': active,
        'missing': missing,
    }
