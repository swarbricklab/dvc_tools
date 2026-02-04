"""Offline mode support - redirect Git URLs to local temp clones.

Uses Git's url.<base>.insteadOf configuration to redirect remote
repository URLs to local temporary clones, enabling DVC operations
on compute nodes without internet access.

Also handles DVC remotes by adding local path overrides for SSH
remotes in .dvc/config.local.

Offline state is tracked in .dt/config.local.yaml under the 'offline' key.
"""

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from . import tmp as tmp_mod
from . import remote as remote_mod
from .errors import OfflineError


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
        Path to .dt/tmp/clones directory.
    """
    return get_dt_root() / 'tmp' / 'clones'


def get_local_config_path() -> Path:
    """Get path to .dt/config.local.yaml."""
    return get_dt_root() / 'config.local.yaml'


def load_offline_state() -> Dict[str, Any]:
    """Load offline state from .dt/config.local.yaml.
    
    Returns:
        Dictionary with offline state:
        - git_redirects: List of repo_ids with active redirects
        - remote_overrides: List of dicts with remote_name and original_url
    """
    config_path = get_local_config_path()
    
    if not config_path.exists():
        return {'git_redirects': [], 'remote_overrides': []}
    
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
    except Exception:
        return {'git_redirects': [], 'remote_overrides': []}
    
    offline = config.get('offline', {})
    return {
        'git_redirects': offline.get('git_redirects', []),
        'remote_overrides': offline.get('remote_overrides', []),
    }


def save_offline_state(state: Dict[str, Any]) -> None:
    """Save offline state to .dt/config.local.yaml.
    
    Args:
        state: Dictionary with git_redirects and remote_overrides.
    """
    config_path = get_local_config_path()
    
    # Load existing config
    if config_path.exists():
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f) or {}
        except Exception:
            config = {}
    else:
        config = {}
    
    # Update offline section
    config['offline'] = {
        'git_redirects': state.get('git_redirects', []),
        'remote_overrides': state.get('remote_overrides', []),
    }
    
    # Write back
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def clear_offline_state() -> None:
    """Clear offline state from .dt/config.local.yaml."""
    save_offline_state({'git_redirects': [], 'remote_overrides': []})


def list_temp_clones() -> List[Tuple[str, Path]]:
    """List all temporary clones and their local paths.
    
    Returns:
        List of (repo_id, local_path) tuples.
        For example: ('github.com/org/repo', Path('.dt/tmp/clones/github.com/org/repo'))
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


def enable(verbose: bool = False) -> Tuple[List[str], List[str]]:
    """Enable offline mode by setting up URL redirects and remote overrides.
    
    Sets git config to redirect remote URLs to local temp clones,
    and DVC config to use local paths for SSH remotes.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        Tuple of (enabled_repos, enabled_remotes).
        
    Raises:
        OfflineError: If no temp clones or SSH remotes exist.
    """
    clones = list_temp_clones()
    ssh_remotes = get_ssh_remotes()
    
    if not clones and not ssh_remotes:
        raise OfflineError(
            "No temporary clones or SSH remotes found.\n"
            "Use 'dt tmp clone <repo>' to clone a repository first."
        )
    
    enabled_repos = []
    
    # Enable git URL redirects for temp clones
    for repo_id, local_path in clones:
        urls = repo_id_to_urls(repo_id)
        
        if verbose:
            print(f"Enabling Git redirect for {repo_id}")
        
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
        
        enabled_repos.append(repo_id)
    
    # Enable DVC remote overrides and track original URLs
    enabled_remotes = []
    remote_overrides = []
    
    for name, ssh_url, local_path in ssh_remotes:
        if verbose:
            print(f"Enabling local override for remote '{name}'")
            print(f"  {ssh_url} -> {local_path}")
        
        result = subprocess.run(
            ['dvc', 'config', '--local', f'remote.{name}.url', local_path],
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            if verbose:
                print(f"  Warning: {result.stderr}")
        else:
            enabled_remotes.append(name)
            remote_overrides.append({
                'remote_name': name,
                'original_url': ssh_url,
            })
    
    # Save state to .dt/config.local.yaml
    save_offline_state({
        'git_redirects': enabled_repos,
        'remote_overrides': remote_overrides,
    })
    
    return enabled_repos, enabled_remotes


def disable(verbose: bool = False) -> Tuple[List[str], List[str]]:
    """Disable offline mode by removing URL redirects and remote overrides.
    
    Reads saved state from .dt/config.local.yaml to know exactly which
    redirects and overrides to remove.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        Tuple of (disabled_repos, disabled_remotes).
    """
    # Load saved state
    state = load_offline_state()
    git_redirects = state.get('git_redirects', [])
    remote_overrides = state.get('remote_overrides', [])
    
    tmp_dir = get_tmp_dir()
    disabled_repos = []
    
    # Remove git redirects from saved state
    for repo_id in git_redirects:
        # Find the temp clone path for this repo_id
        clone_path = tmp_dir / repo_id
        
        if verbose:
            print(f"Disabling Git redirect for {repo_id}")
        
        # Remove all insteadOf entries for this path
        result = subprocess.run(
            ['git', 'config', '--local', '--unset-all',
             get_config_key(clone_path)],
            capture_output=True,
            text=True,
        )
        
        # --unset-all returns 5 if key doesn't exist, which is fine
        if result.returncode not in (0, 5):
            if verbose:
                print(f"  Warning: {result.stderr}")
        else:
            disabled_repos.append(repo_id)
    
    # Remove DVC remote overrides from saved state
    disabled_remotes = []
    for override in remote_overrides:
        name = override.get('remote_name')
        if not name:
            continue
        
        if verbose:
            print(f"Removing local override for remote '{name}'")
        
        result = subprocess.run(
            ['dvc', 'config', '--local', '--unset', f'remote.{name}.url'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            if verbose:
                print(f"  Warning: {result.stderr}")
        else:
            disabled_remotes.append(name)
    
    # Clear saved state
    clear_offline_state()
    
    return disabled_repos, disabled_remotes


def status() -> Dict[str, dict]:
    """Get the status of offline mode.
    
    Uses saved state from .dt/config.local.yaml for accurate tracking
    of what's currently enabled.
    
    Returns:
        Dictionary with status information:
        - 'enabled': bool - Whether offline mode is active
        - 'clones': List of available temp clones
        - 'active': List of active git redirects (repo_ids)
        - 'missing': List of clones without active redirects
        - 'remotes': DVC remote status
    """
    # Load saved state
    state = load_offline_state()
    active_redirects = state.get('git_redirects', [])
    active_overrides = state.get('remote_overrides', [])
    
    # Get available clones
    clones = list_temp_clones()
    clone_ids = [repo_id for repo_id, _ in clones]
    
    # Find clones without redirects
    active_set = set(active_redirects)
    missing = [repo_id for repo_id in clone_ids if repo_id not in active_set]
    
    # Get available SSH remotes
    ssh_remotes = get_ssh_remotes()
    available_remotes = [name for name, _, _ in ssh_remotes]
    active_remote_names = [o.get('remote_name') for o in active_overrides if o.get('remote_name')]
    
    remote_status = {
        'available': available_remotes,
        'active': active_remote_names,
    }
    
    return {
        'enabled': len(active_redirects) > 0 or len(active_remote_names) > 0,
        'clones': clone_ids,
        'active': active_redirects,
        'missing': missing,
        'remotes': remote_status,
    }


# =============================================================================
# DVC Remote Override Functions
# =============================================================================


def get_ssh_remotes() -> List[Tuple[str, str, str]]:
    """Get SSH-based DVC remotes that can be converted to local paths.
    
    Reads from .dvc/config directly (not config.local) to find the
    original SSH URLs even when overrides are active.
    
    Returns:
        List of (remote_name, ssh_url, local_path) tuples for remotes
        whose local paths exist on the filesystem.
    """
    import configparser
    
    dvc_config = Path.cwd() / '.dvc' / 'config'
    if not dvc_config.exists():
        return []
    
    # Parse the DVC config file
    config = configparser.ConfigParser()
    try:
        config.read(dvc_config)
    except Exception:
        return []
    
    ssh_remotes = []
    
    for section in config.sections():
        # DVC sections look like: 'remote "name"' (including outer quotes sometimes)
        # Handle both: 'remote "name"' and "'remote \"name\"'"
        section_clean = section.strip("'")
        if section_clean.startswith('remote "') and section_clean.endswith('"'):
            remote_name = section_clean[8:-1]  # Extract name from 'remote "name"'
            url = config.get(section, 'url', fallback=None)
            
            if not url:
                continue
            
            # Parse the URL to get host and path
            host, path = remote_mod.parse_remote_url(url)
            
            # Only interested in SSH remotes (have a host and path)
            if host is None or path is None:
                continue
            
            # Check if the path exists locally
            if Path(path).exists():
                ssh_remotes.append((remote_name, url, path))
    
    return ssh_remotes


def get_remote_overrides() -> Dict[str, str]:
    """Get current DVC remote overrides from .dvc/config.local.
    
    Looks for remotes we've overridden with local paths.
    
    Returns:
        Dictionary mapping remote names to their override URLs.
    """
    result = subprocess.run(
        ['dvc', 'config', '--local', '--list'],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        return {}
    
    overrides = {}
    
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        # Format: remote.name.url=/path/to/local
        if line.startswith('remote.') and '.url=' in line:
            # Parse: remote.gadi.url=/g/data/...
            parts = line.split('=', 1)
            if len(parts) == 2:
                key, value = parts
                # Extract remote name: remote.gadi.url -> gadi
                key_parts = key.split('.')
                if len(key_parts) == 3 and key_parts[2] == 'url':
                    remote_name = key_parts[1]
                    # Only track if it's a local path (our override)
                    if value.startswith('/'):
                        overrides[remote_name] = value
    
    return overrides


def enable_remote_overrides(verbose: bool = False) -> List[str]:
    """Enable local path overrides for SSH-based DVC remotes.
    
    For each SSH remote whose path exists locally, adds an override
    to .dvc/config.local to use the local path directly.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        List of remote names that were enabled.
    """
    ssh_remotes = get_ssh_remotes()
    
    if not ssh_remotes:
        return []
    
    enabled = []
    
    for name, ssh_url, local_path in ssh_remotes:
        if verbose:
            print(f"Enabling local override for remote '{name}'")
            print(f"  {ssh_url} -> {local_path}")
        
        result = subprocess.run(
            ['dvc', 'config', '--local', f'remote.{name}.url', local_path],
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            if verbose:
                print(f"  Warning: {result.stderr}")
        else:
            enabled.append(name)
    
    return enabled


def disable_remote_overrides(verbose: bool = False) -> List[str]:
    """Disable local path overrides for DVC remotes.
    
    Removes overrides from .dvc/config.local that point to local paths.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        List of remote names that were disabled.
    """
    overrides = get_remote_overrides()
    
    if not overrides:
        return []
    
    disabled = []
    
    for name, local_path in overrides.items():
        if verbose:
            print(f"Disabling local override for remote '{name}'")
        
        result = subprocess.run(
            ['dvc', 'config', '--local', '--unset', f'remote.{name}.url'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            if verbose:
                print(f"  Warning: {result.stderr}")
        else:
            disabled.append(name)
    
    return disabled


def get_remote_override_status() -> Dict[str, list]:
    """Get status of DVC remote overrides.
    
    Returns:
        Dictionary with:
        - 'available': List of SSH remotes that can be overridden
        - 'active': List of SSH remotes with active local overrides
    """
    ssh_remotes = get_ssh_remotes()
    overrides = get_remote_overrides()
    
    available = [name for name, _, _ in ssh_remotes]
    
    # Only count as active if it's an SSH remote that we've overridden
    # (not just any local path in config.local)
    ssh_remote_names = set(available)
    active = [name for name in overrides.keys() if name in ssh_remote_names]
    
    return {
        'available': available,
        'active': active,
    }
