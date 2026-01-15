"""Configuration management for DVC Tools.

Handles hierarchical configuration with four scopes:
- system: XDG_CONFIG_DIRS/dt/config.yaml (team defaults)
- user: ~/.config/dt/config.yaml (personal settings)
- project: .dt/config.yaml (repo-specific, tracked)
- local: .dt/config.local.yaml (workspace-specific, not tracked)

Configuration is loaded in precedence order: local > project > user > system
"""

import os
from pathlib import Path
from typing import Any, Optional, List

import confuse
import yaml


APP_NAME = 'dt'

# Scope names in precedence order (highest first)
SCOPES = ['local', 'project', 'user', 'system']


def get_config_paths() -> dict[str, Path]:
    """Get the configuration file paths for each scope.
    
    Returns:
        Dict mapping scope name to config file path
    """
    paths = {}
    
    # System: search XDG_CONFIG_DIRS
    xdg_config_dirs = os.environ.get('XDG_CONFIG_DIRS', '/etc/xdg')
    for config_dir in xdg_config_dirs.split(':'):
        system_path = Path(config_dir) / APP_NAME / 'config.yaml'
        if system_path.exists():
            paths['system'] = system_path
            break
    else:
        # Use first dir as default location even if file doesn't exist
        first_dir = xdg_config_dirs.split(':')[0]
        paths['system'] = Path(first_dir) / APP_NAME / 'config.yaml'
    
    # User: XDG_CONFIG_HOME or ~/.config
    xdg_config_home = os.environ.get('XDG_CONFIG_HOME', '')
    if xdg_config_home:
        paths['user'] = Path(xdg_config_home) / APP_NAME / 'config.yaml'
    else:
        paths['user'] = Path.home() / '.config' / APP_NAME / 'config.yaml'
    
    # Project and local: relative to git root or cwd
    project_root = find_project_root()
    paths['project'] = project_root / '.dt' / 'config.yaml'
    paths['local'] = project_root / '.dt' / 'config.local.yaml'
    
    return paths


def find_project_root() -> Path:
    """Find the project root by looking for .git directory.
    
    Returns:
        Path to project root, or cwd if not in a git repo
    """
    cwd = Path.cwd()
    for parent in [cwd] + list(cwd.parents):
        if (parent / '.git').exists():
            return parent
    return cwd


def load_config() -> confuse.Configuration:
    """Load configuration from all scopes in precedence order.
    
    Returns:
        Merged configuration object
    """
    config = confuse.Configuration(APP_NAME, read=False)
    paths = get_config_paths()
    
    # Load in reverse precedence order (lowest first, so higher overrides)
    for scope in reversed(SCOPES):
        path = paths.get(scope)
        if path and path.exists():
            config.set_file(path)
    
    return config


def load_scope_config(scope: str) -> dict:
    """Load configuration from a single scope.
    
    Args:
        scope: One of 'local', 'project', 'user', 'system'
        
    Returns:
        Dict of configuration values, or empty dict if file doesn't exist
    """
    paths = get_config_paths()
    path = paths.get(scope)
    
    if path and path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


def get_value(key: str, default: Any = None) -> Any:
    """Get a configuration value by dot-separated key.
    
    Args:
        key: Dot-separated key path (e.g., 'cache.root')
        default: Default value if key not found
        
    Returns:
        Configuration value or default
    """
    config = load_config()
    
    try:
        # Navigate nested keys
        view = config
        for part in key.split('.'):
            view = view[part]
        return view.get()
    except (confuse.NotFoundError, confuse.ConfigError):
        return default


def set_value(key: str, value: str, scope: str = 'user') -> None:
    """Set a configuration value at the specified scope.
    
    Args:
        key: Dot-separated key path (e.g., 'cache.root')
        value: Value to set (will be parsed as YAML)
        scope: One of 'local', 'project', 'user', 'system'
    """
    paths = get_config_paths()
    path = paths[scope]
    
    # Load existing config or start fresh
    if path.exists():
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}
    
    # Parse value as YAML to handle types correctly
    try:
        parsed_value = yaml.safe_load(value)
    except yaml.YAMLError:
        parsed_value = value
    
    # Navigate/create nested structure
    parts = key.split('.')
    current = data
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = parsed_value
    
    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write back
    with open(path, 'w') as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)


def unset_value(key: str, scope: str = 'user') -> bool:
    """Remove a configuration value at the specified scope.
    
    Args:
        key: Dot-separated key path (e.g., 'cache.root')
        scope: One of 'local', 'project', 'user', 'system'
        
    Returns:
        True if key was found and removed, False otherwise
    """
    paths = get_config_paths()
    path = paths[scope]
    
    if not path.exists():
        return False
    
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    
    # Navigate to parent of target key
    parts = key.split('.')
    current = data
    parents = [(data, None)]  # Track parents for cleanup
    
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            return False
        parents.append((current, part))
        current = current[part]
    
    # Remove the key
    final_key = parts[-1]
    if final_key not in current:
        return False
    
    del current[final_key]
    
    # Clean up empty parent dicts
    for parent, key_in_parent in reversed(parents[1:]):
        if key_in_parent and not parent[key_in_parent]:
            del parent[key_in_parent]
    
    # Write back
    with open(path, 'w') as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    
    return True


def flatten_dict(d: dict, parent_key: str = '', sep: str = '.') -> dict:
    """Flatten a nested dict into dot-separated keys.
    
    Args:
        d: Nested dictionary
        parent_key: Prefix for keys
        sep: Separator between key parts
        
    Returns:
        Flat dictionary with dot-separated keys
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def list_config(scope: Optional[str] = None) -> dict:
    """List configuration values.
    
    Args:
        scope: If provided, list only that scope. Otherwise list merged config.
        
    Returns:
        Flat dictionary of configuration values
    """
    if scope:
        data = load_scope_config(scope)
    else:
        config = load_config()
        try:
            data = config.flatten()
        except confuse.ConfigError:
            data = {}
    
    return flatten_dict(data) if isinstance(data, dict) else {}


def list_config_with_sources() -> List[tuple]:
    """List all config values with their source scope.
    
    Returns:
        List of (key, value, scope) tuples
    """
    results = {}
    
    # Load each scope, tracking where values come from
    # Go in precedence order so higher scopes override
    for scope in reversed(SCOPES):
        scope_data = load_scope_config(scope)
        flat = flatten_dict(scope_data)
        for key, value in flat.items():
            results[key] = (key, value, scope)
    
    return sorted(results.values())
