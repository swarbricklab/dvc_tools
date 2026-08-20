"""Configuration management for DVC Tools.

Handles hierarchical configuration with four scopes:
- system: XDG_CONFIG_DIRS/dt/config.yaml (team defaults)
- user: ~/.config/dt/config.yaml (personal settings)
- project: .dt/config.yaml (repo-specific, tracked)
- local: .dt/config.local.yaml (workspace-specific, not tracked)

Configuration is loaded in precedence order: local > project > user > system

Writes default to *user* scope. Most settings (a cache root, an SSH host, a
GitHub owner) describe the person and the machine rather than the repository,
and a user-scope write applies to every repo instead of having to be repeated
in each one. Project scope is the sharper instrument -- it is tracked by git,
so a value written there is imposed on every collaborator -- and now has to be
asked for by name with ``--project``.
"""

import os
from pathlib import Path
from typing import Any, Optional, List

import confuse
import yaml

from . import utils


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
    project_root = utils.find_project_root()
    paths['project'] = project_root / '.dt' / 'config.yaml'
    paths['local'] = project_root / '.dt' / 'config.local.yaml'
    
    return paths


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


def scopes_defining(key: str) -> List[str]:
    """Return the scopes that set *key*, highest precedence first.

    Used to explain a write that will not take effect (a lower scope shadowed
    by a higher one) and a delete that found nothing (the key lives in another
    scope). Both are the confusing half of a scoped config: the command
    succeeds, and the effective value is not what was asked for.

    Args:
        key: Dot-separated key path (e.g. 'cache.root')

    Returns:
        Scope names in precedence order; empty if the key is set nowhere.
    """
    found = []
    for scope in SCOPES:
        data = load_scope_config(scope)
        current = data
        for part in key.split('.'):
            if not isinstance(current, dict) or part not in current:
                break
            current = current[part]
        else:
            found.append(scope)
    return found


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


def _assign(data: dict, key: str, value: Any) -> None:
    """Write *value* at the dot-separated *key* inside *data*, creating parents."""
    parts = key.split('.')
    current = data
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def set_value(key: str, value: str, scope: str = 'user') -> None:
    """Set a configuration value at the specified scope.

    Args:
        key: Dot-separated key path (e.g., 'cache.root')
        value: Value to set (will be parsed as YAML)
        scope: One of 'local', 'project', 'user', 'system'
    """
    # Parse value as YAML to handle types correctly
    try:
        parsed_value = yaml.safe_load(value)
    except yaml.YAMLError:
        parsed_value = value

    set_values({key: parsed_value}, scope)


def set_values(values: dict, scope: str = 'user') -> None:
    """Set several already-parsed values at *scope* in one rewrite.

    Args:
        values: Mapping of dot-separated key to value, stored as given (no
            YAML parsing -- callers that hold a string to parse use
            :func:`set_value`)
        scope: One of 'local', 'project', 'user', 'system'
    """
    paths = get_config_paths()
    path = paths[scope]

    # Load existing config or start fresh. Merging key by key rather than
    # replacing the file keeps whatever else the scope already holds.
    if path.exists():
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}

    for key, value in values.items():
        _assign(data, key, value)

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


#: Keys whose value may legitimately be a list.
#:
#: ``dt config add`` used to accept any key at all, which is how a scalar that
#: every consumer reads with ``get_value`` could quietly become a list. The
#: reader does not crash -- it hands back the list -- so the failure surfaces
#: much later and somewhere else entirely, as ``Path(['/a', '/b'])`` or
#: ``['/a'].strip()``. Worse, the config outlives the tool: a list written
#: today keeps breaking every already-released version, which cannot be
#: patched.
#:
#: Adding a key here is a promise that every consumer of it reads through
#: :func:`get_str_list` (or otherwise tolerates a list).
LIST_VALUED_KEYS = frozenset({
    'remote.root',
    'cache.root',
    'secrets.gcp.locations',
})


def get_str_list(key: str, default: Optional[List[str]] = None) -> List[str]:
    """Get a configuration value as a list of strings, honouring scope override.

    A key may hold either a single scalar or a list; both spellings are valid
    and a plain string keeps working unchanged. Blank entries are dropped and
    order is preserved, since for search paths the first entry carries meaning
    (it is the default used when creating something new).

    Scopes override rather than merge: the highest-precedence scope that
    defines the key supplies the whole list. Merging instead would make an
    inherited entry impossible to drop.

    Args:
        key: Dot-separated key path (e.g. 'remote.root')
        default: Value to return when the key is absent

    Returns:
        List of strings; empty if the key is unset.
    """
    raw = get_value(key)
    if raw is None:
        return list(default) if default else []
    if isinstance(raw, (list, tuple)):
        items = [str(v).strip() for v in raw]
    else:
        items = [str(raw).strip()]
    return [v for v in items if v]


def add_to_list(key: str, value: str, scope: str = 'user') -> bool:
    """Append a value to a list-valued key, promoting a scalar if needed.

    Scopes override rather than merge, so this reads the *effective* value and
    writes the whole list back into ``scope``. Otherwise adding one entry at
    user scope would silently discard the four inherited from system scope.

    Returns:
        True if added, False if it was already present.

    Raises:
        ValueError: If *key* is not in :data:`LIST_VALUED_KEYS`.
    """
    if key not in LIST_VALUED_KEYS:
        known = ', '.join(sorted(LIST_VALUED_KEYS))
        raise ValueError(
            f"{key!r} does not take a list of values.\n"
            f"Keys that do: {known}.\n"
            f"Making another key a list breaks every tool that reads it as a "
            f"single value -- including released versions, which cannot be "
            f"fixed after the fact. Use 'dt config set {key} <value>' instead."
        )
    current = get_str_list(key)
    if value in current:
        return False
    set_value(key, yaml.safe_dump(current + [value], default_flow_style=True,
                                  sort_keys=False).strip(), scope=scope)
    return True


def remove_from_list(key: str, value: str, scope: str = 'user') -> bool:
    """Remove a value from a list-valued key.

    Returns:
        True if removed, False if it was not present.
    """
    current = get_str_list(key)
    if value not in current:
        return False
    remaining = [v for v in current if v != value]
    if remaining:
        set_value(key, yaml.safe_dump(remaining, default_flow_style=True,
                                      sort_keys=False).strip(), scope=scope)
    else:
        unset_value(key, scope=scope)
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



def read_config_file(path) -> dict:
    """Read a config file handed over by someone else, flattened to dot keys.

    The file is an ordinary ``config.yaml``, so the same file works whether it
    is imported with ``dt config import`` or simply copied into place -- there
    is no separate format to learn or keep in step.

    Values are validated here rather than on the way in from the file, because
    the failure this guards against is silent: a list under a key every
    consumer reads as a scalar does not fail on import, it fails much later
    somewhere else, in this version and in every already-released one.

    Args:
        path: Path to a YAML file

    Returns:
        Flat mapping of dot-separated key to value, in file order.

    Raises:
        ValueError: If the file is missing, unreadable, not a YAML mapping,
            empty, or gives a list to a key that is not list-valued.
    """
    path = Path(path)
    try:
        text = path.read_text()
    except FileNotFoundError:
        raise ValueError(f"No such file: {path}")
    except OSError as e:
        raise ValueError(f"Cannot read {path}: {e}")

    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ValueError(f"{path} is not valid YAML: {e}")

    if data is None:
        raise ValueError(f"{path} is empty -- no settings to import.")
    if not isinstance(data, dict):
        raise ValueError(
            f"{path} must contain a mapping of settings, like a dt config "
            f"file:\n"
            f"  owner: myorg\n"
            f"  cache:\n"
            f"    root: /data/dvc_cache\n"
            f"Found {type(data).__name__} instead."
        )

    values = flatten_dict(data)
    if not values:
        raise ValueError(f"{path} contains no settings to import.")

    bad_lists = [
        k for k, v in values.items()
        if isinstance(v, (list, tuple)) and k not in LIST_VALUED_KEYS
    ]
    if bad_lists:
        known = ', '.join(sorted(LIST_VALUED_KEYS))
        raise ValueError(
            f"{path} gives a list to {', '.join(sorted(bad_lists))}, which "
            f"does not take one.\n"
            f"Keys that do: {known}.\n"
            f"Every other setting is read as a single value, so a list breaks "
            f"the tools that read it -- including released versions, which "
            f"cannot be fixed after the fact."
        )

    return values


def add_list_value(key: str, value: str, scope: str = 'local') -> bool:
    """Add a value to a list configuration key.
    
    Args:
        key: Dot-separated key path (e.g., 'cache.alt')
        value: Value to add to the list
        scope: One of 'local', 'project', 'user', 'system'
        
    Returns:
        True if added, False if already exists in that scope
    """
    paths = get_config_paths()
    path = paths[scope]
    
    # Load existing config or start fresh
    if path.exists():
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}
    
    # Navigate/create nested structure
    parts = key.split('.')
    current = data
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    
    # Get or create the list
    final_key = parts[-1]
    if final_key not in current:
        current[final_key] = []
    elif not isinstance(current[final_key], list):
        # Convert existing value to list
        current[final_key] = [current[final_key]]
    
    # Check if already in list
    if value in current[final_key]:
        return False
    
    current[final_key].append(value)
    
    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write back
    with open(path, 'w') as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    
    return True


def remove_list_value(key: str, value: str, scope: str = 'local') -> bool:
    """Remove a value from a list configuration key.
    
    Args:
        key: Dot-separated key path (e.g., 'cache.alt')
        value: Value to remove from the list
        scope: One of 'local', 'project', 'user', 'system'
        
    Returns:
        True if removed, False if not found
    """
    paths = get_config_paths()
    path = paths[scope]
    
    if not path.exists():
        return False
    
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    
    # Navigate to the key
    parts = key.split('.')
    current = data
    parents = [(data, None)]
    
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            return False
        parents.append((current, part))
        current = current[part]
    
    final_key = parts[-1]
    if final_key not in current or not isinstance(current[final_key], list):
        return False
    
    if value not in current[final_key]:
        return False
    
    current[final_key].remove(value)
    
    # Clean up empty list
    if not current[final_key]:
        del current[final_key]
        # Clean up empty parent dicts
        for parent, key_in_parent in reversed(parents[1:]):
            if key_in_parent and not parent[key_in_parent]:
                del parent[key_in_parent]
    
    # Write back
    with open(path, 'w') as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    
    return True
