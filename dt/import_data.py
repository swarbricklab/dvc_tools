"""Import DVC-tracked data from remote repositories.

Enables importing files and directories from other DVC projects
without using `dvc import` (which requires network access to remotes).

Instead, this uses locally-accessible cache paths discovered via
`dt cache add-from`.
"""

import json
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

import yaml

from . import config as cfg
from . import checkout as checkout_mod
from . import remote as remote_mod
from . import tmp as tmp_mod
from . import utils


class ImportError(Exception):
    """Raised when import operations fail."""
    pass


def get_dvc_file_path(repo_path: Path, data_path: str) -> Optional[Path]:
    """Find the .dvc file for a given data path.
    
    Handles both file.txt -> file.txt.dvc and directory -> directory.dvc
    
    Args:
        repo_path: Path to the repository root.
        data_path: Path to the data file/directory (relative to repo).
        
    Returns:
        Path to the .dvc file, or None if not found.
    """
    data_path = data_path.rstrip('/')
    
    # Try path.dvc (for directories)
    dvc_path = repo_path / f"{data_path}.dvc"
    if dvc_path.exists():
        return dvc_path
    
    # Try path (if already ends in .dvc)
    if data_path.endswith('.dvc'):
        dvc_path = repo_path / data_path
        if dvc_path.exists():
            return dvc_path
    
    return None


def read_dvc_file(dvc_path: Path) -> Dict[str, Any]:
    """Read and parse a .dvc file.
    
    Args:
        dvc_path: Path to the .dvc file.
        
    Returns:
        Parsed YAML content as a dictionary.
        
    Raises:
        ImportError: If file cannot be read or parsed.
    """
    try:
        with open(dvc_path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        raise ImportError(f"Failed to read {dvc_path}: {e}")


def write_dvc_file(dvc_path: Path, content: Dict[str, Any]) -> None:
    """Write a .dvc file.
    
    Args:
        dvc_path: Path to write the .dvc file.
        content: Dictionary content to write as YAML.
        
    Raises:
        ImportError: If file cannot be written.
    """
    try:
        dvc_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dvc_path, 'w') as f:
            yaml.dump(content, f, default_flow_style=False, sort_keys=False)
    except Exception as e:
        raise ImportError(f"Failed to write {dvc_path}: {e}")


def ensure_cache_from_repo(repository: str, owner: Optional[str] = None) -> Optional[str]:
    """Ensure we have a cache configured from the given repository.
    
    Args:
        repository: Repository name, alias, or URL.
        owner: Optional owner override for short names.
        
    Returns:
        Path to the added cache, or None if no local cache found.
    """
    # Try to find and add a local remote from the repo
    result = remote_mod.find_local_remote_from_repo(
        repo_spec=repository,
        owner=owner,
    )
    
    if result:
        remote_name, cache_path = result
        # Add to alt caches if not already there
        cfg.add_list_value('cache.alt', cache_path, 'local')
        return cache_path
    
    return None


def import_from_dvc_file(
    source_dvc_path: Path,
    dest_path: Path,
    dest_name: Optional[str] = None,
    verbose: bool = False,
) -> Path:
    """Import data by copying a .dvc file and checking out.
    
    Args:
        source_dvc_path: Path to the source .dvc file.
        dest_path: Destination directory for the imported data.
        dest_name: Optional name override (default: use source name).
        verbose: Print progress messages.
        
    Returns:
        Path to the created .dvc file.
        
    Raises:
        ImportError: If import fails.
    """
    # Read the source .dvc file
    dvc_content = read_dvc_file(source_dvc_path)
    
    if 'outs' not in dvc_content or not dvc_content['outs']:
        raise ImportError(f"No outputs defined in {source_dvc_path}")
    
    # Get the output info
    out = dvc_content['outs'][0]
    original_path = out.get('path', '')
    
    # Determine destination name
    if dest_name:
        out['path'] = dest_name
    
    # Create the destination .dvc file
    final_name = out['path']
    if final_name.endswith('.dvc'):
        dest_dvc_path = dest_path / final_name
    else:
        dest_dvc_path = dest_path / f"{final_name}.dvc"
    
    if verbose:
        print(f"Creating {dest_dvc_path}")
    
    write_dvc_file(dest_dvc_path, dvc_content)
    
    return dest_dvc_path


def import_data(
    repository: str,
    path: str,
    dest: Optional[str] = None,
    name: Optional[str] = None,
    owner: Optional[str] = None,
    checkout: bool = True,
    verbose: bool = False,
) -> Tuple[Path, Optional[str]]:
    """Import DVC-tracked data from a remote repository.
    
    Args:
        repository: Repository name, alias, or URL.
        path: Path to the file/directory in the remote repo.
        dest: Destination directory (default: current directory).
        name: Override name for imported data.
        owner: Optional owner override for short names.
        checkout: Whether to run checkout after import.
        verbose: Print progress messages.
        
    Returns:
        Tuple of (dvc_file_path, cache_path used).
        
    Raises:
        ImportError: If import fails.
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise ImportError(str(e))
    
    dest_path = Path(dest) if dest else Path.cwd()
    
    # Step 1: Ensure we have a sparse clone of the repo
    if verbose:
        print(f"Ensuring clone of {repository}...")
    
    try:
        clone_path = tmp_mod.clone_repo(repository, owner=owner, refresh=True)
    except tmp_mod.TmpError as e:
        raise ImportError(f"Failed to clone repository: {e}")
    
    if verbose:
        print(f"Using clone at {clone_path}")
    
    # Step 2: Find the .dvc file in the clone
    dvc_file = get_dvc_file_path(clone_path, path)
    
    if not dvc_file:
        raise ImportError(
            f"Could not find .dvc file for '{path}' in {repository}\n"
            f"Tried: {path}.dvc"
        )
    
    if verbose:
        print(f"Found {dvc_file.relative_to(clone_path)}")
    
    # Step 3: Ensure we have a cache from this repo
    if verbose:
        print(f"Looking for local cache from {repository}...")
    
    cache_path = ensure_cache_from_repo(repository, owner=owner)
    
    if cache_path:
        if verbose:
            print(f"Using cache: {cache_path}")
    else:
        if verbose:
            print("Warning: No local cache found. Checkout may fail.")
    
    # Step 4: Copy the .dvc file to destination
    created_dvc = import_from_dvc_file(
        source_dvc_path=dvc_file,
        dest_path=dest_path,
        dest_name=name,
        verbose=verbose,
    )
    
    # Step 5: Checkout if requested
    if checkout:
        if verbose:
            print(f"Checking out {created_dvc}...")
        
        results = checkout_mod.checkout(
            targets=[str(created_dvc)],
            verbose=verbose,
        )
        
        # Check if any cache succeeded
        any_success = any(success for _, success, _ in results)
        if not any_success:
            print(f"Warning: Checkout failed. Run 'dt checkout {created_dvc}' after configuring caches.")
    
    return created_dvc, cache_path
