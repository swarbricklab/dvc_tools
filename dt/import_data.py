"""Import DVC-tracked data from remote repositories.

Enables importing files and directories (or subsets) from other DVC projects
by creating our own .dvc and .dir files, then using dt checkout.

Unlike dvc import, this does not require network access to the remote storage.
Instead, it uses locally-accessible cache paths.
"""

import hashlib
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


def configure_clone_cache(clone_path: Path, cache_path: str) -> None:
    """Configure a clone to use a specific cache directory.
    
    Args:
        clone_path: Path to the cloned repository.
        cache_path: Path to the cache directory.
    """
    subprocess.run(
        ['dvc', 'cache', 'dir', '--local', cache_path],
        cwd=clone_path,
        capture_output=True,
        text=True,
    )


def list_files(
    clone_path: Path,
    path: str,
    recursive: bool = True,
) -> List[Dict[str, Any]]:
    """List files at a path in a DVC repository.
    
    Args:
        clone_path: Path to the cloned repository.
        path: Path to list (relative to repo root).
        recursive: Whether to list recursively.
        
    Returns:
        List of file info dicts with 'path', 'md5', 'isdir' keys.
        
    Raises:
        ImportError: If listing fails.
    """
    cmd = ['dvc', 'list', '--json', '--show-hash']
    if recursive:
        cmd.append('-R')
    cmd.extend(['.', path])
    
    result = subprocess.run(
        cmd,
        cwd=clone_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise ImportError(f"Failed to list {path}: {result.stderr}")
    
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise ImportError(f"Failed to parse dvc list output: {e}")


def get_file_size_from_cache(cache_path: str, md5: str) -> Optional[int]:
    """Get file size by looking at the cached file.
    
    Args:
        cache_path: Path to the cache directory.
        md5: MD5 hash of the file.
        
    Returns:
        File size in bytes, or None if not found.
    """
    # Handle .dir suffix
    hash_value = md5.replace('.dir', '')
    cache_file = Path(cache_path) / 'files' / 'md5' / hash_value[:2] / hash_value[2:]
    
    # Check for .dir file
    if md5.endswith('.dir'):
        cache_file = Path(str(cache_file) + '.dir')
    
    if cache_file.exists():
        return cache_file.stat().st_size
    
    return None


def compute_dir_hash(entries: List[Dict[str, str]]) -> str:
    """Compute the MD5 hash for a .dir file content.
    
    Args:
        entries: List of {'md5': ..., 'relpath': ...} dicts.
        
    Returns:
        MD5 hash of the JSON content.
    """
    # Sort by relpath for consistent ordering
    sorted_entries = sorted(entries, key=lambda x: x['relpath'])
    content = json.dumps(sorted_entries, separators=(',', ':'))
    return hashlib.md5(content.encode()).hexdigest()


def create_dir_file(
    entries: List[Dict[str, str]],
    cache_path: str,
) -> Tuple[str, int]:
    """Create a .dir file in the cache.
    
    Args:
        entries: List of {'md5': ..., 'relpath': ...} dicts.
        cache_path: Path to the cache directory.
        
    Returns:
        Tuple of (dir_hash, total_size).
        
    Raises:
        ImportError: If creation fails.
    """
    # Sort by relpath for consistent ordering
    sorted_entries = sorted(entries, key=lambda x: x['relpath'])
    content = json.dumps(sorted_entries, separators=(',', ':'))
    
    # Compute hash
    dir_hash = hashlib.md5(content.encode()).hexdigest()
    
    # Write to cache
    dir_path = Path(cache_path) / 'files' / 'md5' / dir_hash[:2]
    dir_path.mkdir(parents=True, exist_ok=True)
    
    dir_file = dir_path / f"{dir_hash[2:]}.dir"
    
    try:
        dir_file.write_text(content)
    except Exception as e:
        raise ImportError(f"Failed to write .dir file: {e}")
    
    return f"{dir_hash}.dir", len(content)


def create_dvc_file(
    dest_path: Path,
    name: str,
    md5: str,
    size: int,
    nfiles: Optional[int] = None,
) -> Path:
    """Create a .dvc file.
    
    Args:
        dest_path: Destination directory for the .dvc file.
        name: Name of the output (file or directory name).
        md5: MD5 hash (with .dir suffix for directories).
        size: Size in bytes.
        nfiles: Number of files (for directories).
        
    Returns:
        Path to the created .dvc file.
    """
    # Build the output entry
    out = {
        'md5': md5,
        'size': size,
        'hash': 'md5',
        'path': name,
    }
    
    if nfiles is not None:
        out['nfiles'] = nfiles
    
    content = {'outs': [out]}
    
    # Determine .dvc filename
    if name.endswith('/'):
        name = name.rstrip('/')
    
    dvc_filename = f"{name}.dvc"
    dvc_path = dest_path / dvc_filename
    
    dest_path.mkdir(parents=True, exist_ok=True)
    
    with open(dvc_path, 'w') as f:
        yaml.dump(content, f, default_flow_style=False, sort_keys=False)
    
    return dvc_path


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
    
    # Step 2: Find a local cache from this repo
    if verbose:
        print(f"Looking for local cache from {repository}...")
    
    result = remote_mod.find_local_remote_from_repo(
        repo_spec=repository,
        owner=owner,
    )
    
    if not result:
        raise ImportError(
            f"No locally-accessible cache found for {repository}.\n"
            f"Run: dt cache add-from {repository}"
        )
    
    remote_name, cache_path = result
    
    if verbose:
        print(f"Using cache: {cache_path}")
    
    # Step 3: Configure the clone to use this cache
    configure_clone_cache(clone_path, cache_path)
    
    # Step 4: List files at the path
    if verbose:
        print(f"Listing files at {path}...")
    
    files = list_files(clone_path, path, recursive=True)
    
    if not files:
        raise ImportError(f"No files found at {path}")
    
    if verbose:
        print(f"Found {len(files)} file(s)")
    
    # Add cache to alt caches for checkout
    cfg.add_list_value('cache.alt', cache_path, 'local')
    
    # Determine output name
    output_name = name or Path(path.rstrip('/')).name
    
    # Step 5: Create .dvc file (and .dir file if needed)
    if len(files) == 1 and not files[0].get('isdir', False):
        # Single file import
        file_info = files[0]
        md5 = file_info['md5']
        
        # Get size from cache
        size = get_file_size_from_cache(cache_path, md5)
        if size is None:
            size = 0  # Fallback
        
        if verbose:
            print(f"Importing single file: {md5} ({size} bytes)")
        
        dvc_file = create_dvc_file(
            dest_path=dest_path,
            name=output_name,
            md5=md5,
            size=size,
        )
    else:
        # Directory import - need to create .dir file
        if verbose:
            print(f"Creating .dir file for {len(files)} files...")
        
        # Build entries for .dir file
        entries = []
        total_size = 0
        
        for f in files:
            if f.get('isdir', False):
                continue  # Skip directory entries
            
            md5 = f['md5']
            relpath = f['path']
            
            entries.append({
                'md5': md5,
                'relpath': relpath,
            })
            
            # Get size from cache
            file_size = get_file_size_from_cache(cache_path, md5)
            if file_size:
                total_size += file_size
        
        if not entries:
            raise ImportError(f"No files found in {path}")
        
        # Create .dir file in the cache
        dir_hash, dir_size = create_dir_file(entries, cache_path)
        
        if verbose:
            print(f"Created .dir file: {dir_hash}")
        
        # Create .dvc file
        dvc_file = create_dvc_file(
            dest_path=dest_path,
            name=output_name,
            md5=dir_hash,
            size=total_size,
            nfiles=len(entries),
        )
    
    if verbose:
        print(f"Created {dvc_file}")
    
    # Step 6: Checkout if requested
    if checkout:
        if verbose:
            print(f"Checking out {dvc_file}...")
        
        results = checkout_mod.checkout(
            targets=[str(dvc_file)],
            verbose=verbose,
        )
        
        # Check if any cache succeeded
        any_success = any(success for _, success, _ in results)
        if not any_success:
            print(f"Warning: Checkout failed. Run 'dt checkout {dvc_file}' after configuring caches.")
    
    return dvc_file, cache_path
