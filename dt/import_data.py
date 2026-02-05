"""Import DVC-tracked data from remote repositories.

Enables importing files and directories (or subsets) from other DVC projects
by creating our own .dvc and .dir files, then using dt fetch + dvc checkout.

Unlike dvc import, this does not require network access to the remote storage.
Instead, it uses locally-accessible cache paths.
"""

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

import yaml

from . import remote as remote_mod
from . import tmp as tmp_mod
from . import utils
from .errors import ImportError_ as ImportError


def populate_primary_cache(
    files: List[Dict[str, Any]],
    workspace_path: Path,
    primary_cache: str,
    verbose: bool = False,
) -> int:
    """Hardlink workspace symlinks to the primary cache.
    
    After dvc checkout creates symlinks in the workspace pointing to an alt cache,
    this function creates hardlinks to those symlinks in the primary cache.
    This allows regular DVC operations (dvc checkout, dvc status) to work
    transparently by finding files in the primary cache location.
    
    If hardlinking fails (e.g., cross-device), falls back to creating symlinks
    in the cache that point to the same target as the workspace symlinks.
    
    Args:
        files: List of file info dicts with 'md5' and 'path' keys.
        workspace_path: Path to the workspace directory/file that was checked out.
        primary_cache: Path to the primary DVC cache.
        verbose: Print progress messages.
        
    Returns:
        Number of files added to cache.
    """
    count = 0
    cache_base = Path(primary_cache) / 'files' / 'md5'
    
    for f in files:
        if f.get('isdir', False):
            continue
        
        md5 = f.get('md5')
        relpath = f.get('path', '')
        
        if not md5:
            continue
        
        # Workspace file location
        if relpath:
            workspace_file = workspace_path / relpath
        else:
            workspace_file = workspace_path
        
        # Only process if it's a symlink (created by dvc checkout)
        if not workspace_file.is_symlink():
            continue
        
        # Cache file location (hash-based)
        cache_file = cache_base / md5[:2] / md5[2:]
        
        # Skip if already exists in cache
        if cache_file.exists():
            continue
        
        # Create parent directory if needed
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Follow DVC's preferred order: reflink → hardlink → symlink → copy
        actual_file = workspace_file.resolve()
        cached = False
        
        # 1. Try reflink (copy-on-write) - best option
        try:
            import subprocess
            result = subprocess.run(
                ['cp', '--reflink=only', str(actual_file), str(cache_file)],
                capture_output=True
            )
            if result.returncode == 0:
                count += 1
                cached = True
                if verbose:
                    print(f"  Cached (reflink): {md5[:8]}... ({relpath or workspace_path.name})")
        except (OSError, FileNotFoundError):
            pass
        
        if cached:
            continue
        
        # 2. Try hardlink - same inode, no extra space
        try:
            os.link(actual_file, cache_file)
            count += 1
            cached = True
            if verbose:
                print(f"  Cached (hardlink): {md5[:8]}... ({relpath or workspace_path.name})")
        except OSError as e:
            # EXDEV (18): Cross-device link
            # EPERM (1): Operation not permitted (HPC quota restrictions)
            # EACCES (13): Permission denied
            if e.errno not in (1, 13, 18):
                if verbose:
                    print(f"  Warning: Failed to cache {md5[:8]}...: {e}")
                continue
        
        if cached:
            continue
        
        # 3. Try symlink - works across filesystems
        try:
            os.symlink(actual_file, cache_file)
            count += 1
            cached = True
            if verbose:
                print(f"  Cached (symlink): {md5[:8]}... ({relpath or workspace_path.name})")
        except OSError:
            pass
        
        if cached:
            continue
        
        # 4. Fall back to regular copy
        try:
            import shutil
            shutil.copy2(actual_file, cache_file)
            count += 1
            if verbose:
                print(f"  Cached (copy): {md5[:8]}... ({relpath or workspace_path.name})")
        except OSError as e:
            if verbose:
                print(f"  Warning: Failed to copy {md5[:8]}...: {e}")
    
    return count


def is_v3_dvc_file(dvc_data: Dict[str, Any]) -> bool:
    """Detect if a .dvc file uses v3 format based on explicit hash field.
    
    DVC v3 format explicitly includes 'hash: md5' in outputs.
    DVC v2 format only has 'md5:' without the 'hash:' field.
    
    Args:
        dvc_data: Parsed .dvc file contents.
        
    Returns:
        True if v3 format (has explicit hash field), False if v2/legacy.
    """
    outs = dvc_data.get('outs', [])
    if not outs:
        return True  # Default to v3 for empty/new files
    
    # Check first output for explicit hash field
    out = outs[0]
    return 'hash' in out


def build_dir_manifest(entries: List[Dict[str, str]]) -> bytes:
    """Build a .dir manifest file content in exact DVC format.
    
    DVC uses a specific JSON format for .dir files that must be reproduced
    exactly to match the expected hash. The format is:
    - JSON array, no trailing newline
    - Compact format with `: ` after colons and `, ` between entries
    - Keys in order: md5, relpath
    - Entries sorted by relpath
    
    Args:
        entries: List of dicts with 'md5' and 'relpath' keys.
        
    Returns:
        Bytes content of the .dir file.
    """
    # Sort by relpath
    sorted_entries = sorted(entries, key=lambda x: x['relpath'])
    
    # Build JSON with exact DVC format
    # DVC uses: [{"md5": "...", "relpath": "..."}, {"md5": "...", "relpath": "..."}]
    parts = []
    for entry in sorted_entries:
        parts.append(f'{{"md5": "{entry["md5"]}", "relpath": "{entry["relpath"]}"}}')
    
    content = '[' + ', '.join(parts) + ']'
    return content.encode('utf-8')


def construct_dir_file(
    source_dir: Path,
    expected_hash: str,
    dest_cache: str,
    use_v3_layout: bool = True,
    verbose: bool = False,
) -> Optional[List[Dict[str, str]]]:
    """Construct a .dir manifest file from source directory using DVC internals.
    
    When importing a directory that is not DVC-tracked in the source repo,
    the .dir file only exists in the importing repo's cache. This function
    uses DVC's internal build() function to construct it correctly.
    
    Args:
        source_dir: Path to the source directory to scan.
        expected_hash: The expected MD5 hash of the .dir file (without .dir suffix).
        dest_cache: Path to destination cache base directory.
        use_v3_layout: If True, write .dir file to v3 layout.
        verbose: Print progress messages.
        
    Returns:
        List of manifest entries if successful, None if hash mismatch.
    """
    import tempfile
    
    if not source_dir.exists() or not source_dir.is_dir():
        if verbose:
            print(f"  ERROR: Source directory not found: {source_dir}")
        return None
    
    # Check for nested DVC structure (directory containing .dvc files)
    dvc_files_in_source = list(source_dir.rglob('*.dvc'))
    if dvc_files_in_source:
        if verbose:
            print(f"  ERROR: Source directory contains DVC files - cannot reconstruct .dir")
            print(f"    Found {len(dvc_files_in_source)} .dvc file(s):")
            for f in dvc_files_in_source[:5]:
                print(f"      {f.relative_to(source_dir)}")
            if len(dvc_files_in_source) > 5:
                print(f"      ... and {len(dvc_files_in_source) - 5} more")
            print()
            print("    This is a nested DVC import - the source directory itself contains")
            print("    DVC-tracked data that would need 'dvc checkout' to materialize.")
            print()
            print("    The .dir file must be copied from the machine where 'dvc import'")
            print("    was originally run. Look in that machine's cache at:")
            print(f"      .dvc/cache/files/md5/{expected_hash[:2]}/{expected_hash[2:]}.dir")
        return None
    
    try:
        # Use DVC internals to build the directory hash
        from dvc_data.hashfile.build import build
        from dvc_data.hashfile.db.local import LocalHashFileDB
        from dvc_data.hashfile.tree import Tree
        from dvc.fs import LocalFileSystem
        
        fs = LocalFileSystem()
        
        # Create a temp cache for the ODB
        with tempfile.TemporaryDirectory() as tmpdir:
            odb = LocalHashFileDB(fs, tmpdir)
            
            # Build the tree (dry_run=True means just compute hashes, don't save)
            _, _, hash_file = build(odb, str(source_dir), fs, 'md5', dry_run=True)
            
            if not isinstance(hash_file, Tree):
                if verbose:
                    print(f"  ERROR: DVC build did not produce a Tree object")
                return None
            
            # Get the computed hash (without .dir suffix)
            actual_hash = hash_file.hash_info.value.replace('.dir', '')
            
            if actual_hash != expected_hash:
                if verbose:
                    # Get entries for debug output
                    debug_entries = hash_file.as_list()
                    print(f"  ERROR: Constructed .dir hash mismatch!")
                    print(f"    Expected: {expected_hash}")
                    print(f"    Got:      {actual_hash}")
                    print(f"    Source dir: {source_dir}")
                    print(f"    Files found ({len(debug_entries)}):")
                    for entry in debug_entries[:20]:  # Limit to first 20
                        print(f"      {entry.get('relpath')}: {entry.get('md5')}")
                    if len(debug_entries) > 20:
                        print(f"      ... and {len(debug_entries) - 20} more files")
                    print()
                    print("    This can happen when:")
                    print("    1. The source directory contains DVC-tracked files (nested DVC)")
                    print("       In this case, 'dt fetch' cannot reconstruct the .dir file")
                    print("       because it would need to run 'dvc checkout' on the source repo")
                    print("    2. The source directory has changed since the import was created")
                    print()
                    print("    Solution: Copy the .dir file from a machine that has it cached")
                    print("    (likely the machine where 'dvc import' was originally run)")
                return None
            
            # Get manifest content and entries
            manifest_content = hash_file.as_bytes()
            entries = hash_file.as_list()
            
            if verbose:
                print(f"  DVC computed .dir hash: {actual_hash[:12]}... ({len(entries)} files)")
    
    except ImportError as e:
        # Fall back to manual implementation if DVC internals not available
        if verbose:
            print(f"  Warning: DVC internals not available, using fallback: {e}")
        return _construct_dir_file_fallback(
            source_dir, expected_hash, dest_cache, use_v3_layout, verbose
        )
    except Exception as e:
        if verbose:
            print(f"  ERROR: DVC build failed: {e}")
        return None
    
    # Write .dir file to cache
    if use_v3_layout:
        dest_file = Path(dest_cache) / 'files' / 'md5' / expected_hash[:2] / f"{expected_hash[2:]}.dir"
    else:
        dest_file = Path(dest_cache) / expected_hash[:2] / f"{expected_hash[2:]}.dir"
    
    if not dest_file.exists():
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        dest_file.write_bytes(manifest_content)
        if verbose:
            print(f"  Constructed .dir file: {expected_hash[:12]}...")
    
    return entries


def _construct_dir_file_fallback(
    source_dir: Path,
    expected_hash: str,
    dest_cache: str,
    use_v3_layout: bool = True,
    verbose: bool = False,
) -> Optional[List[Dict[str, str]]]:
    """Fallback implementation using manual hashing (for when DVC internals unavailable)."""
    import hashlib
    
    entries = []
    
    # Scan directory recursively
    for file_path in sorted(source_dir.rglob('*')):
        if file_path.is_file():
            # Calculate MD5 hash
            hasher = hashlib.md5()
            try:
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(8192), b''):
                        hasher.update(chunk)
                file_hash = hasher.hexdigest()
                
                # Get relative path from source_dir
                relpath = str(file_path.relative_to(source_dir))
                entries.append({'md5': file_hash, 'relpath': relpath})
            except OSError as e:
                if verbose:
                    print(f"  Warning: Could not hash {file_path}: {e}")
                continue
    
    if not entries:
        if verbose:
            print(f"  Warning: No files found in {source_dir}")
        return None
    
    # Build the manifest content
    manifest_content = build_dir_manifest(entries)
    
    # Verify hash matches expected
    actual_hash = hashlib.md5(manifest_content).hexdigest()
    
    if actual_hash != expected_hash:
        if verbose:
            print(f"  ERROR: Constructed .dir hash mismatch!")
            print(f"    Expected: {expected_hash}")
            print(f"    Got:      {actual_hash}")
        return None
    
    # Write .dir file to cache
    if use_v3_layout:
        dest_file = Path(dest_cache) / 'files' / 'md5' / expected_hash[:2] / f"{expected_hash[2:]}.dir"
    else:
        dest_file = Path(dest_cache) / expected_hash[:2] / f"{expected_hash[2:]}.dir"
    
    if not dest_file.exists():
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        dest_file.write_bytes(manifest_content)
        if verbose:
            print(f"  Constructed .dir file (fallback): {expected_hash[:12]}...")
    
    return entries


def populate_cache_file(
    md5: str,
    source_cache: str,
    dest_cache: str,
    verbose: bool = False,
    use_v3_layout: bool = True,
) -> bool:
    """Copy or link a single file from source cache to destination cache.
    
    Used for .dir files and single file imports where we need to ensure
    the file exists in the primary cache.
    
    Supports both DVC v3 cache layout (files/md5/XX/hash) and legacy 
    DVC v2 layout (XX/hash directly in remote root).
    
    Args:
        md5: The MD5 hash (with optional .dir suffix).
        source_cache: Path to the source cache/remote root (e.g., /path/to/.remote).
            May contain files/md5/ structure (v3) or direct hash dirs (v2).
        dest_cache: Path to the destination cache base directory.
            For v3 layout, files go in dest_cache/files/md5/XX/hash.
            For v2 layout, files go in dest_cache/XX/hash.
        verbose: Print progress messages.
        use_v3_layout: If True, use v3 layout (files/md5/XX/hash) for destination.
            If False, use v2 layout (XX/hash). Should match the .dvc file format.
        
    Returns:
        True if file was added to cache, False otherwise.
    """
    # Handle .dir suffix
    if md5.endswith('.dir'):
        hash_only = md5[:-4]
        filename = hash_only[2:] + '.dir'
    else:
        hash_only = md5
        filename = hash_only[2:]
    
    # Try DVC v3 path first (files/md5/XX/hash)
    source_file_v3 = Path(source_cache) / 'files' / 'md5' / hash_only[:2] / filename
    # Fall back to DVC v2 path (XX/hash directly in remote root)
    source_file_v2 = Path(source_cache) / hash_only[:2] / filename
    
    # Use whichever exists
    if source_file_v3.exists():
        source_file = source_file_v3
    elif source_file_v2.exists():
        source_file = source_file_v2
        if verbose:
            print(f"  Using legacy cache layout for: {md5[:12]}...")
    else:
        # Neither exists - this is a critical error, not a warning
        if verbose:
            print(f"  ERROR: Source file not found in cache: {md5[:12]}...")
            print(f"    Checked: {source_file_v3}")
            print(f"    Checked: {source_file_v2}")
        return False
    
    # Destination path depends on the .dvc file format
    # v3 format (.dvc has 'hash: md5') -> files/md5/XX/hash
    # v2 format (.dvc only has 'md5:') -> XX/hash (at cache root)
    if use_v3_layout:
        dest_file = Path(dest_cache) / 'files' / 'md5' / hash_only[:2] / filename
    else:
        dest_file = Path(dest_cache) / hash_only[:2] / filename
    
    if dest_file.exists():
        return False
    
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Follow DVC's preferred order: reflink → hardlink → symlink → copy
    # See: https://dvc.org/doc/user-guide/data-management/large-dataset-optimization
    
    # 1. Try reflink (copy-on-write) - best option: instant, zero space, safe to modify
    try:
        import subprocess
        result = subprocess.run(
            ['cp', '--reflink=only', str(source_file), str(dest_file)],
            capture_output=True
        )
        if result.returncode == 0:
            if verbose:
                print(f"  Cached (reflink): {md5[:12]}...")
            return True
    except (OSError, FileNotFoundError):
        pass  # cp not available or reflink not supported
    
    # 2. Try hardlink - same inode, no extra space, works within same filesystem/project
    try:
        os.link(source_file, dest_file)
        if verbose:
            print(f"  Cached (hardlink): {md5[:12]}...")
        return True
    except OSError as e:
        # EXDEV (18): Cross-device link (different filesystems)
        # EPERM (1): Operation not permitted (common on HPC with quota restrictions)
        # EACCES (13): Permission denied
        if e.errno not in (1, 13, 18):
            if verbose:
                print(f"  ERROR: Failed to cache {md5[:12]}...: {e}")
            return False
    
    # 3. Try symlink - pointer to source, no extra space, works across filesystems
    try:
        os.symlink(source_file, dest_file)
        if verbose:
            print(f"  Cached (symlink): {md5[:12]}...")
        return True
    except OSError as e:
        if verbose:
            print(f"  Warning: symlink failed for {md5[:12]}...: {e}")
    
    # 4. Fall back to regular copy - slower but universally compatible
    try:
        import shutil
        shutil.copy2(source_file, dest_file)
        if verbose:
            print(f"  Cached (copy): {md5[:12]}...")
        return True
    except OSError as e:
        if verbose:
            print(f"  ERROR: Failed to copy {md5[:12]}...: {e}")
    
    return False


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
    out: Optional[str] = None,
    owner: Optional[str] = None,
    checkout: bool = True,
    verbose: bool = False,
    refresh: bool = True,
) -> Tuple[Path, Optional[str]]:
    """Import DVC-tracked data from a remote repository.
    
    Args:
        repository: Repository name, alias, or URL.
        path: Path to the file/directory in the remote repo.
        out: Destination path to download files to (default: basename of path).
        owner: Optional owner override for short names.
        checkout: Whether to run checkout after import.
        verbose: Print progress messages.
        refresh: Whether to refresh temp clone (default True).
        
    Returns:
        Tuple of (dvc_file_path, cache_path used).
        
    Raises:
        ImportError: If import fails.
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise ImportError(str(e))
    
    # Determine destination path
    if out:
        out_path = Path(out)
    else:
        out_path = Path(Path(path).name)
    
    # Step 1: Ensure we have a sparse clone of the repo
    if verbose:
        print(f"Ensuring clone of {repository}...")
    
    try:
        clone_path = tmp_mod.clone_repo(repository, owner=owner, refresh=refresh)
    except tmp_mod.TmpError as e:
        raise ImportError(f"Failed to clone repository: {e}")
    
    if verbose:
        print(f"Using clone at {clone_path}")
    
    # Step 2: Find a local cache from this repo
    if verbose:
        print(f"Looking for local cache from {repository}...")
    
    cache_path = None
    
    # First, try to find a local remote in the source repo
    result = remote_mod.find_local_remote_from_repo(
        repo_spec=repository,
        owner=owner,
    )
    
    if result:
        _, cache_path = result
    
    if not cache_path:
        raise ImportError(
            f"No locally-accessible cache found for {repository}.\n"
            f"The source repository's remote may not be on this filesystem.\n"
            f"Options:\n"
            f"  1. Ensure the source repo has a locally-accessible remote configured\n"
            f"  2. Use 'dvc import' and 'dvc pull' to fetch from the remote directly"
        )
    
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
    
    # Track whether this is a directory or single file import
    is_directory = not (len(files) == 1 and not files[0].get('isdir', False))
    root_hash = None  # Will be set to dir_hash or single file md5
    
    # Step 5: Create .dvc file (and .dir file if needed)
    if not is_directory:
        # Single file import
        file_info = files[0]
        root_hash = file_info['md5']
        
        # Get size from cache
        size = get_file_size_from_cache(cache_path, root_hash)
        if size is None:
            size = 0  # Fallback
        
        if verbose:
            print(f"Importing single file: {root_hash} ({size} bytes)")
        
        dvc_file = create_dvc_file(
            dest_path=out_path.parent,
            name=out_path.name,
            md5=root_hash,
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
        root_hash = dir_hash  # Already includes .dir suffix
        
        if verbose:
            print(f"Created .dir file: {dir_hash}")
        
        # Create .dvc file
        dvc_file = create_dvc_file(
            dest_path=out_path.parent,
            name=out_path.name,
            md5=dir_hash,
            size=total_size,
            nfiles=len(entries),
        )
    
    if verbose:
        print(f"Created {dvc_file}")
    
    # Step 6: Update .gitignore to exclude the data file/directory
    # This matches DVC's behavior for dvc add and dvc import
    gitignore_pattern = f"/{out_path.name}"
    if utils.update_gitignore(gitignore_pattern):
        if verbose:
            print(f"Added {gitignore_pattern} to .gitignore")
    
    # Step 7: Populate primary cache and checkout if requested
    if checkout:
        if verbose:
            print(f"Populating cache for {dvc_file}...")
        
        # Populate primary cache with links from source cache
        primary_cache = utils.get_cache_dir()
        if primary_cache:
            # First, add the .dir file or single file hash to primary cache
            if root_hash:
                populate_cache_file(
                    md5=root_hash,
                    source_cache=cache_path,
                    dest_cache=primary_cache,
                    verbose=verbose,
                )
            
            # Then, add individual files
            for f in files:
                if f.get('isdir', False):
                    continue
                file_md5 = f.get('md5', '')
                if file_md5:
                    populate_cache_file(
                        md5=file_md5,
                        source_cache=cache_path,
                        dest_cache=primary_cache,
                        verbose=verbose,
                    )
            
            # Now run dvc checkout
            if verbose:
                print(f"Checking out {dvc_file}...")
            
            result = subprocess.run(
                ['dvc', 'checkout', str(dvc_file)],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                print(f"Warning: Checkout failed: {result.stderr.strip()}")
                print(f"Run 'dvc checkout {dvc_file}' after checking cache configuration.")
        else:
            print("Warning: No primary cache configured. Run 'dvc checkout' manually.")
    
    return dvc_file, cache_path
