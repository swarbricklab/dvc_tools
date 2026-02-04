"""Pull DVC-tracked files, automatically handling imports.

For targets tracked by import .dvc files (those with deps.repo), uses
dt fetch to populate the cache from the source repo's cache. For other 
targets, uses regular dvc pull.

Supports parallel distribution via qxub for high-throughput pulls.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from . import hpc
from . import utils
from .fetch import fetch, smart_checkout
from .errors import FetchError, PullError


# =============================================================================
# Utility functions  
# =============================================================================


def get_remote_files_size(file_hashes: List[str], remote: Optional[str] = None) -> int:
    """Estimate total size of files from remote.
    
    Uses local cache if files are present there as a proxy for size.
    
    Args:
        file_hashes: List of MD5 hashes
        remote: Optional remote name
        
    Returns:
        Total size in bytes (0 if not determinable)
    """
    try:
        from dvc.repo import Repo
        repo = Repo()
        cache_dir = Path(repo.cache.local.path)
    except Exception:
        return 0
    
    total = 0
    for file_hash in file_hashes:
        hash_clean = file_hash.replace('.dir', '')
        prefix = hash_clean[:2]
        suffix = hash_clean[2:]
        if file_hash.endswith('.dir'):
            suffix += '.dir'
        
        cache_file = cache_dir / prefix / suffix
        if cache_file.exists():
            try:
                total += cache_file.stat().st_size
            except OSError:
                pass
    
    return total


# =============================================================================
# Parallel pull infrastructure
# =============================================================================


def build_pull_manifest(
    targets: Optional[List[str]] = None,
    remote: Optional[str] = None,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Build a manifest of files to pull using DVC internals.
    
    Args:
        targets: Optional list of targets (.dvc files, paths, stages)
        remote: Optional remote name to pull from
        verbose: Print progress messages
        
    Returns:
        Dict with 'files' (list of hash strings), 'paths' (hash->path mapping),
        and 'remote' info
    """
    try:
        from dvc_data.index.fetch import collect
        from dvc_data.index import ObjectStorage
        from dvc_data.hashfile.status import compare_status
        from fsspec.utils import tokenize
    except ImportError as e:
        raise PullError(f"DVC internals not available: {e}")
    
    if verbose:
        print(f"Collecting files to pull...")
    
    # Use shared helper to collect tracked entries
    try:
        result = utils.collect_tracked_entries(targets=targets, remote=remote, push=False)
    except utils.DependencyError as e:
        raise PullError(str(e))
    
    repo = result['repo']
    indexes = result['indexes']
    hash_to_path = result['hash_to_path']
    
    if not indexes:
        return {'files': [], 'paths': {}, 'remote': remote, 'repo_root': str(repo.root_dir)}
    
    # Generate cache key for collect
    cache_key = (
        'fetch',
        tokenize(sorted(idx.data_tree.hash_info.value for idx in indexes.values())),
    )
    
    # Collect files that could potentially need pulling
    data = collect(
        [idx.data['repo'] for idx in indexes.values()],
        'remote',
        cache_index=repo.data_index,
        cache_key=cache_key,
        push=False,  # Fetch mode
    )
    
    # Filter to only files that actually need pulling (not already in cache)
    files = []
    for fs_idx in data:
        storage = fs_idx.storage_map[()]
        cache = storage.cache
        remote_storage = storage.data
        
        if isinstance(cache, ObjectStorage) and isinstance(remote_storage, ObjectStorage):
            cache_odb = cache.odb
            remote_odb = remote_storage.odb
            
            # Get all hash_info objects
            obj_ids = [entry.hash_info for _, entry in fs_idx.iteritems() if entry.hash_info]
            
            if obj_ids:
                # Compare remote to cache to see what we're missing locally
                # For pull: we want files on remote that are NOT in cache
                status = compare_status(
                    remote_odb,   # Source (remote has files)
                    cache_odb,    # Destination (cache might be missing files)
                    obj_ids,
                    check_deleted=False,
                    shallow=True,
                )
                
                # status.new = files on remote not in cache (what we need to pull)
                for hash_info in status.new:
                    files.append(hash_info.value)
        else:
            # Fallback: include all files (can't check cache)
            for key, entry in fs_idx.items():
                if entry.hash_info and entry.hash_info.value:
                    files.append(entry.hash_info.value)
    
    if verbose:
        print(f"Found {len(files)} file(s) to pull")
    
    return {
        'files': files,
        'paths': hash_to_path,
        'remote': remote,
        'repo_root': str(repo.root_dir),
    }


def partition_manifest(
    manifest: Dict[str, Any],
    num_workers: int,
) -> Dict[int, List[str]]:
    """Partition manifest files across workers by hash prefix.
    
    Args:
        manifest: Manifest from build_pull_manifest()
        num_workers: Number of workers
        
    Returns:
        Dict mapping worker_id to list of file hashes
    """
    partitions: Dict[int, List[str]] = {i: [] for i in range(num_workers)}
    
    for file_hash in manifest['files']:
        # Use first 2 chars (hex prefix) for partitioning
        prefix_value = int(file_hash[:2], 16)
        worker_id = prefix_value % num_workers
        partitions[worker_id].append(file_hash)
    
    return partitions


def pull_partition(
    file_hashes: Set[str],
    remote: Optional[str] = None,
    jobs: int = 1,
    verbose: bool = False,
) -> Tuple[int, int]:
    """Pull a partition of files using direct remote-to-cache transfer.
    
    This bypasses DVC's workspace index entirely, avoiding SQLite lock
    contention when running parallel workers. We transfer directly from
    the remote ODB to the cache ODB using the file hashes.
    
    Args:
        file_hashes: Set of file hashes to pull
        remote: Optional remote name
        jobs: Number of parallel download threads
        verbose: Print progress
        
    Returns:
        Tuple of (pulled_count, failed_count)
    """
    try:
        from dvc.repo import Repo
        from dvc_data.hashfile.transfer import transfer
        from dvc_data.hashfile.hash_info import HashInfo
    except ImportError as e:
        raise PullError(f"DVC internals not available: {e}")
    
    if not file_hashes:
        return 0, 0
    
    repo = Repo()
    
    if verbose:
        print(f"Preparing to pull {len(file_hashes)} files...")
    
    # Get cache and remote ODBs directly (no index access)
    cache_odb = repo.cache.local
    remote_obj = repo.cloud.get_remote(name=remote)
    remote_odb = remote_obj.odb
    
    if verbose:
        print(f"Remote: {remote_odb.path}")
        print(f"Cache: {cache_odb.path}")
    
    # Create HashInfo objects for the files to pull
    obj_ids = [HashInfo('md5', h) for h in file_hashes]
    
    if verbose:
        print(f"Transferring {len(obj_ids)} files...")
    
    # Direct transfer from remote to cache (opposite of push)
    result = transfer(
        remote_odb,   # Source: remote
        cache_odb,    # Destination: cache
        obj_ids,
        jobs=jobs,
    )
    
    pulled = len(result.transferred)
    failed = len(result.failed)
    
    if verbose:
        print(f"Transferred: {pulled}, Failed: {failed}")
    
    return pulled, failed


# =============================================================================
# Distributed pull via qxub
# =============================================================================

def parallel_pull(
    targets: Optional[List[str]] = None,
    remote: Optional[str] = None,
    num_workers: int = 4,
    qxub_args: Optional[List[str]] = None,
    wait: bool = True,
    verbose: bool = False,
) -> Tuple[List[str], Optional[Path]]:
    """Execute a parallel pull using qxub workers.
    
    Args:
        targets: Optional targets to pull
        remote: Optional remote name
        num_workers: Number of worker jobs
        qxub_args: Additional qxub arguments
        wait: Wait for jobs to complete
        verbose: Print progress
        
    Returns:
        Tuple of (job_ids, manifest_dir)
    """
    import uuid
    
    # Build manifest
    if verbose:
        print("Building manifest...")
    
    manifest = build_pull_manifest(targets=targets, remote=remote, verbose=verbose)
    
    if not manifest['files']:
        if verbose:
            print("Nothing to pull")
        return [], None
    
    # Partition files
    partitions = partition_manifest(manifest, num_workers)
    
    # Check how many workers actually have files
    active_workers = sum(1 for files in partitions.values() if files)
    if verbose:
        print(f"Partitioned {len(manifest['files'])} files across {active_workers} workers")
    
    # Save manifest
    job_id = str(uuid.uuid4())[:8]
    manifest_dir = hpc.save_manifest(manifest, partitions, job_id, operation='pull')
    
    if verbose:
        print(f"Manifest saved to {manifest_dir}")
    
    # Submit workers
    try:
        job_ids = hpc.submit_workers(
            manifest_dir,
            num_workers,
            operation='pull',
            qxub_args=qxub_args,
            verbose=verbose,
        )
    except hpc.HPCError as e:
        raise PullError(str(e))
    
    if not job_ids:
        print("No jobs submitted")
        return [], manifest_dir
    
    if verbose:
        print(f"Submitted {len(job_ids)} job(s)")
    
    # Wait for completion if requested
    if wait:
        try:
            success = hpc.monitor_jobs(job_ids, verbose=verbose)
        except hpc.HPCError as e:
            raise PullError(str(e))
        if not success:
            raise PullError("Some jobs failed")
    
    return job_ids, manifest_dir


def worker_pull(
    manifest_dir: Path,
    worker_id: int,
    jobs: int = 1,
    verbose: bool = False,
) -> Tuple[int, int]:
    """Execute a worker pull (called by submitted jobs).
    
    Args:
        manifest_dir: Path to manifest directory
        worker_id: Worker index
        jobs: Parallel download threads
        verbose: Print progress
        
    Returns:
        Tuple of (pulled_count, failed_count)
    """
    # Change to repo root
    metadata, file_hashes = hpc.load_worker_partition(manifest_dir, worker_id)
    
    repo_root = metadata.get('repo_root')
    if repo_root:
        os.chdir(repo_root)
    
    if verbose:
        print(f"Worker {worker_id}: {len(file_hashes)} files to pull")
    
    if not file_hashes:
        if verbose:
            print("Nothing to pull")
        return 0, 0
    
    return pull_partition(
        set(file_hashes),
        remote=metadata.get('remote'),
        jobs=jobs,
        verbose=verbose,
    )


# =============================================================================
# Original path resolution functions
# =============================================================================


def resolve_to_dvc_file(target: str) -> Optional[Path]:
    """Resolve a target to its tracking .dvc file.
    
    Resolution order:
    1. If target ends with .dvc, return it if it exists
    2. If {target}.dvc exists, return it
    3. Check parent directories for a .dvc file tracking the parent
       (stops at project root - directory containing .dvc/)
    
    Args:
        target: Target path (file, directory, or .dvc file).
        
    Returns:
        Path to the tracking .dvc file, or None if not found.
    """
    target_path = Path(target)
    
    # If it's already a .dvc file
    if target.endswith('.dvc'):
        if target_path.exists():
            return target_path
        return None
    
    # Check if {target}.dvc exists
    dvc_path = Path(f"{target}.dvc")
    if dvc_path.exists():
        return dvc_path
    
    # Find project root to know when to stop
    project_root = utils.find_dvc_root(Path.cwd())
    if project_root is None:
        return None
    
    # Check parent directories up to project root
    # For data/subdir/file.txt, check data/subdir.dvc, data.dvc, etc.
    current = target_path.resolve()
    while current >= project_root:
        parent_dvc = Path(f"{current}.dvc")
        if parent_dvc.exists():
            return parent_dvc
        if current == project_root:
            break
        current = current.parent
    
    return None


def is_import_target(target: str) -> Tuple[bool, Optional[Path]]:
    """Check if a target is tracked by an import .dvc file.
    
    Args:
        target: Target path to check.
        
    Returns:
        Tuple of (is_import, dvc_file_path).
    """
    dvc_file = resolve_to_dvc_file(target)
    if dvc_file is None:
        return False, None
    
    # Use DVC's internal is_repo_import check
    if utils.is_repo_import(dvc_file):
        return True, dvc_file
    
    return False, dvc_file


def find_all_dvc_files() -> List[Path]:
    """Find all .dvc files in the current directory tree.
    
    Returns:
        List of paths to .dvc files (excludes .dvc/ directory and .dt/ temp clones).
    """
    cwd = Path.cwd()
    # Exclude:
    # - .dvc/ directory itself (not a .dvc file)
    # - .dt/ directory (temp clones, manifests, etc.)
    # - Must be a file, not a directory
    return sorted(
        f for f in cwd.rglob('*.dvc') 
        if f.is_file() and '.dt' not in f.parts and f.name != '.dvc'
    )


def separate_targets(
    targets: List[str],
    verbose: bool = False,
) -> Tuple[List[str], List[str]]:
    """Separate targets into import and regular targets.
    
    Args:
        targets: List of target paths.
        verbose: Print progress messages.
        
    Returns:
        Tuple of (import_targets, regular_targets).
    """
    import_targets = []
    regular_targets = []
    
    for target in targets:
        is_import, dvc_file = is_import_target(target)
        if is_import:
            if verbose:
                print(f"  {target} → import ({dvc_file})")
            import_targets.append(target)
        else:
            if verbose:
                print(f"  {target} → regular")
            regular_targets.append(target)
    
    return import_targets, regular_targets


def pull(
    targets: Optional[List[str]] = None,
    verbose: bool = False,
    dvc_args: Optional[List[str]] = None,
    refresh: bool = True,
) -> bool:
    """Pull DVC-tracked files, handling imports automatically.
    
    Args:
        targets: Specific targets to pull. If None, pulls all.
        verbose: Print detailed progress.
        dvc_args: Additional arguments to pass to dvc pull.
        refresh: Whether to refresh temp clones (default True).
        
    Returns:
        True if all operations succeeded.
    """
    success = True
    
    # If no targets specified, find all .dvc files
    if not targets:
        if verbose:
            print("Discovering .dvc files...")
        all_dvc_files = find_all_dvc_files()
        targets = [str(f) for f in all_dvc_files]
        if verbose:
            print(f"  Found {len(targets)} .dvc files")
    
    if not targets:
        print("No .dvc files found")
        return True
    
    # Separate import targets from regular targets
    if verbose:
        print("Resolving targets...")
    import_targets, regular_targets = separate_targets(targets, verbose)
    
    # Handle imports with dt fetch + dvc checkout
    if import_targets:
        if verbose:
            print(f"\nHandling {len(import_targets)} import target(s)...")
        
        for target in import_targets:
            try:
                # Resolve to .dvc file for fetch
                dvc_file = resolve_to_dvc_file(target)
                if dvc_file:
                    if verbose:
                        print(f"  dt fetch {dvc_file}")
                    fetch(
                        targets=[str(dvc_file)],
                        verbose=verbose,
                        refresh=refresh,
                    )
                    # Then checkout the fetched files
                    if verbose:
                        print(f"  dvc checkout {dvc_file}")
                    subprocess.run(['dvc', 'checkout', str(dvc_file)], check=False)
            except FetchError as e:
                print(f"Error fetching {target}: {e}")
                success = False
    
    # Handle regular targets with dvc pull
    if regular_targets:
        if verbose:
            print(f"\nPulling {len(regular_targets)} regular target(s)...")
        
        cmd = ['dvc', 'pull']
        if dvc_args:
            cmd.extend(dvc_args)
        cmd.extend(regular_targets)
        
        if verbose:
            print(f"  Running: {' '.join(cmd)}")
        
        result = subprocess.run(cmd)
        if result.returncode != 0:
            success = False
    elif verbose:
        print("\nNo regular targets to pull")
    
    return success
