"""Push DVC-tracked files to remotes with optional parallel distribution.

Supports two modes:
1. Simple mode: Push to all project-configured remotes via dvc push
2. Parallel mode: Distribute push across multiple compute nodes via qxub

The parallel mode uses DVC internals to:
- Enumerate files to push (respecting targets)
- Partition files by hash prefix for lock-free parallel execution
- Call DVC's internal push function directly (bypassing workspace lock)
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from . import config as cfg
from . import hpc
from . import utils


class PushError(Exception):
    """Error during push operation."""
    pass


# =============================================================================
# Utility functions
# =============================================================================

# Use utils.format_size for size formatting
format_size = utils.format_size


def get_files_size(file_hashes: List[str]) -> int:
    """Get total size of files in cache.
    
    Args:
        file_hashes: List of MD5 hashes
        
    Returns:
        Total size in bytes (0 if cache not accessible)
    """
    try:
        from dvc.repo import Repo
        repo = Repo()
        # repo.cache.local.path already points to the hash directory (files/md5)
        cache_dir = Path(repo.cache.local.path)
    except Exception:
        return 0
    
    total = 0
    for file_hash in file_hashes:
        # Handle .dir suffix
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
# Simple push (existing functionality)
# =============================================================================

def get_project_remotes() -> List[Tuple[str, str]]:
    """Get remotes configured at project or local scope.
    
    Uses DVC's internal config API to reliably get remote information.
    
    Returns:
        List of (name, url) tuples for remotes in project/local config.
    """
    try:
        from dvc.repo import Repo
        repo = Repo()
        
        remotes = []
        seen_names = set()
        
        # Read local and repo (project) scopes - local overrides repo
        for level in ('local', 'repo'):
            try:
                level_config = repo.config.read(level)
                level_remotes = level_config.get('remote', {})
                for name, cfg in level_remotes.items():
                    if name not in seen_names:
                        url = cfg.get('url', '')
                        remotes.append((name, url))
                        seen_names.add(name)
            except Exception:
                continue
        
        return remotes
        
    except ImportError:
        # Fall back to CLI parsing if DVC internals unavailable
        return _get_project_remotes_cli()


def _get_project_remotes_cli() -> List[Tuple[str, str]]:
    """Fallback: Get remotes via CLI parsing.
    
    Returns:
        List of (name, url) tuples for remotes in project/local config.
    """
    remotes = []
    
    # Check both local and project scopes
    for scope in ['local', 'project']:
        try:
            result = subprocess.run(
                ['dvc', 'remote', 'list', f'--{scope}'],
                capture_output=True,
                text=True,
                check=True,
            )
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    # Split on whitespace, first part is name
                    parts = line.split(None, 1)
                    if len(parts) >= 1:
                        name = parts[0]
                        # Skip lines that are just "(default)" marker or URLs
                        if name == '(default)' or name.startswith('ssh://') or name.startswith('s3://'):
                            continue
                        url = parts[1] if len(parts) > 1 else ''
                        # Remove trailing (default) marker from URL
                        url = url.replace('\t(default)', '').replace(' (default)', '').strip()
                        # Avoid duplicates (local overrides project)
                        if not any(r[0] == name for r in remotes):
                            remotes.append((name, url))
        except subprocess.CalledProcessError:
            # Scope might not exist, continue
            continue
    
    return remotes


def push_to_remote(remote: str, args: List[str]) -> Tuple[bool, str]:
    """Push to a single remote, passing through all arguments.
    
    Args:
        remote: Name of the remote to push to.
        args: Additional arguments to pass to dvc push.
        
    Returns:
        Tuple of (success, output).
    """
    cmd = ['dvc', 'push', '-r', remote] + list(args)
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output.strip()
    except Exception as e:
        return False, str(e)


def push_all(args: List[str]) -> List[Tuple[str, bool, str]]:
    """Push to all project-configured remotes.
    
    Args:
        args: Arguments to pass through to dvc push.
        
    Returns:
        List of (remote_name, success, output) tuples.
    """
    remotes = get_project_remotes()
    
    if not remotes:
        raise PushError("No remotes configured at project or local scope.")
    
    results = []
    for name, url in remotes:
        success, output = push_to_remote(name, args)
        results.append((name, success, output))
    
    return results


# =============================================================================
# Parallel push infrastructure
# =============================================================================

def get_push_dir() -> Path:
    """Get the .dt/tmp/push directory for manifest storage."""
    push_dir = Path.cwd() / '.dt' / 'tmp' / 'push'
    push_dir.mkdir(parents=True, exist_ok=True)
    return push_dir


def get_prefixes_for_worker(worker_id: int, num_workers: int) -> Set[str]:
    """Get hash prefixes assigned to a worker.
    
    Partitions the 256 possible hash prefixes (00-ff) across workers.
    
    Args:
        worker_id: Worker index (0 to num_workers-1)
        num_workers: Total number of workers
        
    Returns:
        Set of 2-character hex prefixes for this worker.
    """
    prefixes = set()
    for i in range(256):
        if i % num_workers == worker_id:
            prefixes.add(f"{i:02x}")
    return prefixes


def build_manifest(
    targets: Optional[List[str]] = None,
    remote: Optional[str] = None,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Build a manifest of files to push using DVC internals.
    
    Args:
        targets: Optional list of targets (.dvc files, paths, stages)
        remote: Optional remote name to push to
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
        raise PushError(f"DVC internals not available: {e}")
    
    if verbose:
        print(f"Collecting files to push...")
    
    # Use shared helper to collect tracked entries
    try:
        result = utils.collect_tracked_entries(targets=targets, remote=remote, push=True)
    except utils.DependencyError as e:
        raise PushError(str(e))
    
    repo = result['repo']
    indexes = result['indexes']
    hash_to_path = result['hash_to_path']
    
    if not indexes:
        return {'files': [], 'paths': {}, 'remote': remote, 'repo_root': str(repo.root_dir)}
    
    # Generate cache key for collect
    cache_key = (
        'push',
        tokenize(sorted(idx.data_tree.hash_info.value for idx in indexes.values())),
    )
    
    # Collect files that could potentially need pushing
    data = collect(
        [idx.data['repo'] for idx in indexes.values()],
        'remote',
        cache_index=repo.data_index,
        cache_key=cache_key,
        push=True,
    )
    
    # Filter to only files that actually need pushing (not already on remote)
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
                # Compare with remote to see what's actually new
                status = compare_status(
                    cache_odb,
                    remote_odb,
                    obj_ids,
                    check_deleted=False,
                    shallow=True,
                )
                
                # Only include files that are new (not on remote)
                for hash_info in status.new:
                    files.append(hash_info.value)
        else:
            # Fallback: include all files (can't check remote)
            for key, entry in fs_idx.items():
                if entry.hash_info and entry.hash_info.value:
                    files.append(entry.hash_info.value)
    
    if verbose:
        print(f"Found {len(files)} file(s) to push")
    
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
        manifest: Manifest from build_manifest()
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


def save_manifest(
    manifest: Dict[str, Any],
    partitions: Dict[int, List[str]],
    job_id: str,
) -> Path:
    """Save manifest and partitions to disk.
    
    Args:
        manifest: Original manifest with metadata
        partitions: Worker partitions
        job_id: Unique job identifier
        
    Returns:
        Path to the manifest directory
    """
    manifest_dir = get_push_dir() / job_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    
    # Save metadata
    with open(manifest_dir / 'manifest.json', 'w') as f:
        json.dump({
            'remote': manifest.get('remote'),
            'repo_root': manifest.get('repo_root'),
            'total_files': len(manifest.get('files', [])),
            'num_workers': len(partitions),
        }, f, indent=2)
    
    # Save each worker's partition
    for worker_id, files in partitions.items():
        with open(manifest_dir / f'worker_{worker_id}.json', 'w') as f:
            json.dump({'files': files}, f)
    
    return manifest_dir


def load_worker_partition(manifest_dir: Path, worker_id: int) -> Tuple[Dict, List[str]]:
    """Load manifest metadata and worker's file partition.
    
    Args:
        manifest_dir: Path to manifest directory
        worker_id: Worker index
        
    Returns:
        Tuple of (metadata dict, list of file hashes)
    """
    with open(manifest_dir / 'manifest.json') as f:
        metadata = json.load(f)
    
    worker_file = manifest_dir / f'worker_{worker_id}.json'
    if not worker_file.exists():
        return metadata, []
    
    with open(worker_file) as f:
        partition = json.load(f)
    
    return metadata, partition.get('files', [])


def push_partition(
    file_hashes: Set[str],
    remote: Optional[str] = None,
    jobs: int = 1,
    verbose: bool = False,
) -> Tuple[int, int]:
    """Push a partition of files using direct cache-to-remote transfer.
    
    This bypasses DVC's workspace index entirely, avoiding SQLite lock
    contention when running parallel workers. We transfer directly from
    the cache ODB to the remote ODB using the file hashes.
    
    Args:
        file_hashes: Set of file hashes to push
        remote: Optional remote name
        jobs: Number of parallel upload threads
        verbose: Print progress
        
    Returns:
        Tuple of (pushed_count, failed_count)
    """
    try:
        from dvc.repo import Repo
        from dvc_data.hashfile.transfer import transfer
        from dvc_data.hashfile.hash_info import HashInfo
    except ImportError as e:
        raise PushError(f"DVC internals not available: {e}")
    
    if not file_hashes:
        return 0, 0
    
    repo = Repo()
    
    if verbose:
        print(f"Preparing to push {len(file_hashes)} files...")
    
    # Get cache and remote ODBs directly (no index access)
    cache_odb = repo.cache.local
    remote_obj = repo.cloud.get_remote(name=remote)
    remote_odb = remote_obj.odb
    
    if verbose:
        print(f"Cache: {cache_odb.path}")
        print(f"Remote: {remote_odb.path}")
    
    # Create HashInfo objects for the files to push
    obj_ids = [HashInfo('md5', h) for h in file_hashes]
    
    if verbose:
        print(f"Transferring {len(obj_ids)} files...")
    
    # Direct transfer from cache to remote
    result = transfer(
        cache_odb,
        remote_odb,
        obj_ids,
        jobs=jobs,
    )
    
    pushed = len(result.transferred)
    failed = len(result.failed)
    
    if verbose:
        print(f"Transferred: {pushed}, Failed: {failed}")
    
    return pushed, failed


# =============================================================================
# Distributed push via qxub
# =============================================================================

def parallel_push(
    targets: Optional[List[str]] = None,
    remote: Optional[str] = None,
    num_workers: int = 4,
    qxub_args: Optional[List[str]] = None,
    wait: bool = True,
    verbose: bool = False,
) -> Tuple[List[str], Optional[Path]]:
    """Execute a parallel push using qxub workers.
    
    Args:
        targets: Optional targets to push
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
    
    manifest = build_manifest(targets=targets, remote=remote, verbose=verbose)
    
    if not manifest['files']:
        if verbose:
            print("Nothing to push")
        return [], None
    
    # Partition files
    partitions = partition_manifest(manifest, num_workers)
    
    # Check how many workers actually have files
    active_workers = sum(1 for files in partitions.values() if files)
    if verbose:
        print(f"Partitioned {len(manifest['files'])} files across {active_workers} workers")
    
    # Save manifest
    job_id = str(uuid.uuid4())[:8]
    manifest_dir = save_manifest(manifest, partitions, job_id)
    
    if verbose:
        print(f"Manifest saved to {manifest_dir}")
    
    # Submit workers
    try:
        job_ids = hpc.submit_workers(
            manifest_dir,
            num_workers,
            operation='push',
            qxub_args=qxub_args,
            verbose=verbose,
        )
    except hpc.HPCError as e:
        raise PushError(str(e))
    
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
            raise PushError(str(e))
        if not success:
            raise PushError("Some jobs failed")
    
    return job_ids, manifest_dir


def worker_push(
    manifest_dir: Path,
    worker_id: int,
    jobs: int = 1,
    verbose: bool = False,
) -> Tuple[int, int]:
    """Execute a worker push (called by submitted jobs).
    
    Args:
        manifest_dir: Path to manifest directory
        worker_id: Worker index
        jobs: Parallel upload threads
        verbose: Print progress
        
    Returns:
        Tuple of (pushed_count, failed_count)
    """
    # Change to repo root
    metadata, file_hashes = load_worker_partition(manifest_dir, worker_id)
    
    repo_root = metadata.get('repo_root')
    if repo_root:
        os.chdir(repo_root)
    
    if verbose:
        print(f"Worker {worker_id}: {len(file_hashes)} files to push")
    
    if not file_hashes:
        if verbose:
            print("Nothing to push")
        return 0, 0
    
    return push_partition(
        set(file_hashes),
        remote=metadata.get('remote'),
        jobs=jobs,
        verbose=verbose,
    )
