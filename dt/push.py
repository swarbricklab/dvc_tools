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
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dvc.repo import Repo

from . import config as cfg
from . import hpc
from . import utils
from .errors import PushError

# Default number of times a worker re-attempts files that did not land on the
# remote (transient CompleteMultipartUpload / NoSuchUpload failures under
# concurrency; see issue #141). Each retry re-initiates a fresh transfer.
DEFAULT_PUSH_RETRIES = 3


# =============================================================================
# Utility functions
# =============================================================================


def get_file_sizes(file_hashes: List[str]) -> Dict[str, int]:
    """Stat each cached blob once and return per-hash sizes in bytes.

    Args:
        file_hashes: List of MD5 hashes

    Returns:
        Dict mapping each hash to its size in bytes. Missing blobs (and the
        case where the cache is not accessible) map to 0.
    """
    sizes: Dict[str, int] = {h: 0 for h in file_hashes}

    try:
        repo = Repo()
        # repo.cache.local.path already points to the hash directory (files/md5)
        cache_dir = Path(repo.cache.local.path)
    except Exception:
        return sizes

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
                sizes[file_hash] = cache_file.stat().st_size
            except OSError:
                pass

    return sizes


def get_files_size(file_hashes: List[str]) -> int:
    """Get total size of files in cache.

    Args:
        file_hashes: List of MD5 hashes

    Returns:
        Total size in bytes (0 if cache not accessible)
    """
    return sum(get_file_sizes(file_hashes).values())


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
    sizes: Optional[Dict[str, int]] = None,
    verbose: bool = False,
) -> Dict[int, List[str]]:
    """Partition manifest files across workers, balancing bytes per worker.

    Uses greedy longest-processing-time (LPT) bin-packing: files are sorted
    largest-first and each is assigned to the currently least-loaded worker.
    This balances the total bytes per worker, so wave wall-clock approaches
    ``max(total_bytes / num_workers, largest_single_file) / per-worker-rate``
    instead of being dominated by whichever worker a hash-prefix split happened
    to hand the big files (see issue #138).

    The key partitioning invariant is preserved: each file is assigned to
    exactly one worker, so partitions are disjoint and workers never contend.
    Assignment is deterministic given the file sizes (ties broken by hash, then
    by lowest worker id).

    Args:
        manifest: Manifest from build_manifest()
        num_workers: Number of workers
        sizes: Optional precomputed hash->size map (bytes). If omitted, sizes
            are read from the cache. Missing/0-size blobs are treated as 0.
        verbose: Log the resulting per-worker byte balance.

    Returns:
        Dict mapping worker_id to list of file hashes
    """
    files = manifest['files']

    if sizes is None:
        sizes = get_file_sizes(files)

    partitions: Dict[int, List[str]] = {i: [] for i in range(num_workers)}
    loads = [0] * num_workers

    # Largest files first; tie-break by hash so ordering is reproducible
    # regardless of the manifest's file order.
    for file_hash in sorted(files, key=lambda h: (sizes.get(h, 0), h), reverse=True):
        # Assign to the least-loaded worker; tie-break by lowest worker id.
        worker_id = min(range(num_workers), key=lambda w: (loads[w], w))
        partitions[worker_id].append(file_hash)
        loads[worker_id] += sizes.get(file_hash, 0)

    if verbose:
        active = [load for load in loads if load > 0]
        if active:
            print(
                f"Partition byte balance across {len(active)} active worker(s): "
                f"min={utils.format_size(min(active))}, "
                f"max={utils.format_size(max(active))}, "
                f"total={utils.format_size(sum(loads))}"
            )

    return partitions


def _missing_from_remote(
    cache_odb: Any,
    remote_odb: Any,
    file_hashes: Set[str],
    verbose: bool = False,
) -> Set[str]:
    """Return the subset of ``file_hashes`` whose objects are not on the remote.

    Uses the same ``compare_status`` check ``build_manifest`` uses to decide
    what needs pushing, so the answer reflects the real remote state rather
    than the exit code of a prior transfer. The remote filesystem cache is
    invalidated first so existence checks issued right after a transfer see
    freshly-written objects. This is what lets us treat objects that landed
    despite a transient CompleteMultipartUpload error as successful (#141).
    """
    try:
        from dvc_data.hashfile.status import compare_status
        from dvc_data.hashfile.hash_info import HashInfo
    except ImportError as e:
        raise PushError(f"DVC internals not available: {e}")

    if not file_hashes:
        return set()

    # Drop any cached directory listing so a post-transfer existence check
    # does not return a stale "absent" answer for objects just written.
    try:
        remote_odb.fs.invalidate_cache()
    except Exception:
        pass

    obj_ids = [HashInfo('md5', h) for h in file_hashes]
    status = compare_status(
        cache_odb,
        remote_odb,
        obj_ids,
        check_deleted=False,
        shallow=True,
    )
    # status.new == present in cache but not on remote == still needs pushing.
    return {hi.value for hi in status.new}


def push_partition(
    file_hashes: Set[str],
    remote: Optional[str] = None,
    jobs: int = 1,
    verbose: bool = False,
    max_retries: int = DEFAULT_PUSH_RETRIES,
) -> Tuple[int, int]:
    """Push a partition of files using direct cache-to-remote transfer.

    This bypasses DVC's workspace index entirely, avoiding SQLite lock
    contention when running parallel workers. We transfer directly from
    the cache ODB to the remote ODB using the file hashes.

    If ``transfer`` reports any failures, the remote is queried directly to
    find which files genuinely did not land (transient multipart errors can
    fire even though the object was written, #141). Files still missing are
    re-transferred up to ``max_retries`` times with backoff, and the returned
    failure count reflects real remote absence rather than transfer exit codes.

    Args:
        file_hashes: Set of file hashes to push
        remote: Optional remote name
        jobs: Number of parallel upload threads
        verbose: Print progress
        max_retries: Times to re-attempt files missing from the remote

    Returns:
        Tuple of (pushed_count, failed_count), where failed_count is the
        number of files confirmed still missing from the remote.
    """
    try:
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

    total = len(file_hashes)
    remaining: Set[str] = set(file_hashes)
    attempt = 0

    while True:
        if verbose:
            label = "Transferring" if attempt == 0 else f"Retry {attempt}: transferring"
            print(f"{label} {len(remaining)} file(s)...")

        result = transfer(
            cache_odb,
            remote_odb,
            [HashInfo('md5', h) for h in remaining],
            jobs=jobs,
        )

        if not result.failed:
            # Transfer reports clean success; trust it (no extra remote
            # round-trips on the happy path).
            if verbose:
                print(f"Transferred: {total}, Failed: 0")
            return total, 0

        # Some transfers reported failure. A transient CompleteMultipartUpload /
        # NoSuchUpload error can fire even though the object landed, so check the
        # remote directly rather than trusting the reported failure count (#141).
        missing = _missing_from_remote(cache_odb, remote_odb, remaining, verbose=verbose)

        if not missing:
            if verbose:
                print(
                    f"All {len(remaining)} file(s) confirmed on remote despite "
                    f"{len(result.failed)} transfer error(s)"
                )
            return total, 0

        if attempt >= max_retries:
            if verbose:
                print(
                    f"{len(missing)} file(s) still missing from remote after "
                    f"{attempt} retr{'y' if attempt == 1 else 'ies'}"
                )
            return total - len(missing), len(missing)

        attempt += 1
        backoff = min(60, 5 * (2 ** (attempt - 1)))
        if verbose:
            print(
                f"{len(missing)} file(s) did not land; retrying in {backoff}s "
                f"(attempt {attempt}/{max_retries})..."
            )
        time.sleep(backoff)
        remaining = missing


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
    
    # Partition files (size-aware, balances bytes per worker)
    partitions = partition_manifest(manifest, num_workers, verbose=verbose)

    # Check how many workers actually have files
    active_workers = sum(1 for files in partitions.values() if files)
    if verbose:
        print(f"Partitioned {len(manifest['files'])} files across {active_workers} workers")
    
    # Save manifest
    job_id = str(uuid.uuid4())[:8]
    manifest_dir = hpc.save_manifest(manifest, partitions, job_id, operation='push')
    
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

        # Authoritative completion check: a worker's exit code can report
        # failure for objects that actually landed (transient multipart errors,
        # #141), so base success on what is genuinely still missing from the
        # remote rather than on per-job exit status.
        if verbose:
            print("Verifying remote contents...")
        remaining = build_manifest(targets=targets, remote=remote, verbose=False)
        n_remaining = len(remaining['files'])

        if n_remaining:
            raise PushError(
                f"{n_remaining} file(s) still missing from remote after push "
                f"(workers reported {'success' if success else 'failure'}). "
                f"Re-run the push to send the remainder."
            )

        if not success and verbose:
            print("Workers reported errors, but all files are present on the remote.")

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
    metadata, file_hashes = hpc.load_worker_partition(manifest_dir, worker_id)
    
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
