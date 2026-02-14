"""Add files to DVC tracking with configurable checksum parallelism.

Wraps `dvc add` with a --threads option to control checksum computation
parallelism. Can optionally submit the work to a compute node via qxub.
"""

import math
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional

from . import config as cfg
from . import dvc_utils
from .errors import AddError


# Default configuration — re-exported for backward compatibility
DEFAULT_MAX_THREADS = dvc_utils.DEFAULT_MAX_THREADS
DEFAULT_MEM_PER_THREAD = dvc_utils.DEFAULT_MEM_PER_THREAD
THREADS_PER_CPU = dvc_utils.THREADS_PER_CPU


def check_qxub() -> bool:
    """Check if qxub is available."""
    return dvc_utils.check_qxub()


def count_files(path: str) -> int:
    """Count the number of files in a path.
    
    For a file, returns 1. For a directory, counts all files recursively.
    
    Args:
        path: Path to file or directory.
        
    Returns:
        Number of files.
    """
    return dvc_utils.count_files(path)


def get_checksum_jobs() -> Optional[int]:
    """Get the current core.checksum_jobs setting.
    
    Returns:
        Current value or None if not set.
    """
    return dvc_utils.get_checksum_jobs()


def set_checksum_jobs(threads: int) -> None:
    """Set core.checksum_jobs at local scope.
    
    Args:
        threads: Number of threads for checksum computation.
    """
    dvc_utils.set_checksum_jobs(threads)


def unset_checksum_jobs() -> None:
    """Unset core.checksum_jobs at local scope."""
    dvc_utils.unset_checksum_jobs()


def add(
    targets: List[str],
    threads: Optional[int] = None,
    dvc_args: Optional[List[str]] = None,
    verbose: bool = False,
) -> bool:
    """Add files to DVC tracking.
    
    Args:
        targets: Files or directories to add.
        threads: Number of threads for checksum computation.
        dvc_args: Additional arguments to pass to dvc add.
        verbose: Print detailed progress.
        
    Returns:
        True if successful.
    """
    if not targets:
        raise AddError("No targets specified")
    
    # Get max threads from config
    max_threads = int(cfg.get_value('add.max_threads', str(DEFAULT_MAX_THREADS)))
    
    # Validate thread count
    if threads is not None:
        if threads < 1:
            raise AddError("Thread count must be at least 1")
        if threads > max_threads:
            raise AddError(f"Thread count {threads} exceeds maximum {max_threads}")
    
    # Build command
    cmd = ['dvc', 'add']
    if dvc_args:
        cmd.extend(dvc_args)
    cmd.extend(targets)
    
    with dvc_utils.with_checksum_jobs(threads, verbose=verbose):
        if verbose:
            print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd)
        return result.returncode == 0


def add_via_qxub(
    targets: List[str],
    threads: Optional[int] = None,
    dvc_args: Optional[List[str]] = None,
    verbose: bool = False,
    wait: bool = True,
) -> Optional[List[str]]:
    """Add files to DVC tracking via qxub compute node.
    
    Submits a single job to add all targets. DVC uses a lock file so
    parallel jobs would cause lock contention.
    
    Threads are capped to the total number of files across all targets,
    and CPUs are allocated at 1 CPU per 4 threads (checksum jobs).
    
    Args:
        targets: Files or directories to add.
        threads: Max threads for checksum computation.
        dvc_args: Additional arguments to pass to dvc add.
        verbose: Print detailed progress.
        wait: Wait for job to complete.
        
    Returns:
        List containing job ID if not waiting, None if waiting.
    """
    if not check_qxub():
        raise AddError("qxub not found. Install from https://github.com/swarbricklab/qxub")
    
    if not targets:
        raise AddError("No targets specified")
    
    # Count total files across all targets
    total_files = sum(count_files(t) for t in targets)

    # Validate thread count
    max_threads = int(cfg.get_value('add.max_threads', str(DEFAULT_MAX_THREADS)))
    if threads is not None:
        if threads < 1:
            raise AddError("Thread count must be at least 1")
        if threads > max_threads:
            raise AddError(f"Thread count {threads} exceeds maximum {max_threads}")

    # Calculate resources
    res = dvc_utils.calculate_resources(
        threads, file_count=total_files, config_prefix='add',
    )
    
    # Job name from first target
    target_name = Path(targets[0]).name[:20]
    if len(targets) > 1:
        target_name = f"{target_name}+{len(targets)-1}"
    
    # Build worker command
    worker_cmd = ['dt', 'add', '--worker', '--threads', str(res['threads'])]
    if verbose:
        worker_cmd.append('--verbose')
    if dvc_args:
        worker_cmd.extend(dvc_args)
    worker_cmd.extend(targets)

    if verbose:
        print(f"Submitting job for {len(targets)} target(s)")
        print(f"  Files: {total_files}, Threads: {res['threads']}, "
              f"CPUs: {res['cpus']}, Memory: {res['mem_str']}")

    job_id = dvc_utils.submit_via_qxub(
        job_name=f'dt-add-{target_name}',
        worker_cmd=worker_cmd,
        threads=threads,
        file_count=total_files,
        config_prefix='add',
        verbose=False,  # Already printed above
        wait=wait,
        error_class=AddError,
    )

    if not wait and job_id:
        return [job_id]
    return None
