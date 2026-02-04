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
from .errors import AddError


# Default configuration
# Node specs: 48 CPUs, 192 GB RAM, 4 threads per CPU = 192 max threads
DEFAULT_MAX_THREADS = 192
DEFAULT_MEM_PER_THREAD = 1  # GB (192 GB / 192 threads)
THREADS_PER_CPU = 4  # How many checksum jobs per CPU


def check_qxub() -> bool:
    """Check if qxub is available."""
    return shutil.which('qxub') is not None


def count_files(path: str) -> int:
    """Count the number of files in a path.
    
    For a file, returns 1. For a directory, counts all files recursively.
    
    Args:
        path: Path to file or directory.
        
    Returns:
        Number of files.
    """
    p = Path(path)
    if p.is_file():
        return 1
    elif p.is_dir():
        return sum(1 for f in p.rglob('*') if f.is_file())
    else:
        return 1  # Fallback for symlinks etc


def get_checksum_jobs() -> Optional[int]:
    """Get the current core.checksum_jobs setting.
    
    Returns:
        Current value or None if not set.
    """
    try:
        result = subprocess.run(
            ['dvc', 'config', 'core.checksum_jobs'],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip())
    except (subprocess.SubprocessError, ValueError):
        pass
    return None


def set_checksum_jobs(threads: int) -> None:
    """Set core.checksum_jobs at local scope.
    
    Args:
        threads: Number of threads for checksum computation.
    """
    subprocess.run(
        ['dvc', 'config', '--local', 'core.checksum_jobs', str(threads)],
        check=True,
        capture_output=True,
    )


def unset_checksum_jobs() -> None:
    """Unset core.checksum_jobs at local scope."""
    subprocess.run(
        ['dvc', 'config', '--local', '--unset', 'core.checksum_jobs'],
        capture_output=True,  # Don't fail if not set
    )


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
    
    # Set checksum_jobs if threads specified
    original_jobs = None
    if threads is not None:
        original_jobs = get_checksum_jobs()
        if verbose:
            print(f"Setting core.checksum_jobs={threads}")
        set_checksum_jobs(threads)
    
    try:
        if verbose:
            print(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(cmd)
        return result.returncode == 0
        
    finally:
        # Restore original setting
        if threads is not None:
            if verbose:
                print("Unsetting core.checksum_jobs")
            unset_checksum_jobs()
            # Restore original if there was one
            if original_jobs is not None:
                if verbose:
                    print(f"Restoring core.checksum_jobs={original_jobs}")
                set_checksum_jobs(original_jobs)


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
    
    # Get configuration
    max_threads = int(cfg.get_value('add.max_threads', str(DEFAULT_MAX_THREADS)))
    mem_per_thread = int(cfg.get_value('add.mem_per_thread', str(DEFAULT_MEM_PER_THREAD)))
    conda_env = cfg.get_value('qxub.env', 'dt')
    queue = cfg.get_value('qxub.queue', 'normal')  # Use normal queue for compute
    walltime = cfg.get_value('qxub.walltime', '10:00:00')
    
    # Default to max threads if not specified
    if threads is None:
        threads = max_threads
    
    # Validate thread count
    if threads < 1:
        raise AddError("Thread count must be at least 1")
    if threads > max_threads:
        raise AddError(f"Thread count {threads} exceeds maximum {max_threads}")
    
    # Count total files across all targets
    total_files = sum(count_files(t) for t in targets)
    
    # Cap threads to file count
    threads = min(threads, total_files)
    
    # Calculate CPUs: 1 CPU per THREADS_PER_CPU threads, minimum 1
    cpus = max(1, math.ceil(threads / THREADS_PER_CPU))
    
    # Calculate memory based on threads
    total_mem = threads * mem_per_thread
    mem_str = f"{total_mem}GB"
    
    # Job name from first target
    target_name = Path(targets[0]).name[:20]
    if len(targets) > 1:
        target_name = f"{target_name}+{len(targets)-1}"
    
    # Build qxub command
    cmd = [
        'qxub', 'exec',
        '--env', conda_env,
        '--queue', queue,
        '--time', walltime,
        '--mem', mem_str,
        '--cpus', str(cpus),
        '-N', f'dt-add-{target_name}',
    ]
    
    # Use --terse only for no-wait mode
    if not wait:
        cmd.insert(2, '--terse')
    
    # Build dt add --worker command
    cmd.extend(['--', 'dt', 'add', '--worker', '--threads', str(threads)])
    if verbose:
        cmd.append('--verbose')
    if dvc_args:
        cmd.extend(dvc_args)
    cmd.extend(targets)
    
    if verbose:
        print(f"Submitting job for {len(targets)} target(s)")
        print(f"  Files: {total_files}, Threads: {threads}, CPUs: {cpus}, Memory: {mem_str}")
    
    try:
        # Stream output when waiting, capture when not waiting
        result = subprocess.run(cmd, text=True, capture_output=not wait)
        
        if result.returncode != 0:
            if not wait:
                raise AddError(f"Failed to submit job: {result.stderr}")
            else:
                raise AddError("Job failed")
        
        # Return job ID if not waiting
        if not wait:
            job_id = result.stdout.strip().split('\n')[0]
            return [job_id]
        
        return None
        
    except subprocess.SubprocessError as e:
        raise AddError(f"Failed to run job: {e}")
