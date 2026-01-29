"""Add files to DVC tracking with configurable checksum parallelism.

Wraps `dvc add` with a --threads option to control checksum computation
parallelism. Can optionally submit the work to a compute node via qxub.
"""

import shutil
import subprocess
from pathlib import Path
from typing import List, Optional

from . import config as cfg


class AddError(Exception):
    """Error during add operation."""
    pass


# Default configuration
DEFAULT_MAX_THREADS = 48
DEFAULT_MEM_PER_THREAD = 4  # GB


def check_qxub() -> bool:
    """Check if qxub is available."""
    return shutil.which('qxub') is not None


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
) -> Optional[str]:
    """Add files to DVC tracking via qxub compute node.
    
    Submits the add operation to a compute node with appropriate
    resource allocation based on thread count.
    
    Args:
        targets: Files or directories to add.
        threads: Number of threads for checksum computation.
        dvc_args: Additional arguments to pass to dvc add.
        verbose: Print detailed progress.
        wait: Wait for job to complete.
        
    Returns:
        Job ID if submitted, None if run locally.
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
    
    # Calculate memory: threads * mem_per_thread GB
    total_mem = threads * mem_per_thread
    mem_str = f"{total_mem}GB"
    
    # Build qxub command
    cmd = [
        'qxub', 'exec', '--terse',
        '--env', conda_env,
        '--queue', queue,
        '--time', walltime,
        '--mem', mem_str,
        '--ncpus', str(threads),
        '-N', f'dt-add-{Path(targets[0]).name[:20]}',
    ]
    
    # Build dt add --worker command for the compute node
    # --worker flag tells dt add to run dvc add directly instead of submitting another job
    cmd.extend(['--', 'dt', 'add', '--worker', '--threads', str(threads)])
    if verbose:
        cmd.append('--verbose')
    if dvc_args:
        cmd.extend(dvc_args)
    cmd.extend(targets)
    
    if verbose:
        print(f"Submitting to compute node...")
        print(f"  Threads: {threads}")
        print(f"  Memory: {mem_str}")
        print(f"  Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            raise AddError(f"Failed to submit job: {result.stderr}")
        
        job_id = result.stdout.strip().split('\n')[0]
        
        if verbose:
            print(f"  Job ID: {job_id}")
        
        if wait:
            if verbose:
                print(f"Waiting for job to complete...")
            
            monitor_result = subprocess.run(
                ['qxub', 'monitor', '--summary', job_id],
            )
            
            if monitor_result.returncode != 0:
                raise AddError("Job failed")
        
        return job_id
        
    except subprocess.SubprocessError as e:
        raise AddError(f"Failed to submit job: {e}")
