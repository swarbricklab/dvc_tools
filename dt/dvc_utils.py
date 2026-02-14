"""Shared utilities for DVC wrapper commands.

Provides common infrastructure used by ``dt add``, ``dt data status``,
and other commands that wrap DVC operations with configurable checksum
parallelism and optional qxub delegation.
"""

import math
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional

from . import config as cfg


# Default configuration
# Node specs: 48 CPUs, 192 GB RAM, 4 threads per CPU = 192 max threads
DEFAULT_MAX_THREADS = 192
DEFAULT_MEM_PER_THREAD = 1  # GB (192 GB / 192 threads)
THREADS_PER_CPU = 4  # How many checksum jobs per CPU


# ── checksum_jobs helpers ────────────────────────────────────────────

def get_checksum_jobs() -> Optional[int]:
    """Get the current ``core.checksum_jobs`` setting.

    Returns:
        Current value or ``None`` if not set.
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
    """Set ``core.checksum_jobs`` at local scope.

    Args:
        threads: Number of threads for checksum computation.
    """
    subprocess.run(
        ['dvc', 'config', '--local', 'core.checksum_jobs', str(threads)],
        check=True,
        capture_output=True,
    )


def unset_checksum_jobs() -> None:
    """Unset ``core.checksum_jobs`` at local scope."""
    subprocess.run(
        ['dvc', 'config', '--local', '--unset', 'core.checksum_jobs'],
        capture_output=True,  # Don't fail if not set
    )


def with_checksum_jobs(
    threads: Optional[int],
    verbose: bool = False,
):
    """Context manager to temporarily set ``core.checksum_jobs``.

    Sets the value before yielding and restores the original value
    (or unsets it) afterwards.

    Args:
        threads: Thread count to set, or ``None`` to skip.
        verbose: Print what's happening.

    Yields:
        The effective thread count (may be ``None``).
    """
    import contextlib

    @contextlib.contextmanager
    def _ctx():
        if threads is None:
            yield None
            return

        original = get_checksum_jobs()
        if verbose:
            print(f"Setting core.checksum_jobs={threads}")
        set_checksum_jobs(threads)
        try:
            yield threads
        finally:
            if verbose:
                print("Unsetting core.checksum_jobs")
            unset_checksum_jobs()
            if original is not None:
                if verbose:
                    print(f"Restoring core.checksum_jobs={original}")
                set_checksum_jobs(original)

    return _ctx()


# ── resource calculation ─────────────────────────────────────────────

def count_files(path: str) -> int:
    """Count the number of files in a path.

    For a file, returns 1.  For a directory, counts all files recursively.

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


def calculate_resources(
    threads: Optional[int],
    file_count: int = 0,
    config_prefix: str = 'add',
) -> dict:
    """Calculate HPC resource requirements.

    Args:
        threads: Requested thread count (``None`` → use max).
        file_count: Total files (threads are capped to this).
        config_prefix: Config key prefix for ``max_threads`` /
            ``mem_per_thread`` (e.g. ``'add'``).

    Returns:
        Dict with ``threads``, ``cpus``, ``mem_gb``, ``mem_str``.
    """
    max_threads = int(
        cfg.get_value(f'{config_prefix}.max_threads', str(DEFAULT_MAX_THREADS))
    )
    mem_per_thread = int(
        cfg.get_value(f'{config_prefix}.mem_per_thread', str(DEFAULT_MEM_PER_THREAD))
    )

    if threads is None:
        threads = max_threads

    # Cap to file count when known
    if file_count > 0:
        threads = min(threads, file_count)

    cpus = max(1, math.ceil(threads / THREADS_PER_CPU))
    mem_gb = threads * mem_per_thread

    return {
        'threads': threads,
        'cpus': cpus,
        'mem_gb': mem_gb,
        'mem_str': f'{mem_gb}GB',
    }


# ── qxub helpers ─────────────────────────────────────────────────────

def check_qxub() -> bool:
    """Check if qxub is available in PATH."""
    return shutil.which('qxub') is not None


def submit_via_qxub(
    *,
    job_name: str,
    worker_cmd: List[str],
    threads: Optional[int] = None,
    file_count: int = 0,
    config_prefix: str = 'add',
    verbose: bool = False,
    wait: bool = True,
    error_class: type = RuntimeError,
) -> Optional[str]:
    """Submit a DVC wrapper command to a compute node via qxub.

    Calculates resource requirements, builds the qxub command, and either
    waits for the job or returns the job ID.

    Args:
        job_name: PBS job name.
        worker_cmd: The ``dt ... --worker`` command to execute.
        threads: Requested thread count.
        file_count: Total files (for capping threads).
        config_prefix: Config prefix for resource settings.
        verbose: Show progress.
        wait: Block until the job finishes.
        error_class: Exception class to raise on failure.

    Returns:
        Job ID string when *wait* is ``False``, ``None`` otherwise.
    """
    if not check_qxub():
        raise error_class(
            "qxub not found. Install from "
            "https://github.com/swarbricklab/qxub"
        )

    res = calculate_resources(
        threads, file_count=file_count, config_prefix=config_prefix,
    )

    conda_env = cfg.get_value('qxub.env', 'dt')
    queue = cfg.get_value('qxub.queue', 'normal')
    walltime = cfg.get_value('qxub.walltime', '10:00:00')

    cmd: List[str] = [
        'qxub', 'exec',
        '--env', conda_env,
        '--queue', queue,
        '--time', walltime,
        '--mem', res['mem_str'],
        '--cpus', str(res['cpus']),
        '-N', job_name,
    ]

    if not wait:
        cmd.insert(2, '--terse')

    cmd.append('--')
    cmd.extend(worker_cmd)

    if verbose:
        print(f"  Threads: {res['threads']}, CPUs: {res['cpus']}, "
              f"Memory: {res['mem_str']}")

    try:
        result = subprocess.run(cmd, text=True, capture_output=not wait)

        if result.returncode != 0:
            if not wait:
                raise error_class(f"Failed to submit job: {result.stderr}")
            else:
                raise error_class("Job failed")

        if not wait:
            return result.stdout.strip().split('\n')[0]
        return None

    except subprocess.SubprocessError as e:
        raise error_class(f"Failed to run job: {e}")
