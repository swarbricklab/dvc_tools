"""Show DVC data status with configurable checksum parallelism.

Wraps ``dvc data status`` with a ``--threads`` option to control checksum
computation parallelism.  Can optionally submit the work to a compute
node via qxub, using the same resource allocations as ``dt add``.
"""

import subprocess
from typing import List, Optional

from . import config as cfg
from . import dvc_utils
from .errors import DataStatusError


def data_status(
    threads: Optional[int] = None,
    dvc_args: Optional[List[str]] = None,
    verbose: bool = False,
) -> int:
    """Run ``dvc data status`` with optional checksum parallelism.

    Args:
        threads: Number of threads for checksum computation.
        dvc_args: Additional arguments to pass to ``dvc data status``.
        verbose: Print detailed progress.

    Returns:
        The exit code from ``dvc data status``.
    """
    max_threads = int(
        cfg.get_value('add.max_threads', str(dvc_utils.DEFAULT_MAX_THREADS))
    )

    if threads is not None:
        if threads < 1:
            raise DataStatusError("Thread count must be at least 1")
        if threads > max_threads:
            raise DataStatusError(
                f"Thread count {threads} exceeds maximum {max_threads}"
            )

    cmd = ['dvc', 'data', 'status']
    if dvc_args:
        cmd.extend(dvc_args)

    with dvc_utils.with_checksum_jobs(threads, verbose=verbose):
        if verbose:
            print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd)
        return result.returncode


def data_status_via_qxub(
    threads: Optional[int] = None,
    dvc_args: Optional[List[str]] = None,
    verbose: bool = False,
    wait: bool = True,
    no_index_sync: bool = False,
) -> Optional[str]:
    """Run ``dvc data status`` on a compute node via qxub.

    Uses the same resource allocations as ``dt add`` (``add.max_threads``,
    ``add.mem_per_thread``).

    Args:
        threads: Max threads for checksum computation.
        dvc_args: Additional arguments to pass to ``dvc data status``.
        verbose: Print detailed progress.
        wait: Wait for job to complete.
        no_index_sync: Skip automatic index mirror sync on the worker.

    Returns:
        Job ID string when *wait* is ``False``, ``None`` otherwise.
    """
    if not dvc_utils.check_qxub():
        raise DataStatusError(
            "qxub not found. Install from "
            "https://github.com/swarbricklab/qxub"
        )

    # Validate thread count
    max_threads = int(
        cfg.get_value('add.max_threads', str(dvc_utils.DEFAULT_MAX_THREADS))
    )
    if threads is not None:
        if threads < 1:
            raise DataStatusError("Thread count must be at least 1")
        if threads > max_threads:
            raise DataStatusError(
                f"Thread count {threads} exceeds maximum {max_threads}"
            )

    # Build worker command
    worker_cmd = ['dt', 'data', 'status', '--worker']
    if no_index_sync:
        worker_cmd.append('--no-index-sync')
    if threads is not None:
        worker_cmd.extend(['--threads', str(threads)])
    if verbose:
        worker_cmd.append('--verbose')
    if dvc_args:
        worker_cmd.extend(dvc_args)

    if verbose:
        print("Submitting dvc data status job")

    job_id = dvc_utils.submit_via_qxub(
        job_name='dt-data-status',
        worker_cmd=worker_cmd,
        threads=threads,
        config_prefix='add',
        verbose=verbose,
        wait=wait,
        error_class=DataStatusError,
    )

    return job_id
