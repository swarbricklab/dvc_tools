"""Show DVC status with index sync.

Wraps ``dvc status`` to provide a single-command view of pipeline/stage
changes (changed deps, outs, missing cache).  Optionally includes
import freshness via ``dt update --status``.
"""

import subprocess
from typing import List, Optional

from . import config as cfg
from . import utils
from .errors import StatusError


def status(
    targets: Optional[List[str]] = None,
    imports: bool = False,
    verbose: bool = False,
    dvc_args: Optional[List[str]] = None,
) -> int:
    """Run ``dvc status`` with optional index sync and import check.

    Args:
        targets: Specific targets to check.
        imports: If True, also run ``dt update --status`` to show stale imports.
        verbose: Print detailed progress.
        dvc_args: Additional arguments passed through to ``dvc status``.

    Returns:
        Exit code from ``dvc status``.

    Raises:
        StatusError: If the command fails unexpectedly.
    """
    utils.check_dvc()

    # Index sync before status
    try:
        from . import index as index_mod
        if index_mod.is_auto_sync_enabled():
            index_mod.pull(quiet=not verbose, verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"Warning: index sync failed: {e}")

    # Run dvc status
    cmd = ['dvc', 'status']
    if targets:
        cmd.extend(targets)
    if dvc_args:
        cmd.extend(dvc_args)

    if verbose:
        print(f"Running: {' '.join(cmd)}")

    result = subprocess.run(cmd)

    # Optionally show import status
    if imports:
        print()
        _show_import_status(verbose=verbose)

    # Index sync after status
    try:
        from . import index as index_mod
        if index_mod.is_auto_sync_enabled():
            index_mod.push(quiet=not verbose, verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"Warning: index sync failed: {e}")

    return result.returncode


def _show_import_status(verbose: bool = False) -> None:
    """Run ``dt update --status`` to display import freshness."""
    cmd = ['dt', 'update', '--status']
    if verbose:
        print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd)
