"""Workspace lock compatible with DVC's ``.dvc/tmp/lock``.

Provides a context manager that acquires the same lockfile DVC uses
internally, so concurrent ``dt`` and ``dvc`` invocations on the same
workspace serialise their mutating operations. Read-only commands
(``ls``/``find``/``du``/``history``/``status``) and ``dt push`` do
**not** take the lock — only operations that mutate the workspace,
``.dvc`` files, or cache should.

The implementation delegates to ``dvc.lock.make_lock`` (zc.lockfile)
so the lock is interoperable with DVC. The context manager is
reentrant within a single process: nested calls are no-ops and
release happens only when the outermost block exits.

Example
-------

    from dt import dvc_lock

    with dvc_lock.repo_lock():
        ...  # mutating work
"""

from __future__ import annotations

import subprocess
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from . import utils
from .errors import DTError


class WorkspaceLockError(DTError):
    """Raised when the DVC workspace lock cannot be acquired."""


# Per-thread reentrancy state. Lock identity is workspace root; nested
# entries with the *same* root are recursive no-ops, nested entries
# with a *different* root acquire a fresh lock.
_state = threading.local()


def _stack() -> list:
    s = getattr(_state, "stack", None)
    if s is None:
        s = []
        _state.stack = s
    return s


def _hardlink_lock_enabled(repo_root: Path) -> bool:
    """Honour ``core.hardlink_lock`` from the repo's DVC config."""
    try:
        r = subprocess.run(
            ["dvc", "config", "core.hardlink_lock"],
            capture_output=True,
            text=True,
            cwd=str(repo_root),
            check=False,
        )
    except (FileNotFoundError, OSError):
        return False
    if r.returncode != 0:
        return False
    return r.stdout.strip().lower() in ("true", "1", "yes", "on")


def _build_lock(repo_root: Path, wait: bool):
    from dvc.lock import make_lock

    tmp_dir = repo_root / ".dvc" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    return make_lock(
        str(tmp_dir / "lock"),
        tmp_dir=str(tmp_dir),
        friendly=True,
        hardlink_lock=_hardlink_lock_enabled(repo_root),
        wait=wait,
    )


@contextmanager
def repo_lock(
    repo_root: Optional[Path] = None,
    wait: bool = True,
) -> Iterator[Optional[Path]]:
    """Acquire ``<repo_root>/.dvc/tmp/lock`` for the duration of the block.

    Parameters
    ----------
    repo_root:
        Workspace root. If ``None``, discovered via ``utils.find_dvc_root``.
        If no DVC repo is found, the context manager is a no-op.
    wait:
        If ``True`` (default), block until the lock is available.
        If ``False``, retry a few times (DVC's default short loop) and
        then raise :class:`WorkspaceLockError` on failure.

    Yields
    ------
    The resolved repo root, or ``None`` if no DVC repo was detected.
    """
    if repo_root is None:
        repo_root = utils.find_dvc_root()
    if repo_root is None:
        yield None
        return

    repo_root = Path(repo_root)
    stack = _stack()
    if stack and stack[-1][0] == repo_root:
        # Reentrant acquisition for the same workspace: no-op
        stack.append((repo_root, None))
        try:
            yield repo_root
        finally:
            stack.pop()
        return

    try:
        from dvc.lock import LockError
    except ImportError as e:  # pragma: no cover - dvc is a hard dep
        raise WorkspaceLockError(f"DVC not importable: {e}") from e

    lock = _build_lock(repo_root, wait=wait)
    try:
        lock.lock()
    except LockError as e:
        raise WorkspaceLockError(
            f"Could not acquire DVC workspace lock at "
            f"{repo_root}/.dvc/tmp/lock: {e}"
        ) from e

    stack.append((repo_root, lock))
    try:
        yield repo_root
    finally:
        stack.pop()
        try:
            lock.unlock()
        except Exception:
            pass


@contextmanager
def maybe_lock(condition: bool, **kwargs) -> Iterator[Optional[Path]]:
    """Acquire :func:`repo_lock` only when ``condition`` is truthy."""
    if condition:
        with repo_lock(**kwargs) as root:
            yield root
    else:
        yield None
