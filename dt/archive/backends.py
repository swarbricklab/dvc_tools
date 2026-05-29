"""Archive backends.

A backend is a tiny abstraction over "ship a file to cold storage and
ask about it later". Each backend implements the same protocol so the
rest of ``dt.archive`` doesn't care whether the target is MDSS, a local
cold-storage directory, S3 Glacier, rclone, etc.

The first PR ships:

- ``MdssBackend``: NCI tape via the ``mdss`` CLI.
- ``LocalDirBackend``: copies files to a local directory. Used for
  tests and as a fallback when developing on non-NCI machines.

The archive layout is *folder-per-archive*: every inner tarball is
uploaded as its own object inside a directory on the backend, plus a
manifest sidecar uploaded last as a completion sentinel. That replaces
the older single-outer-tar design — partial uploads survive walltime
boundaries, multiple uploads can run in parallel, and MDSS gets
medium-sized files instead of one multi-TB monolith.

To add a backend, subclass :class:`ArchiveBackend` and call
:func:`register_backend`.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Protocol

from ..errors import ArchiveError


# --------------------------------------------------------------------------- #
# Protocol
# --------------------------------------------------------------------------- #

class ArchiveBackend(Protocol):
    """The minimal surface every archive backend must implement."""

    name: str

    def put_file(self, local_path: Path, remote_path: str) -> None:
        """Upload an on-disk file at ``local_path`` to ``remote_path``.

        Implementations must raise :class:`ArchiveError` on failure.
        """
        ...

    def get_file(self, remote_path: str, local_path: Path) -> None:
        """Download ``remote_path`` to ``local_path``."""
        ...

    def exists(self, remote_path: str) -> bool:
        """Whether ``remote_path`` exists on the backend."""
        ...

    def stat(self, remote_path: str) -> Dict[str, int]:
        """Return at least ``{'size_bytes': int}`` for ``remote_path``."""
        ...

    def list_dir(self, remote_dir: str) -> List[str]:
        """List filenames (not full paths) at ``remote_dir``."""
        ...


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

_BACKENDS: Dict[str, type] = {}


def register_backend(name: str, cls: type) -> None:
    """Register a backend class under ``name``.

    Idempotent: re-registering the same (name, cls) pair is a no-op.
    Re-registering a different class for the same name raises.
    """
    existing = _BACKENDS.get(name)
    if existing is not None and existing is not cls:
        raise ArchiveError(
            f"Backend '{name}' is already registered as {existing.__name__}"
        )
    _BACKENDS[name] = cls


def get_backend(name: str, **kwargs) -> ArchiveBackend:
    """Instantiate a registered backend by name.

    ``kwargs`` are forwarded to the backend constructor.
    """
    if name not in _BACKENDS:
        known = ', '.join(sorted(_BACKENDS)) or '(none registered)'
        raise ArchiveError(
            f"Unknown archive backend: '{name}'. Known: {known}"
        )
    return _BACKENDS[name](**kwargs)


def known_backends() -> list:
    """List registered backend names."""
    return sorted(_BACKENDS)


# --------------------------------------------------------------------------- #
# MdssBackend
# --------------------------------------------------------------------------- #

class MdssBackend:
    """NCI MDSS tape archive backend.

    Talks to the ``mdss`` CLI. ``mdss`` only works on data-mover nodes
    (e.g. ``gadi-dm.nci.org.au``), and most operations route via the
    user's MDSS project space (``/massdata/<project>/...``); a leading
    ``/`` is preserved if the caller supplied an absolute path.

    Paths are interpreted relative to the user's MDSS home unless they
    start with ``/``.
    """

    name = 'mdss'

    def __init__(self, mdss_bin: str = 'mdss') -> None:
        self._bin = mdss_bin

    # -- helpers --------------------------------------------------------- #

    def _run(self, args: list, **kwargs) -> subprocess.CompletedProcess:
        cmd = [self._bin] + args
        try:
            return subprocess.run(cmd, capture_output=True, text=True, **kwargs)
        except FileNotFoundError as e:
            raise ArchiveError(
                f"mdss command not found ({self._bin}). "
                f"Are you on an NCI data-mover node?"
            ) from e

    @staticmethod
    def _ensure(result: subprocess.CompletedProcess, what: str) -> None:
        if result.returncode != 0:
            stderr = (result.stderr or '').strip()
            stdout = (result.stdout or '').strip()
            detail = stderr or stdout or '(no output)'
            raise ArchiveError(f"mdss {what} failed: {detail}")

    # -- protocol -------------------------------------------------------- #

    def put_file(self, local_path: Path, remote_path: str) -> None:
        self._ensure_parent_dir(remote_path)
        result = self._run(['put', str(local_path), remote_path])
        self._ensure(result, f"put {local_path} -> {remote_path}")

    def get_file(self, remote_path: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        result = self._run(['get', remote_path, str(local_path)])
        self._ensure(result, f"get {remote_path} -> {local_path}")

    def exists(self, remote_path: str) -> bool:
        result = self._run(['ls', remote_path])
        return result.returncode == 0

    def stat(self, remote_path: str) -> Dict[str, int]:
        # `mdss ls -l <path>` outputs ls-style metadata.
        result = self._run(['ls', '-l', remote_path])
        self._ensure(result, f"stat {remote_path}")
        line = (result.stdout or '').strip().split('\n')[-1]
        parts = line.split()
        # Expected layout: perms links owner group size <date...> name
        if len(parts) < 5:
            raise ArchiveError(
                f"Could not parse mdss ls -l output for {remote_path}: "
                f"{result.stdout!r}"
            )
        try:
            size = int(parts[4])
        except ValueError as e:
            raise ArchiveError(
                f"Could not parse size from mdss ls -l output: {parts!r}"
            ) from e
        return {'size_bytes': size}

    def list_dir(self, remote_dir: str) -> List[str]:
        result = self._run(['ls', remote_dir])
        if result.returncode != 0:
            return []
        names = []
        for line in (result.stdout or '').splitlines():
            line = line.strip()
            if not line:
                continue
            # mdss ls returns one filename per line — strip any trailing /
            names.append(line.rstrip('/').split('/')[-1])
        return names

    # -- internals ------------------------------------------------------- #

    def _ensure_parent_dir(self, remote_path: str) -> None:
        # mdss put refuses if the parent doesn't exist; mkdir -p is the
        # right shape but mdss uses `mkdir`. Use `-p` if supported, else
        # tolerate "exists" errors.
        parent = remote_path.rsplit('/', 1)[0] if '/' in remote_path else ''
        if not parent:
            return
        result = self._run(['mkdir', '-p', parent])
        # Some mdss installs don't support -p; fall back to plain mkdir
        # and ignore "already exists" failures.
        if result.returncode != 0 and '-p' in (result.stderr or ''):
            result = self._run(['mkdir', parent])
            if result.returncode != 0:
                err = (result.stderr or '').lower()
                if 'exist' not in err:
                    self._ensure(result, f"mkdir {parent}")


# --------------------------------------------------------------------------- #
# LocalDirBackend (test/dev)
# --------------------------------------------------------------------------- #

class LocalDirBackend:
    """Archive backend that writes to a local directory.

    Used by the test suite and as a fallback for development on
    non-NCI machines (e.g. to validate the tar/manifest pipeline end-to-end
    without involving tape).
    """

    name = 'local'

    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, remote_path: str) -> Path:
        # Treat all remote_paths as relative to ``root`` so tests can't
        # escape via leading slashes.
        return self.root / remote_path.lstrip('/')

    def put_file(self, local_path: Path, remote_path: str) -> None:
        target = self._resolve(remote_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)

    def get_file(self, remote_path: str, local_path: Path) -> None:
        source = self._resolve(remote_path)
        if not source.exists():
            raise ArchiveError(f"Not found in local backend: {remote_path}")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, local_path)

    def exists(self, remote_path: str) -> bool:
        return self._resolve(remote_path).exists()

    def stat(self, remote_path: str) -> Dict[str, int]:
        p = self._resolve(remote_path)
        if not p.exists():
            raise ArchiveError(f"Not found in local backend: {remote_path}")
        return {'size_bytes': p.stat().st_size}

    def list_dir(self, remote_dir: str) -> List[str]:
        p = self._resolve(remote_dir)
        if not p.is_dir():
            return []
        return sorted(child.name for child in p.iterdir() if child.is_file())


# --------------------------------------------------------------------------- #
# Registration
# --------------------------------------------------------------------------- #

register_backend('mdss', MdssBackend)
register_backend('local', LocalDirBackend)


# --------------------------------------------------------------------------- #
# Convenience hashing helper used by verify/restore
# --------------------------------------------------------------------------- #

def sha256_of_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """sha256 of an on-disk file."""
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
