"""Cache index for fast existence checks.

Maintains a lightweight SQLite index (via diskcache) of OIDs known to exist
in the DVC cache.  This avoids expensive per-file stat() calls on network
filesystems like Lustre, where metadata operations can dominate fetch time.

The index is stored at ``<cache_root>/.dt/cache.db/`` and is *per-cache*,
meaning every repo that shares the same cache shares the same index.

Staleness model
───────────────
The index is **advisory** — it can have false positives (file deleted by
``dvc gc``) or false negatives (file added by ``dvc add``).

* False positives → ``dt fetch`` skips a file it should re-fetch.
  Fix: ``dt fetch --force`` bypasses the index, or ``dt index cache rebuild``.

* False negatives → ``dt fetch`` tries to link, gets FileExistsError, then
  records the OID in the index (self-healing).

Locking
───────
SQLite (via diskcache) provides built-in file-level locking, safe for
concurrent readers and serialised writers.  We set a generous timeout
(120 s) to handle contention on shared caches.

Permissions
───────────
The index mirrors the permissions of the DVC cache it lives in (derived from
DVC's ``cache.shared`` config): a shared (group) cache yields a group
read/writable index — so every member who can write the cache can also
update the index — while a private cache keeps it private.
"""

import os
import stat as stat_mod
from pathlib import Path
from typing import Iterable, Optional, Set

import click


# Default SQLite timeout for concurrent access (seconds)
_DEFAULT_TIMEOUT = 120

# Subdirectory within cache root for our index
_INDEX_DIR = '.dt'
_INDEX_DB = 'cache.db'


class CacheIndex:
    """Fast OID existence index backed by SQLite (diskcache).

    Parameters
    ----------
    cache_root : Path
        Root of the DVC cache (the directory that contains ``files/md5/``).
    timeout : int, optional
        SQLite lock timeout in seconds.
    read_only : bool, optional
        If True, open the DB read-only (no writes, no auto-creation).
    """

    def __init__(
        self,
        cache_root: Path,
        timeout: int = _DEFAULT_TIMEOUT,
        read_only: bool = False,
    ) -> None:
        self._cache_root = Path(cache_root)
        self._read_only = read_only
        self._db_dir = self._cache_root / _INDEX_DIR / _INDEX_DB
        self._db = None
        self._timeout = timeout
        self._modes: Optional[tuple[int, int]] = None

    # -- Lazy open --------------------------------------------------------

    def _open(self):
        """Open (or create) the diskcache database."""
        if self._db is not None:
            return

        import diskcache

        if not self._read_only:
            self._db_dir.mkdir(parents=True, exist_ok=True)

        self._db = diskcache.Cache(
            str(self._db_dir),
            timeout=self._timeout,
            # Use DELETE journal mode instead of WAL for compatibility
            # with network filesystems (WAL requires shared-memory support).
            sqlite_journal_mode='delete',
        )

        # Match the index's permissions to the cache it lives in.  diskcache
        # only creates the SQLite file once the Cache is opened, so we apply
        # permissions here (after open) rather than before.
        if not self._read_only:
            self._apply_permissions()

    @property
    def db(self):
        """Access the underlying diskcache, opening lazily."""
        self._open()
        return self._db

    # -- Permissions ------------------------------------------------------

    def _cache_modes(self) -> tuple[int, int]:
        """Return ``(file_mode, dir_mode)`` to match this cache's own perms."""
        if self._modes is None:
            self._modes = _read_cache_modes(self._cache_root)
        return self._modes

    def _apply_permissions(self) -> None:
        """Match the index's permissions to the DVC cache it lives in.

        The diskcache store is a directory (``cache.db/``) containing a
        SQLite file.  We chmod the directories to the cache's directory mode
        and every file to the cache's file mode, so a shared (group) cache
        keeps the index group read/writable — letting every member who can
        write the cache also update the index — while a private cache keeps
        it private.  Failures (e.g. files owned by another user) are ignored.
        """
        if self._read_only:
            return
        file_mode, dir_mode = self._cache_modes()
        # Directories: .dt/ and .dt/cache.db/
        _chmod(self._db_dir.parent, dir_mode)
        _chmod(self._db_dir, dir_mode)
        # SQLite file(s) diskcache created inside cache.db/
        if self._db_dir.is_dir():
            for entry in self._db_dir.iterdir():
                if entry.is_file():
                    _chmod(entry, file_mode)

    # -- Core API ---------------------------------------------------------

    def contains(self, oid: str) -> bool:
        """Check whether *oid* is recorded in the index.  O(1)."""
        return oid in self.db

    def add(self, oid: str) -> None:
        """Record a single OID as present in cache."""
        if self._read_only:
            return
        self.db.set(oid, True)

    def add_many(self, oids: Iterable[str]) -> int:
        """Record multiple OIDs.  Returns count added."""
        if self._read_only:
            return 0
        n = 0
        for oid in oids:
            self.db.set(oid, True)
            n += 1
        return n

    def remove(self, oid: str) -> None:
        """Remove a single OID from the index."""
        if self._read_only:
            return
        try:
            del self.db[oid]
        except KeyError:
            pass

    def remove_many(self, oids: Iterable[str]) -> int:
        """Remove multiple OIDs.  Returns count removed."""
        if self._read_only:
            return 0
        n = 0
        for oid in oids:
            try:
                del self.db[oid]
                n += 1
            except KeyError:
                pass
        return n

    def clear(self) -> None:
        """Remove all entries from the index."""
        if self._read_only:
            return
        self.db.clear()

    def __len__(self) -> int:
        return len(self.db)

    def __contains__(self, oid: str) -> bool:
        return self.contains(oid)

    def oids(self) -> Set[str]:
        """Return all OIDs currently in the index."""
        return set(self.db.iterkeys())

    def close(self) -> None:
        """Close the database."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- Rebuild ----------------------------------------------------------

    def rebuild(
        self,
        verbose: bool = False,
        show_progress: bool = True,
    ) -> int:
        """Rebuild the index by scanning the cache filesystem.

        Walks both v3 (``files/md5/XX/…``) and v2 (``XX/…``) layouts.

        Returns the number of OIDs recorded.
        """
        if self._read_only:
            raise RuntimeError("Cannot rebuild a read-only index")

        self.clear()
        oids = _scan_cache_oids(self._cache_root, verbose=verbose)

        if show_progress:
            oids = list(oids)
            with click.progressbar(
                oids,
                label='Rebuilding cache index',
                show_pos=True,
                show_percent=True,
            ) as bar:
                for oid in bar:
                    self.db.set(oid, True)
        else:
            for oid in oids:
                self.db.set(oid, True)

        n = len(self.db)
        self._apply_permissions()
        return n

    # -- Info -------------------------------------------------------------

    def info(self) -> dict:
        """Return status information about the index."""
        exists = self._db_dir.exists()
        result = {
            'path': str(self._db_dir),
            'exists': exists,
            'cache_root': str(self._cache_root),
        }
        if exists:
            try:
                result['entries'] = len(self.db)
            except Exception as e:
                result['entries'] = None
                result['error'] = str(e)
        return result


# =========================================================================
# Filesystem scanning
# =========================================================================

def _scan_cache_oids(cache_root: Path, verbose: bool = False) -> list[str]:
    """Walk the cache tree and collect all OIDs.

    Returns a list of OID strings (``<full_hash>`` or ``<full_hash>.dir``).
    """
    oids: list[str] = []

    # v3 layout: files/md5/XX/YYYY…
    v3_base = cache_root / 'files' / 'md5'
    if v3_base.is_dir():
        if verbose:
            print(f"  Scanning v3 layout: {v3_base}")
        for prefix_dir in sorted(v3_base.iterdir()):
            if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
                continue
            # Only consider valid hex prefixes (00-ff)
            try:
                int(prefix_dir.name, 16)
            except ValueError:
                continue
            prefix = prefix_dir.name
            for entry in prefix_dir.iterdir():
                if entry.is_file() or entry.is_symlink():
                    name = entry.name
                    if name.endswith('.dir'):
                        oid = prefix + name[:-4] + '.dir'
                    else:
                        oid = prefix + name
                    oids.append(oid)

    # v2 layout: XX/YYYY… (directly under cache root)
    for prefix_dir in sorted(cache_root.iterdir()):
        if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
            continue
        # Only consider valid hex prefixes (00-ff)
        try:
            int(prefix_dir.name, 16)
        except ValueError:
            continue
        prefix = prefix_dir.name
        for entry in prefix_dir.iterdir():
            if entry.is_file() or entry.is_symlink():
                name = entry.name
                if name.endswith('.dir'):
                    oid = prefix + name[:-4] + '.dir'
                else:
                    oid = prefix + name
                oids.append(oid)

    if verbose:
        print(f"  Found {len(oids)} OIDs in cache")

    return oids


# =========================================================================
# Permissions helpers
# =========================================================================

def _chmod(path: Path, mode: int) -> None:
    """chmod *path* to *mode*, ignoring failures (e.g. not the owner)."""
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def _read_cache_modes(cache_root: Path) -> tuple[int, int]:
    """Return ``(file_mode, dir_mode)`` matching the DVC cache's own perms.

    DVC derives these modes from the ``cache.shared`` config (set via
    ``dvc config cache.shared group``): a shared cache uses ``0o664`` for
    files and ``0o2775`` for directories (group read/write, setgid on dirs)
    so any member of the group can update the cache; an unshared cache uses
    modes derived from the process umask.  We read the modes straight off the
    repo's local object database so the index always mirrors the cache it
    lives in.

    Falls back to umask-derived modes (matching DVC's own non-shared default)
    if the DVC repo can't be loaded.
    """
    try:
        from dvc.repo import Repo

        odb = Repo().cache.local
        return int(odb._file_mode), int(odb._dir_mode)
    except Exception:
        umask = os.umask(0)
        os.umask(umask)
        return 0o666 & ~umask, 0o777 & ~umask


# =========================================================================
# Convenience: open index for the current repo's cache
# =========================================================================

def open_index(read_only: bool = False) -> Optional[CacheIndex]:
    """Open the CacheIndex for the current repo's primary cache.

    Returns None if the cache is not configured.
    """
    from . import utils

    cache_dir = utils.get_cache_dir()
    if cache_dir is None:
        return None

    # cache_dir is typically .../files/md5 — we need the root
    cache_root = _cache_dir_to_root(cache_dir)
    return CacheIndex(cache_root, read_only=read_only)


def _cache_dir_to_root(cache_dir: Path) -> Path:
    """Convert a DVC cache path (``…/files/md5``) to the cache root."""
    p = Path(cache_dir)
    # v3 layout ends in files/md5
    if p.name == 'md5' and p.parent.name == 'files':
        return p.parent.parent
    return p
