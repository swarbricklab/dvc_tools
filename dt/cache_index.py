"""Cache index for fast existence checks.

Maintains a lightweight SQLite index of OIDs known to exist in the DVC
cache.  This avoids expensive per-file stat() calls on network
filesystems like Lustre, where metadata operations can dominate fetch time.

The index is **per user and per cache**: it lives in the user's own cache
directory (``${XDG_CACHE_HOME:-~/.cache}/dt/oid-index/<hash>.sqlite``, keyed
by the cache's real path), never inside the DVC cache itself.  A shared DVC
cache is writable by other users, and SQLite cannot be opened read-write
safely in a directory others can write: a planted journal file can make
SQLite's crash recovery delete arbitrary files.  Keeping the index private
avoids that entirely.  The store is also kept inert — a single table of
plain OID strings, schema code disabled — and any problem with it falls
back to an empty in-memory index, since the index is only advisory.

Staleness model
───────────────
The index is **advisory** — it can have false positives (file deleted by
``dvc gc``) or false negatives (file added by ``dvc add`` or another user's
fetch).

* False positives → ``dt fetch`` skips a file it should re-fetch.
  Fix: ``dt fetch --force`` bypasses the index, or ``dt index cache rebuild``.

* False negatives → ``dt fetch`` tries to link, gets FileExistsError, then
  records the OID in the index (self-healing).
"""

import hashlib
import os
import sqlite3
import stat as stat_mod
from pathlib import Path
from typing import Iterable, Optional, Set

import click


# SQLite busy timeout (seconds); only the user's own processes contend.
_DEFAULT_TIMEOUT = 30

# Buffered adds are written in batches of this size (and on close).
_FLUSH_EVERY = 1000

_SCHEMA = 'CREATE TABLE IF NOT EXISTS oids (oid TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID'


def _candidate_dirs() -> list:
    """Index dir candidates, in order: $XDG_CACHE_HOME, then ~/.cache."""
    bases = []
    if os.environ.get('XDG_CACHE_HOME'):
        bases.append(os.environ['XDG_CACHE_HOME'])
    bases.append(os.path.join(os.path.expanduser('~'), '.cache'))
    out = []
    for b in bases:
        d = Path(b) / 'dt' / 'oid-index'
        if d not in out:
            out.append(d)
    return out


def _index_dir() -> Path:
    return _candidate_dirs()[0]


def _cache_key(cache_root: Path) -> str:
    return hashlib.sha256(os.path.realpath(str(cache_root)).encode()).hexdigest()[:32]


def index_path_for(cache_root: Path) -> Path:
    """Preferred per-user index file for the DVC cache at ``cache_root``."""
    return _index_dir() / f'{_cache_key(cache_root)}.sqlite'


class CacheIndex:
    """Fast OID existence index backed by a private SQLite table.

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
        self._db_path = index_path_for(self._cache_root)
        self._db: Optional[sqlite3.Connection] = None
        self._timeout = timeout
        self._pending: Set[str] = set()
        self._known: Optional[Set[str]] = None  # loaded lazily for fast lookups
        self._fallen_back = False

    @property
    def path(self) -> Path:
        return self._db_path

    # -- Open / fallback ---------------------------------------------------

    def _open(self):
        if self._db is not None:
            return
        try:
            self._db = self._connect()
        except (sqlite3.Error, OSError, _UnsafeIndex) as e:
            self._fall_back(e)

    def _fall_back(self, err) -> None:
        """Switch to an empty in-memory index (the index is advisory)."""
        if not self._fallen_back:
            click.echo(f"warning: not using cache index {self._db_path}: {err}", err=True)
        self._fallen_back = True
        if self._db is not None:
            try:
                self._db.close()
            except sqlite3.Error:
                pass
        self._db = sqlite3.connect(':memory:', isolation_level=None)
        self._db.execute(_SCHEMA)
        self._known = None

    def _connect(self) -> sqlite3.Connection:
        errors = []
        for d in _candidate_dirs():
            try:
                _ensure_private_dir(d, create=not self._read_only)
            except FileNotFoundError:
                if self._read_only:
                    continue
                raise
            except (OSError, _UnsafeIndex) as e:
                errors.append(f"{d}: {e}")
                continue
            self._db_path = d / f'{_cache_key(self._cache_root)}.sqlite'
            break
        else:
            if self._read_only and not errors:
                conn = sqlite3.connect(':memory:', isolation_level=None)
                conn.execute(_SCHEMA)
                return conn
            raise _UnsafeIndex('; '.join(errors) or 'no usable index directory')
        path = self._db_path
        if self._read_only and not os.path.lexists(path):
            conn = sqlite3.connect(':memory:', isolation_level=None)
            conn.execute(_SCHEMA)
            return conn
        _require_private_file_or_missing(path)
        if self._read_only:
            conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True,
                                   timeout=self._timeout, isolation_level=None)
        else:
            old_umask = os.umask(0o077)
            try:
                conn = sqlite3.connect(str(path), timeout=self._timeout,
                                       isolation_level=None)
            finally:
                os.umask(old_umask)
        try:
            try:
                conn.execute('PRAGMA trusted_schema=OFF')
            except sqlite3.Error:
                pass
            if not self._read_only:
                conn.execute('PRAGMA synchronous=OFF')  # advisory data
                conn.execute(_SCHEMA)
            _require_expected_schema(conn)
        except Exception:
            conn.close()
            raise
        return conn

    def _run(self, fn):
        """Run ``fn(conn)``; on any SQLite error fall back and retry once."""
        self._open()
        try:
            return fn(self._db)
        except sqlite3.Error as e:
            self._fall_back(e)
            return fn(self._db)

    @property
    def db(self) -> sqlite3.Connection:
        """Access the underlying SQLite connection, opening lazily."""
        self._open()
        return self._db

    # -- Core API ---------------------------------------------------------

    def contains(self, oid: str) -> bool:
        """Check whether *oid* is recorded in the index."""
        oid = str(oid)
        if oid in self._pending:
            return True
        if self._known is None:
            # One query instead of a locked round-trip per lookup.
            rows = self._run(lambda c: c.execute('SELECT oid FROM oids').fetchall())
            self._known = {r[0] for r in rows if isinstance(r[0], str)}
        return oid in self._known

    def add(self, oid: str) -> None:
        """Record a single OID as present in cache (buffered)."""
        if self._read_only:
            return
        self._pending.add(str(oid))
        if len(self._pending) >= _FLUSH_EVERY:
            self.flush()

    def add_many(self, oids: Iterable[str]) -> int:
        """Record multiple OIDs.  Returns count added."""
        if self._read_only:
            return 0
        rows = [str(o) for o in oids]
        self._pending.update(rows)
        self.flush()
        return len(rows)

    def flush(self) -> None:
        """Write buffered adds."""
        if self._read_only or not self._pending:
            return
        rows = [(o,) for o in self._pending]

        def write(c):
            with _transaction(c):
                c.executemany('INSERT OR IGNORE INTO oids (oid) VALUES (?)', rows)

        self._run(write)
        if self._known is not None:
            self._known.update(self._pending)
        self._pending.clear()

    def remove(self, oid: str) -> None:
        """Remove a single OID from the index."""
        self.remove_many([oid])

    def remove_many(self, oids: Iterable[str]) -> int:
        """Remove multiple OIDs.  Returns count removed."""
        if self._read_only:
            return 0
        oids = [str(o) for o in oids]
        self._pending.difference_update(oids)
        if self._known is not None:
            self._known.difference_update(oids)

        def delete(c):
            n = 0
            with _transaction(c):
                for oid in oids:
                    n += c.execute('DELETE FROM oids WHERE oid = ?', (oid,)).rowcount
            return n

        return self._run(delete)

    def clear(self) -> None:
        """Remove all entries from the index."""
        if self._read_only:
            return
        self._pending.clear()
        self._known = None
        self._run(lambda c: c.execute('DELETE FROM oids'))

    def __len__(self) -> int:
        self.flush()
        return int(self._run(lambda c: c.execute('SELECT count(*) FROM oids').fetchone()[0]))

    def __contains__(self, oid: str) -> bool:
        return self.contains(oid)

    def oids(self) -> Set[str]:
        """Return all OIDs currently in the index."""
        self.flush()
        rows = self._run(lambda c: c.execute('SELECT oid FROM oids').fetchall())
        return {r[0] for r in rows if isinstance(r[0], str)}

    def close(self) -> None:
        """Flush buffered adds and close the database."""
        try:
            self.flush()
        finally:
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

        Walks both v3 (``files/md5/XX/…``) and v2 (``XX/…``) layouts.  The
        index file is recreated from scratch, which also repairs corruption.

        Returns the number of OIDs recorded.
        """
        if self._read_only:
            raise RuntimeError("Cannot rebuild a read-only index")

        oids = _scan_cache_oids(self._cache_root, verbose=verbose)

        self._pending.clear()
        self._known = None
        if self._db is not None:
            self._db.close()
            self._db = None
        self._fallen_back = False
        try:
            _require_private_file_or_missing(self._db_path)
            for suffix in ('', '-journal', '-wal', '-shm'):
                p = Path(str(self._db_path) + suffix)
                if os.path.lexists(p):
                    p.unlink()
        except (OSError, _UnsafeIndex):
            pass

        if show_progress:
            with click.progressbar(
                oids,
                label='Rebuilding cache index',
                show_pos=True,
                show_percent=True,
            ) as bar:
                self._pending.update(bar)
        else:
            self._pending.update(oids)
        self.flush()
        return len(self)

    # -- Info -------------------------------------------------------------

    def info(self) -> dict:
        """Return status information about the index."""
        exists = os.path.lexists(self._db_path)
        result = {
            'path': str(self._db_path),
            'exists': exists,
            'cache_root': str(self._cache_root),
        }
        if exists:
            try:
                result['entries'] = len(self)
            except Exception as e:
                result['entries'] = None
                result['error'] = str(e)
        return result


class _UnsafeIndex(Exception):
    pass


class _transaction:
    """BEGIN IMMEDIATE … COMMIT/ROLLBACK on an autocommit connection."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def __enter__(self):
        self._conn.execute('BEGIN IMMEDIATE')

    def __exit__(self, exc_type, *exc):
        self._conn.execute('ROLLBACK' if exc_type else 'COMMIT')


def _ensure_private_dir(path: Path, create: bool) -> None:
    """Require ``path`` to be a real dir owned by us, writable by no one else,
    reached through a chain no other user can rename or replace.

    SQLite looks up its journal by path on every transaction, and a planted
    journal can make it delete arbitrary files, so the whole chain up to ``/``
    matters: every ancestor must be owned by us or root and not writable by
    others unless sticky.
    """
    _check_chain(path.parent if not os.path.lexists(path) else path)
    if create and not os.path.lexists(path):
        old_umask = os.umask(0o077)
        try:
            path.mkdir(parents=True, exist_ok=True)
        finally:
            os.umask(old_umask)
    st = os.lstat(path)
    if not stat_mod.S_ISDIR(st.st_mode) or st.st_uid != os.geteuid():
        raise _UnsafeIndex(f"{path} is not a directory owned by you")
    if st.st_mode & (stat_mod.S_IWGRP | stat_mod.S_IWOTH):
        raise _UnsafeIndex(f"{path} is writable by other users")
    _check_chain(path)


def _check_chain(path: Path) -> None:
    """Every existing directory from ``path`` up to ``/`` must be owned by us
    or root and not writable by others (unless sticky)."""
    uid = os.geteuid()
    real = Path(os.path.realpath(path))
    for d in (real, *real.parents):
        try:
            st = os.stat(d)
        except FileNotFoundError:
            continue
        if st.st_uid not in (uid, 0):
            raise _UnsafeIndex(f"{d} is owned by another user")
        if (st.st_mode & (stat_mod.S_IWGRP | stat_mod.S_IWOTH)
                and not st.st_mode & stat_mod.S_ISVTX):
            raise _UnsafeIndex(f"{d} is writable by other users")


def _require_private_file_or_missing(path: Path) -> None:
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return
    if (not stat_mod.S_ISREG(st.st_mode) or st.st_uid != os.geteuid()
            or st.st_mode & (stat_mod.S_IWGRP | stat_mod.S_IWOTH)):
        raise _UnsafeIndex("not a private regular file")


def _require_expected_schema(conn: sqlite3.Connection) -> None:
    """Allow only our one table — no views, triggers or other objects."""
    rows = conn.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_autoindex_%'"
    ).fetchall()
    if rows != [('table', 'oids')]:
        raise _UnsafeIndex("unexpected schema")
    cols = [r[1] for r in conn.execute('PRAGMA table_info(oids)')]
    if cols != ['oid']:
        raise _UnsafeIndex("unexpected schema")


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
