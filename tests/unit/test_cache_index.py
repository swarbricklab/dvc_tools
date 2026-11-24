"""Tests for the cache OID index."""

import os
import sqlite3
import stat
from pathlib import Path
from unittest.mock import patch

import pytest

from dt import cache_index
from dt.cache_index import CacheIndex


@pytest.fixture(autouse=True)
def chain_checked_below(tmp_path, monkeypatch):
    """Check the directory chain only up to the test's temp root; the real
    ancestors (e.g. a group-writable /scratch project dir) are out of scope."""
    real = cache_index._check_chain
    top = Path(os.environ['XDG_CACHE_HOME'])

    def bounded(path):
        p = Path(os.path.realpath(path))
        stops = [top, *top.parents]
        chain = [d for d in (p, *p.parents) if d not in stops[1:]]
        for d in chain:
            real_one(d)

    def real_one(d):
        try:
            st = os.stat(d)
        except FileNotFoundError:
            return
        if st.st_uid not in (os.geteuid(), 0):
            raise cache_index._UnsafeIndex(f"{d} is owned by another user")
        if st.st_mode & 0o022 and not st.st_mode & stat.S_ISVTX:
            raise cache_index._UnsafeIndex(f"{d} is writable by other users")

    monkeypatch.setattr(cache_index, '_check_chain', bounded)


@pytest.fixture
def cache_root(tmp_path):
    root = tmp_path / 'cache'
    root.mkdir()
    os.chmod(root, 0o2775)  # shared-cache style
    return root


@pytest.fixture
def victim(tmp_path):
    f = tmp_path / 'victim' / 'important.txt'
    f.parent.mkdir()
    f.write_text('keep me')
    return f


def _exercise(idx):
    """Every write path fetch/rebuild use."""
    idx.add('aa' * 16)
    idx.add_many(['bb' * 16, 'cc' * 16])
    idx.remove_many(['bb' * 16, 'dd' * 16])
    idx.remove('cc' * 16)
    idx.clear()
    idx.rebuild(show_progress=False)
    len(idx)
    idx.oids()


class TestLocation:
    def test_index_is_per_user_and_private(self, cache_root):
        with CacheIndex(cache_root) as idx:
            idx.add('ab' * 16)
            path = idx.path
        assert Path(os.environ['XDG_CACHE_HOME']) in path.parents
        assert not (os.stat(path.parent).st_mode & 0o077)
        assert not (os.stat(path).st_mode & (stat.S_IRWXG | stat.S_IRWXO))
        assert not (cache_root / '.dt').exists()

    def test_keyed_by_real_cache_path(self, tmp_path, cache_root):
        link = tmp_path / 'link'
        link.symlink_to(cache_root)
        assert cache_index.index_path_for(link) == cache_index.index_path_for(cache_root)
        assert cache_index.index_path_for(tmp_path) != cache_index.index_path_for(cache_root)


class TestBasics:
    def test_add_contains_remove(self, cache_root):
        with CacheIndex(cache_root) as idx:
            idx.add('ab' * 16)
            assert 'ab' * 16 in idx  # visible while still buffered
            assert idx.add_many(['cd' * 16, 'ef' * 16]) == 2
            assert len(idx) == 3
            assert idx.remove_many(['cd' * 16, 'zz']) == 1
            idx.remove('ef' * 16)
            assert idx.oids() == {'ab' * 16}

    def test_buffered_adds_flushed_on_close(self, cache_root):
        with CacheIndex(cache_root) as idx:
            for i in range(5):
                idx.add(f'{i:032x}')
        with CacheIndex(cache_root, read_only=True) as idx:
            assert len(idx) == 5

    def test_rebuild_scans_v2_and_v3(self, cache_root):
        (cache_root / 'files' / 'md5' / 'ab').mkdir(parents=True)
        (cache_root / 'files' / 'md5' / 'ab' / ('c' * 30)).write_text('x')
        (cache_root / 'cd').mkdir()
        (cache_root / 'cd' / ('e' * 30 + '.dir')).write_text('x')
        with CacheIndex(cache_root) as idx:
            idx.add('stale')
            assert idx.rebuild(show_progress=False) == 2
            assert idx.oids() == {'ab' + 'c' * 30, 'cd' + 'e' * 30 + '.dir'}

    def test_read_only_missing_creates_nothing(self, cache_root):
        with CacheIndex(cache_root, read_only=True) as idx:
            assert len(idx) == 0
            assert not os.path.lexists(idx.path.parent)


class TestSharedCacheIsNeverOpened:
    """Other users can write the shared cache; nothing there is trusted."""

    def test_planted_files_in_cache_dt_are_ignored(self, cache_root, victim):
        dt_dir = cache_root / '.dt'
        dt_dir.mkdir()
        # Old diskcache-style store naming a victim file, plus a hot-journal-
        # like file: neither is read, so neither can trigger a deletion.
        old = dt_dir / 'cache.db'
        old.mkdir()
        conn = sqlite3.connect(old / 'cache.db')
        conn.executescript(f"""
            CREATE TABLE Cache (rowid INTEGER PRIMARY KEY, key BLOB, raw INTEGER,
                expire_time REAL, mode INTEGER, filename TEXT, value BLOB);
            INSERT INTO Cache VALUES (1, 'k', 1, 0, 2, '{victim}', NULL);
        """)
        conn.commit()
        conn.close()
        (dt_dir / 'oid-index.sqlite-journal').write_bytes(b'\xd9\xd5\x05\xf9\x20\xa1\x63\xd7' + bytes(504))
        before = sorted(os.listdir(dt_dir))
        with CacheIndex(cache_root) as idx:
            _exercise(idx)
        assert victim.read_text() == 'keep me'
        assert sorted(os.listdir(dt_dir)) == before

    def test_symlinked_cache_dt_dir_is_not_chmodded(self, cache_root, tmp_path):
        private = tmp_path / 'home'
        private.mkdir()
        os.chmod(private, 0o700)
        (cache_root / '.dt').symlink_to(private)
        with CacheIndex(cache_root) as idx:
            _exercise(idx)
        assert stat.S_IMODE(os.stat(private).st_mode) == 0o700
        assert os.listdir(private) == []


class TestUntrustedIndexFile:
    def _plant(self, cache_root, sql):
        db = cache_index.index_path_for(cache_root)
        db.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        conn = sqlite3.connect(db)
        conn.executescript(sql)
        conn.commit()
        conn.close()
        os.chmod(db, 0o600)
        return db

    def test_extra_trigger_is_rejected(self, cache_root):
        db = self._plant(cache_root, """
            CREATE TABLE oids (oid TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID;
            CREATE TABLE log (x);
            CREATE TRIGGER t AFTER INSERT ON oids BEGIN INSERT INTO log VALUES (1); END;
        """)
        with CacheIndex(cache_root) as idx:
            idx.add('ab')
            assert 'ab' in idx  # in-memory fallback still works
        conn = sqlite3.connect(db)
        assert conn.execute('SELECT count(*) FROM log').fetchone()[0] == 0

    def test_symlinked_index_file_is_not_followed(self, cache_root, victim):
        db = cache_index.index_path_for(cache_root)
        db.parent.mkdir(parents=True, mode=0o700)
        db.symlink_to(victim)
        with CacheIndex(cache_root) as idx:
            idx.add('ab')
            len(idx)
        assert victim.read_text() == 'keep me'

    def test_group_writable_index_dir_refused_for_read(self, cache_root):
        d = cache_index.index_path_for(cache_root).parent
        d.mkdir(parents=True)
        os.chmod(d, 0o777)
        (d / cache_index.index_path_for(cache_root).name).write_bytes(b'')
        with CacheIndex(cache_root, read_only=True) as idx:
            assert len(idx) == 0  # fell back, didn't open

    def test_group_writable_index_dir_refused_for_write(self, cache_root):
        d = cache_index.index_path_for(cache_root).parent
        d.mkdir(parents=True)
        os.chmod(d, 0o777)
        with CacheIndex(cache_root) as idx:
            idx.add('ab')
            assert 'ab' in idx  # in-memory fallback
        assert not os.listdir(d)  # nothing written into the open dir

    def test_open_ancestor_is_refused(self, cache_root):
        d = cache_index.index_path_for(cache_root).parent
        d.parent.mkdir(parents=True)
        os.chmod(d.parent, 0o777)  # e.g. a group-writable project dir above
        with CacheIndex(cache_root) as idx:
            idx.add('ab')
            assert 'ab' in idx
        assert not d.exists()

    def test_falls_back_to_next_candidate(self, cache_root, tmp_path, monkeypatch):
        bad = tmp_path / 'bad'
        bad.mkdir()
        os.chmod(bad, 0o777)
        good = Path(os.environ['XDG_CACHE_HOME']) / 'alt' / 'dt' / 'oid-index'
        monkeypatch.setattr(cache_index, '_candidate_dirs',
                            lambda: [bad / 'dt' / 'oid-index', good])
        with CacheIndex(cache_root) as idx:
            idx.add('ab')
            len(idx)  # forces open + flush
            path = idx.path
        assert path.parent == good and path.is_file()
        assert not (bad / 'dt').exists()

    def test_garbage_file_falls_back_and_rebuild_repairs(self, cache_root):
        db = cache_index.index_path_for(cache_root)
        db.parent.mkdir(parents=True, mode=0o700)
        db.write_bytes(b'not a database' * 100)
        os.chmod(db, 0o600)
        with CacheIndex(cache_root) as idx:
            idx.add('ab')
            assert 'ab' in idx
            idx.rebuild(show_progress=False)
        with CacheIndex(cache_root) as idx:
            idx.add('cd')
        with CacheIndex(cache_root, read_only=True) as idx:
            assert 'cd' in idx  # real file again after rebuild

    def test_query_time_errors_fall_back(self, cache_root):
        with CacheIndex(cache_root) as idx:
            idx.add('ab')
            idx.flush()
            real = idx._db
            boom = sqlite3.connect(':memory:')  # no oids table -> OperationalError
            idx._db = boom
            assert 'zz' not in idx
            idx.add_many(['cd'])
            assert 'cd' in idx
            real.close()
