"""Tests for dt.cache_ops link primitives.

``link_file`` is the primitive behind every cache and remote transfer, so its
skip-if-present contract matters: cache entries are routinely hardlinked to the
shared remote object, and clobbering one rewrites the other.
"""

import errno
import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest

from dt import cache_ops


@pytest.fixture
def src(tmp_path):
    """A source file to link from."""
    p = tmp_path / 'source'
    p.write_text('NEW CONTENT')
    return p


@pytest.fixture(autouse=True)
def clear_reflink_probe():
    """The per-filesystem reflink probe is module state; don't leak it."""
    cache_ops._reflink_supported.clear()
    yield
    cache_ops._reflink_supported.clear()


class TestLinkFileNewDestination:
    """A destination that does not yet exist."""

    def test_links_and_reports_success(self, tmp_path, src):
        dest = tmp_path / 'cache' / 'ab' / 'cdef'

        success, link_type = cache_ops.link_file(src, dest)

        assert success is True
        assert link_type in ('reflink', 'hardlink', 'symlink', 'copy')
        assert dest.read_text() == 'NEW CONTENT'

    def test_creates_parent_directories(self, tmp_path, src):
        dest = tmp_path / 'deeply' / 'nested' / 'dest'

        assert cache_ops.link_file(src, dest)[0] is True
        assert dest.exists()

    def test_missing_source_fails(self, tmp_path):
        success, link_type = cache_ops.link_file(
            tmp_path / 'absent', tmp_path / 'dest'
        )

        assert (success, link_type) == (False, 'failed')

    def test_explicit_copy_makes_dest_readonly(self, tmp_path, src):
        dest = tmp_path / 'dest'

        success, link_type = cache_ops.link_file(src, dest, cache_type='copy')

        assert (success, link_type) == (True, 'copy')
        assert oct(dest.stat().st_mode)[-3:] == '444'


@pytest.mark.parametrize('mode', [0o444, 0o644, 0o664, 0o666])
class TestLinkFileExistingDestination:
    """An existing destination must never be overwritten, whatever its mode.

    Regression guard: ``shutil.copy2`` happily truncates a writable
    destination, so the copy fallback silently clobbered group-writable cache
    entries -- which on a shared cache is the normal case, not the exception.
    """

    def _make_dest(self, tmp_path, mode):
        dest = tmp_path / 'dest'
        dest.write_text('EXISTING CACHED BLOB')
        os.chmod(dest, mode)
        return dest

    def test_default_order_skips(self, tmp_path, src, mode):
        dest = self._make_dest(tmp_path, mode)

        success, link_type = cache_ops.link_file(src, dest)

        assert (success, link_type) == (False, 'skipped')
        assert dest.read_text() == 'EXISTING CACHED BLOB'

    @pytest.mark.parametrize(
        'cache_type', ['reflink', 'hardlink', 'symlink', 'copy']
    )
    def test_each_cache_type_skips(self, tmp_path, src, mode, cache_type):
        dest = self._make_dest(tmp_path, mode)

        success, link_type = cache_ops.link_file(
            src, dest, cache_type=cache_type
        )

        assert (success, link_type) == (False, 'skipped')
        assert dest.read_text() == 'EXISTING CACHED BLOB'


class TestTryReflink:
    """FICLONE is issued directly; the old code shelled out to `cp` with an
    argument coreutils rejects, so reflinking could never succeed."""

    def test_never_clobbers_existing_destination(self, tmp_path, src):
        dest = tmp_path / 'dest'
        dest.write_text('EXISTING CACHED BLOB')

        assert cache_ops._try_reflink(src, dest) is False
        assert dest.read_text() == 'EXISTING CACHED BLOB'

    def test_leaves_no_stub_behind_when_unsupported(self, tmp_path, src):
        """A failed clone must not leave the empty file it opened with O_EXCL."""
        dest = tmp_path / 'dest'

        if cache_ops._try_reflink(src, dest):
            pytest.skip('filesystem supports reflinks')

        assert not dest.exists()

    def test_unsupported_filesystem_is_probed_once(self, tmp_path, src):
        """After one EOPNOTSUPP the device is remembered, not retried per file."""
        device = tmp_path.stat().st_dev

        with patch.object(cache_ops.fcntl, 'ioctl',
                          side_effect=OSError(errno.EOPNOTSUPP, 'nope')) as mock_ioctl:
            assert cache_ops._try_reflink(src, tmp_path / 'a') is False
            assert cache_ops._try_reflink(src, tmp_path / 'b') is False
            assert cache_ops._try_reflink(src, tmp_path / 'c') is False

        assert cache_ops._reflink_supported[device] is False
        assert mock_ioctl.call_count == 1

    def test_transient_error_is_not_cached(self, tmp_path, src):
        """A one-off EIO must not disable reflinks for the rest of the run."""
        with patch.object(cache_ops.fcntl, 'ioctl',
                          side_effect=OSError(errno.EIO, 'transient')):
            assert cache_ops._try_reflink(src, tmp_path / 'a') is False

        assert tmp_path.stat().st_dev not in cache_ops._reflink_supported

    def test_success_reports_reflink(self, tmp_path, src):
        """When the ioctl succeeds, link_file reports a reflink."""
        dest = tmp_path / 'dest'

        with patch.object(cache_ops.fcntl, 'ioctl', return_value=0):
            success, link_type = cache_ops.link_file(src, dest)

        assert (success, link_type) == (True, 'reflink')
        assert dest.exists()

    def test_missing_source_fails_cleanly(self, tmp_path):
        assert cache_ops._try_reflink(tmp_path / 'absent', tmp_path / 'dest') is False
        assert not (tmp_path / 'dest').exists()


class TestLinkFileFallbackOrder:
    """DVC's preferred order: reflink -> hardlink -> symlink -> copy."""

    def test_falls_back_to_copy_when_links_unavailable(self, tmp_path, src):
        dest = tmp_path / 'dest'

        with patch.object(cache_ops, '_try_reflink', return_value=False):
            with patch.object(cache_ops, '_try_hardlink', return_value=False):
                with patch.object(cache_ops, '_try_symlink', return_value=False):
                    success, link_type = cache_ops.link_file(src, dest)

        assert (success, link_type) == (True, 'copy')
        assert dest.read_text() == 'NEW CONTENT'

    def test_prefers_hardlink_over_copy(self, tmp_path, src):
        dest = tmp_path / 'dest'

        success, link_type = cache_ops.link_file(src, dest)

        # Same filesystem, so a hardlink is available and should win over a
        # full copy. (reflink needs CoW support, which tmpfs/Lustre lack.)
        assert success is True
        if link_type == 'hardlink':
            assert dest.stat().st_ino == src.stat().st_ino

    def test_all_methods_failing_on_new_dest_reports_failed(self, tmp_path, src):
        dest = tmp_path / 'dest'

        with patch.object(cache_ops, '_try_reflink', return_value=False):
            with patch.object(cache_ops, '_try_hardlink', return_value=False):
                with patch.object(cache_ops, '_try_symlink', return_value=False):
                    with patch.object(cache_ops, '_try_copy', return_value=False):
                        result = cache_ops.link_file(src, dest)

        assert result == (False, 'failed')
        assert not dest.exists()


class TestCopyNeverFollowsDestinationLinks:
    """A link already at the cache object name is never written through."""

    def _src(self, tmp_path):
        src = tmp_path / 'src'
        src.write_bytes(b'blob')
        return src

    def test_dangling_link_is_not_written_through(self, tmp_path):
        from dt import cache_ops
        src = self._src(tmp_path)
        target = tmp_path / 'victim' / 'startup.sh'
        target.parent.mkdir()
        dest = tmp_path / 'cache' / 'ab' / 'cdef'
        dest.parent.mkdir(parents=True)
        dest.symlink_to(target)
        ok, kind = cache_ops.link_file(src, dest)
        assert not ok
        assert not target.exists()
        assert os.path.islink(dest)

    def test_copy_primitive_refuses_existing_link(self, tmp_path):
        from dt import cache_ops
        src = self._src(tmp_path)
        existing = tmp_path / 'existing'
        existing.write_bytes(b'mine')
        os.chmod(existing, 0o640)
        dest = tmp_path / 'dest'
        dest.symlink_to(existing)
        assert cache_ops._try_copy(src, dest) is False
        assert existing.read_bytes() == b'mine'

    def test_make_readonly_does_not_follow_links(self, tmp_path):
        from dt import cache_ops
        target = tmp_path / 'private'
        target.write_bytes(b'x')
        os.chmod(target, 0o600)
        link = tmp_path / 'link'
        link.symlink_to(target)
        cache_ops._make_readonly(link)
        assert stat.S_IMODE(os.stat(target).st_mode) == 0o600

    def test_make_readonly_ignores_fifo(self, tmp_path):
        from dt import cache_ops
        fifo = tmp_path / 'fifo'
        os.mkfifo(fifo)
        cache_ops._make_readonly(fifo)  # must not block or chmod
        assert stat.S_IMODE(os.stat(fifo).st_mode) != 0o444

    def test_copy_creates_readonly_regular_file(self, tmp_path):
        from dt import cache_ops
        src = self._src(tmp_path)
        dest = tmp_path / 'out'
        assert cache_ops._try_copy(src, dest)
        cache_ops._make_readonly(dest)
        assert dest.read_bytes() == b'blob'
        assert stat.S_IMODE(os.stat(dest).st_mode) == 0o444
        assert os.stat(dest).st_mtime == os.stat(src).st_mtime


class TestWriteObject:
    def test_creates_readonly_object(self, tmp_path):
        dest = tmp_path / 'files' / 'md5' / 'ab' / ('c' * 30)
        assert cache_ops.write_object(dest, b'data') is True
        assert dest.read_bytes() == b'data'
        assert stat.S_IMODE(os.stat(dest).st_mode) == 0o444
        assert [p.name for p in dest.parent.iterdir()] == [dest.name]  # no temp left

    def test_existing_regular_object_is_left_alone(self, tmp_path):
        dest = tmp_path / 'obj'
        dest.write_bytes(b'old')
        assert cache_ops.write_object(dest, b'new') is False
        assert dest.read_bytes() == b'old'

    def test_dangling_link_is_refused_and_target_never_created(self, tmp_path):
        target = tmp_path / 'home' / '.bashrc'
        target.parent.mkdir()
        dest = tmp_path / 'cache' / 'ab' / ('c' * 30)
        dest.parent.mkdir(parents=True)
        dest.symlink_to(target)
        with pytest.raises(OSError, match='not a regular file'):
            cache_ops.write_object(dest, b'payload')
        assert not target.exists()
        assert [p.name for p in dest.parent.iterdir()] == [dest.name]


class TestGitShowFetchRefusesPlantedLink:
    def test_planted_link_at_object_path(self, tmp_path):
        """dt fetch's git-show route must not write through a planted link."""
        import hashlib
        from unittest.mock import MagicMock, patch
        from dt import fetch
        content = b'imported file\n'
        md5 = hashlib.md5(content).hexdigest()
        cache = tmp_path / 'cache'
        dest = cache / 'files' / 'md5' / md5[:2] / md5[2:]
        dest.parent.mkdir(parents=True)
        target = tmp_path / 'home' / 'planted'
        target.parent.mkdir()
        dest.symlink_to(target)

        stage = MagicMock()
        stage.path = str(tmp_path / 'x.dvc')
        (tmp_path / 'x.dvc').write_text(
            f"outs:\n- md5: {md5}\n  path: x\n")
        info = {'url': 'git@github.com:org/repo.git', 'rev': 'a' * 40, 'path': 'x'}
        clone = tmp_path / 'clone'
        clone.mkdir()
        with patch('dt.fetch.utils.get_import_info', return_value=info), \
             patch('dt.fetch._resolve_primary_cache_base', return_value=str(cache)), \
             patch('dt.tmp.clone_repo', return_value=clone), \
             patch('dt.fetch._run_dvc_fetch', return_value=(False, 'no')), \
             patch('dt.fetch._recover_import_via_source_clone', return_value=(False, 'no')), \
             patch('subprocess.run', return_value=MagicMock(returncode=0, stdout=content)):
            ok, msg = fetch._run_repo_import_network_fetch(stage)
        assert not target.exists()
        assert os.path.islink(dest)
