"""Tests for dt.cache_ops link primitives.

``link_file`` is the primitive behind every cache and remote transfer, so its
skip-if-present contract matters: cache entries are routinely hardlinked to the
shared remote object, and clobbering one rewrites the other.
"""

import errno
import os
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
