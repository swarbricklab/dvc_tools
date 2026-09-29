"""Unit tests for dt.cache_relink module."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from dt import cache_relink as cr
from dt.errors import RemoteError
from dt.utils import md5_file


def _place_v3_blob(root: Path, content: bytes, name: str = None) -> str:
    """Write a v3-layout blob under ``root`` and return its md5 (or ``name``)."""
    root.mkdir(parents=True, exist_ok=True)
    scratch = root / "_scratch"
    scratch.write_bytes(content)
    h = md5_file(scratch)
    scratch.unlink()
    key = name if name is not None else h
    d = root / "files" / "md5" / key[:2]
    d.mkdir(parents=True, exist_ok=True)
    (d / key[2:]).write_bytes(content)
    return key


# =============================================================================
# _remote_blob_path
# =============================================================================

class TestRemoteBlobPath:
    def test_v3_hit(self, tmp_path):
        remote = tmp_path / "remote"
        md5 = _place_v3_blob(remote, b"hello")
        got = cr._remote_blob_path(remote, "dvc-v3", md5)
        assert got == remote / "files" / "md5" / md5[:2] / md5[2:]

    def test_v3_miss(self, tmp_path):
        remote = tmp_path / "remote"
        (remote / "files" / "md5").mkdir(parents=True)
        assert cr._remote_blob_path(remote, "dvc-v3", "ab" + "0" * 30) is None

    def test_v2_hit(self, tmp_path):
        remote = tmp_path / "remote"
        md5 = "ab" + "1" * 30
        d = remote / md5[:2]
        d.mkdir(parents=True)
        (d / md5[2:]).write_bytes(b"x")
        got = cr._remote_blob_path(remote, "dvc-v2", md5)
        assert got == d / md5[2:]

    def test_mixed_checks_both(self, tmp_path):
        remote = tmp_path / "remote"
        # object only present in the v2 (flat) tree
        md5 = "cd" + "2" * 30
        d = remote / md5[:2]
        d.mkdir(parents=True)
        (d / md5[2:]).write_bytes(b"y")
        got = cr._remote_blob_path(remote, "dvc-mixed", md5)
        assert got == d / md5[2:]


# =============================================================================
# _relink_one
# =============================================================================

class TestRelinkOne:
    def test_replaces_file_with_symlink(self, tmp_path):
        target = tmp_path / "remote_obj"
        target.write_bytes(b"payload-1234")
        blob = tmp_path / "cache_obj"
        blob.write_bytes(b"payload-1234")

        freed = cr._relink_one(blob, target)

        assert freed == len(b"payload-1234")
        assert os.path.islink(blob)
        assert Path(os.readlink(blob)) == target.resolve()
        assert blob.read_bytes() == b"payload-1234"
        # no temp file left behind
        assert not (tmp_path / "cache_obj.dt-relink.tmp").exists()

    def test_atomic_over_readonly_blob(self, tmp_path):
        target = tmp_path / "remote_obj"
        target.write_bytes(b"z")
        blob = tmp_path / "cache_obj"
        blob.write_bytes(b"z")
        os.chmod(blob, 0o444)  # DVC keeps cache objects read-only
        cr._relink_one(blob, target)
        assert os.path.islink(blob)


# =============================================================================
# relink_cache end-to-end (skip_verify)
# =============================================================================

class TestRelinkCache:
    def _setup(self, tmp_path):
        cache = tmp_path / "cache"
        remote = tmp_path / "remote"
        return cache, remote

    def _run(self, cache, remote, **kw):
        with patch.object(cr.remote_verify_mod, "resolve_local_remote",
                          return_value=("local", str(remote), remote)), \
             patch.object(cr.utils, "get_cache_root", return_value=cache):
            return cr.relink_cache(skip_verify=True, **kw)

    def test_relinks_present_objects(self, tmp_path):
        cache, remote = self._setup(tmp_path)
        md5 = _place_v3_blob(cache, b"shared-bytes")
        _place_v3_blob(remote, b"shared-bytes")

        stats = self._run(cache, remote)

        assert stats.relinked == 1
        assert stats.missing == 0
        assert stats.bytes_reclaimed == len(b"shared-bytes")
        blob = cache / "files" / "md5" / md5[:2] / md5[2:]
        assert os.path.islink(blob)

    def test_missing_left_untouched(self, tmp_path):
        cache, remote = self._setup(tmp_path)
        md5 = _place_v3_blob(cache, b"only-in-cache")
        (remote / "files" / "md5").mkdir(parents=True)

        stats = self._run(cache, remote, verbose=True)

        assert stats.relinked == 0
        assert stats.missing == 1
        assert md5 in stats.missing_md5s
        blob = cache / "files" / "md5" / md5[:2] / md5[2:]
        assert blob.is_file() and not os.path.islink(blob)

    def test_idempotent(self, tmp_path):
        cache, remote = self._setup(tmp_path)
        _place_v3_blob(cache, b"dup")
        _place_v3_blob(remote, b"dup")

        first = self._run(cache, remote)
        second = self._run(cache, remote)

        assert first.relinked == 1
        assert second.relinked == 0
        assert second.already_linked == 1

    def test_dry_run_makes_no_changes(self, tmp_path):
        cache, remote = self._setup(tmp_path)
        md5 = _place_v3_blob(cache, b"preview")
        _place_v3_blob(remote, b"preview")

        stats = self._run(cache, remote, dry_run=True)

        assert stats.relinked == 1
        assert stats.bytes_reclaimed == len(b"preview")
        blob = cache / "files" / "md5" / md5[:2] / md5[2:]
        assert blob.is_file() and not os.path.islink(blob)

    def test_dir_manifest_relinked(self, tmp_path):
        cache, remote = self._setup(tmp_path)
        dir_md5 = "ab" + "0" * 30 + ".dir"
        _place_v3_blob(cache, b'[{"md5":"x"}]', name=dir_md5)
        _place_v3_blob(remote, b'[{"md5":"x"}]', name=dir_md5)

        stats = self._run(cache, remote)

        assert stats.relinked == 1
        blob = cache / "files" / "md5" / dir_md5[:2] / dir_md5[2:]
        assert os.path.islink(blob)


class TestVerifyGate:
    def test_aborts_on_bad_blobs(self, tmp_path):
        cache = tmp_path / "cache"
        remote = tmp_path / "remote"
        (remote / "files" / "md5").mkdir(parents=True)
        with patch.object(cr.remote_verify_mod, "resolve_local_remote",
                          return_value=("local", str(remote), remote)), \
             patch.object(cr.remote_verify_mod, "verify_remote",
                          return_value=({"objects": 1},
                                        [{"path": "files/md5/ab/c"}], [],
                                        "dvc-v3")):
            with pytest.raises(RemoteError, match="bad/incomplete"):
                cr.relink_cache()

    def test_force_proceeds_despite_bad_blobs(self, tmp_path):
        cache = tmp_path / "cache"
        remote = tmp_path / "remote"
        md5 = _place_v3_blob(cache, b"data")
        _place_v3_blob(remote, b"data")
        with patch.object(cr.remote_verify_mod, "resolve_local_remote",
                          return_value=("local", str(remote), remote)), \
             patch.object(cr.remote_verify_mod, "verify_remote",
                          return_value=({"objects": 1},
                                        [{"path": "files/md5/ab/c"}], [],
                                        "dvc-v3")), \
             patch.object(cr.utils, "get_cache_root", return_value=cache):
            stats = cr.relink_cache(force=True)
        assert stats.relinked == 1
        blob = cache / "files" / "md5" / md5[:2] / md5[2:]
        assert os.path.islink(blob)
