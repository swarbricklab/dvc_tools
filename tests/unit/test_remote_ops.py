"""Unit tests for dt.remote_ops (dt remote move / copy)."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from dt import remote_ops as ops
from dt.errors import RemoteError
from dt.utils import md5_file


def _place_blob(remote_dir: Path, content: bytes, name: str = None) -> str:
    """Write a v3 blob at its correct md5 path (or a given name)."""
    remote_dir.mkdir(parents=True, exist_ok=True)
    scratch = remote_dir / "_scratch"
    scratch.write_bytes(content)
    h = md5_file(scratch)
    scratch.unlink()
    key = name if name is not None else h
    d = remote_dir / "files" / "md5" / key[:2]
    d.mkdir(parents=True, exist_ok=True)
    (d / key[2:]).write_bytes(content)
    return key


def _build_remote(remote_dir: Path, contents) -> None:
    for c in contents:
        _place_blob(remote_dir, c)


# =============================================================================
# same_filesystem / _transfer_file
# =============================================================================

class TestFsHelpers:
    def test_same_filesystem_true(self, tmp_path):
        assert ops.same_filesystem(tmp_path, tmp_path / "does-not-exist-yet")

    def test_transfer_independent_is_new_inode(self, tmp_path):
        src = tmp_path / "s"; src.write_bytes(b"hi")
        dst = tmp_path / "d"
        assert ops._transfer_file(src, dst, independent=True) == "copy"
        assert dst.read_bytes() == b"hi"
        assert src.stat().st_ino != dst.stat().st_ino

    def test_transfer_default_prefers_hardlink(self, tmp_path):
        src = tmp_path / "s"; src.write_bytes(b"hi")
        dst = tmp_path / "d"
        method = ops._transfer_file(src, dst, independent=False)
        # On the test fs reflink usually isn't available, so hardlink wins.
        assert method in ("reflink", "hardlink", "copy")
        if method == "hardlink":
            assert src.stat().st_ino == dst.stat().st_ino

    def test_transfer_skips_existing(self, tmp_path):
        src = tmp_path / "s"; src.write_bytes(b"hi")
        dst = tmp_path / "d"; dst.write_bytes(b"existing")
        assert ops._transfer_file(src, dst, independent=True) == "skipped"
        assert dst.read_bytes() == b"existing"


# =============================================================================
# copy_tree
# =============================================================================

class TestCopyTree:
    def test_replicates_blob_tree(self, tmp_path):
        src = tmp_path / "src"
        _build_remote(src, [b"one\n", b"two\n", b"three\n"])
        dst = tmp_path / "dst"

        totals = ops.copy_tree(src, dst, independent=True, jobs=2)

        assert totals["files"] == 3
        assert totals["layout"] == "dvc-v3"
        # Every source blob exists identically at the destination.
        for p in src.rglob("*"):
            if p.is_file():
                rel = p.relative_to(src)
                assert (dst / rel).read_bytes() == p.read_bytes()

    def test_copies_toplevel_files(self, tmp_path):
        src = tmp_path / "src"
        _build_remote(src, [b"x\n"])
        (src / "ARCHIVED.yaml").write_text("marker")
        dst = tmp_path / "dst"

        ops.copy_tree(src, dst, independent=True, jobs=1)
        assert (dst / "ARCHIVED.yaml").read_text() == "marker"

    def test_group_writable_sets_setgid(self, tmp_path):
        src = tmp_path / "src"
        _build_remote(src, [b"x\n"])
        dst = tmp_path / "dst"

        ops.copy_tree(src, dst, independent=True, jobs=1, group_writable=True)
        mode = (dst / "files" / "md5").stat().st_mode
        assert mode & 0o2000  # setgid
        assert mode & 0o020   # group-writable


# =============================================================================
# _size_presence_check
# =============================================================================

class TestSizePresenceCheck:
    def test_clean_copy_has_no_problems(self, tmp_path):
        src = tmp_path / "src"; _build_remote(src, [b"a\n", b"bb\n"])
        dst = tmp_path / "dst"; ops.copy_tree(src, dst, independent=True, jobs=1)
        assert ops._size_presence_check(src, dst) == []

    def test_detects_missing_and_mismatch(self, tmp_path):
        src = tmp_path / "src"; _build_remote(src, [b"a\n", b"bb\n"])
        dst = tmp_path / "dst"; ops.copy_tree(src, dst, independent=True, jobs=1)
        # Truncate one dest blob and delete another (blobs are 0o444).
        dest_blobs = [p for p in (dst / "files/md5").rglob("*") if p.is_file()]
        os.chmod(dest_blobs[0], 0o644)
        dest_blobs[0].write_bytes(b"")          # size mismatch
        os.chmod(dest_blobs[1], 0o644)
        dest_blobs[1].unlink()                  # missing
        problems = ops._size_presence_check(src, dst)
        assert len(problems) == 2


# =============================================================================
# repoint_remotes
# =============================================================================

class TestRepointRemotes:
    def test_rewrites_matching_url_in_correct_scope(self, tmp_path):
        old = "/data/remotes/proj"
        new = "/scratch/proj"
        remotes_all = [
            ("myremote", f"ssh://gadi{old}", True),    # project scope
            ("local", old, False),                      # local scope
            ("other", "/elsewhere", False),             # unrelated
        ]
        remotes_project = [("myremote", f"ssh://gadi{old}", True)]

        def fake_list(project_only=False):
            return remotes_project if project_only else remotes_all

        calls = []

        class _Ok:
            returncode = 0
            stderr = ""

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return _Ok()

        with patch("dt.remote_ops.remote_mod.list_remotes",
                   side_effect=fake_list), \
             patch("dt.remote_ops.subprocess.run", side_effect=fake_run):
            changed = ops.repoint_remotes(tmp_path, old, new, verbose=False)

        # myremote + local matched; 'other' did not.
        assert {c[0] for c in changed} == {"myremote", "local"}
        modified_names = {c[-3] for c in calls}
        assert modified_names == {"myremote", "local"}
        # local remote modified in --local scope; project one not.
        local_cmd = next(c for c in calls if c[-3] == "local")
        assert "--local" in local_cmd
        my_cmd = next(c for c in calls if c[-3] == "myremote")
        assert "--local" not in my_cmd
        # path rewritten in place, scheme/host preserved.
        assert my_cmd[-1] == f"ssh://gadi{new}"


# =============================================================================
# copy_remote / move_remote (high level)
# =============================================================================

class TestCopyRemote:
    def test_copy_remote_duplicates_tree(self, tmp_path):
        src = tmp_path / "remote"; _build_remote(src, [b"one\n", b"two\n"])
        dst = tmp_path / "remote-copy"
        with patch("dt.remote_verify.resolve_local_remote",
                   return_value=("myremote", str(src), src)):
            result = ops.copy_remote(None, str(dst), independent=True,
                                     jobs=1, verbose=False)
        assert result["files"] == 2
        assert result["registered"] is None
        assert (dst / "files" / "md5").is_dir()

    def test_copy_remote_rejects_nonempty_dest(self, tmp_path):
        src = tmp_path / "remote"; _build_remote(src, [b"x\n"])
        dst = tmp_path / "dst"; dst.mkdir(); (dst / "junk").write_text("x")
        with patch("dt.remote_verify.resolve_local_remote",
                   return_value=("myremote", str(src), src)):
            with pytest.raises(RemoteError, match="not empty"):
                ops.copy_remote(None, str(dst), jobs=1, verbose=False)


class TestMoveRemote:
    def test_same_fs_move_is_rename(self, tmp_path):
        src = tmp_path / "remote"; _build_remote(src, [b"one\n"])
        dst = tmp_path / "remote-moved"
        with patch("dt.remote_verify.resolve_local_remote",
                   return_value=("myremote", str(src), src)), \
             patch("dt.remote_ops.repoint_remotes", return_value=[]):
            result = ops.move_remote(None, str(dst), jobs=1, verbose=False)
        assert result["method"] == "rename"
        assert not src.exists()
        assert (dst / "files" / "md5").is_dir()

    def test_cross_fs_move_copies_verifies_deletes(self, tmp_path):
        src = tmp_path / "remote"; _build_remote(src, [b"one\n", b"two\n"])
        dst = tmp_path / "remote-moved"
        with patch("dt.remote_verify.resolve_local_remote",
                   return_value=("myremote", str(src), src)), \
             patch("dt.remote_ops.same_filesystem", return_value=False), \
             patch("dt.remote_ops.repoint_remotes", return_value=[]):
            result = ops.move_remote(None, str(dst), jobs=1, verbose=False)
        assert result["method"] == "copy+verify+delete"
        assert not src.exists()
        # destination blobs are intact (verification passed)
        blobs = [p for p in (dst / "files/md5").rglob("*") if p.is_file()]
        assert len(blobs) == 2

    def test_cross_fs_move_quick_check(self, tmp_path):
        src = tmp_path / "remote"; _build_remote(src, [b"one\n"])
        dst = tmp_path / "remote-moved"
        with patch("dt.remote_verify.resolve_local_remote",
                   return_value=("myremote", str(src), src)), \
             patch("dt.remote_ops.same_filesystem", return_value=False), \
             patch("dt.remote_ops.repoint_remotes", return_value=[]):
            result = ops.move_remote(None, str(dst), quick=True, jobs=1,
                                     verbose=False)
        assert result["method"] == "copy+verify+delete"
        assert result["quick"] is True
        assert not src.exists()
