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


class TestCopyNeverReadsThroughLinks:
    """Planted links in a shared remote never put outside bytes in a copy."""

    SECRET = b'-----BEGIN PRIVATE KEY----- not really\n'

    def _setup(self, tmp_path):
        src = tmp_path / "remote"
        _build_remote(src, [b"one\n"])
        home = tmp_path / "home"
        home.mkdir()
        (home / "id_key").write_bytes(self.SECRET)
        (home / ("c" * 30)).write_bytes(self.SECRET)
        prefix = next((src / "files" / "md5").iterdir())
        (prefix / ("d" * 30)).symlink_to(home / "id_key")      # file link
        (src / "files" / "md5" / "cc").symlink_to(home)         # dir link
        (src / "TOPLEVEL").symlink_to(home / "id_key")          # top-level link
        return src

    def _leaked(self, dst):
        return [p for p in dst.rglob("*")
                if p.is_file() and not p.is_symlink() and p.read_bytes() == self.SECRET]

    @pytest.mark.parametrize("independent", [True, False])
    def test_copy_skips_and_lists_links(self, tmp_path, independent):
        src = self._setup(tmp_path)
        dst = tmp_path / "dst"
        totals = ops.copy_tree(src, dst, independent=independent, jobs=1)
        assert self._leaked(dst) == []
        assert not any(p.is_symlink() for p in dst.rglob("*"))
        assert "files/md5/cc" in totals["links"]
        assert "TOPLEVEL" in totals["links"]
        assert any(l.endswith("d" * 30) for l in totals["links"])

    def test_cross_fs_move_refuses_with_links(self, tmp_path):
        src = self._setup(tmp_path)
        dst = tmp_path / "moved"
        with patch("dt.remote_verify.resolve_local_remote",
                   return_value=("myremote", str(src), src)), \
             patch("dt.remote_ops.same_filesystem", return_value=False), \
             patch("dt.remote_ops.repoint_remotes", return_value=[]):
            with pytest.raises(RemoteError, match="symbolic link"):
                ops.move_remote(None, str(dst), jobs=1, verbose=False)
        assert src.exists()
        assert self._leaked(dst) == []

    def test_copies_not_world_readable_unless_destination_is(self, tmp_path):
        src = tmp_path / "remote"; _build_remote(src, [b"one\n"])
        dst = tmp_path / "dst"
        dst.mkdir()
        os.chmod(dst, 0o2770)
        ops.copy_tree(src, dst, independent=True, jobs=1)
        blob = next(p for p in (dst / "files" / "md5").rglob("*") if p.is_file())
        assert os.stat(blob).st_mode & 0o007 == 0
        dst2 = tmp_path / "dst2"
        dst2.mkdir()
        os.chmod(dst2, 0o2775)
        ops.copy_tree(src, dst2, independent=True, jobs=1)
        blob2 = next(p for p in (dst2 / "files" / "md5").rglob("*") if p.is_file())
        assert os.stat(blob2).st_mode & 0o004


class TestCopySurvivesParentSwap:
    SECRET = b'id_rsa contents\n'

    @pytest.mark.parametrize("independent", [True, False])
    def test_prefix_dir_swapped_after_listing(self, tmp_path, independent):
        src = tmp_path / "remote"; _build_remote(src, [b"one\n"])
        home = tmp_path / "home"; home.mkdir()
        (home / "id_rsa").write_bytes(self.SECRET)
        os.chmod(home / "id_rsa", 0o600)
        prefix = next((src / "files" / "md5").iterdir())
        real_walk = ops._walk_layout

        def walk_then_swap(fd):
            listed = list(real_walk(fd))
            os.rename(prefix, tmp_path / "moved-prefix")
            prefix.symlink_to(home)
            return iter(listed)

        dst = tmp_path / "dst"
        with patch("dt.remote_ops._walk_layout", side_effect=walk_then_swap):
            totals = ops.copy_tree(src, dst, independent=independent, jobs=1)
        found = [p for p in dst.rglob("*") if p.is_file()
                 and (p.read_bytes() == self.SECRET
                      or os.stat(p).st_ino == os.stat(home / "id_rsa").st_ino)]
        assert found == []
        assert str(prefix.relative_to(src)) in totals["links"]

    def test_fresh_destination_gets_no_world_read(self, tmp_path):
        src = tmp_path / "remote"; _build_remote(src, [b"one\n"])
        dst = tmp_path / "fresh"
        ops.copy_tree(src, dst, independent=True, jobs=1)
        blob = next(p for p in (dst / "files" / "md5").rglob("*") if p.is_file())
        assert os.stat(blob).st_mode & 0o777 == 0o440


class TestArchiveRefusesLinkedPrefix:
    def test_scan_prefixes_raises(self, tmp_path):
        from dt.archive import operations as aops
        from dt.errors import ArchiveError
        src = tmp_path / "remote"; _build_remote(src, [b"one\n"])
        (src / "files" / "md5" / "ee").symlink_to(tmp_path)
        with pytest.raises(ArchiveError, match="symbolic link"):
            aops.scan_prefixes(src, aops.LAYOUT_DVC_V3)
