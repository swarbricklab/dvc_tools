"""Unit tests for dt.remote_ops diff / merge (dt remote diff / merge, issue #166).

Covers a half-finished migration where blobs are split across an old and a new
remote with neither a superset of the other: the diff must report each side's
missing blobs, and the top-up (merge) must copy src-only blobs into dst
additively, atomically, verifiably, and idempotently — never clobbering an
existing destination blob, and copying nothing on a dry run.
"""

import os
import stat
from pathlib import Path

import pytest

from dt import remote_ops as ops
from dt.errors import RemoteError
from dt.utils import md5_file


# --------------------------------------------------------------------------- #
# Blob staging helpers (v2 and v3 layouts)
# --------------------------------------------------------------------------- #

def _md5(content: bytes) -> str:
    import hashlib
    return hashlib.md5(content).hexdigest()


def _place_v3(remote_dir: Path, content: bytes, is_dir: bool = False) -> str:
    """Write a blob at its v3 path (files/md5/ab/cdef...). Returns its md5."""
    h = _md5(content)
    name = h[2:] + ('.dir' if is_dir else '')
    d = remote_dir / "files" / "md5" / h[:2]
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(content)
    return h


def _place_v2(remote_dir: Path, content: bytes, is_dir: bool = False) -> str:
    """Write a blob at its v2 path (ab/cdef...). Returns its md5."""
    h = _md5(content)
    name = h[2:] + ('.dir' if is_dir else '')
    d = remote_dir / h[:2]
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(content)
    return h


def _logical(h: str, is_dir: bool = False) -> str:
    return f"{h[:2]}/{h[2:]}" + ('.dir' if is_dir else '')


# --------------------------------------------------------------------------- #
# index_remote
# --------------------------------------------------------------------------- #

class TestIndexRemote:
    def test_indexes_v3_blobs(self, tmp_path):
        r = tmp_path / "r"
        h1 = _place_v3(r, b"one\n")
        h2 = _place_v3(r, b"two\n")
        index, layout = ops.index_remote(r, jobs=2)
        assert layout == "dvc-v3"
        assert set(index) == {_logical(h1), _logical(h2)}
        rel, size = index[_logical(h1)]
        assert rel == f"files/md5/{h1[:2]}/{h1[2:]}"
        assert size == len(b"one\n")

    def test_v2_and_v3_share_logical_key(self, tmp_path):
        """The same content stored under v2 and v3 has one logical identity."""
        content = b"shared\n"
        v2 = tmp_path / "v2"
        v3 = tmp_path / "v3"
        h = _place_v2(v2, content)
        assert _place_v3(v3, content) == h
        i2, l2 = ops.index_remote(v2)
        i3, l3 = ops.index_remote(v3)
        assert l2 == "dvc-v2" and l3 == "dvc-v3"
        assert set(i2) == set(i3) == {_logical(h)}

    def test_skips_stray_non_blob(self, tmp_path):
        r = tmp_path / "r"
        _place_v3(r, b"real\n")
        # A stray *.tmp left by a killed transfer must not be indexed.
        d = next((r / "files" / "md5").iterdir())
        (d / "deadbeef.tmp").write_bytes(b"junk")
        index, _ = ops.index_remote(r)
        assert len(index) == 1

    def test_dir_object_indexed_distinctly(self, tmp_path):
        r = tmp_path / "r"
        h = _place_v3(r, b'{"a": 1}\n', is_dir=True)
        index, _ = ops.index_remote(r)
        assert set(index) == {_logical(h, is_dir=True)}


# --------------------------------------------------------------------------- #
# diff_remotes
# --------------------------------------------------------------------------- #

class TestDiffRemotes:
    def test_reports_each_sides_missing(self, tmp_path):
        a = tmp_path / "a"
        b = tmp_path / "b"
        common = _place_v3(a, b"common\n")
        _place_v3(b, b"common\n")
        only_a = _place_v3(a, b"only-a-1\n")
        only_a2 = _place_v3(a, b"only-a-2\n")
        only_b = _place_v3(b, b"only-b\n")

        d = ops.diff_remotes(a, b)
        assert d['only_in_a'] == sorted([_logical(only_a), _logical(only_a2)])
        assert d['only_in_b'] == [_logical(only_b)]
        assert d['common_count'] == 1
        assert d['only_in_a_count'] == 2
        assert d['only_in_b_count'] == 1
        assert d['only_in_a_bytes'] == len(b"only-a-1\n") + len(b"only-a-2\n")
        assert d['only_in_b_bytes'] == len(b"only-b\n")

    def test_cross_layout_blobs_are_common(self, tmp_path):
        """A v2 old remote vs a v3 new remote: shared content is not a diff."""
        a = tmp_path / "a"  # old, v2
        b = tmp_path / "b"  # new, v3
        _place_v2(a, b"shared\n")
        _place_v3(b, b"shared\n")
        only_old = _place_v2(a, b"old-only\n")

        d = ops.diff_remotes(a, b)
        assert d['a_layout'] == "dvc-v2"
        assert d['b_layout'] == "dvc-v3"
        assert d['common_count'] == 1
        assert d['only_in_a'] == [_logical(only_old)]
        assert d['only_in_b'] == []

    def test_mixed_layout_remote(self, tmp_path):
        a = tmp_path / "a"
        b = tmp_path / "b"
        # a is mixed: one v2, one v3 blob.
        hv2 = _place_v2(a, b"v2blob\n")
        hv3 = _place_v3(a, b"v3blob\n")
        _place_v3(b, b"v3blob\n")  # b only has the v3 one
        d = ops.diff_remotes(a, b)
        assert d['a_layout'] == "dvc-mixed"
        assert d['only_in_a'] == [_logical(hv2)]
        assert set(d['_a_index']) == {_logical(hv2), _logical(hv3)}


# --------------------------------------------------------------------------- #
# merge_remotes
# --------------------------------------------------------------------------- #

class TestMergeRemotes:
    def test_tops_up_missing_blobs(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        shared = _place_v3(src, b"shared\n")
        _place_v3(dst, b"shared\n")
        m1 = _place_v3(src, b"missing-1\n")
        m2 = _place_v3(src, b"missing-2\n")

        res = ops.merge_remotes(src, dst)
        assert res['copied'] == 2
        assert res['copied_bytes'] == len(b"missing-1\n") + len(b"missing-2\n")
        assert res['mismatch'] == 0 and res['errors'] == 0
        # The two missing blobs now exist in dst at their content-addressed path.
        for h, body in [(m1, b"missing-1\n"), (m2, b"missing-2\n")]:
            p = dst / "files" / "md5" / h[:2] / h[2:]
            assert p.read_bytes() == body
        # After a top-up the diff is empty in the src->dst direction.
        d = ops.diff_remotes(src, dst)
        assert d['only_in_a'] == []

    def test_dry_run_copies_nothing(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        _place_v3(src, b"m1\n")
        _place_v3(src, b"m2\n")
        _place_v3(dst, b"present\n")

        before = {str(p) for p in dst.rglob("*") if p.is_file()}
        res = ops.merge_remotes(src, dst, dry_run=True)
        after = {str(p) for p in dst.rglob("*") if p.is_file()}

        assert res['dry_run'] is True
        assert res['missing_in_dest'] == 2
        assert res['missing_in_dest_bytes'] == len(b"m1\n") + len(b"m2\n")
        assert res['copied'] == 0
        assert before == after  # nothing written

    def test_never_clobbers_existing(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        # Same logical key, but dst already holds (deliberately different) bytes.
        h = _place_v3(src, b"canonical\n")
        d = dst / "files" / "md5" / h[:2]
        d.mkdir(parents=True)
        (d / h[2:]).write_bytes(b"pre-existing-do-not-touch\n")

        res = ops.merge_remotes(src, dst)
        # The blob shares dst's logical key, so it is already common — never
        # queued for copy, never overwritten.
        assert res['copied'] == 0
        assert res['missing_in_dest'] == 0
        assert (d / h[2:]).read_bytes() == b"pre-existing-do-not-touch\n"

    def test_idempotent_second_run_is_noop(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        _place_v3(src, b"a\n")
        _place_v3(src, b"b\n")

        first = ops.merge_remotes(src, dst)
        assert first['copied'] == 2
        # Second run: dst is now a superset, so there is nothing left to copy.
        second = ops.merge_remotes(src, dst)
        assert second['copied'] == 0
        assert second['missing_in_dest'] == 0

    def test_skip_branch_never_overwrites(self, tmp_path):
        """If a queued blob already exists at dst (race), it is skipped intact."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        h = _place_v3(src, b"payload\n")
        # dst already holds this blob but with sentinel bytes; feed a diff that
        # (stale) still lists it as only-in-src to drive the dst.exists() branch.
        d = dst / "files" / "md5" / h[:2]
        d.mkdir(parents=True)
        (d / h[2:]).write_bytes(b"SENTINEL\n")
        stale_diff = {
            'a_layout': 'dvc-v3', 'b_layout': 'dvc-v3',
            'only_in_a': [_logical(h)], 'only_in_a_bytes': len(b"payload\n"),
            '_a_index': {_logical(h): (f"files/md5/{h[:2]}/{h[2:]}",
                                       len(b"payload\n"))},
        }
        res = ops.merge_remotes(src, dst, diff=stale_diff)
        assert res['copied'] == 0
        assert res['skipped'] == 1
        assert (d / h[2:]).read_bytes() == b"SENTINEL\n"

    def test_verify_passes_and_copies(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        h = _place_v3(src, b"verify-me\n")
        res = ops.merge_remotes(src, dst, verify=True)
        assert res['copied'] == 1 and res['mismatch'] == 0
        assert (dst / "files" / "md5" / h[:2] / h[2:]).read_bytes() == b"verify-me\n"

    def test_verify_rejects_corrupt_source(self, tmp_path):
        """A src blob whose bytes don't match its name is reported, not copied."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        # Place a blob then corrupt its content so md5(content) != name.
        h = _place_v3(src, b"good\n")
        bad = src / "files" / "md5" / h[:2] / h[2:]
        os.chmod(bad, 0o644)
        bad.write_bytes(b"CORRUPTED\n")

        res = ops.merge_remotes(src, dst, verify=True)
        assert res['copied'] == 0
        assert res['mismatch'] == 1
        assert res['mismatches'][0]['expected_md5'] == h
        # Nothing published to dst (no partial/corrupt blob leaked through).
        assert not (dst / "files" / "md5" / h[:2] / h[2:]).exists()
        # And no .tmp detritus left behind.
        assert list(dst.rglob("*.tmp")) == []

    def test_copied_blob_is_readonly(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        h = _place_v3(src, b"ro\n")
        ops.merge_remotes(src, dst)
        p = dst / "files" / "md5" / h[:2] / h[2:]
        mode = stat.S_IMODE(p.stat().st_mode)
        assert not (mode & stat.S_IWUSR)  # user write bit cleared (DVC cache style)

    def test_preserves_source_layout_path(self, tmp_path):
        """A v2 src blob is written to dst at its v2 path, not re-laid-out."""
        src = tmp_path / "src"  # v2
        dst = tmp_path / "dst"  # starts v3
        _place_v3(dst, b"seed\n")  # give dst a v3 layout
        h = _place_v2(src, b"v2only\n")
        ops.merge_remotes(src, dst)
        assert (dst / h[:2] / h[2:]).read_bytes() == b"v2only\n"


# --------------------------------------------------------------------------- #
# resolve_remote_arg
# --------------------------------------------------------------------------- #

class TestResolveRemoteArg:
    def test_path_argument(self, tmp_path):
        r = tmp_path / "r"
        r.mkdir()
        label, path = ops.resolve_remote_arg(str(r))
        assert path == r.resolve()
        assert label == str(r.resolve())

    def test_missing_path_raises(self, tmp_path):
        with pytest.raises(RemoteError):
            ops.resolve_remote_arg(str(tmp_path / "nope"))

    def test_missing_allowed_when_not_required(self, tmp_path):
        target = tmp_path / "new"
        label, path = ops.resolve_remote_arg(str(target), must_exist=False)
        assert path == target.resolve()

    def test_configured_remote_name(self, tmp_path, monkeypatch):
        r = tmp_path / "configured"
        r.mkdir()
        monkeypatch.setattr(
            ops.remote_mod, 'list_remotes',
            lambda project_only=False: [('nci', str(r), True)])
        monkeypatch.setattr(
            ops.remote_verify, 'resolve_local_remote',
            lambda name: ('nci', str(r), r))
        label, path = ops.resolve_remote_arg('nci')
        assert label == 'nci'
        assert path == r
