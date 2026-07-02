"""Unit tests for dt.remote_quarantine module (issue #152)."""

import json
from pathlib import Path

import pytest

from dt import remote_quarantine as q
from dt import remote_verify as rv
from dt.utils import md5_file


def _place_blob(remote_dir: Path, content: bytes, name: str = None) -> str:
    """Write a v3 blob at its correct md5 path (or a forced name)."""
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


def _place_dir_manifest(remote_dir: Path, children):
    """Write a .dir manifest blob listing the given (md5, relpath) children.

    Returns the .dir object's remote-relative path.
    """
    entries = [{"md5": md5, "relpath": rel} for md5, rel in children]
    content = json.dumps(entries, sort_keys=True).encode()
    scratch = remote_dir / "_scratch_dir"
    remote_dir.mkdir(parents=True, exist_ok=True)
    scratch.write_bytes(content)
    h = md5_file(scratch)
    scratch.unlink()
    d = remote_dir / "files" / "md5" / h[:2]
    d.mkdir(parents=True, exist_ok=True)
    (d / (h[2:] + ".dir")).write_bytes(content)
    return f"files/md5/{h[:2]}/{h[2:]}.dir"


class TestReverseMap:
    def test_maps_child_to_dir(self, tmp_path):
        remote = tmp_path / "remote"
        child = _place_blob(remote, b"member\n")
        dir_rel = _place_dir_manifest(remote, [(child, "sub/member.txt")])

        rev = q.build_dir_reverse_map(remote)
        assert child in rev
        assert dir_rel in rev[child]


class TestPlanQuarantine:
    def test_bad_member_pulls_in_enclosing_dir(self, tmp_path):
        remote = tmp_path / "remote"
        bad = _place_blob(remote, b"corrupt", name="ff" + "0" * 30)
        dir_rel = _place_dir_manifest(remote, [(bad, "data/x.bin")])

        entries = [{'path': f"files/md5/ff/{'0' * 30}",
                    'expected_md5': "ff" + "0" * 30,
                    'status': rv.STATUS_MISMATCH}]
        plan = q.plan_quarantine(remote, entries)
        paths = {p['path'] for p in plan}
        assert f"files/md5/ff/{'0' * 30}" in paths  # the bad member
        assert dir_rel in paths                       # + its enclosing .dir
        dir_item = next(p for p in plan if p['path'] == dir_rel)
        assert dir_item['reason'] == q.REASON_DIR_ENCLOSING

    def test_bad_dir_itself_no_reverse_lookup_needed(self, tmp_path):
        remote = tmp_path / "remote"
        dir_rel = _place_dir_manifest(remote, [])
        entries = [{'path': dir_rel, 'expected_md5': 'a' * 32 + '.dir',
                    'status': rv.STATUS_MISMATCH}]
        plan = q.plan_quarantine(remote, entries)
        assert [p['path'] for p in plan] == [dir_rel]


class TestQuarantine:
    def test_moves_bad_and_dir_aside_with_manifest(self, tmp_path):
        remote = tmp_path / "remote"
        bad = _place_blob(remote, b"corrupt", name="ff" + "0" * 30)
        dir_rel = _place_dir_manifest(remote, [(bad, "data/x.bin")])
        bad_rel = f"files/md5/ff/{'0' * 30}"

        entries = [{'path': bad_rel, 'expected_md5': "ff" + "0" * 30,
                    'status': rv.STATUS_MISMATCH}]
        result = q.quarantine(remote, entries, timestamp="20260702T000000Z")

        # Both the bad member and the enclosing .dir were moved out.
        assert not (remote / bad_rel).exists()
        assert not (remote / dir_rel).exists()
        qdir = Path(result['quarantine_dir'])
        assert (qdir / bad_rel).exists()
        assert (qdir / dir_rel).exists()
        assert len(result['moved']) == 2

        # Manifest records what moved.
        manifest = json.loads((qdir / q.MANIFEST_FILENAME).read_text())
        moved_paths = {e['path'] for e in manifest['entries']}
        assert moved_paths == {bad_rel, dir_rel}

    def test_dry_run_moves_nothing(self, tmp_path):
        remote = tmp_path / "remote"
        bad = _place_blob(remote, b"corrupt", name="ff" + "0" * 30)
        bad_rel = f"files/md5/ff/{'0' * 30}"
        entries = [{'path': bad_rel, 'expected_md5': "ff" + "0" * 30,
                    'status': rv.STATUS_MISMATCH}]

        result = q.quarantine(remote, entries, dry_run=True)
        assert result['dry_run'] is True
        assert (remote / bad_rel).exists()  # untouched

    def test_same_timestamp_does_not_collide(self, tmp_path):
        """Two quarantine runs with the same timestamp get distinct batch dirs,
        so neither run's blobs/manifest are orphaned or overwritten."""
        remote = tmp_path / "remote"
        b1 = _place_blob(remote, b"corrupt one", name="ff" + "0" * 30)
        b2 = _place_blob(remote, b"corrupt two", name="ee" + "1" * 30)

        r1 = q.quarantine(remote, [{'path': f"files/md5/ff/{'0' * 30}",
                                    'expected_md5': "ff" + "0" * 30,
                                    'status': rv.STATUS_MISMATCH}],
                          timestamp="TS")
        r2 = q.quarantine(remote, [{'path': f"files/md5/ee/{'1' * 30}",
                                    'expected_md5': "ee" + "1" * 30,
                                    'status': rv.STATUS_MISMATCH}],
                          timestamp="TS")

        assert r1['quarantine_dir'] != r2['quarantine_dir']
        # Both batches are independently listed and restorable.
        batches = q.list_quarantines(remote)
        assert len(batches) == 2
        assert q.restore_quarantine(remote, Path(r1['quarantine_dir']).name)['restored']
        assert q.restore_quarantine(remote, Path(r2['quarantine_dir']).name)['restored']

    def test_missing_blob_recorded_not_fatal(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"anchor\n")  # so layout is detectable
        entries = [{'path': f"files/md5/ab/{'c' * 30}",
                    'expected_md5': "ab" + "c" * 30,
                    'status': rv.STATUS_MISMATCH}]
        result = q.quarantine(remote, entries, timestamp="20260702T000001Z")
        assert result['moved'] == []
        assert len(result['missing']) == 1


class TestRestoreAndList:
    def test_restore_round_trip(self, tmp_path):
        remote = tmp_path / "remote"
        bad = _place_blob(remote, b"corrupt", name="ff" + "0" * 30)
        bad_rel = f"files/md5/ff/{'0' * 30}"
        entries = [{'path': bad_rel, 'expected_md5': "ff" + "0" * 30,
                    'status': rv.STATUS_MISMATCH}]
        q.quarantine(remote, entries, timestamp="20260702T010000Z")
        assert not (remote / bad_rel).exists()

        batches = q.list_quarantines(remote)
        assert len(batches) == 1
        assert batches[0]['timestamp'] == "20260702T010000Z"
        assert batches[0]['count'] == 1

        result = q.restore_quarantine(remote, "20260702T010000Z")
        assert bad_rel in result['restored']
        assert (remote / bad_rel).exists()

    def test_restore_skips_when_already_present(self, tmp_path):
        remote = tmp_path / "remote"
        bad = _place_blob(remote, b"corrupt", name="ff" + "0" * 30)
        bad_rel = f"files/md5/ff/{'0' * 30}"
        entries = [{'path': bad_rel, 'expected_md5': "ff" + "0" * 30,
                    'status': rv.STATUS_MISMATCH}]
        q.quarantine(remote, entries, timestamp="20260702T020000Z")

        # Simulate a re-push landing a good copy at the original path.
        (remote / bad_rel).parent.mkdir(parents=True, exist_ok=True)
        (remote / bad_rel).write_bytes(b"good copy")

        result = q.restore_quarantine(remote, "20260702T020000Z")
        assert bad_rel in result['skipped']
        assert result['restored'] == []
