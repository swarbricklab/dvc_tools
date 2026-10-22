"""Unit tests for dt.remote_verify module."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dt import remote_verify as rv
from dt.errors import RemoteError
from dt.utils import md5_file


def _place_blob(remote_dir: Path, content: bytes, name: str = None) -> str:
    """Write a v3 blob; if name is None, place it at its correct md5 path."""
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


# =============================================================================
# expected_md5_for_blob
# =============================================================================

class TestExpectedMd5:
    def test_plain_blob(self):
        assert rv.expected_md5_for_blob("0a", "1b2c3d") == "0a1b2c3d"

    def test_dir_suffix_stripped(self):
        assert rv.expected_md5_for_blob("0a", "1b2c3d.dir") == "0a1b2c3d"


# =============================================================================
# verify_remote
# =============================================================================

class TestVerifyRemote:
    def test_all_valid(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"alpha\n")
        _place_blob(remote, b"beta\n")

        totals, bad, incomplete, layout = rv.verify_remote(
            remote, jobs=2, use_ledger=False)

        assert layout == "dvc-v3"
        assert totals["objects"] == 2
        assert totals["ok"] == 2
        assert totals["bad"] == 0
        assert bad == []
        assert incomplete == []

    def test_detects_corrupt_blob(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"good\n")
        # A blob whose content does not match its name (truncated transfer).
        _place_blob(remote, b"truncated", name="ff" + "0" * 30)

        totals, bad, _inc, _ = rv.verify_remote(
            remote, jobs=2, use_ledger=False)

        assert totals["objects"] == 2
        assert totals["ok"] == 1
        assert totals["bad"] == 1
        assert len(bad) == 1
        assert bad[0]["status"] == rv.STATUS_MISMATCH
        assert bad[0]["path"] == "files/md5/ff/" + "0" * 30
        assert bad[0]["expected_md5"] == "ff" + "0" * 30
        assert bad[0]["actual_md5"] != bad[0]["expected_md5"]

    def test_tmp_file_reported_incomplete_not_bad(self, tmp_path):
        """A leftover *.tmp from a killed transfer is 'incomplete', not 'bad'."""
        remote = tmp_path / "remote"
        _place_blob(remote, b"good\n")
        d = remote / "files" / "md5" / "ab"
        d.mkdir(parents=True, exist_ok=True)
        (d / ("c" * 30 + ".tmp")).write_bytes(b"partial")

        totals, bad, incomplete, _ = rv.verify_remote(
            remote, jobs=2, use_ledger=False)

        assert totals["ok"] == 1
        assert totals["bad"] == 0
        assert totals["incomplete"] == 1
        assert bad == []
        assert len(incomplete) == 1
        assert incomplete[0]["status"] == rv.STATUS_INCOMPLETE
        assert incomplete[0]["path"].endswith(".tmp")

    def test_only_prefixes_restricts_walk(self, tmp_path):
        remote = tmp_path / "remote"
        h = _place_blob(remote, b"good\n")
        _place_blob(remote, b"truncated", name="ff" + "0" * 30)

        # Restrict to the bad blob's prefix only.
        totals, bad, _inc, _ = rv.verify_remote(
            remote, jobs=1, only_prefixes={"ff"}, use_ledger=False)
        assert totals["objects"] == 1
        assert totals["bad"] == 1

        # Restrict to the good blob's prefix only.
        totals2, bad2, _inc2, _ = rv.verify_remote(
            remote, jobs=1, only_prefixes={h[:2]}, use_ledger=False)
        assert totals2["objects"] == 1
        assert totals2["bad"] == 0


# =============================================================================
# incremental ledger
# =============================================================================

class TestIncrementalLedger:
    def test_second_run_skips_unchanged(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"alpha\n")
        _place_blob(remote, b"beta\n")

        # First run hashes everything and writes the ledger.
        totals1, _, _, _ = rv.verify_remote(remote, jobs=2)
        assert totals1["ok"] == 2
        assert totals1["skipped"] == 0
        assert (remote / rv.LEDGER_DIRNAME).is_dir()

        # Second run skips unchanged blobs.
        totals2, _, _, _ = rv.verify_remote(remote, jobs=2)
        assert totals2["objects"] == 2
        assert totals2["ok"] == 0
        assert totals2["skipped"] == 2
        assert totals2["bad"] == 0

    def test_full_ignores_ledger(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"alpha\n")
        rv.verify_remote(remote, jobs=1)  # populate ledger

        totals, _, _, _ = rv.verify_remote(remote, jobs=1, full=True)
        assert totals["ok"] == 1
        assert totals["skipped"] == 0

    def test_changed_blob_is_rehashed(self, tmp_path):
        remote = tmp_path / "remote"
        h = _place_blob(remote, b"alpha\n")
        rv.verify_remote(remote, jobs=1)  # ledger now records the good blob

        # Corrupt the blob in place (size/mtime change → ledger miss).
        import os
        import time
        blob = remote / "files" / "md5" / h[:2] / h[2:]
        time.sleep(0.01)
        blob.write_bytes(b"corrupted now")
        os.utime(blob, None)

        totals, bad, _, _ = rv.verify_remote(remote, jobs=1)
        assert totals["skipped"] == 0
        assert totals["bad"] == 1

    def test_no_ledger_does_not_write(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"alpha\n")
        rv.verify_remote(remote, jobs=1, use_ledger=False)
        assert not (remote / rv.LEDGER_DIRNAME).exists()


# =============================================================================
# build_report / format_report_summary
# =============================================================================

class TestReport:
    def _totals(self, **kw):
        t = {"objects": 0, "ok": 0, "bad": 0, "incomplete": 0,
             "skipped": 0, "bytes": 0}
        t.update(kw)
        return t

    def test_build_report_v3_no_caveat(self):
        rep = rv.build_report(
            "r", "/p", "dvc-v3", self._totals(objects=1, ok=1, bytes=5),
            [], [], jobs=4)
        assert rep["legacy_hash_caveat"] is False
        assert rep["remote"] == "r"
        assert rep["totals"]["ok"] == 1
        assert rep["incomplete"] == []

    def test_build_report_v2_has_caveat(self):
        rep = rv.build_report(
            "r", "/p", "dvc-v2", self._totals(), [], [], jobs=1)
        assert rep["legacy_hash_caveat"] is True

    def test_format_summary_lists_bad_and_incomplete(self):
        rep = rv.build_report(
            "r", "/p", "dvc-v3",
            self._totals(objects=2, bad=1, incomplete=1, bytes=9),
            [{"path": "files/md5/ff/x", "expected_md5": "ffx",
              "actual_md5": "zzz", "size_bytes": 9,
              "status": rv.STATUS_MISMATCH}],
            [{"path": "files/md5/ab/c.tmp", "status": rv.STATUS_INCOMPLETE}],
            jobs=1)
        out = rv.format_report_summary(rep)
        assert "Bad:      1" in out
        assert "files/md5/ff/x" in out
        assert "Incomplete: 1" in out
        assert "files/md5/ab/c.tmp" in out

    def test_format_summary_shows_skipped(self):
        rep = rv.build_report(
            "r", "/p", "dvc-v3", self._totals(objects=5, ok=2, skipped=3),
            [], [], jobs=1, ledger_used=True)
        out = rv.format_report_summary(rep)
        assert "Skipped:  3" in out


# =============================================================================
# _merge_parts
# =============================================================================

class TestMergeParts:
    def test_merges_partial_reports(self, tmp_path):
        (tmp_path / "part_0.json").write_text(json.dumps({
            "totals": {"objects": 2, "ok": 2, "bad": 0, "incomplete": 0,
                       "skipped": 0, "bytes": 10},
            "bad": [], "incomplete": []}))
        (tmp_path / "part_1.json").write_text(json.dumps({
            "totals": {"objects": 3, "ok": 1, "bad": 1, "incomplete": 1,
                       "skipped": 0, "bytes": 20},
            "bad": [{"path": "files/md5/ff/x", "status": "mismatch"}],
            "incomplete": [{"path": "files/md5/ab/y.tmp",
                            "status": "incomplete"}]}))

        totals, bad, incomplete = rv._merge_parts(tmp_path)

        assert totals == {"objects": 5, "ok": 3, "bad": 1, "incomplete": 1,
                          "skipped": 0, "bytes": 30}
        assert len(bad) == 1
        assert len(incomplete) == 1


# =============================================================================
# resolve_local_remote
# =============================================================================

class TestResolveLocalRemote:
    def test_no_remotes(self):
        with patch("dt.remote.list_remotes", return_value=[]):
            with pytest.raises(RemoteError, match="No remotes configured"):
                rv.resolve_local_remote(None)

    def test_unknown_name(self):
        with patch("dt.remote.list_remotes",
                   return_value=[("a", "/p", True)]):
            with pytest.raises(RemoteError, match="No remote named"):
                rv.resolve_local_remote("nope")

    def test_cloud_remote_rejected(self):
        with patch("dt.remote.list_remotes",
                   return_value=[("a", "s3://bucket/x", True)]):
            with pytest.raises(RemoteError, match="locally-accessible"):
                rv.resolve_local_remote("a")

    def test_missing_path_rejected(self, tmp_path):
        missing = str(tmp_path / "nope")
        with patch("dt.remote.list_remotes",
                   return_value=[("a", missing, True)]):
            with pytest.raises(RemoteError, match="not accessible"):
                rv.resolve_local_remote("a")

    def test_archived_remote_rejected(self, tmp_path):
        remote = tmp_path / "remote"
        remote.mkdir()
        with patch("dt.remote.list_remotes",
                   return_value=[("a", str(remote), True)]):
            with patch("dt.archive.signpost.detect", return_value=object()):
                with pytest.raises(RemoteError, match="archived"):
                    rv.resolve_local_remote("a")

    def test_resolves_local_default(self, tmp_path):
        remote = tmp_path / "remote"
        (remote / "files" / "md5").mkdir(parents=True)
        with patch("dt.remote.list_remotes",
                   return_value=[("a", str(remote), True)]):
            with patch("dt.archive.signpost.detect", return_value=None):
                name, url, path = rv.resolve_local_remote(None)
        assert name == "a"
        assert path == remote


# =============================================================================
# Persisted bad-blob report (issue #152)
# =============================================================================

class TestBadReport:
    def test_write_and_load_bad_report(self, tmp_path):
        remote = tmp_path / "remote"
        (remote / "files" / "md5").mkdir(parents=True)
        rep = {
            'report_version': 2, 'remote': 'r', 'url': str(remote),
            'layout': 'dvc-v3', 'scanned_at': '2026-07-02T00:00:00+00:00',
            'totals': {'bad': 1},
            'bad': [{'path': 'files/md5/ab/cd', 'expected_md5': 'ab' + 'c' * 30,
                     'actual_md5': 'de' + 'f' * 30, 'status': rv.STATUS_MISMATCH}],
            'incomplete': [],
        }
        assert rv.write_bad_report(remote, rep) is True
        loaded = rv.load_bad_report(rv.bad_report_file(remote))
        assert loaded is not None
        assert loaded['bad'] == rep['bad']
        assert loaded['scanned_at'] == rep['scanned_at']

    def test_load_missing_returns_none(self, tmp_path):
        assert rv.load_bad_report(tmp_path / "nope.json") is None


# =============================================================================
# recheck_entries (issue #152)
# =============================================================================

class TestRecheck:
    def test_fixed_blob_now_passes_and_enters_ledger(self, tmp_path):
        remote = tmp_path / "remote"
        # A blob that was bad; now write the correct content at its md5 path.
        good = _place_blob(remote, b"repaired\n")
        entries = [{'path': f"files/md5/{good[:2]}/{good[2:]}",
                    'expected_md5': good, 'actual_md5': 'x' * 32,
                    'status': rv.STATUS_MISMATCH}]

        totals, still_bad, still_inc, layout = rv.recheck_entries(
            remote, entries)

        assert totals['objects'] == 1
        assert totals['ok'] == 1
        assert totals['bad'] == 0
        assert still_bad == []
        # Now-OK blob is promoted into the ledger so a later scan skips it.
        ledger = rv._load_prefix_ledger(remote / rv.LEDGER_DIRNAME, good[:2])
        assert good[2:] in ledger

    def test_still_bad_blob_stays_bad(self, tmp_path):
        remote = tmp_path / "remote"
        bad = _place_blob(remote, b"still wrong", name="ff" + "0" * 30)
        entries = [{'path': f"files/md5/ff/{'0' * 30}",
                    'expected_md5': "ff" + "0" * 30, 'actual_md5': None,
                    'status': rv.STATUS_MISMATCH}]

        totals, still_bad, _inc, _ = rv.recheck_entries(remote, entries)
        assert totals['bad'] == 1
        assert len(still_bad) == 1
        assert still_bad[0]['status'] == rv.STATUS_MISMATCH

    def test_vanished_blob_reported_missing(self, tmp_path):
        remote = tmp_path / "remote"
        (remote / "files" / "md5").mkdir(parents=True)
        entries = [{'path': f"files/md5/ab/{'c' * 30}",
                    'expected_md5': "ab" + "c" * 30, 'actual_md5': None,
                    'status': rv.STATUS_MISMATCH}]

        totals, still_bad, _inc, _ = rv.recheck_entries(remote, entries)
        assert totals['bad'] == 1
        assert still_bad[0]['status'] == rv.STATUS_MISSING

    def test_unreadable_blob_not_reported_missing(self, tmp_path):
        """A present-but-unreadable blob (stat EACCES) is 'unreadable', not 'missing'."""
        import errno
        remote = tmp_path / "remote"
        good = _place_blob(remote, b"present\n")
        rel = f"files/md5/{good[:2]}/{good[2:]}"
        entries = [{'path': rel, 'expected_md5': good, 'actual_md5': None,
                    'status': rv.STATUS_MISMATCH}]

        real_stat = Path.stat

        def fake_stat(self, *a, **k):
            if self.name == good[2:]:
                raise OSError(errno.EACCES, "Permission denied")
            return real_stat(self, *a, **k)

        with patch.object(Path, "stat", fake_stat):
            totals, still_bad, _inc, _ = rv.recheck_entries(remote, entries)

        assert totals['bad'] == 1
        assert still_bad[0]['status'] == rv.STATUS_UNREADABLE


# =============================================================================
# status_summary (issue #152)
# =============================================================================

class TestStatus:
    def test_status_after_full_verify(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"alpha\n")
        _place_blob(remote, b"beta\n")
        rv.verify_remote(remote, jobs=1)  # populate ledger

        st = rv.status_summary(remote, jobs=1)
        assert st['total_objects'] == 2
        assert st['ledger_ok'] == 2
        assert st['verified_unchanged'] == 2
        assert st['unscanned_or_changed'] == 0
        assert st['ledger_last_scan'] is not None

    def test_status_counts_unscanned(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"alpha\n")
        rv.verify_remote(remote, jobs=1)  # ledger has the first blob
        _place_blob(remote, b"gamma\n")   # a new, unscanned blob

        st = rv.status_summary(remote, jobs=1)
        assert st['total_objects'] == 2
        assert st['verified_unchanged'] == 1
        assert st['unscanned_or_changed'] == 1

    def test_status_reads_bad_count(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"alpha\n")
        rv.verify_remote(remote, jobs=1)
        rv.write_bad_report(remote, {
            'report_version': 2, 'layout': 'dvc-v3',
            'scanned_at': '2026-07-02T00:00:00+00:00',
            'bad': [{'path': 'files/md5/ff/x', 'status': rv.STATUS_MISMATCH}],
            'incomplete': [],
        })
        st = rv.status_summary(remote, jobs=1)
        assert st['bad'] == 1
        assert st['bad_scanned_at'] == '2026-07-02T00:00:00+00:00'


# =============================================================================
# CLI flag parsing for --status / --recheck (issue #152)
# =============================================================================

class TestVerifyCliFlags:
    """Guards the click option shapes — a bare --recheck must not be a usage
    error, and a remote name must bind to remote_name (not the flag)."""

    def _runner(self):
        from click.testing import CliRunner
        return CliRunner()

    def test_bare_recheck_is_not_a_usage_error(self, tmp_path):
        from dt.cli import cli
        remote = tmp_path / "remote"
        (remote / "files" / "md5").mkdir(parents=True)
        rv.write_bad_report(remote, {
            'report_version': 2, 'layout': 'dvc-v3', 'scanned_at': None,
            'bad': [], 'incomplete': []})
        with patch("dt.remote.list_remotes",
                   return_value=[("r", str(remote), True)]), \
             patch("dt.archive.signpost.detect", return_value=None):
            result = self._runner().invoke(cli, ['remote', 'verify', '--recheck'])
        # exit 2 == click usage error ("requires an argument"); must not happen.
        assert result.exit_code != 2, result.output
        assert 'requires an argument' not in result.output

    def test_recheck_with_remote_name_binds_name(self, tmp_path):
        from dt.cli import cli
        remote = tmp_path / "remote"
        (remote / "files" / "md5").mkdir(parents=True)
        rv.write_bad_report(remote, {
            'report_version': 2, 'layout': 'dvc-v3', 'scanned_at': None,
            'bad': [], 'incomplete': []})
        captured = {}

        def fake_resolve(name):
            captured['name'] = name
            return (name or 'default', str(remote), remote)

        with patch("dt.remote_verify.resolve_local_remote",
                   side_effect=fake_resolve):
            result = self._runner().invoke(
                cli, ['remote', 'verify', 'myremote', '--recheck'])
        assert result.exit_code == 0, result.output
        assert captured['name'] == 'myremote'  # not consumed by --recheck

    def test_status_flag_parses(self, tmp_path):
        from dt.cli import cli
        remote = tmp_path / "remote"
        _place_blob(remote, b"alpha\n")
        with patch("dt.remote_verify.resolve_local_remote",
                   return_value=('r', str(remote), remote)):
            result = self._runner().invoke(cli, ['remote', 'verify', '--status'])
        assert result.exit_code == 0, result.output
        assert 'Verified OK' in result.output


# =============================================================================
# No-egress size verification for cloud remotes (issue #196)
# =============================================================================

def _v3_key(md5: str) -> str:
    """Remote-relative v3 blob path for an md5 (e.g. files/md5/ab/cdef...)."""
    return f"files/md5/{md5[:2]}/{md5[2:]}"


class TestMd5FromCloudKey:
    def test_v3_path(self):
        md5 = "a" * 32
        rel = _v3_key(md5)
        got, prefix, is_dir, is_blob = rv._md5_from_cloud_key(rel)
        assert got == md5
        assert prefix == "aa"
        assert is_dir is False
        assert is_blob is True

    def test_v2_path(self):
        md5 = "b" * 32
        got, prefix, is_dir, is_blob = rv._md5_from_cloud_key(
            f"{md5[:2]}/{md5[2:]}")
        assert got == md5
        assert is_blob is True

    def test_dir_suffix(self):
        md5 = "c" * 32
        got, _p, is_dir, is_blob = rv._md5_from_cloud_key(
            f"files/md5/{md5[:2]}/{md5[2:]}.dir")
        assert got == md5
        assert is_dir is True
        assert is_blob is True

    def test_non_blob_path_ignored(self):
        # Ledger / metadata files under the remote root are not blob paths.
        _md5, _p, _d, is_blob = rv._md5_from_cloud_key(".dt-verify/bad.json")
        assert is_blob is False


class TestCompareRemoteSizes:
    def test_matching_sizes_pass(self):
        md5 = "d" * 32
        expected = {md5: 100}
        objs = [(_v3_key(md5), 100)]
        totals, bad, incomplete = rv.compare_remote_sizes(expected, objs)
        assert totals["objects"] == 1
        assert totals["ok"] == 1
        assert totals["bad"] == 0
        assert bad == []
        assert totals["bytes"] == 100

    def test_truncated_object_flagged(self):
        md5 = "e" * 32
        expected = {md5: 1000}
        objs = [(_v3_key(md5), 512)]  # truncated: remote size < expected
        totals, bad, incomplete = rv.compare_remote_sizes(expected, objs)
        assert totals["bad"] == 1
        assert len(bad) == 1
        e = bad[0]
        assert e["status"] == rv.STATUS_SIZE_MISMATCH
        assert e["expected_size"] == 1000
        assert e["size_bytes"] == 512
        assert e["expected_md5"] == md5

    def test_oversize_object_flagged(self):
        md5 = "f" * 32
        expected = {md5: 10}
        totals, bad, _ = rv.compare_remote_sizes(expected, [(_v3_key(md5), 99)])
        assert totals["bad"] == 1
        assert bad[0]["status"] == rv.STATUS_SIZE_MISMATCH

    def test_unknown_blob_is_skipped_not_bad(self):
        md5 = "1" * 32
        # No expected size known for this md5 -> unchecked, not flagged.
        totals, bad, _ = rv.compare_remote_sizes({}, [(_v3_key(md5), 50)])
        assert totals["objects"] == 1
        assert totals["skipped"] == 1
        assert totals["bad"] == 0

    def test_dir_manifest_is_skipped(self):
        md5 = "2" * 32
        rel = f"files/md5/{md5[:2]}/{md5[2:]}.dir"
        # Even with an (erroneous) expected size, .dir objects aren't checked.
        totals, bad, _ = rv.compare_remote_sizes({md5: 5}, [(rel, 999)])
        assert totals["skipped"] == 1
        assert totals["bad"] == 0

    def test_stray_tmp_reported_incomplete(self):
        rel = "files/md5/ab/cdef0123.tmp"  # not a valid md5 under a hex prefix
        totals, bad, incomplete = rv.compare_remote_sizes({}, [(rel, 7)])
        assert totals["incomplete"] == 1
        assert incomplete[0]["status"] == rv.STATUS_INCOMPLETE
        assert bad == []

    def test_missing_size_reported_unreadable(self):
        md5 = "3" * 32
        totals, bad, _ = rv.compare_remote_sizes(
            {md5: 42}, [(_v3_key(md5), None)])
        assert totals["bad"] == 1
        assert bad[0]["status"] == rv.STATUS_UNREADABLE

    def test_ledger_metadata_ignored(self):
        # A file under the remote root (not a hex prefix dir) is not counted.
        totals, bad, incomplete = rv.compare_remote_sizes(
            {}, [(".dt-verify/bad.json", 10)])
        assert totals["objects"] == 0
        assert bad == [] and incomplete == []

    def test_mixed_batch(self):
        ok_md5 = "4" * 32
        bad_md5 = "5" * 32
        expected = {ok_md5: 100, bad_md5: 100}
        objs = [
            (_v3_key(ok_md5), 100),         # ok
            (_v3_key(bad_md5), 10),         # truncated
            (_v3_key("6" * 32), 100),       # unknown -> skipped
        ]
        totals, bad, _ = rv.compare_remote_sizes(expected, objs)
        assert totals["objects"] == 3
        assert totals["ok"] == 1
        assert totals["bad"] == 1
        assert totals["skipped"] == 1


class TestListRemoteObjectSizes:
    class _FakeFS:
        def __init__(self, detail):
            self._detail = detail

        def find(self, base, detail=True):
            return self._detail

    class _FakeODB:
        def __init__(self, path, fs):
            self.path = path
            self.fs = fs

    def test_dict_detail_with_sizes(self):
        base = "bucket/wgs/files/md5"
        detail = {
            f"{base}/ab/cdef": {"size": 123, "type": "file"},
            f"{base}/ab/0011": {"size": 7, "type": "file"},
        }
        odb = self._FakeODB(base, self._FakeFS(detail))
        got = sorted(rv.list_remote_object_sizes(odb))
        assert got == [("ab/0011", 7), ("ab/cdef", 123)]

    def test_capital_size_key(self):
        base = "bucket/x"
        detail = {f"{base}/ab/cdef": {"Size": 55}}
        odb = self._FakeODB(base, self._FakeFS(detail))
        got = list(rv.list_remote_object_sizes(odb))
        assert got == [("ab/cdef", 55)]

    def test_list_detail_form(self):
        base = "bucket/x"
        detail = [{"name": f"{base}/ab/cdef", "size": 9}]
        odb = self._FakeODB(base, self._FakeFS(detail))
        got = list(rv.list_remote_object_sizes(odb))
        assert got == [("ab/cdef", 9)]


class TestVerifyCloudRemote:
    """Orchestrator: mock DVC/S3 plumbing, assert truncation is flagged."""

    def test_flags_truncated_blob_end_to_end(self):
        good = "7" * 32
        truncated = "8" * 32
        expected = {good: 500, truncated: 500}
        listing = [(_v3_key(good), 500), (_v3_key(truncated), 128)]

        fake_odb = type("ODB", (), {"path": "bucket/wgs/files/md5",
                                    "fs": None})()
        with patch("dt.remote_verify.resolve_cloud_remote",
                   return_value=("wgs", "s3://wgs", fake_odb)), \
             patch("dt.remote_verify.build_expected_sizes",
                   return_value=expected), \
             patch("dt.remote_verify.list_remote_object_sizes",
                   return_value=iter(listing)):
            totals, bad, incomplete, layout, name, url = \
                rv.verify_cloud_remote("wgs")

        assert layout == "dvc-v3"
        assert name == "wgs"
        assert totals["objects"] == 2
        assert totals["ok"] == 1
        assert totals["bad"] == 1
        assert bad[0]["status"] == rv.STATUS_SIZE_MISMATCH
        assert bad[0]["expected_md5"] == truncated

    def test_all_good_passes(self):
        md5 = "9" * 32
        with patch("dt.remote_verify.resolve_cloud_remote",
                   return_value=("r", "s3://r",
                                 type("O", (), {"path": "r/files/md5"})())), \
             patch("dt.remote_verify.build_expected_sizes",
                   return_value={md5: 42}), \
             patch("dt.remote_verify.list_remote_object_sizes",
                   return_value=iter([(_v3_key(md5), 42)])):
            totals, bad, _inc, _layout, _n, _u = rv.verify_cloud_remote("r")
        assert totals["bad"] == 0
        assert bad == []


class TestResolveCloudRemote:
    def test_local_remote_rejected(self):
        with patch("dt.remote.list_remotes",
                   return_value=[("a", "/local/path", True)]):
            with pytest.raises(RemoteError, match="locally-accessible"):
                rv.resolve_cloud_remote("a")

    def test_no_remotes(self):
        with patch("dt.remote.list_remotes", return_value=[]):
            with pytest.raises(RemoteError, match="No remotes configured"):
                rv.resolve_cloud_remote(None)


class TestNoEgressReport:
    def test_build_report_no_egress_has_no_caveat(self):
        totals = {"objects": 1, "ok": 0, "bad": 1, "incomplete": 0,
                  "skipped": 0, "bytes": 128}
        bad = [{"path": _v3_key("8" * 32), "expected_md5": "8" * 32,
                "actual_md5": None, "size_bytes": 128, "expected_size": 500,
                "status": rv.STATUS_SIZE_MISMATCH}]
        # v2 layout would normally carry the dos2unix caveat; size-only must not.
        rep = rv.build_report("wgs", "s3://wgs", "dvc-v2", totals, bad, [],
                              jobs=0, ledger_used=False, size_only=True)
        assert rep["size_only"] is True
        assert rep["mode"] == "no-egress"
        assert rep["legacy_hash_caveat"] is False
        out = rv.format_report_summary(rep)
        assert "no-egress" in out
        assert "size mismatch" in out
        assert "expected_size=500" in out
        assert "actual_size=128" in out


class TestNoEgressCliFlag:
    def _runner(self):
        from click.testing import CliRunner
        return CliRunner()

    def test_no_egress_flag_routes_to_cloud_verify(self):
        from dt.cli import cli
        good = "a" * 32
        bad_md5 = "b" * 32
        fake_odb = type("ODB", (), {"path": "bucket/wgs/files/md5"})()
        with patch("dt.remote_verify.resolve_cloud_remote",
                   return_value=("wgs", "s3://wgs", fake_odb)), \
             patch("dt.remote_verify.build_expected_sizes",
                   return_value={good: 100, bad_md5: 100}), \
             patch("dt.remote_verify.list_remote_object_sizes",
                   return_value=iter([(_v3_key(good), 100),
                                      (_v3_key(bad_md5), 5)])):
            result = self._runner().invoke(
                cli, ['remote', 'verify', 'wgs', '--no-egress'])
        # Bad blobs -> non-zero exit, same as a full verify.
        assert result.exit_code == 1, result.output
        assert "no-egress" in result.output
        assert "Bad:      1" in result.output
