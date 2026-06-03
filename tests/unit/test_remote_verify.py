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
