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

        totals, bad, layout = rv.verify_remote(remote, jobs=2)

        assert layout == "dvc-v3"
        assert totals["objects"] == 2
        assert totals["ok"] == 2
        assert totals["bad"] == 0
        assert bad == []

    def test_detects_corrupt_blob(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"good\n")
        # A blob whose content does not match its name (truncated transfer).
        _place_blob(remote, b"truncated", name="ff" + "0" * 30)

        totals, bad, _ = rv.verify_remote(remote, jobs=2)

        assert totals["objects"] == 2
        assert totals["ok"] == 1
        assert totals["bad"] == 1
        assert len(bad) == 1
        assert bad[0]["status"] == rv.STATUS_MISMATCH
        assert bad[0]["path"] == "files/md5/ff/" + "0" * 30
        assert bad[0]["expected_md5"] == "ff" + "0" * 30
        assert bad[0]["actual_md5"] != bad[0]["expected_md5"]

    def test_only_prefixes_restricts_walk(self, tmp_path):
        remote = tmp_path / "remote"
        h = _place_blob(remote, b"good\n")
        _place_blob(remote, b"truncated", name="ff" + "0" * 30)

        # Restrict to the bad blob's prefix only.
        totals, bad, _ = rv.verify_remote(
            remote, jobs=1, only_prefixes={"ff"})
        assert totals["objects"] == 1
        assert totals["bad"] == 1

        # Restrict to the good blob's prefix only.
        totals2, bad2, _ = rv.verify_remote(
            remote, jobs=1, only_prefixes={h[:2]})
        assert totals2["objects"] == 1
        assert totals2["bad"] == 0


# =============================================================================
# build_report / format_report_summary
# =============================================================================

class TestReport:
    def test_build_report_v3_no_caveat(self):
        rep = rv.build_report(
            "r", "/p", "dvc-v3", {"objects": 1, "ok": 1, "bad": 0, "bytes": 5},
            [], jobs=4)
        assert rep["legacy_hash_caveat"] is False
        assert rep["remote"] == "r"
        assert rep["totals"]["ok"] == 1

    def test_build_report_v2_has_caveat(self):
        rep = rv.build_report(
            "r", "/p", "dvc-v2", {"objects": 0, "ok": 0, "bad": 0, "bytes": 0},
            [], jobs=1)
        assert rep["legacy_hash_caveat"] is True

    def test_format_summary_lists_bad(self):
        rep = rv.build_report(
            "r", "/p", "dvc-v3", {"objects": 1, "ok": 0, "bad": 1, "bytes": 9},
            [{"path": "files/md5/ff/x", "expected_md5": "ffx",
              "actual_md5": "zzz", "size_bytes": 9,
              "status": rv.STATUS_MISMATCH}], jobs=1)
        out = rv.format_report_summary(rep)
        assert "Bad:      1" in out
        assert "files/md5/ff/x" in out


# =============================================================================
# _merge_parts
# =============================================================================

class TestMergeParts:
    def test_merges_partial_reports(self, tmp_path):
        (tmp_path / "part_0.json").write_text(json.dumps({
            "totals": {"objects": 2, "ok": 2, "bad": 0, "bytes": 10},
            "bad": []}))
        (tmp_path / "part_1.json").write_text(json.dumps({
            "totals": {"objects": 3, "ok": 2, "bad": 1, "bytes": 20},
            "bad": [{"path": "files/md5/ff/x", "status": "mismatch"}]}))

        totals, bad = rv._merge_parts(tmp_path)

        assert totals == {"objects": 5, "ok": 4, "bad": 1, "bytes": 30}
        assert len(bad) == 1


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
