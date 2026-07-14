"""Unit tests for dt remote_fsck (symlinked-blob scan/repair)."""

import hashlib
import os
from pathlib import Path

import pytest

from dt import remote_fsck, utils


def _blob(root: Path, md5: str, dirobj: bool = False) -> Path:
    name = md5[2:] + (".dir" if dirobj else "")
    p = root / "files" / "md5" / md5[:2] / name
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


@pytest.fixture
def remote_with_symlinks(tmp_path):
    """A v3 remote containing: one good symlink, one dangling, one md5-mismatch,
    plus a real regular blob that must be ignored. Returns (remote, cache, md5s).
    """
    remote = tmp_path / "remote"
    cache = tmp_path / "cache"
    remote.mkdir()
    cache.mkdir()

    # (1) resolvable symlink whose target hashes to the path-implied md5
    content = b'[{"md5":"abc","relpath":"a.txt"}]'
    md5_ok = utils.md5_bytes(content)
    target = _blob(cache, md5_ok, dirobj=True)
    target.write_bytes(content)
    os.symlink(target, _blob(remote, md5_ok, dirobj=True))

    # (2) dangling symlink
    md5_dangle = hashlib.md5(b"gone").hexdigest()
    os.symlink(cache / "missing.dir", _blob(remote, md5_dangle, dirobj=True))

    # (3) resolvable symlink whose target content does NOT match its path md5
    md5_bad = hashlib.md5(b"expected").hexdigest()
    wrong = cache / "wrong.bin"
    wrong.write_bytes(b"totally different content")
    os.symlink(wrong, _blob(remote, md5_bad))

    # (4) a real regular blob — must be ignored by the scan
    md5_real = hashlib.md5(b"regular").hexdigest()
    _blob(remote, md5_real).write_bytes(b"regular")

    return remote, cache, {
        "ok": md5_ok, "dangle": md5_dangle, "bad": md5_bad, "real": md5_real,
    }


class TestScan:
    def test_finds_only_symlinks(self, remote_with_symlinks):
        remote, _cache, _ = remote_with_symlinks
        layout, findings = remote_fsck.scan_remote_symlinks(remote)
        assert layout == "dvc-v3"
        # 3 symlinks; the real regular blob is not reported.
        assert len(findings) == 3
        assert sum(f.resolves for f in findings) == 2
        assert sum(not f.resolves for f in findings) == 1

    def test_finding_metadata(self, remote_with_symlinks):
        remote, _cache, md5s = remote_with_symlinks
        _layout, findings = remote_fsck.scan_remote_symlinks(remote)
        ok = next(f for f in findings if f.expected_md5 == md5s["ok"])
        assert ok.is_dir_object is True
        assert ok.resolves is True
        assert ok.rel.startswith("files/md5/")


class TestRepair:
    def test_repairs_only_verified_targets(self, remote_with_symlinks):
        remote, _cache, md5s = remote_with_symlinks
        # Drive scan+repair directly (no remote config needed).
        _layout, findings = remote_fsck.scan_remote_symlinks(remote)
        me = os.getuid()
        for f in findings:
            remote_fsck._repair_one(f, me)

        ok = next(f for f in findings if f.expected_md5 == md5s["ok"])
        dang = next(f for f in findings if not f.resolves)
        bad = next(f for f in findings if f.expected_md5 == md5s["bad"])

        # Good one: replaced with a real, read-only file that hashes correctly.
        assert ok.repaired is True
        assert not os.path.islink(ok.path)
        assert utils.md5_file(ok.path) == md5s["ok"]
        assert (ok.path.stat().st_mode & 0o777) == 0o444

        # Dangling: left untouched, reported as unrecoverable.
        assert dang.repaired is False
        assert "dangling" in dang.outcome
        assert os.path.islink(dang.path)

        # md5 mismatch: refused so we never write wrong content into a hash slot.
        assert bad.repaired is False
        assert bad.md5_ok is False
        assert os.path.islink(bad.path)

    def test_skips_symlinks_owned_by_others(self, remote_with_symlinks):
        remote, _cache, md5s = remote_with_symlinks
        _layout, findings = remote_fsck.scan_remote_symlinks(remote)
        # Simulate the good symlink being owned by another user.
        ok = next(f for f in findings if f.expected_md5 == md5s["ok"])
        ok.owner_uid = os.getuid() + 12345
        ok.owner = "someone_else"

        remote_fsck._repair_one(ok, os.getuid())

        assert ok.repaired is False
        assert "owned by someone_else" in ok.outcome
        assert os.path.islink(ok.path), "must not touch another user's symlink"

    def test_clean_remote_reports_nothing(self, tmp_path):
        remote = tmp_path / "remote"
        md5 = hashlib.md5(b"data").hexdigest()
        _blob(remote, md5).write_bytes(b"data")
        _layout, findings = remote_fsck.scan_remote_symlinks(remote)
        assert findings == []


class TestReport:
    def test_to_dict_shape(self, remote_with_symlinks):
        remote, _cache, _ = remote_with_symlinks
        layout, findings = remote_fsck.scan_remote_symlinks(remote)
        report = remote_fsck.FsckReport(
            remote_name="r", remote_path=str(remote), layout=layout,
            current_uid=os.getuid(), findings=findings,
        )
        d = report.to_dict()
        assert d["total_symlinks"] == 3
        assert d["resolvable"] == 2
        assert d["dangling"] == 1
        assert len(d["findings"]) == 3

    def test_other_owners_summary(self, remote_with_symlinks):
        remote, _cache, md5s = remote_with_symlinks
        layout, findings = remote_fsck.scan_remote_symlinks(remote)
        # Re-attribute two findings to two other users (one resolvable, one not).
        other = os.getuid() + 999
        ok = next(f for f in findings if f.expected_md5 == md5s["ok"])
        dang = next(f for f in findings if not f.resolves)
        ok.owner_uid, ok.owner = other, "alice"
        dang.owner_uid, dang.owner = other, "alice"

        report = remote_fsck.FsckReport(
            remote_name="r", remote_path=str(remote), layout=layout,
            current_uid=os.getuid(), findings=findings,
        )
        summary = report.other_owners()
        assert summary == {"alice": {"total": 2, "repairable": 1}}
        assert len(report.owned_by_others) == 2
