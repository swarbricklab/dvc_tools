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


    def test_ignores_manifest_not_matching_its_name(self, tmp_path):
        remote = tmp_path / "remote"
        child = _place_blob(remote, b"member\n")
        dir_rel = _place_dir_manifest(remote, [(child, "sub/member.txt")])
        (remote / dir_rel).write_bytes(
            json.dumps([{"md5": child, "relpath": "other"}]).encode())
        assert q.build_dir_reverse_map(remote) == {}


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


class TestReadListFile:
    def test_strips_blanks_comments_and_columns(self, tmp_path):
        listfile = tmp_path / "bad.txt"
        listfile.write_text(
            "# externally-identified bad R2 blobs\n"
            "\n"
            "   \n"
            "aa" + "1" * 30 + "\n"
            "bb" + "2" * 30 + "   1234   # truncated upstream\n"
            "files/md5/cc/" + "3" * 30 + "\n"
            "  # another comment\n"
        )
        tokens = q.read_list_file(listfile)
        assert tokens == [
            "aa" + "1" * 30,
            "bb" + "2" * 30,
            "files/md5/cc/" + "3" * 30,
        ]

    def test_missing_file_raises(self, tmp_path):
        from dt.errors import RemoteError
        with pytest.raises(RemoteError):
            q.read_list_file(tmp_path / "does-not-exist.txt")


class TestEntriesFromList:
    def test_md5_token_maps_to_v3_path(self, tmp_path):
        remote = tmp_path / "remote"
        md5 = _place_blob(remote, b"member\n")  # real v3 blob
        entries, rejected = q.entries_from_list(remote, [md5])
        assert rejected == []
        assert entries == [{
            'path': f"files/md5/{md5[:2]}/{md5[2:]}",
            'expected_md5': md5,
            'status': rv.STATUS_MISMATCH,
        }]

    def test_dir_md5_token_keeps_suffix_and_strips_for_md5(self, tmp_path):
        remote = tmp_path / "remote"
        child = _place_blob(remote, b"member\n")
        dir_rel = _place_dir_manifest(remote, [(child, "sub/m.txt")])
        dir_md5 = dir_rel.split('/')[-1][:-4]  # strip .dir
        # reconstruct full 32-hex (prefix + rest)
        dir_md5 = dir_rel.split('/')[-2] + dir_md5
        entries, rejected = q.entries_from_list(remote, [dir_md5 + ".dir"])
        assert rejected == []
        assert entries[0]['path'] == dir_rel
        assert entries[0]['expected_md5'] == dir_md5

    def test_blob_path_token(self, tmp_path):
        remote = tmp_path / "remote"
        md5 = _place_blob(remote, b"member\n")
        rel = f"files/md5/{md5[:2]}/{md5[2:]}"
        entries, rejected = q.entries_from_list(remote, [rel])
        assert rejected == []
        assert entries[0] == {'path': rel, 'expected_md5': md5,
                              'status': rv.STATUS_MISMATCH}

    def test_absolute_path_under_remote(self, tmp_path):
        remote = tmp_path / "remote"
        md5 = _place_blob(remote, b"member\n")
        rel = f"files/md5/{md5[:2]}/{md5[2:]}"
        entries, rejected = q.entries_from_list(remote, [str(remote / rel)])
        assert rejected == []
        assert entries[0]['path'] == rel

    def test_garbage_is_rejected(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"member\n")  # gives remote a detectable layout
        entries, rejected = q.entries_from_list(
            remote, ["not-an-md5", "zz" + "0" * 30, "README.md"])
        assert entries == []
        assert {t for t, _ in rejected} == {"not-an-md5", "zz" + "0" * 30,
                                            "README.md"}

    def test_absolute_path_outside_remote_rejected(self, tmp_path):
        remote = tmp_path / "remote"
        _place_blob(remote, b"member\n")
        entries, rejected = q.entries_from_list(
            remote, ["/etc/passwd"])
        assert entries == []
        assert rejected and "outside" in rejected[0][1]

    def test_dedup_by_path(self, tmp_path):
        remote = tmp_path / "remote"
        md5 = _place_blob(remote, b"member\n")
        rel = f"files/md5/{md5[:2]}/{md5[2:]}"
        entries, _ = q.entries_from_list(remote, [md5, rel])
        assert len(entries) == 1


class TestExternalListQuarantineEndToEnd:
    def test_external_member_pulls_enclosing_dir(self, tmp_path):
        """An externally-known bad member quarantines itself AND its .dir."""
        remote = tmp_path / "remote"
        # A truncated member: content differs but the key stays correct, so we
        # register it at a forced md5 name (as the real truncated blob would be).
        bad_md5 = "ff" + "0" * 30
        _place_blob(remote, b"truncated", name=bad_md5)
        dir_rel = _place_dir_manifest(remote, [(bad_md5, "data/x.bin")])
        bad_rel = f"files/md5/{bad_md5[:2]}/{bad_md5[2:]}"

        # External list naming just the bad member md5.
        listfile = tmp_path / "bad.txt"
        listfile.write_text(bad_md5 + "\n")
        tokens = q.read_list_file(listfile)
        entries, rejected = q.entries_from_list(remote, tokens)
        assert rejected == []

        result = q.quarantine(remote, entries, timestamp="20260801T000000Z")
        moved_paths = {m['path'] for m in result['moved']}
        assert bad_rel in moved_paths
        assert dir_rel in moved_paths          # enclosing .dir pulled in
        assert not (remote / bad_rel).exists()
        assert not (remote / dir_rel).exists()

        # Reversible.
        restored = q.restore_quarantine(remote, "20260801T000000Z")
        assert bad_rel in restored['restored']
        assert dir_rel in restored['restored']
        assert (remote / bad_rel).exists()
        assert (remote / dir_rel).exists()


class TestQuarantineStaysInsideRemote:
    def test_paths_outside_layout_are_not_planned(self, tmp_path):
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'data\n')
        outside = tmp_path / 'home' / 'keep.txt'
        outside.parent.mkdir()
        outside.write_text('mine')
        entries = [
            {'path': '../home/keep.txt', 'status': rv.STATUS_MISMATCH},
            {'path': str(outside), 'status': rv.STATUS_MISMATCH},
            {'path': f'files/md5/{good[:2]}/{good[2:]}', 'expected_md5': good,
             'status': rv.STATUS_MISMATCH},
        ]
        plan = q.plan_quarantine(remote, entries)
        assert [p['path'] for p in plan] == [f'files/md5/{good[:2]}/{good[2:]}']
        assert outside.read_text() == 'mine'


class TestQuarantineNeverFollowsLinks:
    def _victim(self, tmp_path):
        v = tmp_path / 'home' / 'private.cfg'
        v.parent.mkdir()
        v.write_text('secret=1\n')
        import os
        os.chmod(v, 0o600)
        return v

    def _state(self, p):
        import os
        return (p.read_text(), os.stat(p).st_mode & 0o7777)

    def test_failed_rename_does_not_touch_link_target(self, tmp_path):
        import errno, os
        from unittest.mock import patch
        from dt.errors import RemoteError
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'data\n')
        victim = self._victim(tmp_path)
        rel = f'files/md5/{good[:2]}/{"f" * 30}'
        (remote / rel).symlink_to(victim)
        before = self._state(victim)
        entries = [{'path': rel, 'status': rv.STATUS_SYMLINK}]
        real_rename = os.rename

        def eperm(src, dst, *a, **k):
            if str(src).endswith('f' * 30):
                raise PermissionError(errno.EPERM, 'sticky')
            return real_rename(src, dst, *a, **k)

        with patch('dt.remote_quarantine.os.rename', side_effect=eperm):
            with pytest.raises(RemoteError):
                q.quarantine(remote, entries=entries)
        assert self._state(victim) == before

    def test_cross_device_moves_link_as_link(self, tmp_path):
        import errno, os
        from unittest.mock import patch
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'data\n')
        victim = self._victim(tmp_path)
        rel = f'files/md5/{good[:2]}/{"f" * 30}'
        (remote / rel).symlink_to(victim)
        before = self._state(victim)

        def exdev(src, dst, *a, **k):
            raise OSError(errno.EXDEV, 'cross-device')

        with patch('dt.remote_quarantine.os.rename', side_effect=exdev):
            res = q.quarantine(remote, entries=[{'path': rel, 'status': rv.STATUS_SYMLINK}])
        moved_to = Path(res['quarantine_dir']) / rel
        assert os.path.islink(moved_to) and not os.path.lexists(remote / rel)
        assert self._state(victim) == before

    def test_linked_quarantine_root_is_refused(self, tmp_path):
        import os
        from dt.errors import RemoteError
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'data\n')
        elsewhere = tmp_path / 'elsewhere'
        elsewhere.mkdir()
        (remote / rv.LEDGER_DIRNAME).mkdir(exist_ok=True)
        (remote / rv.LEDGER_DIRNAME / q.QUARANTINE_DIRNAME).symlink_to(elsewhere)
        entries = [{'path': f'files/md5/{good[:2]}/{good[2:]}', 'status': rv.STATUS_MISMATCH}]
        with pytest.raises(RemoteError, match='real directory'):
            q.quarantine(remote, entries=entries, confirm=False)
        assert list(elsewhere.iterdir()) == []

    def test_restore_ignores_forged_manifest_paths(self, tmp_path):
        import json, os
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'data\n')
        res = q.quarantine(remote, entries=[{'path': f'files/md5/{good[:2]}/{good[2:]}',
                                             'status': rv.STATUS_MISMATCH}],
                           confirm=False)
        qdir = Path(res['quarantine_dir'])
        outside = tmp_path / 'home' / 'keep.txt'
        outside.parent.mkdir()
        outside.write_text('mine')
        man = qdir / q.MANIFEST_FILENAME
        data = json.loads(man.read_text())
        data['entries'].append({'path': '../../../home/keep.txt'})
        man.write_text(json.dumps(data))
        out = q.restore_quarantine(remote, qdir.name)
        assert outside.read_text() == 'mine'
        assert '../../../home/keep.txt' in out['absent']
        assert f'files/md5/{good[:2]}/{good[2:]}' in out['restored']



class TestRestoreOnlyOwnBatches:
    def _batch(self, tmp_path):
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'data\n')
        rel = f'files/md5/{good[:2]}/{good[2:]}'
        res = q.quarantine(remote, entries=[{'path': rel, 'status': rv.STATUS_MISMATCH}],
                           confirm=False)
        return remote, Path(res['quarantine_dir']), rel

    def test_new_batch_is_not_writable_by_others(self, tmp_path):
        import os, stat
        _, qdir, _ = self._batch(tmp_path)
        for p in (qdir, qdir / q.MANIFEST_FILENAME):
            assert not os.stat(p).st_mode & (stat.S_IWGRP | stat.S_IWOTH)

    def test_other_users_batch_is_refused(self, tmp_path, monkeypatch):
        import os
        remote, qdir, rel = self._batch(tmp_path)
        real = os.getuid()
        monkeypatch.setattr(os, 'getuid', lambda: real + 1)
        with pytest.raises(q.RemoteError, match='another user'):
            q.restore_quarantine(remote, qdir.name)
        assert not (remote / rel).exists()

    @pytest.mark.parametrize('target', ['dir', 'manifest'])
    def test_group_writable_batch_is_refused(self, tmp_path, target):
        import os
        remote, qdir, rel = self._batch(tmp_path)
        path = qdir if target == 'dir' else qdir / q.MANIFEST_FILENAME
        os.chmod(path, 0o775 if target == 'dir' else 0o664)
        with pytest.raises(q.RemoteError, match='writable by others'):
            q.restore_quarantine(remote, qdir.name)
        assert not (remote / rel).exists()

    def test_restore_does_not_follow_linked_remote_dir(self, tmp_path):
        import os, shutil
        remote, qdir, rel = self._batch(tmp_path)
        prefix = remote / rel.rsplit('/', 1)[0]
        elsewhere = tmp_path / 'elsewhere'
        elsewhere.mkdir()
        shutil.rmtree(prefix)
        os.symlink(elsewhere, prefix)
        with pytest.raises(q.RemoteError):
            q.restore_quarantine(remote, qdir.name)
        assert list(elsewhere.iterdir()) == []

    def test_restore_does_not_follow_linked_batch_subdir(self, tmp_path):
        """A link inside the batch is never moved through."""
        import os, shutil
        remote, qdir, rel = self._batch(tmp_path)
        sub = qdir / rel.rsplit('/', 1)[0]
        moved = tmp_path / 'moved'
        shutil.move(str(sub), str(moved))
        os.symlink(moved, sub)
        out = q.restore_quarantine(remote, qdir.name)
        assert rel in out['absent']
        assert not (remote / rel).exists()


    @pytest.mark.parametrize('part', ['ledger', 'root'])
    def test_quarantine_never_chmods_or_writes_through_links(self, tmp_path, part):
        import os, stat
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'data\n')
        rel = f'files/md5/{good[:2]}/{good[2:]}'
        victim = tmp_path / 'victim'
        victim.mkdir(mode=0o700)
        before = stat.S_IMODE(os.stat(victim).st_mode)
        if part == 'ledger':
            os.symlink(victim, remote / rv.LEDGER_DIRNAME)
        else:
            (remote / rv.LEDGER_DIRNAME).mkdir()
            os.symlink(victim, remote / rv.LEDGER_DIRNAME / q.QUARANTINE_DIRNAME)
        with pytest.raises(q.RemoteError):
            q.quarantine(remote, entries=[{'path': rel, 'status': rv.STATUS_MISMATCH}],
                         confirm=False)
        assert stat.S_IMODE(os.stat(victim).st_mode) == before
        assert list(victim.iterdir()) == []
        assert (remote / rel).exists()


    def test_manifest_written_when_a_move_fails(self, tmp_path, monkeypatch):
        remote = tmp_path / 'remote'
        a = _place_blob(remote, b'one\n')
        b = _place_blob(remote, b'two\n')
        rels = [f'files/md5/{h[:2]}/{h[2:]}' for h in (a, b)]
        real_move = q._move_entry
        calls = []

        def flaky(sdir, name, ddir):
            calls.append(name)
            if len(calls) == 2:
                raise OSError(5, 'boom')
            real_move(sdir, name, ddir)

        monkeypatch.setattr(q, '_move_entry', flaky)
        with pytest.raises(q.RemoteError):
            q.quarantine(remote, entries=[{'path': r, 'status': rv.STATUS_MISMATCH}
                                          for r in rels], confirm=False)
        [batch] = q.list_quarantines(remote)
        assert batch['count'] == 1
        monkeypatch.setattr(q, '_move_entry', real_move)
        out = q.restore_quarantine(remote, batch['timestamp'])
        assert len(out['restored']) == 1


class TestReportEntriesAreConfirmed:
    def test_forged_entry_for_intact_blob_is_refused(self, tmp_path):
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'healthy\n')
        rel = f'files/md5/{good[:2]}/{good[2:]}'
        res = q.quarantine(remote, entries=[{'path': rel, 'status': rv.STATUS_MISMATCH,
                                             'expected_md5': good}])
        assert (remote / rel).exists()
        assert res['moved'] == []
        assert res['refused'][0]['path'] == rel

    def test_genuinely_corrupt_blob_is_still_quarantined(self, tmp_path):
        remote = tmp_path / 'remote'
        good = _place_blob(remote, b'healthy\n')
        rel = f'files/md5/{good[:2]}/{good[2:]}'
        p = remote / rel
        p.chmod(0o644)
        p.write_bytes(b'corrupted')
        res = q.quarantine(remote, entries=[{'path': rel, 'status': rv.STATUS_MISMATCH,
                                             'expected_md5': good}])
        assert not p.exists()
        assert [m['path'] for m in res['moved']] == [rel]

    @pytest.mark.parametrize('rel', ['../../etc/passwd', '/etc/passwd',
                                     'files/md5/ab/../../../../home/x'])
    def test_out_of_tree_report_paths_move_nothing(self, tmp_path, rel):
        remote = tmp_path / 'remote'
        _place_blob(remote, b'healthy\n')
        res = q.quarantine(remote, entries=[{'path': rel, 'status': rv.STATUS_MISMATCH}])
        assert res['moved'] == []

    def test_from_list_rejects_dotdot_tokens(self, tmp_path):
        remote = tmp_path / 'remote'
        _place_blob(remote, b'healthy\n')
        entries, rejected = q.entries_from_list(
            remote, ['files/md5/ab/../../../home/' + 'c' * 30, 'ab/..'])
        assert entries == []
        assert len(rejected) == 2
