"""Unit tests for dt.tmp_sweep.

Exercised against real directory trees rather than mocks: the behaviour that
matters here is filesystem behaviour (name matching, mtimes, permissions,
symlinks), and mocking it away would test nothing.
"""

import json
import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from dt import tmp_sweep
from dt.errors import CleanError


DAY = 86400.0

# Real names observed on production remotes.
REAL_TMP_NAMES = [
    '.wF9XCrUJvUL4ivKaHJFPdA.tmp',
    '.uhibDiWaUH3sHFz1xGzg3w.tmp',
    '.GAM7bpusP40vRXWKxyasfg.tmp',
    '.EBjO_hH-UQd25Jzo_SGNbQ.tmp',
    '.y8BY1A3_80uaAYFqnIoFNA.tmp',
]


def _store(root: Path, prefixes=('ab', 'cd')) -> Path:
    """Create a minimal DVC v3 blob layout."""
    for p in prefixes:
        (root / 'files' / 'md5' / p).mkdir(parents=True, exist_ok=True)
    return root


def _put(root: Path, prefix: str, name: str, size: int = 10,
         age_days: float = 30.0) -> Path:
    """Write a file into a prefix dir and backdate its mtime."""
    p = root / 'files' / 'md5' / prefix / name
    p.write_bytes(b'x' * size)
    when = time.time() - age_days * DAY
    os.utime(p, (when, when))
    return p


@pytest.fixture
def store(tmp_path):
    return _store(tmp_path / 'remote')


# =============================================================================
# Name matching
# =============================================================================

class TestNameMatching:
    """A loose *.tmp glob would eat real user files, so the shape is exact."""

    @pytest.mark.parametrize('name', REAL_TMP_NAMES)
    def test_accepts_real_observed_names(self, name):
        assert tmp_sweep.is_tmp_name(name)

    @pytest.mark.parametrize('name', [
        'notes.tmp',                      # ordinary user file
        '.tmp',
        '.short.tmp',                     # token too short
        '.wF9XCrUJvUL4ivKaHJFPdAxx.tmp',  # token too long
        '.wF9XCrUJvUL4ivKaHJFPdA.tmp.bak',
        'wF9XCrUJvUL4ivKaHJFPdA.tmp',     # no leading dot
        '.wF9XCrUJvUL4ivKaHJFPd!.tmp',    # illegal character
        'd416f2087cc57e9bf653ccd765fe47',  # a real blob
        '.wF9XCrUJvUL4ivKaHJFPdA.TMP',
    ])
    def test_rejects_everything_else(self, name):
        assert not tmp_sweep.is_tmp_name(name)

    def test_token_length_matches_dvc(self):
        """token_urlsafe(16) yields 22 chars; the regex must agree."""
        from secrets import token_urlsafe
        for _ in range(20):
            assert tmp_sweep.is_tmp_name(f".{token_urlsafe(16)}.tmp")


# =============================================================================
# Scanning
# =============================================================================

class TestScan:

    def test_finds_old_tmp_files(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], size=100, age_days=30)
        _put(store, 'cd', REAL_TMP_NAMES[1], size=200, age_days=30)

        report = tmp_sweep.scan(store)
        assert len(report.candidates) == 2
        assert report.bytes_candidate == 300
        assert report.prefixes_scanned == 2

    def test_ignores_real_blobs(self, store):
        (store / 'files' / 'md5' / 'ab' / 'd416f2087cc57e9bf').write_bytes(b'x')
        _put(store, 'ab', REAL_TMP_NAMES[0])

        report = tmp_sweep.scan(store)
        assert [t.path.name for t in report.candidates] == [REAL_TMP_NAMES[0]]

    def test_ignores_non_matching_tmp_files(self, store):
        p = store / 'files' / 'md5' / 'ab' / 'notes.tmp'
        p.write_bytes(b'important')
        os.utime(p, (time.time() - 400 * DAY,) * 2)

        report = tmp_sweep.scan(store)
        assert report.candidates == []
        assert p.exists()

    def test_min_age_boundary(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], age_days=8)   # older
        _put(store, 'ab', REAL_TMP_NAMES[1], age_days=6)   # younger

        report = tmp_sweep.scan(store, min_age_days=7)
        assert [t.path.name for t in report.candidates] == [REAL_TMP_NAMES[0]]
        assert [t.path.name for t in report.too_recent] == [REAL_TMP_NAMES[1]]

    def test_recent_files_are_reported_not_hidden(self, store):
        """An in-flight transfer must be visible, just not deleted."""
        _put(store, 'ab', REAL_TMP_NAMES[0], size=500, age_days=0.1)
        report = tmp_sweep.scan(store, min_age_days=7)
        assert report.candidates == []
        assert len(report.too_recent) == 1

    def test_min_age_zero_takes_everything(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], age_days=0)
        report = tmp_sweep.scan(store, min_age_days=0)
        assert len(report.candidates) == 1

    def test_only_prefix_dirs_are_walked(self, store):
        """Ledger and quarantine areas are not prefix dirs, so are untouched."""
        ledger = store / '.dt-verify' / 'quarantine'
        ledger.mkdir(parents=True)
        stray = ledger / REAL_TMP_NAMES[0]
        stray.write_bytes(b'x')
        os.utime(stray, (time.time() - 400 * DAY,) * 2)

        report = tmp_sweep.scan(store)
        assert report.candidates == []
        assert stray.exists()

    def test_records_owner_and_age(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], age_days=42)
        tmp = tmp_sweep.scan(store).candidates[0]
        assert tmp.owner_uid == os.getuid()
        assert tmp.owner
        assert 41 < tmp.age_days < 43

    def test_non_dvc_directory_is_an_error_not_a_crash(self, tmp_path):
        plain = tmp_path / 'plain'
        plain.mkdir()
        report = tmp_sweep.scan(plain)
        assert report.error
        assert report.candidates == []

    def test_missing_directory(self, tmp_path):
        report = tmp_sweep.scan(tmp_path / 'nope')
        assert report.error and 'not a directory' in report.error

    def test_v2_layout(self, tmp_path):
        """Older remotes keep prefixes at the top level."""
        root = tmp_path / 'v2'
        (root / 'ab').mkdir(parents=True)
        p = root / 'ab' / REAL_TMP_NAMES[0]
        p.write_bytes(b'x' * 50)
        os.utime(p, (time.time() - 30 * DAY,) * 2)

        report = tmp_sweep.scan(root)
        assert len(report.candidates) == 1

    def test_candidates_sorted_largest_first(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], size=10)
        _put(store, 'ab', REAL_TMP_NAMES[1], size=9000)
        _put(store, 'cd', REAL_TMP_NAMES[2], size=500)

        sizes = [t.size for t in tmp_sweep.scan(store).candidates]
        assert sizes == [9000, 500, 10]


# =============================================================================
# Deletion
# =============================================================================

class TestDelete:

    def test_removes_candidates(self, store):
        a = _put(store, 'ab', REAL_TMP_NAMES[0], size=100)
        b = _put(store, 'cd', REAL_TMP_NAMES[1], size=200)

        report = tmp_sweep.sweep(store, do_delete=True)
        assert not a.exists() and not b.exists()
        assert len(report.deleted) == 2
        assert report.bytes_reclaimed == 300
        assert report.failed == []

    def test_scan_only_deletes_nothing(self, store):
        a = _put(store, 'ab', REAL_TMP_NAMES[0])
        report = tmp_sweep.sweep(store, do_delete=False)
        assert a.exists()
        assert report.deleted == []
        assert len(report.candidates) == 1

    def test_prefix_directories_are_never_removed(self, store):
        """DVC would recreate them with the writer's umask, locking out the
        group -- so an emptied prefix dir must survive."""
        _put(store, 'ab', REAL_TMP_NAMES[0])
        prefix = store / 'files' / 'md5' / 'ab'

        tmp_sweep.sweep(store, do_delete=True)
        assert prefix.is_dir()
        assert list(prefix.iterdir()) == []
        assert (store / 'files' / 'md5' / 'cd').is_dir()

    def test_skips_file_written_since_scan(self, store):
        """Closes the window where a stalled transfer resumed mid-sweep."""
        p = _put(store, 'ab', REAL_TMP_NAMES[0], age_days=30)
        report = tmp_sweep.scan(store)
        assert len(report.candidates) == 1

        # Transfer resumes: content and mtime move.
        p.write_bytes(b'more data')

        tmp_sweep.delete(report)
        assert p.exists()
        assert report.deleted == []
        assert len(report.skipped_changed) == 1

    def test_file_vanishing_between_scan_and_delete_is_not_an_error(self, store):
        p = _put(store, 'ab', REAL_TMP_NAMES[0])
        report = tmp_sweep.scan(store)
        p.unlink()

        tmp_sweep.delete(report)
        assert report.deleted == []
        assert report.failed == []

    @pytest.mark.skipif(os.getuid() == 0, reason="root bypasses permissions")
    def test_permission_failure_is_reported_not_raised(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], size=77)
        prefix = store / 'files' / 'md5' / 'ab'
        report = tmp_sweep.scan(store)

        os.chmod(prefix, 0o555)          # readable, not writable
        try:
            tmp_sweep.delete(report)
            assert report.deleted == []
            assert len(report.failed) == 1
            assert report.failed[0].reason == 'permission denied'
            assert report.bytes_failed == 77
        finally:
            os.chmod(prefix, 0o755)

    @pytest.mark.skipif(os.getuid() == 0, reason="root bypasses permissions")
    def test_partial_failure_still_deletes_the_rest(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0])
        good = _put(store, 'cd', REAL_TMP_NAMES[1])
        report = tmp_sweep.scan(store)

        os.chmod(store / 'files' / 'md5' / 'ab', 0o555)
        try:
            tmp_sweep.delete(report)
            assert not good.exists()
            assert len(report.deleted) == 1
            assert len(report.failed) == 1
        finally:
            os.chmod(store / 'files' / 'md5' / 'ab', 0o755)

    def test_symlink_is_removed_not_followed(self, store, tmp_path):
        """Unlink acts on the link itself; the target must survive."""
        target = tmp_path / 'precious'
        target.write_bytes(b'do not delete')
        link = store / 'files' / 'md5' / 'ab' / REAL_TMP_NAMES[0]
        link.symlink_to(target)
        old = time.time() - 30 * DAY
        os.utime(link, (old, old), follow_symlinks=False)

        report = tmp_sweep.sweep(store, do_delete=True)
        assert len(report.deleted) == 1
        assert not link.exists() and not link.is_symlink()
        assert target.exists() and target.read_bytes() == b'do not delete'


# =============================================================================
# Target resolution
# =============================================================================

class TestResolveTargets:

    def test_explicit_path(self):
        got = tmp_sweep.resolve_remote_targets(path='/some/where')
        assert got[0][1] == Path('/some/where')

    def test_default_remote(self):
        remotes = [('storage', '/g/data/x/remote', True),
                   ('other', '/g/data/y/remote', False)]
        with patch.object(tmp_sweep.remote_mod, 'list_remotes',
                          return_value=remotes):
            got = tmp_sweep.resolve_remote_targets()
        assert got == [('storage', Path('/g/data/x/remote'))]

    def test_named_remote(self):
        remotes = [('storage', '/g/data/x/remote', True),
                   ('other', '/g/data/y/remote', False)]
        with patch.object(tmp_sweep.remote_mod, 'list_remotes',
                          return_value=remotes):
            got = tmp_sweep.resolve_remote_targets(remote_name='other')
        assert got == [('other', Path('/g/data/y/remote'))]

    def test_unknown_remote_name(self):
        with patch.object(tmp_sweep.remote_mod, 'list_remotes',
                          return_value=[('storage', '/x', True)]):
            with pytest.raises(CleanError, match="No remote named"):
                tmp_sweep.resolve_remote_targets(remote_name='nope')

    def test_no_remotes_configured(self):
        with patch.object(tmp_sweep.remote_mod, 'list_remotes',
                          return_value=[]):
            with pytest.raises(CleanError, match='No DVC remotes'):
                tmp_sweep.resolve_remote_targets()

    def test_cloud_remote_is_refused_with_guidance(self):
        """Cloud partial uploads are multipart uploads, not stray files."""
        with patch.object(tmp_sweep.remote_mod, 'list_remotes',
                          return_value=[('gcs', 'gs://bucket/path', True)]):
            with pytest.raises(CleanError, match='lifecycle rule'):
                tmp_sweep.resolve_remote_targets()

    def test_all_requires_a_root(self):
        with patch.object(tmp_sweep.cfg, 'get_value', return_value=None):
            with pytest.raises(CleanError, match='--all needs a remote root'):
                tmp_sweep.resolve_remote_targets(all_remotes=True)

    def test_all_enumerates_dvc_stores_only(self, tmp_path):
        root = tmp_path / 'remotes'
        _store(root / 'projA')
        _store(root / 'projB')
        (root / 'not-a-remote').mkdir(parents=True)
        (root / 'not-a-remote' / 'readme.txt').write_text('hi')

        got = tmp_sweep.resolve_remote_targets(all_remotes=True,
                                               root=str(root))
        assert [n for n, _p in got] == ['projA', 'projB']

    def test_all_with_no_stores(self, tmp_path):
        (tmp_path / 'empty').mkdir()
        with pytest.raises(CleanError, match='No DVC remotes found'):
            tmp_sweep.resolve_remote_targets(all_remotes=True,
                                             root=str(tmp_path / 'empty'))

    def test_cache_target_explicit(self):
        name, path = tmp_sweep.resolve_cache_target('/some/cache')
        assert path == Path('/some/cache')

    def test_cache_target_from_repo(self):
        with patch.object(tmp_sweep.utils, 'get_cache_dir',
                          return_value=Path('/g/data/cache')):
            name, path = tmp_sweep.resolve_cache_target()
        assert path == Path('/g/data/cache')

    def test_cache_target_steps_up_from_files_md5(self):
        """get_cache_dir() returns the odb path, which is already files/md5."""
        with patch.object(tmp_sweep.utils, 'get_cache_dir',
                          return_value=Path('/scratch/cache/proj/files/md5')):
            name, path = tmp_sweep.resolve_cache_target()
        assert path == Path('/scratch/cache/proj')
        assert name == 'proj'

    def test_cache_target_leaves_other_paths_alone(self):
        with patch.object(tmp_sweep.utils, 'get_cache_dir',
                          return_value=Path('/scratch/cache/proj')):
            _name, path = tmp_sweep.resolve_cache_target()
        assert path == Path('/scratch/cache/proj')

    def test_cache_target_outside_repo(self):
        with patch.object(tmp_sweep.utils, 'get_cache_dir', return_value=None):
            with pytest.raises(CleanError, match='Could not determine'):
                tmp_sweep.resolve_cache_target()


# =============================================================================
# Reporting
# =============================================================================

class TestReporting:

    def test_clean_root_is_silent_unless_verbose(self, store):
        report = tmp_sweep.scan(store)
        assert tmp_sweep.format_report(report, deleted_mode=False) == ''
        assert tmp_sweep.format_report(
            report, deleted_mode=False, verbose=True) != ''

    def test_findings_are_always_shown(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0])
        report = tmp_sweep.scan(store)
        out = tmp_sweep.format_report(report, deleted_mode=False)
        assert '1 abandoned .tmp file' in out

    def test_report_groups_by_owner(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], size=100)
        _put(store, 'ab', REAL_TMP_NAMES[1], size=200)
        report = tmp_sweep.scan(store)
        owners = report.by_owner(report.candidates)
        assert sum(n for n, _b in owners.values()) == 2

    def test_summary_tells_you_how_to_delete(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0])
        report = tmp_sweep.scan(store)
        out = tmp_sweep.format_summary([report], deleted_mode=False,
                                       min_age_days=7)
        assert '--delete' in out
        assert 'Nothing was deleted' in out

    def test_summary_when_nothing_found(self, store):
        report = tmp_sweep.scan(store)
        out = tmp_sweep.format_summary([report], deleted_mode=False,
                                       min_age_days=7)
        assert 'No abandoned .tmp files' in out

    def test_summary_after_delete(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], size=1024)
        report = tmp_sweep.sweep(store, do_delete=True)
        out = tmp_sweep.format_summary([report], deleted_mode=True,
                                       min_age_days=7)
        assert 'Removed 1' in out

    @pytest.mark.skipif(os.getuid() == 0, reason="root bypasses permissions")
    def test_failures_grouped_by_directory(self, store):
        """The directory is what governs deletion, so that is what we show."""
        _put(store, 'ab', REAL_TMP_NAMES[0])
        report = tmp_sweep.scan(store)
        prefix = store / 'files' / 'md5' / 'ab'
        os.chmod(prefix, 0o555)
        try:
            tmp_sweep.delete(report)
            out = tmp_sweep.format_report(report, deleted_mode=True)
            assert str(prefix) in out
            assert 'dir owner:' in out
            assert 'files owned by:' in out
        finally:
            os.chmod(prefix, 0o755)

    def test_json_shape(self, store):
        _put(store, 'ab', REAL_TMP_NAMES[0], size=42)
        report = tmp_sweep.scan(store)
        data = json.loads(json.dumps(report.to_dict()))

        assert data['kind'] == 'remote'
        assert data['candidates']['count'] == 1
        assert data['candidates']['bytes'] == 42
        assert data['deleted']['count'] == 0
        assert data['candidates']['by_owner']


class TestSizeFormatting:

    def test_zero_keeps_a_unit(self):
        """A bare '0' in a size column reads as a missing value."""
        assert tmp_sweep._fmt_size(0) == '0B'

    def test_nonzero_delegates(self):
        assert tmp_sweep._fmt_size(1024) == '1.00k'

    def test_zero_byte_file_renders_with_unit(self, store):
        p = store / 'files' / 'md5' / 'ab' / REAL_TMP_NAMES[0]
        p.write_bytes(b'')
        os.utime(p, (time.time() - 30 * DAY,) * 2)

        out = tmp_sweep.format_report(tmp_sweep.scan(store),
                                      deleted_mode=False)
        assert '0B' in out


# =============================================================================
# CLI output
# =============================================================================

class TestCliOutput:
    """Clean roots must not leave gaps in the output."""

    def _run(self, args):
        from click.testing import CliRunner
        from dt.cli import cli
        return CliRunner().invoke(cli, args)

    def test_no_blank_lines_for_clean_remotes(self, tmp_path):
        root = tmp_path / 'remotes'
        clean_names = ['clean1', 'clean2', 'clean3', 'clean4']
        for name in clean_names:
            _store(root / name)
        # only one remote has anything to report
        _put(_store(root / 'dirty'), 'ab', REAL_TMP_NAMES[0], size=99)

        result = self._run(['remote', 'clean', '--all', '--root', str(root)])
        assert result.exit_code == 0

        # Everything before the summary, minus the one blank line that
        # deliberately separates the two.
        head = result.output.split('Found ')[0].rstrip('\n')
        assert head.strip(), "expected the dirty remote to be reported"
        assert '\n\n' not in head, (
            f"clean remotes left blank lines between reports:\n{head!r}"
        )

        lines = result.output.splitlines()
        for name in clean_names:
            assert f'remote: {name}' not in lines
        assert 'remote: dirty' in lines

    def test_verbose_shows_clean_remotes(self, tmp_path):
        root = tmp_path / 'remotes'
        _store(root / 'a')
        result = self._run(['remote', 'clean', '--all', '--root', str(root),
                            '-v'])
        assert result.exit_code == 0
        assert 'remote: a' in result.output

    def test_all_clean_reports_nothing_found(self, tmp_path):
        root = tmp_path / 'remotes'
        _store(root / 'a')
        _store(root / 'b')
        result = self._run(['remote', 'clean', '--all', '--root', str(root)])
        assert result.exit_code == 0
        assert 'No abandoned .tmp files' in result.output

    def test_conflicting_targets_rejected(self, tmp_path):
        result = self._run(['remote', 'clean', '--all', '--path', str(tmp_path)])
        assert result.exit_code != 0
        assert 'only one of' in result.output.lower()

    def test_negative_min_age_rejected(self, tmp_path):
        result = self._run(['cache', 'clean', '--path', str(tmp_path),
                            '--min-age', '-1'])
        assert result.exit_code != 0


# =============================================================================
# Shared behaviour across both commands
# =============================================================================

class TestCacheAndRemoteShareOnePath:

    def test_same_sweep_serves_both_kinds(self, tmp_path):
        cache = _store(tmp_path / 'cache')
        remote = _store(tmp_path / 'remote')
        _put(cache, 'ab', REAL_TMP_NAMES[0], size=11)
        _put(remote, 'ab', REAL_TMP_NAMES[1], size=22)

        c = tmp_sweep.sweep(cache, kind=tmp_sweep.KIND_CACHE, do_delete=True)
        r = tmp_sweep.sweep(remote, kind=tmp_sweep.KIND_REMOTE, do_delete=True)

        assert c.kind == 'cache' and r.kind == 'remote'
        assert c.bytes_reclaimed == 11 and r.bytes_reclaimed == 22
