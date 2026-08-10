"""Unit tests for dt.perms.

Run against real directory trees: the subject is filesystem permission
semantics, so mocking them away would test nothing. The one exception is the
cross-user chmod failure, which cannot be produced as a single user and is
therefore driven by patching os.chmod.
"""

import json
import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest

from dt import perms
from dt.errors import CleanError


def _mkstore(root: Path, n_prefixes: int = 256, mode: int = 0o3770,
             runs: bool = False) -> Path:
    """Create a DVC v3 store with the given number of prefix dirs."""
    md5 = root / 'files' / 'md5'
    md5.mkdir(parents=True, exist_ok=True)
    for i in range(n_prefixes):
        d = md5 / f'{i:02x}'
        d.mkdir(exist_ok=True)
        os.chmod(d, mode)
    if runs:
        (root / 'runs').mkdir(exist_ok=True)
        os.chmod(root / 'runs', mode)
    for d in (root, root / 'files', md5):
        os.chmod(d, mode)
    return root


@pytest.fixture
def store(tmp_path):
    return _mkstore(tmp_path / 'store')


# =============================================================================
# Policy
# =============================================================================

class TestPolicy:

    def test_modes(self):
        # Default policy: sticky, and nothing readable outside the group.
        assert perms.wanted_mode() == 0o3770
        assert perms.wanted_mode(sticky=False) == 0o2770
        assert perms.wanted_mode(allow_other=True) == 0o3775
        assert perms.wanted_mode(sticky=False, allow_other=True) == 0o2775

    def test_defaults(self):
        assert perms.DEFAULT_STICKY is True
        assert perms.DEFAULT_ALLOW_OTHER is False

    def test_sticky_keeps_group_write(self):
        """The whole point: everyone can still create, only owners delete."""
        mode = perms.wanted_mode(sticky=True)
        assert mode & stat.S_IWGRP
        assert mode & stat.S_ISVTX
        assert mode & stat.S_ISGID

    def test_describe_issues(self):
        assert perms.describe_issues(0o2775, 0o2775) == []
        assert 'not group-writable' in perms.describe_issues(0o2755, 0o2775)
        assert 'not setgid' in perms.describe_issues(0o0775, 0o2775)
        assert 'not sticky' in perms.describe_issues(0o2775, 0o3775)
        assert 'readable by others' in perms.describe_issues(0o2775, 0o2770)

    def test_extra_permissiveness_is_not_an_issue(self):
        """Only missing bits count; unrelated extra bits are left alone."""
        assert perms.describe_issues(0o3775, 0o2775) == []


# =============================================================================
# Scanning
# =============================================================================

class TestScan:

    def test_compliant_store_is_ok(self, store):
        report = perms.scan(store)
        assert report.ok
        assert report.findings == []
        assert report.dirs_checked > 256

    def test_detects_non_group_writable(self, tmp_path):
        root = _mkstore(tmp_path / 's', mode=0o2750)
        report = perms.scan(root)
        assert not report.ok
        assert all('not group-writable' in f.issues for f in report.findings)

    def test_detects_missing_setgid(self, tmp_path):
        root = _mkstore(tmp_path / 's', mode=0o1770)
        report = perms.scan(root)
        assert any('not setgid' in f.issues for f in report.findings)

    def test_sticky_required_by_default(self, store):
        assert perms.scan(store).ok
        relaxed = _mkstore(store.parent / 'nosticky', mode=0o2770)
        assert not perms.scan(relaxed).ok
        assert all('not sticky' in f.issues
                   for f in perms.scan(relaxed).findings)
        # ...but only when the policy asks for it
        assert perms.scan(relaxed, sticky=False).ok

    def test_world_readable_flagged_by_default(self, tmp_path):
        root = _mkstore(tmp_path / 's', mode=0o3775)
        assert any('readable by others' in f.issues
                   for f in perms.scan(root).findings)
        # allowed when the policy permits it
        assert perms.scan(root, allow_other=True).ok

    def test_partial_prefix_set_is_reported_as_missing(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=200)
        report = perms.scan(root)
        assert len(report.missing) == 56
        assert report.uninitialised == []

    def test_empty_store_is_uninitialised_not_256_gaps(self, tmp_path):
        """A never-pushed remote shouldn't read as 256 separate faults."""
        root = _mkstore(tmp_path / 's', n_prefixes=0)
        report = perms.scan(root)
        assert report.uninitialised
        assert report.missing == []

    def test_runs_expected_full_only_for_cache(self, tmp_path):
        """A remote may carry an empty runs/; a cache pre-creates it."""
        root = _mkstore(tmp_path / 's', runs=True)

        as_remote = perms.scan(root, kind=perms.KIND_REMOTE)
        assert as_remote.uninitialised == []

        as_cache = perms.scan(root, kind=perms.KIND_CACHE)
        assert as_cache.uninitialised

    def test_v2_root_not_expected_to_be_complete(self, tmp_path):
        """A v2 root holds only the prefixes in use, among other entries."""
        root = tmp_path / 'v2'
        root.mkdir()
        for name in ('00', '01', '02', '03'):
            d = root / name
            d.mkdir()
            os.chmod(d, 0o2775)
        os.chmod(root, 0o2775)

        report = perms.scan(root)
        assert report.missing == []
        assert report.uninitialised == []

    def test_sparse_v2_root_is_detected(self, tmp_path):
        """Probing a fixed handful of names would miss a late-sorting store."""
        root = tmp_path / 'v2'
        root.mkdir()
        for name in ('7f', 'a3'):
            (root / name).mkdir()
            os.chmod(root / name, 0o2750)
        os.chmod(root, 0o2750)

        report = perms.scan(root)
        assert report.error is None
        assert {f.rel for f in report.findings} == {'.', '7f', 'a3'}

    def test_v2_root_counted_once(self, tmp_path):
        """The root is both the store root and a prefix base."""
        root = tmp_path / 'v2'
        root.mkdir()
        (root / '00').mkdir()
        os.chmod(root / '00', 0o2750)
        os.chmod(root, 0o2750)

        rels = [f.rel for f in perms.scan(root).findings]
        assert sorted(rels) == ['.', '00']

    def test_non_dvc_directory(self, tmp_path):
        plain = tmp_path / 'plain'
        plain.mkdir()
        report = perms.scan(plain)
        assert report.error and 'no DVC blob layout' in report.error

    def test_missing_directory(self, tmp_path):
        report = perms.scan(tmp_path / 'nope')
        assert report.error and 'not a directory' in report.error

    def test_records_owner(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2750)
        f = perms.scan(root).findings[0]
        assert f.owner_uid == os.geteuid()
        assert f.mine is True

    def test_checks_the_chain_above_prefixes(self, tmp_path):
        """A non-writable files/md5 blocks creating any missing prefix."""
        root = _mkstore(tmp_path / 's', n_prefixes=4)
        os.chmod(root / 'files' / 'md5', 0o2750)
        report = perms.scan(root)
        assert any(f.rel == 'files/md5' for f in report.findings)


# =============================================================================
# Repair
# =============================================================================

class TestFix:

    def test_applies_mode(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=8, mode=0o2750)
        report = perms.check(root, do_fix=True)

        assert report.unfixed == []
        for i in range(8):
            mode = stat.S_IMODE(os.stat(root / 'files' / 'md5' / f'{i:02x}').st_mode)
            assert mode == 0o3770

    def test_sticky_preserves_group_write(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2770)
        perms.check(root, do_fix=True)
        mode = stat.S_IMODE(os.stat(root / 'files' / 'md5' / '00').st_mode)
        assert mode == 0o3770
        assert mode & stat.S_ISVTX, "sticky must be applied"
        assert mode & stat.S_IWGRP, "group write must survive"

    def test_creates_missing_prefixes(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=200)
        report = perms.check(root, do_fix=True)

        assert sum(1 for m in report.missing if m.created) == 56
        assert len(list((root / 'files' / 'md5').iterdir())) == 256
        mode = stat.S_IMODE(os.stat(root / 'files' / 'md5' / 'ff').st_mode)
        assert mode == 0o3770

    def test_creates_full_set_for_uninitialised_store(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=0)
        perms.check(root, do_fix=True)
        assert len(list((root / 'files' / 'md5').iterdir())) == 256

    def test_fix_is_idempotent(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=16, mode=0o2750)
        perms.check(root, do_fix=True)
        second = perms.check(root, do_fix=True)
        assert second.ok

    def test_chmod_failure_is_reported_not_raised(self, tmp_path):
        """Cross-user chmod always fails; only the owner can repair."""
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2750)
        report = perms.scan(root)
        assert report.findings

        with patch('os.chmod', side_effect=PermissionError(1, 'Operation not permitted')):
            perms.fix(report)

        assert report.fixed == []
        assert len(report.unfixed) == len(report.findings)
        assert 'only the owner' in report.unfixed[0].failure

    def test_mkdir_failure_is_reported(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=100)
        report = perms.scan(root)
        with patch('pathlib.Path.mkdir',
                   side_effect=PermissionError(13, 'Permission denied')):
            perms.fix(report)
        assert all(m.failure for m in report.missing)
        assert not any(m.created for m in report.missing)


# =============================================================================
# Reporting
# =============================================================================

class TestReporting:

    def test_compliant_store_is_silent_unless_verbose(self, store):
        report = perms.scan(store)
        assert perms.format_report(report, fixed_mode=False) == ''
        assert perms.format_report(report, fixed_mode=False, verbose=True)

    def test_reports_grouped_by_owner(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2750)
        report = perms.scan(root)
        out = perms.format_report(report, fixed_mode=False)
        assert 'owner' in out
        assert 'not group-writable' in out

    def test_uninitialised_wording_is_not_alarming(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=0)
        out = perms.format_report(perms.scan(root), fixed_mode=False)
        assert 'not pre-created' in out
        assert '256' not in out

    def test_partial_gap_counts_out_of_256(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=200)
        out = perms.format_report(perms.scan(root), fixed_mode=False)
        assert '56 of 256' in out

    def test_singular_grammar(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4)
        os.chmod(root / 'files' / 'md5' / '00', 0o2750)
        out = perms.format_report(perms.scan(root), fixed_mode=False)
        assert '1 directory deviates' in out

    def test_summary_lists_per_owner_worklist(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2750)
        report = perms.scan(root)
        out = perms.format_summary([report], fixed_mode=False, sticky=False)
        assert 'Needs the owner to run it' in out
        assert '(you)' in out
        assert 'dt remote perms --all --fix' in out

    def test_summary_when_compliant(self, store):
        out = perms.format_summary([perms.scan(store)], fixed_mode=False,
                                   sticky=False)
        assert 'All directories match' in out

    def test_summary_mentions_sticky_flag_when_requested(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2770)
        report = perms.scan(root, sticky=True)
        out = perms.format_summary([report], fixed_mode=False, sticky=True)
        assert '--sticky' in out

    def test_json_shape(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2750)
        data = json.loads(json.dumps(perms.scan(root).to_dict()))
        assert data['wanted'] == '0o3770'
        assert data['deviations']
        assert data['by_owner']
        assert 'uninitialised' in data


# =============================================================================
# Target resolution reuse
# =============================================================================

class TestTargetResolution:
    """Both perms and clean must target identically."""

    def test_delegates_to_the_sweep_module(self):
        with patch('dt.tmp_sweep.resolve_remote_targets',
                   return_value=[('r', Path('/x'))]) as m:
            assert perms.resolve_remote_targets(all_remotes=True) == \
                [('r', Path('/x'))]
        m.assert_called_once()

    def test_cache_delegates(self):
        with patch('dt.tmp_sweep.resolve_cache_target',
                   return_value=('c', Path('/y'))) as m:
            assert perms.resolve_cache_target() == ('c', Path('/y'))
        m.assert_called_once()

    def test_errors_propagate(self):
        with patch('dt.tmp_sweep.resolve_remote_targets',
                   side_effect=CleanError('boom')):
            with pytest.raises(CleanError):
                perms.resolve_remote_targets()


# =============================================================================
# CLI
# =============================================================================

class TestCli:

    def _run(self, args):
        from click.testing import CliRunner
        from dt.cli import cli
        return CliRunner().invoke(cli, args)

    def test_report_then_fix(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=8, mode=0o2750)

        report = self._run(['remote', 'perms', '--path', str(root)])
        assert report.exit_code == 0
        assert 'deviate' in report.output
        # reporting must not modify anything
        assert stat.S_IMODE(os.stat(root / 'files' / 'md5' / '00').st_mode) \
            == 0o2750

        fixed = self._run(['remote', 'perms', '--path', str(root), '--fix'])
        assert fixed.exit_code == 0
        assert stat.S_IMODE(os.stat(root / 'files' / 'md5' / '00').st_mode) \
            == 0o3770

    def test_allow_other_permits_but_never_opens_up(self, tmp_path):
        """--allow-other tolerates world read; it must not grant it."""
        open_store = _mkstore(tmp_path / 'open', n_prefixes=4, mode=0o3775)
        result = self._run(['remote', 'perms', '--path', str(open_store),
                            '--fix', '--allow-other'])
        assert result.exit_code == 0
        assert stat.S_IMODE(
            os.stat(open_store / 'files' / 'md5' / '00').st_mode) == 0o3775

        # A tighter store is never loosened to match the policy.
        tight = _mkstore(tmp_path / 'tight', n_prefixes=4, mode=0o3770)
        result = self._run(['remote', 'perms', '--path', str(tight),
                            '--fix', '--allow-other'])
        assert result.exit_code == 0
        assert stat.S_IMODE(
            os.stat(tight / 'files' / 'md5' / '00').st_mode) == 0o3770

    def test_sticky_can_be_disabled_by_config(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2770)
        with patch('dt.cli.cfg.get_value',
                   side_effect=lambda k, d=None: False
                   if k == 'perms.sticky' else None):
            result = self._run(['remote', 'perms', '--path', str(root),
                                '--fix'])
        assert result.exit_code == 0
        assert stat.S_IMODE(os.stat(root / 'files' / 'md5' / '00').st_mode) \
            == 0o2770

    def test_explicit_flag_overrides_config(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4)
        with patch('dt.cli.cfg.get_value',
                   side_effect=lambda k, d=None: False
                   if k == 'perms.sticky' else None):
            result = self._run(['remote', 'perms', '--path', str(root),
                                '--fix', '--sticky'])
        assert result.exit_code == 0
        assert stat.S_IMODE(os.stat(root / 'files' / 'md5' / '00').st_mode) \
            == 0o3770

    def test_no_other_is_the_default(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o3775)
        result = self._run(['remote', 'perms', '--path', str(root), '--fix'])
        assert result.exit_code == 0
        assert stat.S_IMODE(os.stat(root / 'files' / 'md5' / '00').st_mode) \
            == 0o3770

    def test_clean_root_produces_no_blank_lines(self, tmp_path):
        root = tmp_path / 'remotes'
        # A compliant store needs the full prefix set; a partial one is
        # legitimately reported.
        for name in ('alpha', 'beta', 'gamma'):
            _mkstore(root / name)
        _mkstore(root / 'broken', mode=0o2755)

        result = self._run(['remote', 'perms', '--all', '--root', str(root)])
        assert result.exit_code == 0
        head = result.output.split('\n\n')[0]
        assert 'remote: broken' in head
        for clean in ('alpha', 'beta', 'gamma'):
            assert f'remote: {clean}' not in result.output.splitlines()

    def test_cache_perms(self, tmp_path):
        root = _mkstore(tmp_path / 'cache', n_prefixes=4, mode=0o2750)
        result = self._run(['cache', 'perms', '--path', str(root), '--fix'])
        assert result.exit_code == 0
        assert stat.S_IMODE(os.stat(root / 'files' / 'md5' / '00').st_mode) \
            == 0o3770

    def test_conflicting_targets_rejected(self, tmp_path):
        result = self._run(['remote', 'perms', '--all', '--path',
                            str(tmp_path)])
        assert result.exit_code != 0
        assert 'only one of' in result.output.lower()

    def test_json_output(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2750)
        result = self._run(['remote', 'perms', '--path', str(root), '--json'])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data['policy']['mode'] == '0o3770'
        assert data['roots'][0]['deviations']

    def test_fix_exits_nonzero_when_something_cannot_be_fixed(self, tmp_path):
        root = _mkstore(tmp_path / 's', n_prefixes=4, mode=0o2750)
        with patch('os.chmod',
                   side_effect=PermissionError(1, 'Operation not permitted')):
            result = self._run(['remote', 'perms', '--path', str(root),
                                '--fix'])
        assert result.exit_code != 0
