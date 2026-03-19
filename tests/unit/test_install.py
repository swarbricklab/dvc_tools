"""Tests for dt install module."""

import json
import os
import stat
import subprocess
import textwrap
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dt import install
from dt.errors import HookError, InstallError


@pytest.fixture(autouse=True)
def _isolate_config(tmp_path, monkeypatch):
    """Prevent system/user config from leaking into tests."""
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg_home'))
    monkeypatch.setenv('XDG_CONFIG_DIRS', str(tmp_path / 'xdg_dirs'))


# =============================================================================
# parse_size / format_size
# =============================================================================

class TestParseSize:
    """Tests for parse_size."""

    @pytest.mark.parametrize('input_str,expected', [
        ('0', 0),
        ('100', 100),
        ('100B', 100),
        ('1KB', 1024),
        ('50MB', 50 * 1024 ** 2),
        ('1GB', 1024 ** 3),
        ('1.5GB', int(1.5 * 1024 ** 3)),
        ('500 KB', 500 * 1024),
        ('  10MB  ', 10 * 1024 ** 2),
    ])
    def test_valid_sizes(self, input_str, expected):
        assert install.parse_size(input_str) == expected

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Cannot parse size"):
            install.parse_size('abc')

    def test_unknown_unit(self):
        with pytest.raises(ValueError, match="Unknown size unit"):
            install.parse_size('50XB')


class TestFormatSize:
    """Tests for format_size."""

    def test_bytes(self):
        assert install.format_size(100) == '100B'

    def test_megabytes(self):
        result = install.format_size(50 * 1024 ** 2)
        assert 'MB' in result

    def test_gigabytes(self):
        result = install.format_size(2 * 1024 ** 3)
        assert 'GB' in result


# =============================================================================
# _is_dt_hook
# =============================================================================

class TestIsDtHook:
    """Tests for _is_dt_hook."""

    def test_dt_hook_detected(self, tmp_path):
        hook_file = tmp_path / 'pre-commit'
        hook_file.write_text('#!/bin/sh\nexec dt hook run pre-commit "$@"\n')
        assert install._is_dt_hook(hook_file) is True

    def test_other_hook_not_detected(self, tmp_path):
        hook_file = tmp_path / 'pre-commit'
        hook_file.write_text('#!/bin/sh\nexec dvc git-hook pre-commit $@\n')
        assert install._is_dt_hook(hook_file) is False

    def test_nonexistent_file(self, tmp_path):
        hook_file = tmp_path / 'missing'
        assert install._is_dt_hook(hook_file) is False


# =============================================================================
# install / uninstall
# =============================================================================

class TestInstall:
    """Tests for install and uninstall."""

    @pytest.fixture
    def git_repo(self, tmp_path, monkeypatch):
        """Create a minimal git repo with .dt directory."""
        subprocess.run(['git', 'init', str(tmp_path)], capture_output=True)
        (tmp_path / '.dt').mkdir(exist_ok=True)
        monkeypatch.chdir(tmp_path)
        return tmp_path

    def test_install_creates_hooks(self, git_repo):
        installed = install.install()
        hooks_dir = git_repo / '.git' / 'hooks'
        for hook_name in install.HOOK_NAMES:
            hook_path = hooks_dir / hook_name
            assert hook_path.exists(), f"{hook_name} not created"
            assert 'dt hook run' in hook_path.read_text()
            assert hook_path.stat().st_mode & stat.S_IEXEC

        assert set(installed) == set(install.HOOK_NAMES)

    def test_install_writes_default_config(self, git_repo):
        install.install()
        config_path = git_repo / '.dt' / 'config.local.yaml'
        assert config_path.exists()
        content = config_path.read_text()
        assert 'hooks' in content
        assert 'large-files' in content

    def test_install_refuses_existing_foreign_hook(self, git_repo):
        hooks_dir = git_repo / '.git' / 'hooks'
        hooks_dir.mkdir(parents=True, exist_ok=True)
        (hooks_dir / 'pre-commit').write_text('#!/bin/sh\necho custom\n')

        with pytest.raises(InstallError, match="already exists"):
            install.install()

    def test_install_force_overwrites(self, git_repo):
        hooks_dir = git_repo / '.git' / 'hooks'
        hooks_dir.mkdir(parents=True, exist_ok=True)
        (hooks_dir / 'pre-commit').write_text('#!/bin/sh\necho custom\n')

        installed = install.install(force=True)
        assert 'pre-commit' in installed
        assert 'dt hook run' in (hooks_dir / 'pre-commit').read_text()

    def test_install_idempotent(self, git_repo):
        install.install()
        # Should succeed again since hooks are ours
        installed = install.install()
        assert set(installed) == set(install.HOOK_NAMES)

    def test_uninstall_removes_dt_hooks(self, git_repo):
        install.install()
        removed = install.uninstall()
        assert set(removed) == set(install.HOOK_NAMES)

        hooks_dir = git_repo / '.git' / 'hooks'
        for hook_name in install.HOOK_NAMES:
            assert not (hooks_dir / hook_name).exists()

    def test_uninstall_skips_foreign_hooks(self, git_repo):
        hooks_dir = git_repo / '.git' / 'hooks'
        hooks_dir.mkdir(parents=True, exist_ok=True)
        (hooks_dir / 'pre-commit').write_text('#!/bin/sh\necho custom\n')

        removed = install.uninstall()
        assert 'pre-commit' not in removed
        assert (hooks_dir / 'pre-commit').exists()  # untouched

    def test_uninstall_noop_when_no_hooks(self, git_repo):
        removed = install.uninstall()
        assert removed == []


# =============================================================================
# check_large_files
# =============================================================================

class TestCheckLargeFiles:
    """Tests for check_large_files."""

    @pytest.fixture
    def staged_repo(self, tmp_path, monkeypatch):
        """Git repo with a staged file."""
        subprocess.run(['git', 'init', str(tmp_path)], capture_output=True)
        monkeypatch.chdir(tmp_path)
        return tmp_path

    def test_no_staged_files_passes(self, staged_repo):
        assert install.check_large_files('1MB') is True

    def test_small_file_passes(self, staged_repo):
        small = staged_repo / 'small.txt'
        small.write_text('hello')
        subprocess.run(['git', 'add', 'small.txt'], capture_output=True, cwd=staged_repo)

        assert install.check_large_files('1MB') is True

    def test_large_file_raises(self, staged_repo):
        large = staged_repo / 'big.bin'
        large.write_bytes(b'\0' * (1024 + 1))
        subprocess.run(['git', 'add', 'big.bin'], capture_output=True, cwd=staged_repo)

        with pytest.raises(HookError, match="exceed.*limit") as exc_info:
            install.check_large_files('1KB')
        msg = str(exc_info.value)
        assert 'dt add' in msg
        assert '--no-verify' in msg

    def test_dvc_file_excluded(self, staged_repo):
        dvc_file = staged_repo / 'data.csv.dvc'
        dvc_file.write_bytes(b'\0' * 2048)
        subprocess.run(['git', 'add', 'data.csv.dvc'], capture_output=True, cwd=staged_repo)

        # Should pass because .dvc files are excluded
        assert install.check_large_files('1KB') is True


# =============================================================================
# _get_checks
# =============================================================================

class TestGetChecks:
    """Tests for check resolution from config."""

    def test_returns_empty_when_no_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()
        (tmp_path / '.dt').mkdir()
        checks = install._get_checks('pre-commit')
        assert checks == []

    def test_reads_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()
        dt_dir = tmp_path / '.dt'
        dt_dir.mkdir()

        import yaml
        config_data = {
            'hooks': {
                'pre-commit': {
                    'checks': {
                        'large-files': {
                            'enabled': True,
                            'mode': 'sync',
                            'max_size': '100MB',
                        },
                    },
                },
            },
        }
        (dt_dir / 'config.yaml').write_text(yaml.safe_dump(config_data))

        checks = install._get_checks('pre-commit')
        assert len(checks) == 1
        assert checks[0]['name'] == 'large-files'
        assert checks[0]['max_size'] == '100MB'


# =============================================================================
# hook_run
# =============================================================================

class TestHookRun:
    """Tests for hook_run."""

    def test_no_checks_configured_passes(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()
        (tmp_path / '.dt').mkdir()
        assert install.hook_run('pre-commit') is True

    @patch('dt.install._get_checks')
    def test_sync_check_failure_raises(self, mock_get_checks):
        mock_get_checks.return_value = [{
            'name': 'test-check',
            'enabled': True,
            'mode': 'sync',
            'command': 'false',  # always fails
        }]

        with pytest.raises(HookError, match="failed"):
            install.hook_run('pre-commit')

    @patch('dt.install._get_checks')
    def test_async_check_skipped(self, mock_get_checks):
        mock_get_checks.return_value = [{
            'name': 'slow-check',
            'enabled': True,
            'mode': 'async',
            'command': 'false',
        }]

        # Async checks are deferred — should not fail
        assert install.hook_run('pre-commit', verbose=True) is True

    @patch('dt.install._get_checks')
    def test_disabled_check_skipped(self, mock_get_checks):
        mock_get_checks.return_value = [{
            'name': 'disabled-check',
            'enabled': False,
            'mode': 'sync',
            'command': 'false',
        }]

        assert install.hook_run('pre-commit') is True

    @patch('dt.install._get_checks')
    def test_external_check_success(self, mock_get_checks):
        mock_get_checks.return_value = [{
            'name': 'true-check',
            'enabled': True,
            'mode': 'sync',
            'command': 'true',  # always succeeds
        }]

        assert install.hook_run('pre-commit') is True


# =============================================================================
# hook_list
# =============================================================================

class TestHookList:
    """Tests for hook_list."""

    def test_returns_all_hook_names(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()
        (tmp_path / '.dt').mkdir()

        result = install.hook_list()
        assert set(result.keys()) == set(install.HOOK_NAMES)

    def test_returns_configured_checks(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()
        dt_dir = tmp_path / '.dt'
        dt_dir.mkdir()

        import yaml
        config_data = {
            'hooks': {
                'pre-commit': {
                    'checks': {
                        'dvc-status': {'enabled': True, 'mode': 'sync'},
                        'large-files': {'enabled': True, 'mode': 'sync', 'max_size': '1MB'},
                    },
                },
            },
        }
        (dt_dir / 'config.yaml').write_text(yaml.safe_dump(config_data))

        result = install.hook_list()
        pre_commit_checks = result['pre-commit']
        names = [c['name'] for c in pre_commit_checks]
        assert 'dvc-status' in names
        assert 'large-files' in names


# =============================================================================
# Phase 2: Async dispatch, run_check, hook results
# =============================================================================

class TestGetHookResultsDir:
    """Tests for _get_hook_results_dir."""

    def test_creates_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()
        with patch.object(install.utils, 'find_git_root', return_value=tmp_path):
            results_dir = install._get_hook_results_dir()
        assert results_dir == tmp_path / '.dt' / 'hook-results'
        assert results_dir.is_dir()

    def test_uses_cwd_fallback(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with patch.object(install.utils, 'find_git_root', return_value=None):
            results_dir = install._get_hook_results_dir()
        assert results_dir == tmp_path / '.dt' / 'hook-results'
        assert results_dir.is_dir()


class TestSaveHookResult:
    """Tests for _save_hook_result."""

    def test_saves_json_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with patch.object(install, '_get_hook_results_dir',
                          return_value=tmp_path / 'results'):
            (tmp_path / 'results').mkdir(parents=True)
            path = install._save_hook_result(
                'dvc-status', 'pre-commit', True, 'all clean',
            )
        assert path.exists()
        assert path.suffix == '.json'

        import json
        data = json.loads(path.read_text())
        assert data['check'] == 'dvc-status'
        assert data['hook'] == 'pre-commit'
        assert data['passed'] is True
        assert data['output'] == 'all clean'
        assert 'timestamp' in data

    def test_saves_failure_result(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with patch.object(install, '_get_hook_results_dir',
                          return_value=tmp_path / 'results'):
            (tmp_path / 'results').mkdir(parents=True)
            path = install._save_hook_result(
                'large-files', 'pre-commit', False, 'file too big',
            )
        import json
        data = json.loads(path.read_text())
        assert data['passed'] is False
        assert data['output'] == 'file too big'


class TestDispatchAsyncCheck:
    """Tests for _dispatch_async_check."""

    def test_returns_none_when_qxub_unavailable(self):
        with patch('dt.install.hpc') as mock_hpc:
            mock_hpc.check_qxub.return_value = False
            result = install._dispatch_async_check(
                'test-check', 'pre-commit', {}, [],
            )
        assert result is None

    def test_submits_via_qxub(self):
        with patch('dt.install.hpc') as mock_hpc, \
             patch.object(install.subprocess, 'run') as mock_run:
            mock_hpc.check_qxub.return_value = True
            mock_hpc.build_qxub_command.return_value = ['qxub', 'exec', '--', 'dt', 'hook', 'run-check', 'pre-commit', 'test-check', '--worker']
            mock_run.return_value = MagicMock(
                returncode=0, stdout='12345.gadi-pbs\n', stderr='',
            )

            result = install._dispatch_async_check(
                'test-check', 'pre-commit', {}, [],
            )

        assert result == '12345.gadi-pbs'
        mock_hpc.build_qxub_command.assert_called_once()
        # Verify worker command includes --worker
        call_args = mock_hpc.build_qxub_command.call_args
        worker_cmd = call_args[0][1]  # second positional arg
        assert '--worker' in worker_cmd

    def test_returns_none_on_submission_failure(self):
        with patch('dt.install.hpc') as mock_hpc, \
             patch.object(install.subprocess, 'run') as mock_run:
            mock_hpc.check_qxub.return_value = True
            mock_hpc.build_qxub_command.return_value = ['qxub', 'exec', '--']
            mock_run.return_value = MagicMock(
                returncode=1, stdout='', stderr='error',
            )

            result = install._dispatch_async_check(
                'test-check', 'pre-commit', {}, [],
            )

        assert result is None

    def test_passes_hook_args(self):
        with patch('dt.install.hpc') as mock_hpc, \
             patch.object(install.subprocess, 'run') as mock_run:
            mock_hpc.check_qxub.return_value = True
            mock_hpc.build_qxub_command.return_value = ['qxub', 'exec', '--']
            mock_run.return_value = MagicMock(
                returncode=0, stdout='12345\n', stderr='',
            )

            install._dispatch_async_check(
                'dvc-checkout', 'post-checkout', {},
                ['abc123', 'def456', '1'],
            )

        call_args = mock_hpc.build_qxub_command.call_args
        worker_cmd = call_args[0][1]
        assert '--' in worker_cmd
        idx = worker_cmd.index('--')
        assert worker_cmd[idx + 1:] == ['abc123', 'def456', '1']


class TestRunCheck:
    """Tests for run_check (worker-side)."""

    def test_raises_for_unknown_check(self):
        with patch.object(install, '_get_checks', return_value=[]):
            with pytest.raises(HookError, match="not found"):
                install.run_check('pre-commit', 'nonexistent')

    def test_runs_external_command_pass(self, tmp_path):
        check = {
            'name': 'my-lint',
            'enabled': True,
            'mode': 'async',
            'command': 'echo OK',
            'max_size': '1MB',
        }
        with patch.object(install, '_get_checks', return_value=[check]), \
             patch.object(install, '_save_hook_result', return_value=tmp_path / 'r.json') as mock_save:
            result = install.run_check('pre-commit', 'my-lint')

        assert result is True
        mock_save.assert_called_once()
        args = mock_save.call_args
        assert args[0][0] == 'my-lint'  # check_name
        assert args[0][2] is True       # passed

    def test_runs_external_command_fail(self, tmp_path):
        check = {
            'name': 'my-lint',
            'enabled': True,
            'mode': 'async',
            'command': 'exit 1',
            'max_size': '1MB',
        }
        with patch.object(install, '_get_checks', return_value=[check]), \
             patch.object(install, '_save_hook_result', return_value=tmp_path / 'r.json') as mock_save:
            result = install.run_check('pre-commit', 'my-lint')

        assert result is False
        args = mock_save.call_args
        assert args[0][2] is False  # passed

    def test_runs_builtin_check(self, tmp_path):
        check = {
            'name': 'dvc-status',
            'enabled': True,
            'mode': 'async',
            'command': None,
            'max_size': '1MB',
        }
        with patch.object(install, '_get_checks', return_value=[check]), \
             patch.object(install, '_run_builtin_check', return_value=True), \
             patch.object(install, '_save_hook_result', return_value=tmp_path / 'r.json') as mock_save:
            result = install.run_check('pre-commit', 'dvc-status')

        assert result is True
        mock_save.assert_called_once()

    def test_captures_hook_error_from_builtin(self, tmp_path):
        check = {
            'name': 'large-files',
            'enabled': True,
            'mode': 'async',
            'command': None,
            'max_size': '1MB',
        }
        with patch.object(install, '_get_checks', return_value=[check]), \
             patch.object(install, '_run_builtin_check',
                          side_effect=HookError('file too big')), \
             patch.object(install, '_save_hook_result', return_value=tmp_path / 'r.json') as mock_save:
            result = install.run_check('pre-commit', 'large-files')

        assert result is False
        args = mock_save.call_args
        assert args[0][2] is False
        assert 'file too big' in args[0][3]


class TestListHookResults:
    """Tests for list_hook_results."""

    def test_empty_results(self, tmp_path):
        results_dir = tmp_path / 'results'
        results_dir.mkdir()
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            results = install.list_hook_results()
        assert results == []

    def test_returns_results_sorted(self, tmp_path):
        import json
        results_dir = tmp_path / 'results'
        results_dir.mkdir()

        for i, name in enumerate(['20240101_000000', '20240102_000000', '20240103_000000']):
            data = {
                'check': f'check-{i}',
                'hook': 'pre-commit',
                'passed': True,
                'timestamp': f'2024-01-0{i+1}T00:00:00',
                'output': '',
            }
            (results_dir / f'{name}-pre-commit-check-{i}.json').write_text(
                json.dumps(data),
            )

        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            results = install.list_hook_results()

        assert len(results) == 3
        # Most recent first
        assert results[0]['check'] == 'check-2'
        assert results[2]['check'] == 'check-0'

    def test_respects_limit(self, tmp_path):
        import json
        results_dir = tmp_path / 'results'
        results_dir.mkdir()

        for i in range(5):
            data = {'check': f'c{i}', 'hook': 'h', 'passed': True,
                    'timestamp': '', 'output': ''}
            (results_dir / f'2024010{i}_000000-h-c{i}.json').write_text(
                json.dumps(data),
            )

        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            results = install.list_hook_results(limit=2)
        assert len(results) == 2

    def test_skips_invalid_json(self, tmp_path):
        results_dir = tmp_path / 'results'
        results_dir.mkdir()
        (results_dir / 'bad.json').write_text('not json')

        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            results = install.list_hook_results()
        assert results == []


class TestClearHookResults:
    """Tests for clear_hook_results."""

    def test_clears_all(self, tmp_path):
        import json
        results_dir = tmp_path / 'results'
        results_dir.mkdir()
        for i in range(3):
            (results_dir / f'result_{i}.json').write_text(json.dumps({}))

        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            removed = install.clear_hook_results()
        assert removed == 3
        assert list(results_dir.glob('*.json')) == []

    def test_clears_older_than_days(self, tmp_path):
        import json
        import time
        results_dir = tmp_path / 'results'
        results_dir.mkdir()

        old_file = results_dir / 'old.json'
        old_file.write_text(json.dumps({}))
        # Set mtime to 10 days ago
        old_time = time.time() - (10 * 86400)
        os.utime(old_file, (old_time, old_time))

        new_file = results_dir / 'new.json'
        new_file.write_text(json.dumps({}))

        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            removed = install.clear_hook_results(older_than_days=5)
        assert removed == 1
        assert new_file.exists()
        assert not old_file.exists()


class TestHookRunAsync:
    """Tests for async dispatch path in hook_run."""

    def test_async_check_dispatches(self, tmp_path, monkeypatch):
        """hook_run dispatches async checks via _dispatch_async_check."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()

        checks = [
            {'name': 'my-async-check', 'enabled': True, 'mode': 'async',
             'command': None, 'max_size': '1MB'},
        ]
        with patch.object(install, '_get_checks', return_value=checks), \
             patch.object(install, '_dispatch_async_check',
                          return_value='12345.gadi-pbs') as mock_dispatch:
            result = install.hook_run('pre-commit')

        assert result is True
        mock_dispatch.assert_called_once_with(
            'my-async-check', 'pre-commit', checks[0], [], verbose=False,
        )

    def test_async_check_skipped_when_dispatch_fails(self, tmp_path, monkeypatch):
        """hook_run continues if async dispatch returns None."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()

        checks = [
            {'name': 'async-check', 'enabled': True, 'mode': 'async',
             'command': None, 'max_size': '1MB'},
            {'name': 'sync-check', 'enabled': True, 'mode': 'sync',
             'command': 'true', 'max_size': '1MB'},
        ]
        with patch.object(install, '_get_checks', return_value=checks), \
             patch.object(install, '_dispatch_async_check', return_value=None), \
             patch.object(install.subprocess, 'run',
                          return_value=MagicMock(returncode=0, stdout='', stderr='')):
            result = install.hook_run('pre-commit')

        assert result is True


# =============================================================================
# Unread hook results
# =============================================================================

class TestUnreadResults:
    """Tests for count_unread_results, mark_results_read, and the reminder."""

    def _make_results_dir(self, tmp_path):
        """Create a fake hook-results dir with two JSON files."""
        results_dir = tmp_path / 'results'
        results_dir.mkdir()
        for name in ('a.json', 'b.json'):
            (results_dir / name).write_text(json.dumps({'check': name}))
        return results_dir

    def test_all_unread_when_no_sentinel(self, tmp_path):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            assert install.count_unread_results() == 2

    def test_mark_results_read_creates_sentinel(self, tmp_path):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            install.mark_results_read()
            sentinel = results_dir / install.LAST_READ_SENTINEL
            assert sentinel.exists()

    def test_zero_unread_after_mark(self, tmp_path):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            install.mark_results_read()
            assert install.count_unread_results() == 0

    def test_new_result_after_mark_is_unread(self, tmp_path):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            install.mark_results_read()
            # Write a new result and set its mtime after the sentinel
            new_file = results_dir / 'c.json'
            new_file.write_text(json.dumps({'check': 'c'}))
            sentinel = results_dir / install.LAST_READ_SENTINEL
            future = sentinel.stat().st_mtime + 10
            os.utime(new_file, (future, future))
            assert install.count_unread_results() == 1

    def test_list_hook_results_marks_read(self, tmp_path):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            install.list_hook_results()
            assert install.count_unread_results() == 0

    def test_list_unread_only(self, tmp_path):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            # Mark current results as read
            install.mark_results_read()

            # Set sentinel mtime in the past
            sentinel = results_dir / install.LAST_READ_SENTINEL
            past = sentinel.stat().st_mtime - 20
            os.utime(sentinel, (past, past))

            # Set one file's mtime in the future (unread)
            future = past + 30
            new = results_dir / 'c.json'
            new.write_text(json.dumps({'check': 'c', 'hook': 'h',
                                       'passed': True, 'timestamp': 'T',
                                       'output': ''}))
            os.utime(new, (future, future))
            # Set the other files' mtime in the past (read)
            for name in ('a.json', 'b.json'):
                f = results_dir / name
                os.utime(f, (past - 10, past - 10))

            unread = install.list_hook_results(unread_only=True)
            assert len(unread) == 1
            assert unread[0]['check'] == 'c'

            # Reset sentinel so we can test all mode
            os.utime(sentinel, (past, past))
            os.utime(new, (future, future))
            for name in ('a.json', 'b.json'):
                os.utime(results_dir / name, (past - 10, past - 10))
            all_results = install.list_hook_results(unread_only=False)
            assert len(all_results) == 3

    def test_print_unread_reminder(self, tmp_path, capsys):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            install._print_unread_reminder(install.VERBOSITY_NORMAL)
            output = capsys.readouterr().out
            assert '2 unread hook reports' in output
            assert 'dt hook results' in output

    def test_no_reminder_when_quiet(self, tmp_path, capsys):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            install._print_unread_reminder(install.VERBOSITY_QUIET)
            assert capsys.readouterr().out == ''

    def test_no_reminder_when_all_read(self, tmp_path, capsys):
        results_dir = self._make_results_dir(tmp_path)
        with patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            install.mark_results_read()
            install._print_unread_reminder(install.VERBOSITY_NORMAL)
            assert capsys.readouterr().out == ''

    def test_hook_run_prints_unread_reminder(self, tmp_path, monkeypatch, capsys):
        """hook_run calls _print_unread_reminder after checks complete."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.git').mkdir()
        results_dir = tmp_path / '.dt' / 'hook-results'
        results_dir.mkdir(parents=True)
        (results_dir / 'x.json').write_text(json.dumps({'check': 'x'}))

        checks = [
            {'name': 'my-check', 'enabled': True, 'mode': 'sync',
             'command': 'true'},
        ]
        with patch.object(install, '_get_checks', return_value=checks), \
             patch.object(install.subprocess, 'run',
                          return_value=MagicMock(returncode=0, stdout='', stderr='')), \
             patch.object(install, '_get_hook_results_dir', return_value=results_dir):
            install.hook_run('pre-commit')

        output = capsys.readouterr().out
        assert 'unread hook report' in output
