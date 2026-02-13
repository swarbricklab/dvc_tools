"""Tests for dt.data_status module."""

from unittest.mock import patch, MagicMock

import pytest

from dt.data_status import data_status, data_status_via_qxub
from dt.errors import DataStatusError


class TestDataStatus:
    """Tests for data_status (local execution)."""

    @patch('dt.data_status.dvc_utils.with_checksum_jobs')
    @patch('dt.data_status.subprocess.run')
    def test_basic_run(self, mock_run, mock_ctx):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=None)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        mock_run.return_value = MagicMock(returncode=0)

        rc = data_status()
        assert rc == 0
        mock_run.assert_called_once_with(['dvc', 'data', 'status'])

    @patch('dt.data_status.dvc_utils.with_checksum_jobs')
    @patch('dt.data_status.subprocess.run')
    def test_with_threads(self, mock_run, mock_ctx):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=48)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        mock_run.return_value = MagicMock(returncode=0)

        rc = data_status(threads=48)
        assert rc == 0
        mock_ctx.assert_called_once_with(48, verbose=False)

    @patch('dt.data_status.dvc_utils.with_checksum_jobs')
    @patch('dt.data_status.subprocess.run')
    def test_with_dvc_args(self, mock_run, mock_ctx):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=None)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        mock_run.return_value = MagicMock(returncode=0)

        rc = data_status(dvc_args=['--granular', '--json'])
        assert rc == 0
        mock_run.assert_called_once_with(
            ['dvc', 'data', 'status', '--granular', '--json'],
        )

    @patch('dt.data_status.dvc_utils.with_checksum_jobs')
    @patch('dt.data_status.subprocess.run')
    def test_nonzero_return(self, mock_run, mock_ctx):
        mock_ctx.return_value.__enter__ = MagicMock(return_value=None)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        mock_run.return_value = MagicMock(returncode=1)

        rc = data_status()
        assert rc == 1

    @patch('dt.data_status.cfg.get_value', return_value='192')
    def test_invalid_thread_count(self, mock_cfg):
        with pytest.raises(DataStatusError, match='at least 1'):
            data_status(threads=0)

    @patch('dt.data_status.cfg.get_value', return_value='48')
    def test_excessive_thread_count(self, mock_cfg):
        with pytest.raises(DataStatusError, match='exceeds maximum'):
            data_status(threads=100)


class TestDataStatusViaQxub:
    """Tests for data_status_via_qxub (compute node delegation)."""

    @patch('dt.data_status.dvc_utils.check_qxub', return_value=False)
    def test_raises_when_qxub_missing(self, mock_qxub):
        with pytest.raises(DataStatusError, match='qxub not found'):
            data_status_via_qxub()

    @patch('dt.data_status.dvc_utils.submit_via_qxub', return_value=None)
    @patch('dt.data_status.cfg.get_value', return_value='192')
    @patch('dt.data_status.dvc_utils.check_qxub', return_value=True)
    def test_submits_job_wait(self, mock_qxub, mock_cfg, mock_submit):
        result = data_status_via_qxub(threads=48, wait=True)
        assert result is None
        mock_submit.assert_called_once()
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs['job_name'] == 'dt-data-status'
        assert call_kwargs['wait'] is True
        assert '--worker' in call_kwargs['worker_cmd']

    @patch('dt.data_status.dvc_utils.submit_via_qxub',
           return_value='12345.gadi-pbs')
    @patch('dt.data_status.cfg.get_value', return_value='192')
    @patch('dt.data_status.dvc_utils.check_qxub', return_value=True)
    def test_submits_job_no_wait(self, mock_qxub, mock_cfg, mock_submit):
        result = data_status_via_qxub(threads=48, wait=False)
        assert result == '12345.gadi-pbs'
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs['wait'] is False

    @patch('dt.data_status.dvc_utils.submit_via_qxub')
    @patch('dt.data_status.cfg.get_value', return_value='192')
    @patch('dt.data_status.dvc_utils.check_qxub', return_value=True)
    def test_passes_dvc_args(self, mock_qxub, mock_cfg, mock_submit):
        mock_submit.return_value = None
        data_status_via_qxub(dvc_args=['--granular'])
        call_kwargs = mock_submit.call_args[1]
        assert '--granular' in call_kwargs['worker_cmd']

    @patch('dt.data_status.dvc_utils.submit_via_qxub')
    @patch('dt.data_status.cfg.get_value', return_value='192')
    @patch('dt.data_status.dvc_utils.check_qxub', return_value=True)
    def test_threads_in_worker_cmd(self, mock_qxub, mock_cfg, mock_submit):
        mock_submit.return_value = None
        data_status_via_qxub(threads=24)
        call_kwargs = mock_submit.call_args[1]
        cmd = call_kwargs['worker_cmd']
        idx = cmd.index('--threads')
        assert cmd[idx + 1] == '24'

    @patch('dt.data_status.cfg.get_value', return_value='48')
    @patch('dt.data_status.dvc_utils.check_qxub', return_value=True)
    def test_invalid_thread_count(self, mock_qxub, mock_cfg):
        with pytest.raises(DataStatusError, match='at least 1'):
            data_status_via_qxub(threads=0)

    @patch('dt.data_status.cfg.get_value', return_value='48')
    @patch('dt.data_status.dvc_utils.check_qxub', return_value=True)
    def test_excessive_thread_count(self, mock_qxub, mock_cfg):
        with pytest.raises(DataStatusError, match='exceeds maximum'):
            data_status_via_qxub(threads=100)
