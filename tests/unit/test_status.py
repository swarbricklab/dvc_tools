"""Tests for dt status module."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from dt import status as status_mod


class TestStatusFunction:
    """Tests for the status function interface."""

    def test_status_function_exists(self):
        assert callable(status_mod.status)

    def test_status_signature(self):
        import inspect
        sig = inspect.signature(status_mod.status)
        params = list(sig.parameters.keys())
        assert 'targets' in params
        assert 'imports' in params
        assert 'verbose' in params
        assert 'dvc_args' in params

    @patch('dt.status.subprocess.run')
    @patch('dt.status.utils.check_dvc')
    @patch('dt.status.index_mod', create=True)
    def test_status_calls_dvc_status(self, mock_index, mock_check_dvc, mock_run):
        """Status calls dvc status."""
        mock_run.return_value = MagicMock(returncode=0)
        # Make index import work
        with patch('dt.status.index_mod') as mock_idx:
            mock_idx.is_auto_sync_enabled.return_value = False
            rc = status_mod.status()
        assert rc == 0

    @patch('dt.status.subprocess.run')
    @patch('dt.status.utils.check_dvc')
    def test_status_passes_dvc_args(self, mock_check_dvc, mock_run):
        """Extra args are passed through to dvc status."""
        mock_run.return_value = MagicMock(returncode=0)
        # Index sync will fail silently (no real repo), which is fine
        status_mod.status(dvc_args=['--granular'])

        # Find the call that runs 'dvc status ...'
        dvc_status_calls = [
            c for c in mock_run.call_args_list
            if c[0][0][:2] == ['dvc', 'status']
        ]
        assert len(dvc_status_calls) >= 1
        cmd = dvc_status_calls[0][0][0]
        assert '--granular' in cmd
