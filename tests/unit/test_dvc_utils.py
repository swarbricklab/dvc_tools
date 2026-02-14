"""Tests for dt.dvc_utils module."""

from unittest.mock import patch, MagicMock, call

import pytest

from dt import dvc_utils


class TestGetChecksumJobs:
    """Tests for get_checksum_jobs."""

    def test_returns_int_when_set(self):
        mock_result = MagicMock(returncode=0, stdout='16\n')
        with patch('dt.dvc_utils.subprocess.run', return_value=mock_result):
            assert dvc_utils.get_checksum_jobs() == 16

    def test_returns_none_when_not_set(self):
        mock_result = MagicMock(returncode=1, stdout='')
        with patch('dt.dvc_utils.subprocess.run', return_value=mock_result):
            assert dvc_utils.get_checksum_jobs() is None

    def test_returns_none_on_empty_output(self):
        mock_result = MagicMock(returncode=0, stdout='')
        with patch('dt.dvc_utils.subprocess.run', return_value=mock_result):
            assert dvc_utils.get_checksum_jobs() is None


class TestSetChecksumJobs:
    """Tests for set_checksum_jobs."""

    def test_sets_value_correctly(self):
        with patch('dt.dvc_utils.subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            dvc_utils.set_checksum_jobs(32)
            mock_run.assert_called_once_with(
                ['dvc', 'config', '--local', 'core.checksum_jobs', '32'],
                check=True,
                capture_output=True,
            )


class TestUnsetChecksumJobs:
    """Tests for unset_checksum_jobs."""

    def test_unsets_value(self):
        with patch('dt.dvc_utils.subprocess.run') as mock_run:
            dvc_utils.unset_checksum_jobs()
            mock_run.assert_called_once_with(
                ['dvc', 'config', '--local', '--unset', 'core.checksum_jobs'],
                capture_output=True,
            )


class TestWithChecksumJobs:
    """Tests for with_checksum_jobs context manager."""

    @patch('dt.dvc_utils.unset_checksum_jobs')
    @patch('dt.dvc_utils.set_checksum_jobs')
    @patch('dt.dvc_utils.get_checksum_jobs', return_value=None)
    def test_sets_and_unsets(self, mock_get, mock_set, mock_unset):
        with dvc_utils.with_checksum_jobs(24):
            mock_set.assert_called_once_with(24)
        mock_unset.assert_called_once()

    @patch('dt.dvc_utils.set_checksum_jobs')
    @patch('dt.dvc_utils.get_checksum_jobs', return_value=8)
    def test_restores_original(self, mock_get, mock_set):
        with patch('dt.dvc_utils.unset_checksum_jobs'):
            with dvc_utils.with_checksum_jobs(24):
                pass
        # Should set 24, then unset, then restore 8
        assert mock_set.call_args_list == [call(24), call(8)]

    def test_none_threads_is_noop(self):
        with patch('dt.dvc_utils.set_checksum_jobs') as mock_set:
            with dvc_utils.with_checksum_jobs(None) as t:
                assert t is None
            mock_set.assert_not_called()

    @patch('dt.dvc_utils.unset_checksum_jobs')
    @patch('dt.dvc_utils.set_checksum_jobs')
    @patch('dt.dvc_utils.get_checksum_jobs', return_value=None)
    def test_unsets_on_exception(self, mock_get, mock_set, mock_unset):
        with pytest.raises(ValueError):
            with dvc_utils.with_checksum_jobs(24):
                raise ValueError("boom")
        mock_unset.assert_called_once()


class TestCountFiles:
    """Tests for count_files."""

    def test_single_file(self, tmp_path):
        f = tmp_path / 'test.txt'
        f.write_text('hi')
        assert dvc_utils.count_files(str(f)) == 1

    def test_directory_recursive(self, tmp_path):
        (tmp_path / 'sub').mkdir()
        (tmp_path / 'a.txt').write_text('a')
        (tmp_path / 'sub' / 'b.txt').write_text('b')
        assert dvc_utils.count_files(str(tmp_path)) == 2

    def test_empty_directory(self, tmp_path):
        empty = tmp_path / 'empty'
        empty.mkdir()
        assert dvc_utils.count_files(str(empty)) == 0

    def test_nonexistent_returns_one(self, tmp_path):
        assert dvc_utils.count_files(str(tmp_path / 'nope')) == 1


class TestCalculateResources:
    """Tests for calculate_resources."""

    @patch('dt.dvc_utils.cfg.get_value', side_effect=lambda k, d=None: d)
    def test_defaults(self, mock_cfg):
        res = dvc_utils.calculate_resources(48)
        assert res['threads'] == 48
        assert res['cpus'] == 12  # 48 / 4
        assert res['mem_gb'] == 48  # 48 * 1
        assert res['mem_str'] == '48GB'

    @patch('dt.dvc_utils.cfg.get_value', side_effect=lambda k, d=None: d)
    def test_none_threads_uses_max(self, mock_cfg):
        res = dvc_utils.calculate_resources(None)
        assert res['threads'] == 192

    @patch('dt.dvc_utils.cfg.get_value', side_effect=lambda k, d=None: d)
    def test_capped_to_file_count(self, mock_cfg):
        res = dvc_utils.calculate_resources(100, file_count=10)
        assert res['threads'] == 10

    @patch('dt.dvc_utils.cfg.get_value', side_effect=lambda k, d=None: d)
    def test_min_one_cpu(self, mock_cfg):
        res = dvc_utils.calculate_resources(1)
        assert res['cpus'] == 1


class TestCheckQxub:
    """Tests for check_qxub."""

    def test_available(self):
        with patch('dt.dvc_utils.shutil.which', return_value='/usr/bin/qxub'):
            assert dvc_utils.check_qxub() is True

    def test_missing(self):
        with patch('dt.dvc_utils.shutil.which', return_value=None):
            assert dvc_utils.check_qxub() is False


class TestSubmitViaQxub:
    """Tests for submit_via_qxub."""

    @patch('dt.dvc_utils.cfg.get_value', side_effect=lambda k, d=None: d)
    @patch('dt.dvc_utils.check_qxub', return_value=False)
    def test_raises_when_missing(self, mock_qxub, mock_cfg):
        with pytest.raises(RuntimeError, match='qxub not found'):
            dvc_utils.submit_via_qxub(
                job_name='test',
                worker_cmd=['dt', 'test'],
            )

    @patch('dt.dvc_utils.subprocess.run')
    @patch('dt.dvc_utils.cfg.get_value', side_effect=lambda k, d=None: d)
    @patch('dt.dvc_utils.check_qxub', return_value=True)
    def test_no_wait_returns_job_id(self, mock_qxub, mock_cfg, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0, stdout='12345.gadi-pbs\n',
        )
        job_id = dvc_utils.submit_via_qxub(
            job_name='test',
            worker_cmd=['dt', 'test'],
            wait=False,
        )
        assert job_id == '12345.gadi-pbs'

    @patch('dt.dvc_utils.subprocess.run')
    @patch('dt.dvc_utils.cfg.get_value', side_effect=lambda k, d=None: d)
    @patch('dt.dvc_utils.check_qxub', return_value=True)
    def test_wait_returns_none(self, mock_qxub, mock_cfg, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        result = dvc_utils.submit_via_qxub(
            job_name='test',
            worker_cmd=['dt', 'test'],
            wait=True,
        )
        assert result is None

    @patch('dt.dvc_utils.subprocess.run')
    @patch('dt.dvc_utils.cfg.get_value', side_effect=lambda k, d=None: d)
    @patch('dt.dvc_utils.check_qxub', return_value=True)
    def test_custom_error_class(self, mock_qxub, mock_cfg, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1, stderr='PBS error',
        )

        class MyError(Exception):
            pass

        with pytest.raises(MyError, match='Failed to submit'):
            dvc_utils.submit_via_qxub(
                job_name='test',
                worker_cmd=['dt', 'test'],
                wait=False,
                error_class=MyError,
            )
