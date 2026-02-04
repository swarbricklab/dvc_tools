"""Tests for dt add module."""

import os
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from dt import add
from dt.errors import AddError


class TestCheckQxub:
    """Tests for check_qxub function."""
    
    def test_returns_true_when_qxub_available(self):
        """Returns True when qxub is in PATH."""
        with patch('shutil.which', return_value='/usr/local/bin/qxub'):
            assert add.check_qxub() is True
    
    def test_returns_false_when_qxub_missing(self):
        """Returns False when qxub is not in PATH."""
        with patch('shutil.which', return_value=None):
            assert add.check_qxub() is False


class TestCountFiles:
    """Tests for count_files function."""
    
    def test_single_file_returns_one(self, tmp_path):
        """Single file returns 1."""
        test_file = tmp_path / 'test.txt'
        test_file.write_text('content')
        
        assert add.count_files(str(test_file)) == 1
    
    def test_directory_counts_recursively(self, tmp_path):
        """Directory counts all files recursively."""
        # Create directory structure
        (tmp_path / 'subdir').mkdir()
        (tmp_path / 'file1.txt').write_text('1')
        (tmp_path / 'file2.txt').write_text('2')
        (tmp_path / 'subdir' / 'file3.txt').write_text('3')
        (tmp_path / 'subdir' / 'file4.txt').write_text('4')
        
        assert add.count_files(str(tmp_path)) == 4
    
    def test_empty_directory_returns_zero(self, tmp_path):
        """Empty directory returns 0."""
        empty_dir = tmp_path / 'empty'
        empty_dir.mkdir()
        
        assert add.count_files(str(empty_dir)) == 0
    
    def test_nonexistent_path_returns_one(self, tmp_path):
        """Nonexistent path returns 1 as fallback."""
        # This is the fallback behavior
        result = add.count_files(str(tmp_path / 'nonexistent'))
        assert result == 1


class TestGetChecksumJobs:
    """Tests for get_checksum_jobs function."""
    
    def test_returns_int_when_set(self):
        """Returns integer when core.checksum_jobs is set."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = '16\n'
        
        with patch('subprocess.run', return_value=mock_result):
            result = add.get_checksum_jobs()
        
        assert result == 16
    
    def test_returns_none_when_not_set(self):
        """Returns None when core.checksum_jobs is not set."""
        mock_result = MagicMock()
        mock_result.returncode = 1  # Config not set
        mock_result.stdout = ''
        
        with patch('subprocess.run', return_value=mock_result):
            result = add.get_checksum_jobs()
        
        assert result is None
    
    def test_returns_none_on_empty_output(self):
        """Returns None when output is empty."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ''
        
        with patch('subprocess.run', return_value=mock_result):
            result = add.get_checksum_jobs()
        
        assert result is None
    
    def test_returns_none_on_subprocess_error(self):
        """Returns None on subprocess error."""
        import subprocess
        with patch('subprocess.run', side_effect=subprocess.SubprocessError("error")):
            result = add.get_checksum_jobs()
        
        assert result is None


class TestSetChecksumJobs:
    """Tests for set_checksum_jobs function."""
    
    def test_sets_value_correctly(self):
        """Calls dvc config with correct arguments."""
        with patch('subprocess.run') as mock_run:
            add.set_checksum_jobs(32)
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args == ['dvc', 'config', '--local', 'core.checksum_jobs', '32']


class TestUnsetChecksumJobs:
    """Tests for unset_checksum_jobs function."""
    
    def test_unsets_value(self):
        """Calls dvc config --unset."""
        with patch('subprocess.run') as mock_run:
            add.unset_checksum_jobs()
        
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert '--unset' in args
        assert 'core.checksum_jobs' in args


class TestAdd:
    """Tests for add function."""
    
    def test_raises_error_for_no_targets(self):
        """Raises AddError when no targets specified."""
        with pytest.raises(AddError, match="No targets specified"):
            add.add([])
    
    def test_raises_error_for_invalid_thread_count(self):
        """Raises AddError for thread count < 1."""
        with pytest.raises(AddError, match="Thread count must be at least 1"):
            add.add(['file.txt'], threads=0)
    
    def test_raises_error_for_excessive_thread_count(self):
        """Raises AddError when threads exceed maximum."""
        with patch.object(add.cfg, 'get_value', return_value='192'):
            with pytest.raises(AddError, match="exceeds maximum"):
                add.add(['file.txt'], threads=500)
    
    def test_basic_add_operation(self):
        """Basic add operation runs dvc add."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            result = add.add(['data.csv'])
        
        assert result is True
        # Check dvc add was called
        call_args = mock_run.call_args[0][0]
        assert call_args[0:2] == ['dvc', 'add']
        assert 'data.csv' in call_args
    
    def test_with_threads_sets_checksum_jobs(self):
        """Setting threads configures core.checksum_jobs."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        
        calls = []
        def capture_calls(cmd, **kwargs):
            calls.append(cmd)
            result = MagicMock()
            result.returncode = 0
            result.stdout = ''
            return result
        
        with patch('subprocess.run', side_effect=capture_calls):
            with patch.object(add.cfg, 'get_value', return_value='192'):
                add.add(['data.csv'], threads=16)
        
        # Should have calls: get_checksum_jobs, set_checksum_jobs, dvc add, unset_checksum_jobs
        cmd_strs = [' '.join(c) for c in calls]
        assert any('core.checksum_jobs 16' in s for s in cmd_strs)
        assert any('dvc add' in s for s in cmd_strs)
    
    def test_restores_original_checksum_jobs(self):
        """Restores original checksum_jobs after add."""
        calls = []
        
        def mock_run(cmd, **kwargs):
            calls.append(cmd)
            result = MagicMock()
            result.returncode = 0
            # Return original value of 8 for get query
            if 'config' in cmd and 'core.checksum_jobs' in cmd and '--local' not in cmd:
                result.stdout = '8\n'
            else:
                result.stdout = ''
            return result
        
        with patch('subprocess.run', side_effect=mock_run):
            with patch.object(add.cfg, 'get_value', return_value='192'):
                add.add(['data.csv'], threads=16)
        
        # Should restore original value
        cmd_strs = [' '.join(c) for c in calls]
        # Last set should restore to 8
        assert any('core.checksum_jobs 8' in s for s in cmd_strs)
    
    def test_with_dvc_args(self):
        """Additional dvc_args are passed through."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            add.add(['data.csv'], dvc_args=['--no-commit', '-v'])
        
        call_args = mock_run.call_args[0][0]
        assert '--no-commit' in call_args
        assert '-v' in call_args


class TestAddViaQxub:
    """Tests for add_via_qxub function."""
    
    def test_raises_error_when_qxub_missing(self):
        """Raises AddError when qxub is not available."""
        with patch('shutil.which', return_value=None):
            with pytest.raises(AddError, match="qxub not found"):
                add.add_via_qxub(['data.csv'])
    
    def test_raises_error_for_no_targets(self):
        """Raises AddError when no targets specified."""
        with patch('shutil.which', return_value='/usr/local/bin/qxub'):
            with pytest.raises(AddError, match="No targets specified"):
                add.add_via_qxub([])
    
    def test_raises_error_for_invalid_thread_count(self):
        """Raises AddError for thread count < 1."""
        with patch('shutil.which', return_value='/usr/local/bin/qxub'):
            with pytest.raises(AddError, match="Thread count must be at least 1"):
                add.add_via_qxub(['file.txt'], threads=0)
    
    def test_submits_job_correctly(self, tmp_path):
        """Submits qxub job with correct parameters."""
        # Create a test file
        test_file = tmp_path / 'data.csv'
        test_file.write_text('test data')
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = '12345.pbs\n'
        
        with patch('shutil.which', return_value='/usr/local/bin/qxub'):
            with patch('subprocess.run', return_value=mock_result) as mock_run:
                with patch.object(add.cfg, 'get_value', side_effect=lambda k, d=None: {
                    'add.max_threads': '192',
                    'add.mem_per_thread': '1',
                    'qxub.env': 'dt',
                    'qxub.queue': 'normal',
                    'qxub.walltime': '10:00:00',
                }.get(k, d)):
                    result = add.add_via_qxub([str(test_file)], threads=8, wait=False)
        
        assert result == ['12345.pbs']
        
        # Verify qxub command structure
        call_args = mock_run.call_args[0][0]
        assert call_args[0] == 'qxub'
        assert 'exec' in call_args
        assert '--' in call_args
    
    def test_caps_threads_to_file_count(self, tmp_path):
        """Threads are capped to total file count."""
        # Create a single file (should cap threads to 1)
        test_file = tmp_path / 'single.txt'
        test_file.write_text('test')
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = '12345.pbs\n'
        
        captured_cmd = []
        
        def capture(cmd, **kwargs):
            captured_cmd.extend(cmd)
            return mock_result
        
        with patch('shutil.which', return_value='/usr/local/bin/qxub'):
            with patch('subprocess.run', side_effect=capture):
                with patch.object(add.cfg, 'get_value', side_effect=lambda k, d=None: {
                    'add.max_threads': '192',
                    'add.mem_per_thread': '1',
                    'qxub.env': 'dt',
                    'qxub.queue': 'normal',
                    'qxub.walltime': '10:00:00',
                }.get(k, d)):
                    add.add_via_qxub([str(test_file)], threads=100, wait=False)
        
        # Find --threads in command and check it's 1 (single file)
        cmd_str = ' '.join(captured_cmd)
        assert '--threads 1' in cmd_str
