"""Unit tests for dt.hpc module.

Tests HPC/qxub functionality for parallel DVC operations.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

from dt.hpc import (
    check_qxub,
    require_qxub,
    get_qxub_config,
    build_qxub_command,
    submit_workers,
    monitor_jobs,
    get_transfer_dir,
    get_prefixes_for_worker,
    save_manifest,
)
from dt.errors import HPCError


# =============================================================================
# check_qxub tests
# =============================================================================

class TestCheckQxub:
    """Tests for the check_qxub function."""

    def test_returns_true_when_qxub_available(self):
        """Test returns True when qxub is in PATH."""
        with patch("shutil.which", return_value="/usr/bin/qxub"):
            assert check_qxub() is True

    def test_returns_false_when_qxub_not_available(self):
        """Test returns False when qxub is not in PATH."""
        with patch("shutil.which", return_value=None):
            assert check_qxub() is False


# =============================================================================
# require_qxub tests
# =============================================================================

class TestRequireQxub:
    """Tests for the require_qxub function."""

    def test_no_error_when_qxub_available(self):
        """Test no error raised when qxub is available."""
        with patch("dt.hpc.check_qxub", return_value=True):
            require_qxub()  # Should not raise

    def test_raises_hpc_error_when_qxub_missing(self):
        """Test HPCError raised when qxub is not available."""
        with patch("dt.hpc.check_qxub", return_value=False):
            with pytest.raises(HPCError, match="qxub not found"):
                require_qxub()


# =============================================================================
# get_qxub_config tests
# =============================================================================

class TestGetQxubConfig:
    """Tests for the get_qxub_config function."""

    def test_returns_default_values(self):
        """Test default config values."""
        with patch("dt.hpc.cfg.get_value") as mock_get_value:
            mock_get_value.side_effect = lambda key, default=None: default
            
            config = get_qxub_config()
            
            assert config["env"] == "dt"
            assert config["queue"] == "copyq"
            assert config["walltime"] == "10:00:00"
            assert config["mem"] == "4GB"

    def test_uses_configured_values(self):
        """Test that configured values override defaults."""
        with patch("dt.hpc.cfg.get_value") as mock_get_value:
            def get_value(key, default=None):
                values = {
                    "qxub.env": "myenv",
                    "qxub.queue": "longq",
                    "qxub.walltime": "24:00:00",
                    "qxub.mem": "16GB",
                }
                return values.get(key, default)
            
            mock_get_value.side_effect = get_value
            
            config = get_qxub_config()
            
            assert config["env"] == "myenv"
            assert config["queue"] == "longq"
            assert config["walltime"] == "24:00:00"
            assert config["mem"] == "16GB"


# =============================================================================
# build_qxub_command tests
# =============================================================================

class TestBuildQxubCommand:
    """Tests for the build_qxub_command function."""

    def test_builds_basic_command(self):
        """Test building basic qxub command."""
        with patch("dt.hpc.get_qxub_config") as mock_config:
            mock_config.return_value = {
                "env": "dt",
                "queue": "copyq",
                "walltime": "10:00:00",
                "mem": "4GB",
            }
            
            cmd = build_qxub_command("test-job", ["dt", "push"])
            
            assert cmd[0] == "qxub"
            assert "exec" in cmd
            assert "--terse" in cmd
            assert "-N" in cmd
            assert "test-job" in cmd
            assert "--" in cmd
            assert "dt" in cmd
            assert "push" in cmd

    def test_includes_env_option(self):
        """Test that --env option is included."""
        with patch("dt.hpc.get_qxub_config") as mock_config:
            mock_config.return_value = {
                "env": "myenv",
                "queue": "copyq",
                "walltime": "10:00:00",
                "mem": "4GB",
            }
            
            cmd = build_qxub_command("job", ["cmd"])
            
            assert "--env" in cmd
            env_idx = cmd.index("--env")
            assert cmd[env_idx + 1] == "myenv"

    def test_includes_queue_option(self):
        """Test that --queue option is included."""
        with patch("dt.hpc.get_qxub_config") as mock_config:
            mock_config.return_value = {
                "env": "dt",
                "queue": "longq",
                "walltime": "10:00:00",
                "mem": "4GB",
            }
            
            cmd = build_qxub_command("job", ["cmd"])
            
            assert "--queue" in cmd
            queue_idx = cmd.index("--queue")
            assert cmd[queue_idx + 1] == "longq"

    def test_includes_time_option(self):
        """Test that --time option is included."""
        with patch("dt.hpc.get_qxub_config") as mock_config:
            mock_config.return_value = {
                "env": "dt",
                "queue": "copyq",
                "walltime": "24:00:00",
                "mem": "4GB",
            }
            
            cmd = build_qxub_command("job", ["cmd"])
            
            assert "--time" in cmd
            time_idx = cmd.index("--time")
            assert cmd[time_idx + 1] == "24:00:00"

    def test_includes_mem_option(self):
        """Test that --mem option is included."""
        with patch("dt.hpc.get_qxub_config") as mock_config:
            mock_config.return_value = {
                "env": "dt",
                "queue": "copyq",
                "walltime": "10:00:00",
                "mem": "16GB",
            }
            
            cmd = build_qxub_command("job", ["cmd"])
            
            assert "--mem" in cmd
            mem_idx = cmd.index("--mem")
            assert cmd[mem_idx + 1] == "16GB"

    def test_includes_additional_qxub_args(self):
        """Test that additional qxub_args are included."""
        with patch("dt.hpc.get_qxub_config") as mock_config:
            mock_config.return_value = {
                "env": "dt",
                "queue": "copyq",
                "walltime": "10:00:00",
                "mem": "4GB",
            }
            
            cmd = build_qxub_command("job", ["cmd"], qxub_args=["--cpus", "4"])
            
            assert "--cpus" in cmd
            cpus_idx = cmd.index("--cpus")
            assert cmd[cpus_idx + 1] == "4"


# =============================================================================
# submit_workers tests
# =============================================================================

class TestSubmitWorkers:
    """Tests for the submit_workers function."""

    def test_raises_error_when_qxub_not_available(self):
        """Test that HPCError is raised when qxub is not available."""
        with patch("dt.hpc.require_qxub", side_effect=HPCError("qxub not found")):
            with pytest.raises(HPCError, match="qxub not found"):
                submit_workers(Path("/manifest"), 4, "push")

    def test_skips_workers_with_no_files(self, tmp_path):
        """Test that workers with no files are skipped."""
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()
        
        # Create empty worker partition
        (manifest_dir / "worker_0.json").write_text(json.dumps({"files": []}))
        
        with patch("dt.hpc.require_qxub"):
            job_ids = submit_workers(manifest_dir, 1, "push", verbose=False)
            
            assert job_ids == []

    def test_skips_nonexistent_worker_files(self, tmp_path):
        """Test that non-existent worker files are skipped."""
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()
        # Don't create any worker files
        
        with patch("dt.hpc.require_qxub"):
            job_ids = submit_workers(manifest_dir, 4, "push", verbose=False)
            
            assert job_ids == []

    def test_submits_jobs_for_workers_with_files(self, tmp_path):
        """Test that jobs are submitted for workers with files."""
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()
        
        # Create worker with files
        (manifest_dir / "worker_0.json").write_text(
            json.dumps({"files": ["abc123", "def456"]})
        )
        
        with patch("dt.hpc.require_qxub"):
            with patch("dt.hpc.build_qxub_command", return_value=["qxub", "exec"]):
                with patch("subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(
                        returncode=0,
                        stdout="12345.pbs\n",
                        stderr="",
                    )
                    
                    with patch("pathlib.Path.cwd", return_value=tmp_path):
                        job_ids = submit_workers(manifest_dir, 1, "push", verbose=False)
                    
                    assert job_ids == ["12345.pbs"]


# =============================================================================
# monitor_jobs tests
# =============================================================================

class TestMonitorJobs:
    """Tests for the monitor_jobs function."""

    def test_returns_true_for_empty_job_list(self):
        """Test that True is returned for empty job list."""
        result = monitor_jobs([])
        assert result is True

    def test_raises_error_when_qxub_not_available(self):
        """Test that HPCError is raised when qxub is not available."""
        with patch("dt.hpc.require_qxub", side_effect=HPCError("qxub not found")):
            with pytest.raises(HPCError, match="qxub not found"):
                monitor_jobs(["12345.pbs"])

    def test_runs_qxub_monitor_command(self):
        """Test that qxub monitor command is run."""
        with patch("dt.hpc.require_qxub"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)
                
                result = monitor_jobs(["12345.pbs", "67890.pbs"])
                
                mock_run.assert_called_once()
                call_args = mock_run.call_args[0][0]
                assert "qxub" in call_args
                assert "monitor" in call_args
                assert "12345.pbs" in call_args
                assert "67890.pbs" in call_args

    def test_returns_false_on_failure(self):
        """Test that False is returned when monitoring fails."""
        with patch("dt.hpc.require_qxub"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=1)
                
                result = monitor_jobs(["12345.pbs"])
                
                assert result is False

    def test_returns_true_on_success(self):
        """Test that True is returned when all jobs succeed."""
        with patch("dt.hpc.require_qxub"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)
                
                result = monitor_jobs(["12345.pbs"])
                
                assert result is True


# =============================================================================
# get_transfer_dir tests
# =============================================================================

class TestGetTransferDir:
    """Tests for the get_transfer_dir function."""

    def test_returns_push_directory(self, tmp_path):
        """Test returns correct path for push operation."""
        with patch("pathlib.Path.cwd", return_value=tmp_path):
            result = get_transfer_dir("push")
            
            assert result == tmp_path / ".dt" / "tmp" / "push"
            assert result.exists()

    def test_returns_pull_directory(self, tmp_path):
        """Test returns correct path for pull operation."""
        with patch("pathlib.Path.cwd", return_value=tmp_path):
            result = get_transfer_dir("pull")
            
            assert result == tmp_path / ".dt" / "tmp" / "pull"
            assert result.exists()

    def test_creates_directory_if_not_exists(self, tmp_path):
        """Test that directory is created if it doesn't exist."""
        with patch("pathlib.Path.cwd", return_value=tmp_path):
            transfer_dir = tmp_path / ".dt" / "tmp" / "push"
            assert not transfer_dir.exists()
            
            result = get_transfer_dir("push")
            
            assert result.exists()


# =============================================================================
# get_prefixes_for_worker tests
# =============================================================================

class TestGetPrefixesForWorker:
    """Tests for the get_prefixes_for_worker function."""

    def test_returns_set_of_hex_prefixes(self):
        """Test returns set of 2-char hex strings."""
        prefixes = get_prefixes_for_worker(0, 1)
        
        # With 1 worker, should get all 256 prefixes
        assert len(prefixes) == 256
        assert "00" in prefixes
        assert "ff" in prefixes

    def test_partitions_prefixes_among_workers(self):
        """Test that prefixes are evenly partitioned."""
        num_workers = 4
        all_prefixes = set()
        
        for worker_id in range(num_workers):
            worker_prefixes = get_prefixes_for_worker(worker_id, num_workers)
            # Each worker should get 256/4 = 64 prefixes
            assert len(worker_prefixes) == 64
            # No overlap with previous workers
            assert not worker_prefixes.intersection(all_prefixes)
            all_prefixes.update(worker_prefixes)
        
        # All 256 prefixes should be covered
        assert len(all_prefixes) == 256

    def test_worker_0_gets_prefixes_mod_0(self):
        """Test that worker 0 gets prefixes where int(prefix, 16) % n_workers == 0."""
        prefixes = get_prefixes_for_worker(0, 4)
        
        # Prefix "00" (0 % 4 == 0) should be in worker 0
        assert "00" in prefixes
        # Prefix "04" (4 % 4 == 0) should be in worker 0
        assert "04" in prefixes
        # Prefix "01" (1 % 4 == 1) should NOT be in worker 0
        assert "01" not in prefixes

    def test_worker_1_gets_prefixes_mod_1(self):
        """Test that worker 1 gets prefixes where int(prefix, 16) % n_workers == 1."""
        prefixes = get_prefixes_for_worker(1, 4)
        
        # Prefix "01" (1 % 4 == 1) should be in worker 1
        assert "01" in prefixes
        # Prefix "05" (5 % 4 == 1) should be in worker 1
        assert "05" in prefixes
        # Prefix "00" should NOT be in worker 1
        assert "00" not in prefixes
