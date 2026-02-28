"""Unit tests for dt.index module.

Tests index synchronization with locking for concurrent access.
"""

import pytest
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.index import (
    IndexError,
    IndexLockTimeout,
    IndexNotConfigured,
    get_index_paths,
    get_lock_timeout,
    get_retry_interval,
    is_auto_sync_enabled,
    wait_for_lock,
    acquire_lock,
    release_lock,
    pull,
)


# =============================================================================
# Error class tests
# =============================================================================

class TestIndexErrors:
    """Tests for Index error classes."""

    def test_index_error_inherits_from_exception(self):
        """Test IndexError inherits from Exception."""
        error = IndexError("test error")
        assert isinstance(error, Exception)
        assert str(error) == "test error"

    def test_index_lock_timeout_inherits_from_index_error(self):
        """Test IndexLockTimeout inherits from IndexError."""
        error = IndexLockTimeout("lock timeout")
        assert isinstance(error, IndexError)
        assert str(error) == "lock timeout"

    def test_index_not_configured_inherits_from_index_error(self):
        """Test IndexNotConfigured inherits from IndexError."""
        error = IndexNotConfigured("not configured")
        assert isinstance(error, IndexError)
        assert str(error) == "not configured"


# =============================================================================
# get_index_paths tests
# =============================================================================

class TestGetIndexPaths:
    """Tests for the get_index_paths function."""

    def test_raises_error_when_not_configured(self):
        """Test IndexNotConfigured raised when mirror not set."""
        with patch("dt.index.cfg.get_value", return_value=None):
            with pytest.raises(IndexNotConfigured, match="not configured"):
                get_index_paths()

    def test_raises_error_when_not_in_dvc_repo(self):
        """Test IndexNotConfigured raised when not in DVC repo."""
        import subprocess
        with patch("dt.index.cfg.get_value", return_value="/some/mirror"):
            with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "dvc")):
                with pytest.raises(IndexNotConfigured, match="Not in a DVC repository"):
                    get_index_paths()

    def test_raises_error_when_dvc_not_found(self):
        """Test IndexNotConfigured raised when DVC not found."""
        with patch("dt.index.cfg.get_value", return_value="/some/mirror"):
            with patch("subprocess.run", side_effect=FileNotFoundError()):
                with pytest.raises(IndexNotConfigured, match="DVC not found"):
                    get_index_paths()

    def test_returns_paths_when_configured(self):
        """Test returns both local and mirror paths."""
        mock_doctor_output = MagicMock()
        mock_doctor_output.returncode = 0
        mock_doctor_output.stdout = "site_cache_dir /tmp/dvc-cache/abc123\n"
        
        with patch("dt.index.cfg.get_value", return_value="/mirror/root"):
            with patch("subprocess.run", return_value=mock_doctor_output):
                local, mirror = get_index_paths()
                
                assert isinstance(local, Path)
                assert isinstance(mirror, str)  # mirror can be cloud URL
                assert str(local) == "/tmp/dvc-cache/abc123"
                assert "abc123" in mirror
    
    def test_returns_cloud_mirror_path(self):
        """Test returns cloud URLs as strings."""
        mock_doctor_output = MagicMock()
        mock_doctor_output.returncode = 0
        mock_doctor_output.stdout = "site_cache_dir /tmp/dvc-cache/abc123\n"
        
        with patch("dt.index.cfg.get_value", return_value="gs://my-bucket/index-mirror"):
            with patch("subprocess.run", return_value=mock_doctor_output):
                local, mirror = get_index_paths()
                
                assert isinstance(local, Path)
                assert isinstance(mirror, str)
                assert mirror.startswith("gs://")
                assert "abc123" in mirror


# =============================================================================
# Config function tests
# =============================================================================

class TestGetLockTimeout:
    """Tests for the get_lock_timeout function."""

    def test_returns_config_value(self):
        """Test returns configured timeout."""
        with patch("dt.index.cfg.get_value", return_value=60):
            result = get_lock_timeout()
            assert result == 60

    def test_returns_default_when_not_configured(self):
        """Test returns default (120) when not configured."""
        with patch("dt.index.cfg.get_value", side_effect=lambda key, default=None: default):
            result = get_lock_timeout()
            assert result == 120


class TestGetRetryInterval:
    """Tests for the get_retry_interval function."""

    def test_returns_config_value(self):
        """Test returns configured retry interval."""
        with patch("dt.index.cfg.get_value", return_value=10):
            result = get_retry_interval()
            assert result == 10

    def test_returns_default_when_not_configured(self):
        """Test returns default (5) when not configured."""
        with patch("dt.index.cfg.get_value", side_effect=lambda key, default=None: default):
            result = get_retry_interval()
            assert result == 5


class TestIsAutoSyncEnabled:
    """Tests for the is_auto_sync_enabled function."""

    def test_returns_true_when_enabled(self):
        """Test returns True when auto sync enabled."""
        with patch("dt.index.cfg.get_value", return_value=True):
            result = is_auto_sync_enabled()
            assert result is True

    def test_returns_false_when_disabled(self):
        """Test returns False when auto sync disabled."""
        with patch("dt.index.cfg.get_value", return_value=False):
            result = is_auto_sync_enabled()
            assert result is False

    def test_returns_default_true_when_not_configured(self):
        """Test returns default True when not configured."""
        with patch("dt.index.cfg.get_value", side_effect=lambda key, default=None: default):
            result = is_auto_sync_enabled()
            assert result is True


# =============================================================================
# Locking function tests
# =============================================================================

class TestWaitForLock:
    """Tests for the wait_for_lock function."""

    def test_returns_true_when_no_lock(self, tmp_path):
        """Test returns True immediately when no lock file exists."""
        lock_file = tmp_path / "index.lock"
        
        result = wait_for_lock(lock_file, timeout=1)
        assert result is True

    def test_returns_true_when_lock_released(self, tmp_path):
        """Test returns True when lock file is removed."""
        lock_file = tmp_path / "index.lock"
        lock_file.touch()
        
        # Start a "background" removal
        import threading
        def remove_lock():
            time.sleep(0.1)
            lock_file.unlink()
        
        t = threading.Thread(target=remove_lock)
        t.start()
        
        result = wait_for_lock(lock_file, timeout=2, retry_interval=1)
        t.join()
        
        assert result is True

    def test_returns_false_on_timeout(self, tmp_path):
        """Test returns False when timeout reached."""
        lock_file = tmp_path / "index.lock"
        lock_file.touch()
        
        result = wait_for_lock(lock_file, timeout=0.2, retry_interval=1)
        
        assert result is False


class TestAcquireLock:
    """Tests for the acquire_lock function."""

    def test_creates_lock_file(self, tmp_path):
        """Test creates lock file."""
        lock_file = tmp_path / "subdir" / "index.lock"
        
        acquire_lock(lock_file, timeout=1)
        
        assert lock_file.exists()

    def test_raises_timeout_when_cannot_acquire(self, tmp_path):
        """Test raises IndexLockTimeout when lock persists."""
        lock_file = tmp_path / "index.lock"
        lock_file.touch()
        
        with pytest.raises(IndexLockTimeout, match="Timeout waiting"):
            acquire_lock(lock_file, timeout=0)


class TestReleaseLock:
    """Tests for the release_lock function."""

    def test_removes_lock_file(self, tmp_path):
        """Test removes lock file."""
        lock_file = tmp_path / "index.lock"
        lock_file.touch()
        
        release_lock(lock_file)
        
        assert not lock_file.exists()

    def test_handles_missing_lock_file(self, tmp_path):
        """Test handles case when lock file already removed."""
        lock_file = tmp_path / "index.lock"
        
        # Should not raise
        release_lock(lock_file)


# =============================================================================
# pull tests
# =============================================================================

class TestPull:
    """Tests for the pull function."""

    def test_returns_false_when_not_configured(self, capsys):
        """Test returns False and warns when mirror not set."""
        with patch("dt.index.cfg.get_value", return_value=None):
            result = pull()
            
            assert result is False
            captured = capsys.readouterr()
            assert "not configured" in captured.out.lower() or "warning" in captured.out.lower()
