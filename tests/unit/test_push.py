"""Unit tests for dt.push module.

Tests push functionality for DVC-tracked files.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.push import (
    get_file_sizes,
    get_files_size,
    get_project_remotes,
    partition_manifest,
    push_to_remote,
    push_all,
    build_manifest,
)
from dt.errors import PushError


# =============================================================================
# get_files_size tests
# =============================================================================

class TestGetFilesSize:
    """Tests for the get_files_size function."""

    def test_returns_0_when_repo_not_available(self):
        """Test returns 0 when DVC repo not accessible."""
        with patch("dt.push.Repo", side_effect=Exception("No repo")):
            result = get_files_size(["abc123", "def456"])
            
            assert result == 0

    def test_sums_cache_file_sizes(self, tmp_path):
        """Test sums up sizes of cached files."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        (cache_dir / "de").mkdir(parents=True)
        
        # Create mock cache files
        file1 = cache_dir / "ab" / "c123"
        file1.write_bytes(b"x" * 100)
        file2 = cache_dir / "de" / "f456"
        file2.write_bytes(b"y" * 200)
        
        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_files_size(["abc123", "def456"])
            
            assert result == 300

    def test_handles_dir_suffix(self, tmp_path):
        """Test handles .dir suffix in hashes."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        
        dir_file = cache_dir / "ab" / "c123.dir"
        dir_file.write_bytes(b"x" * 50)
        
        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_files_size(["abc123.dir"])
            
            assert result == 50

    def test_skips_nonexistent_files(self, tmp_path):
        """Test skips files that don't exist in cache."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)

        file1 = cache_dir / "ab" / "c123"
        file1.write_bytes(b"x" * 100)
        # def456 doesn't exist

        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)

        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_files_size(["abc123", "def456"])

            assert result == 100


# =============================================================================
# get_file_sizes tests
# =============================================================================

class TestGetFileSizes:
    """Tests for the get_file_sizes function."""

    def test_returns_zero_map_when_repo_not_available(self):
        """Returns 0 for every hash when the cache is not accessible."""
        with patch("dt.push.Repo", side_effect=Exception("No repo")):
            result = get_file_sizes(["abc123", "def456"])

            assert result == {"abc123": 0, "def456": 0}

    def test_returns_per_file_sizes(self, tmp_path):
        """Returns each blob's size keyed by hash."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        (cache_dir / "de").mkdir(parents=True)
        (cache_dir / "ab" / "c123").write_bytes(b"x" * 100)
        (cache_dir / "de" / "f456").write_bytes(b"y" * 200)

        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)

        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_file_sizes(["abc123", "def456"])

            assert result == {"abc123": 100, "def456": 200}

    def test_missing_blob_is_zero(self, tmp_path):
        """Missing blobs map to 0 rather than being dropped."""
        cache_dir = tmp_path / "files" / "md5"
        (cache_dir / "ab").mkdir(parents=True)
        (cache_dir / "ab" / "c123").write_bytes(b"x" * 100)

        mock_repo = MagicMock()
        mock_repo.cache.local.path = str(cache_dir)

        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_file_sizes(["abc123", "def456"])

            assert result == {"abc123": 100, "def456": 0}


# =============================================================================
# partition_manifest tests
# =============================================================================

class TestPartitionManifest:
    """Tests for size-aware (LPT) partitioning (issue #138)."""

    def test_each_file_assigned_exactly_once(self):
        """Partitions are disjoint and cover every file."""
        files = [f"{i:02x}aaaa" for i in range(20)]
        sizes = {h: 1 for h in files}
        manifest = {"files": files}

        partitions = partition_manifest(manifest, 4, sizes=sizes)

        assigned = [h for fs in partitions.values() for h in fs]
        assert sorted(assigned) == sorted(files)
        assert len(assigned) == len(set(assigned))  # no duplicates

    def test_balances_bytes_not_counts(self):
        """One big file is isolated; small files balance the others."""
        # Hash prefixes chosen so a naive prefix % 2 split would put the big
        # file and several small files on the same worker.
        files = ["00big", "02s", "04s", "06s", "08s"]
        sizes = {"00big": 1000, "02s": 10, "04s": 10, "06s": 10, "08s": 10}
        manifest = {"files": files}

        partitions = partition_manifest(manifest, 2, sizes=sizes)
        loads = {w: sum(sizes[h] for h in fs) for w, fs in partitions.items()}

        # The big file sits alone; all small files land on the other worker.
        big_worker = next(w for w, fs in partitions.items() if "00big" in fs)
        assert loads[big_worker] == 1000
        other = 1 - big_worker
        assert loads[other] == 40

    def test_deterministic_regardless_of_input_order(self):
        """LPT result does not depend on manifest file ordering."""
        sizes = {"a1": 50, "b2": 50, "c3": 30, "d4": 20, "e5": 20}
        p1 = partition_manifest({"files": ["a1", "b2", "c3", "d4", "e5"]}, 3, sizes=sizes)
        p2 = partition_manifest({"files": ["e5", "d4", "c3", "b2", "a1"]}, 3, sizes=sizes)

        assert p1 == p2

    def test_handles_more_workers_than_files(self):
        """Extra workers simply get empty partitions."""
        manifest = {"files": ["a1", "b2"]}
        sizes = {"a1": 5, "b2": 5}

        partitions = partition_manifest(manifest, 5, sizes=sizes)

        assert set(partitions.keys()) == set(range(5))
        non_empty = [fs for fs in partitions.values() if fs]
        assert len(non_empty) == 2

    def test_missing_sizes_treated_as_zero(self):
        """Files absent from the size map are assigned without error."""
        manifest = {"files": ["a1", "b2", "c3"]}
        sizes = {"a1": 100}  # b2, c3 missing

        partitions = partition_manifest(manifest, 2, sizes=sizes)

        assigned = [h for fs in partitions.values() for h in fs]
        assert sorted(assigned) == ["a1", "b2", "c3"]


# =============================================================================
# get_project_remotes tests
# =============================================================================

class TestGetProjectRemotes:
    """Tests for the get_project_remotes function."""

    def test_returns_remotes_from_dvc_config(self):
        """Test returns remotes from DVC config API."""
        mock_repo = MagicMock()
        mock_repo.config.read.side_effect = lambda level: {
            "local": {"remote": {"local": {"url": "/local/path"}}},
            "repo": {"remote": {"origin": {"url": "ssh://host/path"}}},
        }.get(level, {})
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_project_remotes()
            
            assert len(result) == 2
            names = [r[0] for r in result]
            assert "local" in names
            assert "origin" in names

    def test_local_overrides_repo_scope(self):
        """Test that local scope overrides repo scope for same name."""
        mock_repo = MagicMock()
        mock_repo.config.read.side_effect = lambda level: {
            "local": {"remote": {"origin": {"url": "/local/override"}}},
            "repo": {"remote": {"origin": {"url": "ssh://original"}}},
        }.get(level, {})
        
        with patch("dt.push.Repo", return_value=mock_repo):
            result = get_project_remotes()
            
            # Should only have one "origin" entry
            origin = [r for r in result if r[0] == "origin"]
            assert len(origin) == 1
            assert origin[0][1] == "/local/override"


# =============================================================================
# push_to_remote tests
# =============================================================================

class TestPushToRemote:
    """Tests for the push_to_remote function."""

    def test_runs_dvc_push_with_remote(self):
        """Test runs dvc push with -r remote."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="Pushed 5 files\n",
                stderr="",
            )
            
            success, output = push_to_remote("origin", [])
            
            assert success is True
            assert "Pushed" in output
            
            call_args = mock_run.call_args[0][0]
            assert call_args == ["dvc", "push", "-r", "origin"]

    def test_passes_through_additional_args(self):
        """Test passes through additional arguments."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="",
                stderr="",
            )
            
            push_to_remote("origin", ["--jobs", "4", "-v"])
            
            call_args = mock_run.call_args[0][0]
            assert "--jobs" in call_args
            assert "4" in call_args
            assert "-v" in call_args

    def test_returns_failure_on_error(self):
        """Test returns (False, error_message) on failure."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="Push failed: connection refused",
            )
            
            success, output = push_to_remote("origin", [])
            
            assert success is False
            assert "connection refused" in output


# =============================================================================
# push_all tests
# =============================================================================

class TestPushAll:
    """Tests for the push_all function."""

    def test_raises_error_when_no_remotes(self):
        """Test PushError raised when no remotes configured."""
        with patch("dt.push.get_project_remotes", return_value=[]):
            with pytest.raises(PushError, match="No remotes configured"):
                push_all([])

    def test_pushes_to_all_remotes(self):
        """Test pushes to all configured remotes."""
        with patch("dt.push.get_project_remotes", return_value=[
            ("origin", "ssh://host/path"),
            ("local", "/local/path"),
        ]):
            with patch("dt.push.push_to_remote") as mock_push:
                mock_push.return_value = (True, "ok")
                
                results = push_all([])
                
                assert len(results) == 2
                assert mock_push.call_count == 2

    def test_returns_results_for_each_remote(self):
        """Test returns (remote_name, success, output) for each remote."""
        with patch("dt.push.get_project_remotes", return_value=[
            ("origin", "ssh://host/path"),
        ]):
            with patch("dt.push.push_to_remote") as mock_push:
                mock_push.return_value = (True, "Pushed 5 files")
                
                results = push_all([])
                
                assert results[0] == ("origin", True, "Pushed 5 files")


# =============================================================================
# build_manifest tests
# =============================================================================

class TestBuildManifest:
    """Tests for the build_manifest function."""

    def test_returns_empty_manifest_when_no_entries(self):
        """Test returns empty manifest when no tracked entries."""
        with patch("dt.push.utils.collect_tracked_entries") as mock_collect:
            mock_collect.return_value = {
                "repo": MagicMock(root_dir="/repo"),
                "indexes": {},
                "hash_to_path": {},
            }
            
            with patch("dvc_data.index.fetch.collect", create=True):
                result = build_manifest()
                
                assert result["files"] == []
                assert result["paths"] == {}
