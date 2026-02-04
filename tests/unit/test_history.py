"""Unit tests for dt.history module.

Tests history and format_history functions.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from dt.history import history, format_history
from dt.errors import HistoryError


# =============================================================================
# history tests
# =============================================================================

class TestHistory:
    """Tests for the history function."""

    def test_no_candidate_commits_raises_error(self):
        """Test error when no DVC metadata commits found."""
        with patch("dt.history.offline_status", return_value={"enabled": True}):
            with patch("dt.history.Repo") as mock_repo:
                mock_repo.return_value = MagicMock()
                
                with patch("dt.history.get_candidate_commits", return_value=[]):
                    with pytest.raises(HistoryError, match="No DVC metadata commits"):
                        history("/path/to/file.csv")

    def test_file_has_no_tracked_history(self):
        """Test error when file has no DVC-tracked history."""
        with patch("dt.history.offline_status", return_value={"enabled": True}):
            with patch("dt.history.Repo") as mock_repo:
                mock_repo.return_value = MagicMock()
                
                with patch("dt.history.get_candidate_commits", return_value=["abc123", "def456"]):
                    with patch("dt.history.get_hash_at_rev", return_value=None):
                        with pytest.raises(HistoryError, match="has no DVC-tracked history"):
                            history("/path/to/file.csv")

    def test_returns_version_history(self):
        """Test returning version history for a tracked file."""
        mock_commits = ["abc123", "def456"]
        mock_hashes = {
            ("abc123"): "hash1111111111111111111111111111",
            ("def456"): "hash2222222222222222222222222222",
        }
        
        with patch("dt.history.offline_status", return_value={"enabled": True}):
            with patch("dt.history.Repo") as mock_repo:
                mock_repo.return_value = MagicMock()
                
                with patch("dt.history.get_candidate_commits", return_value=mock_commits):
                    with patch("dt.history.get_hash_at_rev") as mock_get_hash:
                        mock_get_hash.side_effect = lambda path, commit, repo=None: mock_hashes.get(commit)
                        
                        with patch("dt.history.get_commit_info") as mock_commit_info:
                            mock_commit_info.return_value = {
                                "hash": "abc123fullhash",
                                "short_hash": "abc123",
                                "date": "2025-01-01",
                                "message": "Initial commit",
                                "author": "Test User",
                            }
                            
                            results = history("/path/to/file.csv")
                            
                            # Should have 2 versions (both hashes differ)
                            assert len(results) == 2

    def test_only_returns_changed_versions(self):
        """Test that only versions where hash changed are returned."""
        # Same hash means no new version
        mock_commits = ["commit1", "commit2", "commit3"]
        
        with patch("dt.history.offline_status", return_value={"enabled": True}):
            with patch("dt.history.Repo") as mock_repo:
                mock_repo.return_value = MagicMock()
                
                with patch("dt.history.get_candidate_commits", return_value=mock_commits):
                    with patch("dt.history.get_hash_at_rev") as mock_get_hash:
                        # commit1 and commit2 have same hash, commit3 has different
                        def get_hash(path, commit, repo=None):
                            if commit in ["commit1", "commit2"]:
                                return "samehash"
                            return "differenthash"
                        mock_get_hash.side_effect = get_hash
                        
                        with patch("dt.history.get_commit_info") as mock_commit_info:
                            mock_commit_info.return_value = {
                                "hash": "fullhash",
                                "short_hash": "abc",
                                "date": "2025-01-01",
                                "message": "Commit",
                                "author": "Test User",
                            }
                            
                            results = history("/path/to/file.csv")
                            
                            # Only 2 versions: first appearance of samehash, then differenthash
                            assert len(results) == 2

    def test_respects_limit_parameter(self):
        """Test that limit parameter is respected."""
        mock_commits = ["c1", "c2", "c3", "c4", "c5"]
        
        with patch("dt.history.offline_status", return_value={"enabled": True}):
            with patch("dt.history.Repo") as mock_repo:
                mock_repo.return_value = MagicMock()
                
                with patch("dt.history.get_candidate_commits", return_value=mock_commits):
                    with patch("dt.history.get_hash_at_rev") as mock_get_hash:
                        # Each commit has unique hash
                        mock_get_hash.side_effect = lambda p, c, repo=None: f"hash_{c}"
                        
                        with patch("dt.history.get_commit_info") as mock_commit_info:
                            mock_commit_info.return_value = {
                                "hash": "fullhash",
                                "short_hash": "abc",
                                "date": "2025-01-01",
                                "message": "Commit",
                                "author": "Test User",
                            }
                            
                            results = history("/path/to/file.csv", limit=2)
                            
                            assert len(results) <= 2

    def test_passes_since_to_get_candidate_commits(self):
        """Test that since parameter is passed through."""
        with patch("dt.history.offline_status", return_value={"enabled": True}):
            with patch("dt.history.Repo") as mock_repo:
                mock_repo.return_value = MagicMock()
                
                with patch("dt.history.get_candidate_commits") as mock_get_candidates:
                    mock_get_candidates.return_value = []
                    
                    with pytest.raises(HistoryError):
                        history("/path/to/file.csv", since="2025-01-01")
                    
                    mock_get_candidates.assert_called_once()
                    call_kwargs = mock_get_candidates.call_args
                    assert call_kwargs.kwargs.get("since") == "2025-01-01"

    def test_enables_offline_mode_if_not_enabled(self):
        """Test that offline mode is enabled during history lookup."""
        with patch("dt.history.offline_status", return_value={"enabled": False}):
            with patch("dt.history.offline_enable") as mock_enable:
                with patch("dt.history.offline_disable") as mock_disable:
                    with patch("dt.history.Repo") as mock_repo:
                        mock_repo.return_value = MagicMock()
                        
                        with patch("dt.history.get_candidate_commits", return_value=[]):
                            with pytest.raises(HistoryError):
                                history("/path/to/file.csv")
                            
                            # Should have tried to enable offline mode
                            mock_enable.assert_called_once()

    def test_normalizes_path_to_string(self):
        """Test that Path objects are converted to strings."""
        with patch("dt.history.offline_status", return_value={"enabled": True}):
            with patch("dt.history.Repo") as mock_repo:
                mock_repo.return_value = MagicMock()
                
                with patch("dt.history.get_candidate_commits", return_value=[]):
                    with pytest.raises(HistoryError):
                        # Pass Path object - should work
                        history(Path("/path/to/file.csv"))


# =============================================================================
# format_history tests
# =============================================================================

class TestFormatHistory:
    """Tests for the format_history function."""

    def test_json_output_format(self):
        """Test JSON output formatting."""
        entries = [
            {
                "commit": "abc123full",
                "short_commit": "abc123",
                "date": "2025-01-01",
                "message": "Initial commit",
                "author": "Test User",
                "hash": "hash1111111111111111111111111111",
            }
        ]
        
        output = format_history(entries, json_output=True)
        parsed = json.loads(output)
        
        assert len(parsed) == 1
        assert parsed[0]["commit"] == "abc123full"

    def test_empty_entries_message(self):
        """Test message for empty entries."""
        output = format_history([])
        assert output == "No history found"

    def test_simple_output_format(self):
        """Test simple tabular output."""
        entries = [
            {
                "commit": "abc123full",
                "short_commit": "abc123",
                "date": "2025-01-01",
                "message": "Initial commit",
                "author": "Test User",
                "hash": "hash1111111111111111111111111111",
            }
        ]
        
        output = format_history(entries)
        
        assert "abc123" in output
        assert "2025-01-01" in output
        assert "Initial commit" in output
        # Hash should be truncated in simple mode
        assert "hash111111111111" in output

    def test_verbose_output_includes_author(self):
        """Test verbose output includes author."""
        entries = [
            {
                "commit": "abc123full",
                "short_commit": "abc123",
                "date": "2025-01-01",
                "message": "Initial commit",
                "author": "Test User",
                "hash": "hash1111111111111111111111111111",
            }
        ]
        
        output = format_history(entries, verbose=True)
        
        assert "Test User" in output

    def test_output_has_header_line(self):
        """Test that output includes header row."""
        entries = [
            {
                "commit": "abc123full",
                "short_commit": "abc123",
                "date": "2025-01-01",
                "message": "Commit",
                "author": "User",
                "hash": "hash1111111111111111111111111111",
            }
        ]
        
        output = format_history(entries)
        lines = output.split("\n")
        
        # First line should be header
        assert "COMMIT" in lines[0]
        assert "DATE" in lines[0]
        assert "HASH" in lines[0]
        
        # Second line should be separator
        assert lines[1].startswith("-")

    def test_verbose_header_includes_author_column(self):
        """Test verbose header includes AUTHOR column."""
        entries = [
            {
                "commit": "abc123full",
                "short_commit": "abc123",
                "date": "2025-01-01",
                "message": "Commit",
                "author": "User",
                "hash": "hash1111111111111111111111111111",
            }
        ]
        
        output = format_history(entries, verbose=True)
        lines = output.split("\n")
        
        assert "AUTHOR" in lines[0]

    def test_message_is_truncated(self):
        """Test that long messages are truncated."""
        long_message = "A" * 100
        entries = [
            {
                "commit": "abc123full",
                "short_commit": "abc123",
                "date": "2025-01-01",
                "message": long_message,
                "author": "User",
                "hash": "hash1111111111111111111111111111",
            }
        ]
        
        output = format_history(entries)
        
        # Message should be truncated to 40 chars
        assert long_message not in output
        assert "A" * 40 in output
