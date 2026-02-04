"""Unit tests for dt.ls module.

Tests list and filter functionality for DVC-tracked files.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.ls import (
    parse_size,
    format_size,
    run_dvc_list,
    filter_items,
    format_output,
    list_files,
)
from dt.errors import LsError


# =============================================================================
# parse_size tests
# =============================================================================

class TestParseSize:
    """Tests for the parse_size function."""

    def test_parses_plain_bytes(self):
        """Test parsing plain byte values."""
        assert parse_size("100") == 100
        assert parse_size("0") == 0
        assert parse_size("1000000") == 1000000

    def test_parses_kilobytes(self):
        """Test parsing kilobyte values."""
        assert parse_size("1K") == 1024
        assert parse_size("10K") == 10 * 1024
        assert parse_size("1k") == 1024  # lowercase

    def test_parses_megabytes(self):
        """Test parsing megabyte values."""
        assert parse_size("1M") == 1024 ** 2
        assert parse_size("5M") == 5 * 1024 ** 2
        assert parse_size("1m") == 1024 ** 2  # lowercase

    def test_parses_gigabytes(self):
        """Test parsing gigabyte values."""
        assert parse_size("1G") == 1024 ** 3
        assert parse_size("2G") == 2 * 1024 ** 3
        assert parse_size("1g") == 1024 ** 3  # lowercase

    def test_parses_terabytes(self):
        """Test parsing terabyte values."""
        assert parse_size("1T") == 1024 ** 4
        assert parse_size("1t") == 1024 ** 4  # lowercase

    def test_parses_decimal_values(self):
        """Test parsing decimal values with units."""
        assert parse_size("1.5K") == int(1.5 * 1024)
        assert parse_size("2.5M") == int(2.5 * 1024 ** 2)

    def test_strips_whitespace(self):
        """Test that whitespace is stripped."""
        assert parse_size("  100  ") == 100
        assert parse_size("  1K  ") == 1024

    def test_raises_error_for_invalid_size(self):
        """Test LsError raised for invalid size strings."""
        with pytest.raises(LsError, match="Invalid size"):
            parse_size("abc")
        
        with pytest.raises(LsError, match="Invalid size"):
            parse_size("1X")  # Invalid unit


# =============================================================================
# format_size tests
# =============================================================================

class TestFormatSize:
    """Tests for the format_size function."""

    def test_returns_dash_for_none(self):
        """Test that None returns dash."""
        assert format_size(None) == "-"

    def test_formats_small_bytes(self):
        """Test formatting small byte values."""
        assert format_size(0) == "0"
        assert format_size(100) == "100"
        assert format_size(1023) == "1023"

    def test_formats_kilobytes(self):
        """Test formatting kilobyte values."""
        result = format_size(1024)
        assert "K" in result
        assert "1" in result

    def test_formats_megabytes(self):
        """Test formatting megabyte values."""
        result = format_size(1024 ** 2)
        assert "M" in result

    def test_formats_gigabytes(self):
        """Test formatting gigabyte values."""
        result = format_size(1024 ** 3)
        assert "G" in result

    def test_formats_terabytes(self):
        """Test formatting terabyte values."""
        result = format_size(1024 ** 4)
        assert "T" in result


# =============================================================================
# run_dvc_list tests
# =============================================================================

class TestRunDvcList:
    """Tests for the run_dvc_list function."""

    def test_runs_dvc_list_command(self):
        """Test that dvc list is called with correct arguments."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='[{"path": "data.csv", "isdir": false}]',
                stderr="",
            )
            
            result = run_dvc_list()
            
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert "dvc" in call_args
            assert "list" in call_args
            assert "--json" in call_args

    def test_includes_path_argument(self):
        """Test that path argument is included."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="[]",
            )
            
            run_dvc_list(path="data/")
            
            call_args = mock_run.call_args[0][0]
            assert "data/" in call_args

    def test_includes_rev_argument(self):
        """Test that rev argument is included."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="[]",
            )
            
            run_dvc_list(rev="v1.0")
            
            call_args = mock_run.call_args[0][0]
            assert "--rev" in call_args
            assert "v1.0" in call_args

    def test_includes_recursive_flag(self):
        """Test that recursive flag is included."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="[]",
            )
            
            run_dvc_list(recursive=True)
            
            call_args = mock_run.call_args[0][0]
            assert "--recursive" in call_args

    def test_raises_ls_error_on_failure(self):
        """Test LsError raised when dvc list fails."""
        with patch("subprocess.run") as mock_run:
            import subprocess
            mock_run.side_effect = subprocess.CalledProcessError(1, "dvc", stderr="error")
            
            with pytest.raises(LsError, match="dvc list failed"):
                run_dvc_list()

    def test_raises_ls_error_on_invalid_json(self):
        """Test LsError raised when output is invalid JSON."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="not json",
            )
            
            with pytest.raises(LsError, match="Failed to parse"):
                run_dvc_list()


# =============================================================================
# filter_items tests
# =============================================================================

class TestFilterItems:
    """Tests for the filter_items function."""

    def test_returns_all_items_with_no_filters(self):
        """Test that all items are returned with no filters."""
        items = [
            {"path": "data.csv", "isdir": False, "size": 100},
            {"path": "images/", "isdir": True, "size": 500},
        ]
        
        result = filter_items(items)
        
        assert len(result) == 2

    def test_filters_by_glob_pattern(self):
        """Test filtering by glob pattern."""
        items = [
            {"path": "data.csv", "isdir": False},
            {"path": "data.txt", "isdir": False},
            {"path": "other.json", "isdir": False},
        ]
        
        result = filter_items(items, pattern="*.csv")
        
        assert len(result) == 1
        assert result[0]["path"] == "data.csv"

    def test_filters_by_regex(self):
        """Test filtering by regex pattern."""
        items = [
            {"path": "data_v1.csv", "isdir": False},
            {"path": "data_v2.csv", "isdir": False},
            {"path": "other.csv", "isdir": False},
        ]
        
        result = filter_items(items, regex=r"data_v\d+\.csv")
        
        assert len(result) == 2

    def test_raises_error_for_invalid_regex(self):
        """Test LsError raised for invalid regex."""
        items = [{"path": "test.csv", "isdir": False}]
        
        with pytest.raises(LsError, match="Invalid regex"):
            filter_items(items, regex="[invalid")

    def test_filters_by_min_size(self):
        """Test filtering by minimum size."""
        items = [
            {"path": "small.csv", "size": 100},
            {"path": "large.csv", "size": 10000},
        ]
        
        result = filter_items(items, min_size=1000)
        
        assert len(result) == 1
        assert result[0]["path"] == "large.csv"

    def test_filters_by_max_size(self):
        """Test filtering by maximum size."""
        items = [
            {"path": "small.csv", "size": 100},
            {"path": "large.csv", "size": 10000},
        ]
        
        result = filter_items(items, max_size=500)
        
        assert len(result) == 1
        assert result[0]["path"] == "small.csv"

    def test_files_only_filter(self):
        """Test filtering to files only."""
        items = [
            {"path": "data.csv", "isdir": False},
            {"path": "images/", "isdir": True},
        ]
        
        result = filter_items(items, files_only=True)
        
        assert len(result) == 1
        assert result[0]["path"] == "data.csv"

    def test_dirs_only_filter(self):
        """Test filtering to directories only."""
        items = [
            {"path": "data.csv", "isdir": False},
            {"path": "images/", "isdir": True},
        ]
        
        result = filter_items(items, dirs_only=True)
        
        assert len(result) == 1
        assert result[0]["path"] == "images/"

    def test_exec_only_filter(self):
        """Test filtering to executable files only."""
        items = [
            {"path": "script.sh", "isdir": False, "isexec": True},
            {"path": "data.csv", "isdir": False, "isexec": False},
        ]
        
        result = filter_items(items, exec_only=True)
        
        assert len(result) == 1
        assert result[0]["path"] == "script.sh"

    def test_hash_prefix_filter(self):
        """Test filtering by hash prefix."""
        items = [
            {"path": "data.csv", "md5": "abc123"},
            {"path": "other.csv", "md5": "def456"},
        ]
        
        result = filter_items(items, hash_prefix="abc")
        
        assert len(result) == 1
        assert result[0]["path"] == "data.csv"

    def test_hash_prefix_strips_dir_suffix(self):
        """Test that .dir suffix is stripped when matching hash prefix."""
        items = [
            {"path": "dataset/", "md5": "abc123.dir"},
        ]
        
        result = filter_items(items, hash_prefix="abc")
        
        assert len(result) == 1


# =============================================================================
# format_output tests
# =============================================================================

class TestFormatOutput:
    """Tests for the format_output function."""

    def test_json_output(self):
        """Test JSON output format."""
        items = [
            {"path": "data.csv", "size": 100, "md5": "abc123"},
        ]
        
        result = format_output(items, json_output=True)
        parsed = json.loads(result)
        
        assert len(parsed) == 1
        assert parsed[0]["path"] == "data.csv"

    def test_empty_items_returns_empty_string(self):
        """Test that empty items returns empty string."""
        result = format_output([])
        assert result == ""

    def test_simple_path_output(self):
        """Test simple path-only output."""
        items = [
            {"path": "data.csv"},
            {"path": "other.txt"},
        ]
        
        result = format_output(items)
        lines = result.split("\n")
        
        assert len(lines) == 2
        assert lines[0] == "data.csv"
        assert lines[1] == "other.txt"

    def test_long_format_includes_type_and_size(self):
        """Test long format includes type indicator and size."""
        items = [
            {"path": "data.csv", "isdir": False, "size": 1024},
        ]
        
        result = format_output(items, long_format=True)
        
        assert "-" in result  # file type indicator
        assert "1" in result  # size
        assert "K" in result  # size unit

    def test_long_format_dir_indicator(self):
        """Test long format shows 'd' for directories."""
        items = [
            {"path": "images/", "isdir": True, "size": 5000},
        ]
        
        result = format_output(items, long_format=True)
        
        assert "d" in result

    def test_show_hash_includes_md5(self):
        """Test show_hash includes MD5 hash."""
        items = [
            {"path": "data.csv", "md5": "abc123def456"},
        ]
        
        result = format_output(items, show_hash=True)
        
        assert "abc123def456" in result


# =============================================================================
# list_files tests
# =============================================================================

class TestListFiles:
    """Tests for the list_files function."""

    def test_combines_list_filter_and_format(self):
        """Test that list_files combines all operations."""
        mock_items = [
            {"path": "data.csv", "isdir": False, "size": 1000, "md5": "abc"},
            {"path": "large.bin", "isdir": False, "size": 100000, "md5": "def"},
        ]
        
        with patch("dt.ls.run_dvc_list", return_value=mock_items):
            items, output = list_files(min_size="10K")
            
            # Only large file should pass filter
            assert len(items) == 1
            assert items[0]["path"] == "large.bin"
            assert "large.bin" in output

    def test_parses_size_strings(self):
        """Test that size strings are parsed."""
        mock_items = [
            {"path": "data.csv", "isdir": False, "size": 500, "md5": "abc"},
        ]
        
        with patch("dt.ls.run_dvc_list", return_value=mock_items):
            items, output = list_files(max_size="1K")
            
            # File under 1K should pass
            assert len(items) == 1

    def test_returns_tuple_of_items_and_output(self):
        """Test that tuple is returned."""
        mock_items = [{"path": "test.csv", "isdir": False}]
        
        with patch("dt.ls.run_dvc_list", return_value=mock_items):
            result = list_files()
            
            assert isinstance(result, tuple)
            assert len(result) == 2
            items, output = result
            assert isinstance(items, list)
            assert isinstance(output, str)
