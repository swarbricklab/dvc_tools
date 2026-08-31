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
    build_tree,
    tree_view,
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

    def test_omits_size_and_hash_by_default(self):
        """Size/hash resolution is off by default (the expensive dvc path)."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="[]", stderr="")

            run_dvc_list()

            call_args = mock_run.call_args[0][0]
            assert "--size" not in call_args
            assert "--show-hash" not in call_args

    def test_requests_size_and_hash_when_asked(self):
        """Explicit size/show_hash add the corresponding dvc flags."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="[]", stderr="")

            run_dvc_list(size=True, show_hash=True)

            call_args = mock_run.call_args[0][0]
            assert "--size" in call_args
            assert "--show-hash" in call_args

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

    def test_plain_listing_skips_size_and_hash(self):
        """A bare path listing does not resolve sizes/hashes."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            list_files()

        kwargs = mock_list.call_args.kwargs
        assert kwargs["size"] is False
        assert kwargs["show_hash"] is False

    def test_long_format_requests_size(self):
        """Long format needs sizes, so dvc is asked to resolve them."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            list_files(long_format=True)

        assert mock_list.call_args.kwargs["size"] is True

    def test_show_hash_requests_hash(self):
        """--show-hash needs md5s, so dvc is asked to resolve them."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            list_files(show_hash=True)

        assert mock_list.call_args.kwargs["show_hash"] is True

    def test_json_output_requests_both(self):
        """JSON output preserves every field, so both are resolved."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            list_files(json_output=True)

        kwargs = mock_list.call_args.kwargs
        assert kwargs["size"] is True
        assert kwargs["show_hash"] is True


# =============================================================================
# build_tree tests
# =============================================================================

class TestBuildTree:
    """Tests for the build_tree function."""

    def test_empty_items_gives_empty_root(self):
        """An empty listing yields a root node with no files or subdirs."""
        tree = build_tree([])
        assert tree == {"_files": []}

    def test_nests_files_by_path_components(self):
        """Files are placed under nested directory nodes."""
        items = [
            {"path": "data/raw/a.csv", "isdir": False},
            {"path": "data/raw/b.csv", "isdir": False},
            {"path": "README.md", "isdir": False},
        ]
        tree = build_tree(items)

        assert "README.md" in tree["_files"]
        assert sorted(tree["data"]["raw"]["_files"]) == ["a.csv", "b.csv"]

    def test_directory_items_create_nodes(self):
        """An isdir item produces a directory node even with no files."""
        items = [{"path": "empty_dir/", "isdir": True}]
        tree = build_tree(items)

        assert "empty_dir" in tree
        assert tree["empty_dir"] == {"_files": []}

    def test_ignores_items_without_path(self):
        """Items with an empty path are skipped."""
        items = [{"path": "", "isdir": False}, {"path": "keep.txt", "isdir": False}]
        tree = build_tree(items)

        assert tree["_files"] == ["keep.txt"]


# =============================================================================
# tree_view tests
# =============================================================================

class TestTreeView:
    """Tests for the tree_view rendering entry point."""

    MOCK_ITEMS = [
        {"path": "data/raw/a.csv", "isdir": False, "isout": True},
        {"path": "data/proc/c.h5ad", "isdir": False, "isout": True},
        {"path": "src/train.py", "isdir": False, "isout": False},
    ]

    @pytest.fixture(autouse=True)
    def _no_git(self):
        """Neutralize git lookups so rendering tests don't touch the real repo.

        ``_git_tracked_paths``/``_git_ignored_paths`` returning None makes the
        tracked filter keep every (non-bookkeeping) item; ``_git_revision_info``
        returning None omits the revision subtitle. Filtering tests override
        these with explicit patches.
        """
        with patch("dt.ls._git_tracked_paths", return_value=None), \
                patch("dt.ls._git_ignored_paths", return_value=None), \
                patch("dt.ls._git_revision_info", return_value=None):
            yield

    def test_lists_full_workspace_recursively(self):
        """Tree view requests a recursive listing including git files."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            tree_view(output_format="text")

        kwargs = mock_list.call_args.kwargs
        assert kwargs["recursive"] is True
        assert kwargs["dvc_only"] is False

    def test_text_format_renders_ascii_tree(self):
        """Text format produces an ASCII tree with a summary line."""
        with patch("dt.ls.run_dvc_list", return_value=self.MOCK_ITEMS):
            output = tree_view(output_format="text")

        assert "├──" in output or "└──" in output
        assert "data/" in output
        assert "a.csv" in output
        assert "4 directories, 3 files" in output

    def test_md_format_wraps_tree_in_code_block(self):
        """Markdown format wraps the tree in a fenced code block."""
        with patch("dt.ls.run_dvc_list", return_value=self.MOCK_ITEMS):
            output = tree_view(output_format="md")

        assert "```text" in output
        assert output.rstrip().endswith("```")
        assert "Repository tree:" in output

    def test_html_format_uses_collapsible_foldup(self):
        """HTML format uses the details/summary fold-up with controls."""
        with patch("dt.ls.run_dvc_list", return_value=self.MOCK_ITEMS):
            output = tree_view(output_format="html")

        assert "<!DOCTYPE html>" in output
        assert "<details" in output
        assert "<summary>" in output
        assert "Expand all" in output
        assert "Collapse all" in output

    def test_html_escapes_file_names(self):
        """File names with HTML metacharacters are escaped."""
        items = [{"path": "weird <name>.csv", "isdir": False}]
        with patch("dt.ls.run_dvc_list", return_value=items):
            output = tree_view(output_format="html")

        assert "&lt;name&gt;" in output
        assert "<name>.csv" not in output

    def test_invalid_format_raises(self):
        """An unsupported output format raises LsError."""
        with patch("dt.ls.run_dvc_list", return_value=[]):
            with pytest.raises(LsError, match="Invalid tree output format"):
                tree_view(output_format="pdf")

    def test_applies_filters(self):
        """Filters (e.g. glob pattern) are applied before building the tree."""
        with patch("dt.ls.run_dvc_list", return_value=self.MOCK_ITEMS):
            output = tree_view(output_format="text", pattern="*.py")

        assert "train.py" in output
        assert "a.csv" not in output

    def test_dvc_only_restricts_listing(self):
        """--dvc-only passes dvc_only=True through to the listing."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            tree_view(output_format="text", dvc_only=True)

        assert mock_list.call_args.kwargs["dvc_only"] is True

    def test_forwards_url_for_remote_repos(self):
        """A repository URL is forwarded verbatim to dvc list."""
        url = "git@github.com:org/repo.git"
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            tree_view(url=url, output_format="text")

        assert mock_list.call_args.kwargs["url"] == url

    def test_level_collapses_deep_contents(self):
        """--level collapses contents below the given depth."""
        items = [
            {"path": "data/raw/a.csv", "isdir": False},
            {"path": "data/raw/b.csv", "isdir": False},
        ]
        with patch("dt.ls.run_dvc_list", return_value=items):
            output = tree_view(output_format="text", level=1)

        assert "data/" in output
        assert "… (2 files)" in output
        assert "a.csv" not in output

    def test_level_deeper_than_tree_shows_everything(self):
        """A generous --level renders the full tree with no placeholder."""
        items = [{"path": "data/raw/a.csv", "isdir": False}]
        with patch("dt.ls.run_dvc_list", return_value=items):
            output = tree_view(output_format="text", level=9)

        assert "a.csv" in output
        assert "…" not in output

    def test_level_placeholder_singular(self):
        """A single collapsed file reads '1 file', not '1 files'."""
        items = [{"path": "data/raw/a.csv", "isdir": False}]
        with patch("dt.ls.run_dvc_list", return_value=items):
            output = tree_view(output_format="text", level=1)

        assert "… (1 file)" in output

    def test_level_collapses_in_html(self):
        """--level collapses deep contents in the HTML fold-up too."""
        items = [
            {"path": "data/raw/a.csv", "isdir": False},
            {"path": "data/raw/b.csv", "isdir": False},
        ]
        with patch("dt.ls.run_dvc_list", return_value=items):
            output = tree_view(output_format="html", level=1)

        assert "… (2 files)" in output
        assert ">a.csv<" not in output

    def test_invalid_level_raises(self):
        """A level below 1 raises LsError."""
        with patch("dt.ls.run_dvc_list", return_value=[]):
            with pytest.raises(LsError, match="level must be 1 or greater"):
                tree_view(output_format="text", level=0)

    def test_skips_size_and_hash_when_no_filter(self):
        """Names-only tree does not ask dvc to resolve sizes/hashes."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            tree_view(output_format="text")

        kwargs = mock_list.call_args.kwargs
        assert kwargs["size"] is False
        assert kwargs["show_hash"] is False

    def test_requests_size_for_size_filter(self):
        """A --min-size/--max-size filter forces dvc to resolve sizes."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            tree_view(output_format="text", min_size="1M")

        assert mock_list.call_args.kwargs["size"] is True

    def test_requests_hash_for_hash_filter(self):
        """A --hash filter forces dvc to resolve hashes."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list:
            tree_view(output_format="text", hash_prefix="abc")

        assert mock_list.call_args.kwargs["show_hash"] is True

    # Items covering every tracking category, plus hidden bookkeeping/metadata.
    _MIXED = [
        {"path": "data/big.h5ad", "isdir": False, "isout": True},       # dvc object
        {"path": "data/big.h5ad.dvc", "isdir": False, "isout": False},  # pointer
        {"path": "dvc.lock", "isdir": False, "isout": False},           # lock
        {"path": ".gitignore", "isdir": False, "isout": False},         # vcs ignore
        {"path": ".dvcignore", "isdir": False, "isout": False},         # dvc ignore
        {"path": ".dvc/config", "isdir": False, "isout": False},        # .dvc dir
        {"path": ".dt/tmp/x", "isdir": False, "isout": False},          # .dt dir
        {"path": "README.md", "isdir": False, "isout": False},          # git-tracked
        {"path": "scratch.md", "isdir": False, "isout": False},         # untracked
        {"path": ".snakemake/log", "isdir": False, "isout": False},     # git-ignored
    ]
    _TRACKED = {"README.md", "data/big.h5ad.dvc", "dvc.lock",
                ".gitignore", ".dvcignore", ".dvc/config", ".dt/tmp/x"}
    _IGNORED = {".snakemake/log", "data/big.h5ad"}

    def _run_mixed(self, **kwargs):
        with patch("dt.ls.run_dvc_list", return_value=self._MIXED), \
                patch("dt.ls._git_tracked_paths", return_value=set(self._TRACKED)), \
                patch("dt.ls._git_ignored_paths", return_value=set(self._IGNORED)):
            return tree_view(output_format="text", **kwargs)

    def test_default_shows_tracked_and_dvc_objects(self):
        """Default keeps git-tracked files and DVC objects only."""
        output = self._run_mixed()
        assert "big.h5ad" in output      # dvc object kept
        assert "README.md" in output     # git-tracked kept

    def test_default_excludes_untracked_and_ignored(self):
        """Default drops untracked and git-ignored files."""
        output = self._run_mixed()
        assert "scratch.md" not in output      # untracked
        assert ".snakemake" not in output      # git-ignored

    def test_hides_bookkeeping_and_metadata(self):
        """Pointers, lock, ignore files, and .dvc/.dt dirs are never shown."""
        output = self._run_mixed()
        assert "big.h5ad.dvc" not in output    # *.dvc pointer
        assert "dvc.lock" not in output
        assert ".gitignore" not in output
        assert ".dvcignore" not in output
        assert ".dvc/" not in output and "config" not in output   # .dvc dir
        assert ".dt" not in output                                 # .dt dir
        assert "big.h5ad" in output            # the tracked object still shows

    def test_all_adds_untracked_but_not_ignored(self):
        """--all (include_untracked) shows untracked but still hides ignored."""
        output = self._run_mixed(include_untracked=True)
        assert "scratch.md" in output          # untracked now shown
        assert ".snakemake" not in output      # ignored still hidden
        assert "big.h5ad" in output            # dvc object still shown

    def test_dvc_only_skips_git_filtering(self):
        """--dvc-only defers to dvc list and does not consult git sets."""
        with patch("dt.ls.run_dvc_list", return_value=[]) as mock_list, \
                patch("dt.ls._git_tracked_paths") as mock_tracked:
            tree_view(output_format="text", dvc_only=True)

        assert mock_list.call_args.kwargs["dvc_only"] is True
        mock_tracked.assert_not_called()

    def test_remote_listing_drops_bookkeeping_only(self):
        """A remote URL is a tracked tree already; git sets aren't consulted."""
        items = [
            {"path": "data/big.h5ad", "isdir": False, "isout": True},
            {"path": "data/big.h5ad.dvc", "isdir": False, "isout": False},
            {"path": "README.md", "isdir": False, "isout": False},
        ]
        with patch("dt.ls.run_dvc_list", return_value=items), \
                patch("dt.ls._git_tracked_paths") as mock_tracked:
            output = tree_view(url="git@github.com:org/repo.git",
                               output_format="text")

        mock_tracked.assert_not_called()
        assert "README.md" in output           # committed file kept
        assert "big.h5ad.dvc" not in output    # bookkeeping still hidden

    def test_rev_listing_drops_bookkeeping_only(self):
        """A committed --rev is a tracked tree; only bookkeeping is stripped."""
        items = [
            {"path": "README.md", "isdir": False, "isout": False},
            {"path": "data/x.csv.dvc", "isdir": False, "isout": False},
        ]
        with patch("dt.ls.run_dvc_list", return_value=items), \
                patch("dt.ls._git_tracked_paths") as mock_tracked:
            output = tree_view(output_format="text", rev="v1.0")

        mock_tracked.assert_not_called()
        assert "README.md" in output
        assert "x.csv.dvc" not in output

    def test_revision_subtitle_in_text(self):
        """The revision (sha, tags, date) appears under the title."""
        items = [{"path": "README.md", "isdir": False, "isout": True}]
        info = {"sha": "a1b2c3d", "tags": ["v1.2.0"], "date": "2026-08-31"}
        with patch("dt.ls.run_dvc_list", return_value=items), \
                patch("dt.ls._git_revision_info", return_value=info):
            output = tree_view(output_format="text")

        assert "a1b2c3d · v1.2.0 · 2026-08-31" in output

    # Reusable identity for HTML-popup tests.
    _REPO = {
        "name": "myrepo",
        "web_url": "https://github.com/org/myrepo",
        "https_url": "https://github.com/org/myrepo.git",
        "ssh_url": "git@github.com:org/myrepo.git",
    }

    def test_html_title_links_to_repo(self):
        """The HTML title is the repo name linked to its web page."""
        items = [{"path": "data/a.csv", "isdir": False}]
        with patch("dt.ls.run_dvc_list", return_value=items), \
                patch("dt.ls._repo_identity", return_value=self._REPO):
            output = tree_view(output_format="html")

        assert '<a href="https://github.com/org/myrepo">myrepo</a>' in output

    def test_html_nodes_carry_full_path_and_popup(self):
        """Each node has a data-path and there is a dvc get/import popup."""
        items = [{"path": "data/raw/a.csv", "isdir": False}]
        with patch("dt.ls.run_dvc_list", return_value=items), \
                patch("dt.ls._repo_identity", return_value=self._REPO):
            output = tree_view(output_format="html")

        assert 'data-path="data/raw/a.csv"' in output      # file full path
        assert 'data-path="data"' in output                # dir full path
        assert 'class="cmd-btn"' in output
        # The get/import commands are assembled in JS from data-cmd values.
        assert "cmdBlock('get', 'Download a copy')" in output
        assert "cmdBlock('import', 'Import (track for updates)')" in output
        assert "'dvc ' + cmd + ' ' + repoUrl()" in output

    def test_html_popup_has_https_and_ssh_tabs(self):
        """The popup embeds both clone URLs, protocol tabs, and defaults HTTPS."""
        items = [{"path": "data/a.csv", "isdir": False}]
        with patch("dt.ls.run_dvc_list", return_value=items), \
                patch("dt.ls._repo_identity", return_value=self._REPO):
            output = tree_view(output_format="html")

        assert 'const REPO_HTTPS = "https://github.com/org/myrepo.git"' in output
        assert 'const REPO_SSH = "git@github.com:org/myrepo.git"' in output
        assert 'data-proto="https"' in output
        assert 'data-proto="ssh"' in output
        assert "proto = 'https'" in output          # default protocol

    def test_html_data_path_includes_subdir_prefix(self):
        """data-path is repo-root-relative even when listing a subdir."""
        items = [{"path": "a.csv", "isdir": False}]
        with patch("dt.ls.run_dvc_list", return_value=items), \
                patch("dt.ls._repo_identity", return_value=self._REPO):
            output = tree_view(output_format="html", path="data")

        assert 'data-path="data/a.csv"' in output


class TestParseRepoUrl:
    """Tests for repo-URL parsing and identity."""

    def test_scp_style_github(self):
        from dt.ls import _parse_repo_url
        name, web = _parse_repo_url("git@github.com:org/repo.git")
        assert name == "repo"
        assert web == "https://github.com/org/repo"

    def test_https_style_strips_git_suffix(self):
        from dt.ls import _parse_repo_url
        name, web = _parse_repo_url("https://github.com/org/repo.git")
        assert name == "repo"
        assert web == "https://github.com/org/repo"

    def test_local_path_has_no_web_url(self):
        from dt.ls import _parse_repo_url
        name, web = _parse_repo_url("/data/projects/myrepo")
        assert name == "myrepo"
        assert web is None

    def test_identity_derives_https_and_ssh_clone_urls(self):
        from dt.ls import _repo_identity
        ident = _repo_identity("git@github.com:org/repo.git")
        assert ident["name"] == "repo"
        assert ident["web_url"] == "https://github.com/org/repo"
        assert ident["https_url"] == "https://github.com/org/repo.git"
        assert ident["ssh_url"] == "git@github.com:org/repo.git"

    def test_identity_derives_ssh_from_https_origin(self):
        from dt.ls import _repo_identity
        ident = _repo_identity("https://gitlab.com/grp/sub/repo.git")
        assert ident["name"] == "repo"
        assert ident["https_url"] == "https://gitlab.com/grp/sub/repo.git"
        assert ident["ssh_url"] == "git@gitlab.com:grp/sub/repo.git"

    def test_identity_unresolvable_url_falls_back(self):
        from dt.ls import _repo_identity
        ident = _repo_identity("/local/path/myrepo")
        assert ident["web_url"] is None
        # Both protocols collapse to the raw source (the popup hides its tabs).
        assert ident["https_url"] == ident["ssh_url"] == "/local/path/myrepo"


class TestHiddenInTree:
    """Tests for which paths the tree suppresses."""

    def test_hidden_paths(self):
        from dt.ls import _is_hidden_in_tree
        hidden = [
            "data.csv.dvc", "dvc.lock", ".gitignore", ".dvcignore",
            ".dvc/config", ".dvc/.gitignore", ".dt/tmp/x",
            "sub/.gitignore", "nested/.dt/thing",
        ]
        for p in hidden:
            assert _is_hidden_in_tree(p), p

    def test_visible_paths(self):
        from dt.ls import _is_hidden_in_tree
        visible = [
            "data.csv", "README.md", "src/train.py",
            "notes.dvcignore.md",       # not exactly .dvcignore
            "dt/module.py",             # 'dt' != '.dt'
        ]
        for p in visible:
            assert not _is_hidden_in_tree(p), p
