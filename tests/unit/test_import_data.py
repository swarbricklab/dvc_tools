"""Tests for dt import_data module.

Covers the --no-download and --csv import features.
"""

import csv
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import pytest
import yaml

from dt import import_data as import_mod
from dt import tmp as tmp_mod
from dt import utils
from dt.errors import ImportError_ as ImportError


# =============================================================================
# resolve_out_path
# =============================================================================

class TestResolveOutPath:
    """Tests for resolve_out_path()."""

    def test_none_uses_basename(self):
        """out=None returns basename of path."""
        result = import_mod.resolve_out_path(None, "data/processed/file.csv")
        assert result == Path("file.csv")

    def test_file_path_used_directly(self, tmp_path):
        """Non-directory out is treated as a file path."""
        result = import_mod.resolve_out_path("my_output.csv", "data/file.csv")
        assert result == Path("my_output.csv")

    def test_existing_directory_places_inside(self, tmp_path):
        """Existing directory → basename is appended."""
        result = import_mod.resolve_out_path(str(tmp_path), "data/file.csv")
        assert result == tmp_path / "file.csv"

    def test_trailing_slash_creates_directory(self, tmp_path):
        """Trailing slash creates directory and places import inside."""
        new_dir = tmp_path / "newdir"
        result = import_mod.resolve_out_path(str(new_dir) + "/", "data/file.csv")
        assert result == new_dir / "file.csv"
        assert new_dir.is_dir()

    def test_nested_trailing_slash_creates_parents(self, tmp_path):
        """Trailing slash with nested path creates all parents."""
        nested = tmp_path / "a" / "b" / "c"
        result = import_mod.resolve_out_path(str(nested) + "/", "data/file.csv")
        assert result == nested / "file.csv"
        assert nested.is_dir()

    def test_subdirectory_file_path(self):
        """out with subdirectory is treated as file path."""
        result = import_mod.resolve_out_path("subdir/output.csv", "data/file.csv")
        assert result == Path("subdir/output.csv")

    def test_basename_from_deep_path(self):
        """Basename is extracted from deeply nested source path."""
        result = import_mod.resolve_out_path(None, "a/b/c/d/myfile.h5ad")
        assert result == Path("myfile.h5ad")


# =============================================================================
# create_no_download_dvc_file
# =============================================================================

class TestCreateNoDownloadDvcFile:
    """Tests for create_no_download_dvc_file()."""

    def test_creates_dvc_file(self, tmp_path):
        """Creates a .dvc file at the correct path."""
        result = import_mod.create_no_download_dvc_file(
            dest_path=tmp_path,
            name="processed",
            repo_url="git@github.com:org/repo.git",
            repo_path="data/processed",
            rev_lock="abc123",
        )

        assert result == tmp_path / "processed.dvc"
        assert result.exists()

    def test_dvc_file_content_matches_no_download_format(self, tmp_path):
        """Output matches dvc import --no-download format: deps + outs without hash/size."""
        dvc_file = import_mod.create_no_download_dvc_file(
            dest_path=tmp_path,
            name="mydata",
            repo_url="git@github.com:org/repo.git",
            repo_path="data/mydata",
            rev_lock="deadbeef1234",
        )

        with open(dvc_file) as f:
            content = yaml.safe_load(f)

        # deps section
        assert 'deps' in content
        assert len(content['deps']) == 1
        dep = content['deps'][0]
        assert dep['path'] == 'data/mydata'
        assert dep['repo']['url'] == 'git@github.com:org/repo.git'
        assert dep['repo']['rev_lock'] == 'deadbeef1234'

        # outs section - no md5 or size
        assert 'outs' in content
        assert len(content['outs']) == 1
        out = content['outs'][0]
        assert out['path'] == 'mydata'
        assert out['hash'] == 'md5'
        assert 'md5' not in out  # no hash value
        assert 'size' not in out
        assert 'nfiles' not in out

    def test_creates_parent_directory(self, tmp_path):
        """Creates parent directories if they don't exist."""
        dest = tmp_path / "sub" / "dir"
        dvc_file = import_mod.create_no_download_dvc_file(
            dest_path=dest,
            name="file.csv",
            repo_url="git@github.com:org/repo.git",
            repo_path="data/file.csv",
            rev_lock="abc123",
        )

        assert dvc_file.exists()
        assert dvc_file == dest / "file.csv.dvc"

    def test_strips_trailing_slash_from_name(self, tmp_path):
        """Trailing slash on dir name is stripped for filename."""
        dvc_file = import_mod.create_no_download_dvc_file(
            dest_path=tmp_path,
            name="mydir/",
            repo_url="git@github.com:org/repo.git",
            repo_path="data/mydir",
            rev_lock="abc123",
        )

        # The path in outs should be "mydir/" (as passed), but filename should strip it
        assert dvc_file.name == "mydir.dvc"


# =============================================================================
# get_rev_from_tmp_clone
# =============================================================================

class TestGetRevFromTmpClone:
    """Tests for get_rev_from_tmp_clone()."""

    @patch.object(tmp_mod, 'clone_repo')
    @patch('subprocess.run')
    def test_returns_clone_path_and_rev(self, mock_run, mock_clone, tmp_path):
        """Returns (clone_path, rev_lock) from tmp clone HEAD."""
        mock_clone.return_value = tmp_path
        mock_run.return_value = MagicMock(
            returncode=0, stdout="abc123def456\n"
        )

        clone_path, rev = import_mod.get_rev_from_tmp_clone("myrepo")

        assert clone_path == tmp_path
        assert rev == "abc123def456"
        mock_clone.assert_called_once_with("myrepo", owner=None, refresh=True, verbose=False)

    @patch.object(tmp_mod, 'clone_repo')
    @patch('subprocess.run')
    def test_passes_owner_and_refresh(self, mock_run, mock_clone, tmp_path):
        """Passes owner and refresh flags through to clone_repo."""
        mock_clone.return_value = tmp_path
        mock_run.return_value = MagicMock(returncode=0, stdout="abc123\n")

        import_mod.get_rev_from_tmp_clone(
            "myrepo", owner="myorg", refresh=False, verbose=True
        )

        mock_clone.assert_called_once_with(
            "myrepo", owner="myorg", refresh=False, verbose=True
        )

    @patch.object(tmp_mod, 'clone_repo', side_effect=tmp_mod.TmpError("fail"))
    def test_raises_import_error_on_clone_failure(self, mock_clone):
        """Raises ImportError when clone fails."""
        with pytest.raises(ImportError, match="Failed to clone"):
            import_mod.get_rev_from_tmp_clone("myrepo")

    @patch.object(tmp_mod, 'clone_repo')
    @patch('subprocess.run')
    def test_raises_import_error_on_rev_parse_failure(self, mock_run, mock_clone, tmp_path):
        """Raises ImportError when git rev-parse fails."""
        mock_clone.return_value = tmp_path
        mock_run.return_value = MagicMock(returncode=1, stdout="")

        with pytest.raises(ImportError, match="Could not determine revision"):
            import_mod.get_rev_from_tmp_clone("myrepo")


# =============================================================================
# import_no_download
# =============================================================================

class TestImportNoDownload:
    """Tests for import_no_download()."""

    @patch.object(utils, 'update_gitignore', return_value=True)
    @patch.object(import_mod, 'get_rev_from_tmp_clone')
    @patch.object(tmp_mod, 'resolve_repository_url')
    @patch.object(utils, 'check_dvc')
    def test_creates_no_download_dvc_file(
        self, mock_check, mock_resolve, mock_rev, mock_gitignore, tmp_path, monkeypatch
    ):
        """Creates a .dvc file without downloading data."""
        monkeypatch.chdir(tmp_path)
        mock_resolve.return_value = "git@github.com:org/repo.git"
        mock_rev.return_value = (tmp_path / "clone", "abc123def")

        result = import_mod.import_no_download(
            repository="repo",
            path="data/processed",
        )

        assert result.name == "processed.dvc"
        assert result.exists()

        with open(result) as f:
            content = yaml.safe_load(f)

        assert content['deps'][0]['repo']['rev_lock'] == 'abc123def'
        assert 'size' not in content['outs'][0]

    @patch.object(utils, 'update_gitignore', return_value=True)
    @patch.object(tmp_mod, 'resolve_repository_url')
    @patch.object(utils, 'check_dvc')
    def test_uses_explicit_rev(
        self, mock_check, mock_resolve, mock_gitignore, tmp_path, monkeypatch
    ):
        """Uses explicit --rev instead of cloning."""
        monkeypatch.chdir(tmp_path)
        mock_resolve.return_value = "git@github.com:org/repo.git"

        result = import_mod.import_no_download(
            repository="repo",
            path="data/file.csv",
            rev="explicit_rev_123",
        )

        with open(result) as f:
            content = yaml.safe_load(f)

        assert content['deps'][0]['repo']['rev_lock'] == 'explicit_rev_123'

    @patch.object(utils, 'update_gitignore', return_value=True)
    @patch.object(import_mod, 'get_rev_from_tmp_clone')
    @patch.object(tmp_mod, 'resolve_repository_url')
    @patch.object(utils, 'check_dvc')
    def test_respects_out_option(
        self, mock_check, mock_resolve, mock_rev, mock_gitignore, tmp_path, monkeypatch
    ):
        """Respects the -o/--out option for destination path."""
        monkeypatch.chdir(tmp_path)
        mock_resolve.return_value = "git@github.com:org/repo.git"
        mock_rev.return_value = (tmp_path, "abc123")

        result = import_mod.import_no_download(
            repository="repo",
            path="data/processed",
            out="my_output",
        )

        assert result.name == "my_output.dvc"

    @patch.object(utils, 'update_gitignore', return_value=True)
    @patch.object(import_mod, 'get_rev_from_tmp_clone')
    @patch.object(tmp_mod, 'resolve_repository_url')
    @patch.object(utils, 'check_dvc')
    def test_updates_gitignore(
        self, mock_check, mock_resolve, mock_rev, mock_gitignore, tmp_path, monkeypatch
    ):
        """Adds output name to .gitignore."""
        monkeypatch.chdir(tmp_path)
        mock_resolve.return_value = "git@github.com:org/repo.git"
        mock_rev.return_value = (tmp_path, "abc123")

        import_mod.import_no_download(
            repository="repo",
            path="data/processed",
        )

        mock_gitignore.assert_called_once_with("/processed")

    @patch.object(utils, 'check_dvc', side_effect=utils.DependencyError("no dvc"))
    def test_raises_if_dvc_not_installed(self, mock_check):
        """Raises ImportError if dvc is not installed."""
        with pytest.raises(ImportError, match="no dvc"):
            import_mod.import_no_download(
                repository="repo",
                path="data/file",
            )


# =============================================================================
# import_from_csv
# =============================================================================

class TestImportFromCsv:
    """Tests for import_from_csv()."""

    def _write_csv(self, path, rows, fieldnames=None):
        """Helper to write a CSV file."""
        if fieldnames is None:
            fieldnames = rows[0].keys()
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    @patch.object(import_mod, 'import_data')
    def test_imports_each_row(self, mock_import, tmp_path, monkeypatch):
        """Calls import_data for each row in CSV."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file1.csv'},
            {'path': 'data/file2.csv'},
        ])
        mock_import.return_value = (Path("file1.csv.dvc"), "/cache")

        results = import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
        )

        assert len(results) == 2
        assert mock_import.call_count == 2

    @patch.object(import_mod, 'import_data')
    def test_uses_output_column_for_out(self, mock_import, tmp_path, monkeypatch):
        """Uses 'output' column as -o/--out argument."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv', 'output': 'my_output.csv'},
        ])
        mock_import.return_value = (Path("my_output.csv.dvc"), "/cache")

        import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
        )

        _, kwargs = mock_import.call_args
        assert kwargs['out'] == 'my_output.csv'

    @patch.object(import_mod, 'import_no_download')
    def test_no_download_mode(self, mock_nd, tmp_path, monkeypatch):
        """Calls import_no_download when no_download=True."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv'},
        ])
        mock_nd.return_value = Path("file.csv.dvc")

        results = import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
            no_download=True,
        )

        assert len(results) == 1
        assert results[0][1] is True  # success
        mock_nd.assert_called_once()

    def test_raises_if_csv_not_found(self, tmp_path):
        """Raises ImportError if CSV file doesn't exist."""
        with pytest.raises(ImportError, match="CSV file not found"):
            import_mod.import_from_csv(
                csv_path=str(tmp_path / "nofile.csv"),
                repository="myrepo",
            )

    def test_raises_if_no_path_column(self, tmp_path):
        """Raises ImportError if CSV has no 'path' column."""
        csv_file = tmp_path / "bad.csv"
        self._write_csv(csv_file, [
            {'name': 'foo', 'value': 'bar'},
        ])

        with pytest.raises(ImportError, match="must have a 'path' column"):
            import_mod.import_from_csv(
                csv_path=str(csv_file),
                repository="myrepo",
            )

    def test_raises_if_csv_empty(self, tmp_path):
        """Raises ImportError if CSV has headers but no data rows."""
        csv_file = tmp_path / "empty.csv"
        with open(csv_file, 'w') as f:
            f.write("path\n")

        with pytest.raises(ImportError, match="CSV file is empty"):
            import_mod.import_from_csv(
                csv_path=str(csv_file),
                repository="myrepo",
            )

    @patch.object(import_mod, 'import_data')
    def test_skips_empty_path_rows(self, mock_import, tmp_path, monkeypatch):
        """Rows with empty 'path' are skipped with an error."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': ''},
            {'path': 'data/real.csv'},
        ])
        mock_import.return_value = (Path("real.csv.dvc"), "/cache")

        results = import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
        )

        assert len(results) == 2
        assert results[0][1] is False  # empty path failed
        assert results[1][1] is True   # real path succeeded
        assert mock_import.call_count == 1

    @patch.object(import_mod, 'import_data', side_effect=Exception("import failed"))
    def test_captures_per_row_errors(self, mock_import, tmp_path, monkeypatch):
        """Individual row failures are captured, not raised."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv'},
        ])

        results = import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
        )

        assert len(results) == 1
        assert results[0][1] is False
        assert "import failed" in results[0][2]

    @patch.object(import_mod, 'import_data')
    def test_ignores_extra_columns(self, mock_import, tmp_path, monkeypatch):
        """Extra CSV columns are silently ignored."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv', 'output': '', 'notes': 'some info', 'priority': '1'},
        ])
        mock_import.return_value = (Path("file.csv.dvc"), "/cache")

        results = import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
        )

        assert len(results) == 1
        assert results[0][1] is True

    @patch.object(import_mod, 'import_no_download')
    def test_passes_rev_through(self, mock_nd, tmp_path, monkeypatch):
        """--rev is passed through to import_no_download."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv'},
        ])
        mock_nd.return_value = Path("file.csv.dvc")

        import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
            no_download=True,
            rev="v1.0.0",
        )

        _, kwargs = mock_nd.call_args
        assert kwargs['rev'] == 'v1.0.0'

    @patch.object(import_mod, 'import_data')
    def test_passes_owner_through(self, mock_import, tmp_path, monkeypatch):
        """--owner is passed through to import_data."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv'},
        ])
        mock_import.return_value = (Path("file.csv.dvc"), "/cache")

        import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
            owner="myorg",
        )

        _, kwargs = mock_import.call_args
        assert kwargs['owner'] == 'myorg'

    @patch.object(import_mod, 'import_data')
    def test_falls_back_to_out_when_no_output_column(self, mock_import, tmp_path, monkeypatch):
        """When CSV has no 'output' column, uses the out parameter."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv'},
        ])
        mock_import.return_value = (Path("dest.csv.dvc"), "/cache")

        import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
            out="dest.csv",
        )

        _, kwargs = mock_import.call_args
        assert kwargs['out'] == 'dest.csv'

    @patch.object(import_mod, 'import_data')
    def test_csv_output_column_overrides_out(self, mock_import, tmp_path, monkeypatch):
        """CSV 'output' column takes precedence over the out parameter."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv', 'output': 'from_csv.csv'},
        ])
        mock_import.return_value = (Path("from_csv.csv.dvc"), "/cache")

        import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
            out="from_cli.csv",
        )

        _, kwargs = mock_import.call_args
        assert kwargs['out'] == 'from_csv.csv'

    @patch.object(import_mod, 'import_data')
    def test_empty_output_cell_falls_back_to_out(self, mock_import, tmp_path, monkeypatch):
        """Empty 'output' cell in CSV falls back to the out parameter."""
        monkeypatch.chdir(tmp_path)
        csv_file = tmp_path / "paths.csv"
        self._write_csv(csv_file, [
            {'path': 'data/file.csv', 'output': ''},
        ])
        mock_import.return_value = (Path("fallback.csv.dvc"), "/cache")

        import_mod.import_from_csv(
            csv_path=str(csv_file),
            repository="myrepo",
            out="fallback.csv",
        )

        _, kwargs = mock_import.call_args
        assert kwargs['out'] == 'fallback.csv'


# =============================================================================
# create_dvc_file (existing function – contrast with no-download)
# =============================================================================

class TestCreateDvcFile:
    """Tests for the existing create_dvc_file function."""

    def test_import_dvc_file_has_hash_and_size(self, tmp_path):
        """Normal import .dvc file contains md5/size/hash fields."""
        dvc_file = import_mod.create_dvc_file(
            dest_path=tmp_path,
            name="data",
            md5="abc123.dir",
            size=4096,
            nfiles=10,
            repo_url="git@github.com:org/repo.git",
            repo_path="data",
            rev_lock="rev123",
        )

        with open(dvc_file) as f:
            content = yaml.safe_load(f)

        out = content['outs'][0]
        assert out['md5'] == 'abc123.dir'
        assert out['size'] == 4096
        assert out['hash'] == 'md5'
        assert out['nfiles'] == 10

    def test_no_download_file_lacks_hash_and_size(self, tmp_path):
        """No-download .dvc file does NOT contain md5/size."""
        dvc_file = import_mod.create_no_download_dvc_file(
            dest_path=tmp_path,
            name="data",
            repo_url="git@github.com:org/repo.git",
            repo_path="data",
            rev_lock="rev123",
        )

        with open(dvc_file) as f:
            content = yaml.safe_load(f)

        out = content['outs'][0]
        assert 'md5' not in out or out.get('md5') is None
        assert 'size' not in out
        assert 'nfiles' not in out
        assert out['hash'] == 'md5'
