"""Tests for dt.utils module.

Tests utility functions that can be unit tested in isolation.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from dt import utils
from dt.errors import DependencyError, DVCFileError, TargetNotFoundError


# =============================================================================
# Formatting Utilities
# =============================================================================

class TestFormatSize:
    """Tests for format_size function."""
    
    def test_human_readable_bytes(self):
        """Small sizes shown in bytes."""
        result = utils.format_size(100, human_readable=True)
        assert 'B' in result or '100' in result
    
    def test_human_readable_kilobytes(self):
        """KB sizes formatted correctly."""
        result = utils.format_size(2048, human_readable=True)
        # Should show as ~2 KB
        assert 'K' in result or 'KB' in result or '2' in result
    
    def test_human_readable_megabytes(self):
        """MB sizes formatted correctly."""
        result = utils.format_size(5 * 1024 * 1024, human_readable=True)
        assert 'M' in result or 'MB' in result
    
    def test_human_readable_gigabytes(self):
        """GB sizes formatted correctly."""
        result = utils.format_size(3 * 1024 * 1024 * 1024, human_readable=True)
        assert 'G' in result or 'GB' in result
    
    def test_human_readable_terabytes(self):
        """TB sizes formatted correctly."""
        result = utils.format_size(2 * 1024 * 1024 * 1024 * 1024, human_readable=True)
        assert 'T' in result or 'TB' in result
    
    def test_not_human_readable(self):
        """Raw bytes returned when human_readable=False."""
        result = utils.format_size(12345, human_readable=False)
        assert result == '12345'
    
    def test_zero_bytes(self):
        """Zero bytes handled correctly."""
        result = utils.format_size(0, human_readable=True)
        assert '0' in result


# =============================================================================
# DVC Cache Utilities
# =============================================================================

class TestHashToCachePath:
    """Tests for hash_to_cache_path function."""
    
    def test_regular_hash(self, tmp_path):
        """Regular hash converted to cache path."""
        cache_dir = tmp_path / 'cache'
        file_hash = 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6'
        
        result = utils.hash_to_cache_path(cache_dir, file_hash)
        
        expected = cache_dir / 'a1' / 'b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6'
        assert result == expected
    
    def test_dir_suffix_hash(self, tmp_path):
        """Hash with .dir suffix handled correctly."""
        cache_dir = tmp_path / 'cache'
        file_hash = 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6.dir'
        
        result = utils.hash_to_cache_path(cache_dir, file_hash)
        
        expected = cache_dir / 'a1' / 'b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6.dir'
        assert result == expected


class TestGetCacheDir:
    """Tests for get_cache_dir function."""
    
    def test_returns_none_outside_dvc_repo(self, tmp_path, monkeypatch):
        """Returns None when not in a DVC repo."""
        monkeypatch.chdir(tmp_path)
        result = utils.get_cache_dir()
        assert result is None
    
    @pytest.mark.skipif(not shutil.which('dvc'), reason="DVC not installed")
    def test_returns_path_in_dvc_repo(self, tmp_path, monkeypatch):
        """Returns Path when in a DVC repo with cache configured."""
        # Initialize git and DVC
        monkeypatch.chdir(tmp_path)
        subprocess.run(['git', 'init'], capture_output=True)
        subprocess.run(['dvc', 'init'], capture_output=True)
        
        result = utils.get_cache_dir()
        # Should return a Path or None if cache not configured
        assert result is None or isinstance(result, Path)


# =============================================================================
# DVC File Utilities
# =============================================================================

class TestParseDvcFile:
    """Tests for parse_dvc_file function."""
    
    def test_valid_dvc_file(self, tmp_path):
        """Valid .dvc file parsed correctly."""
        dvc_file = tmp_path / 'data.csv.dvc'
        dvc_file.write_text(
            'outs:\n'
            '  - md5: a1b2c3d4e5f6\n'
            '    path: data.csv\n'
        )
        
        result = utils.parse_dvc_file(dvc_file)
        
        assert 'outs' in result
        assert len(result['outs']) == 1
        assert result['outs'][0]['md5'] == 'a1b2c3d4e5f6'
        assert result['outs'][0]['path'] == 'data.csv'
    
    def test_empty_dvc_file(self, tmp_path):
        """Empty .dvc file returns empty dict."""
        dvc_file = tmp_path / 'empty.dvc'
        dvc_file.write_text('')
        
        result = utils.parse_dvc_file(dvc_file)
        assert result == {}
    
    def test_invalid_yaml_raises_error(self, tmp_path):
        """Invalid YAML raises DVCFileError."""
        dvc_file = tmp_path / 'invalid.dvc'
        dvc_file.write_text('outs:\n  - [invalid yaml structure')
        
        with pytest.raises(DVCFileError):
            utils.parse_dvc_file(dvc_file)
    
    def test_nonexistent_file_raises_error(self, tmp_path):
        """Nonexistent file raises DVCFileError."""
        dvc_file = tmp_path / 'nonexistent.dvc'
        
        with pytest.raises(DVCFileError):
            utils.parse_dvc_file(dvc_file)
    
    def test_import_dvc_file(self, tmp_path):
        """Import .dvc file parsed correctly."""
        dvc_file = tmp_path / 'imported.csv.dvc'
        dvc_file.write_text(
            'deps:\n'
            '  - path: source/data.csv\n'
            '    repo:\n'
            '      url: https://github.com/example/repo\n'
            '      rev: v1.0\n'
            'outs:\n'
            '  - md5: abc123\n'
            '    path: imported.csv\n'
        )
        
        result = utils.parse_dvc_file(dvc_file)
        
        assert 'deps' in result
        assert 'outs' in result
        assert result['deps'][0]['repo']['url'] == 'https://github.com/example/repo'


# =============================================================================
# Project Utilities
# =============================================================================

class TestGetProjectName:
    """Tests for get_project_name function."""
    
    def test_returns_current_directory_name(self, tmp_path, monkeypatch):
        """Returns the name of the current directory."""
        test_dir = tmp_path / 'my_project'
        test_dir.mkdir()
        monkeypatch.chdir(test_dir)
        
        result = utils.get_project_name()
        assert result == 'my_project'


class TestCheckCommand:
    """Tests for check_command function."""
    
    def test_existing_command_passes(self):
        """Existing command (like 'ls' or 'python') passes."""
        # 'python' should exist since we're running tests
        utils.check_command('python')  # Should not raise
    
    def test_missing_command_raises(self):
        """Missing command raises DependencyError."""
        with pytest.raises(DependencyError) as exc_info:
            utils.check_command('nonexistent_command_xyz')
        assert 'not found' in str(exc_info.value)
    
    def test_install_hint_in_error(self):
        """Install hint included in error message."""
        with pytest.raises(DependencyError) as exc_info:
            utils.check_command('nonexistent_cmd', install_hint='pip install something')
        assert 'pip install something' in str(exc_info.value)


class TestCheckDvc:
    """Tests for check_dvc function."""
    
    @pytest.mark.skipif(not shutil.which('dvc'), reason="DVC not installed")
    def test_passes_when_dvc_installed(self):
        """Passes when DVC is installed."""
        utils.check_dvc()  # Should not raise
    
    def test_raises_when_dvc_missing(self):
        """Raises DependencyError when DVC not found."""
        with patch.object(shutil, 'which', return_value=None):
            with pytest.raises(DependencyError) as exc_info:
                utils.check_dvc()
            assert 'dvc' in str(exc_info.value).lower()


class TestCheckGit:
    """Tests for check_git function."""
    
    @pytest.mark.skipif(not shutil.which('git'), reason="git not installed")
    def test_passes_when_git_installed(self):
        """Passes when git is installed."""
        utils.check_git()  # Should not raise
    
    def test_raises_when_git_missing(self):
        """Raises DependencyError when git not found."""
        with patch.object(shutil, 'which', return_value=None):
            with pytest.raises(DependencyError):
                utils.check_git()


class TestUpdateGitignore:
    """Tests for update_gitignore function."""
    
    def test_adds_new_pattern(self, tmp_path, monkeypatch):
        """New pattern added to .gitignore."""
        monkeypatch.chdir(tmp_path)
        
        result = utils.update_gitignore('/data.csv')
        
        assert result is True
        gitignore = (tmp_path / '.gitignore').read_text()
        assert '/data.csv' in gitignore
    
    def test_returns_false_for_existing_pattern(self, tmp_path, monkeypatch):
        """Returns False when pattern already exists."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').write_text('/data.csv\n')
        
        result = utils.update_gitignore('/data.csv')
        
        assert result is False
    
    def test_creates_gitignore_if_missing(self, tmp_path, monkeypatch):
        """Creates .gitignore if it doesn't exist."""
        monkeypatch.chdir(tmp_path)
        assert not (tmp_path / '.gitignore').exists()
        
        utils.update_gitignore('.dt/tmp/')
        
        assert (tmp_path / '.gitignore').exists()
    
    def test_appends_to_existing_gitignore(self, tmp_path, monkeypatch):
        """Appends pattern to existing .gitignore."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').write_text('*.pyc\n')
        
        utils.update_gitignore('/data.csv')
        
        gitignore = (tmp_path / '.gitignore').read_text()
        assert '*.pyc' in gitignore
        assert '/data.csv' in gitignore
    
    def test_custom_gitignore_path(self, tmp_path):
        """Uses custom .gitignore path when provided."""
        custom_path = tmp_path / 'subdir' / '.gitignore'
        custom_path.parent.mkdir()
        
        utils.update_gitignore('/pattern', gitignore_path=custom_path)
        
        assert custom_path.exists()
        assert '/pattern' in custom_path.read_text()


class TestSetGroupWritable:
    """Tests for set_group_writable function."""
    
    def test_sets_permissions(self, tmp_path):
        """Sets group write permissions."""
        test_dir = tmp_path / 'test_dir'
        test_dir.mkdir()
        
        utils.set_group_writable(test_dir)
        
        # Check group write permission is set
        mode = test_dir.stat().st_mode
        assert mode & 0o020  # Group write bit
    
    def test_sets_setgid_by_default(self, tmp_path):
        """Sets setgid bit by default."""
        test_dir = tmp_path / 'test_dir'
        test_dir.mkdir()
        
        utils.set_group_writable(test_dir, setgid=True)
        
        mode = test_dir.stat().st_mode
        assert mode & 0o2000  # Setgid bit


class TestCreateMd5Subdirs:
    """Tests for create_md5_subdirs function."""
    
    def test_creates_256_subdirectories(self, tmp_path):
        """Creates 256 subdirectories (00-ff)."""
        utils.create_md5_subdirs(tmp_path, verbose=False)
        
        files_md5 = tmp_path / 'files' / 'md5'
        assert files_md5.exists()
        
        subdirs = list(files_md5.iterdir())
        assert len(subdirs) == 256
        
        # Check names are hex values
        names = sorted([d.name for d in subdirs])
        expected = [f'{i:02x}' for i in range(256)]
        assert names == expected


# =============================================================================
# Project Root Discovery
# =============================================================================

class TestFindDvcRoot:
    """Tests for find_dvc_root function."""
    
    def test_returns_none_outside_dvc_project(self, tmp_path, monkeypatch):
        """Returns None when not in a DVC project."""
        monkeypatch.chdir(tmp_path)
        result = utils.find_dvc_root()
        assert result is None
    
    @pytest.mark.skipif(not shutil.which('dvc'), reason="DVC not installed")
    def test_finds_root_in_dvc_project(self, tmp_path, monkeypatch):
        """Finds root when in a DVC project."""
        # Initialize git and DVC
        monkeypatch.chdir(tmp_path)
        subprocess.run(['git', 'init'], capture_output=True)
        subprocess.run(['dvc', 'init'], capture_output=True)
        
        result = utils.find_dvc_root()
        assert result == tmp_path


class TestFindGitRoot:
    """Tests for find_git_root function."""

    def test_returns_none_outside_git_repo(self, tmp_path, monkeypatch):
        """Returns None when not in a git repository."""
        monkeypatch.chdir(tmp_path)
        result = utils.find_git_root()
        assert result is None

    def test_finds_root_in_git_repo(self, tmp_path, monkeypatch):
        """Finds the root of a plain git repository -- no DVC required.

        This used to go through ``dvc.repo.Repo``, which needs a ``.dvc``
        directory, so it returned None here. The old assertion accepted
        ``None or tmp_path`` and therefore never noticed.
        """
        (tmp_path / '.git').mkdir()
        monkeypatch.chdir(tmp_path)

        assert utils.find_git_root() == tmp_path.resolve()

    def test_finds_root_from_subdirectory(self, tmp_path, monkeypatch):
        """Walks up from a nested working directory."""
        (tmp_path / '.git').mkdir()
        nested = tmp_path / 'a' / 'b' / 'c'
        nested.mkdir(parents=True)
        monkeypatch.chdir(nested)

        assert utils.find_git_root() == tmp_path.resolve()

    def test_accepts_git_file_for_worktrees(self, tmp_path, monkeypatch):
        """A worktree or submodule has .git as a file, not a directory."""
        (tmp_path / '.git').write_text('gitdir: /elsewhere/.git/worktrees/wt\n')
        monkeypatch.chdir(tmp_path)

        assert utils.find_git_root() == tmp_path.resolve()

    def test_honours_start_argument(self, tmp_path):
        """Searches from `start` rather than the working directory."""
        repo = tmp_path / 'repo'
        (repo / 'sub').mkdir(parents=True)
        (repo / '.git').mkdir()

        assert utils.find_git_root(start=repo / 'sub') == repo.resolve()

    def test_does_not_open_a_dvc_repo(self, tmp_path, monkeypatch):
        """Locating the git root must not construct a dvc.repo.Repo.

        Doing so opened the repo's state database, which on shared network
        storage cost hundreds of milliseconds on a path that every single
        config lookup goes through.
        """
        (tmp_path / '.git').mkdir()
        monkeypatch.chdir(tmp_path)

        with patch.object(utils, 'Repo') as mock_repo:
            assert utils.find_git_root() == tmp_path.resolve()

        mock_repo.assert_not_called()


class TestFindProjectRoot:
    """Tests for find_project_root function."""

    def test_returns_cwd_as_fallback(self, tmp_path, monkeypatch):
        """Returns cwd when no git/DVC root found."""
        monkeypatch.chdir(tmp_path)

        result = utils.find_project_root()

        assert result == tmp_path

    def test_uses_start_path(self, tmp_path):
        """Uses provided start path."""
        result = utils.find_project_root(start=tmp_path)
        assert result == tmp_path

    def test_prefers_git_root_over_dvc_root(self, tmp_path, monkeypatch):
        """Documented order: git root first, then DVC root, then cwd."""
        (tmp_path / '.git').mkdir()
        nested = tmp_path / 'analysis'
        nested.mkdir()
        (nested / '.dvc').mkdir()
        monkeypatch.chdir(nested)

        assert utils.find_project_root() == tmp_path.resolve()

    def test_falls_back_to_dvc_root_without_git(self, tmp_path, monkeypatch):
        """A DVC project outside git still resolves to the DVC root."""
        (tmp_path / '.dvc').mkdir()
        nested = tmp_path / 'analysis'
        nested.mkdir()
        monkeypatch.chdir(nested)

        assert utils.find_project_root() == tmp_path


# =============================================================================
# Git Revision Utilities
# =============================================================================

class TestGetCandidateCommits:
    """Tests for get_candidate_commits function."""
    
    @pytest.mark.skipif(not shutil.which('git'), reason="git not installed")
    def test_returns_list_in_git_repo(self, tmp_path, monkeypatch):
        """Returns list of commits (possibly empty) in git repo."""
        monkeypatch.chdir(tmp_path)
        subprocess.run(['git', 'init'], capture_output=True)
        subprocess.run(['git', 'config', 'user.email', 'test@test.com'], capture_output=True)
        subprocess.run(['git', 'config', 'user.name', 'Test'], capture_output=True)
        
        # Create a .dvc file and commit
        (tmp_path / 'data.csv.dvc').write_text('outs:\n  - path: data.csv\n')
        subprocess.run(['git', 'add', '.'], capture_output=True)
        subprocess.run(['git', 'commit', '-m', 'Add data'], capture_output=True)
        
        result = utils.get_candidate_commits()
        
        assert isinstance(result, list)
        assert len(result) >= 1
    
    def test_returns_empty_outside_git(self, tmp_path, monkeypatch):
        """Returns empty list outside git repository."""
        monkeypatch.chdir(tmp_path)
        result = utils.get_candidate_commits()
        assert result == []
    
    @pytest.mark.skipif(not shutil.which('git'), reason="git not installed")
    def test_limit_parameter(self, tmp_path, monkeypatch):
        """Limit parameter restricts number of commits."""
        monkeypatch.chdir(tmp_path)
        subprocess.run(['git', 'init'], capture_output=True)
        subprocess.run(['git', 'config', 'user.email', 'test@test.com'], capture_output=True)
        subprocess.run(['git', 'config', 'user.name', 'Test'], capture_output=True)
        
        # Create multiple commits
        for i in range(5):
            (tmp_path / f'data{i}.dvc').write_text(f'outs:\n  - path: data{i}.csv\n')
            subprocess.run(['git', 'add', '.'], capture_output=True)
            subprocess.run(['git', 'commit', '-m', f'Add data{i}'], capture_output=True)
        
        result = utils.get_candidate_commits(limit=2)
        
        assert len(result) <= 2


class TestGetCommitInfo:
    """Tests for get_commit_info function."""
    
    @pytest.mark.skipif(not shutil.which('git'), reason="git not installed")
    def test_returns_commit_info(self, tmp_path, monkeypatch):
        """Returns dict with commit metadata."""
        monkeypatch.chdir(tmp_path)
        subprocess.run(['git', 'init'], capture_output=True)
        subprocess.run(['git', 'config', 'user.email', 'test@test.com'], capture_output=True)
        subprocess.run(['git', 'config', 'user.name', 'Test Author'], capture_output=True)
        
        (tmp_path / 'test.txt').write_text('test')
        subprocess.run(['git', 'add', '.'], capture_output=True)
        subprocess.run(['git', 'commit', '-m', 'Test commit message'], capture_output=True)
        
        # Get the commit hash
        result = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
        commit_hash = result.stdout.strip()
        
        info = utils.get_commit_info(commit_hash)
        
        assert 'hash' in info
        assert 'short_hash' in info
        assert 'date' in info
        assert 'message' in info
        assert 'author' in info
        assert info['message'] == 'Test commit message'
        assert info['author'] == 'Test Author'
    
    def test_invalid_commit_returns_partial_info(self, tmp_path, monkeypatch):
        """Invalid commit returns dict with partial info."""
        monkeypatch.chdir(tmp_path)
        subprocess.run(['git', 'init'], capture_output=True)
        
        info = utils.get_commit_info('invalidhash123')
        
        assert 'hash' in info
        assert info['hash'] == 'invalidhash123'


# =============================================================================
# ensure_dt_gitignore tests
# =============================================================================

class TestEnsureDtGitignore:
    """Tests for the ensure_dt_gitignore function."""

    def test_creates_gitignore_from_scratch(self, tmp_path):
        """Creates .dt/.gitignore with all required entries."""
        result = utils.ensure_dt_gitignore(tmp_path)
        assert result == tmp_path / '.dt' / '.gitignore'
        content = result.read_text()
        for entry in utils._DT_GITIGNORE_ENTRIES:
            assert entry in content

    def test_preserves_existing_entries(self, tmp_path):
        """Existing entries in .dt/.gitignore are preserved."""
        dt_dir = tmp_path / '.dt'
        dt_dir.mkdir()
        gitignore = dt_dir / '.gitignore'
        gitignore.write_text('/my-custom-junk\n')

        utils.ensure_dt_gitignore(tmp_path)

        content = gitignore.read_text()
        assert '/my-custom-junk' in content
        for entry in utils._DT_GITIGNORE_ENTRIES:
            assert entry in content

    def test_no_duplicates_on_rerun(self, tmp_path):
        """Running twice does not duplicate entries."""
        utils.ensure_dt_gitignore(tmp_path)
        utils.ensure_dt_gitignore(tmp_path)

        content = (tmp_path / '.dt' / '.gitignore').read_text()
        for entry in utils._DT_GITIGNORE_ENTRIES:
            assert content.count(entry) == 1

    def test_adds_missing_entry_to_existing(self, tmp_path):
        """Adds only the missing entry when some already exist."""
        dt_dir = tmp_path / '.dt'
        dt_dir.mkdir()
        gitignore = dt_dir / '.gitignore'
        gitignore.write_text('/config.local.yaml\n/tmp/\n')

        utils.ensure_dt_gitignore(tmp_path)

        content = gitignore.read_text()
        assert '/hook-results/' in content
        assert content.count('/config.local.yaml') == 1

    def test_defaults_to_project_root(self, tmp_path, monkeypatch):
        """Uses find_project_root when no path is given."""
        monkeypatch.chdir(tmp_path)
        with patch.object(utils, 'find_project_root', return_value=tmp_path):
            result = utils.ensure_dt_gitignore()
        assert result.exists()


class TestParseRemoteRef:
    """Tests for parse_remote_ref."""

    def _remotes(self, *names):
        return MagicMock(returncode=0, stdout="\n".join(names) + "\n", stderr="")

    def test_returns_none_for_non_slash_refs(self):
        with patch("subprocess.run", return_value=self._remotes("origin")):
            assert utils.parse_remote_ref("HEAD") is None
            assert utils.parse_remote_ref("main") is None
            assert utils.parse_remote_ref("v1.0") is None

    def test_returns_none_for_empty(self):
        assert utils.parse_remote_ref("") is None

    def test_parses_remote_branch(self):
        with patch("subprocess.run", return_value=self._remotes("origin")):
            assert utils.parse_remote_ref("origin/main") == ("origin", "main")

    def test_returns_none_for_unknown_remote(self):
        with patch("subprocess.run", return_value=self._remotes("origin")):
            # 'upstream' isn't a configured remote
            assert utils.parse_remote_ref("upstream/main") is None

    def test_handles_branch_with_slashes(self):
        with patch("subprocess.run", return_value=self._remotes("origin")):
            assert utils.parse_remote_ref("origin/feat/x") == ("origin", "feat/x")

    def test_prefers_longest_remote_match(self):
        # Both 'org' and 'org/sub' exist as remotes; the longer wins.
        with patch("subprocess.run", return_value=self._remotes("org", "org/sub")):
            assert utils.parse_remote_ref("org/sub/main") == ("org/sub", "main")


class TestCheckRemoteRefFreshness:
    """Tests for check_remote_ref_freshness."""

    def test_returns_none_for_non_remote_ref(self):
        with patch.object(utils, "parse_remote_ref", return_value=None):
            assert utils.check_remote_ref_freshness("HEAD") is None

    def test_fresh_when_local_matches_remote(self):
        calls = [
            MagicMock(returncode=0, stdout="abc123\trefs/heads/main\n", stderr=""),
            MagicMock(returncode=0, stdout="abc123\n", stderr=""),
        ]
        with patch.object(utils, "parse_remote_ref", return_value=("origin", "main")):
            with patch("subprocess.run", side_effect=calls):
                info = utils.check_remote_ref_freshness("origin/main")
        assert info["stale"] is False
        assert info["remote"] == "origin"
        assert info["branch"] == "main"

    def test_stale_when_local_differs(self):
        calls = [
            MagicMock(returncode=0, stdout="newsha\trefs/heads/main\n", stderr=""),
            MagicMock(returncode=0, stdout="oldsha\n", stderr=""),
        ]
        with patch.object(utils, "parse_remote_ref", return_value=("origin", "main")):
            with patch("subprocess.run", side_effect=calls):
                info = utils.check_remote_ref_freshness("origin/main")
        assert info["stale"] is True
        assert info["local"] == "oldsha"
        assert info["remote_sha"] == "newsha"

    def test_stale_when_ref_never_fetched(self):
        calls = [
            MagicMock(returncode=0, stdout="newsha\trefs/heads/main\n", stderr=""),
            MagicMock(returncode=1, stdout="", stderr=""),  # rev-parse fails
        ]
        with patch.object(utils, "parse_remote_ref", return_value=("origin", "main")):
            with patch("subprocess.run", side_effect=calls):
                info = utils.check_remote_ref_freshness("origin/main")
        assert info["stale"] is True
        assert info["local"] is None

    def test_returns_none_when_remote_unreachable(self):
        # ls-remote fails (offline / unreachable) -> can't determine freshness
        calls = [MagicMock(returncode=128, stdout="", stderr="fatal")]
        with patch.object(utils, "parse_remote_ref", return_value=("origin", "main")):
            with patch("subprocess.run", side_effect=calls):
                assert utils.check_remote_ref_freshness("origin/main") is None


# Run with: pytest tests/test_utils.py -v


class TestCacheRootFromOdbPath:
    """Tests for cache_root_from_odb_path / get_cache_root.

    ``get_cache_dir()`` returns DVC's odb path, which under the v3 layout
    ends in ``files/md5``. Anything handing a location back to DVC needs the
    root above that, or the layout gets appended twice.
    """

    def test_strips_files_md5_suffix(self):
        assert utils.cache_root_from_odb_path(
            Path('/g/data/ab12/dvc/cache/files/md5')
        ) == Path('/g/data/ab12/dvc/cache')

    def test_leaves_bare_root_alone(self):
        assert utils.cache_root_from_odb_path(
            Path('/g/data/ab12/dvc/cache')
        ) == Path('/g/data/ab12/dvc/cache')

    def test_does_not_strip_partial_match(self):
        """`md5` without a `files` parent is a real directory name."""
        assert utils.cache_root_from_odb_path(
            Path('/some/where/md5')
        ) == Path('/some/where/md5')

    def test_accepts_str(self):
        assert utils.cache_root_from_odb_path(
            '/cache/files/md5'
        ) == Path('/cache')

    def test_get_cache_root_strips(self):
        with patch.object(utils, 'get_cache_dir',
                          return_value=Path('/cache/files/md5')):
            assert utils.get_cache_root() == Path('/cache')

    def test_get_cache_root_none_outside_repo(self):
        with patch.object(utils, 'get_cache_dir', return_value=None):
            assert utils.get_cache_root() is None


# =============================================================================
# .dvcignore seeding and mixed-tree detection (#182)
# =============================================================================

class TestEnsureDvcignore:
    """DVC's generated .gitignore files must not become other repos' data.

    `dvc import <repo> <dir>`, where dir is not itself an out, hashes the
    git-tracked files it finds -- including the .gitignore DVC wrote there --
    and folds them into the importer's payload, nfiles and size. One
    .dvcignore pattern in the source repo prevents that.
    """

    def test_creates_with_explanation(self, tmp_path):
        path, changed = utils.ensure_dvcignore(tmp_path)

        assert changed is True
        assert path == tmp_path / '.dvcignore'
        text = path.read_text()
        assert '.gitignore' in text.splitlines()
        assert text.startswith('#'), "the pattern needs its rationale beside it"

    def test_is_idempotent(self, tmp_path):
        utils.ensure_dvcignore(tmp_path)
        before = (tmp_path / '.dvcignore').read_text()

        path, changed = utils.ensure_dvcignore(tmp_path)

        assert changed is False
        assert path.read_text() == before

    def test_appends_without_clobbering(self, tmp_path):
        existing = "raw/scratch\n*.tmp\n"
        (tmp_path / '.dvcignore').write_text(existing)

        _, changed = utils.ensure_dvcignore(tmp_path)

        text = (tmp_path / '.dvcignore').read_text()
        assert changed is True
        assert text.startswith(existing), "existing patterns must survive"
        assert '.gitignore' in text.splitlines()

    def test_tolerates_missing_trailing_newline(self, tmp_path):
        (tmp_path / '.dvcignore').write_text("raw/scratch")

        utils.ensure_dvcignore(tmp_path)

        lines = (tmp_path / '.dvcignore').read_text().splitlines()
        assert 'raw/scratch' in lines
        assert '.gitignore' in lines


class TestIsDvcMetadataFile:
    """Mirrors dvc.fs.dvc._is_dvc_file -- note .gitignore is NOT on it."""

    @pytest.mark.parametrize('path', [
        'data.dvc', 'a/b/data.dvc', 'dvc.yaml', 'a/dvc.lock', '.dvcignore',
    ])
    def test_metadata(self, path):
        assert utils.is_dvc_metadata_file(path) is True

    @pytest.mark.parametrize('path', [
        '.gitignore', 'a/b/.gitignore', 'README.md', 'data/x.fq', 'dvc.yamlx',
    ])
    def test_not_metadata(self, path):
        assert utils.is_dvc_metadata_file(path) is False


class TestUnhashedListingPaths:
    """Which entries in a `dvc list` result make a faithful manifest impossible."""

    def test_flags_git_tracked_files(self):
        files = [
            {'path': '.gitignore', 'isout': False, 'md5': None},
            {'path': 'README.md', 'isout': False, 'md5': None},
            {'path': 'in_house/a.txt', 'isout': True, 'md5': 'a' * 32},
        ]
        assert utils.unhashed_listing_paths(files) == ['.gitignore', 'README.md']

    def test_ignores_dvc_files_and_dirs(self):
        """A directory of outs plus their .dvc files is still rebuildable."""
        files = [
            {'path': 'in_house.dvc', 'isout': False, 'md5': None},
            {'path': 'imerit.dvc', 'isout': False, 'md5': None},
            {'path': 'sub', 'isdir': True, 'md5': None},
            {'path': 'in_house/a.txt', 'isout': True, 'md5': 'a' * 32},
        ]
        assert utils.unhashed_listing_paths(files) == []

    def test_empty_for_a_clean_out(self):
        files = [{'path': 'a.txt', 'isout': True, 'md5': 'a' * 32}]
        assert utils.unhashed_listing_paths(files) == []


class TestDescribeMixedTree:
    """The refusal has to say what to do about it."""

    def test_names_the_files_and_the_dvcignore_remedy(self):
        msg = utils.describe_mixed_tree(
            ['.gitignore'], 'data/annotations',
            'git@github.com:swarbricklab/image-repo.git', action='rebuild',
        )
        assert 'data/annotations' in msg
        assert '.gitignore' in msg
        assert '.dvcignore' in msg, "for a bare .gitignore, name the real fix"
        assert 'dvc update' in msg

    def test_omits_dvcignore_advice_for_other_files(self):
        msg = utils.describe_mixed_tree(
            ['README.md'], 'data/annotations', 'repo', action='import',
        )
        assert 'README.md' in msg
        assert '.dvcignore' not in msg, "adding it would not help here"

    def test_summarises_a_long_list(self):
        msg = utils.describe_mixed_tree(
            [f'f{i}.txt' for i in range(25)], 'd', 'repo', action='import',
        )
        assert '... and 15 more' in msg


class TestCollectTrackedEntriesBadTarget:
    """A target DVC cannot resolve is an error message, not a traceback."""

    def test_dvc_collection_error_becomes_target_not_found(self, tmp_path, monkeypatch):
        """DVC's NoOutputOrStageError is converted, naming the target."""
        from dvc.exceptions import NoOutputOrStageError

        monkeypatch.chdir(tmp_path)
        with patch.object(utils, 'Repo'):
            with patch(
                'dvc.repo.fetch._collect_indexes',
                side_effect=NoOutputOrStageError('myremote', 'dvc.yaml'),
                create=True,
            ):
                with pytest.raises(TargetNotFoundError) as exc_info:
                    utils.collect_tracked_entries(targets=['myremote'])

        assert exc_info.value.targets == ['myremote']
        assert "'myremote'" in str(exc_info.value)

    def test_names_only_the_unresolvable_target(self, tmp_path, monkeypatch):
        """A target that does exist is not blamed alongside the bad one."""
        from dvc.exceptions import OutputNotFoundError

        monkeypatch.chdir(tmp_path)
        (tmp_path / 'data.txt').write_text('x')
        with patch.object(utils, 'Repo'):
            with patch(
                'dvc.repo.fetch._collect_indexes',
                side_effect=OutputNotFoundError('typo.txt'),
                create=True,
            ):
                with pytest.raises(TargetNotFoundError) as exc_info:
                    utils.collect_tracked_entries(targets=['data.txt', 'typo.txt'])

        assert exc_info.value.targets == ['typo.txt']


class TestUnresolvableTargets:
    """Which targets cannot possibly name tracked data."""

    def test_existing_path_is_resolvable(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'data.txt').write_text('x')
        assert utils.unresolvable_targets(['data.txt']) == []

    def test_dvc_file_makes_a_missing_path_resolvable(self, tmp_path, monkeypatch):
        """A not-checked-out out is still a target: its .dvc file is there."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'data.txt.dvc').write_text('outs: []\n')
        assert utils.unresolvable_targets(['data.txt']) == []

    def test_unknown_name_is_unresolvable(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert utils.unresolvable_targets(['myremote']) == ['myremote']

    def test_stage_name_is_resolvable(self, tmp_path, monkeypatch):
        """A stage in dvc.yaml is a target even though it is not a path."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'dvc.yaml').write_text('stages:\n  train:\n    cmd: echo hi\n')
        assert utils.unresolvable_targets(['train']) == []

    def test_stage_named_by_file_is_resolvable(self, tmp_path, monkeypatch):
        """`dvc.yaml:stage` resolves via the file it names."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'dvc.yaml').write_text('stages:\n  train:\n    cmd: echo hi\n')
        assert utils.unresolvable_targets(['dvc.yaml:train']) == []

    def test_no_targets(self):
        assert utils.unresolvable_targets(None) == []


class TestUnresolvableTargetsInsideTrackedDir:
    """A path under a tracked directory resolves via the directory's .dvc."""

    def test_file_in_tracked_dir_not_checked_out(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'dir.dvc').write_text('outs: []\n')
        assert utils.unresolvable_targets(['dir/b.txt']) == []

    def test_file_under_untracked_dir_is_unresolvable(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert utils.unresolvable_targets(['dir/b.txt']) == ['dir/b.txt']


class TestRevAndPathValidation:
    @pytest.mark.parametrize('rev', [
        'deadbeef', 'a' * 40, 'main', 'feature/x-y_z', 'v1.2.3', 'HEAD~1',
        'v1^{commit}', 'release@2024', 'fix#123', 'release=1',
    ])
    def test_accepts_normal_revs(self, rev):
        assert utils.is_valid_git_rev(rev)

    @pytest.mark.parametrize('rev', [
        None, '', '-x', '--upload-pack=x', 'a b', 'a:b', 'a..b', 'a\nb', 'abc\n', 'a' * 256,
    ])
    def test_rejects_malformed_revs(self, rev):
        assert not utils.is_valid_git_rev(rev)

    @pytest.mark.parametrize('path,ok', [
        ('data/x.csv', True), ('data', True), ('', False), ('-r', False),
        ('--all', False), ('a\nb', False), (None, False),
    ])
    def test_repo_path(self, path, ok):
        assert utils.is_valid_repo_path(path) is ok


class TestDvcLocalConfigProblem:
    def _repo(self, tmp_path):
        repo = tmp_path / 'repo'
        (repo / '.dvc').mkdir(parents=True)
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        return repo

    def test_clean_repo_is_fine(self, tmp_path):
        repo = self._repo(tmp_path)
        assert utils.dvc_local_config_problem(repo) is None
        (repo / '.dvc' / 'config.local').write_text('[core]\n')
        (repo / '.dvc' / 'tmp').mkdir()
        assert utils.dvc_local_config_problem(repo) is None

    def test_not_a_dvc_repo_is_left_to_dvc(self, tmp_path):
        assert utils.dvc_local_config_problem(tmp_path) is None

    def test_symlinked_config_local(self, tmp_path):
        repo = self._repo(tmp_path)
        (repo / '.dvc' / 'config.local').symlink_to(tmp_path / 'elsewhere')
        assert 'not a regular file' in utils.dvc_local_config_problem(repo)

    def test_hard_linked_config_local(self, tmp_path):
        repo = self._repo(tmp_path)
        other = tmp_path / 'other'
        other.write_text('')
        os.link(other, repo / '.dvc' / 'config.local')
        assert 'hard links' in utils.dvc_local_config_problem(repo)

    def test_symlinked_dvc_dir(self, tmp_path):
        repo = tmp_path / 'repo'
        repo.mkdir()
        (repo / '.dvc').symlink_to(tmp_path)
        assert 'not a directory' in utils.dvc_local_config_problem(repo)

    def test_symlinked_tmp_dir_and_links_inside(self, tmp_path):
        repo = self._repo(tmp_path)
        (repo / '.dvc' / 'tmp').symlink_to(tmp_path)
        assert 'not a directory' in utils.dvc_local_config_problem(repo)
        (repo / '.dvc' / 'tmp').unlink()
        (repo / '.dvc' / 'tmp').mkdir()
        (repo / '.dvc' / 'tmp' / 'lock').symlink_to(tmp_path / 'x')
        assert 'is a link' in utils.dvc_local_config_problem(repo)

    def test_tracked_config_local(self, tmp_path):
        repo = self._repo(tmp_path)
        (repo / '.dvc' / 'config.local').write_text('[core]\n')
        subprocess.run(['git', '-C', str(repo), 'add', '-f', '.dvc/config.local'], check=True)
        assert 'tracks DVC local state' in utils.dvc_local_config_problem(repo)

    def test_found_from_a_subdirectory(self, tmp_path, monkeypatch):
        repo = self._repo(tmp_path)
        (repo / '.dvc' / 'config.local').symlink_to(tmp_path / 'x')
        (repo / 'sub').mkdir()
        monkeypatch.chdir(repo / 'sub')
        assert 'not a regular file' in utils.dvc_local_config_problem()

    def test_symlinked_project_config(self, tmp_path):
        repo = self._repo(tmp_path)
        (repo / '.dvc' / 'config').symlink_to(tmp_path / 'x')
        assert 'not a regular file' in utils.dvc_local_config_problem(repo)

    def test_git_failure_fails_closed(self, tmp_path):
        repo = tmp_path / 'repo'
        (repo / '.dvc').mkdir(parents=True)
        (repo / '.git').mkdir()  # not a valid git dir
        assert 'could not check' in utils.dvc_local_config_problem(repo)

    def test_nested_links_under_tmp_are_not_flagged(self, tmp_path):
        repo = self._repo(tmp_path)
        exp = repo / '.dvc' / 'tmp' / 'exps' / 'tmpabc'
        exp.mkdir(parents=True)
        (exp / 'ref').symlink_to(tmp_path)
        assert utils.dvc_local_config_problem(repo) is None

    def test_inherited_git_env_is_ignored(self, tmp_path, monkeypatch):
        repo = self._repo(tmp_path)
        (repo / '.dvc' / 'config.local').write_text('')
        subprocess.run(['git', '-C', str(repo), 'add', '-f', '.dvc/config.local'], check=True)
        other = tmp_path / 'other'
        subprocess.run(['git', 'init', '-q', str(other)], check=True)
        monkeypatch.setenv('GIT_DIR', str(other / '.git'))
        assert 'tracks DVC local state' in utils.dvc_local_config_problem(repo)

    def test_ensure_dt_gitignore_refuses_symlinked_dt(self, tmp_path):
        from dt.errors import DTError
        repo = tmp_path / 'repo'
        repo.mkdir()
        outside = tmp_path / 'outside'
        outside.mkdir()
        (repo / '.dt').symlink_to(outside)
        with pytest.raises(DTError, match='Refusing'):
            utils.ensure_dt_gitignore(repo)
        assert list(outside.iterdir()) == []

    def test_tracked_in_subdir_dvc_project(self, tmp_path):
        outer = tmp_path / 'mono'
        sub = outer / 'proj'
        (sub / '.dvc').mkdir(parents=True)
        subprocess.run(['git', 'init', '-q', str(outer)], check=True)
        (sub / '.dvc' / 'config.local').write_text('')
        subprocess.run(['git', '-C', str(outer), 'add', '-f', 'proj/.dvc/config.local'], check=True)
        assert 'tracks DVC local state' in utils.dvc_local_config_problem(sub)

    def test_no_scm_repo_is_fine(self, tmp_path):
        repo = tmp_path / 'noscm'
        (repo / '.dvc').mkdir(parents=True)
        assert utils.dvc_local_config_problem(repo) is None

    def test_committed_symlink_never_written_through(self, tmp_path):
        """End to end: a source clone's committed config.local link."""
        from dt import import_data
        from dt.errors import ImportError_
        target = tmp_path / 'victim-config.local'
        target.write_text('[core]\n    remote = mine\n')
        clone = self._repo(tmp_path)
        (clone / '.dvc' / 'config.local').symlink_to(target)
        subprocess.run(['git', '-C', str(clone), 'add', '-f', '.dvc/config.local'], check=True)
        with pytest.raises(ImportError_, match='Refusing'):
            import_data.configure_clone_cache(clone, str(tmp_path / 'attacker-cache'))
        assert target.read_text() == '[core]\n    remote = mine\n'


class TestObjectIdPaths:
    @pytest.mark.parametrize('oid', ['0' * 32, 'a' * 32 + '.dir', '0123456789abcdef' * 2])
    def test_valid_md5_ids(self, oid):
        assert utils.is_md5_oid(oid)
        prefix, rest = utils.oid_path_parts(oid)
        assert prefix == oid[:2] and '/' not in rest

    @pytest.mark.parametrize('oid', [
        'ab/../../../../etc/x.dir', 'a/' + 'b' * 30 + '.dir', '..' + 'a' * 30,
        'A' * 32, 'g' * 32, 'a' * 31, 'a' * 33, 'a' * 32 + '.dir\n', '', None,
    ])
    def test_invalid_md5_ids_rejected(self, oid):
        assert not utils.is_md5_oid(oid)
        with pytest.raises(ValueError):
            utils.oid_path_parts(oid)

    @pytest.mark.parametrize('oid', ['ab/c', '..cdef', 'a', 'ab..', 'x\0y'])
    def test_non_strict_still_blocks_escape(self, oid, tmp_path):
        with pytest.raises(ValueError):
            utils.hash_to_cache_path(tmp_path, oid)

    def test_non_strict_allows_other_hash_kinds(self, tmp_path):
        assert utils.hash_to_cache_path(tmp_path, 'abc123etag-5') == tmp_path / 'ab' / 'c123etag-5'


class TestDtDirLinksAreRefused:
    def _repo(self, tmp_path):
        repo = tmp_path / 'repo'
        repo.mkdir()
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        return repo

    def test_hook_results_link_is_not_written_or_cleared(self, tmp_path, monkeypatch):
        from dt import install
        from dt.errors import DTError
        repo = self._repo(tmp_path)
        outside = tmp_path / 'outside'
        outside.mkdir()
        (outside / 'keep.json').write_text('{}')
        (repo / '.dt').mkdir()
        (repo / '.dt' / install.HOOK_RESULTS_DIR_NAME).symlink_to(outside)
        monkeypatch.chdir(repo)
        with pytest.raises(DTError, match='real directory'):
            install._save_hook_result('c', 'pre-commit', True, 'out')
        assert install.clear_hook_results() == 0
        assert install.count_unread_results() == 0
        assert sorted(p.name for p in outside.iterdir()) == ['keep.json']

    def test_dvcignore_link_is_left_unchanged(self, tmp_path):
        from dt.errors import DTError
        repo = self._repo(tmp_path)
        target = tmp_path / 'victim.cfg'
        target.write_text('strict=1\n')
        (repo / '.dvcignore').symlink_to(target)
        with pytest.raises(DTError, match='regular file'):
            utils.ensure_dvcignore(repo)
        assert target.read_text() == 'strict=1\n'

    def test_dt_subdir_refuses_links_at_any_level(self, tmp_path):
        from dt.errors import DTError
        repo = self._repo(tmp_path)
        outside = tmp_path / 'outside'
        outside.mkdir()
        (repo / '.dt').mkdir()
        (repo / '.dt' / 'tmp').symlink_to(outside)
        with pytest.raises(DTError):
            utils.dt_subdir('tmp', 'push', root=repo)
        assert list(outside.iterdir()) == []

    def test_local_config_write_refuses_linked_dt(self, tmp_path, monkeypatch):
        from dt import config as cfg
        from dt.errors import DTError
        repo = self._repo(tmp_path)
        outside = tmp_path / 'outside'
        outside.mkdir()
        (repo / '.dt').symlink_to(outside)
        monkeypatch.chdir(repo)
        with patch('dt.config.get_config_paths',
                   return_value={'local': repo / '.dt' / 'config.local.yaml'}):
            with pytest.raises(DTError):
                cfg.set_values({'owner': 'x'}, scope='local')
        assert list(outside.iterdir()) == []


class TestProjectRootMustBeTrusted:
    """A .git/.dt in someone else's directory is not this user's project."""

    @pytest.fixture
    def foreign(self, tmp_path, monkeypatch):
        """tmp_path with a .git and .dt/config.yaml, made to look foreign."""
        (tmp_path / '.git').mkdir()
        (tmp_path / '.dt').mkdir()
        (tmp_path / '.dt' / 'config.yaml').write_text('owner: someone-else\n')
        sub = tmp_path / 'work'
        sub.mkdir()
        monkeypatch.chdir(sub)
        real = os.geteuid()
        monkeypatch.setattr(utils.os, 'geteuid', lambda: real + 1)
        monkeypatch.setattr(utils, '_trusted_cache', {})
        return tmp_path

    def test_foreign_root_is_located_but_not_trusted(self, foreign):
        assert utils.find_project_root() == foreign.resolve()
        assert utils.is_trusted_root(foreign.resolve()) is False

    def test_foreign_project_config_is_ignored(self, foreign):
        from dt import config as cfg
        assert cfg.get_value('owner') != 'someone-else'
        assert cfg.load_scope_config('project') == {}

    def test_no_dt_state_or_config_writes_there(self, foreign):
        """Not in the foreign root, and not in the user's own subfolder of it."""
        from dt import config as cfg
        from dt.errors import DTError
        for call in (utils.get_dt_dir, lambda: utils.dt_subdir('tmp'),
                     utils.ensure_dt_gitignore,
                     lambda: cfg.set_value('owner', 'x', scope='project'),
                     lambda: cfg.set_value('owner', 'x', scope='local'),
                     lambda: cfg.unset_value('owner', scope='project')):
            with pytest.raises(DTError):
                call()
        assert not (foreign / '.dt' / 'tmp').exists()
        assert not (foreign / '.dt' / '.gitignore').exists()
        assert not (foreign / 'work' / '.dt').exists()

    def test_init_refuses_before_doing_anything(self, foreign):
        from dt import init as init_mod
        from dt.errors import DTError
        with patch.object(init_mod, 'init_git') as git:
            with pytest.raises(DTError):
                init_mod.init_project(repo_path=foreign, verbose=False)
        git.assert_not_called()


class TestTrustedRootDetails:
    def test_foreign_marker_in_own_dir(self, tmp_path, monkeypatch):
        (tmp_path / '.git').mkdir()
        real_lstat = os.lstat
        st = real_lstat(tmp_path)

        class _Other:
            st_uid = os.geteuid() + 1
            st_mode = st.st_mode
        monkeypatch.setattr(utils, '_trusted_cache', {})
        monkeypatch.setattr(utils.os, 'lstat', lambda p, *a, **k: _Other()
                            if str(p).endswith('/.git') else real_lstat(p, *a, **k))
        assert utils.is_trusted_root(tmp_path) is False

    def test_root_not_yet_created_is_ours(self, tmp_path, monkeypatch):
        monkeypatch.setattr(utils, '_trusted_cache', {})
        assert utils.is_trusted_root(tmp_path / 'new') is True

    def test_untrusted_note_printed_once(self, tmp_path, monkeypatch, capsys):
        real = os.geteuid()
        monkeypatch.setattr(utils.os, 'geteuid', lambda: real + 1)
        monkeypatch.setattr(utils, '_trusted_cache', {})
        monkeypatch.setattr(utils, '_warned_untrusted', set())
        for _ in range(3):
            utils.is_trusted_root(tmp_path)
            utils._trusted_cache.clear()
        assert capsys.readouterr().err.count('ignoring') == 1
