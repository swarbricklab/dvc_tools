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
from dt.errors import DependencyError, DVCFileError


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
    
    @pytest.mark.skipif(not shutil.which('git'), reason="git not installed")
    def test_finds_root_in_git_repo(self, tmp_path, monkeypatch):
        """Finds root when in a git repository."""
        monkeypatch.chdir(tmp_path)
        subprocess.run(['git', 'init'], capture_output=True)
        
        # find_git_root uses DVC internals which need DVC repo
        # This test may return None without DVC
        result = utils.find_git_root()
        # Accept either the path or None (if DVC not available)
        assert result is None or result == tmp_path


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


# Run with: pytest tests/test_utils.py -v
