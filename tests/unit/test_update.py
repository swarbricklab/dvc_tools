"""Unit tests for dt update module."""

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
import yaml

from dt import update, utils


# =============================================================================
# Test _find_import_files
# =============================================================================

class TestFindImportFiles:
    """Tests for _find_import_files function."""
    
    @patch('dt.update.subprocess.run')
    def test_finds_import_files(self, mock_run, tmp_path, monkeypatch):
        """Finds import files using git ls-files."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        # Create regular file
        regular_file = tmp_path / "regular.dvc"
        regular_file.write_text(yaml.dump({
            'outs': [{'path': 'regular.csv'}]
        }))
        
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="imported.dvc\nregular.dvc\n"
        )
        
        result = update._find_import_files()
        
        assert len(result) == 1
        assert 'imported.dvc' in result[0]
    
    @patch('dt.update.subprocess.run')
    def test_returns_empty_when_no_imports(self, mock_run, tmp_path, monkeypatch):
        """Returns empty list when no import files found."""
        monkeypatch.chdir(tmp_path)
        
        # Create only regular file
        regular_file = tmp_path / "regular.dvc"
        regular_file.write_text(yaml.dump({
            'outs': [{'path': 'regular.csv'}]
        }))
        
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="regular.dvc\n"
        )
        
        result = update._find_import_files()
        
        assert result == []


# =============================================================================
# Test update function
# =============================================================================

class TestUpdate:
    """Tests for update function."""
    
    @patch('dt.update.subprocess.run')
    def test_update_nonexistent_target(self, mock_run, tmp_path, monkeypatch):
        """Returns failure for non-existent target."""
        monkeypatch.chdir(tmp_path)
        
        results = update.update(targets=['nonexistent.dvc'])
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is False
        assert 'not found' in message.lower()
    
    @patch('dt.update.subprocess.run')
    def test_update_non_import_file(self, mock_run, tmp_path, monkeypatch):
        """Returns failure for non-import .dvc file."""
        monkeypatch.chdir(tmp_path)
        
        # Create regular file (not an import)
        regular_file = tmp_path / "regular.dvc"
        regular_file.write_text(yaml.dump({
            'outs': [{'path': 'regular.csv'}]
        }))
        
        results = update.update(targets=['regular.dvc'])
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is False
        assert 'not an import' in message.lower()
    
    @patch('dt.update.subprocess.run')
    @patch('dt.update._find_import_files')
    def test_update_no_targets_finds_imports(self, mock_find, mock_run, tmp_path, monkeypatch):
        """When no targets specified, finds and updates all imports."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        mock_find.return_value = ['imported.dvc']
        mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
        
        results = update.update()
        
        assert len(results) == 1
        mock_find.assert_called_once()
    
    @patch('dt.update.subprocess.run')
    @patch('dt.update._find_import_files')
    def test_update_no_imports_found(self, mock_find, mock_run, tmp_path, monkeypatch):
        """Returns success message when no imports found."""
        monkeypatch.chdir(tmp_path)
        
        mock_find.return_value = []
        
        results = update.update()
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is True
        assert 'no import' in message.lower()


# =============================================================================
# Test _push_dir_to_remote (regression: must never write a symlink into a remote)
# =============================================================================

class TestPushDirToRemote:
    """A .dir seeded into a shared remote must be a real file, never a symlink.

    Regression guard for the bug where the cache link-ladder's cross-filesystem
    fallback planted a symlinked <hash>.dir in the remote pointing at a
    per-machine cache path.
    """

    def test_writes_real_file_not_symlink(self, tmp_path):
        cache = tmp_path / "cache"
        remote = tmp_path / "remote"
        cache.mkdir()
        remote.mkdir()

        content = b'[{"md5":"deadbeef","relpath":"x.txt"}]'
        md5 = utils.md5_bytes(content)
        dir_file = cache / "files" / "md5" / md5[:2] / f"{md5[2:]}.dir"
        dir_file.parent.mkdir(parents=True)
        dir_file.write_bytes(content)

        pushed = update._push_dir_to_remote(dir_file, remote, md5 + ".dir")

        dest = remote / "files" / "md5" / md5[:2] / f"{md5[2:]}.dir"
        assert pushed is True
        assert dest.exists()
        assert not os.path.islink(dest), "must not plant a symlink in the remote"
        assert dest.read_bytes() == content

    def test_idempotent_when_already_present(self, tmp_path):
        cache = tmp_path / "cache"
        remote = tmp_path / "remote"
        cache.mkdir()
        remote.mkdir()

        content = b'{"a":1}'
        md5 = utils.md5_bytes(content)
        dir_file = cache / "files" / "md5" / md5[:2] / f"{md5[2:]}.dir"
        dir_file.parent.mkdir(parents=True)
        dir_file.write_bytes(content)

        assert update._push_dir_to_remote(dir_file, remote, md5 + ".dir") is True
        # Second push sees it already there and is a no-op.
        assert update._push_dir_to_remote(dir_file, remote, md5 + ".dir") is False
        dest = remote / "files" / "md5" / md5[:2] / f"{md5[2:]}.dir"
        assert not os.path.islink(dest)

