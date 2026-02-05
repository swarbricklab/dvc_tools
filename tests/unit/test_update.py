"""Unit tests for dt update module."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
import yaml

from dt import update


# =============================================================================
# Test _is_import_file
# =============================================================================

class TestIsImportFile:
    """Tests for _is_import_file function."""
    
    def test_import_file_with_deps_repo(self, tmp_path):
        """Returns True for .dvc file with deps.repo section."""
        dvc_file = tmp_path / "file.csv.dvc"
        dvc_file.write_text(yaml.dump({
            'md5': 'abc123',
            'deps': [{
                'path': 'data/file.csv',
                'repo': {
                    'url': 'git@github.com:org/repo.git',
                    'rev_lock': 'abc123',
                }
            }],
            'outs': [{'path': 'file.csv', 'md5': 'def456'}]
        }))
        
        assert update._is_import_file(dvc_file) is True
    
    def test_regular_dvc_file_no_deps(self, tmp_path):
        """Returns False for .dvc file without deps."""
        dvc_file = tmp_path / "data.csv.dvc"
        dvc_file.write_text(yaml.dump({
            'md5': 'abc123',
            'outs': [{'path': 'data.csv', 'md5': 'def456'}]
        }))
        
        assert update._is_import_file(dvc_file) is False
    
    def test_dvc_file_with_deps_no_repo(self, tmp_path):
        """Returns False for .dvc file with deps but no repo."""
        dvc_file = tmp_path / "output.csv.dvc"
        dvc_file.write_text(yaml.dump({
            'md5': 'abc123',
            'deps': [{'path': 'input.csv'}],
            'outs': [{'path': 'output.csv', 'md5': 'def456'}]
        }))
        
        assert update._is_import_file(dvc_file) is False
    
    def test_nonexistent_file(self, tmp_path):
        """Returns False for non-existent file."""
        dvc_file = tmp_path / "nonexistent.dvc"
        
        assert update._is_import_file(dvc_file) is False
    
    def test_invalid_yaml(self, tmp_path):
        """Returns False for invalid YAML file."""
        dvc_file = tmp_path / "invalid.dvc"
        dvc_file.write_text("not: valid: yaml: {{")
        
        assert update._is_import_file(dvc_file) is False
    
    def test_empty_file(self, tmp_path):
        """Returns False for empty file."""
        dvc_file = tmp_path / "empty.dvc"
        dvc_file.write_text("")
        
        assert update._is_import_file(dvc_file) is False


# =============================================================================
# Test get_import_info
# =============================================================================

class TestGetImportInfo:
    """Tests for get_import_info function."""
    
    def test_returns_import_info(self, tmp_path):
        """Returns correct info for import file."""
        dvc_file = tmp_path / "file.csv.dvc"
        dvc_file.write_text(yaml.dump({
            'deps': [{
                'path': 'data/file.csv',
                'repo': {
                    'url': 'git@github.com:org/repo.git',
                    'rev': 'main',
                }
            }],
            'outs': [{'path': 'file.csv'}]
        }))
        
        info = update.get_import_info(dvc_file)
        
        assert info['repo_url'] == 'git@github.com:org/repo.git'
        assert info['path'] == 'data/file.csv'
        assert info['rev'] == 'main'
    
    def test_returns_empty_for_non_import(self, tmp_path):
        """Returns empty dict for non-import file."""
        dvc_file = tmp_path / "data.csv.dvc"
        dvc_file.write_text(yaml.dump({
            'outs': [{'path': 'data.csv', 'md5': 'def456'}]
        }))
        
        info = update.get_import_info(dvc_file)
        
        assert info == {}
    
    def test_returns_empty_for_nonexistent(self, tmp_path):
        """Returns empty dict for non-existent file."""
        dvc_file = tmp_path / "nonexistent.dvc"
        
        info = update.get_import_info(dvc_file)
        
        assert info == {}


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
    def test_update_single_target(self, mock_run, tmp_path, monkeypatch):
        """Updates a single target successfully."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
        
        results = update.update(targets=['imported.dvc'])
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is True
        assert 'Updated' in message
    
    @patch('dt.update.subprocess.run')
    def test_update_with_rev(self, mock_run, tmp_path, monkeypatch):
        """Updates with --rev option."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
        
        results = update.update(targets=['imported.dvc'], rev='v1.2.0')
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is True
        assert 'v1.2.0' in message
        
        # Check that --rev was passed to dvc update
        call_args = mock_run.call_args[0][0]
        assert '--rev' in call_args
        assert 'v1.2.0' in call_args
    
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
    
    @patch('dt.update.subprocess.run')
    def test_update_with_no_download(self, mock_run, tmp_path, monkeypatch):
        """Passes --no-download to dvc update."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
        
        update.update(targets=['imported.dvc'], no_download=True)
        
        call_args = mock_run.call_args[0][0]
        assert '--no-download' in call_args
    
    @patch('dt.update.subprocess.run')
    def test_update_dvc_error(self, mock_run, tmp_path, monkeypatch):
        """Handles dvc update errors gracefully."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout='',
            stderr='ERROR: Repository not found'
        )
        
        results = update.update(targets=['imported.dvc'])
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is False
        assert 'not found' in message.lower()
    
    @patch('dt.update.subprocess.run')
    def test_update_adds_dvc_extension(self, mock_run, tmp_path, monkeypatch):
        """Adds .dvc extension if missing."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
        
        # Pass without .dvc extension
        results = update.update(targets=['imported'])
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is True
