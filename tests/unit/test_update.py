"""Unit tests for dt update module."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
import yaml

from dt import update


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

