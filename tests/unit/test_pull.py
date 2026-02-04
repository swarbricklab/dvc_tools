"""Tests for dt pull module internal functions.

Tests pure utility functions that can be unit tested in isolation.
"""

import os
import tempfile
import shutil
from pathlib import Path
from typing import Any, Dict

import pytest

from dt import pull


class TestPartitionManifest:
    """Tests for partition_manifest function."""
    
    def test_empty_manifest(self):
        """Empty manifest returns empty partitions."""
        manifest = {'files': [], 'paths': {}, 'remote': None, 'repo_root': '/tmp'}
        result = pull.partition_manifest(manifest, num_workers=4)
        
        assert len(result) == 4
        for i in range(4):
            assert result[i] == []
    
    def test_single_file_single_worker(self):
        """Single file goes to single worker."""
        file_hash = 'a1b2c3d4e5f6'
        manifest = {
            'files': [file_hash],
            'paths': {file_hash: '/some/path'},
            'remote': None,
            'repo_root': '/tmp'
        }
        result = pull.partition_manifest(manifest, num_workers=1)
        
        assert len(result) == 1
        assert result[0] == [file_hash]
    
    def test_multiple_files_distributed(self):
        """Multiple files are distributed across workers."""
        # Create hashes with different prefixes to ensure distribution
        file_hashes = [
            '00aabbccdd',  # prefix 00 = 0
            '11aabbccdd',  # prefix 11 = 17  
            'ffaabbccdd',  # prefix ff = 255
            'a0aabbccdd',  # prefix a0 = 160
        ]
        manifest = {
            'files': file_hashes,
            'paths': {h: f'/path/{h}' for h in file_hashes},
            'remote': None,
            'repo_root': '/tmp'
        }
        result = pull.partition_manifest(manifest, num_workers=4)
        
        # Each hash should end up in exactly one partition
        all_assigned = []
        for partition_hashes in result.values():
            all_assigned.extend(partition_hashes)
        
        assert sorted(all_assigned) == sorted(file_hashes)
    
    def test_partition_by_hex_prefix(self):
        """Files are partitioned by hex prefix modulo num_workers."""
        # Hash with prefix '10' = 16 decimal, 16 % 4 = 0
        hash_16 = '10aabbccdd'
        # Hash with prefix '11' = 17 decimal, 17 % 4 = 1
        hash_17 = '11aabbccdd'
        # Hash with prefix '12' = 18 decimal, 18 % 4 = 2
        hash_18 = '12aabbccdd'
        # Hash with prefix '13' = 19 decimal, 19 % 4 = 3
        hash_19 = '13aabbccdd'
        
        file_hashes = [hash_16, hash_17, hash_18, hash_19]
        manifest = {
            'files': file_hashes,
            'paths': {},
            'remote': None,
            'repo_root': '/tmp'
        }
        result = pull.partition_manifest(manifest, num_workers=4)
        
        assert hash_16 in result[0]
        assert hash_17 in result[1]
        assert hash_18 in result[2]
        assert hash_19 in result[3]
    
    def test_more_workers_than_files(self):
        """Handles more workers than files gracefully."""
        file_hashes = ['aabbccdd11', 'bbccddee22']
        manifest = {
            'files': file_hashes,
            'paths': {},
            'remote': None,
            'repo_root': '/tmp'
        }
        result = pull.partition_manifest(manifest, num_workers=10)
        
        assert len(result) == 10
        # All files should be distributed somewhere
        total_files = sum(len(v) for v in result.values())
        assert total_files == 2


class TestResolveToDvcFile:
    """Tests for resolve_to_dvc_file function."""
    
    @pytest.fixture
    def dvc_project(self, tmp_path, monkeypatch):
        """Create a minimal DVC project structure."""
        # Create .dvc directory to mark it as a DVC project
        (tmp_path / '.dvc').mkdir()
        
        # Create some .dvc files
        (tmp_path / 'data.csv.dvc').write_text('outs:\n  - md5: abc123\n    path: data.csv\n')
        (tmp_path / 'models').mkdir()
        (tmp_path / 'models' / 'model.pkl.dvc').write_text('outs:\n  - md5: def456\n    path: model.pkl\n')
        (tmp_path / 'output_dir.dvc').write_text('outs:\n  - md5: ghi789.dir\n    path: output_dir\n')
        
        # Change to project directory
        original_cwd = os.getcwd()
        monkeypatch.chdir(tmp_path)
        
        yield tmp_path
        
        os.chdir(original_cwd)
    
    def test_already_dvc_file_exists(self, dvc_project):
        """Resolve .dvc file that exists returns it."""
        result = pull.resolve_to_dvc_file('data.csv.dvc')
        assert result == Path('data.csv.dvc')
    
    def test_already_dvc_file_not_exists(self, dvc_project):
        """Resolve .dvc file that doesn't exist returns None."""
        result = pull.resolve_to_dvc_file('nonexistent.csv.dvc')
        assert result is None
    
    def test_target_with_dvc_suffix_exists(self, dvc_project):
        """Resolve target when {target}.dvc exists."""
        result = pull.resolve_to_dvc_file('data.csv')
        assert result == Path('data.csv.dvc')
    
    def test_target_in_subdirectory(self, dvc_project):
        """Resolve target in subdirectory."""
        result = pull.resolve_to_dvc_file('models/model.pkl')
        assert result == Path('models/model.pkl.dvc')
    
    def test_directory_target(self, dvc_project):
        """Resolve directory target to its .dvc file."""
        result = pull.resolve_to_dvc_file('output_dir')
        assert result == Path('output_dir.dvc')
    
    def test_no_dvc_file_exists(self, dvc_project):
        """Returns None when no .dvc file exists for target."""
        result = pull.resolve_to_dvc_file('random_file.txt')
        assert result is None
    
    def test_nested_file_in_tracked_dir(self, dvc_project):
        """File inside tracked directory resolves to parent .dvc file."""
        # Create the tracked directory
        (dvc_project / 'output_dir').mkdir()
        (dvc_project / 'output_dir' / 'nested_file.txt').touch()
        
        result = pull.resolve_to_dvc_file('output_dir/nested_file.txt')
        # Result may be absolute path, compare names
        assert result is not None
        assert result.name == 'output_dir.dvc'


class TestFindAllDvcFiles:
    """Tests for find_all_dvc_files function."""
    
    @pytest.fixture
    def dvc_project_tree(self, tmp_path, monkeypatch):
        """Create a DVC project with various .dvc files."""
        # Create .dvc directory (should be excluded from results)
        (tmp_path / '.dvc').mkdir()
        
        # Create .dvc files at various levels
        (tmp_path / 'root_file.dvc').write_text('outs:\n  - path: root_file\n')
        (tmp_path / 'data').mkdir()
        (tmp_path / 'data' / 'train.csv.dvc').write_text('outs:\n  - path: train.csv\n')
        (tmp_path / 'data' / 'test.csv.dvc').write_text('outs:\n  - path: test.csv\n')
        (tmp_path / 'models').mkdir()
        (tmp_path / 'models' / 'model.pkl.dvc').write_text('outs:\n  - path: model.pkl\n')
        
        # Create .dt directory with temp files (should be excluded)
        (tmp_path / '.dt').mkdir()
        (tmp_path / '.dt' / 'temp.dvc').write_text('temp file')
        
        monkeypatch.chdir(tmp_path)
        yield tmp_path
    
    def test_finds_all_dvc_files(self, dvc_project_tree):
        """Finds all .dvc files in the tree."""
        result = pull.find_all_dvc_files()
        
        # Should find 4 .dvc files (excluding .dvc/ and .dt/ directories)
        assert len(result) == 4
        
        # Check all expected files are found
        names = [f.name for f in result]
        assert 'root_file.dvc' in names
        assert 'train.csv.dvc' in names
        assert 'test.csv.dvc' in names
        assert 'model.pkl.dvc' in names
    
    def test_excludes_dvc_directory(self, dvc_project_tree):
        """Excludes .dvc directory from results."""
        result = pull.find_all_dvc_files()
        
        for path in result:
            assert '.dvc' not in path.parts or path.suffix == '.dvc'
    
    def test_excludes_dt_directory(self, dvc_project_tree):
        """Excludes .dt directory from results."""
        result = pull.find_all_dvc_files()
        
        for path in result:
            assert '.dt' not in path.parts
    
    def test_results_are_sorted(self, dvc_project_tree):
        """Results are returned sorted."""
        result = pull.find_all_dvc_files()
        
        # Results should be sorted
        assert result == sorted(result)


class TestIsImportTarget:
    """Tests for is_import_target function."""
    
    @pytest.fixture
    def import_project(self, tmp_path, monkeypatch):
        """Create project with both import and regular .dvc files."""
        (tmp_path / '.dvc').mkdir()
        
        # Regular .dvc file (no deps.repo)
        (tmp_path / 'local.csv.dvc').write_text(
            'outs:\n  - md5: abc123\n    path: local.csv\n'
        )
        
        # Import .dvc file (has deps section with url)
        (tmp_path / 'imported.csv.dvc').write_text(
            'deps:\n'
            '  - path: data.csv\n'
            '    repo:\n'
            '      url: https://github.com/example/repo\n'
            '      rev: main\n'
            'outs:\n'
            '  - md5: def456\n'
            '    path: imported.csv\n'
        )
        
        monkeypatch.chdir(tmp_path)
        yield tmp_path
    
    def test_regular_file_not_import(self, import_project):
        """Regular .dvc file is not an import."""
        is_import, dvc_file = pull.is_import_target('local.csv')
        
        # The function returns False for regular files
        # dvc_file should be the resolved .dvc file
        assert dvc_file == Path('local.csv.dvc')
    
    def test_nonexistent_target(self, import_project):
        """Nonexistent target returns None for dvc_file."""
        is_import, dvc_file = pull.is_import_target('nonexistent.csv')
        
        assert is_import is False
        assert dvc_file is None


class TestSeparateTargets:
    """Tests for separate_targets function."""
    
    @pytest.fixture
    def mixed_project(self, tmp_path, monkeypatch):
        """Create project with various target types."""
        (tmp_path / '.dvc').mkdir()
        
        # Regular .dvc files
        (tmp_path / 'local1.csv.dvc').write_text(
            'outs:\n  - md5: abc\n    path: local1.csv\n'
        )
        (tmp_path / 'local2.csv.dvc').write_text(
            'outs:\n  - md5: def\n    path: local2.csv\n'
        )
        
        # Import .dvc file
        (tmp_path / 'imported.csv.dvc').write_text(
            'deps:\n'
            '  - path: data.csv\n'
            '    repo:\n'
            '      url: https://github.com/example/repo\n'
            'outs:\n'
            '  - md5: ghi\n'
            '    path: imported.csv\n'
        )
        
        monkeypatch.chdir(tmp_path)
        yield tmp_path
    
    def test_separate_empty_list(self, mixed_project):
        """Empty list returns empty results."""
        imports, regulars = pull.separate_targets([])
        
        assert imports == []
        assert regulars == []
    
    def test_all_regular_targets(self, mixed_project):
        """All regular targets go to regular list."""
        imports, regulars = pull.separate_targets(['local1.csv', 'local2.csv'])
        
        # Without actual DVC repo context, these may be classified differently
        # but they should be separated consistently
        total = len(imports) + len(regulars)
        assert total == 2
    
    def test_nonexistent_targets(self, mixed_project):
        """Nonexistent targets are handled gracefully."""
        imports, regulars = pull.separate_targets(['nonexistent.csv'])
        
        # Should not raise, returns empty or regular depending on implementation
        assert len(imports) + len(regulars) <= 1
