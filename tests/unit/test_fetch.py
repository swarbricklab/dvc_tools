"""Tests for dt fetch module internal functions.

Tests utility functions from fetch and import_data modules.
"""

import os
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from dt import fetch
from dt import import_data


class TestPopulateCacheFile:
    """Tests for populate_cache_file function."""
    
    @pytest.fixture
    def cache_dirs(self, tmp_path):
        """Create source and destination cache directories.
        
        Source is a remote/cache root (contains files/md5 structure).
        Dest is the cache root (populate_cache_file adds files/md5 internally for v3).
        """
        source = tmp_path / 'source_cache'
        dest = tmp_path / 'dest_cache'
        
        # Create cache structure
        (source / 'files' / 'md5').mkdir(parents=True)
        dest.mkdir(parents=True)
        
        # Both source and dest are cache roots
        return {'source': str(source), 'dest': str(dest)}
    
    def test_single_file_cached_via_hardlink(self, cache_dirs):
        """Single file is cached using hardlink."""
        md5 = 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6'
        
        # Create source file
        source_dir = Path(cache_dirs['source']) / 'files' / 'md5' / md5[:2]
        source_dir.mkdir(parents=True)
        source_file = source_dir / md5[2:]
        source_file.write_text('test content')
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=True,
        )
        
        # File should be cached
        assert result is True
        
        # Destination file should exist in v3 layout (files/md5/XX/hash)
        dest_file = Path(cache_dirs['dest']) / 'files' / 'md5' / md5[:2] / md5[2:]
        assert dest_file.exists()
        assert dest_file.read_text() == 'test content'
    
    def test_dir_file_cached(self, cache_dirs):
        """Directory manifest (.dir file) is cached correctly."""
        md5 = 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6.dir'
        hash_only = md5[:-4]
        
        # Create source .dir file
        source_dir = Path(cache_dirs['source']) / 'files' / 'md5' / hash_only[:2]
        source_dir.mkdir(parents=True)
        source_file = source_dir / (hash_only[2:] + '.dir')
        dir_content = [{'relpath': 'a.txt', 'md5': 'abc123'}]
        source_file.write_text(json.dumps(dir_content))
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=True,
        )
        
        assert result is True
        
        # Destination .dir file should exist in v3 layout
        dest_file = Path(cache_dirs['dest']) / 'files' / 'md5' / hash_only[:2] / (hash_only[2:] + '.dir')
        assert dest_file.exists()
        assert json.loads(dest_file.read_text()) == dir_content
    
    def test_already_exists_returns_false(self, cache_dirs):
        """File already in destination returns False."""
        md5 = 'deadbeefcafe1234567890abcdef0123'
        
        # Create source file (source needs files/md5 added)
        source_dir = Path(cache_dirs['source']) / 'files' / 'md5' / md5[:2]
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / md5[2:]).write_text('source content')
        
        # Create dest file in v3 layout
        dest_dir = Path(cache_dirs['dest']) / 'files' / 'md5' / md5[:2]
        dest_dir.mkdir(parents=True, exist_ok=True)
        (dest_dir / md5[2:]).write_text('dest content')
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=True,
        )
        
        # Should return False (already exists)
        assert result is False
    
    def test_source_not_found_returns_none(self, cache_dirs):
        """Source file not found returns None."""
        md5 = 'nonexistent123456789012345678901234'
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            verbose=True,
            use_v3_layout=True,
        )
        
        assert result is None

    def test_dvc_v2_layout_fallback(self, cache_dirs):
        """Falls back to DVC v2 layout (XX/hash directly in root) for source.
        
        When source uses v2 layout but .dvc file is v3, dest uses v3 layout.
        """
        md5 = 'v2layout1234567890abcdef12345678'
        
        # Create source file in v2 layout (directly in root, no files/md5)
        source_dir = Path(cache_dirs['source']) / md5[:2]
        source_dir.mkdir(parents=True)
        source_file = source_dir / md5[2:]
        source_file.write_text('v2 layout content')
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=True,  # .dvc file is v3 format
        )
        
        assert result is True
        
        # Destination file should exist in v3 layout
        dest_file = Path(cache_dirs['dest']) / 'files' / 'md5' / md5[:2] / md5[2:]
        assert dest_file.exists()
        assert dest_file.read_text() == 'v2 layout content'

    def test_dvc_v2_dest_layout(self, cache_dirs):
        """Test v2 destination layout for legacy .dvc files."""
        md5 = 'v2destlayout12345678901234567890'
        
        # Create source file in v2 layout
        source_dir = Path(cache_dirs['source']) / md5[:2]
        source_dir.mkdir(parents=True)
        source_file = source_dir / md5[2:]
        source_file.write_text('v2 content')
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=False,  # .dvc file is v2 format (no 'hash:' field)
        )
        
        assert result is True
        
        # Destination file should exist in v2 layout (no files/md5)
        dest_file = Path(cache_dirs['dest']) / md5[:2] / md5[2:]
        assert dest_file.exists()
        assert dest_file.read_text() == 'v2 content'
        
        # Should NOT be in v3 layout
        v3_path = Path(cache_dirs['dest']) / 'files' / 'md5' / md5[:2] / md5[2:]
        assert not v3_path.exists()

    def test_dvc_v3_preferred_over_v2(self, cache_dirs):
        """DVC v3 layout (files/md5/) is preferred over v2 layout for source."""
        md5 = 'v3preferred12345678901234567890ab'
        
        # Create source file in BOTH v3 and v2 layouts
        v3_dir = Path(cache_dirs['source']) / 'files' / 'md5' / md5[:2]
        v3_dir.mkdir(parents=True)
        (v3_dir / md5[2:]).write_text('v3 content')
        
        v2_dir = Path(cache_dirs['source']) / md5[:2]
        v2_dir.mkdir(parents=True)
        (v2_dir / md5[2:]).write_text('v2 content')
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=True,
        )
        
        assert result is True
        
        # Should have used v3 content from source, dest in v3 layout
        dest_file = Path(cache_dirs['dest']) / 'files' / 'md5' / md5[:2] / md5[2:]
        assert dest_file.exists()
        assert dest_file.read_text() == 'v3 content'


class TestBuildDirManifest:
    """Tests for build_dir_manifest function."""
    
    def test_builds_correct_format(self):
        """Test manifest is built with correct DVC JSON format."""
        entries = [
            {'md5': 'aaaa', 'relpath': 'b.txt'},
            {'md5': 'bbbb', 'relpath': 'a.txt'},
        ]
        
        content = import_data.build_dir_manifest(entries)
        
        # Should be sorted by relpath
        expected = b'[{"md5": "bbbb", "relpath": "a.txt"}, {"md5": "aaaa", "relpath": "b.txt"}]'
        assert content == expected
    
    def test_hash_is_deterministic(self):
        """Test same entries produce same hash."""
        import hashlib
        
        entries = [
            {'md5': '7320ddd77a276f2ecd73ed18e631ee2b', 'relpath': 'a.csv'},
            {'md5': 'c2ad4b026e39ec2257321d20373b9f47', 'relpath': 'b.csv'},
        ]
        
        content = import_data.build_dir_manifest(entries)
        actual_hash = hashlib.md5(content).hexdigest()
        
        # This is the actual hash from dt-test-registry's data/dir
        expected_hash = 'bc894c83412ff34cbc40f9bcb5983258'
        assert actual_hash == expected_hash


class TestConstructDirFile:
    """Tests for construct_dir_file function."""
    
    @pytest.fixture
    def source_dir_setup(self, tmp_path):
        """Create a source directory with files for testing."""
        source_dir = tmp_path / 'source_dir'
        source_dir.mkdir()
        
        # Create files with known content (matching dt-test-registry)
        (source_dir / 'a.csv').write_text('header\na,1,2\na,2,3\na,3,4\n')
        (source_dir / 'b.csv').write_text('header\nb,1,2\nb,2,3\n')
        
        cache = tmp_path / 'cache'
        cache.mkdir()
        
        return {
            'source_dir': source_dir,
            'cache': cache,
        }
    
    def test_constructs_dir_file_with_matching_hash(self, source_dir_setup):
        """Test .dir file is constructed with correct hash."""
        import hashlib
        
        source_dir = source_dir_setup['source_dir']
        cache = source_dir_setup['cache']
        
        # Calculate expected hash from the files
        entries = []
        for file in sorted(source_dir.iterdir()):
            content = file.read_bytes()
            md5 = hashlib.md5(content).hexdigest()
            entries.append({'md5': md5, 'relpath': file.name})
        
        manifest_content = import_data.build_dir_manifest(entries)
        expected_hash = hashlib.md5(manifest_content).hexdigest()
        
        # Now construct and verify it matches
        result = import_data.construct_dir_file(
            source_dir=source_dir,
            expected_hash=expected_hash,
            dest_cache=str(cache),
            use_v3_layout=True,
        )
        
        assert result is not None
        assert len(result) == 2
        
        # Verify .dir file was created
        dir_file = cache / 'files' / 'md5' / expected_hash[:2] / f"{expected_hash[2:]}.dir"
        assert dir_file.exists()
    
    def test_returns_none_on_hash_mismatch(self, source_dir_setup):
        """Test returns None when constructed hash doesn't match expected."""
        source_dir = source_dir_setup['source_dir']
        cache = source_dir_setup['cache']
        
        # Use a wrong expected hash
        result = import_data.construct_dir_file(
            source_dir=source_dir,
            expected_hash='0000000000000000000000000000000',
            dest_cache=str(cache),
            use_v3_layout=True,
        )
        
        assert result is None
    
    def test_returns_none_for_nonexistent_dir(self, tmp_path):
        """Test returns None when source directory doesn't exist."""
        result = import_data.construct_dir_file(
            source_dir=tmp_path / 'nonexistent',
            expected_hash='abc123',
            dest_cache=str(tmp_path / 'cache'),
            use_v3_layout=True,
        )
        
        assert result is None


class TestPopulateCacheFromSource:
    """Tests for _populate_cache_from_source function."""
    
    @pytest.fixture
    def fetch_setup(self, tmp_path):
        """Create DVC project with cache for testing."""
        project = tmp_path / 'project'
        project.mkdir()
        (project / '.dvc').mkdir()
        
        # Create primary cache (files/md5 path, matching repo.cache.local.path)
        cache = tmp_path / 'cache' / 'files' / 'md5'
        cache.mkdir(parents=True)
        
        # Create source cache with files (source is cache root)
        source_cache = tmp_path / 'source_cache'
        (source_cache / 'files' / 'md5').mkdir(parents=True)
        
        return {
            'project': project,
            'cache': cache,  # This is the files/md5 path
            'source_cache': source_cache,
        }
    
    def test_single_file_fetch(self, fetch_setup, monkeypatch):
        """Fetch single file populates cache (v3 format)."""
        project = fetch_setup['project']
        source_cache = fetch_setup['source_cache']
        cache = fetch_setup['cache']
        
        # Create .dvc file in v3 format (has hash: md5 field)
        md5 = 'abcdef1234567890abcdef1234567890'
        dvc_content = f'outs:\n  - md5: {md5}\n    size: 8\n    hash: md5\n    path: data.csv\n'
        dvc_file = project / 'data.csv.dvc'
        dvc_file.write_text(dvc_content)
        
        # Create source cache file in v3 layout
        source_dir = source_cache / 'files' / 'md5' / md5[:2]
        source_dir.mkdir(parents=True)
        (source_dir / md5[2:]).write_text('csv,data')
        
        # Mock get_cache_dir to return our test cache
        monkeypatch.chdir(project)
        
        with patch('dt.fetch.utils.get_cache_dir', return_value=cache):
            count, failed = fetch._populate_cache_from_source(
                dvc_path=dvc_file,
                source_cache=str(source_cache),
                verbose=False,
            )
        
        assert count == 1
        assert failed == 0
        
        # Verify file is in v3 cache layout (files/md5/XX/hash)
        dest_file = cache / md5[:2] / md5[2:]
        assert dest_file.exists()
    
    def test_directory_fetch(self, fetch_setup, monkeypatch):
        """Fetch directory populates cache with .dir and files (v3 format)."""
        project = fetch_setup['project']
        source_cache = fetch_setup['source_cache']
        cache = fetch_setup['cache']
        
        # Create .dvc file for directory in v3 format
        dir_hash = 'abcdef1234567890abcdef1234567890'
        file1_hash = '1111111111111111111111111111111a'
        file2_hash = '2222222222222222222222222222222b'
        
        dvc_content = f'outs:\n  - md5: {dir_hash}.dir\n    size: 100\n    hash: md5\n    nfiles: 2\n    path: mydir\n'
        dvc_file = project / 'mydir.dvc'
        dvc_file.write_text(dvc_content)
        
        # Create source cache .dir file
        source_dir = source_cache / 'files' / 'md5' / dir_hash[:2]
        source_dir.mkdir(parents=True)
        dir_manifest = [
            {'relpath': 'file1.txt', 'md5': file1_hash},
            {'relpath': 'file2.txt', 'md5': file2_hash},
        ]
        (source_dir / (dir_hash[2:] + '.dir')).write_text(json.dumps(dir_manifest))
        
        # Create source cache individual files
        for fhash in [file1_hash, file2_hash]:
            fdir = source_cache / 'files' / 'md5' / fhash[:2]
            fdir.mkdir(parents=True, exist_ok=True)
            (fdir / fhash[2:]).write_text(f'content for {fhash}')
        
        monkeypatch.chdir(project)
        
        with patch('dt.fetch.utils.get_cache_dir', return_value=cache):
            count, failed = fetch._populate_cache_from_source(
                dvc_path=dvc_file,
                source_cache=str(source_cache),
                verbose=False,
            )
        
        # Should cache .dir file + 2 individual files = 3
        assert count == 3
        assert failed == 0
    
    def test_no_cache_returns_zero(self, fetch_setup, monkeypatch):
        """No cache configured returns 0."""
        project = fetch_setup['project']
        source_cache = fetch_setup['source_cache']
        
        md5 = 'abcdef1234567890abcdef1234567890'
        dvc_file = project / 'data.csv.dvc'
        dvc_file.write_text(f'outs:\n  - md5: {md5}\n    size: 8\n    hash: md5\n    path: data.csv\n')
        
        monkeypatch.chdir(project)
        
        with patch('dt.fetch.utils.get_cache_dir', return_value=None):
            count, failed = fetch._populate_cache_from_source(
                dvc_path=dvc_file,
                source_cache=str(source_cache),
            )
        
        assert count == 0
        assert failed == 0
    
    def test_no_outs_returns_zero(self, fetch_setup, monkeypatch):
        """DVC file with no outputs returns 0."""
        project = fetch_setup['project']
        source_cache = fetch_setup['source_cache']
        cache = fetch_setup['cache']
        
        dvc_file = project / 'empty.dvc'
        dvc_file.write_text('# empty dvc file\n')
        
        monkeypatch.chdir(project)
        
        with patch('dt.fetch.utils.get_cache_dir', return_value=cache):
            count, failed = fetch._populate_cache_from_source(
                dvc_path=dvc_file,
                source_cache=str(source_cache),
            )
        
        assert count == 0
        assert failed == 0

    def test_missing_source_file_reports_failure(self, fetch_setup, monkeypatch):
        """Missing source file is counted as failure."""
        project = fetch_setup['project']
        source_cache = fetch_setup['source_cache']
        cache = fetch_setup['cache']
        
        # Create .dvc file pointing to hash that doesn't exist in source (v3 format)
        md5 = 'missing1234567890abcdef1234567890'
        dvc_content = f'outs:\n  - md5: {md5}\n    size: 8\n    hash: md5\n    path: data.csv\n'
        dvc_file = project / 'data.csv.dvc'
        dvc_file.write_text(dvc_content)
        
        # Don't create the source file - it should fail
        
        monkeypatch.chdir(project)
        
        with patch('dt.fetch.utils.get_cache_dir', return_value=cache):
            count, failed = fetch._populate_cache_from_source(
                dvc_path=dvc_file,
                source_cache=str(source_cache),
                verbose=False,
            )
        
        # Should report 0 successful, 1 failed
        assert count == 0
        assert failed == 1

    def test_dvc_v2_layout_directory_fetch(self, fetch_setup, monkeypatch):
        """Fetch directory from DVC v2 layout (XX/hash in root)."""
        project = fetch_setup['project']
        source_cache = fetch_setup['source_cache']
        cache = fetch_setup['cache']
        
        # Create .dvc file for directory
        dir_hash = 'v2dir12345678901234567890123456'
        file1_hash = 'v2file1234567890123456789012345a'
        
        dvc_content = f'outs:\n  - md5: {dir_hash}.dir\n    path: mydir\n'
        dvc_file = project / 'mydir.dvc'
        dvc_file.write_text(dvc_content)
        
        # Create source cache in v2 layout (directly in root)
        source_dir = source_cache / dir_hash[:2]
        source_dir.mkdir(parents=True)
        dir_manifest = [{'relpath': 'file1.txt', 'md5': file1_hash}]
        (source_dir / (dir_hash[2:] + '.dir')).write_text(json.dumps(dir_manifest))
        
        # Create individual file in v2 layout
        fdir = source_cache / file1_hash[:2]
        fdir.mkdir(parents=True, exist_ok=True)
        (fdir / file1_hash[2:]).write_text('v2 file content')
        
        monkeypatch.chdir(project)
        
        with patch('dt.fetch.utils.get_cache_dir', return_value=cache):
            count, failed = fetch._populate_cache_from_source(
                dvc_path=dvc_file,
                source_cache=str(source_cache),
                verbose=False,
            )
        
        # Should cache .dir file + 1 individual file = 2
        assert count == 2
        assert failed == 0


class TestFetch:
    """Tests for the main fetch function."""
    
    @pytest.fixture
    def dvc_project(self, tmp_path, monkeypatch):
        """Create a minimal DVC project."""
        project = tmp_path / 'project'
        project.mkdir()
        (project / '.dvc').mkdir()
        
        # Create cache
        cache = tmp_path / 'cache' / 'files' / 'md5'
        cache.mkdir(parents=True)
        
        monkeypatch.chdir(project)
        yield project, tmp_path / 'cache'
    
    def test_not_in_git_repo_raises_error(self, dvc_project):
        """Fetch fails with clear error when not in a git repository."""
        from dt import doctor
        project, cache = dvc_project
        
        # Mock check_environment to return status showing not in git repo
        mock_env = MagicMock()
        mock_env.in_git_repo = False
        mock_env.require_git_repo.side_effect = fetch.FetchError("Not in a git repository")
        
        with patch.object(doctor, 'check_environment', return_value=mock_env):
            with pytest.raises(fetch.FetchError) as exc_info:
                fetch.fetch(targets=['data.csv.dvc'])
        
        assert 'git repository' in str(exc_info.value).lower()
    
    def test_nonexistent_target_fails(self, dvc_project):
        """Nonexistent target returns failure via DVC stage system."""
        from dvc.stage.exceptions import StageFileDoesNotExistError
        from dt import doctor
        project, cache = dvc_project
        
        # Mock check_environment to return valid status
        mock_env = MagicMock()
        mock_env.in_git_repo = True
        mock_env.require_git_repo.return_value = None
        
        with patch.object(doctor, 'check_environment', return_value=mock_env), \
             patch('dt.fetch.utils.check_dvc'), \
             patch('dt.fetch.utils.collect_stages', side_effect=StageFileDoesNotExistError('nonexistent.csv.dvc')):
            
            with pytest.raises(fetch.FetchError) as exc_info:
                fetch.fetch(targets=['nonexistent.csv.dvc'])
        
        assert 'nonexistent' in str(exc_info.value).lower()
    
    def test_empty_stages_returns_empty_results(self, dvc_project):
        """No stages to process returns empty results."""
        from dt import doctor
        project, cache = dvc_project
        
        # Mock check_environment to return valid status
        mock_env = MagicMock()
        mock_env.in_git_repo = True
        mock_env.require_git_repo.return_value = None
        
        with patch.object(doctor, 'check_environment', return_value=mock_env), \
             patch('dt.fetch.utils.check_dvc'), \
             patch('dt.fetch.utils.collect_stages', return_value=[]):
            
            results = fetch.fetch()
        
        assert results == []
    
    def test_import_stage_without_local_cache_fails(self, dvc_project):
        """Import stage without local cache returns 'no local source' error."""
        from dt import doctor
        project, cache = dvc_project
        
        # Mock check_environment to return valid status
        mock_env = MagicMock()
        mock_env.in_git_repo = True
        mock_env.require_git_repo.return_value = None
        
        # Create a mock stage that is_repo_import
        mock_stage = MagicMock()
        mock_stage.is_repo_import = True
        mock_stage.is_import = False  # repo imports have is_import=False
        mock_stage.addressing = 'imported.csv.dvc'
        mock_stage.path = str(project / 'imported.csv.dvc')
        mock_stage.outs = [MagicMock(use_cache=True, hash_info=MagicMock(value='abc123'), changed_cache=MagicMock(return_value=True))]
        
        with patch.object(doctor, 'check_environment', return_value=mock_env), \
             patch('dt.fetch.utils.check_dvc'), \
             patch('dt.fetch.utils.collect_stages', return_value=[mock_stage]), \
             patch('dt.fetch.utils.get_import_info', return_value=None):
            
            results = fetch.fetch(targets=['imported.csv.dvc'])
        
        # Without local cache info, import goes to no_source and fails
        assert len(results) == 1
        target, success, message = results[0]
        assert success is False
        assert 'No local source' in message

    def test_import_stage_without_local_cache_uses_network(self, dvc_project):
        """Import stage without local cache can use --network flag."""
        from dt import doctor
        project, cache = dvc_project
        
        # Mock check_environment to return valid status
        mock_env = MagicMock()
        mock_env.in_git_repo = True
        mock_env.require_git_repo.return_value = None
        
        # Create a mock stage that is_repo_import
        mock_stage = MagicMock()
        mock_stage.is_repo_import = True
        mock_stage.is_import = False
        mock_stage.addressing = 'imported.csv.dvc'
        mock_stage.path = str(project / 'imported.csv.dvc')
        mock_stage.outs = [MagicMock(use_cache=True, hash_info=MagicMock(value='abc123'), changed_cache=MagicMock(return_value=True))]
        
        with patch.object(doctor, 'check_environment', return_value=mock_env), \
             patch('dt.fetch.utils.check_dvc'), \
             patch('dt.fetch.utils.collect_stages', return_value=[mock_stage]), \
             patch('dt.fetch.utils.get_import_info', return_value=None), \
             patch('dt.fetch._run_dvc_fetch') as mock_dvc_fetch:
            
            # Simulate successful network fetch
            mock_dvc_fetch.return_value = (True, 'Fetched via network')
            
            results = fetch.fetch(targets=['imported.csv.dvc'], network=True)
        
        assert len(results) == 1
        assert mock_dvc_fetch.called
        target, success, message = results[0]
        assert success is True


class TestRunDvcFetch:
    """Tests for _run_dvc_fetch function."""
    
    def test_successful_fetch(self, tmp_path):
        """Successful dvc fetch returns success."""
        dvc_file = tmp_path / 'data.dvc'
        dvc_file.write_text('outs:\n  - md5: abc\n')
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
            
            success, message = fetch._run_dvc_fetch(dvc_file)
        
        assert success is True
        assert 'network' in message
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd == ['dvc', 'fetch', str(dvc_file)]
    
    def test_failed_fetch(self, tmp_path):
        """Failed dvc fetch returns failure with error."""
        dvc_file = tmp_path / 'data.dvc'
        dvc_file.write_text('outs:\n  - md5: abc\n')
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout='', stderr='Connection refused')
            
            success, message = fetch._run_dvc_fetch(dvc_file)
        
        assert success is False
        assert 'Connection refused' in message
    
    def test_exception_during_fetch(self, tmp_path):
        """Exception during dvc fetch returns failure."""
        dvc_file = tmp_path / 'data.dvc'
        dvc_file.write_text('outs:\n  - md5: abc\n')
        
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = OSError("Command not found")
            
            success, message = fetch._run_dvc_fetch(dvc_file)
        
        assert success is False
        assert 'Command not found' in message


class TestFetchUrlImport:
    """Tests for _fetch_url_import function."""
    
    @pytest.fixture
    def dvc_project(self, tmp_path, monkeypatch):
        """Create a minimal DVC project with cache."""
        project = tmp_path / 'project'
        project.mkdir()
        (project / '.dvc').mkdir()
        
        cache = tmp_path / 'cache'
        (cache / 'files' / 'md5').mkdir(parents=True)
        
        monkeypatch.chdir(project)
        return {'project': project, 'cache': cache}
    
    def test_already_in_cache_returns_success(self, dvc_project):
        """URL import already in cache returns success without running dvc update."""
        cache = dvc_project['cache']
        md5 = 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6'
        
        # Create cache file (in files/md5 subdirectory)
        cache_dir = cache / 'files' / 'md5' / md5[:2]
        cache_dir.mkdir(parents=True)
        (cache_dir / md5[2:]).write_text('content')
        
        dvc_file = dvc_project['project'] / 'data.dvc'
        dvc_file.write_text(f'outs:\n  - md5: {md5}\n    path: data.csv\n')
        
        with patch('dt.fetch.utils.parse_dvc_file', return_value={'outs': [{'md5': md5}]}), \
             patch('dt.fetch.utils.get_cache_dir', return_value=cache / 'files' / 'md5'), \
             patch('dt.fetch.utils.get_url_import_info', return_value={'url': 's3://bucket/data.csv'}), \
             patch('subprocess.run') as mock_run:
            
            success, message = fetch._fetch_url_import(dvc_file)
        
        # Should not call subprocess since file is in cache
        mock_run.assert_not_called()
        assert success is True
        assert 'Already in cache' in message
    
    def test_successful_update(self, dvc_project):
        """Successful dvc update returns success."""
        dvc_file = dvc_project['project'] / 'data.dvc'
        dvc_file.write_text('deps:\n  - path: s3://bucket/data.csv\nouts:\n  - md5: abc\n')
        
        with patch('dt.fetch.utils.parse_dvc_file', return_value={'outs': [{'md5': 'abc'}]}), \
             patch('dt.fetch.utils.get_cache_dir', return_value=None), \
             patch('dt.fetch.utils.get_url_import_info', return_value={'url': 's3://bucket/data.csv', 'out': 'data.csv'}), \
             patch('subprocess.run') as mock_run:
            
            mock_run.return_value = MagicMock(returncode=0, stdout='Importing', stderr='')
            
            success, message = fetch._fetch_url_import(dvc_file)
        
        assert success is True
        assert 's3://bucket/data.csv' in message
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd == ['dvc', 'update', str(dvc_file)]
    
    def test_failed_update(self, dvc_project):
        """Failed dvc update returns failure with error."""
        dvc_file = dvc_project['project'] / 'data.dvc'
        dvc_file.write_text('deps:\n  - path: s3://bucket/data.csv\nouts:\n  - md5: abc\n')
        
        with patch('dt.fetch.utils.parse_dvc_file', return_value={'outs': [{'md5': 'abc'}]}), \
             patch('dt.fetch.utils.get_cache_dir', return_value=None), \
             patch('dt.fetch.utils.get_url_import_info', return_value={'url': 's3://bucket/data.csv'}), \
             patch('subprocess.run') as mock_run:
            
            mock_run.return_value = MagicMock(returncode=1, stdout='', stderr='Access denied')
            
            success, message = fetch._fetch_url_import(dvc_file)
        
        assert success is False
        assert 'dvc update failed' in message
    
    def test_source_not_found(self, dvc_project):
        """Source not accessible returns clear error."""
        dvc_file = dvc_project['project'] / 'data.dvc'
        dvc_file.write_text('deps:\n  - path: /nonexistent/path\nouts:\n  - md5: abc\n')
        
        with patch('dt.fetch.utils.parse_dvc_file', return_value={'outs': [{'md5': 'abc'}]}), \
             patch('dt.fetch.utils.get_cache_dir', return_value=None), \
             patch('dt.fetch.utils.get_url_import_info', return_value={'url': '/nonexistent/path'}), \
             patch('subprocess.run') as mock_run:
            
            mock_run.return_value = MagicMock(returncode=1, stdout='', stderr='No such file or directory')
            
            success, message = fetch._fetch_url_import(dvc_file)
        
        assert success is False
        assert 'Source not accessible' in message


class TestFetchWithUrlImport:
    """Tests for fetch() function handling URL imports."""
    
    @pytest.fixture
    def dvc_project(self, tmp_path, monkeypatch):
        """Create a minimal DVC project."""
        project = tmp_path / 'project'
        project.mkdir()
        (project / '.dvc').mkdir()
        
        monkeypatch.chdir(project)
        return project
    
    def test_url_import_stage_calls_fetch_url_import(self, dvc_project):
        """URL import stage triggers _fetch_url_import via stage-based flow."""
        from dt import doctor
        
        # Mock check_environment to return valid status
        mock_env = MagicMock()
        mock_env.in_git_repo = True
        mock_env.require_git_repo.return_value = None
        
        # Create a mock stage that is a URL import (is_import=True, is_repo_import=False)
        mock_stage = MagicMock()
        mock_stage.is_repo_import = False
        mock_stage.is_import = True  # URL imports have is_import=True
        mock_stage.addressing = 'data.dvc'
        mock_stage.path = str(dvc_project / 'data.dvc')
        mock_stage.outs = [MagicMock(use_cache=True, hash_info=MagicMock(value='abc'), changed_cache=MagicMock(return_value=True))]
        
        with patch.object(doctor, 'check_environment', return_value=mock_env), \
             patch('dt.fetch.utils.check_dvc'), \
             patch('dt.fetch.utils.collect_stages', return_value=[mock_stage]), \
             patch.object(doctor, 'check_network_connectivity', return_value=True), \
             patch('dt.fetch._fetch_url_import') as mock_fetch_url:
            
            mock_fetch_url.return_value = (True, "Fetched from s3://bucket/data.csv")
            
            results = fetch.fetch(targets=['data.dvc'])
        
        assert len(results) == 1
        assert mock_fetch_url.called
        target, success, message = results[0]
        assert success is True
        assert 's3://bucket' in message

