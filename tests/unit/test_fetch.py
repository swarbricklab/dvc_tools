"""Tests for dt fetch module internal functions.

Tests utility functions from fetch and import_data modules.
Organized by the new fetch flow:
1. Stage Categorization (categorize_stages → StageCategorization)
2. Fetch Plan (build_fetch_plan → FetchPlan with source→hashes)  
3. Plan Execution (fetch_from_plan)
4. Main fetch() function integration
5. Legacy/helper functions
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


# =============================================================================
# Stage Categorization Tests
# =============================================================================

class TestStageCategorization:
    """Tests for StageCategorization dataclass."""
    
    def test_empty_categorization(self):
        """Empty categorization has zero counts."""
        cat = fetch.StageCategorization()
        assert cat.total_stages == 0
        assert cat.repo_import_count == 0
        assert cat.url_imports == []
        assert cat.regular_stages == []
        assert cat.repo_imports == {}
    
    def test_total_stages_counts_all_types(self):
        """total_stages counts across all categories."""
        cat = fetch.StageCategorization()
        cat.url_imports = [MagicMock(), MagicMock()]
        cat.regular_stages = [MagicMock()]
        
        group = fetch.RepoImportGroup(url='http://example.com', rev='main')
        group.stages = [MagicMock(), MagicMock(), MagicMock()]
        cat.repo_imports['http://example.com'] = group
        
        assert cat.total_stages == 6  # 2 + 1 + 3
        assert cat.repo_import_count == 3
    
    def test_summary_lines_shows_categories(self):
        """summary_lines shows all categories with counts."""
        cat = fetch.StageCategorization()
        cat.url_imports = [MagicMock()]
        cat.regular_stages = [MagicMock(), MagicMock()]
        cat.has_local_remote = True
        cat.local_remote_name = 'myremote'
        
        lines = cat.summary_lines()
        
        assert any('URL imports: 1' in line for line in lines)
        assert any('Regular stages: 2' in line for line in lines)
        assert any('myremote' in line for line in lines)


class TestRepoImportGroup:
    """Tests for RepoImportGroup dataclass."""
    
    def test_short_name_extracts_repo_name(self):
        """short_name extracts last path component."""
        group = fetch.RepoImportGroup(url='https://github.com/org/myrepo.git', rev='main')
        assert group.short_name == 'myrepo'
    
    def test_short_name_handles_path(self):
        """short_name works with local paths."""
        group = fetch.RepoImportGroup(url='/path/to/project', rev=None)
        assert group.short_name == 'project'
    
    def test_add_stage_and_count(self):
        """add_stage adds to list and count updates."""
        group = fetch.RepoImportGroup(url='http://example.com', rev='v1')
        assert group.count == 0
        
        group.add_stage(MagicMock())
        group.add_stage(MagicMock())
        
        assert group.count == 2
        assert len(group.stages) == 2


class TestCategorizeStages:
    """Tests for categorize_stages function."""
    
    def test_empty_stages_returns_empty_categorization(self):
        """Empty stage list returns empty categorization."""
        result = fetch.categorize_stages([])
        assert result.total_stages == 0
    
    def test_regular_stage_categorized(self):
        """Non-import stage goes to regular_stages."""
        mock_stage = MagicMock()
        mock_stage.is_repo_import = False
        mock_stage.is_import = False
        
        result = fetch.categorize_stages([mock_stage])
        
        assert len(result.regular_stages) == 1
        assert result.url_imports == []
        assert result.repo_imports == {}
    
    def test_url_import_categorized(self):
        """URL import (is_import=True) goes to url_imports."""
        mock_stage = MagicMock()
        mock_stage.is_repo_import = False
        mock_stage.is_import = True
        
        result = fetch.categorize_stages([mock_stage])
        
        assert len(result.url_imports) == 1
        assert result.regular_stages == []
    
    def test_repo_import_grouped_by_url(self):
        """Repo imports are grouped by source URL."""
        stage1 = MagicMock()
        stage1.is_repo_import = True
        stage1.is_import = False
        stage1.path = '/project/a.dvc'
        
        stage2 = MagicMock()
        stage2.is_repo_import = True
        stage2.is_import = False
        stage2.path = '/project/b.dvc'
        
        with patch('dt.fetch.utils.get_import_info') as mock_info, \
             patch('dt.fetch.remote.find_local_remote_from_repo', return_value=None):
            mock_info.return_value = {'url': 'http://example.com/repo', 'rev': 'main'}
            
            result = fetch.categorize_stages([stage1, stage2])
        
        assert len(result.repo_imports) == 1
        assert 'http://example.com/repo' in result.repo_imports
        assert result.repo_imports['http://example.com/repo'].count == 2
    
    def test_checks_local_remote_for_regular_stages(self):
        """Regular stages check for locally accessible remote."""
        mock_stage = MagicMock()
        mock_stage.is_repo_import = False
        mock_stage.is_import = False
        
        with patch('dt.fetch.remote.list_remotes', return_value={'local': '/path/to/remote'}), \
             patch('dt.fetch.remote.find_local_remote', return_value=('local', '/path/to/remote')):
            
            result = fetch.categorize_stages([mock_stage])
        
        assert result.has_local_remote is True
        assert result.local_remote_name == 'local'


# =============================================================================
# Fetch Plan Tests
# =============================================================================

class TestSourceGroup:
    """Tests for SourceGroup dataclass."""
    
    def test_add_hash(self):
        """add_hash adds single hash."""
        group = fetch.SourceGroup(source_path=Path('/cache'), source_name='remote')
        group.add_hash('abc123')
        group.add_hash('def456')
        
        assert group.hashes == {'abc123', 'def456'}
    
    def test_add_hashes_bulk(self):
        """add_hashes_with_paths adds multiple hashes with paths."""
        group = fetch.SourceGroup(source_path=Path('/cache'), source_name='remote')
        group.add_hashes_with_paths([('hash1', 'path1'), ('hash2', 'path2'), ('hash3', 'path3')])
        
        assert len(group.hashes) == 3
        assert group.get_path_for_hash('hash1') == 'path1'
    
    def test_hashes_are_deduplicated(self):
        """Same hash added twice only stored once."""
        group = fetch.SourceGroup(source_path=Path('/cache'), source_name='remote')
        group.add_hash('abc123')
        group.add_hash('abc123')
        
        assert len(group.hashes) == 1


class TestFetchPlan:
    """Tests for FetchPlan dataclass."""
    
    def test_empty_plan(self):
        """Empty plan has zero hashes."""
        plan = fetch.FetchPlan()
        assert plan.total_hashes == 0
        assert plan.sources == {}
        assert plan.url_imports == []
        assert plan.no_source == []
    
    def test_add_source_creates_group(self):
        """add_source creates new SourceGroup."""
        plan = fetch.FetchPlan()
        group = plan.add_source(Path('/cache/a'), 'remote-a')
        
        assert isinstance(group, fetch.SourceGroup)
        assert group.source_name == 'remote-a'
        assert str(Path('/cache/a')) in plan.sources
    
    def test_add_source_returns_existing(self):
        """add_source returns existing group for same path."""
        plan = fetch.FetchPlan()
        group1 = plan.add_source(Path('/cache/a'), 'remote-a')
        group1.add_hash('hash1')
        
        group2 = plan.add_source(Path('/cache/a'), 'remote-a')
        
        assert group1 is group2
        assert 'hash1' in group2.hashes
    
    def test_total_hashes_across_sources(self):
        """total_hashes sums across all sources."""
        plan = fetch.FetchPlan()
        
        g1 = plan.add_source(Path('/cache/a'), 'a')
        g1.add_hashes_with_paths([('h1', 'p1'), ('h2', 'p2')])
        
        g2 = plan.add_source(Path('/cache/b'), 'b')
        g2.add_hashes_with_paths([('h3', 'p3'), ('h4', 'p4'), ('h5', 'p5')])
        
        assert plan.total_hashes == 5
    
    def test_summary_lines(self):
        """summary_lines shows all sources and special categories."""
        plan = fetch.FetchPlan()
        
        g = plan.add_source(Path('/cache/a'), 'remote-a')
        g.add_hashes_with_paths([('h1', 'p1'), ('h2', 'p2')])
        
        plan.url_imports = [MagicMock()]
        plan.no_source = [MagicMock(), MagicMock()]
        
        lines = plan.summary_lines()
        
        assert any('remote-a: 2 hashes' in line for line in lines)
        assert any('URL imports: 1' in line for line in lines)
        assert any('No local source: 2' in line for line in lines)


class TestBuildFetchPlan:
    """Tests for build_fetch_plan function."""
    
    def test_empty_categorization_returns_empty_plan(self):
        """Empty categorization produces empty plan."""
        cat = fetch.StageCategorization()
        plan = fetch.build_fetch_plan(cat)
        
        assert plan.total_hashes == 0
        assert plan.sources == {}
    
    def test_regular_stages_without_remote_go_to_no_source(self):
        """Regular stages without local remote go to no_source."""
        cat = fetch.StageCategorization()
        cat.regular_stages = [MagicMock()]
        cat.has_local_remote = False
        
        plan = fetch.build_fetch_plan(cat)
        
        assert len(plan.no_source) == 1
        assert plan.total_hashes == 0
    
    def test_regular_stages_with_remote_collect_hashes(self):
        """Regular stages with local remote have hashes collected."""
        mock_stage = MagicMock()
        mock_out = MagicMock()
        mock_out.use_cache = True
        mock_out.hash_info.value = 'abc123'
        mock_stage.outs = [mock_out]
        
        cat = fetch.StageCategorization()
        cat.regular_stages = [mock_stage]
        cat.has_local_remote = True
        cat.local_remote_name = 'local'
        
        with patch('dt.fetch.remote.list_remotes', return_value={'local': '/path'}), \
             patch('dt.fetch.remote.find_local_remote', return_value=('local', '/path')), \
             patch('dt.fetch._create_source_cache_db', return_value=None):
            
            plan = fetch.build_fetch_plan(cat)
        
        assert plan.total_hashes == 1
        assert len(plan.sources) == 1
    
    def test_repo_imports_without_cache_go_to_no_source(self):
        """Repo imports without local cache go to no_source."""
        mock_stage = MagicMock()
        
        group = fetch.RepoImportGroup(url='http://example.com', rev='main')
        group.stages = [mock_stage]
        group.has_local_cache = False
        
        cat = fetch.StageCategorization()
        cat.repo_imports['http://example.com'] = group
        
        plan = fetch.build_fetch_plan(cat)
        
        assert len(plan.no_source) == 1
    
    def test_url_imports_passed_through(self):
        """URL imports are passed through to plan."""
        cat = fetch.StageCategorization()
        cat.url_imports = [MagicMock(), MagicMock()]
        
        plan = fetch.build_fetch_plan(cat)
        
        assert len(plan.url_imports) == 2


class TestExpandDirHash:
    """Tests for _expand_dir_hash function."""
    
    def test_returns_empty_on_error(self):
        """Returns empty list if tree load fails."""
        mock_db = MagicMock()
        
        with patch('dvc_data.hashfile.tree.Tree.load', side_effect=Exception("Not found")):
            result = fetch._expand_dir_hash('abc123.dir', mock_db)
        
        assert result == []


class TestCollectHashesFromStage:
    """Tests for _collect_hashes_from_stage function."""
    
    def test_collects_hashes_from_outs(self):
        """Collects hash values and paths from stage outputs."""
        out1 = MagicMock()
        out1.use_cache = True
        out1.hash_info.value = 'hash1'
        out1.path = 'path1'
        
        out2 = MagicMock()
        out2.use_cache = True
        out2.hash_info.value = 'hash2'
        out2.path = 'path2'
        
        stage = MagicMock()
        stage.outs = [out1, out2]
        
        result = fetch._collect_hashes_from_stage(stage)
        
        # Returns list of (hash, path) tuples
        hashes = {h for h, p in result}
        assert hashes == {'hash1', 'hash2'}
    
    def test_skips_non_cached_outputs(self):
        """Skips outputs with use_cache=False."""
        out1 = MagicMock()
        out1.use_cache = True
        out1.hash_info.value = 'cached'
        out1.path = 'path1'
        
        out2 = MagicMock()
        out2.use_cache = False
        out2.hash_info.value = 'not_cached'
        out2.path = 'path2'
        
        stage = MagicMock()
        stage.outs = [out1, out2]
        
        result = fetch._collect_hashes_from_stage(stage)
        
        hashes = {h for h, p in result}
        assert hashes == {'cached'}
    
    def test_handles_missing_hash_info(self):
        """Handles outputs without hash_info."""
        out = MagicMock()
        out.use_cache = True
        out.hash_info = None
        
        stage = MagicMock()
        stage.outs = [out]
        
        result = fetch._collect_hashes_from_stage(stage)
        
        assert result == []


# =============================================================================
# Fetch Plan Execution Tests
# =============================================================================

class TestFetchFromPlan:
    """Tests for fetch_from_plan function."""
    
    def test_empty_plan_returns_empty(self):
        """Empty plan returns empty results."""
        plan = fetch.FetchPlan()
        results = fetch.fetch_from_plan(plan)
        
        assert results == []
    
    def test_no_source_stages_fail_without_network(self):
        """Stages in no_source fail without --network."""
        mock_stage = MagicMock()
        mock_stage.addressing = 'data.dvc'
        
        plan = fetch.FetchPlan()
        plan.no_source = [mock_stage]
        
        results = fetch.fetch_from_plan(plan, network=False)
        
        assert len(results) == 1
        target, success, msg = results[0]
        assert success is False
        assert 'No local source' in msg
    
    def test_no_source_stages_use_dvc_fetch_with_network(self):
        """Stages in no_source use dvc fetch with --network."""
        mock_stage = MagicMock()
        mock_stage.addressing = 'data.dvc'
        
        plan = fetch.FetchPlan()
        plan.no_source = [mock_stage]
        
        with patch('dt.fetch._run_dvc_fetch', return_value=(True, 'Fetched')) as mock_fetch:
            results = fetch.fetch_from_plan(plan, network=True)
        
        assert mock_fetch.called
        assert len(results) == 1
        target, success, msg = results[0]
        assert success is True
    
    def test_url_imports_call_fetch_url_import_stage(self):
        """URL imports are processed via _fetch_url_import_stage."""
        mock_stage = MagicMock()
        
        plan = fetch.FetchPlan()
        plan.url_imports = [mock_stage]
        
        with patch('dt.fetch._fetch_url_import_stage', return_value=('data.dvc', True, 'OK')) as mock_fetch:
            results = fetch.fetch_from_plan(plan)
        
        assert mock_fetch.called
        assert len(results) == 1


# =============================================================================
# Main Fetch Function Tests
# =============================================================================

# Note: TestFetch class is defined in the Legacy section below with all tests


# =============================================================================
# Legacy/Helper Function Tests (populate_cache_file, build_dir_manifest, etc.)
# =============================================================================

class TestPopulateCacheFile:
    """Tests for populate_cache_file function."""
    
    @pytest.fixture
    def cache_dirs(self, tmp_path):
        """Create source and destination cache directories."""
        source = tmp_path / 'source_cache'
        dest = tmp_path / 'dest_cache'
        (source / 'files' / 'md5').mkdir(parents=True)
        dest.mkdir(parents=True)
        return {'source': str(source), 'dest': str(dest)}
    
    def test_single_file_cached_via_hardlink(self, cache_dirs):
        """Single file is cached using hardlink."""
        md5 = 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6'
        
        source_dir = Path(cache_dirs['source']) / 'files' / 'md5' / md5[:2]
        source_dir.mkdir(parents=True)
        (source_dir / md5[2:]).write_text('test content')
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=True,
        )
        
        assert result is True
        dest_file = Path(cache_dirs['dest']) / 'files' / 'md5' / md5[:2] / md5[2:]
        assert dest_file.exists()
        assert dest_file.read_text() == 'test content'
    
    def test_already_exists_returns_false(self, cache_dirs):
        """File already in destination returns False."""
        md5 = 'deadbeefcafe1234567890abcdef0123'
        
        source_dir = Path(cache_dirs['source']) / 'files' / 'md5' / md5[:2]
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / md5[2:]).write_text('source content')
        
        dest_dir = Path(cache_dirs['dest']) / 'files' / 'md5' / md5[:2]
        dest_dir.mkdir(parents=True, exist_ok=True)
        (dest_dir / md5[2:]).write_text('dest content')
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=True,
        )
        
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
        """Falls back to DVC v2 layout for source."""
        md5 = 'v2layout1234567890abcdef12345678'
        
        source_dir = Path(cache_dirs['source']) / md5[:2]
        source_dir.mkdir(parents=True)
        (source_dir / md5[2:]).write_text('v2 layout content')
        
        result = import_data.populate_cache_file(
            md5=md5,
            source_cache=cache_dirs['source'],
            dest_cache=cache_dirs['dest'],
            use_v3_layout=True,
        )
        
        assert result is True
        dest_file = Path(cache_dirs['dest']) / 'files' / 'md5' / md5[:2] / md5[2:]
        assert dest_file.exists()


class TestBuildDirManifest:
    """Tests for build_dir_manifest function."""
    
    def test_builds_correct_format(self):
        """Test manifest is built with correct DVC JSON format."""
        entries = [
            {'md5': 'aaaa', 'relpath': 'b.txt'},
            {'md5': 'bbbb', 'relpath': 'a.txt'},
        ]
        
        content = import_data.build_dir_manifest(entries)
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

    def test_dry_mode_returns_empty_and_prints_summary(self, dvc_project, capsys):
        """Dry mode prints categorization but doesn't fetch."""
        from dt import doctor
        project, cache = dvc_project
        
        mock_stage = MagicMock()
        mock_stage.is_repo_import = False
        mock_stage.is_import = False
        mock_stage.addressing = 'data.dvc'
        
        mock_env = MagicMock()
        mock_env.in_git_repo = True
        mock_env.require_git_repo.return_value = None
        
        with patch.object(doctor, 'check_environment', return_value=mock_env), \
             patch('dt.fetch.utils.check_dvc'), \
             patch('dt.fetch.utils.collect_stages', return_value=[mock_stage]), \
             patch('dt.fetch.remote.list_remotes', return_value={}), \
             patch('dt.fetch.remote.find_local_remote', return_value=None):
            
            results = fetch.fetch(dry=True)
        
        assert results == []
        captured = capsys.readouterr()
        assert 'Stage categorization' in captured.out
    
    def test_filter_imports_only(self, dvc_project):
        """--imports filter only processes repo imports."""
        from dt import doctor
        project, cache = dvc_project
        
        regular = MagicMock()
        regular.is_repo_import = False
        regular.is_import = False
        regular.addressing = 'regular.dvc'
        
        repo_import = MagicMock()
        repo_import.is_repo_import = True
        repo_import.is_import = False
        repo_import.addressing = 'imported.dvc'
        repo_import.path = str(project / 'imported.dvc')
        repo_import.outs = []
        
        mock_env = MagicMock()
        mock_env.in_git_repo = True
        mock_env.require_git_repo.return_value = None
        
        with patch.object(doctor, 'check_environment', return_value=mock_env), \
             patch('dt.fetch.utils.check_dvc'), \
             patch('dt.fetch.utils.collect_stages', return_value=[regular, repo_import]), \
             patch('dt.fetch.utils.get_import_info', return_value=None), \
             patch('dt.fetch.remote.list_remotes', return_value={}), \
             patch('dt.fetch.remote.find_local_remote', return_value=None):
            
            results = fetch.fetch(imports=True)
        
        # Only repo import processed (and it fails due to no local cache)
        assert len(results) == 1
        target, success, msg = results[0]
        assert target == 'imported.dvc'
    
    def test_filter_regular_only(self, dvc_project):
        """--regular filter only processes regular stages."""
        from dt import doctor
        project, cache = dvc_project
        
        regular = MagicMock()
        regular.is_repo_import = False
        regular.is_import = False
        regular.addressing = 'regular.dvc'
        regular.outs = []
        
        url_import = MagicMock()
        url_import.is_repo_import = False
        url_import.is_import = True
        url_import.addressing = 'url.dvc'
        
        mock_env = MagicMock()
        mock_env.in_git_repo = True
        mock_env.require_git_repo.return_value = None
        
        with patch.object(doctor, 'check_environment', return_value=mock_env), \
             patch('dt.fetch.utils.check_dvc'), \
             patch('dt.fetch.utils.collect_stages', return_value=[regular, url_import]), \
             patch('dt.fetch.remote.list_remotes', return_value={}), \
             patch('dt.fetch.remote.find_local_remote', return_value=None):
            
            results = fetch.fetch(regular=True)
        
        # Only regular stage processed (fails due to no remote)
        assert len(results) == 1
        target, success, msg = results[0]
        assert target == 'regular.dvc'


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

