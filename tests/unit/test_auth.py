"""Unit tests for dt.auth module.

Tests endpoint discovery and classification for ``dt auth list``.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

from dt.auth import (
    Endpoint,
    ENDPOINT_TYPES,
    classify_url,
    discover_endpoints,
    format_endpoints,
    format_endpoints_json,
    _discover_dt_config,
    _discover_dvc_remotes,
    _discover_git_remotes,
    _discover_import_sources,
    _short_repo_name,
    _apply_type_filter,
    _merge_children,
)


# =============================================================================
# classify_url tests
# =============================================================================

class TestClassifyUrl:
    """Tests for classify_url."""

    def test_absolute_path(self):
        assert classify_url('/g/data/a56/dvc_cache') == 'filesystem'

    def test_file_url(self):
        assert classify_url('file:///g/data/a56/dvc_cache') == 'filesystem'

    def test_ssh_url(self):
        assert classify_url('ssh://gadi.nci.org.au/g/data/a56/remote') == 'ssh'

    def test_ssh_url_with_user(self):
        assert classify_url('ssh://user@gadi.nci.org.au/g/data/remote') == 'ssh'

    def test_s3_url(self):
        assert classify_url('s3://my-bucket/dvc-remote') == 's3'

    def test_gs_url(self):
        assert classify_url('gs://my-bucket/dvc-remote') == 'gs'

    def test_https_url(self):
        assert classify_url('https://example.com/data') == 'http'

    def test_http_url(self):
        assert classify_url('http://example.com/data') == 'http'

    def test_scp_style_git(self):
        assert classify_url('git@github.com:org/repo.git') == 'git'

    def test_scp_style_ssh(self):
        assert classify_url('user@host:/some/path') == 'ssh'

    def test_whitespace_stripped(self):
        assert classify_url('  s3://bucket/key  ') == 's3'


# =============================================================================
# Endpoint tests
# =============================================================================

class TestEndpoint:
    """Tests for the Endpoint dataclass."""

    def test_key_deduplication(self):
        a = Endpoint(type='filesystem', url='/data', source='config')
        b = Endpoint(type='filesystem', url='/data', source='other')
        assert a.key == b.key

    def test_key_differs_by_type(self):
        a = Endpoint(type='filesystem', url='/data', source='config')
        b = Endpoint(type='ssh', url='/data', source='config')
        assert a.key != b.key

    def test_key_differs_by_url(self):
        a = Endpoint(type='filesystem', url='/data1', source='config')
        b = Endpoint(type='filesystem', url='/data2', source='config')
        assert a.key != b.key

    def test_to_dict_minimal(self):
        ep = Endpoint(type='s3', url='s3://bucket', source='remote')
        d = ep.to_dict()
        assert d == {'type': 's3', 'url': 's3://bucket', 'source': 'remote'}

    def test_to_dict_with_local_path(self):
        ep = Endpoint(type='ssh', url='ssh://host/path', source='r',
                      local_path='/path')
        d = ep.to_dict()
        assert d['local_path'] == '/path'

    def test_to_dict_with_children(self):
        child = Endpoint(type='ssh', url='ssh://h/p', source='child')
        ep = Endpoint(type='git', url='git@github.com:o/r.git', source='import',
                      children=[child])
        d = ep.to_dict()
        assert len(d['children']) == 1
        assert d['children'][0]['type'] == 'ssh'

    def test_to_dict_omits_empty_local_path(self):
        ep = Endpoint(type='filesystem', url='/data', source='config')
        d = ep.to_dict()
        assert 'local_path' not in d

    def test_to_dict_omits_empty_children(self):
        ep = Endpoint(type='filesystem', url='/data', source='config')
        d = ep.to_dict()
        assert 'children' not in d


# =============================================================================
# _discover_dt_config tests
# =============================================================================

class TestDiscoverDtConfig:
    """Tests for _discover_dt_config."""

    @patch('dt.auth.cfg.get_value')
    def test_cache_root_only(self, mock_get):
        mock_get.side_effect = lambda k: '/cache' if k == 'cache.root' else None
        eps = _discover_dt_config()
        assert len(eps) == 1
        assert eps[0].type == 'filesystem'
        assert eps[0].url == '/cache'
        assert eps[0].source == 'cache.root'

    @patch('dt.auth.utils.get_project_name', return_value='myproj')
    @patch('dt.auth.cfg.get_value')
    def test_remote_root_appends_project_name(self, mock_get, _):
        mock_get.side_effect = lambda k: '/remote' if k == 'remote.root' else None
        eps = _discover_dt_config()
        assert len(eps) == 1
        assert eps[0].url == '/remote/myproj'
        assert eps[0].source == 'remote.root'

    @patch('dt.auth.utils.get_project_name', return_value='proj')
    @patch('dt.auth.cfg.get_value')
    def test_both_cache_and_remote(self, mock_get, _):
        def side_effect(k):
            return {
                'cache.root': '/cache',
                'remote.root': '/remote',
            }.get(k)
        mock_get.side_effect = side_effect
        eps = _discover_dt_config()
        assert len(eps) == 2
        assert eps[0].url == '/cache'
        assert eps[1].url == '/remote/proj'

    @patch('dt.auth.cfg.get_value', return_value=None)
    def test_nothing_configured(self, _):
        eps = _discover_dt_config()
        assert eps == []


# =============================================================================
# _discover_dvc_remotes tests
# =============================================================================

class TestDiscoverDvcRemotes:
    """Tests for _discover_dvc_remotes."""

    @patch('dt.auth.remote_mod.list_remotes')
    def test_uses_project_scope(self, mock_list):
        """Ensures _discover_dvc_remotes passes project_only=True."""
        mock_list.return_value = []
        _discover_dvc_remotes()
        mock_list.assert_called_once_with(None, project_only=True)

    @patch('dt.auth.remote_mod.list_remotes')
    def test_ssh_remote_with_local_path(self, mock_list):
        mock_list.return_value = [
            ('origin', 'ssh://gadi.nci.org.au/g/data/remote', True),
        ]
        with patch('dt.auth.remote_mod.extract_local_path',
                   return_value='/g/data/remote'):
            eps = _discover_dvc_remotes()

        assert len(eps) == 1
        assert eps[0].type == 'ssh'
        assert eps[0].local_path == '/g/data/remote'
        assert '(default)' in eps[0].source

    @patch('dt.auth.remote_mod.list_remotes')
    def test_s3_remote(self, mock_list):
        mock_list.return_value = [
            ('cloud', 's3://my-r2-bucket/dvc', False),
        ]
        eps = _discover_dvc_remotes()
        assert len(eps) == 1
        assert eps[0].type == 's3'
        assert eps[0].url == 's3://my-r2-bucket/dvc'

    @patch('dt.auth.remote_mod.list_remotes')
    def test_local_remote(self, mock_list):
        mock_list.return_value = [
            ('local', '/g/data/remote', False),
        ]
        eps = _discover_dvc_remotes()
        assert len(eps) == 1
        assert eps[0].type == 'filesystem'

    @patch('dt.auth.remote_mod.list_remotes')
    def test_skips_empty_url(self, mock_list):
        mock_list.return_value = [('bad', '', False)]
        eps = _discover_dvc_remotes()
        assert eps == []

    @patch('dt.auth.remote_mod.list_remotes')
    def test_multiple_remotes(self, mock_list):
        mock_list.return_value = [
            ('origin', 'ssh://host/path', True),
            ('cloud', 's3://bucket/key', False),
            ('local', '/local/path', False),
        ]
        with patch('dt.auth.remote_mod.extract_local_path', return_value=None):
            eps = _discover_dvc_remotes()

        assert len(eps) == 3
        types = {e.type for e in eps}
        assert types == {'ssh', 's3', 'filesystem'}


# =============================================================================
# _discover_git_remotes tests
# =============================================================================

class TestDiscoverGitRemotes:
    """Tests for _discover_git_remotes."""

    @patch('subprocess.run')
    def test_single_origin(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='origin\tgit@github.com:org/repo.git (fetch)\n'
                   'origin\tgit@github.com:org/repo.git (push)\n',
        )
        eps = _discover_git_remotes()
        assert len(eps) == 1
        assert eps[0].type == 'git'
        assert eps[0].url == 'git@github.com:org/repo.git'
        assert eps[0].source == "git remote 'origin'"

    @patch('subprocess.run')
    def test_multiple_git_remotes(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='origin\tgit@github.com:org/repo.git (fetch)\n'
                   'origin\tgit@github.com:org/repo.git (push)\n'
                   'upstream\thttps://github.com/other/repo.git (fetch)\n'
                   'upstream\thttps://github.com/other/repo.git (push)\n',
        )
        eps = _discover_git_remotes()
        assert len(eps) == 2
        types = {e.type for e in eps}
        assert types == {'git', 'http'}

    @patch('subprocess.run')
    def test_handles_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout='')
        eps = _discover_git_remotes()
        assert eps == []

    @patch('subprocess.run')
    def test_deduplicates_fetch_push(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='origin\tgit@github.com:org/repo.git (fetch)\n'
                   'origin\tgit@github.com:org/repo.git (push)\n',
        )
        eps = _discover_git_remotes()
        assert len(eps) == 1


# =============================================================================
# _discover_import_sources tests
# =============================================================================

class TestDiscoverImportSources:
    """Tests for _discover_import_sources."""

    def test_source_repo_uses_project_scope(self, tmp_path):
        """Ensures source repo remote discovery passes project_only=True."""
        import yaml
        (tmp_path / 'imp.dvc').write_text(yaml.dump({
            'deps': [{'path': 'f', 'repo': {'url': 'git@github.com:org/r.git'}}],
            'outs': [{'md5': '111', 'path': 'imp'}],
        }))
        with patch('dt.auth.remote_mod.list_remotes_from_repo',
                   return_value=[]) as mock_list:
            _discover_import_sources(repo_path=tmp_path)
        mock_list.assert_called_once_with(
            'git@github.com:org/r.git', project_only=True)

    def test_finds_import_urls(self, tmp_path):
        """Finds unique import source URLs from .dvc files."""
        dvc_content = {
            'md5': 'abc123',
            'deps': [{'path': 'data/file.csv',
                       'repo': {'url': 'git@github.com:org/data.git'}}],
            'outs': [{'md5': 'def456', 'path': 'file.csv'}],
        }
        import yaml
        dvc_file = tmp_path / 'file.csv.dvc'
        dvc_file.write_text(yaml.dump(dvc_content))

        with patch('dt.auth.remote_mod.list_remotes_from_repo', return_value=[]):
            eps = _discover_import_sources(repo_path=tmp_path)

        assert len(eps) == 1
        assert eps[0].type == 'git'
        assert eps[0].url == 'git@github.com:org/data.git'
        assert 'import source' in eps[0].source

    def test_deduplicates_same_url(self, tmp_path):
        """Multiple .dvc files importing from the same repo produce one endpoint."""
        import yaml
        for name in ['a.dvc', 'b.dvc']:
            (tmp_path / name).write_text(yaml.dump({
                'deps': [{'path': 'x', 'repo': {'url': 'git@github.com:org/data.git'}}],
                'outs': [{'md5': '111', 'path': name.replace('.dvc', '')}],
            }))

        with patch('dt.auth.remote_mod.list_remotes_from_repo', return_value=[]):
            eps = _discover_import_sources(repo_path=tmp_path)

        assert len(eps) == 1

    def test_discovers_source_repo_remotes(self, tmp_path):
        """Discovers DVC remotes of the import source repo as children."""
        import yaml
        (tmp_path / 'imp.dvc').write_text(yaml.dump({
            'deps': [{'path': 'f', 'repo': {'url': 'git@github.com:org/data.git'}}],
            'outs': [{'md5': '111', 'path': 'imp'}],
        }))

        source_remotes = [
            ('origin', 'ssh://gadi/g/data/remote/data', True),
            ('cloud', 's3://bucket/data', False),
        ]
        with patch('dt.auth.remote_mod.list_remotes_from_repo',
                   return_value=source_remotes):
            with patch('dt.auth.remote_mod.extract_local_path', return_value=None):
                eps = _discover_import_sources(repo_path=tmp_path)

        assert len(eps) == 1
        assert len(eps[0].children) == 2
        child_types = {c.type for c in eps[0].children}
        assert child_types == {'ssh', 's3'}

    def test_skips_dvc_directory(self, tmp_path):
        """Ignores .dvc files inside the .dvc directory."""
        import yaml
        dvc_dir = tmp_path / '.dvc'
        dvc_dir.mkdir()
        (dvc_dir / 'something.dvc').write_text(yaml.dump({
            'deps': [{'path': 'x', 'repo': {'url': 'git@github.com:org/r.git'}}],
            'outs': [{'md5': '111', 'path': 'x'}],
        }))

        with patch('dt.auth.remote_mod.list_remotes_from_repo', return_value=[]):
            eps = _discover_import_sources(repo_path=tmp_path)

        assert eps == []

    def test_skips_non_import_dvc_files(self, tmp_path):
        """Regular .dvc files (no deps.repo) are ignored."""
        import yaml
        (tmp_path / 'data.txt.dvc').write_text(yaml.dump({
            'outs': [{'md5': 'abc', 'path': 'data.txt'}],
        }))

        with patch('dt.auth.remote_mod.list_remotes_from_repo', return_value=[]):
            eps = _discover_import_sources(repo_path=tmp_path)

        assert eps == []

    def test_handles_malformed_dvc_files(self, tmp_path):
        """Malformed .dvc files are skipped gracefully."""
        (tmp_path / 'bad.dvc').write_text('not: [valid: yaml: {')

        eps = _discover_import_sources(repo_path=tmp_path)
        assert eps == []

    def test_child_ssh_gets_local_path(self, tmp_path):
        """SSH children get local_path when host is local."""
        import yaml
        (tmp_path / 'imp.dvc').write_text(yaml.dump({
            'deps': [{'path': 'f', 'repo': {'url': 'git@github.com:org/r.git'}}],
            'outs': [{'md5': '111', 'path': 'imp'}],
        }))
        with patch('dt.auth.remote_mod.list_remotes_from_repo',
                   return_value=[('origin', 'ssh://host/path', True)]):
            with patch('dt.auth.remote_mod.extract_local_path',
                       return_value='/path'):
                eps = _discover_import_sources(repo_path=tmp_path)

        assert eps[0].children[0].local_path == '/path'


# =============================================================================
# discover_endpoints (integration) tests
# =============================================================================

class TestDiscoverEndpoints:
    """Tests for the top-level discover_endpoints function."""

    def test_prints_scanning_message(self, capsys):
        """Always prints scanning message even without verbose."""
        with patch('dt.auth._discover_import_sources', return_value=[]), \
             patch('dt.auth._discover_git_remotes', return_value=[]), \
             patch('dt.auth._discover_dvc_remotes', return_value=[]), \
             patch('dt.auth._discover_dt_config', return_value=[]):
            discover_endpoints()
        captured = capsys.readouterr()
        assert 'Scanning endpoints for project' in captured.out

    def test_verbose_shows_step_counts(self, capsys):
        """Verbose mode shows per-step endpoint counts."""
        dvc_eps = [Endpoint(type='ssh', url='ssh://h/p', source='remote')]
        with patch('dt.auth._discover_import_sources', return_value=[]), \
             patch('dt.auth._discover_git_remotes', return_value=[]), \
             patch('dt.auth._discover_dvc_remotes', return_value=dvc_eps), \
             patch('dt.auth._discover_dt_config', return_value=[]):
            discover_endpoints(verbose=True)
        captured = capsys.readouterr()
        assert 'DVC remotes (project scope): 1 endpoint(s)' in captured.out

    @patch('dt.auth._discover_import_sources', return_value=[])
    @patch('dt.auth._discover_git_remotes', return_value=[])
    @patch('dt.auth._discover_dvc_remotes', return_value=[])
    @patch('dt.auth._discover_dt_config')
    def test_deduplicates(self, mock_dt, *_):
        """Duplicate (type, url) pairs are merged."""
        mock_dt.return_value = [
            Endpoint(type='filesystem', url='/cache', source='cache.root'),
            Endpoint(type='filesystem', url='/cache', source='other'),
        ]
        eps = discover_endpoints()
        assert len(eps) == 1
        assert eps[0].source == 'cache.root'  # first wins

    @patch('dt.auth._discover_import_sources', return_value=[])
    @patch('dt.auth._discover_git_remotes', return_value=[])
    @patch('dt.auth._discover_dvc_remotes')
    @patch('dt.auth._discover_dt_config', return_value=[])
    def test_type_filter(self, _, mock_dvc, *__):
        """Type filter keeps only matching endpoints."""
        mock_dvc.return_value = [
            Endpoint(type='ssh', url='ssh://host/path', source='remote'),
            Endpoint(type='s3', url='s3://bucket', source='cloud'),
        ]
        eps = discover_endpoints(type_filter={'s3'})
        assert len(eps) == 1
        assert eps[0].type == 's3'

    @patch('dt.auth._discover_import_sources', return_value=[])
    @patch('dt.auth._discover_git_remotes', return_value=[])
    @patch('dt.auth._discover_dvc_remotes')
    @patch('dt.auth._discover_dt_config', return_value=[])
    def test_type_filter_promotes_children(self, _, mock_dvc, *__):
        """When parent is filtered out, matching children are promoted."""
        child = Endpoint(type='ssh', url='ssh://host/p', source='child remote')
        parent = Endpoint(type='git', url='git@github.com:o/r.git',
                          source='import', children=[child])
        mock_dvc.return_value = [parent]

        eps = discover_endpoints(type_filter={'ssh'})
        assert len(eps) == 1
        assert eps[0].type == 'ssh'
        assert 'via' in eps[0].source

    @patch('dt.auth._discover_import_sources', return_value=[])
    @patch('dt.auth._discover_git_remotes', return_value=[])
    @patch('dt.auth._discover_dvc_remotes', return_value=[])
    @patch('dt.auth._discover_dt_config', return_value=[])
    def test_empty_project(self, *_):
        """Empty project returns empty list."""
        eps = discover_endpoints()
        assert eps == []

    @patch('dt.auth._discover_import_sources')
    @patch('dt.auth._discover_git_remotes', return_value=[])
    @patch('dt.auth._discover_dvc_remotes')
    @patch('dt.auth._discover_dt_config', return_value=[])
    def test_children_merged_on_dedup(self, _, mock_dvc, __, mock_imports):
        """When parent endpoints are deduplicated, children are merged."""
        child_a = Endpoint(type='ssh', url='ssh://host/a', source='from dvc')
        child_b = Endpoint(type='s3', url='s3://bucket', source='from import')

        mock_dvc.return_value = [
            Endpoint(type='git', url='git@github.com:o/r.git',
                     source='DVC remote', children=[child_a]),
        ]
        mock_imports.return_value = [
            Endpoint(type='git', url='git@github.com:o/r.git',
                     source='import', children=[child_b]),
        ]

        eps = discover_endpoints()
        git_eps = [e for e in eps if e.type == 'git']
        assert len(git_eps) == 1
        assert len(git_eps[0].children) == 2


# =============================================================================
# format tests
# =============================================================================

class TestFormat:
    """Tests for formatting functions."""

    @patch('dt.auth.utils.get_project_name', return_value='test-proj')
    def test_format_endpoints_groups_by_type(self, _):
        eps = [
            Endpoint(type='filesystem', url='/cache', source='cache.root'),
            Endpoint(type='s3', url='s3://bucket', source='cloud'),
        ]
        output = format_endpoints(eps)
        assert 'test-proj' in output
        assert 'filesystem' in output
        assert 's3' in output
        assert '/cache' in output

    @patch('dt.auth.utils.get_project_name', return_value='test-proj')
    def test_format_endpoints_shows_local_equivalent(self, _):
        eps = [
            Endpoint(type='ssh', url='ssh://host/path', source='remote',
                     local_path='/path'),
        ]
        output = format_endpoints(eps)
        assert 'local equivalent' in output

    @patch('dt.auth.utils.get_project_name', return_value='test-proj')
    def test_format_endpoints_shows_children(self, _):
        child = Endpoint(type='ssh', url='ssh://h/p', source='child')
        ep = Endpoint(type='git', url='git@github.com:o/r.git',
                      source='import', children=[child])
        output = format_endpoints([ep])
        assert '→ remote:' in output

    @patch('dt.auth.utils.get_project_name', return_value='test-proj')
    def test_format_endpoints_empty(self, _):
        output = format_endpoints([])
        assert 'no endpoints discovered' in output

    def test_format_json(self):
        eps = [
            Endpoint(type='s3', url='s3://bucket', source='cloud'),
        ]
        output = format_endpoints_json(eps)
        data = json.loads(output)
        assert len(data) == 1
        assert data[0]['type'] == 's3'

    def test_format_json_roundtrips_children(self):
        child = Endpoint(type='ssh', url='ssh://h/p', source='child')
        ep = Endpoint(type='git', url='git@github.com:o/r.git',
                      source='import', children=[child])
        output = format_endpoints_json([ep])
        data = json.loads(output)
        assert len(data[0]['children']) == 1


# =============================================================================
# _apply_type_filter tests
# =============================================================================

class TestApplyTypeFilter:
    """Tests for _apply_type_filter."""

    def test_keeps_matching_type(self):
        eps = [
            Endpoint(type='filesystem', url='/x', source='a'),
            Endpoint(type='ssh', url='ssh://h/p', source='b'),
        ]
        result = _apply_type_filter(eps, {'filesystem'})
        assert len(result) == 1
        assert result[0].type == 'filesystem'

    def test_promotes_matching_children(self):
        child = Endpoint(type='s3', url='s3://b', source='child')
        parent = Endpoint(type='git', url='git@g:o/r.git', source='import',
                          children=[child])
        result = _apply_type_filter([parent], {'s3'})
        assert len(result) == 1
        assert result[0].type == 's3'
        assert 'via' in result[0].source

    def test_filters_children_of_kept_parent(self):
        child_s3 = Endpoint(type='s3', url='s3://b', source='c1')
        child_ssh = Endpoint(type='ssh', url='ssh://h/p', source='c2')
        parent = Endpoint(type='git', url='git@g:o/r.git', source='import',
                          children=[child_s3, child_ssh])
        result = _apply_type_filter([parent], {'git', 's3'})
        assert len(result) == 1
        assert result[0].type == 'git'
        assert len(result[0].children) == 1
        assert result[0].children[0].type == 's3'

    def test_multiple_types(self):
        eps = [
            Endpoint(type='filesystem', url='/x', source='a'),
            Endpoint(type='ssh', url='ssh://h/p', source='b'),
            Endpoint(type='s3', url='s3://b', source='c'),
        ]
        result = _apply_type_filter(eps, {'filesystem', 's3'})
        assert len(result) == 2
        types = {e.type for e in result}
        assert types == {'filesystem', 's3'}


# =============================================================================
# Helper tests
# =============================================================================

class TestHelpers:
    """Tests for helper functions."""

    def test_short_repo_name_git_url(self):
        assert _short_repo_name('git@github.com:org/repo.git') == 'repo'

    def test_short_repo_name_https(self):
        assert _short_repo_name('https://github.com/org/repo') == 'repo'

    def test_short_repo_name_path(self):
        assert _short_repo_name('/local/path/myrepo') == 'myrepo'

    def test_short_repo_name_trailing_slash(self):
        assert _short_repo_name('https://github.com/org/repo/') == 'repo'

    def test_short_repo_name_empty(self):
        assert _short_repo_name('') == 'unknown'

    def test_merge_children_adds_new(self):
        target = Endpoint(type='git', url='u', source='a',
                          children=[Endpoint(type='ssh', url='s1', source='c1')])
        source = Endpoint(type='git', url='u', source='b',
                          children=[Endpoint(type='s3', url='s2', source='c2')])
        _merge_children(target, source)
        assert len(target.children) == 2

    def test_merge_children_skips_duplicates(self):
        child = Endpoint(type='ssh', url='s1', source='c1')
        target = Endpoint(type='git', url='u', source='a', children=[child])
        source = Endpoint(type='git', url='u', source='b',
                          children=[Endpoint(type='ssh', url='s1', source='c1')])
        _merge_children(target, source)
        assert len(target.children) == 1


# =============================================================================
# ENDPOINT_TYPES constant
# =============================================================================

class TestEndpointTypes:
    """Tests for the ENDPOINT_TYPES constant."""

    def test_all_types_present(self):
        expected = {'filesystem', 'ssh', 's3', 'gs', 'http', 'git'}
        assert ENDPOINT_TYPES == expected

    def test_is_frozen(self):
        assert isinstance(ENDPOINT_TYPES, frozenset)
