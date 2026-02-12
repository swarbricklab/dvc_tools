"""Unit tests for dt.auth module.

Tests endpoint discovery and classification for ``dt auth list``.
"""

import json
import subprocess
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

from dt.auth import (
    Endpoint,
    ENDPOINT_TYPES,
    STATUS_PASS,
    STATUS_FAIL,
    STATUS_WARN,
    STATUS_SKIP,
    AccessRequest,
    CheckResult,
    Identity,
    classify_url,
    discover_endpoints,
    check_endpoints,
    generate_request,
    get_identities,
    detect_identities,
    compare_identities,
    save_detected_identities,
    format_endpoints,
    format_endpoints_json,
    format_check_results,
    format_check_results_json,
    format_request_text,
    format_request_markdown,
    format_request_json,
    format_identities,
    format_identities_json,
    format_whoami_comparison,
    send_request,
    send_request_slack,
    send_request_email,
    _format_slack_blocks,
    _detect_github_user,
    _detect_github_teams,
    _detect_gcp_email,
    _detect_aws_identity,
    _discover_dt_config,
    _discover_dvc_remotes,
    _discover_git_remotes,
    _discover_import_sources,
    _check_filesystem,
    _check_ssh,
    _check_git,
    _check_http,
    _check_s3,
    _check_gs,
    _check_dvc_remote,
    _check_dvc_remote_impl,
    _try_check,
    _extract_remote_name,
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

    @patch('dt.auth.cfg.get_value', return_value=None)
    @patch('dt.auth.utils.get_cache_dir')
    def test_dvc_cache_dir(self, mock_cache, _):
        """DVC cache directory is discovered via utils.get_cache_dir()."""
        mock_cache.return_value = Path('/data/cache/files/md5')
        eps = _discover_dt_config()
        assert len(eps) == 1
        assert eps[0].type == 'filesystem'
        assert eps[0].url == '/data/cache'  # grandparent of files/md5
        assert 'DVC cache' in eps[0].source

    @patch('dt.auth.cfg.get_value')
    @patch('dt.auth.utils.get_cache_dir', return_value=None)
    def test_fallback_to_dt_config_cache_root(self, _, mock_get):
        """Falls back to dt config cache.root when DVC cache unavailable."""
        mock_get.side_effect = lambda k: '/cache' if k == 'cache.root' else None
        eps = _discover_dt_config()
        assert len(eps) == 1
        assert eps[0].url == '/cache'
        assert 'dt config' in eps[0].source

    @patch('dt.auth.utils.get_project_name', return_value='myproj')
    @patch('dt.auth.cfg.get_value')
    @patch('dt.auth.utils.get_cache_dir', return_value=None)
    def test_remote_root_appends_project_name(self, _, mock_get, __):
        mock_get.side_effect = lambda k: '/remote' if k == 'remote.root' else None
        eps = _discover_dt_config()
        assert len(eps) == 1
        assert eps[0].url == '/remote/myproj'
        assert eps[0].source == 'remote.root'

    @patch('dt.auth.utils.get_project_name', return_value='proj')
    @patch('dt.auth.cfg.get_value')
    @patch('dt.auth.utils.get_cache_dir')
    def test_both_cache_and_remote(self, mock_cache, mock_get, _):
        mock_cache.return_value = Path('/data/cache/files/md5')
        mock_get.side_effect = lambda k: '/remote' if k == 'remote.root' else None
        eps = _discover_dt_config()
        assert len(eps) == 2
        assert eps[0].url == '/data/cache'
        assert eps[1].url == '/remote/proj'

    @patch('dt.auth.cfg.get_value', return_value=None)
    @patch('dt.auth.utils.get_cache_dir', return_value=None)
    def test_nothing_configured(self, *_):
        eps = _discover_dt_config()
        assert eps == []

    @patch('dt.auth.cfg.get_value', return_value=None)
    @patch('dt.auth.utils.get_cache_dir')
    def test_dvc_cache_preempts_dt_config(self, mock_cache, _):
        """When DVC cache dir is available, dt config cache.root is ignored."""
        mock_cache.return_value = Path('/dvc/cache/files/md5')
        eps = _discover_dt_config()
        # Should only have the DVC cache, not the dt config one
        cache_eps = [e for e in eps if 'cache' in e.source.lower()]
        assert len(cache_eps) == 1
        assert cache_eps[0].url == '/dvc/cache'


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
        assert '2 files' in eps[0].source

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
        assert 'local path' in output

    @patch('dt.auth.utils.get_project_name', return_value='test-proj')
    def test_format_endpoints_shows_children(self, _):
        child = Endpoint(type='ssh', url='ssh://h/p', source='child')
        ep = Endpoint(type='git', url='git@github.com:o/r.git',
                      source='import', children=[child])
        output = format_endpoints([ep])
        assert 'ssh://h/p' in output
        assert 'child' in output

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


# =============================================================================
# CheckResult tests
# =============================================================================

class TestCheckResult:
    """Tests for the CheckResult dataclass."""

    def test_to_dict_minimal(self):
        ep = Endpoint(type='ssh', url='ssh://h/p', source='remote')
        r = CheckResult(endpoint=ep, status=STATUS_PASS, summary='OK')
        d = r.to_dict()
        assert d['status'] == 'pass'
        assert d['summary'] == 'OK'
        assert 'endpoint' in d
        assert 'details' not in d
        assert 'hints' not in d

    def test_to_dict_with_details_and_hints(self):
        ep = Endpoint(type='filesystem', url='/x', source='a')
        r = CheckResult(
            endpoint=ep, status=STATUS_FAIL, summary='bad',
            details=['line1'], hints=['fix it'],
        )
        d = r.to_dict()
        assert d['details'] == ['line1']
        assert d['hints'] == ['fix it']


# =============================================================================
# _check_filesystem tests
# =============================================================================

class TestCheckFilesystem:
    """Tests for _check_filesystem."""

    def test_pass_empty_dir(self, tmp_path):
        ep = Endpoint(type='filesystem', url=str(tmp_path), source='test')
        r = _check_filesystem(ep)
        assert r.status == STATUS_PASS
        assert 'read/write' in r.summary

    def test_pass_with_subdirs(self, tmp_path):
        (tmp_path / 'aa').mkdir()
        (tmp_path / 'bb').mkdir()
        ep = Endpoint(type='filesystem', url=str(tmp_path), source='test')
        r = _check_filesystem(ep)
        assert r.status == STATUS_PASS
        assert '2/2 subdirs OK' in r.summary

    def test_nonexistent_path(self):
        ep = Endpoint(type='filesystem', url='/nonexistent/path/xyz', source='test')
        r = _check_filesystem(ep)
        assert r.status == STATUS_FAIL
        assert 'does not exist' in r.summary

    def test_verbose_lists_subdirs(self, tmp_path):
        (tmp_path / 'aa').mkdir()
        ep = Endpoint(type='filesystem', url=str(tmp_path), source='test')
        r = _check_filesystem(ep, verbose=True)
        assert r.status == STATUS_PASS
        assert any('aa' in d for d in r.details)

    def test_not_a_directory(self, tmp_path):
        f = tmp_path / 'file.txt'
        f.write_text('hello')
        ep = Endpoint(type='filesystem', url=str(f), source='test')
        r = _check_filesystem(ep)
        assert r.status == STATUS_FAIL
        assert 'not a directory' in r.summary


# =============================================================================
# _check_ssh tests
# =============================================================================

class TestCheckSsh:
    """Tests for _check_ssh."""

    def test_local_path_delegates_to_filesystem(self, tmp_path):
        """SSH endpoint with local_path checks the filesystem instead."""
        ep = Endpoint(
            type='ssh', url='ssh://host/path', source='remote',
            local_path=str(tmp_path),
        )
        r = _check_ssh(ep)
        assert r.status == STATUS_PASS
        assert 'checked as local path' in r.summary
        # Endpoint should still be the original SSH endpoint
        assert r.endpoint.type == 'ssh'

    @patch('subprocess.run')
    def test_remote_ssh_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        ep = Endpoint(type='ssh', url='ssh://gadi.nci.org.au/data', source='r')
        r = _check_ssh(ep)
        assert r.status == STATUS_PASS
        assert 'connection OK' in r.summary

    @patch('subprocess.run')
    def test_remote_ssh_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=255)
        ep = Endpoint(type='ssh', url='ssh://gadi.nci.org.au/data', source='r')
        r = _check_ssh(ep)
        assert r.status == STATUS_FAIL
        assert r.hints  # should suggest ssh-add

    @patch('subprocess.run', side_effect=subprocess.TimeoutExpired('ssh', 10))
    def test_remote_ssh_timeout(self, _):
        ep = Endpoint(type='ssh', url='ssh://host/data', source='r')
        r = _check_ssh(ep)
        assert r.status == STATUS_FAIL
        assert 'timed out' in r.summary


# =============================================================================
# _check_git tests
# =============================================================================

class TestCheckGit:
    """Tests for _check_git."""

    @patch('subprocess.run')
    def test_reachable(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        ep = Endpoint(type='git', url='git@github.com:o/r.git', source='origin')
        r = _check_git(ep)
        assert r.status == STATUS_PASS
        assert 'reachable' in r.summary

    @patch('subprocess.run')
    def test_not_reachable_ssh(self, mock_run):
        mock_run.return_value = MagicMock(returncode=128)
        ep = Endpoint(type='git', url='git@github.com:o/r.git', source='origin')
        r = _check_git(ep)
        assert r.status == STATUS_FAIL
        assert r.hints  # SSH-based URL should suggest ssh-add

    @patch('subprocess.run')
    def test_not_reachable_https(self, mock_run):
        mock_run.return_value = MagicMock(returncode=128)
        ep = Endpoint(type='git', url='https://github.com/o/r.git', source='o')
        r = _check_git(ep)
        assert r.status == STATUS_FAIL
        assert not r.hints  # HTTPS — no SSH hint

    @patch('subprocess.run', side_effect=subprocess.TimeoutExpired('git', 15))
    def test_timeout(self, _):
        ep = Endpoint(type='git', url='git@github.com:o/r.git', source='o')
        r = _check_git(ep)
        assert r.status == STATUS_FAIL
        assert 'timed out' in r.summary


# =============================================================================
# _check_http tests
# =============================================================================

class TestCheckHttp:
    """Tests for _check_http."""

    @patch('shutil.which', return_value='/usr/bin/curl')
    @patch('subprocess.run')
    def test_reachable(self, mock_run, _):
        mock_run.return_value = MagicMock(returncode=0)
        ep = Endpoint(type='http', url='https://example.com/data', source='r')
        r = _check_http(ep)
        assert r.status == STATUS_PASS

    @patch('shutil.which', return_value='/usr/bin/curl')
    @patch('subprocess.run')
    def test_not_reachable(self, mock_run, _):
        mock_run.return_value = MagicMock(returncode=22)
        ep = Endpoint(type='http', url='https://example.com/data', source='r')
        r = _check_http(ep)
        assert r.status == STATUS_FAIL

    @patch('shutil.which', return_value=None)
    def test_curl_not_installed(self, _):
        ep = Endpoint(type='http', url='https://example.com/data', source='r')
        r = _check_http(ep)
        assert r.status == STATUS_SKIP


# =============================================================================
# _check_s3 tests
# =============================================================================

class TestCheckS3:
    """Tests for _check_s3."""

    @patch('shutil.which', return_value=None)
    def test_aws_not_installed(self, _):
        ep = Endpoint(type='s3', url='s3://bucket', source='cloud')
        r = _check_s3(ep)
        assert r.status == STATUS_SKIP

    @patch('shutil.which', return_value='/usr/bin/aws')
    @patch('subprocess.run')
    def test_credentials_fail(self, mock_run, _):
        mock_run.return_value = MagicMock(returncode=1)
        ep = Endpoint(type='s3', url='s3://bucket', source='cloud')
        r = _check_s3(ep)
        assert r.status == STATUS_FAIL
        assert 'credentials not configured' in r.summary

    @patch('shutil.which', return_value='/usr/bin/aws')
    @patch('subprocess.run')
    def test_full_pass(self, mock_run, _):
        mock_run.return_value = MagicMock(returncode=0)
        ep = Endpoint(type='s3', url='s3://bucket/prefix', source='cloud')
        r = _check_s3(ep)
        assert r.status == STATUS_PASS
        assert 'bucket accessible' in r.summary


# =============================================================================
# _check_gs tests
# =============================================================================

class TestCheckGs:
    """Tests for _check_gs."""

    @patch('shutil.which', return_value=None)
    def test_gcloud_not_installed(self, _):
        ep = Endpoint(type='gs', url='gs://bucket', source='cloud')
        r = _check_gs(ep)
        assert r.status == STATUS_SKIP

    @patch('shutil.which', return_value='/usr/bin/gcloud')
    @patch('subprocess.run')
    def test_no_auth(self, mock_run, _):
        mock_run.return_value = MagicMock(returncode=0, stdout='')
        ep = Endpoint(type='gs', url='gs://bucket', source='cloud')
        r = _check_gs(ep)
        assert r.status == STATUS_WARN  # soft failure
        assert 'no gcloud auth' in r.summary


# =============================================================================
# _extract_remote_name tests
# =============================================================================

class TestExtractRemoteName:
    """Tests for _extract_remote_name."""

    def test_default_remote(self):
        assert _extract_remote_name("DVC remote 'nci' (default)") == 'nci'

    def test_non_default(self):
        assert _extract_remote_name("DVC remote 'cloud'") == 'cloud'

    def test_no_match(self):
        assert _extract_remote_name('cache.root') is None

    def test_child_remote(self):
        assert _extract_remote_name("DVC remote 'gadi' of chromium (default)") == 'gadi'


# =============================================================================
# check_endpoints orchestrator tests
# =============================================================================

class TestCheckDvcRemote:
    """Tests for _check_dvc_remote_impl."""

    def test_pass_when_reachable(self):
        ep = Endpoint(type='s3', url='s3://bucket/prefix',
                      source="DVC remote 'cloud'")
        mock_odb = MagicMock()
        mock_odb.fs.exists.return_value = True
        mock_odb.fs.ls.return_value = ['/bucket/prefix/aa', '/bucket/prefix/bb']
        mock_odb.path = '/bucket/prefix/files/md5'

        mock_repo = MagicMock()
        mock_repo.cloud.get_remote_odb.return_value = mock_odb

        result = _check_dvc_remote_impl(ep, 'cloud',
                                        _repo_factory=lambda: mock_repo)
        assert result is not None
        assert result.status == STATUS_PASS
        assert 'via DVC' in result.summary
        assert '2 entries' in result.summary

    def test_fail_when_not_reachable(self):
        ep = Endpoint(type='gs', url='gs://bucket',
                      source="DVC remote 'gcs'")
        mock_odb = MagicMock()
        mock_odb.fs.exists.return_value = False
        mock_odb.path = '/bucket/files/md5'

        mock_repo = MagicMock()
        mock_repo.cloud.get_remote_odb.return_value = mock_odb

        result = _check_dvc_remote_impl(ep, 'gcs',
                                        _repo_factory=lambda: mock_repo)
        assert result.status == STATUS_FAIL
        assert 'does not exist' in result.summary

    def test_fail_when_access_error(self):
        ep = Endpoint(type='s3', url='s3://bucket',
                      source="DVC remote 'r2'")
        mock_odb = MagicMock()
        mock_odb.fs.exists.side_effect = Exception('access denied')
        mock_odb.path = '/bucket/files/md5'

        mock_repo = MagicMock()
        mock_repo.cloud.get_remote_odb.return_value = mock_odb

        result = _check_dvc_remote_impl(ep, 'r2',
                                        _repo_factory=lambda: mock_repo)
        assert result.status == STATUS_FAIL
        assert 'not reachable' in result.summary

    def test_fail_when_remote_init_error(self):
        ep = Endpoint(type='gs', url='gs://bucket',
                      source="DVC remote 'gcs'")
        mock_repo = MagicMock()
        mock_repo.cloud.get_remote_odb.side_effect = Exception('bad config')

        result = _check_dvc_remote_impl(ep, 'gcs',
                                        _repo_factory=lambda: mock_repo)
        assert result.status == STATUS_FAIL
        assert 'cannot initialise' in result.summary

    def test_returns_none_when_repo_fails(self):
        ep = Endpoint(type='s3', url='s3://b', source="DVC remote 'x'")

        def bad_factory():
            raise Exception('no repo')

        result = _check_dvc_remote_impl(ep, 'x', _repo_factory=bad_factory)
        assert result is None

    def test_verbose_lists_entries(self):
        ep = Endpoint(type='s3', url='s3://bucket/prefix',
                      source="DVC remote 'cloud'")
        mock_odb = MagicMock()
        mock_odb.fs.exists.return_value = True
        mock_odb.fs.ls.return_value = ['/prefix/aa', '/prefix/bb', '/prefix/cc']
        mock_odb.path = '/prefix/files/md5'

        mock_repo = MagicMock()
        mock_repo.cloud.get_remote_odb.return_value = mock_odb

        result = _check_dvc_remote_impl(ep, 'cloud', verbose=True,
                                        _repo_factory=lambda: mock_repo)
        assert result.status == STATUS_PASS
        assert len(result.details) == 3
        assert 'aa' in result.details[0]


class TestTryCheck:
    """Tests for _try_check — DVC-native vs per-type fallback."""

    def test_dvc_remote_uses_native_check(self):
        """DVC remotes are checked via _check_dvc_remote."""
        ep = Endpoint(type='s3', url='s3://bucket',
                      source="DVC remote 'cloud'")
        dvc_result = CheckResult(
            endpoint=ep, status=STATUS_PASS, summary='via DVC'
        )
        with patch('dt.auth._check_dvc_remote', return_value=dvc_result) as mock_dvc:
            result = _try_check(ep)
        mock_dvc.assert_called_once_with(ep, 'cloud', verbose=False)
        assert result.summary == 'via DVC'

    def test_non_dvc_endpoint_uses_type_checker(self, tmp_path):
        """Non-DVC endpoints use per-type checkers."""
        d = tmp_path / 'data'
        d.mkdir()
        ep = Endpoint(type='filesystem', url=str(d), source='cache.root')
        result = _try_check(ep)
        assert result.status == STATUS_PASS

    def test_dvc_native_fallback_to_type_checker(self):
        """Falls back to per-type checker when DVC is unavailable."""
        ep = Endpoint(type='git', url='git@g:o/r.git',
                      source="DVC remote 'origin'")
        with patch('dt.auth._check_dvc_remote', return_value=None), \
             patch('subprocess.run', return_value=MagicMock(returncode=0)):
            result = _try_check(ep)
        assert result.status == STATUS_PASS

    def test_import_source_child_skips_dvc_native(self):
        """Children from import sources (source contains ' of ') skip
        DVC-native check since they need a different repo context."""
        ep = Endpoint(type='ssh', url='ssh://host/path',
                      source="DVC remote 'nci' of data-repo")
        with patch('dt.auth._check_dvc_remote') as mock_dvc, \
             patch('subprocess.run', return_value=MagicMock(returncode=1)):
            mock_dvc.return_value = None  # should not matter
            result = _try_check(ep)
        mock_dvc.assert_not_called()


class TestCheckEndpoints:
    """Tests for check_endpoints."""

    def test_checks_all_endpoints(self, tmp_path):
        """Checks each endpoint and returns results."""
        d = tmp_path / 'cache'
        d.mkdir()
        eps = [
            Endpoint(type='filesystem', url=str(d), source='cache'),
        ]
        results = check_endpoints(endpoints=eps)
        assert len(results) == 1
        assert results[0].status == STATUS_PASS

    @patch('subprocess.run')
    def test_checks_children(self, mock_run):
        """Children of endpoints are checked too."""
        mock_run.return_value = MagicMock(returncode=0)
        child = Endpoint(type='git', url='git@g:o/r.git', source='child')
        parent = Endpoint(
            type='git', url='git@g:o/parent.git', source='origin',
            children=[child],
        )
        results = check_endpoints(endpoints=[parent])
        assert len(results) == 2  # parent + child

    def test_discovers_if_not_provided(self):
        """When endpoints=None, calls discover_endpoints."""
        with patch('dt.auth.discover_endpoints', return_value=[]) as mock_disc:
            results = check_endpoints(endpoints=None)
        mock_disc.assert_called_once()
        assert results == []


# =============================================================================
# format_check_results tests
# =============================================================================

class TestFormatCheckResults:
    """Tests for format_check_results."""

    def test_shows_pass_icon(self):
        ep = Endpoint(type='filesystem', url='/cache', source='test')
        r = CheckResult(endpoint=ep, status=STATUS_PASS, summary='OK')
        output = format_check_results([r])
        assert '✓' in output
        assert '1 passed' in output

    def test_shows_fail_icon_and_hints(self):
        ep = Endpoint(type='ssh', url='ssh://h/p', source='test')
        r = CheckResult(
            endpoint=ep, status=STATUS_FAIL, summary='bad',
            hints=['do this'],
        )
        output = format_check_results([r])
        assert '✗' in output
        assert 'Hint:' in output
        assert '1 failed' in output

    def test_shows_warn_icon(self):
        ep = Endpoint(type='gs', url='gs://b', source='test')
        r = CheckResult(endpoint=ep, status=STATUS_WARN, summary='warn')
        output = format_check_results([r])
        assert '⚠' in output
        assert 'warning' in output

    def test_json_format(self):
        ep = Endpoint(type='filesystem', url='/x', source='test')
        r = CheckResult(endpoint=ep, status=STATUS_PASS, summary='OK')
        output = format_check_results_json([r])
        data = json.loads(output)
        assert len(data) == 1
        assert data[0]['status'] == 'pass'

    def test_summary_line_counts(self):
        ep1 = Endpoint(type='filesystem', url='/a', source='a')
        ep2 = Endpoint(type='ssh', url='ssh://h/p', source='b')
        results = [
            CheckResult(endpoint=ep1, status=STATUS_PASS, summary='OK'),
            CheckResult(endpoint=ep2, status=STATUS_FAIL, summary='bad'),
        ]
        output = format_check_results(results)
        assert '1 passed' in output
        assert '1 failed' in output


# =============================================================================
# AccessRequest tests
# =============================================================================

class TestAccessRequest:

    def test_to_dict(self):
        ep = Endpoint(type='filesystem', url='/data', source='test')
        item = CheckResult(endpoint=ep, status=STATUS_FAIL, summary='bad')
        req = AccessRequest(
            user='jsmith', project='proj', platform_name='gadi',
            dt_version='0.1.0', request_date='2026-01-15', items=[item],
        )
        d = req.to_dict()
        assert d['user'] == 'jsmith'
        assert d['project'] == 'proj'
        assert d['platform'] == 'gadi'
        assert d['dt_version'] == '0.1.0'
        assert d['date'] == '2026-01-15'
        assert len(d['items']) == 1
        assert d['items'][0]['status'] == 'fail'

    def test_to_dict_empty_items(self):
        req = AccessRequest(
            user='u', project='p', platform_name='h',
            dt_version='0.1.0', request_date='2026-01-01',
        )
        d = req.to_dict()
        assert d['items'] == []


class TestGenerateRequest:

    @patch('dt.auth.check_endpoints')
    def test_collects_failures(self, mock_check):
        ep1 = Endpoint(type='filesystem', url='/ok', source='a')
        ep2 = Endpoint(type='ssh', url='ssh://x/y', source='b')
        ep3 = Endpoint(type='gs', url='gs://b', source='c')
        mock_check.return_value = [
            CheckResult(endpoint=ep1, status=STATUS_PASS, summary='OK'),
            CheckResult(endpoint=ep2, status=STATUS_FAIL, summary='bad',
                        hints=['fix it']),
            CheckResult(endpoint=ep3, status=STATUS_WARN, summary='warn'),
        ]
        req = generate_request()
        assert len(req.items) == 2  # fail + warn
        assert req.items[0].endpoint.url == 'ssh://x/y'
        assert req.items[1].endpoint.url == 'gs://b'

    @patch('dt.auth.check_endpoints')
    def test_excludes_warnings_when_disabled(self, mock_check):
        ep1 = Endpoint(type='ssh', url='ssh://x/y', source='a')
        ep2 = Endpoint(type='gs', url='gs://b', source='b')
        mock_check.return_value = [
            CheckResult(endpoint=ep1, status=STATUS_FAIL, summary='bad'),
            CheckResult(endpoint=ep2, status=STATUS_WARN, summary='warn'),
        ]
        req = generate_request(include_warnings=False)
        assert len(req.items) == 1
        assert req.items[0].status == STATUS_FAIL

    @patch('dt.auth.check_endpoints')
    def test_all_pass_empty_items(self, mock_check):
        ep = Endpoint(type='filesystem', url='/x', source='a')
        mock_check.return_value = [
            CheckResult(endpoint=ep, status=STATUS_PASS, summary='OK'),
        ]
        req = generate_request()
        assert len(req.items) == 0

    @patch('dt.auth.check_endpoints')
    def test_passes_type_filter(self, mock_check):
        mock_check.return_value = []
        generate_request(type_filter={'s3'})
        mock_check.assert_called_once_with(type_filter={'s3'}, verbose=False)

    @patch('dt.auth.check_endpoints')
    def test_metadata_populated(self, mock_check):
        mock_check.return_value = []
        req = generate_request()
        assert req.user  # non-empty
        assert req.project  # non-empty
        assert req.platform_name  # non-empty
        assert req.dt_version  # non-empty
        assert req.request_date  # non-empty, ISO format


class TestFormatRequestText:

    def test_no_items(self):
        req = AccessRequest(
            user='jsmith', project='proj', platform_name='gadi',
            dt_version='0.1.0', request_date='2026-01-15',
        )
        output = format_request_text(req)
        assert 'jsmith' in output
        assert 'proj' in output
        assert 'no request needed' in output

    def test_single_failure(self):
        ep = Endpoint(type='filesystem', url='/scratch/data', source='cache.root')
        item = CheckResult(
            endpoint=ep, status=STATUS_FAIL, summary='not readable',
            hints=['chmod -R g+rw /scratch/data'],
        )
        req = AccessRequest(
            user='jsmith', project='proj', platform_name='gadi',
            dt_version='0.1.0', request_date='2026-01-15', items=[item],
        )
        output = format_request_text(req)
        assert 'jsmith' in output
        assert 'Filesystem: /scratch/data' in output
        assert 'not readable' in output
        assert 'read/write access' in output
        assert 'chmod' in output
        assert 'Platform: gadi' in output
        assert 'dt version: 0.1.0' in output
        assert 'Date: 2026-01-15' in output

    def test_multiple_types(self):
        ep1 = Endpoint(type='s3', url='s3://bucket', source='remote')
        ep2 = Endpoint(type='git', url='git@github.com:o/r.git', source='import')
        req = AccessRequest(
            user='u', project='p', platform_name='h',
            dt_version='v', request_date='d',
            items=[
                CheckResult(endpoint=ep1, status=STATUS_FAIL,
                            summary='creds bad'),
                CheckResult(endpoint=ep2, status=STATUS_FAIL,
                            summary='not reachable'),
            ],
        )
        output = format_request_text(req)
        assert '1. S3: s3://bucket' in output
        assert '2. Git: git@github.com:o/r.git' in output
        assert 'read access' in output
        assert 'connection access' in output

    def test_warning_item(self):
        ep = Endpoint(type='gs', url='gs://b', source='remote')
        item = CheckResult(endpoint=ep, status=STATUS_WARN, summary='no auth')
        req = AccessRequest(
            user='u', project='p', platform_name='h',
            dt_version='v', request_date='d', items=[item],
        )
        output = format_request_text(req)
        assert 'Gs: gs://b' in output  # capitalised type
        assert 'no auth' in output


class TestFormatRequestMarkdown:

    def test_no_items(self):
        req = AccessRequest(
            user='u', project='p', platform_name='h',
            dt_version='v', request_date='d',
        )
        output = format_request_markdown(req)
        assert '# Access request' in output
        assert 'no request needed' in output

    def test_failure_with_hints(self):
        ep = Endpoint(type='filesystem', url='/data', source='cache.root')
        item = CheckResult(
            endpoint=ep, status=STATUS_FAIL, summary='not readable',
            hints=['chmod -R g+rw /data'],
        )
        req = AccessRequest(
            user='jsmith', project='proj', platform_name='gadi',
            dt_version='0.1.0', request_date='2026-01-15', items=[item],
        )
        output = format_request_markdown(req)
        assert '# Access request — proj' in output
        assert '**User:** jsmith' in output
        assert '🔴' in output
        assert '`/data`' in output
        assert 'chmod' in output
        assert '**Source:** cache.root' in output

    def test_warning_gets_yellow_icon(self):
        ep = Endpoint(type='gs', url='gs://b', source='remote')
        item = CheckResult(endpoint=ep, status=STATUS_WARN, summary='warn')
        req = AccessRequest(
            user='u', project='p', platform_name='h',
            dt_version='v', request_date='d', items=[item],
        )
        output = format_request_markdown(req)
        assert '🟡' in output


class TestFormatRequestJson:

    def test_roundtrip(self):
        ep = Endpoint(type='filesystem', url='/data', source='test')
        item = CheckResult(
            endpoint=ep, status=STATUS_FAIL, summary='bad',
            hints=['fix it'],
        )
        req = AccessRequest(
            user='u', project='p', platform_name='h',
            dt_version='v', request_date='d', items=[item],
        )
        output = format_request_json(req)
        data = json.loads(output)
        assert data['user'] == 'u'
        assert data['project'] == 'p'
        assert len(data['items']) == 1
        assert data['items'][0]['status'] == 'fail'
        assert data['items'][0]['hints'] == ['fix it']

    def test_empty_items(self):
        req = AccessRequest(
            user='u', project='p', platform_name='h',
            dt_version='v', request_date='d',
        )
        data = json.loads(format_request_json(req))
        assert data['items'] == []


# =============================================================================
# Slack formatting tests
# =============================================================================

class TestFormatSlackBlocks:
    """Tests for _format_slack_blocks."""

    def _make_req(self, items=None):
        return AccessRequest(
            user='alice', project='my-proj', platform_name='gadi',
            dt_version='0.3.0', request_date='2025-07-17',
            items=items or [],
        )

    def test_no_items_shows_all_clear(self):
        payload = _format_slack_blocks(self._make_req())
        texts = json.dumps(payload)
        assert 'All endpoints are accessible' in texts

    def test_header_contains_project_name(self):
        payload = _format_slack_blocks(self._make_req())
        header = payload['blocks'][0]
        assert header['type'] == 'header'
        assert 'my-proj' in header['text']['text']

    def test_metadata_fields(self):
        payload = _format_slack_blocks(self._make_req())
        section = payload['blocks'][1]
        fields_text = ' '.join(f['text'] for f in section['fields'])
        assert 'alice' in fields_text
        assert 'gadi' in fields_text
        assert '0.3.0' in fields_text

    def test_fail_item_red_circle(self):
        ep = Endpoint(type='filesystem', url='/data', source='test')
        item = CheckResult(endpoint=ep, status=STATUS_FAIL, summary='not readable')
        payload = _format_slack_blocks(self._make_req([item]))
        text = json.dumps(payload)
        assert ':red_circle:' in text
        assert '/data' in text

    def test_warn_item_yellow_circle(self):
        ep = Endpoint(type='gs', url='gs://bucket', source='test')
        item = CheckResult(endpoint=ep, status=STATUS_WARN, summary='warning')
        payload = _format_slack_blocks(self._make_req([item]))
        text = json.dumps(payload)
        assert ':large_yellow_circle:' in text

    def test_hints_included(self):
        ep = Endpoint(type='ssh', url='ssh://host', source='test')
        item = CheckResult(
            endpoint=ep, status=STATUS_FAIL, summary='timeout',
            hints=['check ssh-agent'],
        )
        payload = _format_slack_blocks(self._make_req([item]))
        text = json.dumps(payload)
        assert 'check ssh-agent' in text


# =============================================================================
# send_request_slack tests
# =============================================================================

class TestSendRequestSlack:
    """Tests for send_request_slack."""

    def _make_req(self):
        return AccessRequest(
            user='alice', project='proj', platform_name='h',
            dt_version='v', request_date='d',
        )

    @patch('dt.auth.urllib.request.urlopen')
    def test_success(self, mock_urlopen):
        resp = MagicMock()
        resp.status = 200
        resp.read.return_value = b'ok'
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = resp

        send_request_slack(self._make_req(), 'https://hooks.slack.com/test')
        mock_urlopen.assert_called_once()
        call_args = mock_urlopen.call_args
        request_obj = call_args[0][0]
        assert request_obj.full_url == 'https://hooks.slack.com/test'
        assert request_obj.get_header('Content-type') == 'application/json'

    @patch('dt.auth.urllib.request.urlopen')
    def test_non_200_raises(self, mock_urlopen):
        from dt.errors import AuthError
        resp = MagicMock()
        resp.status = 500
        resp.read.return_value = b'server error'
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = resp

        with pytest.raises(AuthError, match='unexpected response'):
            send_request_slack(self._make_req(), 'https://hooks.slack.com/test')

    @patch('dt.auth.urllib.request.urlopen')
    def test_url_error_raises(self, mock_urlopen):
        import urllib.error
        from dt.errors import AuthError
        mock_urlopen.side_effect = urllib.error.URLError('connection refused')

        with pytest.raises(AuthError, match='Failed to send Slack'):
            send_request_slack(self._make_req(), 'https://hooks.slack.com/bad')


# =============================================================================
# send_request_email tests
# =============================================================================

class TestSendRequestEmail:
    """Tests for send_request_email."""

    def _make_req(self):
        return AccessRequest(
            user='alice', project='proj', platform_name='h',
            dt_version='v', request_date='d',
            items=[
                CheckResult(
                    endpoint=Endpoint(type='filesystem', url='/data', source='t'),
                    status=STATUS_FAIL, summary='not readable',
                ),
            ],
        )

    @patch('dt.auth.subprocess.run')
    @patch('dt.auth.shutil.which', return_value='/usr/bin/mail')
    def test_success(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stderr='')
        send_request_email(self._make_req(), 'admin@example.com')

        mock_run.assert_called_once()
        args = mock_run.call_args
        cmd = args[0][0]
        assert cmd[0] == 'mail'
        assert 'admin@example.com' in cmd
        assert 'proj' in cmd[2]  # subject contains project name
        assert args[1]['input']  # body is non-empty

    @patch('dt.auth.shutil.which', return_value=None)
    def test_mail_not_found_raises(self, mock_which):
        from dt.errors import AuthError
        with pytest.raises(AuthError, match="'mail' command is not available"):
            send_request_email(self._make_req(), 'admin@example.com')

    @patch('dt.auth.subprocess.run')
    @patch('dt.auth.shutil.which', return_value='/usr/bin/mail')
    def test_nonzero_exit_raises(self, mock_which, mock_run):
        from dt.errors import AuthError
        mock_run.return_value = MagicMock(returncode=1, stderr='no such user')
        with pytest.raises(AuthError, match='mail command failed'):
            send_request_email(self._make_req(), 'admin@example.com')


# =============================================================================
# send_request (auto-detect) tests
# =============================================================================

class TestSendRequest:
    """Tests for send_request auto-detection."""

    def _make_req(self):
        ep = Endpoint(type='filesystem', url='/data', source='t')
        item = CheckResult(endpoint=ep, status=STATUS_FAIL, summary='bad')
        return AccessRequest(
            user='alice', project='proj', platform_name='h',
            dt_version='v', request_date='d', items=[item],
        )

    @patch('dt.auth.send_request_slack')
    @patch('dt.auth.cfg.get_value')
    def test_explicit_slack(self, mock_cfg, mock_send_slack):
        mock_cfg.side_effect = lambda k: {
            'auth.slack_webhook': 'https://hooks.slack.com/xxx',
        }.get(k)
        msg = send_request(self._make_req(), method='slack')
        mock_send_slack.assert_called_once()
        assert 'Slack' in msg

    @patch('dt.auth.send_request_email')
    @patch('dt.auth.cfg.get_value')
    def test_explicit_email(self, mock_cfg, mock_send_email):
        mock_cfg.side_effect = lambda k: {
            'auth.admin_email': 'admin@test.com',
        }.get(k)
        msg = send_request(self._make_req(), method='email')
        mock_send_email.assert_called_once()
        assert 'admin@test.com' in msg

    @patch('dt.auth.cfg.get_value')
    def test_explicit_slack_no_config_raises(self, mock_cfg):
        from dt.errors import AuthError
        mock_cfg.side_effect = KeyError('not found')
        with pytest.raises(AuthError, match='Slack webhook not configured'):
            send_request(self._make_req(), method='slack')

    @patch('dt.auth.cfg.get_value')
    def test_explicit_email_no_config_raises(self, mock_cfg):
        from dt.errors import AuthError
        mock_cfg.side_effect = KeyError('not found')
        with pytest.raises(AuthError, match='Admin email not configured'):
            send_request(self._make_req(), method='email')

    @patch('dt.auth.send_request_slack')
    @patch('dt.auth.cfg.get_value')
    def test_auto_prefers_slack(self, mock_cfg, mock_send_slack):
        mock_cfg.side_effect = lambda k: {
            'auth.slack_webhook': 'https://hooks.slack.com/xxx',
            'auth.admin_email': 'admin@test.com',
        }[k]
        msg = send_request(self._make_req(), method=None)
        mock_send_slack.assert_called_once()
        assert 'Slack' in msg

    @patch('dt.auth.send_request_email')
    @patch('dt.auth.cfg.get_value')
    def test_auto_falls_back_to_email(self, mock_cfg, mock_send_email):
        def cfg_side(k):
            if k == 'auth.slack_webhook':
                raise KeyError('not found')
            if k == 'auth.admin_email':
                return 'admin@test.com'
            raise KeyError
        mock_cfg.side_effect = cfg_side
        msg = send_request(self._make_req(), method=None)
        mock_send_email.assert_called_once()
        assert 'admin@test.com' in msg

    @patch('dt.auth.cfg.get_value')
    def test_auto_no_config_raises(self, mock_cfg):
        from dt.errors import AuthError
        mock_cfg.side_effect = KeyError('not found')
        with pytest.raises(AuthError, match='No delivery method configured'):
            send_request(self._make_req(), method=None)


# =============================================================================
# Identity dataclass tests
# =============================================================================

class TestIdentity:
    """Tests for the Identity dataclass."""

    def test_to_dict(self):
        i = Identity(system='GitHub user', value='alice', source='config')
        d = i.to_dict()
        assert d == {'system': 'GitHub user', 'value': 'alice', 'source': 'config'}

    def test_default_source(self):
        i = Identity(system='test', value='val')
        assert i.source == 'config'


# =============================================================================
# get_identities tests
# =============================================================================

class TestGetIdentities:
    """Tests for get_identities (config-based)."""

    @patch('dt.auth.cfg.get_value')
    @patch('dt.auth.getpass.getuser', return_value='alice')
    def test_always_includes_nci_username(self, mock_user, mock_cfg):
        mock_cfg.side_effect = KeyError('not set')
        ids = get_identities()
        assert len(ids) == 1
        assert ids[0].system == 'NCI username'
        assert ids[0].value == 'alice'
        assert ids[0].source == 'detected'

    @patch('dt.auth.cfg.get_value')
    @patch('dt.auth.getpass.getuser', return_value='alice')
    def test_reads_config_values(self, mock_user, mock_cfg):
        def cfg_side(k):
            return {
                'auth.github_user': 'alice-gh',
                'auth.github_teams': 'data-team, ops',
            }.get(k)
        mock_cfg.side_effect = lambda k: cfg_side(k) or (_ for _ in ()).throw(KeyError)
        # Fix: need a proper side effect
        def cfg_effect(k):
            val = {'auth.github_user': 'alice-gh', 'auth.github_teams': 'data-team, ops'}.get(k)
            if val is None:
                raise KeyError(k)
            return val
        mock_cfg.side_effect = cfg_effect
        ids = get_identities()
        systems = {i.system: i.value for i in ids}
        assert systems['NCI username'] == 'alice'
        assert systems['GitHub user'] == 'alice-gh'
        assert systems['GitHub teams'] == 'data-team, ops'

    @patch('dt.auth.cfg.get_value')
    @patch('dt.auth.getpass.getuser', return_value='bob')
    def test_skips_empty_values(self, mock_user, mock_cfg):
        def cfg_effect(k):
            if k == 'auth.github_user':
                return ''
            raise KeyError(k)
        mock_cfg.side_effect = cfg_effect
        ids = get_identities()
        # Empty string should be skipped
        assert len(ids) == 1
        assert ids[0].system == 'NCI username'


# =============================================================================
# detect_* function tests
# =============================================================================

class TestDetectGithubUser:
    """Tests for _detect_github_user."""

    @patch('dt.auth.subprocess.run')
    def test_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout='alice-gh\n')
        assert _detect_github_user() == 'alice-gh'

    @patch('dt.auth.subprocess.run')
    def test_failure_returns_none(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout='')
        assert _detect_github_user() is None

    @patch('dt.auth.subprocess.run', side_effect=FileNotFoundError)
    def test_gh_not_installed(self, mock_run):
        assert _detect_github_user() is None

    @patch('dt.auth.subprocess.run', side_effect=subprocess.TimeoutExpired('gh', 15))
    def test_timeout(self, mock_run):
        assert _detect_github_user() is None


class TestDetectGithubTeams:
    """Tests for _detect_github_teams."""

    @patch('dt.auth.subprocess.run')
    def test_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout='data-team\nops\n')
        assert _detect_github_teams() == 'data-team, ops'

    @patch('dt.auth.subprocess.run')
    def test_no_teams(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout='')
        assert _detect_github_teams() is None


class TestDetectGcpEmail:
    """Tests for _detect_gcp_email."""

    @patch('dt.auth.subprocess.run')
    def test_success(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0, stdout='alice@proj.iam.gserviceaccount.com\n',
        )
        assert _detect_gcp_email() == 'alice@proj.iam.gserviceaccount.com'

    @patch('dt.auth.subprocess.run', side_effect=FileNotFoundError)
    def test_gcloud_not_installed(self, mock_run):
        assert _detect_gcp_email() is None


class TestDetectAwsIdentity:
    """Tests for _detect_aws_identity."""

    @patch('dt.auth.subprocess.run')
    def test_success(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0, stdout='arn:aws:iam::123:user/alice\n',
        )
        assert _detect_aws_identity() == 'arn:aws:iam::123:user/alice'

    @patch('dt.auth.subprocess.run', side_effect=FileNotFoundError)
    def test_aws_not_installed(self, mock_run):
        assert _detect_aws_identity() is None


# =============================================================================
# detect_identities tests
# =============================================================================

class TestDetectIdentities:
    """Tests for detect_identities."""

    @patch('dt.auth._DETECT_FNS', {
        'auth.github_user': lambda: 'alice-gh',
        'auth.github_teams': lambda: None,
        'auth.gcp_email': lambda: 'a@gcp.com',
        'auth.aws_identity': lambda: None,
    })
    @patch('dt.auth.getpass.getuser', return_value='alice')
    def test_collects_detected(self, mock_user):
        ids = detect_identities()
        systems = {i.system for i in ids}
        assert 'NCI username' in systems
        assert 'GitHub user' in systems
        assert 'GCP email' in systems
        # GitHub teams and AWS were None, should be absent
        assert 'GitHub teams' not in systems
        assert 'AWS identity' not in systems

    @patch('dt.auth._DETECT_FNS', {
        'auth.github_user': lambda: None,
        'auth.github_teams': lambda: None,
        'auth.gcp_email': lambda: None,
        'auth.aws_identity': lambda: None,
    })
    @patch('dt.auth.getpass.getuser', return_value='bob')
    def test_no_tools_returns_only_nci(self, mock_user):
        ids = detect_identities()
        assert len(ids) == 1
        assert ids[0].system == 'NCI username'


# =============================================================================
# compare_identities tests
# =============================================================================

class TestCompareIdentities:
    """Tests for compare_identities."""

    def test_match(self):
        stored = [Identity('GitHub user', 'alice', 'config')]
        detected = [Identity('GitHub user', 'alice', 'detected via gh api')]
        results = compare_identities(stored, detected)
        assert len(results) == 1
        assert results[0][2] == 'match'

    def test_mismatch(self):
        stored = [Identity('GitHub teams', 'a, b', 'config')]
        detected = [Identity('GitHub teams', 'a, b, c', 'detected via gh api')]
        results = compare_identities(stored, detected)
        assert len(results) == 1
        best, other, note = results[0]
        assert note == 'mismatch'
        assert best.value == 'a, b, c'  # detected is best
        assert other.value == 'a, b'

    def test_config_only(self):
        stored = [Identity('GCP email', 'a@gcp.com', 'config')]
        detected = []
        results = compare_identities(stored, detected)
        assert results[0][2] == 'config only'

    def test_detected_only(self):
        stored = []
        detected = [Identity('GCP email', 'a@gcp.com', 'detected via gcloud')]
        results = compare_identities(stored, detected)
        assert results[0][2] == 'detected only'

    def test_nci_always_matches(self):
        stored = [Identity('NCI username', 'alice', 'detected')]
        detected = [Identity('NCI username', 'alice', 'detected')]
        results = compare_identities(stored, detected)
        assert results[0][2] == 'match'


# =============================================================================
# format_identities tests
# =============================================================================

class TestFormatIdentities:
    """Tests for format_identities and format_identities_json."""

    def test_text_output(self):
        ids = [
            Identity('NCI username', 'alice', 'detected'),
            Identity('GitHub user', 'alice-gh', 'config'),
        ]
        output = format_identities(ids)
        assert 'alice' in output
        assert 'alice-gh' in output

    def test_empty(self):
        output = format_identities([])
        assert 'No identities found' in output

    def test_json_output(self):
        ids = [Identity('NCI username', 'alice', 'detected')]
        data = json.loads(format_identities_json(ids))
        assert len(data) == 1
        assert data[0]['system'] == 'NCI username'
        assert data[0]['value'] == 'alice'

    def test_comparison_output(self):
        comparisons = [
            (Identity('NCI username', 'alice', 'detected'), None, 'match'),
            (Identity('GCP email', 'a@gcp.com', 'detected via gcloud'), None, 'detected only'),
        ]
        output = format_whoami_comparison(comparisons)
        assert 'alice' in output
        assert 'a@gcp.com' in output


# =============================================================================
# save_detected_identities tests
# =============================================================================

class TestSaveDetectedIdentities:
    """Tests for save_detected_identities."""

    @patch('dt.auth.cfg.set_value')
    @patch('dt.auth.cfg.get_value', side_effect=KeyError)
    def test_saves_new_values(self, mock_get, mock_set):
        detected = [
            Identity('NCI username', 'alice', 'detected'),  # should skip
            Identity('GitHub user', 'alice-gh', 'detected via gh api'),
            Identity('GCP email', 'a@gcp.com', 'detected via gcloud'),
        ]
        count = save_detected_identities(detected)
        assert count == 2
        assert mock_set.call_count == 2
        # Check the calls — scope is passed as keyword arg
        calls = mock_set.call_args_list
        assert calls[0] == (('auth.github_user', 'alice-gh'), {'scope': 'user'})
        assert calls[1] == (('auth.gcp_email', 'a@gcp.com'), {'scope': 'user'})

    @patch('dt.auth.cfg.set_value')
    @patch('dt.auth.cfg.get_value')
    def test_skips_matching_values(self, mock_get, mock_set):
        mock_get.return_value = 'alice-gh'
        detected = [
            Identity('GitHub user', 'alice-gh', 'detected via gh api'),
        ]
        count = save_detected_identities(detected)
        assert count == 0
        mock_set.assert_not_called()

    @patch('dt.auth.cfg.set_value')
    @patch('dt.auth.cfg.get_value')
    def test_updates_changed_values(self, mock_get, mock_set):
        mock_get.return_value = 'old-name'
        detected = [
            Identity('GitHub user', 'new-name', 'detected via gh api'),
        ]
        count = save_detected_identities(detected)
        assert count == 1
        mock_set.assert_called_once_with('auth.github_user', 'new-name', scope='user')
