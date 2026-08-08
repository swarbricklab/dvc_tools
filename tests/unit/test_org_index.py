"""Unit tests for dt.org_index.

The cache format, the staleness logic, and downstream traversal are tested
directly. GitHub access is mocked -- no network is used.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from dt import org_index, repo_graph
from dt.dvc_deps import RepoEdge
from dt.errors import DepsError


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    """Point the index cache at a throwaway directory."""
    monkeypatch.setattr(org_index, 'cache_root', lambda: tmp_path / 'cache')
    return tmp_path / 'cache'


def _edge(source, target, n=1):
    return RepoEdge(source=source, target=target, n_imports=n)


def _entry(repo_id, edges=(), sha='abc', pushed_at='2026-01-01T00:00:00Z',
           status=repo_graph.STATUS_OK):
    entry = org_index.RepoEntry(
        repo_id=repo_id,
        url=f'git@github.com:{repo_id.split("/", 1)[1]}.git',
        name=repo_id.split('/')[-1],
        default_branch='main',
        pushed_at=pushed_at,
        status=status,
    )
    if edges or sha:
        entry.scans[sha] = org_index.ScanRecord(
            sha=sha, ref='main', n_dvc_files=len(edges) or 1,
            n_imports=len(edges), edges=list(edges),
            scanned_at='2026-01-01T00:00:00+00:00',
        )
    return entry


def _index(*entries):
    idx = org_index.OrgIndex(org='testorg')
    for e in entries:
        idx.repos[e.repo_id] = e
    return idx


# =============================================================================
# Cache location
# =============================================================================

class TestCacheLocation:

    def test_honours_configured_cache_dir(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            org_index.cfg, 'get_value',
            lambda key, default=None: str(tmp_path) if key == 'deps.cache_dir'
            else None,
        )
        assert org_index.cache_root() == tmp_path

    def test_falls_back_to_user_cache(self, monkeypatch):
        monkeypatch.setattr(
            org_index.cfg, 'get_value', lambda key, default=None: None
        )
        root = org_index.cache_root()
        assert root.name == 'repo-deps'

    def test_org_dir_is_namespaced_by_host(self, cache_dir):
        path = org_index.org_cache_dir('myorg', 'github.com')
        assert path == cache_dir / 'github.com' / 'myorg'


# =============================================================================
# Persistence
# =============================================================================

class TestPersistence:

    def test_roundtrip(self, cache_dir):
        idx = _index(
            _entry('github.com/testorg/a',
                   edges=[_edge('github.com/testorg/b', 'github.com/testorg/a', 3)]),
            _entry('github.com/testorg/b'),
        )
        org_index.save_index(idx)

        loaded = org_index.load_index('testorg')
        assert set(loaded.repos) == set(idx.repos)

        edges = loaded.repos['github.com/testorg/a'].edges
        assert len(edges) == 1
        assert edges[0].source == 'github.com/testorg/b'
        assert edges[0].n_imports == 3

    def test_missing_cache_returns_empty(self, cache_dir):
        loaded = org_index.load_index('never-built')
        assert loaded.repos == {}

    def test_schema_mismatch_discards_cache(self, cache_dir):
        org_index.save_index(_index(_entry('github.com/testorg/a')))

        manifest = cache_dir / 'github.com' / 'testorg' / 'index.json'
        data = json.loads(manifest.read_text())
        data['schema_version'] = org_index.SCHEMA_VERSION + 1
        manifest.write_text(json.dumps(data))

        # Stale format is dropped rather than mis-parsed.
        assert org_index.load_index('testorg').repos == {}

    def test_corrupt_manifest_returns_empty(self, cache_dir):
        base = cache_dir / 'github.com' / 'testorg'
        base.mkdir(parents=True)
        (base / 'index.json').write_text('{not json')
        assert org_index.load_index('testorg').repos == {}

    def test_corrupt_edges_file_does_not_lose_whole_index(self, cache_dir):
        """One unreadable repo must not poison the cache."""
        org_index.save_index(_index(
            _entry('github.com/testorg/a'), _entry('github.com/testorg/b'),
        ))
        edges_dir = cache_dir / 'github.com' / 'testorg' / 'edges'
        (edges_dir / 'github.com__testorg__a.json').write_text('{broken')

        loaded = org_index.load_index('testorg')
        assert set(loaded.repos) == {
            'github.com/testorg/a', 'github.com/testorg/b',
        }
        assert loaded.repos['github.com/testorg/a'].scans == {}
        assert loaded.repos['github.com/testorg/b'].scans

    def test_scans_pruned_to_limit(self, cache_dir):
        entry = _entry('github.com/testorg/a')
        for i in range(10):
            entry.scans[f'sha{i}'] = org_index.ScanRecord(
                sha=f'sha{i}', ref='main', n_dvc_files=1, n_imports=0,
                scanned_at=f'2026-01-{i + 1:02d}T00:00:00+00:00',
            )
        org_index.save_index(_index(entry))

        loaded = org_index.load_index('testorg')
        assert len(loaded.repos['github.com/testorg/a'].scans) \
            == org_index.MAX_SCANS_PER_REPO

    def test_clear_index(self, cache_dir):
        org_index.save_index(_index(_entry('github.com/testorg/a')))
        assert org_index.clear_index('testorg') is True
        assert org_index.load_index('testorg').repos == {}
        assert org_index.clear_index('testorg') is False

    def test_scan_method_is_recorded(self, cache_dir):
        """A zero-edge result must say whether it was cloned or inferred."""
        entry = _entry('github.com/testorg/a', sha='t1')
        entry.scans['t1'].method = 'tree-api'
        org_index.save_index(_index(entry))

        loaded = org_index.load_index('testorg')
        assert loaded.repos['github.com/testorg/a'].scans['t1'].method \
            == 'tree-api'


# =============================================================================
# GitHub access (mocked)
# =============================================================================

class TestListOrgRepos:

    def _gh_result(self, repos):
        return MagicMock(
            returncode=0,
            stdout='\n'.join(json.dumps(r) for r in repos),
            stderr='',
        )

    def test_filters_archived_forks_and_empty(self):
        repos = [
            {'name': 'keep', 'ssh_url': 'git@github.com:o/keep.git',
             'archived': False, 'fork': False, 'size': 100},
            {'name': 'arch', 'ssh_url': 'git@github.com:o/arch.git',
             'archived': True, 'fork': False, 'size': 100},
            {'name': 'forked', 'ssh_url': 'git@github.com:o/forked.git',
             'archived': False, 'fork': True, 'size': 100},
            {'name': 'empty', 'ssh_url': 'git@github.com:o/empty.git',
             'archived': False, 'fork': False, 'size': 0},
        ]
        with patch.object(org_index, '_check_gh'), \
                patch('subprocess.run', return_value=self._gh_result(repos)):
            got = org_index.list_org_repos('o')

        assert [r['name'] for r in got] == ['keep']

    def test_include_flags(self):
        repos = [
            {'name': 'arch', 'ssh_url': 'git@github.com:o/arch.git',
             'archived': True, 'fork': False, 'size': 100},
        ]
        with patch.object(org_index, '_check_gh'), \
                patch('subprocess.run', return_value=self._gh_result(repos)):
            got = org_index.list_org_repos('o', include_archived=True)

        assert [r['name'] for r in got] == ['arch']

    def test_failure_raises_deps_error(self):
        fail = MagicMock(returncode=1, stdout='', stderr='Not Found')
        with patch.object(org_index, '_check_gh'), \
                patch('subprocess.run', return_value=fail):
            with pytest.raises(DepsError, match='Could not list repositories'):
                org_index.list_org_repos('nope')


class TestProbeDvcFiles:

    def _result(self, payload):
        return MagicMock(returncode=0, stdout=json.dumps(payload), stderr='')

    def test_counts_dvc_files(self):
        with patch('subprocess.run',
                   return_value=self._result(
                       {'sha': 'tree1', 'truncated': False, 'n': 27})):
            probe = org_index.probe_dvc_files('o', 'r', 'main')
        assert probe.n_dvc_files == 27
        assert probe.tree_sha == 'tree1'

    def test_zero_is_definitive(self):
        with patch('subprocess.run',
                   return_value=self._result(
                       {'sha': 'tree1', 'truncated': False, 'n': 0})):
            probe = org_index.probe_dvc_files('o', 'r', 'main')
        assert probe.n_dvc_files == 0

    def test_truncated_is_unknown_not_zero(self):
        """A truncated tree must not be mistaken for 'no .dvc files'."""
        with patch('subprocess.run',
                   return_value=self._result(
                       {'sha': 'tree1', 'truncated': True, 'n': 0})):
            probe = org_index.probe_dvc_files('o', 'r', 'main')
        assert probe.n_dvc_files is None
        assert probe.truncated is True

    def test_api_failure_is_unknown(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=1, stdout='', stderr='x')):
            probe = org_index.probe_dvc_files('o', 'r', 'main')
        assert probe.n_dvc_files is None


class TestLsRemoteHeads:

    def test_parses_heads(self):
        out = (
            'abc123\trefs/heads/main\n'
            'def456\trefs/heads/dev\n'
            'ghi789\trefs/tags/v1\n'
        )
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout=out, stderr='')):
            heads = org_index.ls_remote_heads('git@github.com:o/r.git')
        assert heads == {'main': 'abc123', 'dev': 'def456'}

    def test_failure_returns_empty(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=1, stdout='', stderr='')):
            assert org_index.ls_remote_heads('bad') == {}


# =============================================================================
# Refresh staleness logic
# =============================================================================

class TestRefreshStaleness:
    """pushed_at is the whole cost model -- these are the rules that matter."""

    def _listing(self, pushed_at):
        return [{
            'name': 'a', 'ssh_url': 'git@github.com:testorg/a.git',
            'default_branch': 'main', 'pushed_at': pushed_at,
            'archived': False, 'fork': False, 'size': 10,
        }]

    def test_unchanged_repo_is_not_scanned(self, cache_dir):
        org_index.save_index(_index(
            _entry('github.com/testorg/a', pushed_at='2026-01-01T00:00:00Z')
        ))

        with patch.object(org_index, 'list_org_repos',
                          return_value=self._listing('2026-01-01T00:00:00Z')), \
                patch.object(org_index, 'probe_dvc_files') as probe, \
                patch.object(org_index, '_scan_repo_for_index') as scan:
            _, stats = org_index.refresh_index('testorg')

        assert stats['skipped'] == 1
        scan.assert_not_called()
        probe.assert_not_called()

    def test_moved_repo_is_rescanned(self, cache_dir):
        org_index.save_index(_index(
            _entry('github.com/testorg/a', pushed_at='2026-01-01T00:00:00Z')
        ))

        with patch.object(org_index, 'list_org_repos',
                          return_value=self._listing('2026-06-01T00:00:00Z')), \
                patch.object(org_index, 'probe_dvc_files',
                             return_value=org_index.TreeProbe('t1', 5)), \
                patch.object(org_index, '_scan_repo_for_index',
                             side_effect=lambda e, *a: e) as scan:
            _, stats = org_index.refresh_index('testorg')

        assert stats['skipped'] == 0
        scan.assert_called_once()

    def test_force_rescans_unchanged_repo(self, cache_dir):
        org_index.save_index(_index(
            _entry('github.com/testorg/a', pushed_at='2026-01-01T00:00:00Z')
        ))

        with patch.object(org_index, 'list_org_repos',
                          return_value=self._listing('2026-01-01T00:00:00Z')), \
                patch.object(org_index, 'probe_dvc_files',
                             return_value=org_index.TreeProbe('t1', 5)), \
                patch.object(org_index, '_scan_repo_for_index',
                             side_effect=lambda e, *a: e) as scan:
            _, stats = org_index.refresh_index('testorg', force=True)

        assert stats['skipped'] == 0
        scan.assert_called_once()

    def test_prefilter_skips_cloning_repos_without_dvc(self, cache_dir):
        with patch.object(org_index, 'list_org_repos',
                          return_value=self._listing('2026-01-01T00:00:00Z')), \
                patch.object(org_index, 'probe_dvc_files',
                             return_value=org_index.TreeProbe('t1', 0)), \
                patch.object(org_index, '_scan_repo_for_index') as scan:
            index, stats = org_index.refresh_index('testorg')

        assert stats['no_dvc'] == 1
        scan.assert_not_called()
        record = index.repos['github.com/testorg/a'].scans['t1']
        assert record.method == 'tree-api'
        assert record.n_dvc_files == 0

    def test_truncated_probe_forces_a_clone(self, cache_dir):
        """Unknown must not be treated as 'no .dvc files'."""
        with patch.object(org_index, 'list_org_repos',
                          return_value=self._listing('2026-01-01T00:00:00Z')), \
                patch.object(org_index, 'probe_dvc_files',
                             return_value=org_index.TreeProbe(
                                 't1', None, truncated=True)), \
                patch.object(org_index, '_scan_repo_for_index',
                             side_effect=lambda e, *a: e) as scan:
            org_index.refresh_index('testorg')

        scan.assert_called_once()

    def test_failed_scan_does_not_commit_pushed_at(self, cache_dir):
        """Otherwise a transient failure would never be retried."""
        def fail(entry, *args):
            entry.status = repo_graph.STATUS_CLONE_FAILED
            entry.detail = 'boom'
            return entry

        with patch.object(org_index, 'list_org_repos',
                          return_value=self._listing('2026-06-01T00:00:00Z')), \
                patch.object(org_index, 'probe_dvc_files',
                             return_value=org_index.TreeProbe('t1', 5)), \
                patch.object(org_index, '_scan_repo_for_index',
                             side_effect=fail):
            index, stats = org_index.refresh_index('testorg')

        assert stats['failed'] == 1
        assert index.repos['github.com/testorg/a'].pushed_at != \
            '2026-06-01T00:00:00Z'

    def test_limit_leaves_remainder_for_next_run(self, cache_dir):
        listing = [
            {'name': f'r{i}', 'ssh_url': f'git@github.com:testorg/r{i}.git',
             'default_branch': 'main', 'pushed_at': '2026-01-01T00:00:00Z',
             'archived': False, 'fork': False, 'size': 10}
            for i in range(10)
        ]
        with patch.object(org_index, 'list_org_repos', return_value=listing), \
                patch.object(org_index, 'probe_dvc_files',
                             return_value=org_index.TreeProbe('t1', 5)), \
                patch.object(org_index, '_scan_repo_for_index',
                             side_effect=lambda e, *a: e) as scan:
            _, stats = org_index.refresh_index('testorg', limit=3)

        assert scan.call_count == 3
        assert stats['limit_dropped'] == 7

    def test_repo_removed_from_org_is_dropped(self, cache_dir):
        org_index.save_index(_index(
            _entry('github.com/testorg/a'), _entry('github.com/testorg/gone'),
        ))
        with patch.object(org_index, 'list_org_repos',
                          return_value=self._listing('2026-01-01T00:00:00Z')):
            index, _ = org_index.refresh_index('testorg')

        assert 'github.com/testorg/gone' not in index.repos


# =============================================================================
# Downstream traversal
# =============================================================================

class TestDownstream:

    def _populated(self):
        # b imports from a; c imports from b; d imports from a
        return _index(
            _entry('github.com/testorg/a'),
            _entry('github.com/testorg/b',
                   edges=[_edge('github.com/testorg/a', 'github.com/testorg/b', 2)]),
            _entry('github.com/testorg/c',
                   edges=[_edge('github.com/testorg/b', 'github.com/testorg/c')]),
            _entry('github.com/testorg/d',
                   edges=[_edge('github.com/testorg/a', 'github.com/testorg/d')]),
        )

    def test_invert_edges(self):
        inverted = org_index.invert_edges(self._populated())
        assert {e.target for e in inverted['github.com/testorg/a']} == {
            'github.com/testorg/b', 'github.com/testorg/d',
        }

    def test_direct_and_transitive_consumers(self):
        graph = org_index.downstream_graph(
            self._populated(), 'github.com/testorg/a'
        )
        assert set(graph.nodes) == {
            'github.com/testorg/a', 'github.com/testorg/b',
            'github.com/testorg/c', 'github.com/testorg/d',
        }
        assert graph.nodes['github.com/testorg/c'].depth == 2

    def test_depth_limits_expansion(self):
        graph = org_index.downstream_graph(
            self._populated(), 'github.com/testorg/a', depth=1
        )
        assert 'github.com/testorg/c' not in graph.nodes
        assert 'github.com/testorg/b' in graph.nodes

    def test_leaf_has_no_consumers(self):
        graph = org_index.downstream_graph(
            self._populated(), 'github.com/testorg/c'
        )
        assert set(graph.nodes) == {'github.com/testorg/c'}

    def test_cycle_terminates(self):
        idx = _index(
            _entry('github.com/testorg/a',
                   edges=[_edge('github.com/testorg/b', 'github.com/testorg/a')]),
            _entry('github.com/testorg/b',
                   edges=[_edge('github.com/testorg/a', 'github.com/testorg/b')]),
        )
        graph = org_index.downstream_graph(idx, 'github.com/testorg/a')
        assert set(graph.nodes) == {
            'github.com/testorg/a', 'github.com/testorg/b',
        }
        assert graph.cycles == [[
            'github.com/testorg/a', 'github.com/testorg/b',
        ]]

    def test_unknown_root_yields_lone_node(self):
        graph = org_index.downstream_graph(
            self._populated(), 'github.com/testorg/never-heard-of-it'
        )
        assert len(graph.nodes) == 1


class TestMergeGraphs:

    def test_merges_nodes_and_edges(self):
        up = repo_graph.RepoGraph(root='r')
        up.nodes['r'] = repo_graph.RepoNode('r', is_root=True)
        up.nodes['src'] = repo_graph.RepoNode('src', depth=1)
        up.edges[('src', 'r')] = _edge('src', 'r')

        down = repo_graph.RepoGraph(root='r')
        down.nodes['r'] = repo_graph.RepoNode('r', is_root=True)
        down.nodes['consumer'] = repo_graph.RepoNode('consumer', depth=1)
        down.edges[('r', 'consumer')] = _edge('r', 'consumer')

        merged = org_index.merge_graphs(up, down)
        assert set(merged.nodes) == {'r', 'src', 'consumer'}
        assert set(merged.edges) == {('src', 'r'), ('r', 'consumer')}

    def test_upstream_node_wins_on_conflict(self):
        up = repo_graph.RepoGraph(root='r')
        up.nodes['x'] = repo_graph.RepoNode('x', depth=1, n_dvc_files=99)
        down = repo_graph.RepoGraph(root='r')
        down.nodes['x'] = repo_graph.RepoNode('x', depth=5, n_dvc_files=0)

        merged = org_index.merge_graphs(up, down)
        assert merged.nodes['x'].n_dvc_files == 99


# =============================================================================
# Summary
# =============================================================================

class TestSummary:

    def test_counts(self, cache_dir):
        idx = _index(
            _entry('github.com/testorg/a'),
            _entry('github.com/testorg/b',
                   edges=[_edge('github.com/testorg/a', 'github.com/testorg/b')]),
            _entry('github.com/testorg/gone', sha='',
                   status=repo_graph.STATUS_NO_ACCESS),
        )
        s = idx.summary()
        assert s['n_repos'] == 3
        assert s['n_edges'] == 1
        assert s['n_importing_repos'] == 1
        assert s['n_gaps'] == 1

    def test_format_reports_limit_truncation(self, cache_dir):
        """A capped run must say so rather than look complete."""
        out = org_index.format_index_summary(
            _index(_entry('github.com/testorg/a')),
            {'scanned': 1, 'skipped': 0, 'failed': 0, 'no_dvc': 0,
             'limit_dropped': 42},
        )
        assert '42 repo(s) left unscanned' in out

    def test_format_lists_gaps(self, cache_dir):
        out = org_index.format_index_summary(_index(
            _entry('github.com/testorg/gone', sha='',
                   status=repo_graph.STATUS_NO_ACCESS),
        ))
        assert 'Unreadable repos' in out
        assert 'testorg/gone' in out
