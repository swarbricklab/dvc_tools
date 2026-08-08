"""Unit tests for dt.repo_graph and dt.repo_graph_render.

Cycle detection and error classification are tested directly. Traversal is
tested end-to-end against a network of real local git repos wired together with
file:// URLs, which exercises the actual clone/scan path without network access.
"""

import json
import subprocess

import pytest

from dt import repo_graph, repo_graph_render
from dt.dvc_deps import RepoEdge
from dt.errors import DepsError


# =============================================================================
# Helpers
# =============================================================================

def _edge(source, target, n=1):
    return RepoEdge(source=source, target=target, n_imports=n)


def _edges(*pairs):
    return {(s, t): _edge(s, t) for s, t in pairs}


def _run(args, cwd):
    return subprocess.run(
        ['git', *args], cwd=str(cwd), capture_output=True, text=True, check=True
    )


def _make_repo(path, origin_url=None):
    """Create a git repo with one commit, optionally with an origin remote."""
    path.mkdir(parents=True, exist_ok=True)
    _run(['init', '-q', '-b', 'main'], path)
    _run(['config', 'user.email', 'test@example.com'], path)
    _run(['config', 'user.name', 'Test'], path)
    if origin_url:
        _run(['remote', 'add', 'origin', origin_url], path)
    (path / 'README.md').write_text('seed\n')
    _run(['add', '-A'], path)
    _run(['commit', '-q', '-m', 'seed'], path)
    return path


def _add_import(repo, name, source_url, source_path='data/thing'):
    """Write an import .dvc file in ``repo`` pointing at ``source_url``."""
    (repo / f'{name}.dvc').write_text(
        "md5: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
        "frozen: true\n"
        "deps:\n"
        f"- path: {source_path}\n"
        "  repo:\n"
        f"    url: {source_url}\n"
        "    rev_lock: 1111111111111111111111111111111111111111\n"
        "outs:\n"
        "- md5: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n"
        "  size: 100\n"
        "  hash: md5\n"
        f"  path: {name}\n"
    )


def _commit(repo, message='update'):
    _run(['add', '-A'], repo)
    _run(['commit', '-q', '-m', message], repo)


@pytest.fixture
def network(tmp_path, monkeypatch):
    """A small network of local repos: root -> mid -> leaf, plus an orphan.

    Repos are wired with file:// URLs so the real clone path runs offline.
    Returns the root repo path.
    """
    remotes = tmp_path / 'remotes'
    leaf = _make_repo(remotes / 'leaf')
    mid = _make_repo(remotes / 'mid')
    root = _make_repo(tmp_path / 'root', origin_url=f'file://{remotes}/rootrepo')

    _add_import(mid, 'from_leaf', f'file://{leaf}')
    _commit(mid)

    _add_import(root, 'from_mid', f'file://{mid}')
    _commit(root)

    # .dt/tmp/clones lives under the root project.
    monkeypatch.chdir(root)
    return root


# =============================================================================
# Cycle detection
# =============================================================================

class TestDetectCycles:
    """Cycles are legitimate here, so they must be found, not raised on."""

    def test_acyclic_graph_has_no_cycles(self):
        edges = _edges(('a', 'b'), ('b', 'c'))
        assert repo_graph.detect_cycles(edges) == []

    def test_self_loop(self):
        edges = _edges(('a', 'a'))
        assert repo_graph.detect_cycles(edges) == [['a']]

    def test_two_node_cycle(self):
        edges = _edges(('a', 'b'), ('b', 'a'))
        assert repo_graph.detect_cycles(edges) == [['a', 'b']]

    def test_longer_cycle(self):
        edges = _edges(('a', 'b'), ('b', 'c'), ('c', 'a'))
        assert repo_graph.detect_cycles(edges) == [['a', 'b', 'c']]

    def test_cycle_with_acyclic_tail(self):
        edges = _edges(('a', 'b'), ('b', 'a'), ('b', 'c'), ('c', 'd'))
        assert repo_graph.detect_cycles(edges) == [['a', 'b']]

    def test_two_disjoint_cycles(self):
        edges = _edges(('a', 'b'), ('b', 'a'), ('x', 'y'), ('y', 'x'))
        assert repo_graph.detect_cycles(edges) == [['a', 'b'], ['x', 'y']]

    def test_empty_graph(self):
        assert repo_graph.detect_cycles({}) == []

    def test_deep_chain_does_not_hit_recursion_limit(self):
        """Traversal is iterative, so a long chain must not blow the stack."""
        edges = {}
        for i in range(2000):
            edges[(f'r{i}', f'r{i + 1}')] = _edge(f'r{i}', f'r{i + 1}')
        assert repo_graph.detect_cycles(edges) == []


# =============================================================================
# Error classification
# =============================================================================

class TestClassifyGitError:

    def test_permission_denied_is_no_access(self):
        status, _ = repo_graph._classify_git_error(
            'git@github.com: Permission denied (publickey).'
        )
        assert status == repo_graph.STATUS_NO_ACCESS

    def test_github_repository_not_found_is_no_access(self):
        """GitHub returns this for private-no-access as well as missing."""
        status, detail = repo_graph._classify_git_error(
            'ERROR: Repository not found.\nfatal: Could not read from remote repository.'
        )
        assert status == repo_graph.STATUS_NO_ACCESS
        assert 'Repository not found' in detail

    def test_404_is_not_found(self):
        status, _ = repo_graph._classify_git_error(
            'fatal: unable to access ... The requested URL returned error: 404'
        )
        assert status == repo_graph.STATUS_NOT_FOUND

    def test_other_error_is_clone_failed(self):
        status, _ = repo_graph._classify_git_error(
            'fatal: unable to connect: Connection timed out'
        )
        assert status == repo_graph.STATUS_CLONE_FAILED

    def test_detail_is_preserved(self):
        _, detail = repo_graph._classify_git_error('  some raw git text  ')
        assert detail == 'some raw git text'


class TestUrlForRepoId:

    def test_reconstructs_ssh_url(self):
        assert repo_graph.url_for_repo_id('github.com/org/repo') \
            == 'git@github.com:org/repo.git'

    def test_passes_through_unrecognisable_id(self):
        assert repo_graph.url_for_repo_id('weird') == 'weird'


# =============================================================================
# Traversal
# =============================================================================

class TestBuildUpstream:

    def test_rejects_unknown_mode(self, network):
        with pytest.raises(DepsError, match='Unknown mode'):
            repo_graph.build_upstream(root=network, mode='sideways')

    def test_depth_one_does_not_clone(self, network):
        """Depth 1 is the direct-sources case and must not touch the network."""
        graph = repo_graph.build_upstream(root=network, depth=1)

        assert len(graph.edges) == 1
        source = next(iter(graph.edges))[0]
        assert graph.nodes[source].status == repo_graph.STATUS_NOT_SCANNED
        assert graph.truncated
        # Depth truncation is requested, not an access failure.
        assert graph.gaps == []

    def test_recursive_traversal_finds_transitive_source(self, network):
        graph = repo_graph.build_upstream(root=network)

        ids = set(graph.nodes)
        # root -> mid -> leaf, all discovered by following imports.
        assert len(ids) == 3
        assert graph.gaps == []
        assert len(graph.edges) == 2

        depths = {n.depth for n in graph.nodes.values()}
        assert depths == {0, 1, 2}

    def test_leaf_with_no_dvc_files_is_not_a_gap(self, network):
        graph = repo_graph.build_upstream(root=network)
        leaf = next(n for n in graph.nodes.values()
                    if n.depth == 2)
        assert leaf.status == repo_graph.STATUS_NO_DVC
        assert leaf.is_gap is False

    def test_unreachable_source_recorded_as_gap(self, tmp_path, monkeypatch):
        """A source we cannot clone becomes a node, not a silent omission."""
        root = _make_repo(tmp_path / 'root',
                          origin_url='git@github.com:org/root.git')
        _add_import(root, 'missing', f'file://{tmp_path}/nope-does-not-exist')
        _commit(root)
        monkeypatch.chdir(root)

        graph = repo_graph.build_upstream(root=root)

        assert len(graph.gaps) == 1
        gap = graph.gaps[0]
        assert gap.status in repo_graph.GAP_STATUSES
        assert gap.detail
        # The edge to it still exists -- we know it is depended on.
        assert len(graph.edges) == 1

    def test_cycle_terminates_traversal(self, tmp_path, monkeypatch):
        """Mutually importing repos must not loop forever."""
        remotes = tmp_path / 'remotes'
        a = _make_repo(remotes / 'a')
        b = _make_repo(remotes / 'b')

        _add_import(a, 'from_b', f'file://{b}')
        _commit(a)
        _add_import(b, 'from_a', f'file://{a}')
        _commit(b)

        root = _make_repo(tmp_path / 'root',
                          origin_url='git@github.com:org/root.git')
        _add_import(root, 'from_a', f'file://{a}')
        _commit(root)
        monkeypatch.chdir(root)

        graph = repo_graph.build_upstream(root=root)

        assert graph.cycles, "expected the a<->b cycle to be reported"
        cycle = graph.cycles[0]
        assert len(cycle) == 2

    def test_uses_source_url_from_edge(self, network):
        """Cloning must use the URL as written, not a reconstructed one."""
        graph = repo_graph.build_upstream(root=network, depth=1)
        edge = next(iter(graph.edges.values()))
        assert edge.source_url.startswith('file://')

    def test_no_refresh_reuses_clone(self, network):
        first = repo_graph.build_upstream(root=network)
        second = repo_graph.build_upstream(root=network, refresh=False)
        assert set(first.nodes) == set(second.nodes)
        assert set(first.edges) == set(second.edges)


# =============================================================================
# Rendering
# =============================================================================

class TestRendering:

    def _graph(self):
        g = repo_graph.RepoGraph(root='github.com/org/root')
        g.nodes['github.com/org/root'] = repo_graph.RepoNode(
            'github.com/org/root', depth=0, is_root=True,
        )
        g.nodes['github.com/org/mid'] = repo_graph.RepoNode(
            'github.com/org/mid', depth=1,
        )
        g.nodes['github.com/org/gone'] = repo_graph.RepoNode(
            'github.com/org/gone', depth=1,
            status=repo_graph.STATUS_NO_ACCESS,
            detail='ERROR: Repository not found.',
        )
        g.edges[('github.com/org/mid', 'github.com/org/root')] = _edge(
            'github.com/org/mid', 'github.com/org/root', n=3,
        )
        g.edges[('github.com/org/gone', 'github.com/org/root')] = _edge(
            'github.com/org/gone', 'github.com/org/root',
        )
        return g

    def test_text_renders_tree(self):
        out = repo_graph_render.render_text(self._graph())
        assert 'org/root' in out
        assert 'org/mid' in out
        assert '3 imports' in out

    def test_text_flags_gap_nodes(self):
        out = repo_graph_render.render_text(self._graph())
        assert 'NO_ACCESS' in out
        assert 'Access gaps' in out

    def test_text_reports_cycles(self):
        g = self._graph()
        g.cycles = [['github.com/org/a', 'github.com/org/b']]
        out = repo_graph_render.render_text(g)
        assert 'Cycles:' in out
        assert 'org/a <-> org/b' in out

    def test_text_reports_self_loop_readably(self):
        g = self._graph()
        g.cycles = [['github.com/org/a']]
        out = repo_graph_render.render_text(g)
        assert 'imports from itself' in out

    def test_shared_subtree_printed_once(self):
        """A diamond must not reprint the shared upstream's subtree."""
        g = repo_graph.RepoGraph(root='r')
        for name in ('r', 'a', 'b', 'shared', 'deep'):
            g.nodes[name] = repo_graph.RepoNode(name)
        for pair in (('a', 'r'), ('b', 'r'),
                     ('shared', 'a'), ('shared', 'b'), ('deep', 'shared')):
            g.edges[pair] = _edge(*pair)

        out = repo_graph_render.render_text(g)
        assert out.count('deep') == 1
        assert repo_graph_render.SEEN_MARK in out

    def test_cycle_marked_in_tree_not_infinite(self):
        g = repo_graph.RepoGraph(root='r')
        for name in ('r', 'a', 'b'):
            g.nodes[name] = repo_graph.RepoNode(name)
        g.edges[('a', 'r')] = _edge('a', 'r')
        g.edges[('b', 'a')] = _edge('b', 'a')
        g.edges[('a', 'b')] = _edge('a', 'b')

        out = repo_graph_render.render_text(g)
        assert repo_graph_render.CYCLE_MARK in out

    def test_gaps_report_includes_hint(self):
        out = repo_graph_render.format_gaps(self._graph())
        assert 'dt auth request' in out
        assert 'org/gone' in out

    def test_gaps_report_when_clean(self):
        g = repo_graph.RepoGraph(root='r')
        g.nodes['r'] = repo_graph.RepoNode('r', is_root=True)
        assert 'none' in repo_graph_render.format_gaps(g)

    def test_mermaid_is_wellformed(self):
        out = repo_graph_render.render_mermaid(self._graph())
        assert out.startswith('```mermaid')
        assert out.rstrip().endswith('```')
        assert 'flowchart LR' in out
        assert '-->' in out

    def test_mermaid_marks_gap_nodes(self):
        out = repo_graph_render.render_mermaid(self._graph())
        assert 'no_access' in out
        assert 'stroke-dasharray' in out

    def test_dot_is_wellformed(self):
        out = repo_graph_render.render_dot(self._graph())
        assert out.startswith('digraph repo_deps {')
        assert out.rstrip().endswith('}')
        assert '"github.com/org/mid" -> "github.com/org/root"' in out

    def test_json_roundtrips(self):
        data = json.loads(repo_graph_render.render_json(self._graph()))
        assert data['root'] == 'github.com/org/root'
        assert len(data['nodes']) == 3
        assert len(data['edges']) == 2
        assert len(data['gaps']) == 1

    def test_render_dispatch_rejects_unknown_format(self):
        with pytest.raises(ValueError, match='Unknown format'):
            repo_graph_render.render(self._graph(), 'ascii-art')

    def _directional(self):
        """root has one source (up) and one consumer (down)."""
        g = repo_graph.RepoGraph(root='github.com/org/root')
        for name in ('github.com/org/root', 'github.com/org/src',
                     'github.com/org/consumer'):
            g.nodes[name] = repo_graph.RepoNode(name)
        g.edges[('github.com/org/src', 'github.com/org/root')] = _edge(
            'github.com/org/src', 'github.com/org/root')
        g.edges[('github.com/org/root', 'github.com/org/consumer')] = _edge(
            'github.com/org/root', 'github.com/org/consumer')
        return g

    def test_direction_up_shows_only_sources(self):
        out = repo_graph_render.render_text(self._directional(), direction='up')
        assert 'org/src' in out
        assert 'org/consumer' not in out

    def test_direction_down_shows_only_consumers(self):
        out = repo_graph_render.render_text(
            self._directional(), direction='down'
        )
        assert 'org/consumer' in out
        assert 'org/src' not in out
        assert 'Repos importing from' in out

    def test_direction_both_shows_two_trees(self):
        out = repo_graph_render.render_text(
            self._directional(), direction='both'
        )
        assert 'Upstream' in out and 'Downstream' in out
        assert 'org/src' in out and 'org/consumer' in out

    def test_direction_down_with_no_consumers(self):
        g = repo_graph.RepoGraph(root='r')
        g.nodes['r'] = repo_graph.RepoNode('r', is_root=True)
        out = repo_graph_render.render_text(g, direction='down')
        assert 'no importing repos found' in out

    def test_render_text_rejects_unknown_direction(self):
        with pytest.raises(ValueError, match='Unknown direction'):
            repo_graph_render.render_text(self._graph(), direction='sideways')
