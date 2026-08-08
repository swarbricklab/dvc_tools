"""Unit tests for dt.dvc_deps module.

Covers repo-id normalisation, .dvc parsing, edge aggregation, and the
git-ref-backed scanning path (exercised against real throwaway git repos,
which is where the subtle bugs live).
"""

import json
import subprocess

import pytest

from dt import dvc_deps
from dt.errors import DepsError


# =============================================================================
# Fixture content
# =============================================================================

IMPORT_DVC = """\
md5: ae51b2729e737c5819ff0113342d2549
frozen: true
deps:
- path: data/final
  repo:
    url: git@github.com:Swarbricklab/metadata.git
    rev_lock: 9b0730c70709fb8d73b0a142ea27250821a3e1ac
    rev: main
outs:
- md5: ace1a88c79ce2590e589c2bf9afb828a.dir
  size: 1069722
  nfiles: 6
  hash: md5
  path: final
"""

IMPORT_DVC_NO_REV = """\
md5: 582944f71ceaba3d6f3c7db48ad15380
frozen: true
deps:
- path: data/overview/samplesheets.csv
  repo:
    url: git@github.com:Swarbricklab/projects.git
    rev_lock: 1f0718c5c7c6e2166ec1ed2f696fcf7f9963bbdb
outs:
- md5: 12f8564770021cebb42afef20b226717
  size: 163649
  hash: md5
  path: projects.csv
"""

PLAIN_DVC = """\
outs:
- md5: abcdef1234567890abcdef1234567890
  size: 1024
  hash: md5
  path: data.csv
"""

IMPORT_URL_DVC = """\
md5: 111144447777aaaa
deps:
- path: s3://some-bucket/some/key.csv
  etag: '"abc123"'
outs:
- md5: 22224444666688880000aaaaccccffff
  size: 4096
  hash: md5
  path: external.csv
"""


def _make_git_repo(path, origin_url='git@github.com:swarbricklab/testrepo.git'):
    """Create a git repo with an origin remote and one commit."""
    path.mkdir(parents=True, exist_ok=True)
    run = lambda *a: subprocess.run(
        ['git', *a], cwd=str(path), capture_output=True, check=True
    )
    run('init', '-q', '-b', 'main')
    run('config', 'user.email', 'test@example.com')
    run('config', 'user.name', 'Test')
    run('remote', 'add', 'origin', origin_url)
    (path / 'seed.txt').write_text('seed\n')
    run('add', '-A')
    run('commit', '-q', '-m', 'seed')
    return path


def _commit_all(path, message):
    subprocess.run(['git', 'add', '-A'], cwd=str(path),
                   capture_output=True, check=True)
    subprocess.run(['git', 'commit', '-q', '-m', message], cwd=str(path),
                   capture_output=True, check=True)


# =============================================================================
# normalize_repo_id
# =============================================================================

class TestNormalizeRepoId:
    """Repo identity is the join key for the whole graph."""

    @pytest.mark.parametrize('url', [
        'git@github.com:Swarbricklab/metadata.git',
        'git@github.com:swarbricklab/metadata',
        'https://github.com/swarbricklab/metadata',
        'https://github.com/Swarbricklab/metadata.git',
        'https://github.com/swarbricklab/metadata.git/',
    ])
    def test_github_spellings_collapse_to_one_id(self, url):
        """Real .dvc files mix casing and URL forms for the same repo."""
        assert dvc_deps.normalize_repo_id(url) == 'github.com/swarbricklab/metadata'

    def test_unknown_host_preserves_path_case(self):
        """Only known case-insensitive hosts get their path lowercased."""
        result = dvc_deps.normalize_repo_id('git@gitlab.internal:Team/Proj.git')
        assert result == 'gitlab.internal/Team/Proj'

    def test_host_is_always_lowercased(self):
        result = dvc_deps.normalize_repo_id('https://GitHub.com/Org/Repo')
        assert result == 'github.com/org/repo'

    def test_empty_url(self):
        assert dvc_deps.normalize_repo_id('') == ''

    def test_short_name_with_owner(self):
        result = dvc_deps.normalize_repo_id('metadata', owner='swarbricklab')
        assert result == 'github.com/swarbricklab/metadata'

    @pytest.mark.parametrize('spec', [
        'swarbricklab/metadata',
        'Swarbricklab/Metadata',
        'github.com/swarbricklab/metadata',
        'swarbricklab/metadata/',
    ])
    def test_bare_path_forms_from_command_line(self, spec):
        """owner/repo is what people type; it must not hit the sanitiser."""
        assert dvc_deps.normalize_repo_id(spec) \
            == 'github.com/swarbricklab/metadata'

    def test_bare_path_with_explicit_host_is_preserved(self):
        assert dvc_deps.normalize_repo_id('gitlab.internal/Team/Proj') \
            == 'gitlab.internal/Team/Proj'

    def test_unparseable_url_does_not_raise(self):
        """A bad URL must not abort a whole-repo scan."""
        assert dvc_deps.normalize_repo_id('!!! not a url !!!')


# =============================================================================
# parse_import_refs
# =============================================================================

class TestParseImportRefs:
    """Parsing is pure, so it is tested exhaustively against fixture text."""

    def test_parses_repo_import(self):
        imports, externals = dvc_deps.parse_import_refs(IMPORT_DVC, 'a.dvc')
        assert externals == []
        assert len(imports) == 1

        ref = imports[0]
        assert ref.repo_id == 'github.com/swarbricklab/metadata'
        assert ref.repo_url == 'git@github.com:Swarbricklab/metadata.git'
        assert ref.path == 'data/final'
        assert ref.out_path == 'final'
        assert ref.rev == 'main'
        assert ref.rev_lock == '9b0730c70709fb8d73b0a142ea27250821a3e1ac'
        assert ref.is_directory is True
        assert ref.nfiles == 6
        assert ref.size == 1069722

    def test_locked_rev_prefers_rev_lock(self):
        imports, _ = dvc_deps.parse_import_refs(IMPORT_DVC, 'a.dvc')
        assert imports[0].locked_rev.startswith('9b0730c7')

    def test_locked_rev_falls_back_to_rev(self):
        text = IMPORT_DVC.replace(
            '    rev_lock: 9b0730c70709fb8d73b0a142ea27250821a3e1ac\n', ''
        )
        imports, _ = dvc_deps.parse_import_refs(text, 'a.dvc')
        assert imports[0].locked_rev == 'main'

    def test_import_without_rev(self):
        imports, _ = dvc_deps.parse_import_refs(IMPORT_DVC_NO_REV, 'b.dvc')
        assert imports[0].rev is None
        assert imports[0].is_directory is False

    def test_plain_dvc_file_yields_nothing(self):
        assert dvc_deps.parse_import_refs(PLAIN_DVC, 'c.dvc') == ([], [])

    def test_import_url_is_external_not_repo(self):
        imports, externals = dvc_deps.parse_import_refs(IMPORT_URL_DVC, 'd.dvc')
        assert imports == []
        assert len(externals) == 1
        assert externals[0].scheme == 's3'
        assert externals[0].url == 's3://some-bucket/some/key.csv'

    def test_malformed_yaml_yields_nothing(self):
        """A broken file must not abort the scan."""
        assert dvc_deps.parse_import_refs('deps: [oops', 'e.dvc') == ([], [])

    def test_empty_file_yields_nothing(self):
        assert dvc_deps.parse_import_refs('', 'f.dvc') == ([], [])

    def test_non_dict_yaml_yields_nothing(self):
        assert dvc_deps.parse_import_refs('- just\n- a list\n', 'g.dvc') == ([], [])

    def test_ref_is_recorded(self):
        imports, _ = dvc_deps.parse_import_refs(IMPORT_DVC, 'a.dvc', ref='origin/dev')
        assert imports[0].ref == 'origin/dev'

    def test_multiple_deps_all_parsed(self):
        text = (
            "deps:\n"
            "- path: p1\n"
            "  repo:\n"
            "    url: git@github.com:org/one.git\n"
            "- path: p2\n"
            "  repo:\n"
            "    url: git@github.com:org/two.git\n"
            "outs:\n"
            "- md5: aaa\n"
            "  path: o1\n"
            "- md5: bbb\n"
            "  path: o2\n"
        )
        imports, _ = dvc_deps.parse_import_refs(text, 'multi.dvc')
        assert [i.repo_id for i in imports] == [
            'github.com/org/one', 'github.com/org/two',
        ]
        # Outs are paired positionally when the counts line up.
        assert [i.out_path for i in imports] == ['o1', 'o2']


# =============================================================================
# aggregate_edges
# =============================================================================

class TestAggregateEdges:
    """Aggregation is the thousands-of-imports to handful-of-repos reduction."""

    def _ref(self, repo_id, path, out_path, rev_lock='abc123', ref=None, size=10):
        return dvc_deps.ImportRef(
            dvc_file=f'{out_path}.dvc',
            repo_url=f'git@github.com:{repo_id}.git',
            repo_id=repo_id,
            path=path,
            rev=None,
            rev_lock=rev_lock,
            out_path=out_path,
            size=size,
            nfiles=None,
            is_directory=False,
            ref=ref,
        )

    def test_groups_by_source_repo(self):
        refs = [
            self._ref('github.com/org/a', 'p1', 'o1'),
            self._ref('github.com/org/a', 'p2', 'o2'),
            self._ref('github.com/org/b', 'p3', 'o3'),
        ]
        edges = dvc_deps.aggregate_edges(refs, 'github.com/org/target')
        assert [e.source for e in edges] == [
            'github.com/org/a', 'github.com/org/b',
        ]
        assert edges[0].n_imports == 2
        assert edges[1].n_imports == 1

    def test_same_import_across_branches_counted_once(self):
        """Scanning N branches must not multiply the import count by N."""
        refs = [
            self._ref('github.com/org/a', 'p1', 'o1', ref='main'),
            self._ref('github.com/org/a', 'p1', 'o1', ref='dev'),
            self._ref('github.com/org/a', 'p1', 'o1', ref='origin/main'),
        ]
        edges = dvc_deps.aggregate_edges(refs, 'github.com/org/target')
        assert len(edges) == 1
        assert edges[0].n_imports == 1
        assert edges[0].refs == ['dev', 'main', 'origin/main']

    def test_distinct_revs_collected(self):
        refs = [
            self._ref('github.com/org/a', 'p1', 'o1', rev_lock='aaa'),
            self._ref('github.com/org/a', 'p2', 'o2', rev_lock='bbb'),
            self._ref('github.com/org/a', 'p3', 'o3', rev_lock='aaa'),
        ]
        edges = dvc_deps.aggregate_edges(refs, 'github.com/org/target')
        assert edges[0].revs == ['aaa', 'bbb']

    def test_self_loop_flagged(self):
        refs = [self._ref('github.com/org/target', 'p1', 'o1')]
        edges = dvc_deps.aggregate_edges(refs, 'github.com/org/target')
        assert edges[0].is_self_loop is True

    def test_total_size_summed(self):
        refs = [
            self._ref('github.com/org/a', 'p1', 'o1', size=100),
            self._ref('github.com/org/a', 'p2', 'o2', size=250),
        ]
        edges = dvc_deps.aggregate_edges(refs, 'github.com/org/target')
        assert edges[0].total_size == 350

    def test_sample_paths_capped(self):
        refs = [
            self._ref('github.com/org/a', f'p{i}', f'o{i}') for i in range(20)
        ]
        edges = dvc_deps.aggregate_edges(refs, 'target', max_paths=3)
        assert len(edges[0].sample_paths) == 3
        assert edges[0].n_imports == 20

    def test_refs_without_repo_id_skipped(self):
        refs = [self._ref('', 'p1', 'o1')]
        assert dvc_deps.aggregate_edges(refs, 'target') == []

    def test_empty_input(self):
        assert dvc_deps.aggregate_edges([], 'target') == []


# =============================================================================
# Git-backed scanning
# =============================================================================

class TestGitScanning:
    """The ref-reading path, against real throwaway repos."""

    def test_scan_worktree(self, tmp_path):
        repo = _make_git_repo(tmp_path / 'repo')
        (repo / 'a.dvc').write_text(IMPORT_DVC)
        (repo / 'plain.dvc').write_text(PLAIN_DVC)
        _commit_all(repo, 'add imports')

        imports, externals = dvc_deps.scan_imports(repo)
        assert len(imports) == 1
        assert imports[0].repo_id == 'github.com/swarbricklab/metadata'
        assert imports[0].ref is None
        assert externals == []

    def test_scan_ref_matches_worktree(self, tmp_path):
        repo = _make_git_repo(tmp_path / 'repo')
        (repo / 'a.dvc').write_text(IMPORT_DVC)
        (repo / 'b.dvc').write_text(IMPORT_DVC_NO_REV)
        _commit_all(repo, 'add imports')

        from_tree, _ = dvc_deps.scan_imports(repo)
        from_ref, _ = dvc_deps.scan_imports(repo, ref='main')

        assert {i.repo_id for i in from_tree} == {i.repo_id for i in from_ref}
        assert all(i.ref == 'main' for i in from_ref)

    def test_scan_ref_does_not_touch_working_tree(self, tmp_path):
        """Reading a ref must not check anything out."""
        repo = _make_git_repo(tmp_path / 'repo')
        (repo / 'a.dvc').write_text(IMPORT_DVC)
        _commit_all(repo, 'main import')

        subprocess.run(['git', 'checkout', '-q', '-b', 'side'], cwd=str(repo),
                       capture_output=True, check=True)
        (repo / 'b.dvc').write_text(IMPORT_DVC_NO_REV)
        _commit_all(repo, 'side import')
        subprocess.run(['git', 'checkout', '-q', 'main'], cwd=str(repo),
                       capture_output=True, check=True)

        imports, _ = dvc_deps.scan_imports(repo, ref='side')
        assert {i.repo_id for i in imports} == {
            'github.com/swarbricklab/metadata',
            'github.com/swarbricklab/projects',
        }
        # Still on main, and side's file was never materialised.
        assert not (repo / 'b.dvc').exists()

    def test_scan_missing_ref_raises(self, tmp_path):
        repo = _make_git_repo(tmp_path / 'repo')
        with pytest.raises(DepsError, match='nope'):
            list(dvc_deps.iter_dvc_files_ref('nope', repo))

    def test_many_dvc_files_read_in_one_batch(self, tmp_path):
        """Exercises the cat-file --batch parser past a single buffer entry."""
        repo = _make_git_repo(tmp_path / 'repo')
        for i in range(50):
            (repo / f'f{i}.dvc').write_text(
                IMPORT_DVC.replace('path: final', f'path: final{i}')
            )
        _commit_all(repo, 'many')

        imports, _ = dvc_deps.scan_imports(repo, ref='main')
        assert len(imports) == 50
        assert {i.out_path for i in imports} == {f'final{i}' for i in range(50)}

    def test_list_refs_excludes_symbolic_origin_head(self, tmp_path):
        """refs/remotes/origin/HEAD shortens to plain 'origin' -- not a branch."""
        repo = _make_git_repo(tmp_path / 'repo')
        subprocess.run(
            ['git', 'update-ref', 'refs/remotes/origin/main', 'HEAD'],
            cwd=str(repo), capture_output=True, check=True,
        )
        subprocess.run(
            ['git', 'symbolic-ref', 'refs/remotes/origin/HEAD',
             'refs/remotes/origin/main'],
            cwd=str(repo), capture_output=True, check=True,
        )

        refs = dvc_deps.list_refs(repo)
        assert 'origin' not in refs
        assert 'main' in refs
        assert 'origin/main' in refs

    def test_scan_refs_unions_across_branches(self, tmp_path):
        repo = _make_git_repo(tmp_path / 'repo')
        (repo / 'a.dvc').write_text(IMPORT_DVC)
        _commit_all(repo, 'main import')

        subprocess.run(['git', 'checkout', '-q', '-b', 'side'], cwd=str(repo),
                       capture_output=True, check=True)
        (repo / 'b.dvc').write_text(IMPORT_DVC_NO_REV)
        _commit_all(repo, 'side import')

        imports, _ = dvc_deps.scan_refs(['main', 'side'], repo)
        edges = dvc_deps.aggregate_edges(imports, 'target')
        sources = {e.source for e in edges}

        # The side-branch-only import is found without checking it out.
        assert sources == {
            'github.com/swarbricklab/metadata',
            'github.com/swarbricklab/projects',
        }
        projects = next(e for e in edges
                        if e.source == 'github.com/swarbricklab/projects')
        assert projects.refs == ['side']

    def test_scan_refs_dedups_identical_shas(self, tmp_path):
        """Two refs at the same commit are scanned once but both annotated."""
        repo = _make_git_repo(tmp_path / 'repo')
        (repo / 'a.dvc').write_text(IMPORT_DVC)
        _commit_all(repo, 'import')
        subprocess.run(['git', 'branch', 'copy'], cwd=str(repo),
                       capture_output=True, check=True)

        imports, _ = dvc_deps.scan_refs(['main', 'copy'], repo)
        edges = dvc_deps.aggregate_edges(imports, 'target')

        assert len(edges) == 1
        assert edges[0].n_imports == 1          # not double-counted
        assert edges[0].refs == ['copy', 'main']  # but both refs recorded

    def test_scan_refs_skips_nonexistent_ref(self, tmp_path):
        repo = _make_git_repo(tmp_path / 'repo')
        (repo / 'a.dvc').write_text(IMPORT_DVC)
        _commit_all(repo, 'import')

        imports, _ = dvc_deps.scan_refs(['main', 'does-not-exist'], repo)
        assert len(imports) == 1

    def test_current_repo_id_from_origin(self, tmp_path):
        repo = _make_git_repo(
            tmp_path / 'repo', origin_url='git@github.com:Swarbricklab/Portal.git'
        )
        assert dvc_deps.current_repo_id(repo) == 'github.com/swarbricklab/portal'

    def test_current_repo_id_without_origin(self, tmp_path):
        repo = tmp_path / 'noremote'
        repo.mkdir()
        subprocess.run(['git', 'init', '-q'], cwd=str(repo),
                       capture_output=True, check=True)
        assert dvc_deps.current_repo_id(repo) is None

    def test_resolve_ref(self, tmp_path):
        repo = _make_git_repo(tmp_path / 'repo')
        sha = dvc_deps.resolve_ref('main', repo)
        assert sha and len(sha) == 40
        assert dvc_deps.resolve_ref('nope', repo) is None


# =============================================================================
# list_imports and rendering
# =============================================================================

class TestListImports:

    def test_end_to_end_worktree(self, tmp_path):
        repo = _make_git_repo(
            tmp_path / 'repo', origin_url='git@github.com:org/target.git'
        )
        (repo / 'a.dvc').write_text(IMPORT_DVC)
        (repo / 'ext.dvc').write_text(IMPORT_URL_DVC)
        _commit_all(repo, 'imports')

        result = dvc_deps.list_imports(root=repo)
        assert result.target == 'github.com/org/target'
        assert [e.source for e in result.edges] == [
            'github.com/swarbricklab/metadata',
        ]
        assert 's3' in result.externals

    def test_all_branches(self, tmp_path):
        repo = _make_git_repo(
            tmp_path / 'repo', origin_url='git@github.com:org/target.git'
        )
        (repo / 'a.dvc').write_text(IMPORT_DVC)
        _commit_all(repo, 'main')
        subprocess.run(['git', 'checkout', '-q', '-b', 'side'], cwd=str(repo),
                       capture_output=True, check=True)
        (repo / 'b.dvc').write_text(IMPORT_DVC_NO_REV)
        _commit_all(repo, 'side')

        result = dvc_deps.list_imports(root=repo, all_branches=True)
        assert sorted(result.scanned_refs) == ['main', 'side']
        assert len(result.edges) == 2


class TestFormatting:

    def _edges(self):
        return [dvc_deps.RepoEdge(
            source='github.com/org/a', target='github.com/org/t',
            n_imports=3, revs=['abc12345'], sample_paths=['p -> o'],
            total_size=1024, refs=['main'],
        )]

    def test_text_output(self):
        out = dvc_deps.format_edges(self._edges(), 'github.com/org/t')
        assert 'github.com/org/a' in out
        assert '3 imports' in out
        assert '1 source repo, 3 imports' in out

    def test_no_edges_message(self):
        out = dvc_deps.format_edges([], 'github.com/org/t')
        assert 'No repo imports found' in out

    def test_include_paths(self):
        out = dvc_deps.format_edges(
            self._edges(), 'github.com/org/t', include_paths=True
        )
        assert 'p -> o' in out
        assert 'and 2 more' in out

    def test_flags_edge_missing_from_default_branch(self):
        """The actionable signal when scanning many branches."""
        edges = [dvc_deps.RepoEdge(
            source='github.com/org/side-only', target='github.com/org/t',
            n_imports=1, refs=['feature-x'],
        )]
        out = dvc_deps.format_edges(
            edges, 'github.com/org/t',
            scanned_refs=['main', 'feature-x'], default_ref='main',
        )
        assert 'NOT on main' in out
        assert '1 source repo not present on main' in out

    def test_refs_listing_capped(self):
        edges = [dvc_deps.RepoEdge(
            source='github.com/org/a', target='github.com/org/t',
            n_imports=1, refs=[f'b{i}' for i in range(20)],
        )]
        out = dvc_deps.format_edges(
            edges, 'github.com/org/t',
            scanned_refs=[f'b{i}' for i in range(20)],
            show_refs=True, max_refs=3,
        )
        assert 'and 17 more' in out

    def test_json_output_is_valid(self):
        out = dvc_deps.edges_to_json(
            self._edges(), 'github.com/org/t',
            scanned_refs=['main'], default_ref='main',
        )
        data = json.loads(out)
        assert data['target'] == 'github.com/org/t'
        assert data['default_ref'] == 'main'
        assert data['edges'][0]['source'] == 'github.com/org/a'
        assert data['edges'][0]['n_imports'] == 3
