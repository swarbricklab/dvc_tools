"""Tests for dt tmp module."""

import os
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from dt import tmp
from dt import config as cfg
from dt.errors import TmpError


def _fake_clone(path):
    """Minimal dt clone root: a real .git dir with HEAD and objects."""
    (path / '.git' / 'objects').mkdir(parents=True)
    (path / '.git' / 'refs').mkdir()
    (path / '.git' / 'HEAD').write_text('ref: refs/heads/main\n')
    return path


class TestGetTmpDir:
    """Tests for get_tmp_dir function."""
    
    def test_returns_correct_path(self, tmp_path, monkeypatch):
        """Returns .dt/tmp/clones path."""
        monkeypatch.chdir(tmp_path)
        
        result = tmp.get_tmp_dir()
        
        assert result == tmp_path / '.dt' / 'tmp' / 'clones'


class TestResolveRepositoryUrl:
    """Tests for resolve_repository_url function."""
    
    def test_full_ssh_url_returned_as_is(self):
        """Full SSH URL is returned unchanged."""
        url = "git@github.com:swarbricklab/dt-test-registry.git"
        result = tmp.resolve_repository_url(url)
        
        assert result == url
    
    def test_full_https_url_returned_as_is(self):
        """Full HTTPS URL is returned unchanged."""
        url = "https://github.com/swarbricklab/dt-test-registry.git"
        result = tmp.resolve_repository_url(url)
        
        assert result == url
    
    def test_short_name_with_owner_argument(self):
        """Short name with owner argument constructs URL."""
        result = tmp.resolve_repository_url("dt-test-registry", owner="swarbricklab")
        
        assert result == "git@github.com:swarbricklab/dt-test-registry.git"
    
    def test_short_name_uses_config_owner(self):
        """Short name uses owner from config."""
        with patch.object(cfg, 'get_value', return_value="swarbricklab"):
            result = tmp.resolve_repository_url("dt-test-registry")
        
        assert result == "git@github.com:swarbricklab/dt-test-registry.git"
    
    def test_raises_error_when_owner_missing(self):
        """Raises TmpError when owner is needed but not configured."""
        with patch.object(cfg, 'get_value', return_value=None):
            with pytest.raises(TmpError, match="no owner is configured"):
                tmp.resolve_repository_url("myrepo")


class TestGetRepoId:
    """Tests for get_repo_id function."""
    
    def test_ssh_format(self):
        """Converts SSH URL to repo ID."""
        url = "git@github.com:swarbricklab/dt-test-registry.git"
        result = tmp.get_repo_id(url)
        
        assert result == "github.com/swarbricklab/dt-test-registry"
    
    def test_https_format(self):
        """Converts HTTPS URL to repo ID."""
        url = "https://github.com/swarbricklab/dt-test-registry.git"
        result = tmp.get_repo_id(url)
        
        assert result == "github.com/swarbricklab/dt-test-registry"
    
    def test_short_name_with_owner(self):
        """Converts short name with owner to repo ID."""
        result = tmp.get_repo_id("dt-test-registry", owner="swarbricklab")
        
        assert result == "github.com/swarbricklab/dt-test-registry"
    
    def test_removes_git_suffix(self):
        """Removes .git suffix from repo ID."""
        url = "git@github.com:org/repo.git"
        result = tmp.get_repo_id(url)
        
        assert not result.endswith('.git')
        assert result == "github.com/org/repo"


class TestCloneRepo:
    """Tests for clone_repo function."""
    
    def test_creates_full_clone(self, tmp_path, monkeypatch):
        """Creates a full clone -- not shallow, not sparse."""
        monkeypatch.chdir(tmp_path)

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ''
        
        calls = []
        def capture_run(cmd, **kwargs):
            calls.append(cmd)
            return mock_result
        
        with patch('subprocess.run', side_effect=capture_run):
            with patch('dt.tmp.utils.check_git'):
                result = tmp.clone_repo(
                    'git@github.com:swarbricklab/dt-test-registry.git',
                    verbose=False
                )
        
        assert result == tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'swarbricklab' / 'dt-test-registry'
        
        # Full history, so `dvc diff` can compare against any locked revision.
        # Shallowness survives only as an incremental `git fetch --depth 1`.
        clone_cmd = calls[0]
        assert clone_cmd[0:2] == ['git', 'clone']
        assert '--depth' not in clone_cmd
        assert '--single-branch' not in clone_cmd
        # No longer using sparse checkout
        assert '--no-checkout' not in clone_cmd
    
    def test_refreshes_existing_clone(self, tmp_path, monkeypatch):
        """Refreshes existing clone when refresh=True."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').touch()
        
        # Create existing clone directory
        repo_path = tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        _fake_clone(repo_path)
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ''
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            with patch('dt.tmp.utils.check_git'):
                result = tmp.clone_repo(
                    'git@github.com:org/repo.git',
                    refresh=True,
                    verbose=False
                )
        
        assert result == repo_path
    
    def test_uses_cached_clone(self, tmp_path, monkeypatch):
        """Uses cached clone when refresh=False."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').touch()
        
        # Create existing clone directory
        repo_path = tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        _fake_clone(repo_path)
        
        with patch('subprocess.run') as mock_run:
            with patch('dt.tmp.utils.check_git'):
                result = tmp.clone_repo(
                    'git@github.com:org/repo.git',
                    refresh=False,
                    verbose=False
                )
        
        assert result == repo_path
        # Should not call subprocess.run (no refresh, no clone)
        mock_run.assert_not_called()
    
    def test_raises_error_on_clone_failure(self, tmp_path, monkeypatch):
        """Raises TmpError when git clone fails."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.gitignore').touch()

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = 'clone failed'

        with patch('subprocess.run', return_value=mock_result):
            with patch('dt.tmp.utils.check_git'):
                with pytest.raises(TmpError, match="Failed to clone"):
                    tmp.clone_repo(
                        'git@github.com:org/repo.git',
                        verbose=False
                    )

    @patch('dt.tmp._require_clone_root')  # git clone is mocked
    def test_checks_out_rev_after_clone(self, _root, tmp_path, monkeypatch):
        """When rev is given, the clone is checked out at that revision."""
        monkeypatch.chdir(tmp_path)

        ok = MagicMock()
        ok.returncode = 0
        ok.stderr = ''

        calls = []
        def capture_run(cmd, **kwargs):
            calls.append(cmd)
            return ok

        with patch('subprocess.run', side_effect=capture_run):
            with patch('dt.tmp.utils.check_git'):
                tmp.clone_repo(
                    'git@github.com:org/repo.git',
                    refresh=False,
                    verbose=False,
                    rev='dev',
                )

        # git clone, then a fetch of the rev, then a checkout of the rev.
        assert calls[0][0:2] == ['git', 'clone']
        assert any('fetch' in c and 'dev' in c for c in calls)
        assert any('checkout' in c and c[-2:] == ['dev', '--'] for c in calls)

    @pytest.mark.parametrize('rev', ['--upload-pack=x', '-b', 'a b', 'a:b', 'a..b', ''])
    def test_rejects_malformed_rev(self, tmp_path, monkeypatch, rev):
        """clone_repo refuses a malformed rev before git runs."""
        monkeypatch.chdir(tmp_path)
        with patch('subprocess.run') as mock_run, \
             patch('dt.tmp.utils.check_git'):
            with pytest.raises(TmpError, match="Invalid git revision"):
                tmp.clone_repo('git@github.com:org/repo.git', refresh=False,
                               verbose=False, rev=rev)
        mock_run.assert_not_called()

    def test_checkout_path_at_revision_rejects_malformed_rev(self, tmp_path):
        with patch('subprocess.run') as mock_run:
            assert tmp.checkout_path_at_revision(tmp_path, 'x', '-x') is False
        mock_run.assert_not_called()

    @patch('dt.tmp._require_clone_root')  # git clone is mocked
    def test_git_args_are_terminated(self, _root, tmp_path, monkeypatch):
        """User-derived url/rev are placed after an option terminator."""
        monkeypatch.chdir(tmp_path)
        calls = []
        def run(cmd, **kwargs):
            calls.append(cmd)
            return MagicMock(returncode=0, stderr='')
        with patch('subprocess.run', side_effect=run), \
             patch('dt.tmp.utils.check_git'):
            tmp.clone_repo('git@github.com:org/repo.git', refresh=False,
                           verbose=False, rev='v1.0')
        clone = calls[0]
        assert clone.index('--') < clone.index('git@github.com:org/repo.git')
        for c in calls[1:]:
            if 'v1.0' in c:
                # option end before the rev, or a pathspec end after it
                assert ('--end-of-options' in c and c.index('--end-of-options') < c.index('v1.0')) \
                    or c[c.index('v1.0') + 1:] == ['--']

    @patch('dt.tmp._require_clone_root')  # git clone is mocked
    def test_checkout_rev_falls_back_to_fetch_head(self, _root, tmp_path, monkeypatch):
        """If `git checkout <rev>` fails, fall back to FETCH_HEAD."""
        monkeypatch.chdir(tmp_path)

        def run(cmd, **kwargs):
            r = MagicMock()
            r.stderr = ''
            # Only `git checkout <rev>` (named) fails; everything else succeeds.
            if 'checkout' in cmd and cmd[-2:] == ['abc123', '--']:
                r.returncode = 1
                r.stderr = 'pathspec did not match'
            else:
                r.returncode = 0
            return r

        calls = []
        def capture(cmd, **kwargs):
            calls.append(cmd)
            return run(cmd, **kwargs)

        with patch('subprocess.run', side_effect=capture):
            with patch('dt.tmp.utils.check_git'):
                tmp.clone_repo('git@github.com:org/repo.git', refresh=False,
                               verbose=False, rev='abc123')

        assert any('checkout' in c and c[-2:] == ['FETCH_HEAD', '--'] for c in calls)

    @patch('dt.tmp._require_clone_root')  # git clone is mocked
    def test_checkout_rev_raises_when_unresolvable(self, _root, tmp_path, monkeypatch):
        """A revision that can't be fetched or checked out raises TmpError."""
        monkeypatch.chdir(tmp_path)

        def run(cmd, **kwargs):
            r = MagicMock()
            r.stderr = 'no such ref'
            # clone succeeds; every fetch/checkout of the rev fails.
            r.returncode = 0 if cmd[0:2] == ['git', 'clone'] else 1
            return r

        with patch('subprocess.run', side_effect=run):
            with patch('dt.tmp.utils.check_git'):
                with pytest.raises(TmpError, match="Could not check out revision"):
                    tmp.clone_repo('git@github.com:org/repo.git', refresh=False,
                                   verbose=False, rev='nope')


class TestClonePathContainment:
    """Clone paths always stay inside .dt/tmp/clones and git is bound to them."""

    @pytest.mark.parametrize('spec', [
        'git@github.com:../../../..',
        'git@github.com:org/../../..',
        'https://github.com/../../x',
        'git@github.com:org/repo/sub/dir.git',
        'git@github.com:org/./repo',
        '..',
        '.',
        '/abs/path',
        'ssh://host/a/b/c/d',
    ])
    def test_paths_stay_inside_clones(self, tmp_path, monkeypatch, spec):
        monkeypatch.chdir(tmp_path)
        clones = tmp_path / '.dt' / 'tmp' / 'clones'
        repo_id = tmp.get_repo_id(spec, owner='org')
        path = tmp.clone_path_for(repo_id, clones)
        resolved = Path(os.path.normpath(path))
        assert clones in resolved.parents
        rel = resolved.relative_to(clones).parts
        assert '..' not in rel and '.' not in rel
        # Either a plain host/owner/repo or a flat name under _other/.
        assert len(rel) == 3 or (len(rel) == 2 and rel[0] == '_other')

    def test_normal_ids_keep_nested_layout(self, tmp_path):
        clones = tmp_path / 'clones'
        assert tmp.clone_path_for('github.com/org/repo', clones) == clones / 'github.com' / 'org' / 'repo'

    def test_symlink_in_path_is_refused(self, tmp_path):
        clones = tmp_path / 'clones'
        (clones / 'github.com').mkdir(parents=True)
        (clones / 'github.com' / 'org').symlink_to(tmp_path)
        with pytest.raises(TmpError, match='symlink'):
            tmp.clone_path_for('github.com/org/repo', clones)

    def test_existing_dir_without_git_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo').mkdir(parents=True)
        with patch('subprocess.run') as run, patch('dt.tmp.utils.check_git'):
            with pytest.raises(TmpError, match='not a dt clone'):
                tmp.clone_repo('git@github.com:org/repo.git', refresh=True, verbose=False)
        run.assert_not_called()

    def test_git_is_bound_to_the_clone(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        repo = tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        _fake_clone(repo)
        calls = []
        with patch('subprocess.run', side_effect=lambda cmd, **k: calls.append(cmd) or MagicMock(returncode=0, stderr='')), \
             patch('dt.tmp.utils.check_git'):
            tmp.clone_repo('git@github.com:org/repo.git', refresh=True, verbose=False, rev='v1')
        assert calls
        for cmd in calls:
            assert cmd[1] == f'--git-dir={repo / ".git"}'
            assert cmd[2] == f'--work-tree={repo}'

    def test_dot_segment_url_never_touches_project_repo(self, tmp_path, monkeypatch):
        """End to end with real git: a '..' import URL can't reset the project."""
        project = tmp_path / 'project'
        project.mkdir()
        subprocess.run(['git', 'init', '-q', str(project)], check=True)
        (project / 'work.txt').write_text('uncommitted')
        monkeypatch.chdir(project)
        real_run = subprocess.run
        seen = []

        def run(cmd, **kwargs):
            seen.append(cmd)
            if 'clone' in cmd:  # no network: the clone itself just fails
                return MagicMock(returncode=128, stderr='offline')
            return real_run(cmd, **kwargs)

        with patch('dt.tmp.subprocess.run', side_effect=run), \
             patch('dt.tmp.utils.check_git'):
            with pytest.raises(TmpError):
                tmp.clone_repo('git@github.com:../../../..', refresh=True, verbose=False)
        assert (project / 'work.txt').read_text() == 'uncommitted'
        # Only a clone into the clones dir was attempted -- no fetch/reset.
        assert [c for c in seen if 'clone' not in c] == []
        target = Path(seen[0][-1])
        assert (project / '.dt' / 'tmp' / 'clones') in target.parents
        assert sorted(p.name for p in tmp_path.iterdir()) == ['project']


class TestCloneTrust:
    def test_symlinked_clones_dir_is_refused(self, tmp_path, monkeypatch):
        """A committed .dt/tmp/clones symlink can't redirect clones."""
        project = tmp_path / 'project'
        (project / '.dt' / 'tmp').mkdir(parents=True)
        (project / '.dt' / 'tmp' / 'clones').symlink_to(tmp_path)
        monkeypatch.chdir(project)
        with patch('subprocess.run') as run, patch('dt.tmp.utils.check_git'), \
             patch('dt.tmp.get_tmp_dir', return_value=project / '.dt' / 'tmp' / 'clones'):
            with pytest.raises(TmpError, match='symlink'):
                tmp.clone_repo('https://x/y/z', refresh=True, verbose=False)
        run.assert_not_called()

    @pytest.mark.parametrize('link', ['.dt', '.dt/tmp'])
    def test_symlinked_dt_parents_are_refused(self, tmp_path, link):
        project = tmp_path / 'project'
        (project / link).parent.mkdir(parents=True, exist_ok=True)
        (project / link).symlink_to(tmp_path)
        clones = project / '.dt' / 'tmp' / 'clones'
        with pytest.raises(TmpError, match='symlink'):
            tmp.clone_path_for('github.com/org/repo', clones)

    def test_clone_owned_by_someone_else_is_refused(self, tmp_path):
        repo = _fake_clone(tmp_path / 'repo')
        real = os.geteuid()
        with patch('dt.tmp.os.geteuid', return_value=real + 1):
            with pytest.raises(TmpError, match='not a dt clone owned by you'):
                tmp._require_clone_root(repo)

    def test_empty_git_dir_is_not_a_clone(self, tmp_path):
        (tmp_path / 'repo' / '.git').mkdir(parents=True)
        with pytest.raises(TmpError, match='not a dt clone'):
            tmp._require_clone_root(tmp_path / 'repo')

    def test_long_components_use_flat_name(self, tmp_path):
        clones = tmp_path / 'clones'
        path = tmp.clone_path_for('h/o/' + 'x' * 300, clones)
        assert path.parent == clones / '_other'
        assert len(path.name) < 120

    def test_newline_in_component_uses_flat_name(self, tmp_path):
        clones = tmp_path / 'clones'
        path = tmp.clone_path_for('a\n/b/c', clones)
        assert path.parent == clones / '_other'

    def test_dvc_root_required(self, tmp_path):
        clone = tmp_path / 'clone'
        clone.mkdir()
        with pytest.raises(TmpError, match='not a DVC repository'):
            tmp.require_dvc_root(clone)
        (clone / '.dvc').symlink_to(tmp_path)
        with pytest.raises(TmpError, match='not a directory'):
            tmp.require_dvc_root(clone)

    def test_clean_skips_non_clone(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        d = tmp_path / '.dt' / 'tmp' / 'clones' / 'github.com' / 'org' / 'repo'
        (d / 'keep').mkdir(parents=True)
        with patch('dt.tmp.get_tmp_dir', return_value=tmp_path / '.dt' / 'tmp' / 'clones'):
            assert tmp.clean_repos('git@github.com:org/repo.git') == []
        assert (d / 'keep').exists()
