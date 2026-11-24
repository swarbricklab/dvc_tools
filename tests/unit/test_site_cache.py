"""Tests for site_cache directory creation and adoption."""

import os
import stat
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dt import site_cache
from dt.errors import SiteCacheError

USER = site_cache._current_user()
OTHER_UID = os.geteuid() + 1


def _mode(p: Path) -> int:
    return stat.S_IMODE(os.stat(p).st_mode)


@pytest.fixture(autouse=True)
def no_dt_config():
    """Keep the host's dt config (any scope) out of these tests."""
    with patch('dt.site_cache.cfg.load_scope_config', return_value={}), \
         patch('dt.site_cache.cfg.get_value', side_effect=lambda k, default=None: default):
        yield


@pytest.fixture
def fs(tmp_path):
    """Control ownership as seen by dt.

    Everything above ``tmp_path`` looks system-owned (uid 0) so the ancestor
    walk stops there; paths added to ``fs.system`` also look uid 0 and paths
    in ``fs.foreign`` look owned by another user.
    """
    class FS:
        system = set()
        foreign = set()
    orig = Path.stat

    def fake(self, *args, **kwargs):
        st = orig(self, *args, **kwargs)
        p = Path(os.path.abspath(self))
        uid = None
        if p in FS.foreign:
            uid = OTHER_UID
        elif p in FS.system or not (p == tmp_path or tmp_path in p.parents):
            uid = 0
        if uid is None:
            return st
        fields = list(st)
        fields[4] = uid
        return os.stat_result(fields)

    with patch.object(Path, 'stat', fake):
        yield FS


@pytest.fixture
def root(tmp_path, fs):
    r = tmp_path / 'site'
    r.mkdir()
    os.chmod(r, 0o3775)  # setgid + sticky
    return r


@pytest.fixture
def dvc_ok():
    with patch('dt.site_cache.subprocess.run',
               return_value=MagicMock(returncode=0, stdout='', stderr='')) as m, \
         patch('dt.site_cache._has_default_acl', return_value=False):
        yield m


def _configured(dvc_ok):
    calls = [c[0][0] for c in dvc_ok.call_args_list if c[0][0][:2] == ['dvc', 'config']]
    return calls[-1][-1] if calls else None


def _apply(tmp_path, root, name='proj'):
    target = site_cache.resolve_path(name=name, site_cache_root=str(root))
    site_cache.apply_to_repo(tmp_path, target, verbose=False, site_cache_root=str(root))
    return target


class TestResolvePath:
    def test_root_layout_is_per_user(self, root):
        p = site_cache.resolve_path(name='proj', site_cache_root=str(root))
        assert p == root / USER / 'proj'

    @pytest.mark.parametrize('name', ['..', '.', 'a/b'])
    def test_rejects_bad_names(self, root, name):
        with pytest.raises(SiteCacheError):
            site_cache.resolve_path(name=name, site_cache_root=str(root))


class TestApplyToRepo:
    def test_creates_private_dirs(self, tmp_path, root, dvc_ok):
        target = _apply(tmp_path, root)
        assert _mode(target) == 0o700
        assert _mode(target.parent) == 0o700
        assert _configured(dvc_ok) == str(target)

    def test_group_shared_is_opt_in(self, tmp_path, root, dvc_ok):
        with patch('dt.site_cache._group_shared', return_value=True):
            target = _apply(tmp_path, root)
        assert _mode(target) & 0o777 == 0o770
        assert not (_mode(target.parent) & (stat.S_IWGRP | stat.S_IWOTH))

    def test_tightens_owned_permissive_dirs(self, tmp_path, root, dvc_ok):
        target = root / USER / 'proj'
        target.mkdir(parents=True)
        os.chmod(target.parent, 0o2775)
        os.chmod(target, 0o2775)
        _apply(tmp_path, root)
        assert _mode(target) == 0o700
        assert _mode(target.parent) == 0o700

    def test_refuses_target_owned_by_someone_else(self, tmp_path, root, fs, dvc_ok):
        target = root / USER / 'proj'
        target.mkdir(parents=True)
        fs.foreign.add(target)
        with pytest.raises(SiteCacheError, match=r'proj: it is not a directory owned by you'):
            _apply(tmp_path, root)
        assert _configured(dvc_ok) is None

    def test_refuses_user_dir_owned_by_someone_else(self, tmp_path, root, fs, dvc_ok):
        (root / USER).mkdir()
        fs.foreign.add(root / USER)
        with pytest.raises(SiteCacheError, match='not a directory owned by you'):
            _apply(tmp_path, root)
        assert _configured(dvc_ok) is None

    def test_refuses_symlink_foreign_by_another_user(self, tmp_path, root, fs, dvc_ok):
        elsewhere = tmp_path / 'elsewhere'
        elsewhere.mkdir()
        (root / USER).symlink_to(elsewhere)
        fs.foreign.add(root / USER)
        with pytest.raises(SiteCacheError, match='symlink owned by another user'):
            _apply(tmp_path, root)
        assert not (elsewhere / 'proj').exists()
        assert _configured(dvc_ok) is None

    def test_follows_own_symlink_and_records_real_path(self, tmp_path, root, dvc_ok):
        link = tmp_path / 'link-to-site'
        link.symlink_to(root)
        target = site_cache.resolve_path(name='proj', site_cache_root=str(link))
        site_cache.apply_to_repo(tmp_path, target, verbose=False, site_cache_root=str(link))
        assert _configured(dvc_ok) == str(root / USER / 'proj')

    def test_refuses_root_writable_without_sticky(self, tmp_path, root, dvc_ok):
        os.chmod(root, 0o2775)
        with pytest.raises(SiteCacheError, match=f'{root} is writable'):
            _apply(tmp_path, root)
        assert not (root / USER).exists()
        assert _configured(dvc_ok) is None

    def test_refuses_non_directory(self, tmp_path, dvc_ok, fs):
        target = tmp_path / 'file'
        target.write_text('x')
        with pytest.raises(SiteCacheError, match='not a directory owned by you'):
            site_cache.apply_to_repo(tmp_path, target, verbose=False)

    def test_notes_open_system_parent(self, tmp_path, fs, dvc_ok, capsys):
        sysdir = tmp_path / 'project'
        sysdir.mkdir()
        os.chmod(sysdir, 0o2770)
        fs.system.add(sysdir)
        r = sysdir / 'site'
        r.mkdir()
        os.chmod(r, 0o3775)
        site_cache.apply_to_repo(
            tmp_path, site_cache.resolve_path(name='p', site_cache_root=str(r)),
            verbose=True, site_cache_root=str(r))
        assert f'{sysdir} (system-managed)' in capsys.readouterr().out

    def test_os_errors_become_site_cache_errors(self, tmp_path, fs, dvc_ok):
        ro = tmp_path / 'ro'
        ro.mkdir()
        os.chmod(ro, 0o555)
        try:
            with pytest.raises(SiteCacheError):
                _apply(tmp_path, ro / 'site')
        finally:
            os.chmod(ro, 0o755)


class TestAdoptionContents:
    def test_refuses_owned_dir_with_writable_contents(self, tmp_path, root, dvc_ok):
        target = root / USER / 'proj'
        target.mkdir(parents=True)
        (target / 'state.db').write_text('foreign')
        os.chmod(target / 'state.db', 0o664)
        with pytest.raises(SiteCacheError, match='not private to you'):
            _apply(tmp_path, root)
        assert (target / 'state.db').exists()  # never deleted
        assert _configured(dvc_ok) is None

    def test_adopts_owned_dir_with_private_contents(self, tmp_path, root, dvc_ok):
        target = root / USER / 'proj'
        target.mkdir(parents=True)
        os.chmod(target, 0o2775)
        (target / 'state.db').write_text('mine')
        os.chmod(target / 'state.db', 0o600)
        _apply(tmp_path, root)
        assert _mode(target) == 0o700
        assert _configured(dvc_ok) == str(target)


class TestTrustedConfig:
    def _scopes(self, data_by_scope):
        return patch('dt.site_cache.cfg.load_scope_config',
                     side_effect=lambda scope: data_by_scope.get(scope, {}))

    def test_ignores_project_scope(self, capsys):
        with self._scopes({'project': {'site_cache': {'group_shared': True}}}), \
             patch('dt.site_cache.cfg.get_value', return_value=True):
            assert site_cache._group_shared() is False
        assert 'ignoring site_cache.group_shared' in capsys.readouterr().out

    def test_root_from_project_scope_is_ignored(self, tmp_path):
        with self._scopes({'project': {'site_cache': {'root': str(tmp_path)}}}), \
             patch('dt.site_cache.cfg.get_value', return_value=str(tmp_path)):
            with pytest.raises(SiteCacheError, match='not configured'):
                site_cache.resolve_path(name='p')

    def test_user_scope_is_honoured(self):
        with self._scopes({'user': {'site_cache': {'group_shared': 'true'}}}):
            assert site_cache._group_shared() is True


class TestMigrate:
    def _current(self, path):
        return patch('dt.site_cache.get_current', return_value=path)

    def test_refused_target_copies_nothing(self, tmp_path, root, fs, dvc_ok):
        target = root / USER / 'proj'
        target.mkdir(parents=True)
        fs.foreign.add(target)
        with self._current(tmp_path / 'old') as cur, patch('shutil.copytree') as copy:
            with pytest.raises(SiteCacheError, match='not a directory owned by you'):
                site_cache.migrate(target, repo_path=tmp_path, verbose=False,
                                   site_cache_root=str(root))
        copy.assert_not_called()
        cur.assert_not_called()

    def test_does_not_copy_non_private_source(self, tmp_path, root, dvc_ok):
        old = tmp_path / 'old'
        old.mkdir()
        os.chmod(old, 0o2775)
        (old / 'state').write_text('foreign')
        target = root / USER / 'proj'
        with self._current(old):
            site_cache.migrate(target, repo_path=tmp_path, verbose=False,
                               site_cache_root=str(root))
        assert not (target / 'state').exists()
        assert _configured(dvc_ok) == str(target)

    def test_copies_private_source_and_keeps_target_private(self, tmp_path, root, dvc_ok):
        old = tmp_path / 'old'
        old.mkdir()
        os.chmod(old, 0o700)
        (old / 'state').write_text('mine')
        os.chmod(old / 'state', 0o600)
        target = root / USER / 'proj'
        with self._current(old):
            site_cache.migrate(target, repo_path=tmp_path, verbose=False,
                               site_cache_root=str(root))
        assert (target / 'state').read_text() == 'mine'
        assert _mode(target) == 0o700


class TestDescribeRisk:
    def test_flags_group_writable(self, tmp_path, fs):
        d = tmp_path / 'd'
        d.mkdir()
        os.chmod(d, 0o2775)
        assert 'writable by other users' in site_cache.describe_risk(d)

    def test_private_is_fine(self, tmp_path, fs):
        d = tmp_path / 'd'
        d.mkdir()
        os.chmod(d, 0o700)
        assert site_cache.describe_risk(d) is None


class TestSharedPermsGuidance:
    def test_guidance_recommends_sticky_not_group_acl(self, tmp_path, capsys):
        d = tmp_path / 'r'
        d.mkdir()
        os.chmod(d, 0o2775)
        with patch('dt.site_cache._has_default_acl', return_value=False):
            site_cache._check_shared_perms(d)
        out = capsys.readouterr().out
        assert 'sticky' in out and '3775' in out
        assert 'umask' not in out and 'g::rwx' not in out
