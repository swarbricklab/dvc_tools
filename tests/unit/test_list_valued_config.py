"""Tests for list-valued config keys and the consumers that read them.

`remote.root` became a list of four roots at some point. Every dt older than
0.20.0 read it with `get_value` and did `Path(remote_root)` on the result,
so a config edit retroactively broke 26 already-published container images,
each of which aborts with a bare TypeError before doing any work.

Two lessons, both tested here:

1. A scalar consumer reading a list does not fail where the mistake was made.
   `get_value` hands back the list quite happily; the crash lands later in
   `Path()` or `.strip()`, in a different module, with nothing pointing back
   to the config.
2. Config outlives code. Released versions cannot be patched, so the guard
   belongs at the point of writing -- `dt config add` -- not only at the
   point of reading.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from dt import cache as cache_mod
from dt import config as cfg
from dt import doctor as doctor_mod
from dt.auth import endpoints as ep_mod


# =============================================================================
# Writing: only known keys may become lists
# =============================================================================

class TestAddToListIsRestricted:
    """`dt config add <anything>` used to be accepted."""

    def test_known_list_keys_are_allowed(self):
        for key in ('remote.root', 'cache.root', 'secrets.gcp.locations'):
            assert key in cfg.LIST_VALUED_KEYS

    @pytest.mark.parametrize('key', [
        'owner', 'secrets.gcp.project', 'cache.permissions', 'auth.admin_email',
    ])
    def test_scalar_keys_are_refused(self, key):
        with pytest.raises(ValueError, match='does not take a list'):
            cfg.add_to_list(key, '/some/value')

    def test_refusal_names_the_keys_that_do(self):
        with pytest.raises(ValueError) as exc:
            cfg.add_to_list('owner', 'x')
        assert 'remote.root' in str(exc.value)

    def test_refusal_suggests_config_set(self):
        with pytest.raises(ValueError) as exc:
            cfg.add_to_list('owner', 'x')
        assert "dt config set owner" in str(exc.value)

    def test_allowed_key_reaches_the_write(self):
        with patch('dt.config.get_str_list', return_value=['/a']), \
             patch('dt.config.set_value') as set_value:
            assert cfg.add_to_list('remote.root', '/b', scope='user') is True
        set_value.assert_called_once()

    def test_no_write_attempted_for_a_refused_key(self):
        with patch('dt.config.set_value') as set_value:
            with pytest.raises(ValueError):
                cfg.add_to_list('owner', 'x')
        set_value.assert_not_called()


class TestConfigAddCli:
    """The refusal has to reach the user as a usage error, not a traceback."""

    def _invoke(self, args):
        from click.testing import CliRunner
        from dt.cli import cli
        return CliRunner().invoke(cli, args)

    def test_scalar_key_is_a_usage_error(self):
        result = self._invoke(['config', 'add', 'owner', 'swarbricklab'])
        assert result.exit_code == 2
        assert 'does not take a list' in result.output

    def test_no_traceback_leaks(self):
        result = self._invoke(['config', 'add', 'owner', 'swarbricklab'])
        assert 'Traceback' not in result.output


# =============================================================================
# Reading: consumers tolerate a list that is already in someone's config
# =============================================================================

CACHE_LIST = ['/scratch/a56/dvc/cache', '/g/data/a56/dvc/cache2']


class TestCacheRootConsumers:
    """cache.root is in LIST_VALUED_KEYS, so every reader must cope.

    The first entry is the primary, matching remote.root's convention that
    the first is the one used when creating something new.
    """

    def test_endpoint_discovery_yields_one_endpoint_per_root(self):
        with patch('dt.auth.endpoints.cfg.get_str_list',
                   side_effect=lambda k, *a: CACHE_LIST if k == 'cache.root' else []), \
             patch('dt.auth.endpoints.cfg.get_value', return_value=None), \
             patch('dt.auth.endpoints.utils.get_cache_dir', return_value=None), \
             patch('dt.auth.endpoints.remote_mod.remote_roots', return_value=[]), \
             patch('dt.auth.endpoints.utils.get_project_name', return_value='p'):
            endpoints = ep_mod._discover_dt_config()

        urls = [e.url for e in endpoints if e.source.startswith('cache.root')]
        assert urls == CACHE_LIST
        assert all(isinstance(u, str) for u in urls), "a list must never reach url"

    def test_endpoint_urls_survive_classification(self):
        """The real failure was downstream: classify_url on a list."""
        with patch('dt.auth.endpoints.cfg.get_str_list',
                   side_effect=lambda k, *a: CACHE_LIST if k == 'cache.root' else []), \
             patch('dt.auth.endpoints.cfg.get_value', return_value=None), \
             patch('dt.auth.endpoints.utils.get_cache_dir', return_value=None), \
             patch('dt.auth.endpoints.remote_mod.remote_roots', return_value=[]), \
             patch('dt.auth.endpoints.utils.get_project_name', return_value='p'):
            endpoints = ep_mod._discover_dt_config()

        for e in endpoints:
            assert ep_mod.classify_url(e.url) is not None

    def test_resolve_cache_path_uses_the_first_root(self):
        with patch('dt.cache.cfg.get_str_list', return_value=CACHE_LIST), \
             patch('dt.cache.utils.get_project_name', return_value='proj'):
            assert cache_mod.resolve_cache_path() == Path(CACHE_LIST[0]) / 'proj'

    def test_resolve_cache_path_still_takes_a_scalar(self):
        with patch('dt.cache.cfg.get_str_list', return_value=['/one/root']), \
             patch('dt.cache.utils.get_project_name', return_value='proj'):
            assert cache_mod.resolve_cache_path() == Path('/one/root') / 'proj'

    def test_resolve_cache_path_explicit_argument_still_wins(self):
        with patch('dt.cache.cfg.get_str_list', return_value=CACHE_LIST), \
             patch('dt.cache.utils.get_project_name', return_value='proj'):
            assert cache_mod.resolve_cache_path(cache_root='/explicit') == \
                Path('/explicit') / 'proj'

    def test_unconfigured_cache_root_still_errors_clearly(self):
        with patch('dt.cache.cfg.get_str_list', return_value=[]):
            with pytest.raises(cache_mod.CacheError, match='not configured'):
                cache_mod.resolve_cache_path()

    def test_doctor_checks_the_first_root(self, tmp_path):
        first = tmp_path / 'primary'
        first.mkdir()
        with patch('dt.doctor.cfg.get_str_list',
                   return_value=[str(first), '/nonexistent/second']):
            result = doctor_mod.check_cache_root()
        assert result.passed is True
        assert str(first) in result.message

    def test_doctor_does_not_crash_on_a_list(self):
        """It used to hand Path() the list itself."""
        with patch('dt.doctor.cfg.get_str_list', return_value=CACHE_LIST):
            result = doctor_mod.check_cache_root()
        assert isinstance(result.message, str)

    def test_doctor_unconfigured_is_unchanged(self):
        with patch('dt.doctor.cfg.get_str_list', return_value=[]):
            result = doctor_mod.check_cache_root()
        assert result.passed is False
        assert 'not configured' in result.message


class TestGetStrListContract:
    """The accessor both spellings rely on."""

    def test_scalar_becomes_a_one_item_list(self):
        with patch('dt.config.get_value', return_value='/a'):
            assert cfg.get_str_list('cache.root') == ['/a']

    def test_list_is_preserved_in_order(self):
        with patch('dt.config.get_value', return_value=['/b', '/a']):
            assert cfg.get_str_list('cache.root') == ['/b', '/a']

    def test_blank_entries_are_dropped(self):
        with patch('dt.config.get_value', return_value=['/a', '', '  ']):
            assert cfg.get_str_list('cache.root') == ['/a']

    def test_absent_key_is_empty(self):
        with patch('dt.config.get_value', return_value=None):
            assert cfg.get_str_list('cache.root') == []
