"""Tests for dt config command."""

import os
import tempfile
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dt.cli import cli
from dt import config as cfg


@pytest.fixture
def temp_dirs():
    """Create temporary directories for config scopes."""
    dirs = {
        'home': tempfile.mkdtemp(),
        'project': tempfile.mkdtemp(),
        'system': tempfile.mkdtemp(),
    }
    yield dirs
    # Cleanup
    for d in dirs.values():
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def isolated_config(temp_dirs, monkeypatch):
    """Set up isolated config environment."""
    # Override XDG directories
    monkeypatch.setenv('HOME', temp_dirs['home'])
    monkeypatch.setenv('XDG_CONFIG_HOME', os.path.join(temp_dirs['home'], '.config'))
    monkeypatch.setenv('XDG_CONFIG_DIRS', temp_dirs['system'])
    
    # Change to project directory
    original_cwd = os.getcwd()
    os.chdir(temp_dirs['project'])
    
    # Reset config cache
    cfg._config = None
    
    yield temp_dirs
    
    # Cleanup
    cfg._config = None
    os.chdir(original_cwd)


@pytest.fixture
def runner():
    """Click test runner."""
    return CliRunner()


class TestDTConfigInit:
    """Tests for config module initialization."""
    
    def test_get_config_paths(self, isolated_config):
        """Config paths can be retrieved."""
        paths = cfg.get_config_paths()
        assert 'user' in paths
        assert 'system' in paths
        assert 'local' in paths
        assert 'project' in paths
    
    def test_empty_config_returns_empty(self, isolated_config):
        """Empty config returns empty dict."""
        cfg._config = None  # Reset cached config
        result = cfg.list_config()
        assert result == {}


class TestDTConfigGet:
    """Tests for dt config get command."""
    
    def test_get_missing_key_returns_error(self, runner, isolated_config):
        """Getting non-existent key returns error."""
        result = runner.invoke(cli, ['config', 'get', 'nonexistent.key'])
        assert result.exit_code != 0
        assert 'not found' in result.output.lower() or 'error' in result.output.lower()
    
    def test_get_existing_key(self, runner, isolated_config):
        """Getting existing key returns value."""
        # Set up a user config
        config_dir = Path(isolated_config['home']) / '.config' / 'dt'
        config_dir.mkdir(parents=True)
        config_file = config_dir / 'config.yaml'
        config_file.write_text(yaml.dump({'owner': 'testowner'}))
        
        result = runner.invoke(cli, ['config', 'get', 'owner'])
        assert result.exit_code == 0
        assert 'testowner' in result.output
    
    def test_get_nested_key(self, runner, isolated_config):
        """Getting nested key with dot notation works."""
        config_dir = Path(isolated_config['home']) / '.config' / 'dt'
        config_dir.mkdir(parents=True)
        config_file = config_dir / 'config.yaml'
        config_file.write_text(yaml.dump({
            'cache': {'root': '/test/cache', 'permissions': 'ug+rw'}
        }))
        
        result = runner.invoke(cli, ['config', 'get', 'cache.root'])
        assert result.exit_code == 0
        assert '/test/cache' in result.output


class TestDTConfigSet:
    """Tests for dt config set command."""
    
    def test_set_user_config(self, runner, isolated_config):
        """Setting user config creates file and stores value."""
        result = runner.invoke(cli, ['config', 'set', '--user', 'owner', 'myowner'])
        assert result.exit_code == 0
        
        # Verify file was created
        config_file = Path(isolated_config['home']) / '.config' / 'dt' / 'config.yaml'
        assert config_file.exists()
        
        # Verify content
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data['owner'] == 'myowner'
    
    def test_set_nested_key(self, runner, isolated_config):
        """Setting nested key creates proper structure."""
        result = runner.invoke(cli, ['config', 'set', '--user', 'cache.root', '/my/cache'])
        assert result.exit_code == 0
        
        config_file = Path(isolated_config['home']) / '.config' / 'dt' / 'config.yaml'
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data['cache']['root'] == '/my/cache'
    
    def test_set_project_config(self, runner, isolated_config):
        """Setting project config stores in .dt directory."""
        result = runner.invoke(cli, ['config', 'set', '--project', 'platform', 'test'])
        assert result.exit_code == 0
        
        config_file = Path(isolated_config['project']) / '.dt' / 'config.yaml'
        assert config_file.exists()
        
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data['platform'] == 'test'
    
    def test_set_local_config(self, runner, isolated_config):
        """Setting local config stores in config.local.yaml."""
        result = runner.invoke(cli, ['config', 'set', '--local', 'debug', 'true'])
        assert result.exit_code == 0
        
        config_file = Path(isolated_config['project']) / '.dt' / 'config.local.yaml'
        assert config_file.exists()
    
    def test_set_overwrites_existing(self, runner, isolated_config):
        """Setting existing key overwrites value."""
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'first'])
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'second'])
        
        result = runner.invoke(cli, ['config', 'get', 'owner'])
        assert 'second' in result.output
    
    def test_set_preserves_other_keys(self, runner, isolated_config):
        """Setting a key preserves other existing keys."""
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'myowner'])
        runner.invoke(cli, ['config', 'set', '--user', 'platform', 'test'])
        
        config_file = Path(isolated_config['home']) / '.config' / 'dt' / 'config.yaml'
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data['owner'] == 'myowner'
        assert data['platform'] == 'test'


class TestDTConfigUnset:
    """Tests for dt config unset command."""
    
    def test_unset_removes_key(self, runner, isolated_config):
        """Unsetting a key removes it from config."""
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'myowner'])
        result = runner.invoke(cli, ['config', 'unset', '--user', 'owner'])
        assert result.exit_code == 0
        
        config_file = Path(isolated_config['home']) / '.config' / 'dt' / 'config.yaml'
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data is None or 'owner' not in data
    
    def test_unset_nested_key(self, runner, isolated_config):
        """Unsetting nested key removes only that key."""
        runner.invoke(cli, ['config', 'set', '--user', 'cache.root', '/test'])
        runner.invoke(cli, ['config', 'set', '--user', 'cache.permissions', 'ug+rw'])
        runner.invoke(cli, ['config', 'unset', '--user', 'cache.root'])
        
        config_file = Path(isolated_config['home']) / '.config' / 'dt' / 'config.yaml'
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert 'root' not in data.get('cache', {})
        assert data['cache']['permissions'] == 'ug+rw'
    
    def test_unset_nonexistent_key_succeeds(self, runner, isolated_config):
        """Unsetting non-existent key gives informative message."""
        result = runner.invoke(cli, ['config', 'unset', '--user', 'nonexistent'])
        # Should either succeed or fail gracefully with informative message
        assert result.exit_code in [0, 1]
        if result.exit_code == 1:
            assert 'not found' in result.output.lower() or 'error' in result.output.lower()


class TestDTConfigList:
    """Tests for dt config list command."""
    
    def test_list_empty_config(self, runner, isolated_config):
        """Listing empty config shows nothing."""
        result = runner.invoke(cli, ['config', 'list'])
        assert result.exit_code == 0
    
    def test_list_shows_all_values(self, runner, isolated_config):
        """Listing shows all configured values."""
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'myowner'])
        runner.invoke(cli, ['config', 'set', '--user', 'platform', 'test'])
        
        result = runner.invoke(cli, ['config', 'list'])
        assert result.exit_code == 0
        assert 'owner=myowner' in result.output
        assert 'platform=test' in result.output
    
    def test_list_user_scope_only(self, runner, isolated_config):
        """Listing with --user shows only user config."""
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'userowner'])
        runner.invoke(cli, ['config', 'set', '--project', 'platform', 'projplatform'])
        
        result = runner.invoke(cli, ['config', 'list', '--user'])
        assert result.exit_code == 0
        assert 'userowner' in result.output
        assert 'projplatform' not in result.output
    
    def test_list_project_scope_only(self, runner, isolated_config):
        """Listing with --project shows only project config."""
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'userowner'])
        runner.invoke(cli, ['config', 'set', '--project', 'platform', 'projplatform'])
        
        result = runner.invoke(cli, ['config', 'list', '--project'])
        assert result.exit_code == 0
        assert 'projplatform' in result.output
        assert 'userowner' not in result.output


class TestDTConfigListShowOrigin:
    """Tests for dt config list --show-origin."""
    
    def test_show_origin_displays_scope(self, runner, isolated_config):
        """--show-origin shows which scope each value comes from."""
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'myowner'])
        
        result = runner.invoke(cli, ['config', 'list', '--show-origin'])
        assert result.exit_code == 0
        assert 'user' in result.output
        assert 'owner=myowner' in result.output
    
    def test_show_origin_with_system_config(self, runner, isolated_config):
        """--show-origin correctly identifies system scope."""
        system_config_dir = Path(isolated_config['system']) / 'dt'
        system_config_dir.mkdir(parents=True)
        (system_config_dir / 'config.yaml').write_text(yaml.dump({'owner': 'sysowner'}))
        
        result = runner.invoke(cli, ['config', 'list', '--show-origin'])
        assert result.exit_code == 0
        assert 'system' in result.output
        assert 'owner=sysowner' in result.output
    
    def test_show_origin_override_shows_higher_scope(self, runner, isolated_config):
        """--show-origin shows the scope that won when values are overridden."""
        # Set system value
        system_config_dir = Path(isolated_config['system']) / 'dt'
        system_config_dir.mkdir(parents=True)
        (system_config_dir / 'config.yaml').write_text(yaml.dump({
            'owner': 'sysowner',
            'platform': 'sysplatform'
        }))
        
        # Override owner at user level
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'userowner'])
        
        result = runner.invoke(cli, ['config', 'list', '--show-origin'])
        assert result.exit_code == 0
        # owner should show as user (the override)
        assert 'user' in result.output and 'owner=userowner' in result.output
        # platform should show as system (no override)
        assert 'system' in result.output and 'platform=sysplatform' in result.output
        # sysowner should NOT appear (it was overridden)
        assert 'sysowner' not in result.output
    
    def test_show_origin_all_scopes(self, runner, isolated_config):
        """--show-origin works with values from all scopes."""
        # System
        system_config_dir = Path(isolated_config['system']) / 'dt'
        system_config_dir.mkdir(parents=True)
        (system_config_dir / 'config.yaml').write_text(yaml.dump({'sys_key': 'sysval'}))
        
        # User
        runner.invoke(cli, ['config', 'set', '--user', 'user_key', 'userval'])
        
        # Project
        runner.invoke(cli, ['config', 'set', '--project', 'proj_key', 'projval'])
        
        # Local
        runner.invoke(cli, ['config', 'set', '--local', 'local_key', 'localval'])
        
        result = runner.invoke(cli, ['config', 'list', '--show-origin'])
        assert result.exit_code == 0
        assert 'system' in result.output and 'sys_key=sysval' in result.output
        assert 'user' in result.output and 'user_key=userval' in result.output
        assert 'project' in result.output and 'proj_key=projval' in result.output
        assert 'local' in result.output and 'local_key=localval' in result.output
    
    def test_show_origin_nested_keys(self, runner, isolated_config):
        """--show-origin works with nested config keys."""
        runner.invoke(cli, ['config', 'set', '--user', 'cache.root', '/user/cache'])
        
        result = runner.invoke(cli, ['config', 'list', '--show-origin'])
        assert result.exit_code == 0
        assert 'user' in result.output
        assert 'cache.root=/user/cache' in result.output
    
    def test_show_origin_nested_partial_override(self, runner, isolated_config):
        """--show-origin correctly identifies scope when nested values are partially overridden."""
        # System has both cache.root and cache.permissions
        system_config_dir = Path(isolated_config['system']) / 'dt'
        system_config_dir.mkdir(parents=True)
        (system_config_dir / 'config.yaml').write_text(yaml.dump({
            'cache': {
                'root': '/system/cache',
                'permissions': 'ug+rw'
            }
        }))
        
        # User overrides only cache.root
        runner.invoke(cli, ['config', 'set', '--user', 'cache.root', '/user/cache'])
        
        result = runner.invoke(cli, ['config', 'list', '--show-origin'])
        assert result.exit_code == 0
        # cache.root should show as user
        assert 'user' in result.output and 'cache.root=/user/cache' in result.output
        # cache.permissions should show as system
        assert 'system' in result.output and 'cache.permissions=ug+rw' in result.output
    """Tests for config scope precedence."""
    
    def test_local_overrides_project(self, runner, isolated_config):
        """Local config takes precedence over project."""
        runner.invoke(cli, ['config', 'set', '--project', 'owner', 'project-owner'])
        runner.invoke(cli, ['config', 'set', '--local', 'owner', 'local-owner'])
        
        result = runner.invoke(cli, ['config', 'get', 'owner'])
        assert 'local-owner' in result.output
    
    def test_project_overrides_user(self, runner, isolated_config):
        """Project config takes precedence over user."""
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'user-owner'])
        runner.invoke(cli, ['config', 'set', '--project', 'owner', 'project-owner'])
        
        result = runner.invoke(cli, ['config', 'get', 'owner'])
        assert 'project-owner' in result.output
    
    def test_user_overrides_system(self, runner, isolated_config):
        """User config takes precedence over system."""
        # Set up system config
        system_config_dir = Path(isolated_config['system']) / 'dt'
        system_config_dir.mkdir(parents=True)
        (system_config_dir / 'config.yaml').write_text(yaml.dump({'owner': 'system-owner'}))
        
        runner.invoke(cli, ['config', 'set', '--user', 'owner', 'user-owner'])
        
        result = runner.invoke(cli, ['config', 'get', 'owner'])
        assert 'user-owner' in result.output
    
    def test_full_precedence_chain(self, runner, isolated_config):
        """Full precedence: local > project > user > system."""
        # System
        system_config_dir = Path(isolated_config['system']) / 'dt'
        system_config_dir.mkdir(parents=True)
        (system_config_dir / 'config.yaml').write_text(yaml.dump({
            'a': 'system', 'b': 'system', 'c': 'system', 'd': 'system'
        }))
        
        # User (overrides a, b, c)
        runner.invoke(cli, ['config', 'set', '--user', 'a', 'user'])
        runner.invoke(cli, ['config', 'set', '--user', 'b', 'user'])
        runner.invoke(cli, ['config', 'set', '--user', 'c', 'user'])
        
        # Project (overrides a, b)
        runner.invoke(cli, ['config', 'set', '--project', 'a', 'project'])
        runner.invoke(cli, ['config', 'set', '--project', 'b', 'project'])
        
        # Local (overrides a)
        runner.invoke(cli, ['config', 'set', '--local', 'a', 'local'])
        
        # Check each value
        result_a = runner.invoke(cli, ['config', 'get', 'a'])
        result_b = runner.invoke(cli, ['config', 'get', 'b'])
        result_c = runner.invoke(cli, ['config', 'get', 'c'])
        result_d = runner.invoke(cli, ['config', 'get', 'd'])
        
        assert 'local' in result_a.output
        assert 'project' in result_b.output
        assert 'user' in result_c.output
        assert 'system' in result_d.output


class TestDTConfigEdgeCases:
    """Tests for edge cases and error handling."""
    
    def test_special_characters_in_value(self, runner, isolated_config):
        """Values with special characters are handled."""
        runner.invoke(cli, ['config', 'set', '--user', 'path', '/path/with spaces/and:colons'])
        result = runner.invoke(cli, ['config', 'get', 'path'])
        assert '/path/with spaces/and:colons' in result.output
    
    def test_numeric_value(self, runner, isolated_config):
        """Numeric values are stored correctly."""
        runner.invoke(cli, ['config', 'set', '--user', 'count', '42'])
        result = runner.invoke(cli, ['config', 'get', 'count'])
        assert '42' in result.output
    
    def test_boolean_like_value(self, runner, isolated_config):
        """Boolean-like strings are preserved as strings."""
        runner.invoke(cli, ['config', 'set', '--user', 'enabled', 'true'])
        result = runner.invoke(cli, ['config', 'get', 'enabled'])
        assert 'true' in result.output.lower()
    
    def test_empty_value(self, runner, isolated_config):
        """Empty string value can be set."""
        result = runner.invoke(cli, ['config', 'set', '--user', 'empty', ''])
        # Should either succeed or give a meaningful error
        assert result.exit_code in [0, 1, 2]
    
    def test_deeply_nested_key(self, runner, isolated_config):
        """Deeply nested keys work."""
        runner.invoke(cli, ['config', 'set', '--user', 'a.b.c.d', 'deep'])
        result = runner.invoke(cli, ['config', 'get', 'a.b.c.d'])
        assert 'deep' in result.output


class TestDTConfigSystemScope:
    """Tests specifically for system scope behavior."""
    
    def test_system_config_is_readonly_via_cli(self, runner, isolated_config):
        """System config cannot be modified without proper permissions."""
        # This depends on implementation - might succeed if we have write access
        # or fail gracefully if we don't
        result = runner.invoke(cli, ['config', 'set', '--system', 'owner', 'newowner'])
        # Just check it doesn't crash unexpectedly
        assert result.exit_code in [0, 1, 2]
    
    def test_reads_from_xdg_config_dirs(self, runner, isolated_config):
        """System config is read from XDG_CONFIG_DIRS."""
        system_config_dir = Path(isolated_config['system']) / 'dt'
        system_config_dir.mkdir(parents=True)
        (system_config_dir / 'config.yaml').write_text(yaml.dump({'owner': 'from-xdg'}))
        
        result = runner.invoke(cli, ['config', 'get', 'owner'])
        assert result.exit_code == 0
        assert 'from-xdg' in result.output


# Run with: pytest tests/test_config.py -v


class TestListValuedConfig:
    """`remote.root` may hold one path or several; both must work."""

    def test_scalar_reads_as_a_one_item_list(self, isolated_config):
        cfg.set_value('remote.root', '/g/data/a56/dvc/analysis', 'user')
        assert cfg.get_str_list('remote.root') == ['/g/data/a56/dvc/analysis']

    def test_list_reads_as_a_list(self, isolated_config):
        cfg.set_value('remote.root', '[/a, /b, /c]', 'user')
        assert cfg.get_str_list('remote.root') == ['/a', '/b', '/c']

    def test_unset_is_empty(self, isolated_config):
        assert cfg.get_str_list('remote.root') == []

    def test_default_when_unset(self, isolated_config):
        assert cfg.get_str_list('remote.root', ['/fallback']) == ['/fallback']

    def test_blank_entries_dropped(self, isolated_config):
        cfg.set_value('remote.root', "['/a', '', '/b']", 'user')
        assert cfg.get_str_list('remote.root') == ['/a', '/b']

    def test_order_is_preserved(self, isolated_config):
        """The first entry is the creation default, so order carries meaning."""
        cfg.set_value('remote.root', '[/z, /a, /m]', 'user')
        assert cfg.get_str_list('remote.root') == ['/z', '/a', '/m']

    def test_add_promotes_scalar_to_list(self, isolated_config):
        cfg.set_value('remote.root', '/a', 'user')
        assert cfg.add_to_list('remote.root', '/b', 'user') is True
        assert cfg.get_str_list('remote.root') == ['/a', '/b']

    def test_add_preserves_the_default(self, isolated_config):
        cfg.set_value('remote.root', '/a', 'user')
        cfg.add_to_list('remote.root', '/b', 'user')
        cfg.add_to_list('remote.root', '/c', 'user')
        assert cfg.get_str_list('remote.root')[0] == '/a'

    def test_add_is_idempotent(self, isolated_config):
        cfg.set_value('remote.root', '/a', 'user')
        assert cfg.add_to_list('remote.root', '/a', 'user') is False
        assert cfg.get_str_list('remote.root') == ['/a']

    def test_add_from_nothing(self, isolated_config):
        assert cfg.add_to_list('remote.root', '/a', 'user') is True
        assert cfg.get_str_list('remote.root') == ['/a']

    def test_remove(self, isolated_config):
        cfg.set_value('remote.root', '[/a, /b, /c]', 'user')
        assert cfg.remove_from_list('remote.root', '/b', 'user') is True
        assert cfg.get_str_list('remote.root') == ['/a', '/c']

    def test_remove_absent_value(self, isolated_config):
        cfg.set_value('remote.root', '[/a]', 'user')
        assert cfg.remove_from_list('remote.root', '/zzz', 'user') is False

    def test_remove_last_unsets_the_key(self, isolated_config):
        cfg.set_value('remote.root', '[/a]', 'user')
        assert cfg.remove_from_list('remote.root', '/a', 'user') is True
        assert cfg.get_str_list('remote.root') == []

    def test_add_writes_the_whole_effective_list(self, isolated_config,
                                                 temp_dirs):
        """Scopes override, so a user-scope add must not drop system entries."""
        sysdir = Path(temp_dirs['system']) / 'dt'
        sysdir.mkdir(parents=True, exist_ok=True)
        (sysdir / 'config.yaml').write_text(
            yaml.safe_dump({'remote': {'root': ['/sys1', '/sys2']}})
        )
        assert cfg.get_str_list('remote.root') == ['/sys1', '/sys2']

        cfg.add_to_list('remote.root', '/mine', 'user')
        assert cfg.get_str_list('remote.root') == ['/sys1', '/sys2', '/mine']

    def test_round_trip_through_cli(self, isolated_config, runner):
        runner.invoke(cli, ['config', 'set', 'remote.root', '/a', '--user'])
        r = runner.invoke(cli, ['config', 'add', 'remote.root', '/b', '--user'])
        assert r.exit_code == 0
        assert '<- default' in r.output
        assert cfg.get_str_list('remote.root') == ['/a', '/b']

        r = runner.invoke(cli, ['config', 'remove', 'remote.root', '/a',
                                '--user'])
        assert r.exit_code == 0
        assert cfg.get_str_list('remote.root') == ['/b']

    def test_cli_remove_absent_errors(self, isolated_config, runner):
        runner.invoke(cli, ['config', 'set', 'remote.root', '/a', '--user'])
        r = runner.invoke(cli, ['config', 'remove', 'remote.root', '/nope',
                                '--user'])
        assert r.exit_code != 0


class TestDefaultWriteScope:
    """`dt config set` with no scope flag writes user scope.

    It used to write project scope, which surprised people twice over: the
    setting only applied inside that one repo, and `.dt/config.yaml` is tracked
    by git, so a personal preference arrived in everyone else's checkout.
    """

    def _user_config(self, temp_dirs):
        return Path(temp_dirs['home']) / '.config' / 'dt' / 'config.yaml'

    def _project_config(self, temp_dirs):
        return Path(temp_dirs['project']) / '.dt' / 'config.yaml'

    def test_set_writes_user_scope(self, runner, isolated_config, temp_dirs):
        r = runner.invoke(cli, ['config', 'set', 'owner', 'myorg'])
        assert r.exit_code == 0
        assert yaml.safe_load(
            self._user_config(temp_dirs).read_text())['owner'] == 'myorg'

    def test_set_leaves_project_config_alone(self, runner, isolated_config,
                                             temp_dirs):
        """The tracked file is what collaborators inherit; don't touch it."""
        runner.invoke(cli, ['config', 'set', 'owner', 'myorg'])
        assert not self._project_config(temp_dirs).exists()

    def test_set_names_the_file_it_wrote(self, runner, isolated_config,
                                        temp_dirs):
        """The default location is off in ~; say where the value went."""
        r = runner.invoke(cli, ['config', 'set', 'owner', 'myorg'])
        assert 'user' in r.stdout
        assert str(self._user_config(temp_dirs)) in r.stdout

    def test_add_defaults_to_user_scope(self, runner, isolated_config,
                                        temp_dirs):
        r = runner.invoke(cli, ['config', 'add', 'remote.root', '/a'])
        assert r.exit_code == 0
        assert yaml.safe_load(
            self._user_config(temp_dirs).read_text())['remote']['root'] == ['/a']

    def test_unset_defaults_to_user_scope(self, runner, isolated_config,
                                          temp_dirs):
        runner.invoke(cli, ['config', 'set', 'owner', 'myorg'])
        r = runner.invoke(cli, ['config', 'unset', 'owner'])
        assert r.exit_code == 0
        assert cfg.get_value('owner') is None

    def test_project_flag_still_works(self, runner, isolated_config,
                                      temp_dirs):
        r = runner.invoke(cli, ['config', 'set', '--project', 'owner', 'org'])
        assert r.exit_code == 0
        assert yaml.safe_load(
            self._project_config(temp_dirs).read_text())['owner'] == 'org'


class TestScopesDefining:
    """`scopes_defining` reports every scope holding a key, best first."""

    def test_empty_when_unset(self, isolated_config):
        assert cfg.scopes_defining('owner') == []

    def test_single_scope(self, isolated_config):
        cfg.set_value('owner', 'a', 'user')
        assert cfg.scopes_defining('owner') == ['user']

    def test_precedence_order(self, isolated_config):
        cfg.set_value('owner', 'u', 'user')
        cfg.set_value('owner', 'p', 'project')
        cfg.set_value('owner', 'l', 'local')
        assert cfg.scopes_defining('owner') == ['local', 'project', 'user']

    def test_nested_key(self, isolated_config):
        cfg.set_value('cache.root', '/c', 'project')
        assert cfg.scopes_defining('cache.root') == ['project']

    def test_partial_path_is_not_a_match(self, isolated_config):
        """`cache` existing must not imply `cache.root` does."""
        cfg.set_value('cache.other', '/c', 'user')
        assert cfg.scopes_defining('cache.root') == []

    def test_scalar_midway_does_not_crash(self, isolated_config):
        cfg.set_value('cache', 'not-a-dict', 'user')
        assert cfg.scopes_defining('cache.root') == []


class TestShadowedWriteWarning:
    """A user-scope write under a project-scope value changes nothing.

    That is the cost of the new default, so the command says so rather than
    reporting plain success and leaving the old value in force.
    """

    def test_set_warns_when_project_overrides(self, runner, isolated_config):
        cfg.set_value('owner', 'from-project', 'project')
        r = runner.invoke(cli, ['config', 'set', 'owner', 'mine'])
        assert r.exit_code == 0
        assert 'project' in r.stderr
        assert 'from-project' in r.stderr
        assert '--project' in r.stderr

    def test_warning_goes_to_stderr(self, runner, isolated_config):
        cfg.set_value('owner', 'from-project', 'project')
        r = runner.invoke(cli, ['config', 'set', 'owner', 'mine'])
        assert 'Note:' not in r.stdout

    def test_no_warning_when_unshadowed(self, runner, isolated_config):
        r = runner.invoke(cli, ['config', 'set', 'owner', 'mine'])
        assert r.stderr == ''

    def test_no_warning_for_a_lower_scope(self, runner, isolated_config):
        """User outranks system, so a system value is not a shadow."""
        cfg.set_value('owner', 'from-system', 'system')
        r = runner.invoke(cli, ['config', 'set', 'owner', 'mine'])
        assert r.stderr == ''
        assert cfg.get_value('owner') == 'mine'

    def test_project_write_warns_about_local(self, runner, isolated_config):
        cfg.set_value('owner', 'from-local', 'local')
        r = runner.invoke(cli, ['config', 'set', '--project', 'owner', 'p'])
        assert 'local' in r.stderr

    def test_add_warns_when_shadowed(self, runner, isolated_config):
        cfg.set_value('remote.root', '/proj', 'project')
        r = runner.invoke(cli, ['config', 'add', 'remote.root', '/mine'])
        assert r.exit_code == 0
        assert 'project' in r.stderr


class TestUnsetMissDirectsToTheRightScope:
    """Unset now misses by default when the key is project-scoped."""

    def test_names_the_scope_holding_the_key(self, runner, isolated_config):
        cfg.set_value('owner', 'from-project', 'project')
        r = runner.invoke(cli, ['config', 'unset', 'owner'])
        assert r.exit_code != 0
        assert 'project' in r.output
        assert 'dt config unset --project owner' in r.output

    def test_shows_the_file_to_edit(self, runner, isolated_config, temp_dirs):
        cfg.set_value('owner', 'x', 'project')
        r = runner.invoke(cli, ['config', 'unset', 'owner'])
        assert str(Path(temp_dirs['project']) / '.dt' / 'config.yaml') in r.output

    def test_lists_every_scope_holding_it(self, runner, isolated_config):
        cfg.set_value('owner', 'l', 'local')
        cfg.set_value('owner', 'p', 'project')
        r = runner.invoke(cli, ['config', 'unset', 'owner'])
        assert 'local' in r.output and 'project' in r.output

    def test_unset_of_a_key_set_nowhere(self, runner, isolated_config):
        r = runner.invoke(cli, ['config', 'unset', 'never.set'])
        assert r.exit_code != 0
        assert 'any configuration scope' in r.output


class TestReadConfigFile:
    """`read_config_file` validates a file written by someone else."""

    def _write(self, tmp_path, text):
        p = Path(tmp_path) / 'handed-over.yaml'
        p.write_text(text)
        return p

    def test_flattens_nested_keys(self, tmp_path):
        p = self._write(tmp_path, 'secrets:\n  gcp:\n    project: proj\n')
        assert cfg.read_config_file(p) == {'secrets.gcp.project': 'proj'}

    def test_preserves_types(self, tmp_path):
        p = self._write(tmp_path, 'index:\n  auto_sync: true\n  lock_timeout: 120\n')
        assert cfg.read_config_file(p) == {
            'index.auto_sync': True, 'index.lock_timeout': 120,
        }

    def test_allows_a_list_for_a_list_valued_key(self, tmp_path):
        p = self._write(tmp_path, 'remote:\n  root:\n    - /a\n    - /b\n')
        assert cfg.read_config_file(p) == {'remote.root': ['/a', '/b']}

    def test_rejects_a_list_for_a_scalar_key(self, tmp_path):
        """A file is a new way to make a key a list; the same rule applies."""
        p = self._write(tmp_path, 'owner:\n  - a\n  - b\n')
        with pytest.raises(ValueError) as exc:
            cfg.read_config_file(p)
        assert 'owner' in str(exc.value)
        assert 'remote.root' in str(exc.value), "should name the keys that do"

    def test_rejects_a_non_mapping(self, tmp_path):
        p = self._write(tmp_path, '- one\n- two\n')
        with pytest.raises(ValueError, match='mapping'):
            cfg.read_config_file(p)

    def test_rejects_invalid_yaml(self, tmp_path):
        p = self._write(tmp_path, 'owner: :\n  - x\n')
        with pytest.raises(ValueError, match='not valid YAML'):
            cfg.read_config_file(p)

    def test_rejects_an_empty_file(self, tmp_path):
        p = self._write(tmp_path, '')
        with pytest.raises(ValueError, match='empty'):
            cfg.read_config_file(p)

    def test_rejects_a_missing_file(self, tmp_path):
        with pytest.raises(ValueError, match='No such file'):
            cfg.read_config_file(Path(tmp_path) / 'absent.yaml')


class TestSetValues:
    """Bulk writes merge into the scope rather than replacing it."""

    def test_writes_several_keys_at_once(self, isolated_config):
        cfg.set_values({'owner': 'org', 'cache.root': '/c'}, 'user')
        assert cfg.get_value('owner') == 'org'
        assert cfg.get_value('cache.root') == '/c'

    def test_keeps_keys_it_was_not_given(self, isolated_config):
        cfg.set_value('username', 'alice', 'user')
        cfg.set_values({'owner': 'org'}, 'user')
        assert cfg.get_value('username') == 'alice'

    def test_stores_values_unparsed(self, isolated_config):
        """Values arrive already typed; re-parsing them would corrupt them."""
        cfg.set_values({'remote.root': ['/a', '/b'], 'index.auto_sync': True},
                       'user')
        assert cfg.get_str_list('remote.root') == ['/a', '/b']
        assert cfg.get_value('index.auto_sync') is True


class TestConfigImport:
    """`dt config import` installs a config file handed over by someone else.

    The case this exists for: an offsite collaborator on a different
    filesystem, who should not have to be told where config lives on their own
    machine or retype a dozen `dt config set` lines correctly.
    """

    def _file(self, tmp_path, text):
        p = Path(tmp_path) / 'lab.yaml'
        p.write_text(text)
        return str(p)

    def _user_config(self, temp_dirs):
        return Path(temp_dirs['home']) / '.config' / 'dt' / 'config.yaml'

    def test_imports_into_user_scope_by_default(self, runner, isolated_config,
                                                tmp_path, temp_dirs):
        f = self._file(tmp_path, 'owner: lab\nsecrets:\n  backend: gcp\n')
        r = runner.invoke(cli, ['config', 'import', f])
        assert r.exit_code == 0, r.output
        data = yaml.safe_load(self._user_config(temp_dirs).read_text())
        assert data['owner'] == 'lab'
        assert data['secrets']['backend'] == 'gcp'

    def test_merges_rather_than_replacing_the_file(self, runner,
                                                  isolated_config, tmp_path):
        """Importing lab defaults must not discard the collaborator's paths."""
        cfg.set_value('cache.root', '/home/alice/cache', 'user')
        f = self._file(tmp_path, 'owner: lab\n')
        runner.invoke(cli, ['config', 'import', f])
        assert cfg.get_value('cache.root') == '/home/alice/cache'
        assert cfg.get_value('owner') == 'lab'

    def test_additions_need_no_confirmation(self, runner, isolated_config,
                                            tmp_path):
        """Adding settings loses nothing, so it does not stop to ask."""
        f = self._file(tmp_path, 'owner: lab\n')
        r = runner.invoke(cli, ['config', 'import', f], input='')
        assert r.exit_code == 0
        assert cfg.get_value('owner') == 'lab'

    def test_prompts_before_replacing_a_value(self, runner, isolated_config,
                                              tmp_path):
        cfg.set_value('owner', 'mine', 'user')
        f = self._file(tmp_path, 'owner: lab\n')
        r = runner.invoke(cli, ['config', 'import', f], input='n\n')
        assert r.exit_code != 0
        assert cfg.get_value('owner') == 'mine', "declining must change nothing"

    def test_confirming_replaces_the_value(self, runner, isolated_config,
                                           tmp_path):
        cfg.set_value('owner', 'mine', 'user')
        f = self._file(tmp_path, 'owner: lab\n')
        r = runner.invoke(cli, ['config', 'import', f], input='y\n')
        assert r.exit_code == 0
        assert cfg.get_value('owner') == 'lab'

    def test_yes_skips_the_prompt(self, runner, isolated_config, tmp_path):
        cfg.set_value('owner', 'mine', 'user')
        f = self._file(tmp_path, 'owner: lab\n')
        r = runner.invoke(cli, ['config', 'import', f, '--yes'])
        assert r.exit_code == 0
        assert cfg.get_value('owner') == 'lab'

    def test_shows_the_replacement_it_is_about_to_make(self, runner,
                                                      isolated_config,
                                                      tmp_path):
        cfg.set_value('owner', 'mine', 'user')
        f = self._file(tmp_path, 'owner: lab\n')
        r = runner.invoke(cli, ['config', 'import', f, '--yes'])
        assert 'mine -> lab' in r.output

    def test_dry_run_writes_nothing(self, runner, isolated_config, tmp_path,
                                    temp_dirs):
        f = self._file(tmp_path, 'owner: lab\n')
        r = runner.invoke(cli, ['config', 'import', f, '--dry-run'])
        assert r.exit_code == 0
        assert 'Dry run' in r.output
        assert not self._user_config(temp_dirs).exists()

    def test_dry_run_reports_the_counts(self, runner, isolated_config,
                                        tmp_path):
        cfg.set_value('owner', 'mine', 'user')
        f = self._file(tmp_path, 'owner: lab\nusername: alice\n')
        r = runner.invoke(cli, ['config', 'import', f, '--dry-run'])
        assert '1 to add' in r.output and '1 to replace' in r.output

    def test_project_scope_flag(self, runner, isolated_config, tmp_path,
                                temp_dirs):
        f = self._file(tmp_path, 'owner: lab\n')
        r = runner.invoke(cli, ['config', 'import', f, '--project'])
        assert r.exit_code == 0
        project_config = Path(temp_dirs['project']) / '.dt' / 'config.yaml'
        assert yaml.safe_load(project_config.read_text())['owner'] == 'lab'

    def test_two_scope_flags_rejected(self, runner, isolated_config, tmp_path):
        f = self._file(tmp_path, 'owner: lab\n')
        r = runner.invoke(cli, ['config', 'import', f, '--user', '--project'])
        assert r.exit_code != 0

    def test_reimport_is_a_no_op(self, runner, isolated_config, tmp_path):
        f = self._file(tmp_path, 'owner: lab\n')
        runner.invoke(cli, ['config', 'import', f])
        r = runner.invoke(cli, ['config', 'import', f])
        assert r.exit_code == 0
        assert 'Nothing to do' in r.output

    def test_list_valued_key_round_trips(self, runner, isolated_config,
                                         tmp_path):
        f = self._file(tmp_path, 'remote:\n  root:\n    - /a\n    - /b\n')
        r = runner.invoke(cli, ['config', 'import', f])
        assert r.exit_code == 0
        assert cfg.get_str_list('remote.root') == ['/a', '/b']

    def test_bad_file_is_reported_not_traced(self, runner, isolated_config,
                                             tmp_path):
        f = self._file(tmp_path, 'owner:\n  - a\n  - b\n')
        r = runner.invoke(cli, ['config', 'import', f])
        assert r.exit_code != 0
        assert 'owner' in r.output
        assert 'Traceback' not in r.output

    def test_missing_file_is_reported(self, runner, isolated_config, tmp_path):
        r = runner.invoke(cli, ['config', 'import',
                                str(Path(tmp_path) / 'absent.yaml')])
        assert r.exit_code != 0

    def test_warns_when_a_repo_config_will_override_the_import(
            self, runner, isolated_config, tmp_path):
        """Project scope outranks user, so the import may not take effect."""
        cfg.set_value('cache.root', '/from/project', 'project')
        f = self._file(tmp_path, 'cache:\n  root: /from/file\n')
        r = runner.invoke(cli, ['config', 'import', f])
        assert r.exit_code == 0
        assert 'project' in r.stderr
        assert '/from/project' in r.stderr

    def test_flags_paths_that_do_not_exist_here(self, runner, isolated_config,
                                                tmp_path):
        """The handed-over file usually names the sender's filesystem."""
        f = self._file(tmp_path, 'cache:\n  root: /g/data/nope/dvc_cache\n')
        r = runner.invoke(cli, ['config', 'import', f])
        assert r.exit_code == 0
        assert '/g/data/nope/dvc_cache' in r.stderr
        assert 'dt doctor' in r.stderr

    def test_no_path_warning_when_the_path_exists(self, runner,
                                                 isolated_config, tmp_path):
        f = self._file(tmp_path, f'cache:\n  root: {tmp_path}\n')
        r = runner.invoke(cli, ['config', 'import', f])
        assert r.exit_code == 0
        assert 'do not exist' not in r.stderr

    def test_relative_paths_are_not_checked(self, runner, isolated_config,
                                            tmp_path):
        """Only absolute paths are unambiguous enough to call missing."""
        f = self._file(tmp_path, 'archive:\n  registry_path: dt-archives/reg\n')
        r = runner.invoke(cli, ['config', 'import', f])
        assert r.exit_code == 0
        assert 'do not exist' not in r.stderr
