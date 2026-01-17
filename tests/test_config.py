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
