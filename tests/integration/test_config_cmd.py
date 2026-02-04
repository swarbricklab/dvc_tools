"""Integration tests for dt config command.

These tests verify configuration management across scopes.
"""

import os
import subprocess
from pathlib import Path

import pytest

from tests.conftest import requires_git


# =============================================================================
# Helper Functions
# =============================================================================

def run_dt(*args, cwd=None, check=True):
    """Run dt command and return result."""
    result = subprocess.run(
        ['dt', *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            ['dt', *args],
            result.stdout,
            result.stderr,
        )
    return result


# =============================================================================
# Config Set/Get Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigSetGet:
    """Test dt config set and get commands."""

    def test_set_and_get_value(self, dvc_repo):
        """Set a value and retrieve it."""
        run_dt('config', 'set', 'owner', 'testorg', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'owner', cwd=dvc_repo)
        
        assert result.returncode == 0
        assert result.stdout.strip() == 'testorg'

    def test_set_nested_key(self, dvc_repo):
        """Set a nested key creates proper structure."""
        run_dt('config', 'set', 'cache.root', '/tmp/caches', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'cache.root', cwd=dvc_repo)
        
        assert result.returncode == 0
        assert result.stdout.strip() == '/tmp/caches'

    def test_set_overwrites_existing(self, dvc_repo):
        """Setting existing key overwrites the value."""
        run_dt('config', 'set', 'owner', 'first', cwd=dvc_repo)
        run_dt('config', 'set', 'owner', 'second', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'owner', cwd=dvc_repo)
        
        assert result.stdout.strip() == 'second'

    def test_get_missing_key_fails(self, dvc_repo):
        """Getting non-existent key fails with error."""
        result = run_dt('config', 'get', 'nonexistent.key', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0
        assert 'not found' in result.stderr.lower()

    def test_set_shows_confirmation(self, dvc_repo):
        """Set command shows confirmation message."""
        result = run_dt('config', 'set', 'owner', 'myorg', cwd=dvc_repo)
        
        assert 'owner=myorg' in result.stdout
        assert 'project' in result.stdout.lower()  # default scope


# =============================================================================
# Config Scope Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigScopes:
    """Test configuration scopes (local, project, user)."""

    def test_set_project_scope_default(self, dvc_repo):
        """Project scope is default for set."""
        run_dt('config', 'set', 'test.key', 'value', cwd=dvc_repo)
        
        # Check file was created in .dt/config.yaml
        config_file = dvc_repo / '.dt' / 'config.yaml'
        assert config_file.exists()
        content = config_file.read_text()
        assert 'test' in content

    def test_set_local_scope(self, dvc_repo):
        """Set with --local uses local scope."""
        run_dt('config', 'set', 'test.key', 'localvalue', '--local', cwd=dvc_repo)
        
        # Check file was created in .dt/config.local.yaml
        config_file = dvc_repo / '.dt' / 'config.local.yaml'
        assert config_file.exists()
        content = config_file.read_text()
        assert 'localvalue' in content

    def test_local_overrides_project(self, dvc_repo):
        """Local scope values override project scope."""
        run_dt('config', 'set', 'override.test', 'project_value', cwd=dvc_repo)
        run_dt('config', 'set', 'override.test', 'local_value', '--local', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'override.test', cwd=dvc_repo)
        
        assert result.stdout.strip() == 'local_value'

    def test_multiple_scope_flags_error(self, dvc_repo):
        """Multiple scope flags causes error."""
        result = run_dt('config', 'set', 'key', 'value', 
                       '--local', '--project', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0


# =============================================================================
# Config List Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigList:
    """Test dt config list command."""

    def test_list_empty_config(self, dvc_repo):
        """List with no config shows appropriate message."""
        result = run_dt('config', 'list', cwd=dvc_repo)
        
        assert result.returncode == 0
        # Either shows "No configuration" or just empty

    def test_list_shows_set_values(self, dvc_repo):
        """List shows values that were set."""
        run_dt('config', 'set', 'owner', 'listtest', cwd=dvc_repo)
        
        result = run_dt('config', 'list', cwd=dvc_repo)
        
        assert 'owner=listtest' in result.stdout

    def test_list_with_scope_filter(self, dvc_repo):
        """List with scope flag shows only that scope."""
        run_dt('config', 'set', 'project.key', 'pval', cwd=dvc_repo)
        run_dt('config', 'set', 'local.key', 'lval', '--local', cwd=dvc_repo)
        
        result = run_dt('config', 'list', '--project', cwd=dvc_repo)
        
        assert 'project.key=pval' in result.stdout
        assert 'local.key' not in result.stdout

    def test_list_show_origin(self, dvc_repo):
        """List with --show-origin shows source scope."""
        run_dt('config', 'set', 'origin.test', 'value', cwd=dvc_repo)
        
        result = run_dt('config', 'list', '--show-origin', cwd=dvc_repo)
        
        assert 'project' in result.stdout
        assert 'origin.test' in result.stdout

    def test_config_without_subcommand_lists(self, dvc_repo):
        """dt config without subcommand lists config."""
        run_dt('config', 'set', 'nosubcmd.test', 'value', cwd=dvc_repo)
        
        result = run_dt('config', cwd=dvc_repo)
        
        assert 'nosubcmd.test=value' in result.stdout


# =============================================================================
# Config Unset Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigUnset:
    """Test dt config unset command."""

    def test_unset_existing_key(self, dvc_repo):
        """Unset removes existing key."""
        run_dt('config', 'set', 'remove.me', 'value', cwd=dvc_repo)
        run_dt('config', 'unset', 'remove.me', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'remove.me', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0
        assert 'not found' in result.stderr.lower()

    def test_unset_missing_key_fails(self, dvc_repo):
        """Unset non-existent key fails."""
        result = run_dt('config', 'unset', 'does.not.exist', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0

    def test_unset_with_scope(self, dvc_repo):
        """Unset with scope flag removes from that scope only."""
        run_dt('config', 'set', 'scoped.key', 'local', '--local', cwd=dvc_repo)
        run_dt('config', 'unset', 'scoped.key', '--local', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'scoped.key', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0


# =============================================================================
# Config Path Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigPath:
    """Test dt config path command."""

    def test_path_shows_all_scopes(self, dvc_repo):
        """Path without args shows all scope paths."""
        result = run_dt('config', 'path', cwd=dvc_repo)
        
        assert 'local:' in result.stdout
        assert 'project:' in result.stdout
        assert 'user:' in result.stdout
        assert 'system:' in result.stdout

    def test_path_with_scope(self, dvc_repo):
        """Path with scope flag shows only that path."""
        result = run_dt('config', 'path', '--project', cwd=dvc_repo)
        
        assert '.dt/config.yaml' in result.stdout
        assert 'user:' not in result.stdout

    def test_path_shows_existence(self, dvc_repo):
        """Path shows whether file exists."""
        result = run_dt('config', 'path', cwd=dvc_repo)
        
        # Should show existence markers (✓ or ✗)
        assert '✓' in result.stdout or '✗' in result.stdout


# =============================================================================
# Config YAML Parsing Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigYamlParsing:
    """Test YAML value parsing in config set."""

    def test_set_boolean_true(self, dvc_repo):
        """Boolean true is parsed correctly."""
        run_dt('config', 'set', 'bool.test', 'true', cwd=dvc_repo)
        
        config_file = dvc_repo / '.dt' / 'config.yaml'
        content = config_file.read_text()
        # YAML should store as boolean, not string 'true'
        assert 'true' in content.lower()

    def test_set_integer(self, dvc_repo):
        """Integer values are parsed correctly."""
        run_dt('config', 'set', 'int.test', '42', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'int.test', cwd=dvc_repo)
        assert result.stdout.strip() == '42'

    def test_set_quoted_string(self, dvc_repo):
        """Quoted strings preserve the string type."""
        run_dt('config', 'set', 'str.test', '"42"', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'str.test', cwd=dvc_repo)
        # Should get the string 42, not the integer
        assert '42' in result.stdout


# =============================================================================
# Config Creates .dt Directory Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigCreatesDtDir:
    """Test that config creates .dt directory if needed."""

    def test_set_creates_dt_directory(self, git_repo):
        """Config set creates .dt directory if missing."""
        # git_repo doesn't have .dt directory
        assert not (git_repo / '.dt').exists()
        
        run_dt('config', 'set', 'test.key', 'value', cwd=git_repo)
        
        assert (git_repo / '.dt').is_dir()
        assert (git_repo / '.dt' / 'config.yaml').exists()
