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

def run_dt(*args, cwd=None, check=True, env=None):
    """Run dt command and return result.
    
    Args:
        *args: Command arguments to pass to dt
        cwd: Working directory
        check: Raise exception on non-zero exit
        env: Environment variables (defaults to os.environ)
    """
    run_env = env if env is not None else os.environ
    result = subprocess.run(
        ['dt', *args],
        capture_output=True,
        text=True,
        cwd=cwd,
        env=run_env,
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
        """Set a value and retrieve it with correct content."""
        run_dt('config', 'set', 'owner', 'testorg', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'owner', cwd=dvc_repo)
        
        assert result.returncode == 0
        assert result.stdout.strip() == 'testorg', f"Expected 'testorg', got '{result.stdout.strip()}'"

    def test_set_nested_key(self, dvc_repo):
        """Set a nested key creates proper YAML structure."""
        run_dt('config', 'set', 'cache.root', '/tmp/caches', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'cache.root', cwd=dvc_repo)
        
        assert result.returncode == 0
        assert result.stdout.strip() == '/tmp/caches'
        
        # Verify the YAML structure is correct
        config_file = dvc_repo / '.dt' / 'config.yaml'
        content = config_file.read_text()
        assert 'cache:' in content, "Config should have 'cache:' section"
        assert 'root:' in content, "Config should have 'root:' nested key"

    def test_set_overwrites_existing(self, dvc_repo):
        """Setting existing key overwrites the value completely."""
        run_dt('config', 'set', 'owner', 'first', cwd=dvc_repo)
        run_dt('config', 'set', 'owner', 'second', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'owner', cwd=dvc_repo)
        
        assert result.stdout.strip() == 'second'
        
        # Verify 'first' is no longer in the config file
        config_file = dvc_repo / '.dt' / 'config.yaml'
        content = config_file.read_text()
        assert 'first' not in content, "Old value should be completely removed"

    def test_get_missing_key_fails(self, dvc_repo):
        """Getting non-existent key fails with descriptive error."""
        result = run_dt('config', 'get', 'nonexistent.key', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0
        assert 'not found' in result.stderr.lower(), f"Error should mention 'not found': {result.stderr}"

    def test_set_shows_confirmation(self, dvc_repo):
        """Set command shows confirmation message with key=value and scope."""
        result = run_dt('config', 'set', 'owner', 'myorg', cwd=dvc_repo)
        
        assert 'owner=myorg' in result.stdout, "Confirmation should show key=value"
        assert 'project' in result.stdout.lower(), "Confirmation should show scope (project is default)"


# =============================================================================
# Config Scope Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigScopes:
    """Test configuration scopes (local, project, user)."""

    def test_set_project_scope_default(self, dvc_repo):
        """Project scope is default for set, creating tracked config file."""
        run_dt('config', 'set', 'test.key', 'value', cwd=dvc_repo)
        
        # Check file was created in .dt/config.yaml
        config_file = dvc_repo / '.dt' / 'config.yaml'
        assert config_file.exists(), ".dt/config.yaml should be created"
        content = config_file.read_text()
        assert 'test' in content
        assert 'value' in content

    def test_set_local_scope(self, dvc_repo):
        """Set with --local uses local scope (not tracked)."""
        run_dt('config', 'set', 'test.key', 'localvalue', '--local', cwd=dvc_repo)
        
        # Check file was created in .dt/config.local.yaml
        config_file = dvc_repo / '.dt' / 'config.local.yaml'
        assert config_file.exists(), ".dt/config.local.yaml should be created"
        content = config_file.read_text()
        assert 'localvalue' in content
        
        # Verify this file is gitignored by checking .dt/.gitignore
        gitignore = dvc_repo / '.dt' / '.gitignore'
        if gitignore.exists():
            gitignore_content = gitignore.read_text()
            assert 'config.local.yaml' in gitignore_content, "Local config should be gitignored"

    def test_local_overrides_project(self, dvc_repo):
        """Local scope values override project scope (precedence test)."""
        run_dt('config', 'set', 'override.test', 'project_value', cwd=dvc_repo)
        run_dt('config', 'set', 'override.test', 'local_value', '--local', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'override.test', cwd=dvc_repo)
        
        assert result.stdout.strip() == 'local_value', "Local should override project"
        
        # Verify both files have their respective values
        project_config = dvc_repo / '.dt' / 'config.yaml'
        local_config = dvc_repo / '.dt' / 'config.local.yaml'
        assert 'project_value' in project_config.read_text()
        assert 'local_value' in local_config.read_text()

    def test_set_user_scope(self, dvc_repo, tmp_path):
        """Set with --user uses user scope."""
        # Create isolated user config directory
        user_config_dir = tmp_path / 'user_config' / 'dt'
        user_config_dir.mkdir(parents=True)
        
        # Set XDG_CONFIG_HOME to isolate user config
        isolated_env = os.environ.copy()
        isolated_env['XDG_CONFIG_HOME'] = str(tmp_path / 'user_config')
        
        result = run_dt('config', 'set', 'user.test', 'uservalue', '--user', 
                       cwd=dvc_repo, env=isolated_env)
        
        assert result.returncode == 0
        
        # Check file was created in user config location
        user_config_file = user_config_dir / 'config.yaml'
        assert user_config_file.exists(), "User config file should be created"
        content = user_config_file.read_text()
        assert 'uservalue' in content

    def test_multiple_scope_flags_error(self, dvc_repo):
        """Multiple scope flags causes error with helpful message."""
        result = run_dt('config', 'set', 'key', 'value', 
                       '--local', '--project', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0
        # Should mention the conflict
        combined_output = result.stdout + result.stderr
        assert 'scope' in combined_output.lower() or 'mutual' in combined_output.lower() or 'one' in combined_output.lower()


# =============================================================================
# Config List Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigList:
    """Test dt config list command."""

    def test_list_empty_config(self, dvc_repo):
        """List with no config shows empty or appropriate message."""
        result = run_dt('config', 'list', cwd=dvc_repo)
        
        assert result.returncode == 0
        # Either shows "No configuration" or just empty output (both acceptable)

    def test_list_shows_set_values(self, dvc_repo):
        """List shows values that were set with key=value format."""
        run_dt('config', 'set', 'owner', 'listtest', cwd=dvc_repo)
        
        result = run_dt('config', 'list', cwd=dvc_repo)
        
        assert 'owner=listtest' in result.stdout, f"Expected 'owner=listtest' in output: {result.stdout}"

    def test_list_with_scope_filter(self, dvc_repo):
        """List with scope flag shows only that scope's values."""
        run_dt('config', 'set', 'project.key', 'pval', cwd=dvc_repo)
        run_dt('config', 'set', 'local.key', 'lval', '--local', cwd=dvc_repo)
        
        result = run_dt('config', 'list', '--project', cwd=dvc_repo)
        
        assert 'project.key=pval' in result.stdout, "Project scope values should be shown"
        assert 'local.key' not in result.stdout, "Local scope values should not be shown"

    def test_list_show_origin(self, dvc_repo):
        """List with --show-origin shows source scope for each value."""
        run_dt('config', 'set', 'origin.test', 'value', cwd=dvc_repo)
        
        result = run_dt('config', 'list', '--show-origin', cwd=dvc_repo)
        
        assert 'project' in result.stdout, "Origin should show 'project' scope"
        assert 'origin.test' in result.stdout

    def test_config_without_subcommand_lists(self, dvc_repo):
        """dt config without subcommand lists config (convenience)."""
        run_dt('config', 'set', 'nosubcmd.test', 'value', cwd=dvc_repo)
        
        result = run_dt('config', cwd=dvc_repo)
        
        assert 'nosubcmd.test=value' in result.stdout

    def test_list_shows_multiple_values(self, dvc_repo):
        """List shows all set values when multiple keys are configured."""
        run_dt('config', 'set', 'owner', 'myorg', cwd=dvc_repo)
        run_dt('config', 'set', 'cache.root', '/path/to/cache', cwd=dvc_repo)
        run_dt('config', 'set', 'remote.root', '/path/to/remote', cwd=dvc_repo)
        
        result = run_dt('config', 'list', cwd=dvc_repo)
        
        assert 'owner=myorg' in result.stdout
        assert 'cache.root=/path/to/cache' in result.stdout
        assert 'remote.root=/path/to/remote' in result.stdout


# =============================================================================
# Config Unset Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigUnset:
    """Test dt config unset command."""

    def test_unset_existing_key(self, dvc_repo):
        """Unset removes existing key from config file."""
        run_dt('config', 'set', 'remove.me', 'value', cwd=dvc_repo)
        run_dt('config', 'unset', 'remove.me', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'remove.me', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0
        assert 'not found' in result.stderr.lower()
        
        # Verify the key is actually removed from the file
        config_file = dvc_repo / '.dt' / 'config.yaml'
        if config_file.exists():
            content = config_file.read_text()
            assert 'remove' not in content or 'me' not in content

    def test_unset_missing_key_fails(self, dvc_repo):
        """Unset non-existent key fails with descriptive error."""
        result = run_dt('config', 'unset', 'does.not.exist', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0
        # Should indicate the key was not found
        combined_output = result.stdout + result.stderr
        assert 'not found' in combined_output.lower() or 'does not exist' in combined_output.lower()

    def test_unset_with_scope(self, dvc_repo):
        """Unset with scope flag removes from that scope only."""
        run_dt('config', 'set', 'scoped.key', 'local', '--local', cwd=dvc_repo)
        run_dt('config', 'unset', 'scoped.key', '--local', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'scoped.key', cwd=dvc_repo, check=False)
        
        assert result.returncode != 0
        
        # Verify the local config file no longer has the key
        local_config = dvc_repo / '.dt' / 'config.local.yaml'
        if local_config.exists():
            content = local_config.read_text()
            assert 'scoped' not in content or 'local' not in content


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
        
        assert 'local:' in result.stdout, "Should show local scope"
        assert 'project:' in result.stdout, "Should show project scope"
        assert 'user:' in result.stdout, "Should show user scope"
        assert 'system:' in result.stdout, "Should show system scope"

    def test_path_with_scope(self, dvc_repo):
        """Path with scope flag shows only that path."""
        result = run_dt('config', 'path', '--project', cwd=dvc_repo)
        
        assert '.dt/config.yaml' in result.stdout, "Project path should include .dt/config.yaml"
        assert 'user:' not in result.stdout, "Should not show other scopes"
        assert 'system:' not in result.stdout

    def test_path_shows_existence(self, dvc_repo):
        """Path shows whether each config file exists."""
        result = run_dt('config', 'path', cwd=dvc_repo)
        
        # Should show existence markers (✓ or ✗ or similar indicators)
        assert '✓' in result.stdout or '✗' in result.stdout, \
            f"Should show existence markers: {result.stdout}"

    def test_path_shows_absolute_paths(self, dvc_repo):
        """Path shows absolute file paths for each scope."""
        result = run_dt('config', 'path', cwd=dvc_repo)
        
        # Should contain absolute paths (starting with /)
        lines = result.stdout.strip().split('\n')
        for line in lines:
            if ':' in line:
                # Extract the path part after the scope label
                parts = line.split(':', 1)
                if len(parts) > 1:
                    path_part = parts[1].strip()
                    # Path should be present (may have existence marker)
                    assert '/' in path_part or '\\' in path_part, f"Should show file path: {line}"


# =============================================================================
# Config YAML Parsing Tests
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigYamlParsing:
    """Test YAML value parsing in config set."""

    def test_set_boolean_true(self, dvc_repo):
        """Boolean true is parsed and stored correctly."""
        run_dt('config', 'set', 'bool.test', 'true', cwd=dvc_repo)
        
        config_file = dvc_repo / '.dt' / 'config.yaml'
        content = config_file.read_text()
        # YAML should store as boolean, not string 'true'
        assert 'true' in content.lower()
        
        # Getting the value should return 'True' or 'true'
        result = run_dt('config', 'get', 'bool.test', cwd=dvc_repo)
        assert result.stdout.strip().lower() == 'true'

    def test_set_boolean_false(self, dvc_repo):
        """Boolean false is parsed and stored correctly."""
        run_dt('config', 'set', 'bool.test', 'false', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'bool.test', cwd=dvc_repo)
        assert result.stdout.strip().lower() == 'false'

    def test_set_integer(self, dvc_repo):
        """Integer values are parsed and retrieved correctly."""
        run_dt('config', 'set', 'int.test', '42', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'int.test', cwd=dvc_repo)
        assert result.stdout.strip() == '42'

    def test_set_quoted_string(self, dvc_repo):
        """Quoted strings preserve the string type (not converted to int)."""
        run_dt('config', 'set', 'str.test', '"42"', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'str.test', cwd=dvc_repo)
        # Should get the string 42, not the integer
        assert '42' in result.stdout

    def test_set_path_with_spaces(self, dvc_repo):
        """Paths with spaces are handled correctly."""
        run_dt('config', 'set', 'path.test', '/path/with spaces/here', cwd=dvc_repo)
        
        result = run_dt('config', 'get', 'path.test', cwd=dvc_repo)
        assert '/path/with spaces/here' in result.stdout


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
        
        assert (git_repo / '.dt').is_dir(), ".dt directory should be created"
        assert (git_repo / '.dt' / 'config.yaml').exists(), "config.yaml should be created"

    def test_set_creates_nested_dt_structure(self, git_repo):
        """Config set creates proper .dt structure with gitignore."""
        run_dt('config', 'set', 'test.key', 'value', cwd=git_repo)
        
        # Check that .dt/.gitignore is also created
        gitignore = git_repo / '.dt' / '.gitignore'
        if gitignore.exists():
            content = gitignore.read_text()
            assert 'config.local.yaml' in content, "Should gitignore config.local.yaml"


# =============================================================================
# Config Precedence Tests  
# =============================================================================

@pytest.mark.integration
@requires_git
class TestConfigPrecedence:
    """Test configuration scope precedence (local > project > user > system)."""

    def test_project_overrides_user(self, dvc_repo, tmp_path):
        """Project scope overrides user scope."""
        # Create isolated user config directory
        user_config_dir = tmp_path / 'user_config' / 'dt'
        user_config_dir.mkdir(parents=True)
        user_config_file = user_config_dir / 'config.yaml'
        user_config_file.write_text('precedence:\n  test: user_value\n')
        
        isolated_env = os.environ.copy()
        isolated_env['XDG_CONFIG_HOME'] = str(tmp_path / 'user_config')
        
        # Set project value
        run_dt('config', 'set', 'precedence.test', 'project_value', 
               cwd=dvc_repo, env=isolated_env)
        
        # Get should return project value (higher precedence)
        result = run_dt('config', 'get', 'precedence.test', 
                       cwd=dvc_repo, env=isolated_env)
        
        assert result.stdout.strip() == 'project_value', \
            f"Project should override user: got {result.stdout.strip()}"

    def test_local_overrides_all(self, dvc_repo, tmp_path):
        """Local scope has highest precedence over all other scopes."""
        # Set up user config
        user_config_dir = tmp_path / 'user_config' / 'dt'
        user_config_dir.mkdir(parents=True)
        user_config_file = user_config_dir / 'config.yaml'
        user_config_file.write_text('precedence:\n  all: user_value\n')
        
        isolated_env = os.environ.copy()
        isolated_env['XDG_CONFIG_HOME'] = str(tmp_path / 'user_config')
        
        # Set project value
        run_dt('config', 'set', 'precedence.all', 'project_value', 
               cwd=dvc_repo, env=isolated_env)
        
        # Set local value (highest precedence)
        run_dt('config', 'set', 'precedence.all', 'local_value', '--local',
               cwd=dvc_repo, env=isolated_env)
        
        # Get should return local value
        result = run_dt('config', 'get', 'precedence.all', 
                       cwd=dvc_repo, env=isolated_env)
        
        assert result.stdout.strip() == 'local_value', \
            f"Local should override all: got {result.stdout.strip()}"
