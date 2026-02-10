"""Integration tests for dt update command."""

import os
import subprocess

import pytest
import yaml


# =============================================================================
# Test Update Help
# =============================================================================

class TestUpdateHelp:
    """Tests for update command help."""
    
    def test_update_help(self):
        """'dt update --help' shows usage."""
        result = subprocess.run(
            ['dt', 'update', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'Update imported data' in result.stdout
        assert '--rev' in result.stdout
        assert '--no-download' in result.stdout
    
    def test_update_shows_examples(self):
        """Help shows usage examples."""
        result = subprocess.run(
            ['dt', 'update', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'Examples:' in result.stdout
        assert 'dt update' in result.stdout


# =============================================================================
# Test Update Options
# =============================================================================

class TestUpdateOptions:
    """Tests for update command options."""
    
    def test_update_rev_option(self):
        """'--rev' option is available."""
        result = subprocess.run(
            ['dt', 'update', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '--rev' in result.stdout
        assert 'revision' in result.stdout.lower()
    
    def test_update_no_download_option(self):
        """'--no-download' option is available."""
        result = subprocess.run(
            ['dt', 'update', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '--no-download' in result.stdout
    
    def test_update_dry_run_option(self):
        """'--dry-run' option is available."""
        result = subprocess.run(
            ['dt', 'update', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '--dry-run' in result.stdout


# =============================================================================
# Test Update Outside Repository
# =============================================================================

class TestUpdateOutsideRepo:
    """Tests for update command outside a repository."""
    
    def test_update_outside_git_repo(self, tmp_path, monkeypatch):
        """Update outside git repo shows error."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'update'],
            capture_output=True,
            text=True,
        )
        
        # Should either fail or show no imports found
        # (depending on whether git ls-files fails)
        assert result.returncode == 0 or 'error' in result.stderr.lower()


# =============================================================================
# Test Update With Import Files
# =============================================================================

class TestUpdateWithImports:
    """Tests for update command with import files."""
    
    @pytest.fixture
    def repo_with_import(self, tmp_path, monkeypatch):
        """Create a git/DVC repo with an import file."""
        git_env = {
            **os.environ,
            'GIT_COMMITTER_NAME': 'Test',
            'GIT_COMMITTER_EMAIL': 'test@test.com',
            'GIT_AUTHOR_NAME': 'Test',
            'GIT_AUTHOR_EMAIL': 'test@test.com',
        }
        
        monkeypatch.chdir(tmp_path)
        
        # Initialize git
        subprocess.run(['git', 'init'], check=True, capture_output=True)
        subprocess.run(
            ['git', 'config', 'user.email', 'test@example.com'],
            check=True, capture_output=True
        )
        subprocess.run(
            ['git', 'config', 'user.name', 'Test User'],
            check=True, capture_output=True
        )
        # Disable GPG signing for tests
        subprocess.run(
            ['git', 'config', 'commit.gpgsign', 'false'],
            check=True, capture_output=True
        )
        
        # Initialize DVC
        subprocess.run(['dvc', 'init'], check=True, capture_output=True)
        
        # Create an import .dvc file (fake, won't actually work with dvc update)
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'md5': 'abc123',
            'frozen': True,
            'deps': [{
                'path': 'data/file.csv',
                'repo': {
                    'url': 'https://github.com/nonexistent/repo.git',
                    'rev_lock': 'abc123',
                }
            }],
            'outs': [{
                'path': 'file.csv',
                'md5': 'def456',
            }]
        }))
        
        # Create initial commit
        subprocess.run(['git', 'add', '.'], check=True, capture_output=True)
        subprocess.run(
            ['git', 'commit', '-m', 'Initial commit'],
            check=True, capture_output=True, env=git_env
        )
        
        return tmp_path
    
    def test_update_finds_import_files(self, repo_with_import):
        """Update with no targets finds import files."""
        result = subprocess.run(
            ['dt', 'update', '--no-download', '-v'],
            capture_output=True,
            text=True,
            cwd=repo_with_import,
        )
        
        # Should find the import file (may fail on actual update due to fake repo)
        assert 'imported.dvc' in result.stdout or 'imported.dvc' in result.stderr
    
    def test_update_specific_target(self, repo_with_import):
        """Update specific target."""
        result = subprocess.run(
            ['dt', 'update', '--no-download', 'imported.dvc'],
            capture_output=True,
            text=True,
            cwd=repo_with_import,
        )
        
        # Will fail because repo doesn't exist, but should try
        assert 'imported.dvc' in result.stdout


# =============================================================================
# Test Update Non-Import Files
# =============================================================================

class TestUpdateNonImport:
    """Tests for update command with non-import files."""
    
    @pytest.fixture
    def repo_with_regular_dvc(self, tmp_path, monkeypatch):
        """Create a git/DVC repo with a regular .dvc file (not import)."""
        git_env = {
            **os.environ,
            'GIT_COMMITTER_NAME': 'Test',
            'GIT_COMMITTER_EMAIL': 'test@test.com',
            'GIT_AUTHOR_NAME': 'Test',
            'GIT_AUTHOR_EMAIL': 'test@test.com',
        }
        
        monkeypatch.chdir(tmp_path)
        
        # Initialize git
        subprocess.run(['git', 'init'], check=True, capture_output=True)
        subprocess.run(
            ['git', 'config', 'user.email', 'test@example.com'],
            check=True, capture_output=True
        )
        subprocess.run(
            ['git', 'config', 'user.name', 'Test User'],
            check=True, capture_output=True
        )
        # Disable GPG signing for tests
        subprocess.run(
            ['git', 'config', 'commit.gpgsign', 'false'],
            check=True, capture_output=True
        )
        
        # Initialize DVC
        subprocess.run(['dvc', 'init'], check=True, capture_output=True)
        
        # Create a regular .dvc file (not an import)
        regular_file = tmp_path / "data.csv.dvc"
        regular_file.write_text(yaml.dump({
            'md5': 'abc123',
            'outs': [{
                'path': 'data.csv',
                'md5': 'def456',
            }]
        }))
        
        # Create initial commit
        subprocess.run(['git', 'add', '.'], check=True, capture_output=True)
        subprocess.run(
            ['git', 'commit', '-m', 'Initial commit'],
            check=True, capture_output=True, env=git_env
        )
        
        return tmp_path
    
    def test_update_non_import_shows_error(self, repo_with_regular_dvc):
        """Update on non-import file shows appropriate error."""
        result = subprocess.run(
            ['dt', 'update', 'data.csv.dvc'],
            capture_output=True,
            text=True,
            cwd=repo_with_regular_dvc,
        )
        
        # Should indicate it's not an import file
        assert 'not an import' in result.stdout.lower() or result.returncode != 0
    
    def test_update_no_imports_in_repo(self, repo_with_regular_dvc):
        """Update with no imports shows appropriate message."""
        result = subprocess.run(
            ['dt', 'update', '-v'],
            capture_output=True,
            text=True,
            cwd=repo_with_regular_dvc,
        )
        
        # Should indicate no imports found
        assert 'no import' in result.stdout.lower() or result.returncode == 0


# =============================================================================
# Test Update With Real Repository (Network)
# =============================================================================

@pytest.mark.requires_network
class TestUpdateNetwork:
    """Tests for update command with real repositories."""
    
    def test_update_from_test_fixtures(self, tmp_path, monkeypatch):
        """Update import from dt-test-registry."""
        git_env = {
            **os.environ,
            'GIT_COMMITTER_NAME': 'Test',
            'GIT_COMMITTER_EMAIL': 'test@test.com',
            'GIT_AUTHOR_NAME': 'Test',
            'GIT_AUTHOR_EMAIL': 'test@test.com',
        }
        
        monkeypatch.chdir(tmp_path)
        
        # Initialize git
        subprocess.run(['git', 'init'], check=True, capture_output=True)
        subprocess.run(
            ['git', 'config', 'user.email', 'test@example.com'],
            check=True, capture_output=True
        )
        subprocess.run(
            ['git', 'config', 'user.name', 'Test User'],
            check=True, capture_output=True
        )
        
        # Initialize DVC
        subprocess.run(['dvc', 'init'], check=True, capture_output=True)
        
        # Import from test registry
        result = subprocess.run(
            ['dvc', 'import', 
             'https://github.com/swarbricklab/dt-test-registry',
             'data/file.csv',
             '--no-download'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            pytest.skip("Could not import from test registry")
        
        # Commit import
        subprocess.run(['git', 'add', '.'], check=True, capture_output=True)
        subprocess.run(
            ['git', 'commit', '-m', 'Add import'],
            check=True, capture_output=True, env=git_env
        )
        
        # Now test dt update
        result = subprocess.run(
            ['dt', 'update', '--no-download', '-v'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'Updated' in result.stdout
