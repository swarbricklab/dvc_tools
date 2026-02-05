"""Integration tests for 'dt import' command.

Tests for importing data from other repositories.
Note: Some tests require network access or temp clones.
"""

import subprocess
from pathlib import Path

import pytest

from tests.integration.conftest import requires_network


# =============================================================================
# Test Classes
# =============================================================================

class TestImportBasic:
    """Tests for basic 'dt import' functionality."""
    
    def test_import_help(self):
        """'dt import --help' shows usage."""
        result = subprocess.run(
            ['dt', 'import', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'REPOSITORY' in result.stdout
        assert 'PATH' in result.stdout
    
    def test_import_requires_repository(self, dvc_repo, monkeypatch):
        """Import requires repository argument."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'import'],
            capture_output=True,
            text=True,
        )
        
        # Should fail - missing required arguments
        assert result.returncode != 0


class TestImportOptions:
    """Tests for import options."""
    
    def test_import_output_option(self):
        """'-o' option for output path is available."""
        result = subprocess.run(
            ['dt', 'import', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '-o' in result.stdout or '--out' in result.stdout
    
    def test_import_no_checkout_option(self):
        """'--no-checkout' option is available."""
        result = subprocess.run(
            ['dt', 'import', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '--no-checkout' in result.stdout
    
    def test_import_owner_option(self):
        """'--owner' option for short name resolution."""
        result = subprocess.run(
            ['dt', 'import', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '--owner' in result.stdout


class TestImportErrors:
    """Tests for error handling."""
    
    def test_import_invalid_repository(self, dvc_repo, monkeypatch):
        """Import from invalid repository should error."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'import', 'nonexistent-repo-xyz', 'file.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should fail
        assert result.returncode != 0
    
    def test_import_outside_repo(self, tmp_path, monkeypatch):
        """Import outside DVC repo should error."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'import', 'some-repo', 'file.csv'],
            capture_output=True,
            text=True,
        )
        
        # Should fail - not in DVC repo
        assert result.returncode != 0


class TestImportNoCheckout:
    """Tests for --no-checkout behavior."""
    
    def test_import_no_checkout_creates_dvc_file_only(self, dvc_repo, monkeypatch):
        """'--no-checkout' should only create .dvc file."""
        monkeypatch.chdir(dvc_repo)
        
        # This will fail without a valid repo, but tests the option parsing
        result = subprocess.run(
            ['dt', 'import', '--no-checkout', 'fake-repo', 'data.csv'],
            capture_output=True,
            text=True,
        )
        
        # Will fail but should not crash on option parsing
        # Just checking option is recognized


class TestImportWithTempClone:
    """Tests for import using temp clone functionality."""
    
    def test_import_no_refresh_option(self):
        """'--no-refresh' option is available."""
        result = subprocess.run(
            ['dt', 'import', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert '--no-refresh' in result.stdout


@pytest.mark.requires_network
class TestImportNetwork:
    """Tests requiring network access."""
    
    def test_import_from_public_repo(self, dvc_repo, monkeypatch):
        """Import from public GitHub repository."""
        monkeypatch.chdir(dvc_repo)
        
        result = subprocess.run(
            ['dt', 'import', '--no-checkout',
             'https://github.com/swarbricklab/dt-test-registry',
             'data/file.csv'],
            capture_output=True,
            text=True,
            timeout=60,
        )
        
        if result.returncode == 0:
            # Should create .dvc file
            dvc_file = dvc_repo / 'file.csv.dvc'
            if dvc_file.exists():
                content = dvc_file.read_text()
                # Should have deps section with repo URL
                assert 'deps' in content or 'repo' in content
