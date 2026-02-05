"""Integration tests for 'dt find' command.

Tests for finding workspace paths by hash.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def dvc_repo_with_hash(dvc_repo_with_files, monkeypatch):
    """DVC repo with known hash for testing.
    
    Returns the repo path and the hash of the tracked file.
    """
    monkeypatch.chdir(dvc_repo_with_files)
    
    # Read the .dvc file to get the hash
    dvc_file = dvc_repo_with_files / 'data.csv.dvc'
    content = dvc_file.read_text()
    
    # Parse hash from YAML
    import yaml
    data = yaml.safe_load(content)
    md5_hash = data.get('outs', [{}])[0].get('md5', '')
    
    return {
        'repo': dvc_repo_with_files,
        'hash': md5_hash,
        'file': 'data.csv',
    }


# =============================================================================
# Test Classes
# =============================================================================

class TestFindBasic:
    """Tests for basic 'dt find' functionality."""
    
    def test_find_help(self):
        """'dt find --help' shows usage."""
        result = subprocess.run(
            ['dt', 'find', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'HASH' in result.stdout or 'hash' in result.stdout.lower()
    
    def test_find_by_full_hash(self, dvc_repo_with_hash):
        """Find file by full MD5 hash."""
        repo = dvc_repo_with_hash['repo']
        md5_hash = dvc_repo_with_hash['hash']
        expected_file = dvc_repo_with_hash['file']
        
        if not md5_hash:
            pytest.skip("Could not extract hash from fixture")
        
        result = subprocess.run(
            ['dt', 'find', md5_hash],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        assert expected_file in result.stdout
    
    def test_find_by_partial_hash(self, dvc_repo_with_hash):
        """Find file by partial hash (first 8 chars)."""
        repo = dvc_repo_with_hash['repo']
        md5_hash = dvc_repo_with_hash['hash']
        expected_file = dvc_repo_with_hash['file']
        
        if not md5_hash or len(md5_hash) < 8:
            pytest.skip("Could not extract hash from fixture")
        
        partial_hash = md5_hash[:8]
        
        result = subprocess.run(
            ['dt', 'find', partial_hash],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        assert expected_file in result.stdout
    
    def test_find_hash_not_found(self, dvc_repo_with_files, monkeypatch):
        """Find with non-existent hash returns error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        # Use a hash that shouldn't exist
        fake_hash = 'abcd1234abcd1234abcd1234abcd1234'
        
        result = subprocess.run(
            ['dt', 'find', fake_hash],
            capture_output=True,
            text=True,
        )
        
        # Should exit with error or show "not found"
        assert result.returncode == 1 or 'not found' in result.stdout.lower() or 'no match' in result.stdout.lower()


class TestFindShortHash:
    """Tests for hash length validation."""
    
    def test_find_too_short_hash(self, dvc_repo_with_files, monkeypatch):
        """Hash too short should error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'find', 'abc'],  # Only 3 chars
            capture_output=True,
            text=True,
        )
        
        # Should fail with informative error
        assert result.returncode != 0 or 'too short' in result.stderr.lower() or 'at least' in result.stderr.lower()
    
    def test_find_minimum_hash_length(self, dvc_repo_with_hash):
        """Find with minimum 4-char hash."""
        repo = dvc_repo_with_hash['repo']
        md5_hash = dvc_repo_with_hash['hash']
        
        if not md5_hash or len(md5_hash) < 4:
            pytest.skip("Could not extract hash from fixture")
        
        partial_hash = md5_hash[:4]
        
        result = subprocess.run(
            ['dt', 'find', partial_hash],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        # Should work with 4 chars (minimum)
        assert result.returncode == 0


class TestFindOutput:
    """Tests for output formats."""
    
    def test_find_json_output(self, dvc_repo_with_hash):
        """'--json' produces valid JSON."""
        import json
        
        repo = dvc_repo_with_hash['repo']
        md5_hash = dvc_repo_with_hash['hash']
        
        if not md5_hash:
            pytest.skip("Could not extract hash from fixture")
        
        result = subprocess.run(
            ['dt', 'find', '--json', md5_hash],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        # Should be valid JSON
        try:
            data = json.loads(result.stdout)
            assert isinstance(data, (list, dict))
        except json.JSONDecodeError:
            pytest.fail("Output is not valid JSON")
    
    def test_find_verbose(self, dvc_repo_with_hash):
        """'--verbose' shows all details."""
        repo = dvc_repo_with_hash['repo']
        md5_hash = dvc_repo_with_hash['hash']
        
        if not md5_hash:
            pytest.skip("Could not extract hash from fixture")
        
        result = subprocess.run(
            ['dt', 'find', '--verbose', md5_hash],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        # Verbose should show more information
        assert dvc_repo_with_hash['file'] in result.stdout


class TestFindOptions:
    """Tests for additional options."""
    
    def test_find_dvc_file(self, dvc_repo_with_hash):
        """'--dvc-file' option is recognized by dt find."""
        repo = dvc_repo_with_hash['repo']
        md5_hash = dvc_repo_with_hash['hash']
        
        if not md5_hash:
            pytest.skip("Could not extract hash from fixture")
        
        result = subprocess.run(
            ['dt', 'find', '--dvc-file', md5_hash],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0
        # Should find the file (shows data file path)
        assert 'data.csv' in result.stdout or result.stdout.strip() != ''
    
    def test_find_cache_path(self, dvc_repo_with_hash):
        """'--cache-path' shows cache path."""
        repo = dvc_repo_with_hash['repo']
        md5_hash = dvc_repo_with_hash['hash']
        
        if not md5_hash:
            pytest.skip("Could not extract hash from fixture")
        
        result = subprocess.run(
            ['dt', 'find', '--cache-path', md5_hash],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        
        assert result.returncode == 0


class TestFindOutsideRepo:
    """Tests for error handling outside DVC repo."""
    
    def test_find_outside_dvc_repo(self, tmp_path, monkeypatch):
        """Find outside DVC repo should error."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'find', 'abcd1234'],
            capture_output=True,
            text=True,
        )
        
        # Should fail with helpful message
        assert result.returncode != 0
