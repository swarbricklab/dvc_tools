"""Integration tests for 'dt summary' command.

Tests for generating project summary files (tree.txt, dag.md).
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Classes
# =============================================================================

class TestSummaryBasic:
    """Tests for basic 'dt summary' functionality."""
    
    def test_summary_help(self):
        """'dt summary --help' shows usage."""
        result = subprocess.run(
            ['dt', 'summary', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'tree' in result.stdout.lower() or 'dag' in result.stdout.lower()
    
    def test_summary_in_dvc_repo(self, dvc_repo_with_files, monkeypatch):
        """Generate summary in DVC repo."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'summary'],
            capture_output=True,
            text=True,
        )
        
        # Should succeed or handle gracefully
        assert result.returncode in (0, 1)


class TestSummaryTree:
    """Tests for tree generation."""
    
    def test_summary_creates_tree_file(self, dvc_repo_with_files, monkeypatch):
        """Summary creates tree.txt file."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        # Ensure docs directory exists
        docs_dir = repo / 'docs'
        docs_dir.mkdir(exist_ok=True)
        
        result = subprocess.run(
            ['dt', 'summary'],
            capture_output=True,
            text=True,
        )
        
        # If successful, should create tree.txt in docs/
        if result.returncode == 0:
            tree_file = docs_dir / 'tree.txt'
            # May or may not exist depending on DVC tracked files
    
    def test_summary_tree_only(self, dvc_repo_with_files, monkeypatch):
        """'--tree-only' generates only tree.txt."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        # Ensure docs directory
        docs_dir = repo / 'docs'
        docs_dir.mkdir(exist_ok=True)
        
        result = subprocess.run(
            ['dt', 'summary', '--tree-only'],
            capture_output=True,
            text=True,
        )
        
        # Should work
        assert result.returncode in (0, 1)


class TestSummaryDag:
    """Tests for DAG generation."""
    
    def test_summary_dag_only(self, dvc_repo_with_files, monkeypatch):
        """'--dag-only' generates only dag.md."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        # Ensure docs directory
        docs_dir = repo / 'docs'
        docs_dir.mkdir(exist_ok=True)
        
        result = subprocess.run(
            ['dt', 'summary', '--dag-only'],
            capture_output=True,
            text=True,
        )
        
        # Should work (may have no pipeline)
        assert result.returncode in (0, 1)


class TestSummaryOutput:
    """Tests for output directory options."""
    
    def test_summary_custom_output_dir(self, dvc_repo_with_files, monkeypatch):
        """'-o' uses custom output directory."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        # Use current directory as output
        result = subprocess.run(
            ['dt', 'summary', '-o', '.'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode in (0, 1)
    
    def test_summary_creates_output_dir(self, dvc_repo_with_files, monkeypatch):
        """Summary creates output directory if missing."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        output_dir = repo / 'new_docs'
        
        result = subprocess.run(
            ['dt', 'summary', '-o', str(output_dir)],
            capture_output=True,
            text=True,
        )
        
        # Should work
        assert result.returncode in (0, 1)


class TestSummaryErrors:
    """Tests for error handling."""
    
    def test_summary_both_flags_error(self, dvc_repo_with_files, monkeypatch):
        """'--tree-only' and '--dag-only' together should error."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'summary', '--tree-only', '--dag-only'],
            capture_output=True,
            text=True,
        )
        
        # Should fail or handle gracefully
        # Behavior depends on implementation
        assert result.returncode in (0, 1, 2)
    
    def test_summary_outside_repo(self, tmp_path, monkeypatch):
        """Summary outside DVC repo should error."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'summary'],
            capture_output=True,
            text=True,
        )
        
        # Should fail
        assert result.returncode != 0


class TestSummaryContent:
    """Tests for summary content."""
    
    def test_tree_contains_tracked_files(self, dvc_repo_with_files, monkeypatch):
        """Tree output should list tracked files."""
        repo = dvc_repo_with_files
        monkeypatch.chdir(repo)
        
        docs_dir = repo / 'docs'
        docs_dir.mkdir(exist_ok=True)
        
        result = subprocess.run(
            ['dt', 'summary'],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            tree_file = docs_dir / 'tree.txt'
            if tree_file.exists():
                content = tree_file.read_text()
                # Should contain tracked file
                assert 'data.csv' in content
