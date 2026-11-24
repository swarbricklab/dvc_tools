"""Unit tests for dt.summary module.

Tests project summary generation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.summary import (
    get_output_dir,
    generate_tree,
    generate_dag,
    generate_all,
)
from dt.errors import SummaryError
from dt import summary


# =============================================================================
# get_output_dir tests
# =============================================================================

class TestGetOutputDir:
    """Tests for the get_output_dir function."""

    def test_uses_explicit_output_dir(self):
        """Test uses explicitly provided output_dir."""
        result = get_output_dir(output_dir="/custom/output")
        
        assert result == Path("/custom/output")
        assert get_output_dir(output_dir="rel") == Path.cwd() / "rel"

    def test_uses_config_value(self):
        """Test uses summary.output_dir from config."""
        with patch("dt.summary.cfg.get_trusted_value_with_scope",
                   return_value=("/config/docs", "user")):
            result = get_output_dir()
            
            assert result == Path("/config/docs")

    def test_defaults_to_docs(self):
        """Test defaults to docs/ directory."""
        with patch("dt.summary.cfg.get_trusted_value_with_scope",
                   return_value=(None, None)):
            result = get_output_dir()
            
            assert result == Path("docs")


# =============================================================================
# generate_tree tests
# =============================================================================

class TestGenerateTree:
    """Tests for the generate_tree function."""

    def test_checks_dvc_dependency(self, tmp_path):
        """Test checks DVC dependency first."""
        from dt import utils
        
        with patch("dt.summary.utils.check_dvc") as mock_check:
            mock_check.side_effect = utils.DependencyError("dvc not found")
            
            with pytest.raises(SummaryError, match="dvc not found"):
                generate_tree(output_dir=str(tmp_path))

    def test_creates_output_directory(self, tmp_path):
        """Test creates output directory if not exists."""
        output_dir = tmp_path / "docs"
        
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="├── data.csv\n└── images/\n",
                    stderr="",
                )
                
                generate_tree(output_dir=str(output_dir), verbose=False)
                
                assert output_dir.exists()

    def test_runs_dvc_list_tree(self, tmp_path):
        """Test runs dvc list --tree command."""
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="tree output",
                    stderr="",
                )
                
                generate_tree(output_dir=str(tmp_path), verbose=False)
                
                call_args = mock_run.call_args[0][0]
                assert "dvc" in call_args
                assert "list" in call_args
                assert "--tree" in call_args
                assert "--dvc-only" in call_args

    def test_writes_output_to_file(self, tmp_path):
        """Test writes output to tree.txt file."""
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="├── data.csv\n└── images/\n",
                    stderr="",
                )
                
                result = generate_tree(output_dir=str(tmp_path), verbose=False)
                
                assert result == tmp_path / "tree.txt"
                assert result.exists()
                assert "data.csv" in result.read_text()

    def test_uses_custom_filename(self, tmp_path):
        """Test uses custom filename when provided."""
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                
                result = generate_tree(
                    output_dir=str(tmp_path),
                    filename="custom_tree.txt",
                    verbose=False,
                )
                
                assert result == tmp_path / "custom_tree.txt"

    def test_raises_error_on_failure(self, tmp_path):
        """Test SummaryError raised when dvc list fails."""
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=1,
                    stdout="",
                    stderr="failed to list",
                )
                
                with pytest.raises(SummaryError, match="Failed to generate tree"):
                    generate_tree(output_dir=str(tmp_path), verbose=False)


# =============================================================================
# generate_dag tests
# =============================================================================

class TestGenerateDag:
    """Tests for the generate_dag function."""

    def test_checks_dvc_dependency(self, tmp_path):
        """Test checks DVC dependency first."""
        from dt import utils
        
        with patch("dt.summary.utils.check_dvc") as mock_check:
            mock_check.side_effect = utils.DependencyError("dvc not found")
            
            with pytest.raises(SummaryError, match="dvc not found"):
                generate_dag(output_dir=str(tmp_path))

    def test_runs_dvc_dag_md(self, tmp_path):
        """Test runs dvc dag --md command."""
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="```mermaid\nflowchart\n```",
                    stderr="",
                )
                
                generate_dag(output_dir=str(tmp_path), verbose=False)
                
                call_args = mock_run.call_args[0][0]
                assert "dvc" in call_args
                assert "dag" in call_args
                assert "--md" in call_args

    def test_writes_output_to_file(self, tmp_path):
        """Test writes output to dag.md file."""
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="```mermaid\nflowchart\n```",
                    stderr="",
                )
                
                result = generate_dag(output_dir=str(tmp_path), verbose=False)
                
                assert result == tmp_path / "dag.md"
                assert result.exists()
                assert "mermaid" in result.read_text()

    def test_uses_custom_filename(self, tmp_path):
        """Test uses custom filename when provided."""
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                
                result = generate_dag(
                    output_dir=str(tmp_path),
                    filename="pipeline.md",
                    verbose=False,
                )
                
                assert result == tmp_path / "pipeline.md"

    def test_raises_error_on_failure(self, tmp_path):
        """Test SummaryError raised when dvc dag fails."""
        with patch("dt.summary.utils.check_dvc"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=1,
                    stdout="",
                    stderr="no pipeline",
                )
                
                with pytest.raises(SummaryError, match="Failed to generate DAG"):
                    generate_dag(output_dir=str(tmp_path), verbose=False)


# =============================================================================
# generate_all tests
# =============================================================================

class TestGenerateAll:
    """Tests for the generate_all function."""

    def test_generates_tree_and_dag(self, tmp_path):
        """Test generates both tree and dag files."""
        with patch("dt.summary.generate_tree") as mock_tree:
            with patch("dt.summary.generate_dag") as mock_dag:
                mock_tree.return_value = tmp_path / "tree.txt"
                mock_dag.return_value = tmp_path / "dag.md"
                
                result = generate_all(output_dir=str(tmp_path), verbose=False)
                
                mock_tree.assert_called_once()
                mock_dag.assert_called_once()
                assert result["tree"] == tmp_path / "tree.txt"
                assert result["dag"] == tmp_path / "dag.md"

    def test_passes_output_dir_to_both(self, tmp_path):
        """Test passes output_dir to both functions."""
        with patch("dt.summary.generate_tree") as mock_tree:
            with patch("dt.summary.generate_dag") as mock_dag:
                mock_tree.return_value = tmp_path / "tree.txt"
                mock_dag.return_value = tmp_path / "dag.md"
                
                generate_all(output_dir=str(tmp_path), verbose=False)
                
                tree_kwargs = mock_tree.call_args[1]
                dag_kwargs = mock_dag.call_args[1]
                assert tree_kwargs["output_dir"] == str(tmp_path)
                assert dag_kwargs["output_dir"] == str(tmp_path)

    def test_passes_verbose_to_both(self, tmp_path):
        """Test passes verbose flag to both functions."""
        with patch("dt.summary.generate_tree") as mock_tree:
            with patch("dt.summary.generate_dag") as mock_dag:
                mock_tree.return_value = tmp_path / "tree.txt"
                mock_dag.return_value = tmp_path / "dag.md"
                
                generate_all(verbose=True)
                
                tree_kwargs = mock_tree.call_args[1]
                dag_kwargs = mock_dag.call_args[1]
                assert tree_kwargs["verbose"] is True
                assert dag_kwargs["verbose"] is True



class TestReportsStayInTheProject:
    @pytest.mark.parametrize('value', ['/etc', '../outside', 'docs/../../x'])
    def test_repository_output_dir_must_stay_inside(self, value):
        from dt.errors import SummaryError
        with patch("dt.summary.cfg.get_trusted_value_with_scope",
                   return_value=(value, "project")):
            with pytest.raises(SummaryError):
                summary.get_output_dir()

    def test_linked_report_file_is_not_written_through(self, tmp_path, monkeypatch):
        import os
        monkeypatch.chdir(tmp_path)
        victim = tmp_path / 'victim.txt'
        victim.write_text('keep')
        (tmp_path / 'docs').mkdir()
        os.symlink(victim, tmp_path / 'docs' / 'tree.txt')
        summary._write_report(summary.Path('docs'), 'tree.txt', 'new')
        assert victim.read_text() == 'keep'
        assert (tmp_path / 'docs' / 'tree.txt').read_text() == 'new'

    def test_linked_output_dir_is_refused(self, tmp_path, monkeypatch):
        import os
        from dt.errors import SummaryError
        monkeypatch.chdir(tmp_path)
        elsewhere = tmp_path / 'elsewhere'
        elsewhere.mkdir()
        os.symlink(elsewhere, tmp_path / 'docs')
        with pytest.raises(SummaryError):
            summary._write_report(summary.Path('docs'), 'tree.txt', 'new')
        assert list(elsewhere.iterdir()) == []


    def test_repository_relative_output_dir_is_accepted(self):
        with patch("dt.summary.cfg.get_trusted_value_with_scope",
                   return_value=("site/docs", "local")):
            assert summary.get_output_dir() == summary.Path("site/docs")

    def test_nested_missing_dirs_are_created(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        summary._write_report(summary.Path('a/b/c'), 'tree.txt', 'x')
        assert (tmp_path / 'a' / 'b' / 'c' / 'tree.txt').read_text() == 'x'
