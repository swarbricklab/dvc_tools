"""Unit tests for dt.mv module.

Tests move/rename functionality for DVC-tracked files.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from dt.mv import mv
from dt.errors import MvError


# =============================================================================
# mv tests
# =============================================================================

class TestMv:
    """Tests for the mv function."""

    def test_raises_error_when_source_dvc_not_found(self, tmp_path):
        """Test MvError raised when source .dvc file doesn't exist."""
        src = tmp_path / "data.csv"
        dst = tmp_path / "newdata.csv"
        
        with pytest.raises(MvError, match="Source .dvc file not found"):
            mv(str(src), str(dst))

    def test_handles_dvc_suffix_in_source(self, tmp_path):
        """Test that .dvc suffix in source is handled."""
        src_dvc = tmp_path / "data.csv.dvc"
        src_dvc.write_text("outs:\n  - md5: abc123\n    path: data.csv\n")
        dst = tmp_path / "newdata.csv"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            with patch("dt.mv.utils.is_repo_import", return_value=False):
                # Create the destination .dvc file
                dst_dvc = tmp_path / "newdata.csv.dvc"
                dst_dvc.write_text("outs:\n  - md5: abc123\n    path: newdata.csv\n")
                
                result = mv(str(src_dvc), str(dst))
                
                assert result[0] == src_dvc

    def test_runs_dvc_mv_for_non_imports(self, tmp_path):
        """Test that dvc mv is run for non-import files."""
        src = tmp_path / "data.csv"
        src_dvc = tmp_path / "data.csv.dvc"
        src_dvc.write_text("outs:\n  - md5: abc123\n    path: data.csv\n")
        dst = tmp_path / "newdata.csv"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            with patch("dt.mv.utils.is_repo_import", return_value=False):
                # Create the destination .dvc file
                dst_dvc = tmp_path / "newdata.csv.dvc"
                dst_dvc.write_text("outs:\n  - md5: abc123\n    path: newdata.csv\n")
                
                mv(str(src), str(dst), verbose=False)
                
                mock_run.assert_called_once()
                call_args = mock_run.call_args[0][0]
                assert call_args == ["dvc", "mv", str(src), str(dst)]

    def test_raises_error_when_dvc_mv_fails(self, tmp_path):
        """Test MvError raised when dvc mv fails."""
        src = tmp_path / "data.csv"
        src_dvc = tmp_path / "data.csv.dvc"
        src_dvc.write_text("outs:\n  - md5: abc123\n    path: data.csv\n")
        dst = tmp_path / "newdata.csv"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="dvc mv failed",
            )
            
            with patch("dt.mv.utils.is_repo_import", return_value=False):
                with pytest.raises(MvError, match="dvc mv failed"):
                    mv(str(src), str(dst))

    def test_preserves_deps_for_imports(self, tmp_path):
        """Test that deps are preserved for imported files."""
        src = tmp_path / "data.csv"
        src_dvc = tmp_path / "data.csv.dvc"
        
        # Create .dvc file with deps (import)
        dvc_content = {
            "outs": [{"md5": "abc123", "path": "data.csv"}],
            "deps": [{"path": "data.csv", "repo": {"url": "https://github.com/org/repo"}}],
        }
        src_dvc.write_text(yaml.dump(dvc_content))
        
        dst = tmp_path / "newdata.csv"
        dst_dvc = tmp_path / "newdata.csv.dvc"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            with patch("dt.mv.utils.is_repo_import", return_value=True):
                with patch("dt.mv.utils.parse_dvc_file", return_value=dvc_content):
                    # Simulate dvc mv creating the new .dvc file (without deps)
                    def create_dst_dvc(*args, **kwargs):
                        dst_dvc.write_text(yaml.dump({
                            "outs": [{"md5": "abc123", "path": "newdata.csv"}],
                        }))
                        return MagicMock(returncode=0, stdout="", stderr="")
                    
                    mock_run.side_effect = create_dst_dvc
                    
                    mv(str(src), str(dst), verbose=False)
                    
                    # Check that deps were restored
                    result = yaml.safe_load(dst_dvc.read_text())
                    assert "deps" in result
                    assert len(result["deps"]) == 1

    def test_returns_old_and_new_dvc_paths(self, tmp_path):
        """Test that function returns (old_dvc, new_dvc) tuple."""
        src = tmp_path / "data.csv"
        src_dvc = tmp_path / "data.csv.dvc"
        src_dvc.write_text("outs:\n  - md5: abc123\n    path: data.csv\n")
        dst = tmp_path / "newdata.csv"
        dst_dvc = tmp_path / "newdata.csv.dvc"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            with patch("dt.mv.utils.is_repo_import", return_value=False):
                dst_dvc.write_text("outs:\n  - md5: abc123\n    path: newdata.csv\n")
                
                old_path, new_path = mv(str(src), str(dst))
                
                assert old_path == src_dvc
                assert new_path == dst_dvc

    def test_handles_directory_destination(self, tmp_path):
        """Test handling when destination is a directory."""
        src = tmp_path / "data.csv"
        src_dvc = tmp_path / "data.csv.dvc"
        src_dvc.write_text("outs:\n  - md5: abc123\n    path: data.csv\n")
        
        dst_dir = tmp_path / "newdir"
        dst_dir.mkdir()
        
        dst_dvc = dst_dir / "data.dvc"  # Note: stem, not full name
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            with patch("dt.mv.utils.is_repo_import", return_value=False):
                # Create the expected destination .dvc file
                expected_dst_dvc = dst_dir / "data.dvc"
                expected_dst_dvc.write_text("outs:\n  - md5: abc123\n    path: data.csv\n")
                
                result = mv(str(src), str(dst_dir))
                
                assert result[0] == src_dvc

    def test_verbose_prints_progress(self, tmp_path, capsys):
        """Test that verbose mode prints progress."""
        src = tmp_path / "data.csv"
        src_dvc = tmp_path / "data.csv.dvc"
        src_dvc.write_text("outs:\n  - md5: abc123\n    path: data.csv\n")
        dst = tmp_path / "newdata.csv"
        dst_dvc = tmp_path / "newdata.csv.dvc"
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            
            with patch("dt.mv.utils.is_repo_import", return_value=False):
                dst_dvc.write_text("outs:\n  - md5: abc123\n    path: newdata.csv\n")
                
                mv(str(src), str(dst), verbose=True)
                
                captured = capsys.readouterr()
                assert "dvc mv" in captured.out
