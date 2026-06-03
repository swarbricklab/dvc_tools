"""Unit tests for dt.remote module.

Tests remote storage management functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dt.remote import (
    resolve_remote_path,
    init_remote_structure,
    configure_dvc_remote,
    configure_local_override,
    init_remote,
    list_remotes,
    classify_location,
    gather_remote_status,
    format_remote_status,
)
from dt.errors import RemoteError


# =============================================================================
# resolve_remote_path tests
# =============================================================================

class TestResolveRemotePath:
    """Tests for the resolve_remote_path function."""

    def test_uses_remote_path_when_provided(self):
        """Test uses remote_path directly when provided."""
        result = resolve_remote_path(remote_path="/explicit/path")
        
        assert result == Path("/explicit/path")

    def test_constructs_path_from_root_and_name(self):
        """Test constructs path from remote_root and name."""
        with patch("dt.remote.cfg.get_value", return_value=None):
            result = resolve_remote_path(
                name="myproject",
                remote_root="/remote/storage",
            )
            
            assert result == Path("/remote/storage/myproject")

    def test_uses_config_remote_root(self):
        """Test uses remote.root from config."""
        with patch("dt.remote.cfg.get_value", return_value="/config/remote"):
            with patch("dt.remote.utils.get_project_name", return_value="project"):
                result = resolve_remote_path()
                
                assert result == Path("/config/remote/project")

    def test_raises_error_when_no_root_configured(self):
        """Test RemoteError raised when remote root not configured."""
        with patch("dt.remote.cfg.get_value", return_value=None):
            with pytest.raises(RemoteError, match="Remote root not configured"):
                resolve_remote_path(name="project")


# =============================================================================
# init_remote_structure tests
# =============================================================================

class TestInitRemoteStructure:
    """Tests for the init_remote_structure function."""

    def test_creates_remote_directory(self, tmp_path):
        """Test creates remote directory if not exists."""
        remote_dir = tmp_path / "remote"
        
        with patch("dt.remote.utils.set_group_writable"):
            with patch("dt.remote.utils.create_md5_subdirs"):
                init_remote_structure(remote_dir, verbose=False)
                
                assert remote_dir.exists()

    def test_calls_set_group_writable(self, tmp_path):
        """Test calls set_group_writable on remote dir."""
        remote_dir = tmp_path / "remote"
        
        with patch("dt.remote.utils.set_group_writable") as mock_sgw:
            with patch("dt.remote.utils.create_md5_subdirs"):
                init_remote_structure(remote_dir, verbose=False)
                
                mock_sgw.assert_called_once_with(remote_dir)

    def test_creates_md5_subdirs(self, tmp_path):
        """Test creates files/md5 subdirectory structure."""
        remote_dir = tmp_path / "remote"
        
        with patch("dt.remote.utils.set_group_writable"):
            with patch("dt.remote.utils.create_md5_subdirs") as mock_create:
                init_remote_structure(remote_dir, verbose=False)
                
                mock_create.assert_called_once()


# =============================================================================
# configure_dvc_remote tests
# =============================================================================

class TestConfigureDvcRemote:
    """Tests for the configure_dvc_remote function."""

    def test_adds_ssh_remote_when_host_configured(self, tmp_path):
        """Test adds SSH remote when ssh.host is configured."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = Path("/remote/storage/project")
        
        with patch("dt.remote.cfg.get_value", return_value="hostname.example.com"):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stderr="")
                
                configure_dvc_remote(repo_path, remote_dir, verbose=False)
                
                # Should have called dvc remote add twice (SSH and local)
                assert mock_run.call_count == 2
                
                # First call should be SSH remote
                first_call = mock_run.call_args_list[0][0][0]
                assert "ssh://hostname.example.com" in " ".join(first_call)

    def test_adds_local_remote(self, tmp_path):
        """Test always adds local remote."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = Path("/remote/storage/project")
        
        with patch("dt.remote.cfg.get_value", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stderr="")
                
                configure_dvc_remote(repo_path, remote_dir, verbose=False)
                
                call_args = mock_run.call_args[0][0]
                assert "--local" in call_args
                assert "local" in call_args

    def test_ignores_already_exists_error(self, tmp_path):
        """Test ignores 'already exists' error."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = Path("/remote/storage/project")
        
        with patch("dt.remote.cfg.get_value", return_value=None):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=1,
                    stderr="remote 'local' already exists",
                )
                
                # Should not raise
                configure_dvc_remote(repo_path, remote_dir, verbose=False)


# =============================================================================
# init_remote tests
# =============================================================================

class TestInitRemote:
    """Tests for the init_remote function."""

    def test_checks_dvc_dependency(self, tmp_path):
        """Test checks DVC dependency first."""
        from dt import utils
        
        with patch("dt.remote.utils.check_dvc") as mock_check:
            mock_check.side_effect = utils.DependencyError("dvc not found")
            
            with pytest.raises(RemoteError, match="dvc not found"):
                init_remote(repo_path=tmp_path)

    def test_creates_remote_if_not_exists(self, tmp_path):
        """Test creates remote directory if not exists."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = tmp_path / "remote"
        
        with patch("dt.remote.utils.check_dvc"):
            with patch("dt.remote.resolve_remote_path", return_value=remote_dir):
                with patch("dt.remote.init_remote_structure") as mock_init:
                    with patch("dt.remote.configure_dvc_remote"):
                        init_remote(repo_path=repo_path, verbose=False)
                        
                        mock_init.assert_called_once()

    def test_skips_creation_if_exists(self, tmp_path):
        """Test skips creation if remote directory exists."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        
        with patch("dt.remote.utils.check_dvc"):
            with patch("dt.remote.resolve_remote_path", return_value=remote_dir):
                with patch("dt.remote.init_remote_structure") as mock_init:
                    with patch("dt.remote.configure_dvc_remote"):
                        init_remote(repo_path=repo_path, verbose=False)
                        
                        mock_init.assert_not_called()

    def test_returns_remote_path(self, tmp_path):
        """Test returns the remote directory path."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        
        with patch("dt.remote.utils.check_dvc"):
            with patch("dt.remote.resolve_remote_path", return_value=remote_dir):
                with patch("dt.remote.configure_dvc_remote"):
                    result = init_remote(repo_path=repo_path, verbose=False)
                    
                    assert result == remote_dir


# =============================================================================
# list_remotes tests
# =============================================================================

class TestListRemotes:
    """Tests for the list_remotes function."""

    def test_returns_empty_list_when_no_remotes(self, tmp_path):
        """Test returns empty list when no remotes configured."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="",
            )
            
            result = list_remotes(tmp_path)
            
            assert result == []

    def test_parses_remote_list_output(self, tmp_path):
        """Test parses dvc remote list output."""
        with patch("subprocess.run") as mock_run:
            # First call: dvc remote list
            # Second call: dvc config core.remote
            mock_run.side_effect = [
                MagicMock(
                    returncode=0,
                    stdout="origin\tssh://host/path\nlocal\t/local/path\n",
                ),
                MagicMock(
                    returncode=0,
                    stdout="origin\n",
                ),
            ]
            
            result = list_remotes(tmp_path)
            
            assert len(result) == 2
            # Check first remote
            assert result[0][0] == "origin"
            assert "ssh://host/path" in result[0][1]

    def test_identifies_default_remote(self, tmp_path):
        """Test correctly identifies default remote."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(
                    returncode=0,
                    stdout="origin\tssh://host/path\nlocal\t/local/path\n",
                ),
                MagicMock(
                    returncode=0,
                    stdout="origin\n",
                ),
            ]
            
            result = list_remotes(tmp_path)
            
            # origin should be default
            origin = [r for r in result if r[0] == "origin"][0]
            assert origin[2] is True  # is_default
            
            # local should not be default
            local = [r for r in result if r[0] == "local"][0]
            assert local[2] is False

    def test_uses_cwd_when_no_path_provided(self):
        """Test uses current directory when no path provided."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="")
            
            with patch("pathlib.Path.cwd", return_value=Path("/current/dir")):
                list_remotes()
                
                call_kwargs = mock_run.call_args[1]
                assert call_kwargs.get("cwd") == Path("/current/dir")


# =============================================================================
# configure_local_override tests
# =============================================================================

class TestConfigureLocalOverride:
    """Tests for the configure_local_override function."""

    def test_returns_none_when_no_remotes(self, tmp_path):
        """Returns None when .dvc/config has no remotes."""
        with patch("dt.remote.list_remotes", return_value=[]):
            result = configure_local_override(repo_path=tmp_path, verbose=False)

        assert result is None

    def test_returns_local_path_and_runs_dvc(self, tmp_path):
        """Adds a local remote when an existing remote resolves locally."""
        remotes = [("myremote", "ssh://host/g/data/a56/dvc/remotes/proj", True)]

        with patch("dt.remote.list_remotes", return_value=remotes):
            with patch(
                "dt.remote.find_local_remote",
                return_value=("myremote", "/g/data/a56/dvc/remotes/proj"),
            ):
                with patch("subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(returncode=0, stderr="")
                    result = configure_local_override(
                        repo_path=tmp_path, verbose=False,
                    )

        assert result == "/g/data/a56/dvc/remotes/proj"
        # Should have run `dvc remote add --local -d local <path>`
        cmd = mock_run.call_args[0][0]
        assert cmd == [
            "dvc", "remote", "add", "--local", "-d", "local",
            "/g/data/a56/dvc/remotes/proj",
        ]
        assert mock_run.call_args[1]["cwd"] == tmp_path

    def test_returns_none_when_no_local_remote(self, tmp_path):
        """Returns None when no remote resolves to a local path."""
        remotes = [("s3remote", "s3://bucket/prefix", True)]

        with patch("dt.remote.list_remotes", return_value=remotes):
            with patch("dt.remote.find_local_remote", return_value=None):
                result = configure_local_override(
                    repo_path=tmp_path, verbose=False,
                )

        assert result is None

    def test_falls_back_to_check_exists_false(self, tmp_path):
        """Uses check_exists=False when True returns None."""
        remotes = [("myremote", "ssh://host/unmounted/path", True)]

        with patch("dt.remote.list_remotes", return_value=remotes):
            with patch("dt.remote.find_local_remote") as mock_find:
                # First call (check_exists=True) returns None,
                # second call (check_exists=False) succeeds.
                mock_find.side_effect = [
                    None,
                    ("myremote", "/unmounted/path"),
                ]
                with patch("subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(returncode=0, stderr="")
                    result = configure_local_override(
                        repo_path=tmp_path, verbose=False,
                    )

        assert result == "/unmounted/path"
        assert mock_find.call_count == 2
        assert mock_find.call_args_list[0][1]["check_exists"] is True
        assert mock_find.call_args_list[1][1]["check_exists"] is False

    def test_raises_on_dvc_error(self, tmp_path):
        """Raises RemoteError when dvc remote add fails."""
        remotes = [("myremote", "/local/path", True)]

        with patch("dt.remote.list_remotes", return_value=remotes):
            with patch(
                "dt.remote.find_local_remote",
                return_value=("myremote", "/local/path"),
            ):
                with patch("subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(
                        returncode=1, stderr="some error",
                    )
                    with pytest.raises(RemoteError, match="Failed to add local remote"):
                        configure_local_override(
                            repo_path=tmp_path, verbose=False,
                        )

    def test_ignores_already_exists_error(self, tmp_path):
        """Does not raise when local remote already exists."""
        remotes = [("myremote", "/local/path", True)]

        with patch("dt.remote.list_remotes", return_value=remotes):
            with patch(
                "dt.remote.find_local_remote",
                return_value=("myremote", "/local/path"),
            ):
                with patch("subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(
                        returncode=1,
                        stderr="remote 'local' already exists",
                    )
                    result = configure_local_override(
                        repo_path=tmp_path, verbose=False,
                    )

        assert result == "/local/path"


# =============================================================================
# classify_location tests
# =============================================================================

class TestClassifyLocation:
    """Tests for the classify_location function."""

    def test_bare_local_path(self):
        loc = classify_location("/data/remotes/proj")
        assert loc["kind"] == "local-path"
        assert loc["host"] is None
        assert loc["path"] == "/data/remotes/proj"
        assert loc["is_local"] is True

    def test_cloud_remote(self):
        loc = classify_location("s3://bucket/key")
        assert loc["kind"] == "cloud"
        assert loc["scheme"] == "s3"
        assert loc["path"] is None
        assert loc["is_local"] is False

    def test_ssh_remote_non_local(self):
        with patch("dt.remote.is_local_host", return_value=False):
            loc = classify_location("ssh://elsewhere.example.org/data/proj")
        assert loc["kind"] == "ssh"
        assert loc["host"] == "elsewhere.example.org"
        assert loc["path"] == "/data/proj"
        assert loc["is_local"] is False

    def test_ssh_remote_local_host(self):
        with patch("dt.remote.is_local_host", return_value=True):
            loc = classify_location("ssh://here.example.org/data/proj")
        assert loc["is_local"] is True


# =============================================================================
# gather_remote_status tests
# =============================================================================

class TestGatherRemoteStatus:
    """Tests for the gather_remote_status function."""

    def test_accessible_v3_remote_not_archived(self, tmp_path):
        """A reachable v3 remote reports layout and group-writability."""
        remote_dir = tmp_path / "proj"
        (remote_dir / "files" / "md5" / "00").mkdir(parents=True)

        info = gather_remote_status((str(remote_dir).split("/")[-1], str(remote_dir), True))

        assert info["is_default"] is True
        assert info["kind"] == "local-path"
        assert info["is_local"] is True
        assert info["access"]["accessible"] is True
        assert info["layout"] == "v3"
        assert info["archived"] is None
        # No deep scan requested.
        assert info["objects"] is None

    def test_uninitialized_remote(self, tmp_path):
        """A configured-but-empty remote reports 'uninitialized'."""
        remote_dir = tmp_path / "proj"
        remote_dir.mkdir()

        info = gather_remote_status(("proj", str(remote_dir), False))

        assert info["layout"] == "uninitialized"
        assert info["archived"] is None

    def test_missing_path_not_accessible(self, tmp_path):
        """A remote whose path does not exist is reported inaccessible."""
        missing = tmp_path / "nope"

        info = gather_remote_status(("proj", str(missing), False))

        assert info["access"]["accessible"] is False
        assert "path not found" in info["access"]["detail"]
        assert info["layout"] is None

    def test_ssh_remote_not_locally_checkable(self):
        """A non-local SSH remote is not probed on the filesystem."""
        with patch("dt.remote.is_local_host", return_value=False):
            info = gather_remote_status(
                ("rem", "ssh://elsewhere.example.org/data/proj", False))

        assert info["kind"] == "ssh"
        assert info["is_local"] is False
        assert info["access"]["checkable"] is False
        assert info["layout"] is None

    def test_deep_scan_counts_objects(self, tmp_path):
        """--deep walks the blob tree and reports object/byte counts."""
        remote_dir = tmp_path / "proj"
        prefix = remote_dir / "files" / "md5" / "00"
        prefix.mkdir(parents=True)
        (prefix / "abc123").write_bytes(b"hello")  # 5 bytes

        info = gather_remote_status(("proj", str(remote_dir), True), deep=True)

        assert info["layout"] == "v3"
        assert info["objects"] == 1
        assert info["size_bytes"] == 5

    def test_archived_remote_reads_signpost(self, tmp_path):
        """A pruned remote with a signpost reports archive destination."""
        from dt.archive.signpost import ArchiveSignpost

        remote_dir = tmp_path / "proj"
        remote_dir.mkdir()
        fake_signpost = ArchiveSignpost(
            archive_name="proj-2026",
            backend="mdss",
            backend_dir="tape/proj",
            source_layout="dvc-v3",
            source_remote=str(remote_dir),
            git_url="https://example.org/proj",
            git_ref="abc",
            manifest_in_repo=".dvc/archives/proj-2026.manifest.yaml",
            pruned_at="2026-05-30T10:00:00+00:00",
            pruned_by="someone",
            path=remote_dir / "ARCHIVED.yaml",
        )

        with patch("dt.archive.signpost.detect", return_value=fake_signpost):
            with patch("dt.archive.registry.read_entry", return_value=None):
                info = gather_remote_status(("proj", str(remote_dir), True))

        assert info["archived"]["backend"] == "mdss"
        assert info["archived"]["backend_dir"] == "tape/proj"
        assert info["layout"] == "v3"
        assert info["layout_note"] == "pruned — data in cold storage"


# =============================================================================
# format_remote_status tests
# =============================================================================

class TestFormatRemoteStatus:
    """Tests for the format_remote_status function."""

    def test_empty(self):
        assert format_remote_status([]) == "No remotes configured."

    def test_renders_key_fields(self):
        status = {
            "name": "myremote",
            "url": "/data/proj",
            "is_default": True,
            "kind": "local-path",
            "host": None,
            "scheme": None,
            "path": "/data/proj",
            "is_local": True,
            "access": {"accessible": True, "detail": "accessible, group-writable (group: ab12)"},
            "layout": "v3",
            "archived": None,
            "objects": None,
            "size_bytes": None,
        }
        out = format_remote_status([status])
        assert "Remote: myremote  (default)" in out
        assert "Location:   local (this filesystem)" in out
        assert "Layout:     v3" in out
        assert "Archived:   no" in out

    def test_renders_archived(self):
        status = {
            "name": "myremote",
            "url": "/data/proj",
            "is_default": False,
            "kind": "local-path",
            "host": None,
            "scheme": None,
            "path": "/data/proj",
            "is_local": True,
            "access": {"accessible": True, "detail": "accessible"},
            "layout": "v3",
            "layout_note": "pruned — data in cold storage",
            "archived": {
                "backend": "mdss",
                "backend_dir": "tape/proj",
                "pruned_at": "2026-05-30T10:00:00+00:00",
                "verified_at": "2026-05-31T00:00:00+00:00",
                "verified_ok": True,
                "total_objects": 1234,
                "total_size_bytes": 5000,
            },
            "objects": None,
            "size_bytes": None,
        }
        out = format_remote_status([status])
        assert "Archived:   yes -> mdss:tape/proj" in out
        assert "pruned 2026-05-30" in out
        assert "verified 2026-05-31" in out
        assert "1,234" in out
