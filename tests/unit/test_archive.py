"""Unit tests for dt.archive.

Covers the full create / list / verify / restore / prune pipeline using
the ``LocalDirBackend`` so no tape / MDSS access is required.
"""

import hashlib
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from dt.archive import operations as ops
from dt.archive import manifest as manifest_mod
from dt.archive import backends as backends_mod
from dt.archive.backends import LocalDirBackend, MdssBackend
from dt.errors import ArchiveError


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

def _md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


@pytest.fixture
def sample_remote(tmp_path):
    """Build a tiny synthetic DVC remote at tmp_path/remote.

    Lays out a few md5-named blobs across multiple prefixes plus one
    extra file outside files/md5/ so we can exercise the extras path.
    """
    remote = tmp_path / 'remote'
    files_md5 = remote / 'files' / 'md5'

    payloads = [
        b'hello world',
        b'another blob',
        b'a third blob with some content',
        b'\x00\x01\x02 binary-ish payload',
    ]

    for p in payloads:
        h = _md5(p)
        prefix = h[:2]
        rest = h[2:]
        prefix_dir = files_md5 / prefix
        prefix_dir.mkdir(parents=True, exist_ok=True)
        (prefix_dir / rest).write_bytes(p)

    # An "extra" file outside files/md5
    (remote / 'README.txt').write_text('not a DVC object\n')
    (remote / 'config').write_text('endpointurl=https://example\n')

    return remote, payloads


@pytest.fixture
def project_root(tmp_path, monkeypatch):
    """A temp repo root with .dvc/ so find_project_root() resolves here."""
    root = tmp_path / 'repo'
    (root / '.dvc').mkdir(parents=True)
    (root / '.dt').mkdir()
    monkeypatch.chdir(root)
    return root


@pytest.fixture
def local_backend(tmp_path):
    return LocalDirBackend(root=str(tmp_path / 'cold-storage'))


@pytest.fixture
def staging_dir(tmp_path, monkeypatch):
    """Set the staging dir via dt config-style override."""
    p = tmp_path / 'staging'
    p.mkdir()
    monkeypatch.setattr(
        'dt.archive.operations.cfg.get_value',
        lambda key, default=None: str(p) if key == 'archive.staging_dir' else default,
    )
    return p


# --------------------------------------------------------------------------- #
# Manifest schema
# --------------------------------------------------------------------------- #

class TestManifestRoundTrip:
    def test_round_trip(self, tmp_path):
        m = manifest_mod.ArchiveManifest(
            archive_name='demo',
            source_remote='/some/remote',
            backend='local',
            backend_path='cold/demo.tar',
            tarball_filename='demo.tar',
            tarball_size_bytes=12345,
            tarball_sha256='deadbeef' * 8,
            total_objects=42,
            total_bytes=99999,
            compression='none',
            inner_tars={
                '00': manifest_mod.InnerTar(
                    filename='00.tar', size_bytes=100, sha256='aa' * 32,
                    n_objects=3,
                ),
            },
            extras_at_archive_time=[
                manifest_mod.ExtraFile(path='README.txt', size=4),
            ],
            created_at='2026-05-28T00:00:00+00:00',
            created_by='alice',
            git_ref='abc123',
            dt_version='0.6.0',
        )

        # Save into a temp project root and load back.
        path = manifest_mod.save_manifest(m, repo_root=tmp_path)
        assert path.exists()
        loaded = manifest_mod.load_manifest('demo', repo_root=tmp_path)

        assert loaded.archive_name == 'demo'
        assert loaded.tarball_sha256 == m.tarball_sha256
        assert loaded.inner_tars['00'].n_objects == 3
        assert loaded.extras_at_archive_time[0].path == 'README.txt'

    def test_rejects_future_version(self, tmp_path):
        path = manifest_mod.archives_dir(tmp_path)
        path.mkdir(parents=True)
        (path / 'future.yaml').write_text(
            "version: 99\narchive_name: future\nsource_remote: /x\n"
            "backend: local\nbackend_path: x\n"
        )
        with pytest.raises(ArchiveError, match='Unsupported manifest version'):
            manifest_mod.load_manifest('future', repo_root=tmp_path)


# --------------------------------------------------------------------------- #
# Scanning helpers
# --------------------------------------------------------------------------- #

class TestScanning:
    def test_scan_files_md5_counts_objects_per_prefix(self, sample_remote):
        remote, payloads = sample_remote
        prefix_dirs, stats = ops.scan_files_md5(remote)
        total = sum(n for n, _ in stats.values())
        assert total == len(payloads)
        # Each populated prefix dir is listed.
        assert all(p.is_dir() for p in prefix_dirs)

    def test_scan_files_md5_missing_dir(self, tmp_path):
        with pytest.raises(ArchiveError, match='No files/md5'):
            ops.scan_files_md5(tmp_path / 'nope')

    def test_scan_extras_picks_up_nonmd5_files(self, sample_remote):
        remote, _ = sample_remote
        extras = ops.scan_extras(remote)
        paths = {e.path for e in extras}
        assert 'README.txt' in paths
        assert 'config' in paths

    def test_scan_extras_does_not_descend_into_files_md5(self, sample_remote):
        remote, _ = sample_remote
        extras = ops.scan_extras(remote)
        for e in extras:
            assert not e.path.startswith('files/md5/'), \
                f"unexpected files/md5 entry classified as extra: {e.path}"


# --------------------------------------------------------------------------- #
# Staging-dir resolution
# --------------------------------------------------------------------------- #

class TestResolveStagingDir:
    def test_flag_wins(self, tmp_path):
        result = ops.resolve_staging_dir(str(tmp_path))
        assert result == tmp_path.resolve()

    def test_config_used_when_flag_missing(self, staging_dir):
        result = ops.resolve_staging_dir(None)
        assert result == staging_dir.resolve()

    def test_no_flag_no_config_raises(self, monkeypatch):
        monkeypatch.setattr(
            'dt.archive.operations.cfg.get_value',
            lambda key, default=None: default,
        )
        with pytest.raises(ArchiveError, match='No staging directory configured'):
            ops.resolve_staging_dir(None)


# --------------------------------------------------------------------------- #
# default_jobs
# --------------------------------------------------------------------------- #

class TestDefaultJobs:
    def test_uses_pbs_ncpus_when_set(self, monkeypatch):
        monkeypatch.setenv('PBS_NCPUS', '4')
        assert ops.default_jobs() == 4

    def test_caps_at_8(self, monkeypatch):
        monkeypatch.setenv('PBS_NCPUS', '48')
        assert ops.default_jobs() == 8

    def test_ignores_bad_pbs_ncpus(self, monkeypatch):
        monkeypatch.setenv('PBS_NCPUS', 'banana')
        # Fall back to cpu_count branch — just assert it's >= 1.
        assert ops.default_jobs() >= 1


# --------------------------------------------------------------------------- #
# create end-to-end (using LocalDirBackend)
# --------------------------------------------------------------------------- #

class TestCreateEndToEnd:
    def test_create_round_trip(self, sample_remote, project_root, local_backend,
                               staging_dir, capsys):
        remote, payloads = sample_remote
        result = ops.create_archive(
            name='demo',
            source_remote=remote,
            backend='local',
            backend_path='cold/demo.tar',
            jobs=2,
            compress='none',
            verbose=False,
            backend_override=local_backend,
            repo_root=project_root,
        )

        # Backend has the outer tar.
        outer = Path(local_backend.root) / 'cold/demo.tar'
        assert outer.exists()

        # Manifest is reachable via load_manifest.
        loaded = manifest_mod.load_manifest('demo', repo_root=project_root)
        assert loaded.tarball_sha256 == result.manifest.tarball_sha256
        assert loaded.total_objects == len(payloads)

        # sha256 recorded matches the file we shipped.
        h = hashlib.sha256()
        with open(outer, 'rb') as f:
            for chunk in iter(lambda: f.read(1 << 16), b''):
                h.update(chunk)
        assert h.hexdigest() == loaded.tarball_sha256

        # Inner tar entries: one per populated prefix.
        prefix_dirs, _ = ops.scan_files_md5(remote)
        assert set(loaded.inner_tars) == {p.name for p in prefix_dirs}

        # Extras are recorded but were not archived.
        extras_paths = {e.path for e in loaded.extras_at_archive_time}
        assert 'README.txt' in extras_paths

        # Warning was printed to stderr about extras.
        captured = capsys.readouterr()
        assert 'outside files/md5' in captured.err

    def test_create_dry_run_does_not_write(self, sample_remote, project_root,
                                            local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='dry',
            source_remote=remote,
            backend='local',
            backend_path='cold/dry.tar',
            jobs=2,
            dry_run=True,
            backend_override=local_backend,
            repo_root=project_root,
        )
        outer = Path(local_backend.root) / 'cold/dry.tar'
        assert not outer.exists()
        assert not manifest_mod.manifest_path('dry', repo_root=project_root).exists()

    def test_create_refuses_to_overwrite_existing_manifest(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote, _ = sample_remote
        ops.create_archive(
            name='dup',
            source_remote=remote,
            backend='local',
            backend_path='cold/dup.tar',
            jobs=1,
            backend_override=local_backend,
            repo_root=project_root,
        )
        with pytest.raises(ArchiveError, match='already exists'):
            ops.create_archive(
                name='dup',
                source_remote=remote,
                backend='local',
                backend_path='cold/dup.tar',
                jobs=1,
                backend_override=local_backend,
                repo_root=project_root,
            )

    def test_create_with_zstd_compression(self, sample_remote, project_root,
                                          local_backend, staging_dir):
        if shutil.which('zstd') is None:
            pytest.skip("zstd not available")
        # GNU tar pre-1.31 doesn't know --zstd; the system tar on some
        # NCI nodes is older. Probe before we exercise the path.
        probe = subprocess.run(
            ['tar', '--help'], capture_output=True, text=True,
        )
        if '--zstd' not in (probe.stdout or '') + (probe.stderr or ''):
            pytest.skip("system tar does not support --zstd")
        remote, _ = sample_remote
        result = ops.create_archive(
            name='zstd-demo',
            source_remote=remote,
            backend='local',
            backend_path='cold/zstd-demo.tar',
            compress='zstd',
            jobs=1,
            backend_override=local_backend,
            repo_root=project_root,
        )
        for prefix, inner in result.manifest.inner_tars.items():
            assert inner.filename.endswith('.tar.zst')


# --------------------------------------------------------------------------- #
# resume
# --------------------------------------------------------------------------- #

class TestResume:
    def _simulate_killed_run(
        self, remote, staging_dir, name='partial',
    ):
        """Build inner tarballs into staging without finishing the upload.

        Returns the staging subdir for *name* with its sentinels intact,
        as if a previous create_archive call had been killed between
        Phase 1 and Phase 2.
        """
        staging = staging_dir / name
        staging.mkdir(parents=True)
        prefix_dirs, stats = ops.scan_files_md5(remote)
        for p in prefix_dirs:
            ops.build_prefix_tarball(
                str(remote), p.name, str(staging),
                'none', stats[p.name][0],
            )
        return staging

    def test_build_writes_sentinel(self, sample_remote, tmp_path):
        remote, _ = sample_remote
        staging = tmp_path / 'staging'
        staging.mkdir()
        prefix_dirs, stats = ops.scan_files_md5(remote)
        p = prefix_dirs[0]
        row = ops.build_prefix_tarball(
            str(remote), p.name, str(staging), 'none', stats[p.name][0],
        )
        sentinel = staging / f"{row['filename']}{ops.SENTINEL_SUFFIX}"
        assert sentinel.exists()
        data = ops._load_sentinel(sentinel)
        assert data['sha256'] == row['sha256']
        assert data['size_bytes'] == row['size_bytes']

    def test_resume_skips_completed_prefixes(
        self, sample_remote, project_root, local_backend, staging_dir,
        capsys,
    ):
        remote, _ = sample_remote
        self._simulate_killed_run(remote, staging_dir, name='r')
        prefix_dirs, _ = ops.scan_files_md5(remote)

        result = ops.create_archive(
            name='r',
            source_remote=remote,
            backend='local',
            backend_path='cold/r.tar',
            jobs=1,
            resume=True,
            backend_override=local_backend,
            repo_root=project_root,
        )

        # Manifest covers every prefix.
        assert set(result.manifest.inner_tars) == {p.name for p in prefix_dirs}
        # Output confirms resume took every sentinel and ran zero workers.
        out = capsys.readouterr().out
        assert (
            f'Resume: {len(prefix_dirs)} of {len(prefix_dirs)} '
            f"prefix(es) already complete"
        ) in out
        assert (
            f'Phase 1/3: building 0 inner tarball(s)'
        ) in out

    def test_resume_rebuilds_prefix_when_sentinel_missing(
        self, sample_remote, project_root, local_backend, staging_dir,
        capsys,
    ):
        remote, _ = sample_remote
        staging = self._simulate_killed_run(remote, staging_dir, name='r2')
        sentinels = list(staging.glob(f'*{ops.SENTINEL_SUFFIX}'))
        assert sentinels
        target_sentinel = sentinels[0]
        target_sentinel.unlink()
        rebuilt_prefix = target_sentinel.name.split('.tar')[0]
        other_count = len(sentinels) - 1

        ops.create_archive(
            name='r2',
            source_remote=remote,
            backend='local',
            backend_path='cold/r2.tar',
            jobs=1,
            resume=True,
            backend_override=local_backend,
            repo_root=project_root,
        )

        out = capsys.readouterr().out
        assert 'building 1 inner tarball' in out
        assert f'skipping {other_count} already done' in out
        # And the rebuilt prefix name appears in the progress lines.
        assert rebuilt_prefix in out

    def test_failed_run_preserves_staging(
        self, sample_remote, project_root, staging_dir,
    ):
        remote, _ = sample_remote

        class BoomBackend(LocalDirBackend):
            name = 'boom'
            def put_stream(self, *_a, **_k):
                raise RuntimeError('upload exploded')

        with pytest.raises(Exception):
            ops.create_archive(
                name='boom',
                source_remote=remote,
                backend='local',
                backend_path='cold/boom.tar',
                jobs=1,
                backend_override=BoomBackend(root=str(project_root / 'cold')),
                repo_root=project_root,
            )

        staging = staging_dir / 'boom'
        # Staging must survive the failure so the user can --resume.
        assert staging.exists()
        # And at least one sentinel must be present.
        assert list(staging.glob(f'*{ops.SENTINEL_SUFFIX}'))


# --------------------------------------------------------------------------- #
# verify
# --------------------------------------------------------------------------- #

class TestVerify:
    def test_verify_passes_after_create(self, sample_remote, project_root,
                                         local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='vdemo', source_remote=remote, backend='local',
            backend_path='cold/v.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        res = ops.verify_archive('vdemo', backend_override=local_backend,
                                 repo_root=project_root)
        assert res.ok is True
        assert res.size_ok is True
        assert res.sha256_ok is True

    def test_verify_detects_size_mismatch(self, sample_remote, project_root,
                                           local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='size', source_remote=remote, backend='local',
            backend_path='cold/size.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        outer = Path(local_backend.root) / 'cold/size.tar'
        with open(outer, 'ab') as f:
            f.write(b'!!!')  # corrupt by appending
        res = ops.verify_archive('size', backend_override=local_backend,
                                 repo_root=project_root)
        assert res.ok is False
        assert res.size_ok is False

    def test_verify_detects_missing_backend_object(self, sample_remote,
                                                    project_root, local_backend,
                                                    staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='miss', source_remote=remote, backend='local',
            backend_path='cold/miss.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        outer = Path(local_backend.root) / 'cold/miss.tar'
        outer.unlink()
        res = ops.verify_archive('miss', backend_override=local_backend,
                                 repo_root=project_root)
        assert res.ok is False
        assert any('missing' in m for m in res.messages)

    def test_verify_deep_passes(self, sample_remote, project_root,
                                local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='deep', source_remote=remote, backend='local',
            backend_path='cold/deep.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        res = ops.verify_archive('deep', deep=True,
                                 backend_override=local_backend,
                                 repo_root=project_root)
        assert res.deep_ok is True
        assert res.ok is True


# --------------------------------------------------------------------------- #
# restore
# --------------------------------------------------------------------------- #

class TestRestore:
    def test_full_restore(self, sample_remote, project_root, local_backend,
                          staging_dir, tmp_path):
        remote, payloads = sample_remote
        ops.create_archive(
            name='r', source_remote=remote, backend='local',
            backend_path='cold/r.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        dest = tmp_path / 'restored'
        ops.restore_archive('r', to_path=dest,
                            backend_override=local_backend,
                            repo_root=project_root)
        # Every original payload should be retrievable from the restored tree.
        for p in payloads:
            h = _md5(p)
            restored = dest / 'files' / 'md5' / h[:2] / h[2:]
            assert restored.read_bytes() == p

    def test_restore_single_object(self, sample_remote, project_root,
                                    local_backend, staging_dir, tmp_path):
        remote, payloads = sample_remote
        ops.create_archive(
            name='one', source_remote=remote, backend='local',
            backend_path='cold/one.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        target_payload = payloads[1]
        target_hash = _md5(target_payload)
        dest = tmp_path / 'one-restore'
        written = ops.restore_archive(
            'one', to_path=dest, object_hash=target_hash,
            backend_override=local_backend, repo_root=project_root,
        )
        assert len(written) == 1
        assert written[0].read_bytes() == target_payload

    def test_restore_prefix(self, sample_remote, project_root,
                            local_backend, staging_dir, tmp_path):
        remote, payloads = sample_remote
        ops.create_archive(
            name='pre', source_remote=remote, backend='local',
            backend_path='cold/pre.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        # Pick the prefix of the first payload.
        target = payloads[0]
        prefix = _md5(target)[:2]
        dest = tmp_path / 'pre-restore'
        ops.restore_archive(
            'pre', to_path=dest, prefix=prefix,
            backend_override=local_backend, repo_root=project_root,
        )
        # Confirm the file lands at the expected path.
        rest = _md5(target)[2:]
        assert (dest / 'files' / 'md5' / prefix / rest).read_bytes() == target


# --------------------------------------------------------------------------- #
# prune
# --------------------------------------------------------------------------- #

class TestPrune:
    def test_prune_refuses_with_extras(self, sample_remote, project_root,
                                        local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='pr1', source_remote=remote, backend='local',
            backend_path='cold/pr1.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        with pytest.raises(ArchiveError, match='Refusing to prune'):
            ops.prune_archive('pr1', yes=True,
                              backend_override=local_backend,
                              repo_root=project_root)

    def test_prune_succeeds_after_extras_removed(self, sample_remote,
                                                  project_root, local_backend,
                                                  staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='pr2', source_remote=remote, backend='local',
            backend_path='cold/pr2.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        # Remove the extras.
        (remote / 'README.txt').unlink()
        (remote / 'config').unlink()

        result = ops.prune_archive('pr2', yes=True,
                                   backend_override=local_backend,
                                   repo_root=project_root)
        assert result.deleted_path == remote / 'files' / 'md5'
        assert not (remote / 'files' / 'md5').exists()
        assert result.bytes_freed > 0

    def test_prune_force_skips_extras_check(self, sample_remote, project_root,
                                             local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='pr3', source_remote=remote, backend='local',
            backend_path='cold/pr3.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        result = ops.prune_archive('pr3', yes=True, force=True,
                                   backend_override=local_backend,
                                   repo_root=project_root)
        assert not (remote / 'files' / 'md5').exists()

    def test_prune_refuses_on_verify_failure(self, sample_remote, project_root,
                                              local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='pr4', source_remote=remote, backend='local',
            backend_path='cold/pr4.tar', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        # Corrupt the backend object.
        outer = Path(local_backend.root) / 'cold/pr4.tar'
        outer.write_bytes(b'corrupted')

        with pytest.raises(ArchiveError, match='did not verify'):
            ops.prune_archive(
                'pr4', yes=True, force=True,
                backend_override=local_backend, repo_root=project_root,
            )
        # files/md5 must still be there.
        assert (remote / 'files' / 'md5').exists()


# --------------------------------------------------------------------------- #
# CLI default-name behavior
# --------------------------------------------------------------------------- #

class TestCliDefaultName:
    def test_create_without_name_uses_default(self, sample_remote, project_root,
                                                staging_dir, cli_runner,
                                                monkeypatch):
        """Omitting NAME picks ``<remote-dir>-<YYYY-MM-DD>``."""
        import datetime as _dt
        from dt import cli as cli_mod

        remote, _ = sample_remote
        monkeypatch.setattr(
            cli_mod.remote_mod, 'resolve_remote_path',
            lambda *a, **kw: remote,
        )
        monkeypatch.setattr(
            'dt.archive.operations.utils.find_project_root',
            lambda *a, **kw: project_root,
        )

        # --dry-run skips backend interaction, so we can exercise the
        # name-defaulting path without registering a real backend.
        result = cli_runner.invoke(
            cli_mod.cli,
            ['remote', 'archive', 'create',
             '--backend', 'local',
             '--staging-dir', str(staging_dir),
             '--dry-run'],
        )
        assert result.exit_code == 0, (
            f"CLI returned {result.exit_code}: {result.output}"
        )
        today = _dt.date.today().isoformat()
        expected_name = f"{remote.name}-{today}"
        # The dry-run message echoes the resolved name and manifest path.
        assert expected_name in result.output, result.output
        assert "Using default archive name" in result.output


# --------------------------------------------------------------------------- #
# Backend registry
# --------------------------------------------------------------------------- #

class TestBackendRegistry:
    def test_known_backends_includes_defaults(self):
        names = backends_mod.known_backends()
        assert 'mdss' in names
        assert 'local' in names

    def test_get_backend_unknown(self):
        with pytest.raises(ArchiveError, match='Unknown archive backend'):
            backends_mod.get_backend('does-not-exist')

    def test_register_backend_conflict(self):
        class Other:
            name = 'mdss'

            def __init__(self):
                pass

        with pytest.raises(ArchiveError, match='already registered'):
            backends_mod.register_backend('mdss', Other)


# --------------------------------------------------------------------------- #
# MdssBackend (no MDSS access required — just test argv construction)
# --------------------------------------------------------------------------- #

class TestMdssBackend:
    def test_put_file_invokes_mdss(self, tmp_path):
        be = MdssBackend()
        recorded = {}

        def fake_run(cmd, capture_output, text, **kwargs):
            recorded.setdefault('calls', []).append(cmd)

            class R:
                returncode = 0
                stdout = ''
                stderr = ''
            return R()

        with patch('dt.archive.backends.subprocess.run', side_effect=fake_run):
            be.put_file(tmp_path / 'x', 'foo/bar.tar')

        # First call is mkdir -p; second is put.
        calls = recorded['calls']
        assert any('put' in c for c in calls)
        assert any('mkdir' in c for c in calls)
