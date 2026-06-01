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
import yaml

from dt.archive import operations as ops
from dt.archive import manifest as manifest_mod
from dt.archive import backends as backends_mod
from dt.archive import registry as registry_mod
from dt.archive import signpost as signpost_mod
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
    """A temp repo root with .dvc/ and .dt/ so find_project_root() resolves here."""
    root = tmp_path / 'repo'
    (root / '.dvc').mkdir(parents=True)
    (root / '.dt' / 'archives').mkdir(parents=True)
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
            backend_dir='cold/demo/',
            total_objects=42,
            total_bytes=99999,
            compression='zstd',
            inner_tars={
                '00': manifest_mod.InnerTar(
                    filename='00.tar.zst', size_bytes=100, sha256='aa' * 32,
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
        assert loaded.backend_dir == 'cold/demo/'
        assert loaded.inner_tars['00'].sha256 == 'aa' * 32
        assert loaded.inner_tars['00'].n_objects == 3
        assert loaded.compression == 'zstd'
        assert loaded.extras_at_archive_time[0].path == 'README.txt'
        assert loaded.layout == manifest_mod.LAYOUT_FOLDER_PER_PREFIX

    def test_writes_to_dt_archives_not_dvc_archives(self, tmp_path):
        # save_manifest must target .dt/archives/, not the legacy path.
        m = manifest_mod.ArchiveManifest(
            archive_name='loc', source_remote='/x', backend='local',
            backend_dir='loc/', total_objects=0, total_bytes=0,
            inner_tars={}, compression='none',
        )
        path = manifest_mod.save_manifest(m, repo_root=tmp_path)
        assert path == tmp_path / '.dt' / 'archives' / 'loc.yaml'
        assert path.exists()
        assert not (tmp_path / '.dvc' / 'archives' / 'loc.yaml').exists()

    def test_load_falls_back_to_legacy_dvc_archives(self, tmp_path):
        # Pre-existing manifest under .dvc/archives/ must still load.
        legacy = tmp_path / '.dvc' / 'archives'
        legacy.mkdir(parents=True)
        m = manifest_mod.ArchiveManifest(
            archive_name='leg', source_remote='/x', backend='local',
            backend_dir='leg/', total_objects=0, total_bytes=0,
            inner_tars={}, compression='none',
        )
        with open(legacy / 'leg.yaml', 'w') as f:
            yaml.safe_dump(m.to_dict(), f)

        loaded = manifest_mod.load_manifest('leg', repo_root=tmp_path)
        assert loaded.archive_name == 'leg'

    def test_dt_archives_wins_over_legacy_on_conflict(self, tmp_path):
        new = tmp_path / '.dt' / 'archives'; new.mkdir(parents=True)
        legacy = tmp_path / '.dvc' / 'archives'; legacy.mkdir(parents=True)
        m_new = manifest_mod.ArchiveManifest(
            archive_name='c', source_remote='/x', backend='local',
            backend_dir='new/', total_objects=0, total_bytes=0,
            inner_tars={}, compression='none',
        )
        m_old = manifest_mod.ArchiveManifest(
            archive_name='c', source_remote='/x', backend='local',
            backend_dir='old/', total_objects=0, total_bytes=0,
            inner_tars={}, compression='none',
        )
        with open(new / 'c.yaml', 'w') as f:
            yaml.safe_dump(m_new.to_dict(), f)
        with open(legacy / 'c.yaml', 'w') as f:
            yaml.safe_dump(m_old.to_dict(), f)

        loaded = manifest_mod.load_manifest('c', repo_root=tmp_path)
        assert loaded.backend_dir == 'new/'

    def test_rejects_future_version(self, tmp_path):
        path = manifest_mod.archives_dir(tmp_path)
        path.mkdir(parents=True)
        (path / 'future.yaml').write_text(
            "version: 99\narchive_name: future\nsource_remote: /x\n"
            "backend: local\nbackend_dir: x/\n"
        )
        with pytest.raises(ArchiveError, match='newer than this dt'):
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
            backend_dir='cold/demo/',
            jobs=2,
            compress='none',
            verbose=False,
            backend_override=local_backend,
            repo_root=project_root,
        )

        backend_root = Path(local_backend.root)
        # Each inner tar lands at <backend_dir>/<filename>.
        for prefix, inner in result.manifest.inner_tars.items():
            assert (backend_root / 'cold/demo' / inner.filename).exists()
        # Manifest sidecar marks completion.
        sidecar = backend_root / 'cold/demo' / manifest_mod.sidecar_name('demo')
        assert sidecar.exists()

        # Manifest is reachable via load_manifest.
        loaded = manifest_mod.load_manifest('demo', repo_root=project_root)
        assert loaded.backend_dir == 'cold/demo/'
        assert loaded.total_objects == len(payloads)

        # Per-inner sha256 in the manifest matches the file actually on
        # the backend.
        for prefix, inner in loaded.inner_tars.items():
            local = backend_root / 'cold/demo' / inner.filename
            h = hashlib.sha256()
            with open(local, 'rb') as f:
                for chunk in iter(lambda: f.read(1 << 16), b''):
                    h.update(chunk)
            assert h.hexdigest() == inner.sha256
            assert local.stat().st_size == inner.size_bytes

        # Inner tar entries: one per populated prefix.
        prefix_dirs, _ = ops.scan_files_md5(remote)
        assert set(loaded.inner_tars) == {p.name for p in prefix_dirs}

        # Extras are recorded but were not archived.
        extras_paths = {e.path for e in loaded.extras_at_archive_time}
        assert 'README.txt' in extras_paths

        # Warning was printed to stderr about extras.
        captured = capsys.readouterr()
        assert 'outside the DVC blob layout' in captured.err

    def test_create_dry_run_does_not_write(self, sample_remote, project_root,
                                            local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='dry',
            source_remote=remote,
            backend='local',
            backend_dir='cold/dry/',
            jobs=2,
            dry_run=True,
            backend_override=local_backend,
            repo_root=project_root,
        )
        assert not (Path(local_backend.root) / 'cold/dry').exists()
        assert not manifest_mod.manifest_path('dry', repo_root=project_root).exists()

    def test_create_refuses_to_overwrite_existing_manifest(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote, _ = sample_remote
        ops.create_archive(
            name='dup',
            source_remote=remote,
            backend='local',
            backend_dir='cold/dup/',
            jobs=1,
            backend_override=local_backend,
            repo_root=project_root,
        )
        with pytest.raises(ArchiveError, match='already exists'):
            ops.create_archive(
                name='dup',
                source_remote=remote,
                backend='local',
                backend_dir='cold/dup/',
                jobs=1,
                backend_override=local_backend,
                repo_root=project_root,
            )

    def test_create_records_git_url_from_repo_origin(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        # Initialise a real git repo with an origin URL set.
        subprocess.run(['git', 'init', '-q'], cwd=project_root, check=True)
        subprocess.run(
            ['git', 'remote', 'add', 'origin',
             'git@github.com:example/proj.git'],
            cwd=project_root, check=True,
        )
        # Need at least one commit so git rev-parse HEAD works.
        (project_root / 'placeholder').write_text('hi')
        subprocess.run(['git', 'config', 'user.email', 'a@b.c'], cwd=project_root, check=True)
        subprocess.run(['git', 'config', 'user.name', 't'], cwd=project_root, check=True)
        subprocess.run(['git', 'add', 'placeholder'], cwd=project_root, check=True)
        subprocess.run(['git', 'commit', '-q', '-m', 'init'], cwd=project_root, check=True)

        remote, _ = sample_remote
        ops.create_archive(
            name='gu', source_remote=remote, backend='local',
            backend_dir='cold/gu/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        loaded = manifest_mod.load_manifest('gu', repo_root=project_root)
        assert loaded.git_url == 'git@github.com:example/proj.git'
        assert loaded.git_ref  # populated from real git

    def test_create_url_override_wins_over_repo_origin(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote, _ = sample_remote
        ops.create_archive(
            name='gu2', source_remote=remote, backend='local',
            backend_dir='cold/gu2/', jobs=1,
            git_url='https://example.com/explicit',
            backend_override=local_backend, repo_root=project_root,
        )
        loaded = manifest_mod.load_manifest('gu2', repo_root=project_root)
        assert loaded.git_url == 'https://example.com/explicit'

    def test_create_url_empty_when_not_a_repo(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        # project_root has no git repo, no --url override → empty string.
        remote, _ = sample_remote
        ops.create_archive(
            name='gu3', source_remote=remote, backend='local',
            backend_dir='cold/gu3/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        loaded = manifest_mod.load_manifest('gu3', repo_root=project_root)
        assert loaded.git_url == ''

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
            backend_dir='cold/zstd-demo/',
            compress='zstd',
            jobs=1,
            backend_override=local_backend,
            repo_root=project_root,
        )
        for prefix, inner in result.manifest.inner_tars.items():
            assert inner.filename.endswith('.tar.zst')


# --------------------------------------------------------------------------- #
# layout detection + v2 / mixed support
# --------------------------------------------------------------------------- #

@pytest.fixture
def v2_remote(tmp_path):
    """A DVC v2 layout — prefix dirs directly under the remote root."""
    remote = tmp_path / 'v2-remote'
    payloads = [b'alpha', b'beta', b'gamma']
    for p in payloads:
        h = hashlib.md5(p).hexdigest()
        prefix_dir = remote / h[:2]
        prefix_dir.mkdir(parents=True, exist_ok=True)
        (prefix_dir / h[2:]).write_bytes(p)
    (remote / 'README').write_text('v2 remote\n')
    return remote, payloads


@pytest.fixture
def mixed_remote(tmp_path):
    """Both v2 and v3 layouts populated in the same remote dir."""
    remote = tmp_path / 'mixed-remote'

    v3_payloads = [b'v3-one', b'v3-two']
    v2_payloads = [b'v2-one', b'v2-two']
    for p in v3_payloads:
        h = hashlib.md5(p).hexdigest()
        (remote / 'files' / 'md5' / h[:2]).mkdir(parents=True, exist_ok=True)
        (remote / 'files' / 'md5' / h[:2] / h[2:]).write_bytes(p)
    for p in v2_payloads:
        h = hashlib.md5(p).hexdigest()
        (remote / h[:2]).mkdir(parents=True, exist_ok=True)
        (remote / h[:2] / h[2:]).write_bytes(p)
    (remote / 'README').write_text('mixed\n')
    return remote, v3_payloads + v2_payloads, v3_payloads, v2_payloads


class TestLayoutDetection:
    def test_detects_v3(self, sample_remote):
        remote, _ = sample_remote
        assert ops.detect_source_layout(remote) == ops.LAYOUT_DVC_V3

    def test_detects_v2(self, v2_remote):
        remote, _ = v2_remote
        assert ops.detect_source_layout(remote) == ops.LAYOUT_DVC_V2

    def test_detects_mixed(self, mixed_remote):
        remote, *_ = mixed_remote
        assert ops.detect_source_layout(remote) == ops.LAYOUT_DVC_MIXED

    def test_raises_when_no_layout(self, tmp_path):
        empty = tmp_path / 'empty'
        empty.mkdir()
        with pytest.raises(ArchiveError, match='No DVC blob layout'):
            ops.detect_source_layout(empty)


class TestCreateV2Layout:
    def test_v2_round_trip(self, v2_remote, project_root, local_backend,
                           staging_dir, tmp_path):
        remote, payloads = v2_remote
        result = ops.create_archive(
            name='v2demo', source_remote=remote, backend='local',
            backend_dir='cold/v2demo/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        assert result.manifest.source_layout == ops.LAYOUT_DVC_V2
        # Tar filenames mirror the bare prefix (no v2- / v3- prefix in
        # pure layouts).
        prefixes = {hashlib.md5(p).hexdigest()[:2] for p in payloads}
        assert set(result.manifest.inner_tars) == prefixes

        # Full restore brings back the v2 paths (no files/md5/ wrapper).
        dest = tmp_path / 'restored-v2'
        ops.restore_archive(
            'v2demo', to_path=dest,
            backend_override=local_backend, repo_root=project_root,
        )
        for p in payloads:
            h = hashlib.md5(p).hexdigest()
            assert (dest / h[:2] / h[2:]).read_bytes() == p


class TestCreateMixedLayout:
    def test_mixed_round_trip(self, mixed_remote, project_root, local_backend,
                              staging_dir, tmp_path):
        remote, _all, v3_payloads, v2_payloads = mixed_remote
        result = ops.create_archive(
            name='mixed', source_remote=remote, backend='local',
            backend_dir='cold/mixed/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        assert result.manifest.source_layout == ops.LAYOUT_DVC_MIXED
        # Mixed-mode keys are namespaced.
        keys = set(result.manifest.inner_tars)
        assert any(k.startswith('v3-') for k in keys)
        assert any(k.startswith('v2-') for k in keys)
        # Filenames mirror the namespaced key.
        for k, inner in result.manifest.inner_tars.items():
            assert inner.filename.startswith(k + '.tar')

        # Full restore reproduces both layouts side by side.
        dest = tmp_path / 'restored-mixed'
        ops.restore_archive(
            'mixed', to_path=dest,
            backend_override=local_backend, repo_root=project_root,
        )
        for p in v3_payloads:
            h = hashlib.md5(p).hexdigest()
            assert (dest / 'files' / 'md5' / h[:2] / h[2:]).read_bytes() == p
        for p in v2_payloads:
            h = hashlib.md5(p).hexdigest()
            assert (dest / h[:2] / h[2:]).read_bytes() == p


class TestMixedRestoreErgonomics:
    """Mixed-aware restore: --prefix XX expands to both v2-XX and v3-XX,
    --object <hash> tries both candidate inner tars."""

    @pytest.fixture
    def collision_remote(self, tmp_path):
        """Mixed remote where one md5 prefix has BOTH a v2 blob and a
        v3 blob, so restoring --prefix XX must pull both halves."""
        remote = tmp_path / 'collide'
        v3 = b'data only in v3'
        v2 = b'data only in v2'
        h3 = hashlib.md5(v3).hexdigest()
        h2 = hashlib.md5(v2).hexdigest()
        # Force the two payloads to land in the same 2-char prefix dir.
        # The synthetic data above happens to differ in their hashes,
        # so we just take whichever prefix the v3 hash falls in and
        # synthesise a v2 file whose hash starts with the same 2 chars.
        prefix = h3[:2]
        (remote / 'files' / 'md5' / prefix).mkdir(parents=True)
        (remote / 'files' / 'md5' / prefix / h3[2:]).write_bytes(v3)
        (remote / prefix).mkdir(parents=True)
        # use a synthetic v2-only object name in the same prefix dir
        (remote / prefix / 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa').write_bytes(v2)
        return remote, prefix, v3, h3, v2

    def _archive(self, remote, project_root, local_backend, name):
        return ops.create_archive(
            name=name, source_remote=remote, backend='local',
            backend_dir=f'cold/{name}/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )

    def test_prefix_XX_expands_to_both_halves_in_mixed(
        self, collision_remote, project_root, local_backend, staging_dir,
        tmp_path,
    ):
        remote, prefix, v3_blob, v3_hash, v2_blob = collision_remote
        self._archive(remote, project_root, local_backend, 'mx1')

        dest = tmp_path / 'r-mx1'
        ops.restore_archive(
            'mx1', to_path=dest, prefix=prefix,
            backend_override=local_backend, repo_root=project_root,
        )
        # Both v3 and v2 content land in their original tree shapes.
        assert (dest / 'files' / 'md5' / prefix / v3_hash[2:]).read_bytes() == v3_blob
        assert (dest / prefix / 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa').read_bytes() == v2_blob

    def test_explicit_namespaced_prefix_still_works(
        self, collision_remote, project_root, local_backend, staging_dir,
        tmp_path,
    ):
        remote, prefix, v3_blob, v3_hash, v2_blob = collision_remote
        self._archive(remote, project_root, local_backend, 'mx2')

        dest = tmp_path / 'r-mx2'
        ops.restore_archive(
            'mx2', to_path=dest, prefix=f'v3-{prefix}',
            backend_override=local_backend, repo_root=project_root,
        )
        # Only the v3 half should be restored.
        assert (dest / 'files' / 'md5' / prefix / v3_hash[2:]).read_bytes() == v3_blob
        assert not (dest / prefix).exists()

    def test_object_restore_tries_both_candidates(
        self, mixed_remote, project_root, local_backend, staging_dir,
        tmp_path,
    ):
        remote, _all, v3_payloads, v2_payloads = mixed_remote
        self._archive(remote, project_root, local_backend, 'obj')

        # Pick a v2-only payload — restore-by-object should find it
        # despite the v3 candidate inner tar not containing it.
        target = v2_payloads[0]
        target_hash = hashlib.md5(target).hexdigest()
        dest = tmp_path / 'r-obj-v2'
        written = ops.restore_archive(
            'obj', to_path=dest, object_hash=target_hash,
            backend_override=local_backend, repo_root=project_root,
        )
        assert len(written) == 1
        assert written[0].read_bytes() == target

        # And a v3-only payload.
        target = v3_payloads[0]
        target_hash = hashlib.md5(target).hexdigest()
        dest = tmp_path / 'r-obj-v3'
        written = ops.restore_archive(
            'obj', to_path=dest, object_hash=target_hash,
            backend_override=local_backend, repo_root=project_root,
        )
        assert len(written) == 1
        assert written[0].read_bytes() == target


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
            backend_dir='cold/r/',
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
            f"prefix(es) already staged"
        ) in out
        assert 'Stage: building 0 inner tarball(s)' in out

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
            backend_dir='cold/r2/',
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

    def test_deposit_resume_skips_already_uploaded(
        self, sample_remote, project_root, local_backend, staging_dir,
        capsys,
    ):
        remote, _ = sample_remote
        # First run uploads everything; --keep-staging leaves sentinels behind.
        ops.create_archive(
            name='dr', source_remote=remote, backend='local',
            backend_dir='cold/dr/', jobs=1, keep_staging=True,
            backend_override=local_backend, repo_root=project_root,
        )
        staging = staging_dir / 'dr'
        deposited = list(staging.glob(f'*{ops.DEPOSITED_SENTINEL_SUFFIX}'))
        assert deposited, "first run should have produced deposit sentinels"
        n = len(deposited)
        capsys.readouterr()  # drain

        # Spy on the backend to confirm no upload happens on resume.
        calls = []

        class CountingBackend(LocalDirBackend):
            name = 'counting'

            def put_file(self, local_path, remote_path):  # type: ignore[override]
                calls.append(remote_path)
                return super().put_file(local_path, remote_path)

        ops.deposit_archive(
            name='dr', resume=True,
            backend_override=CountingBackend(root=str(local_backend.root)),
            repo_root=project_root,
        )

        # All inner tars skipped — only the sidecar should be re-uploaded.
        uploaded_inner = [c for c in calls if not c.endswith('.manifest.yaml')]
        assert uploaded_inner == [], (
            f"Resume should not have re-uploaded inner tars: {uploaded_inner}"
        )
        out = capsys.readouterr().out
        assert f"Resume: {n} of {n} file(s) already deposited" in out

    def test_qxub_worker_builds_from_config(
        self, sample_remote, tmp_path,
    ):
        remote, _ = sample_remote
        staging = tmp_path / 'staging' / 'qx'
        staging.mkdir(parents=True)
        prefix_dirs, stats = ops.scan_files_md5(remote)
        ops._save_qxub_job_config(staging, remote, 'none', stats, ops.LAYOUT_DVC_V3)

        # Worker entry point: should produce the same sentinel as a
        # direct build_prefix_tarball call.
        p = prefix_dirs[0]
        row = ops.build_prefix_from_config(staging, p.name)
        sentinel = staging / f"{row['filename']}{ops.SENTINEL_SUFFIX}"
        assert sentinel.exists()
        data = ops._load_sentinel(sentinel)
        assert data['sha256'] == row['sha256']
        assert data['size_bytes'] == row['size_bytes']
        assert data['n_objects'] == stats[p.name][0]

    def test_failed_run_preserves_staging(
        self, sample_remote, project_root, staging_dir,
    ):
        remote, _ = sample_remote

        class BoomBackend(LocalDirBackend):
            name = 'boom'
            def put_file(self, *_a, **_k):
                raise RuntimeError('upload exploded')

        with pytest.raises(Exception):
            ops.create_archive(
                name='boom',
                source_remote=remote,
                backend='local',
                backend_dir='cold/boom/',
                jobs=1,
                backend_override=BoomBackend(root=str(project_root / 'cold')),
                repo_root=project_root,
            )

        staging = staging_dir / 'boom'
        # Staging must survive the failure so the user can --resume.
        assert staging.exists()
        # And at least one stage-phase sentinel must be present.
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
            backend_dir='cold/v/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        res = ops.verify_archive('vdemo', backend_override=local_backend,
                                 repo_root=project_root)
        assert res.ok is True
        assert res.sidecar_ok is True
        assert res.files_ok is True

    def test_verify_detects_size_mismatch(self, sample_remote, project_root,
                                           local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='size', source_remote=remote, backend='local',
            backend_dir='cold/size/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        # Corrupt one inner tar on the backend by appending bytes.
        manifest = manifest_mod.load_manifest('size', repo_root=project_root)
        first_inner = next(iter(manifest.inner_tars.values()))
        target = Path(local_backend.root) / 'cold/size' / first_inner.filename
        with open(target, 'ab') as f:
            f.write(b'!!!')
        res = ops.verify_archive('size', backend_override=local_backend,
                                 repo_root=project_root)
        assert res.ok is False
        assert res.files_ok is False
        assert any('size mismatch' in m for m in res.messages)

    def test_verify_detects_missing_backend_object(self, sample_remote,
                                                    project_root, local_backend,
                                                    staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='miss', source_remote=remote, backend='local',
            backend_dir='cold/miss/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        manifest = manifest_mod.load_manifest('miss', repo_root=project_root)
        first_inner = next(iter(manifest.inner_tars.values()))
        target = Path(local_backend.root) / 'cold/miss' / first_inner.filename
        target.unlink()
        res = ops.verify_archive('miss', backend_override=local_backend,
                                 repo_root=project_root)
        assert res.ok is False
        assert any('missing' in m for m in res.messages)

    def test_verify_detects_missing_sidecar(self, sample_remote, project_root,
                                             local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='ms', source_remote=remote, backend='local',
            backend_dir='cold/ms/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        sidecar = Path(local_backend.root) / 'cold/ms' / manifest_mod.sidecar_name('ms')
        sidecar.unlink()
        res = ops.verify_archive('ms', backend_override=local_backend,
                                 repo_root=project_root)
        assert res.ok is False
        assert res.sidecar_ok is False

    def test_verify_deep_passes(self, sample_remote, project_root,
                                local_backend, staging_dir):
        remote, _ = sample_remote
        ops.create_archive(
            name='deep', source_remote=remote, backend='local',
            backend_dir='cold/deep/', jobs=1,
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
            backend_dir='cold/r/', jobs=1,
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
            backend_dir='cold/one/', jobs=1,
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
            backend_dir='cold/pre/', jobs=1,
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
            backend_dir='cold/pr1/', jobs=1,
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
            backend_dir='cold/pr2/', jobs=1,
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
            backend_dir='cold/pr3/', jobs=1,
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
            backend_dir='cold/pr4/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        # Delete the sidecar so verify refuses (acts as the completion sentinel).
        sidecar = Path(local_backend.root) / 'cold/pr4' / manifest_mod.sidecar_name('pr4')
        sidecar.unlink()

        with pytest.raises(ArchiveError, match='did not verify'):
            ops.prune_archive(
                'pr4', yes=True, force=True,
                backend_override=local_backend, repo_root=project_root,
            )
        # files/md5 must still be there.
        assert (remote / 'files' / 'md5').exists()


# --------------------------------------------------------------------------- #
# destroy
# --------------------------------------------------------------------------- #

class TestDestroy:
    def test_destroy_removes_backend_files_and_local_manifest(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote, _ = sample_remote
        ops.create_archive(
            name='gone', source_remote=remote, backend='local',
            backend_dir='cold/gone/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        backend_root = Path(local_backend.root)
        # Sanity: backend has the archive + sidecar.
        assert (backend_root / 'cold/gone' / manifest_mod.sidecar_name('gone')).exists()
        assert any((backend_root / 'cold/gone').glob('*.tar'))
        assert manifest_mod.manifest_path('gone', repo_root=project_root).exists()

        result = ops.destroy_archive(
            'gone', yes=True,
            backend_override=local_backend, repo_root=project_root,
        )

        # Backend is empty (or directory gone).
        assert not (backend_root / 'cold/gone' / manifest_mod.sidecar_name('gone')).exists()
        assert list((backend_root / 'cold/gone').glob('*.tar')) == []
        # Local manifest cleared.
        assert not manifest_mod.manifest_path('gone', repo_root=project_root).exists()
        # Source remote untouched.
        assert (remote / 'files' / 'md5').is_dir()
        # Result fields populated.
        assert result.files_deleted >= 2  # sidecar + at least one inner
        assert result.manifest_deleted is True

    def test_destroy_keep_manifest(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote, _ = sample_remote
        ops.create_archive(
            name='retry', source_remote=remote, backend='local',
            backend_dir='cold/retry/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        ops.destroy_archive(
            'retry', yes=True, keep_manifest=True,
            backend_override=local_backend, repo_root=project_root,
        )
        # Backend wiped …
        assert list((Path(local_backend.root) / 'cold/retry').glob('*')) == []
        # … but local manifest survives so the user can retry deposit.
        assert manifest_mod.manifest_path('retry', repo_root=project_root).exists()

    def test_destroy_does_not_touch_source(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote, payloads = sample_remote
        ops.create_archive(
            name='src', source_remote=remote, backend='local',
            backend_dir='cold/src/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        ops.destroy_archive(
            'src', yes=True,
            backend_override=local_backend, repo_root=project_root,
        )
        # Every original payload still readable on the source remote.
        for p in payloads:
            h = hashlib.md5(p).hexdigest()
            assert (remote / 'files' / 'md5' / h[:2] / h[2:]).read_bytes() == p

    def test_destroy_idempotent_on_already_gone_files(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote, _ = sample_remote
        ops.create_archive(
            name='twice', source_remote=remote, backend='local',
            backend_dir='cold/twice/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        # Manually pre-remove some backend files; destroy should still succeed.
        backend_dir = Path(local_backend.root) / 'cold/twice'
        for tar in list(backend_dir.glob('*.tar'))[:2]:
            tar.unlink()
        # Should not raise.
        ops.destroy_archive(
            'twice', yes=True, keep_manifest=True,
            backend_override=local_backend, repo_root=project_root,
        )


# --------------------------------------------------------------------------- #
# registry
# --------------------------------------------------------------------------- #

@pytest.fixture
def registry_dir(tmp_path, monkeypatch):
    """Configure archive.registry_path to a tmp dir.

    Chained with staging_dir's get_value patch so both keys resolve.
    """
    reg = tmp_path / 'registry'

    def fake_get_value(key, default=None):
        if key == 'archive.registry_path':
            return str(reg)
        if key == 'archive.staging_dir':
            existing = monkeypatch._marker_for_staging
            return str(existing) if existing is not None else default
        return default

    # Hold the staging_dir-fixture path for the lambda above.
    monkeypatch._marker_for_staging = None
    monkeypatch.setattr(
        'dt.archive.operations.cfg.get_value', fake_get_value,
    )
    monkeypatch.setattr(
        'dt.archive.registry.cfg.get_value', fake_get_value,
    )
    return reg


class TestRegistry:
    def test_disabled_when_unconfigured(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            'dt.archive.registry.cfg.get_value',
            lambda key, default=None: default,
        )
        assert registry_mod.registry_path() is None
        assert registry_mod.list_entries() == []
        # Hooks return None silently — they must never raise.
        m = manifest_mod.ArchiveManifest(
            archive_name='x', source_remote='/x', backend='local',
            backend_dir='x/', total_objects=0, total_bytes=0,
            inner_tars={}, compression='none',
        )
        assert registry_mod.record_created(m, tmp_path) is None
        assert registry_mod.record_verified('x', tmp_path, True, 'now') is None
        assert registry_mod.record_pruned('x', tmp_path, 'now') is None

    def test_record_created_writes_entry(self, tmp_path, monkeypatch):
        reg = tmp_path / 'registry'
        monkeypatch.setattr(
            'dt.archive.registry.cfg.get_value',
            lambda key, default=None: str(reg) if key == 'archive.registry_path' else default,
        )
        repo = tmp_path / 'myproject'
        repo.mkdir()
        m = manifest_mod.ArchiveManifest(
            archive_name='demo', source_remote='/x', backend='mdss',
            backend_dir='cold/demo/', total_objects=10, total_bytes=1234,
            compression='zstd',
            inner_tars={
                '00': manifest_mod.InnerTar(
                    filename='00.tar.zst', size_bytes=100, sha256='aa' * 32,
                    n_objects=3,
                ),
            },
            created_at='2026-05-29T00:00:00+00:00',
            created_by='alice',
        )
        path = registry_mod.record_created(m, repo)
        assert path is not None and path.exists()

        slug = registry_mod.project_slug(repo)
        assert path.name == f"{slug}__demo.yaml"
        entries = registry_mod.list_entries()
        assert len(entries) == 1
        assert entries[0].archive_name == 'demo'
        assert entries[0].project_name == 'myproject'
        assert entries[0].backend == 'mdss'
        assert entries[0].total_size_bytes == 100
        assert entries[0].compression == 'zstd'

    def test_record_verified_updates_status(self, tmp_path, monkeypatch):
        reg = tmp_path / 'registry'
        monkeypatch.setattr(
            'dt.archive.registry.cfg.get_value',
            lambda key, default=None: str(reg) if key == 'archive.registry_path' else default,
        )
        repo = tmp_path / 'p'
        repo.mkdir()
        m = manifest_mod.ArchiveManifest(
            archive_name='v', source_remote='/x', backend='local',
            backend_dir='v/', total_objects=0, total_bytes=0,
            inner_tars={}, compression='none',
        )
        registry_mod.record_created(m, repo)
        registry_mod.record_verified('v', repo, True, '2026-05-30T12:00:00+00:00')

        slug = registry_mod.project_slug(repo)
        entry = registry_mod.read_entry(slug, 'v')
        assert entry is not None
        assert entry.status.verified_ok is True
        assert entry.status.verified_at == '2026-05-30T12:00:00+00:00'

    def test_sync_from_roots_rebuilds(self, sample_remote, project_root,
                                     local_backend, staging_dir,
                                     tmp_path, monkeypatch):
        # Layer the registry key on top of the staging fixture's patch.
        # staging_dir already patched dt.archive.operations.cfg.get_value to
        # return the staging path for 'archive.staging_dir'. Wrap it.
        reg = tmp_path / 'registry'
        existing = ops.cfg.get_value

        def both(key, default=None):
            if key == 'archive.registry_path':
                return str(reg)
            return existing(key, default)

        monkeypatch.setattr(
            'dt.archive.operations.cfg.get_value', both,
        )
        monkeypatch.setattr(
            'dt.archive.registry.cfg.get_value', both,
        )

        remote, _ = sample_remote
        ops.create_archive(
            name='synced', source_remote=remote, backend='local',
            backend_dir='cold/synced/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        # Wipe the register; sync should rebuild from the manifest.
        for p in reg.glob('*.yaml'):
            p.unlink()
        assert registry_mod.list_entries() == []

        stats = registry_mod.sync_from_roots([project_root])
        assert stats['written'] == 1
        rebuilt = registry_mod.list_entries()
        assert len(rebuilt) == 1
        assert rebuilt[0].archive_name == 'synced'


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


# --------------------------------------------------------------------------- #
# signpost (ARCHIVED.yaml)
# --------------------------------------------------------------------------- #

class TestSignpost:
    def _archive_and_prune(
        self, sample_remote, project_root, local_backend, name,
    ):
        remote, _ = sample_remote
        # Strip extras so prune doesn't refuse.
        for extra in ('README.txt', 'config'):
            p = remote / extra
            if p.exists():
                p.unlink()
        ops.create_archive(
            name=name, source_remote=remote, backend='local',
            backend_dir=f'cold/{name}/', jobs=1,
            backend_override=local_backend, repo_root=project_root,
        )
        ops.prune_archive(
            name, yes=True,
            backend_override=local_backend, repo_root=project_root,
        )
        return remote

    def test_prune_drops_signpost(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote = self._archive_and_prune(
            sample_remote, project_root, local_backend, 'sp1',
        )
        sp_path = remote / signpost_mod.SIGNPOST_FILENAME
        assert sp_path.is_file()

        sp = signpost_mod.detect(remote)
        assert sp is not None
        assert sp.archive_name == 'sp1'
        assert sp.backend == 'local'
        assert sp.source_layout == ops.LAYOUT_DVC_V3
        assert sp.pruned_at  # non-empty timestamp
        assert sp.pruned_by  # populated from getpass.getuser()

        # The signpost yaml has a human-readable comment header.
        body = sp_path.read_text()
        assert body.lstrip().startswith('#')
        assert 'dt remote archive restore' in body
        assert 'dt_archive_signpost: 1' in body

    def test_detect_returns_none_when_absent(self, tmp_path):
        assert signpost_mod.detect(tmp_path) is None
        # Even when the file exists but lacks the marker key.
        (tmp_path / signpost_mod.SIGNPOST_FILENAME).write_text(
            "not_a_signpost: true\n",
        )
        assert signpost_mod.detect(tmp_path) is None

    def test_format_message_includes_restore_command(self, tmp_path):
        sp = signpost_mod.ArchiveSignpost(
            archive_name='foo', backend='mdss',
            backend_dir='cold/foo/', source_layout='dvc-v3',
            source_remote='/g/data/x/foo', git_url='', git_ref='',
            manifest_in_repo='.dt/archives/foo.yaml',
            pruned_at='2026-06-01T00:00:00+00:00', pruned_by='alice',
            path=tmp_path / 'ARCHIVED.yaml',
        )
        msg = signpost_mod.format_message(sp)
        assert 'dt remote archive restore foo' in msg
        assert 'mdss:cold/foo/' in msg
        assert '2026-06-01' in msg

    def test_restore_defaults_to_source_remote(
        self, sample_remote, project_root, local_backend, staging_dir,
    ):
        remote = self._archive_and_prune(
            sample_remote, project_root, local_backend, 'sp2',
        )
        # The remote dir exists but contains only the signpost; data is gone.
        assert not (remote / 'files' / 'md5').exists()

        # Restore with no --to: defaults to manifest.source_remote.
        written = ops.restore_archive(
            'sp2',
            backend_override=local_backend, repo_root=project_root,
        )
        # Data lands back in the source remote, signpost gets cleared.
        assert (remote / 'files' / 'md5').is_dir()
        assert not (remote / signpost_mod.SIGNPOST_FILENAME).exists()
        assert written  # at least one prefix dir reported

    def test_partial_restore_leaves_signpost(
        self, sample_remote, project_root, local_backend, staging_dir,
        tmp_path,
    ):
        remote = self._archive_and_prune(
            sample_remote, project_root, local_backend, 'sp3',
        )
        # Pick any one prefix to restore.
        manifest = manifest_mod.load_manifest('sp3', repo_root=project_root)
        any_prefix = next(iter(manifest.inner_tars))

        ops.restore_archive(
            'sp3', prefix=any_prefix,
            backend_override=local_backend, repo_root=project_root,
        )
        # Partial restore: signpost is still accurate ("most data still
        # archived"), so it must not be removed.
        assert (remote / signpost_mod.SIGNPOST_FILENAME).is_file()
