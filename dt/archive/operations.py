"""High-level archive operations.

Public entry points
-------------------

- :func:`stage_archive`   — scan the DVC remote and build inner tarballs
  (one per md5 prefix) into a staging directory. Writes the manifest
  locally with no ``backend_dir`` yet.
- :func:`deposit_archive` — upload the staged inner tarballs to the
  backend folder, then upload a manifest sidecar last as the completion
  sentinel.
- :func:`create_archive`  — convenience wrapper that runs stage then
  deposit inline. Useful for tests and small archives that fit in one
  walltime.
- :func:`verify_archive`  — confirm every inner tar in the manifest is
  present on the backend with the expected size, optionally hashing
  them deep.
- :func:`restore_archive` — pull back full / per-prefix / single-object.
- :func:`prune_archive`   — delete the on-disk DVC remote once the
  archive verifies.

Design notes
------------

- Inner tarballs are one-per-md5-prefix and written by
  :func:`build_prefix_tarball`. The function is intentionally pure given
  its inputs so a future multi-node ``--via-qxub`` mode can dispatch one
  prefix per qsub job.

- Each ``<prefix>.tar[.zst]`` lives next to a ``<prefix>.tar*.done.json``
  sentinel (post-stage) and a ``<prefix>.tar*.deposited.json`` sentinel
  (post-deposit). Both make their phase independently resumable.

- The backend layout is *folder-per-archive*: every inner tar lands at
  ``<backend_dir>/<filename>`` and the manifest sidecar lands at
  ``<backend_dir>/<archive_name>.manifest.yaml`` *last*. Verify treats the
  sidecar's presence as proof the archive completed.

- No outer tar exists. The previous design streamed a single multi-TB
  tar to the backend, which serialised the upload and lost all of
  Phase 1's parallel work on every walltime hit.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .. import config as cfg
from .. import utils
from ..errors import ArchiveError
from . import backends as _backends
from .backends import (
    ArchiveBackend,
    LocalDirBackend,
    get_backend,
    sha256_of_file,
)
from .manifest import (
    ArchiveManifest,
    ExtraFile,
    InnerTar,
    LAYOUT_FOLDER_PER_PREFIX,
    archives_dir,
    list_manifests,
    load_manifest,
    manifest_path,
    now_iso,
    save_manifest,
    sidecar_name,
)


# --------------------------------------------------------------------------- #
# Public dataclasses
# --------------------------------------------------------------------------- #

@dataclass
class CreateResult:
    manifest: ArchiveManifest
    manifest_path: Path


@dataclass
class VerifyResult:
    archive_name: str
    backend: str
    backend_dir: str
    sidecar_ok: bool
    files_ok: bool
    deep_ok: Optional[bool]  # None when --deep wasn't run
    messages: List[str]

    @property
    def ok(self) -> bool:
        if not (self.sidecar_ok and self.files_ok):
            return False
        if self.deep_ok is False:
            return False
        return True


@dataclass
class PruneResult:
    archive_name: str
    deleted_path: Path
    bytes_freed: int


# --------------------------------------------------------------------------- #
# Sentinel filenames
# --------------------------------------------------------------------------- #

STAGED_SENTINEL_SUFFIX = '.done.json'
DEPOSITED_SENTINEL_SUFFIX = '.deposited.json'

# Backwards-compat alias for the old name still referenced by tests.
SENTINEL_SUFFIX = STAGED_SENTINEL_SUFFIX


# --------------------------------------------------------------------------- #
# Helpers: configuration / environment
# --------------------------------------------------------------------------- #

def resolve_staging_dir(staging_dir: Optional[str]) -> Path:
    """Resolve the staging directory using the agreed precedence.

    Order: ``--staging-dir`` argument → ``archive.staging_dir`` config →
    error with a helpful hint.
    """
    if staging_dir:
        return Path(staging_dir).expanduser().resolve()
    configured = cfg.get_value('archive.staging_dir')
    if configured:
        return Path(str(configured)).expanduser().resolve()
    raise ArchiveError(
        "No staging directory configured.\n"
        "Either pass --staging-dir or set archive.staging_dir:\n"
        "  dt config set archive.staging_dir /scratch/<project>/<user>/dt-archive"
    )


def default_stage_jobs() -> int:
    """Default for ``stage --jobs``.

    Reads ``archive.stage_jobs`` config, else uses ``$PBS_NCPUS`` (so
    users inside a PBS allocation don't over-parallelise), else
    ``os.cpu_count()``. Cap at 8 because past that, Lustre OST contention
    erases the gains.
    """
    configured = cfg.get_value('archive.stage_jobs')
    if configured:
        try:
            return max(1, int(configured))
        except (TypeError, ValueError):
            pass
    pbs_ncpus = os.environ.get('PBS_NCPUS')
    if pbs_ncpus:
        try:
            return max(1, min(int(pbs_ncpus), 8))
        except ValueError:
            pass
    return max(1, min(os.cpu_count() or 1, 8))


# Backwards-compat for tests / callers that still import default_jobs.
default_jobs = default_stage_jobs


def default_deposit_jobs() -> int:
    """Default for ``deposit --jobs``.

    Reads ``archive.deposit_jobs`` config, else ``4``. Bounded by MDSS
    politeness, not data-mover cores — wide fan-out can stall MDSS.
    """
    configured = cfg.get_value('archive.deposit_jobs')
    if configured:
        try:
            return max(1, int(configured))
        except (TypeError, ValueError):
            pass
    return 4


def default_compression() -> str:
    """Default compression for inner tarballs.

    Reads ``archive.compress`` config, else ``'zstd'``. DVC blobs are
    typically already-compressed scientific data, but per-prefix tar
    headers + occasional plaintext can still squeeze a few percent off,
    and zstd is fast enough that the cost is negligible.
    """
    configured = cfg.get_value('archive.compress')
    if configured and str(configured) in _COMPRESSION_TAR_FLAGS:
        return str(configured)
    return 'zstd'


def default_backend_root() -> str:
    """Default base path on the backend.

    Reads ``archive.backend_root`` config, else ``'dt-archive'``.
    """
    configured = cfg.get_value('archive.backend_root')
    if configured:
        return str(configured).rstrip('/')
    return 'dt-archive'


def _current_user() -> str:
    import getpass
    try:
        return getpass.getuser()
    except Exception:
        return os.environ.get('USER', 'unknown')


def _git_ref(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return out.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        return ''


def _dt_version() -> str:
    from .. import __version__
    return __version__


# --------------------------------------------------------------------------- #
# Helpers: remote scanning
# --------------------------------------------------------------------------- #

def scan_files_md5(
    remote_dir: Path, progress: bool = False,
) -> Tuple[List[Path], Dict[str, Tuple[int, int]]]:
    """Walk ``<remote_dir>/files/md5/`` and return per-prefix stats.

    Returns ``(prefix_dirs, stats)`` where ``stats[prefix] = (n_objects,
    total_bytes)``. Empty prefix dirs are included with zero counts so
    the resulting tarball layout is identical regardless of which
    prefixes happened to have data.

    When ``progress`` is True, emits per-prefix progress lines to
    stderr — useful on real remotes with many objects where each
    prefix scan can take seconds to minutes.
    """
    files_md5 = remote_dir / 'files' / 'md5'
    if not files_md5.is_dir():
        raise ArchiveError(
            f"No files/md5 directory found at {files_md5}.\n"
            f"Is {remote_dir} actually a DVC remote?"
        )

    if progress:
        print(f"Scanning {files_md5} ...", file=sys.stderr, flush=True)

    prefix_dirs: List[Path] = sorted(
        p for p in files_md5.iterdir() if p.is_dir()
    )
    stats: Dict[str, Tuple[int, int]] = {}
    total = len(prefix_dirs)
    cumulative_objects = 0
    cumulative_bytes = 0
    for idx, p in enumerate(prefix_dirs, start=1):
        n = 0
        size_sum = 0
        for entry in p.iterdir():
            if entry.is_file():
                n += 1
                size_sum += entry.stat().st_size
        stats[p.name] = (n, size_sum)
        cumulative_objects += n
        cumulative_bytes += size_sum
        if progress:
            print(
                f"  [{idx:>3}/{total}] {p.name}: {n:>9} object(s), "
                f"{utils.format_size(size_sum):>10}  "
                f"(running total: {cumulative_objects:>9} obj, "
                f"{utils.format_size(cumulative_bytes):>10})",
                file=sys.stderr,
                flush=True,
            )
    return prefix_dirs, stats


def scan_extras(
    remote_dir: Path, progress: bool = False,
) -> List[ExtraFile]:
    """Find files in ``remote_dir`` outside ``files/md5/``.

    Walks the whole remote dir but prunes descent into ``files/md5/``
    so the 256 prefix subdirs (potentially containing millions of
    blobs) don't get scanned. Returns files only — empty dirs are
    ignored.
    """
    if progress:
        print(
            f"Scanning {remote_dir} for files outside files/md5/ ...",
            file=sys.stderr, flush=True,
        )
    extras: List[ExtraFile] = []
    for root, dirs, files in os.walk(remote_dir):
        root_path = Path(root)
        try:
            rel = root_path.relative_to(remote_dir)
        except ValueError:
            continue
        # Prune the walk so we don't descend into files/md5/
        if rel == Path('files'):
            dirs[:] = [d for d in dirs if d != 'md5']
        for fname in files:
            fp = root_path / fname
            rel_file = fp.relative_to(remote_dir)
            try:
                size = fp.stat().st_size
            except OSError:
                size = 0
            extras.append(ExtraFile(path=str(rel_file), size=size))
    if progress:
        print(
            f"  found {len(extras)} extra file(s) outside files/md5/",
            file=sys.stderr, flush=True,
        )
    return extras


# --------------------------------------------------------------------------- #
# Helpers: tar / hashing
# --------------------------------------------------------------------------- #

_COMPRESSION_TAR_FLAGS = {
    'none': [],
    'gzip': ['-z'],
    'zstd': ['--zstd'],
}

_COMPRESSION_EXT = {
    'none': '',
    'gzip': '.gz',
    'zstd': '.zst',
}


def _validate_compression(compress: str) -> None:
    if compress not in _COMPRESSION_TAR_FLAGS:
        valid = ', '.join(sorted(_COMPRESSION_TAR_FLAGS))
        raise ArchiveError(
            f"Invalid compression '{compress}'. Must be one of: {valid}"
        )


_tar_supports_zstd_cache: Optional[bool] = None


def _tar_supports_zstd() -> bool:
    """Probe the system tar for ``--zstd`` support.

    GNU tar < 1.31 doesn't know ``--zstd``; some NCI nodes still carry
    an older tar. Cached for the lifetime of the process.
    """
    global _tar_supports_zstd_cache
    if _tar_supports_zstd_cache is not None:
        return _tar_supports_zstd_cache
    try:
        probe = subprocess.run(
            ['tar', '--help'], capture_output=True, text=True,
        )
        haystack = (probe.stdout or '') + (probe.stderr or '')
        _tar_supports_zstd_cache = '--zstd' in haystack
    except (FileNotFoundError, OSError):
        _tar_supports_zstd_cache = False
    return _tar_supports_zstd_cache


def _resolve_compression(requested: str) -> str:
    """Apply the zstd-fallback policy.

    If the caller asked for zstd but the system tar can't do it, fall
    back to gzip with a one-line stderr warning. Any other value is
    returned unchanged.
    """
    if requested == 'zstd' and not _tar_supports_zstd():
        print(
            "warning: system tar does not support --zstd; "
            "falling back to gzip compression.",
            file=sys.stderr,
            flush=True,
        )
        return 'gzip'
    return requested


def _format_duration(seconds: float) -> str:
    """Compact h/m/s formatter for elapsed times in progress lines."""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


# --------------------------------------------------------------------------- #
# Helpers: sentinels
# --------------------------------------------------------------------------- #

def _sentinel_path_for(staging: Path, filename: str, suffix: str) -> Path:
    """Return the sentinel path for ``filename`` in ``staging``."""
    return staging / f"{filename}{suffix}"


def _load_sentinel(sentinel: Path) -> Optional[Dict]:
    """Load and shallow-validate a sentinel. Returns None on any problem."""
    try:
        with open(sentinel) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    required = {'prefix', 'filename', 'size_bytes', 'sha256', 'n_objects'}
    if not isinstance(data, dict) or not required.issubset(data):
        return None
    return data


def _write_sentinel(sentinel: Path, data: Dict) -> None:
    """Atomic sentinel write: tmp then rename."""
    tmp = sentinel.with_suffix(sentinel.suffix + '.tmp')
    with open(tmp, 'w') as f:
        json.dump(data, f)
    tmp.rename(sentinel)


# --------------------------------------------------------------------------- #
# Phase 1 worker: build one inner tarball
# --------------------------------------------------------------------------- #

def build_prefix_tarball(
    remote_dir: str,
    prefix: str,
    staging_dir: str,
    compress: str,
    n_objects: int,
) -> Dict:
    """Create one inner tarball for ``files/md5/<prefix>/``.

    Pure function: takes only picklable inputs and returns a dict.
    Safe to invoke as a ProcessPoolExecutor task and (in a future
    multi-node mode) as an independent qsub job.

    Writes ``<prefix>.tar[.ext]`` atomically (``.tmp`` + rename) and
    drops a ``<prefix>.tar[.ext].done.json`` sentinel beside it.

    Returns a dict compatible with :class:`InnerTar`.
    """
    _validate_compression(compress)
    remote = Path(remote_dir)
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)

    ext = _COMPRESSION_EXT[compress]
    filename = f"{prefix}.tar{ext}"
    target = staging / filename
    target_tmp = staging / f"{filename}.tmp"
    sentinel = _sentinel_path_for(staging, filename, STAGED_SENTINEL_SUFFIX)

    # Wipe any leftover from a previous killed attempt at this prefix —
    # the .done.json sentinel is the source of truth, not the presence of
    # a .tar file on disk.
    for p in (target, target_tmp, sentinel):
        if p.exists():
            p.unlink()

    cmd = ['tar', '-C', str(remote), '-cf', str(target_tmp)]
    cmd.extend(_COMPRESSION_TAR_FLAGS[compress])
    cmd.extend(['files/md5/' + prefix])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        if target_tmp.exists():
            target_tmp.unlink()
        raise ArchiveError(
            f"tar failed for prefix {prefix}: {result.stderr.strip() or '(no output)'}"
        )

    # Atomic publish: rename .tmp → final, then write the sentinel.
    target_tmp.rename(target)
    sha = sha256_of_file(target)
    size = target.stat().st_size

    data = {
        'prefix': prefix,
        'filename': filename,
        'size_bytes': size,
        'sha256': sha,
        'n_objects': n_objects,
    }
    _write_sentinel(sentinel, data)
    return data


# --------------------------------------------------------------------------- #
# Default paths
# --------------------------------------------------------------------------- #

def _default_backend_dir(name: str, source_remote: Path) -> str:
    """Where on the backend should this archive's folder live by default?"""
    root = default_backend_root()
    return f"{root}/{source_remote.name}/{name}/"


def _check_jobfs_headroom(staging_dir: Path, estimated_bytes: int,
                          force: bool, verbose: bool) -> None:
    """Warn if staging_dir's filesystem has < 1.5× estimated_bytes free."""
    try:
        usage = shutil.disk_usage(staging_dir)
    except OSError:
        return
    headroom = usage.free
    needed = int(estimated_bytes * 1.5)
    if headroom < needed:
        msg = (
            f"warning: staging dir {staging_dir} has "
            f"{utils.format_size(headroom)} free, "
            f"recommended {utils.format_size(needed)} "
            f"(1.5x estimated archive size {utils.format_size(estimated_bytes)})"
        )
        if force:
            print(msg + " — proceeding because --force", file=sys.stderr)
        else:
            print(msg, file=sys.stderr)
    elif verbose:
        print(
            f"  staging headroom: {utils.format_size(headroom)} free "
            f"vs. {utils.format_size(needed)} recommended"
        )


# --------------------------------------------------------------------------- #
# stage
# --------------------------------------------------------------------------- #

def stage_archive(
    name: str,
    source_remote: Path,
    *,
    backend: str = 'mdss',
    backend_dir: Optional[str] = None,
    staging_dir: Optional[str] = None,
    jobs: Optional[int] = None,
    compress: Optional[str] = None,
    dry_run: bool = False,
    force: bool = False,
    resume: bool = False,
    verbose: bool = False,
    repo_root: Optional[Path] = None,
) -> CreateResult:
    """Build all inner tarballs and write the manifest locally.

    Does not contact the backend. Safe to run on any compute node with
    parallel CPUs — the data-mover-only step is :func:`deposit_archive`.

    On success the manifest exists at ``.dvc/archives/<name>.yaml`` with
    ``backend_dir`` recorded (so deposit knows where to put files), and
    every prefix has a valid ``<prefix>.tar[.ext].done.json`` sentinel in
    the staging dir.
    """
    repo_root = repo_root or utils.find_project_root()
    source_remote = source_remote.expanduser().resolve()
    if not source_remote.is_dir():
        raise ArchiveError(f"Source remote not found: {source_remote}")

    compress = compress if compress is not None else default_compression()
    _validate_compression(compress)
    compress = _resolve_compression(compress)

    target_manifest = manifest_path(name, repo_root=repo_root)
    if target_manifest.exists() and not (force or resume):
        raise ArchiveError(
            f"Archive manifest already exists at {target_manifest}.\n"
            f"Choose a different name, or rerun with --force / --resume."
        )

    prefix_dirs, stats = scan_files_md5(source_remote, progress=True)
    extras = scan_extras(source_remote, progress=True)
    total_objects = sum(n for n, _ in stats.values())
    total_bytes = sum(b for _, b in stats.values())

    if extras:
        print(
            f"warning: {len(extras)} file(s) in {source_remote} are outside "
            f"files/md5/ and will NOT be archived:",
            file=sys.stderr,
        )
        for e in extras[:20]:
            print(
                f"  {e.path}  ({utils.format_size(e.size)})",
                file=sys.stderr,
            )
        if len(extras) > 20:
            print(f"  ... and {len(extras) - 20} more", file=sys.stderr)
        print(
            "  These will be recorded in the manifest under "
            "'extras_at_archive_time' for forensics.",
            file=sys.stderr,
        )

    staging_root = resolve_staging_dir(staging_dir)
    staging_root.mkdir(parents=True, exist_ok=True)
    staging = staging_root / name
    _check_jobfs_headroom(staging_root, total_bytes, force=force, verbose=verbose)

    bdir = backend_dir or _default_backend_dir(name, source_remote)
    if not bdir.endswith('/'):
        bdir = bdir + '/'
    n_jobs = jobs if jobs and jobs > 0 else default_stage_jobs()

    if dry_run:
        print(f"[dry-run] would stage: {source_remote}")
        print(f"[dry-run] prefixes:     {len(prefix_dirs)}")
        print(f"[dry-run] objects:      {total_objects}")
        print(f"[dry-run] total size:   {utils.format_size(total_bytes)}")
        print(f"[dry-run] compression:  {compress}")
        print(f"[dry-run] jobs:         {n_jobs}")
        print(f"[dry-run] staging:      {staging}")
        print(f"[dry-run] backend:      {backend}")
        print(f"[dry-run] backend dir:  {bdir}")
        print(f"[dry-run] manifest:     {target_manifest}")
        manifest = ArchiveManifest(
            archive_name=name,
            source_remote=str(source_remote),
            backend=backend,
            backend_dir=bdir,
            layout=LAYOUT_FOLDER_PER_PREFIX,
            total_objects=total_objects,
            total_bytes=total_bytes,
            compression=compress,
            inner_tars={},
            extras_at_archive_time=extras,
            created_at=now_iso(),
            created_by=_current_user(),
            git_ref=_git_ref(repo_root),
            dt_version=_dt_version(),
        )
        return CreateResult(manifest=manifest, manifest_path=target_manifest)

    # Prepare staging dir.
    if staging.exists():
        if force:
            shutil.rmtree(staging)
            staging.mkdir(parents=True)
        elif resume:
            print(
                f"Resume: reusing existing staging dir {staging}",
                flush=True,
            )
        else:
            raise ArchiveError(
                f"Staging directory {staging} already exists. "
                f"Delete it, rerun with --resume to continue, "
                f"or --force to start over."
            )
    else:
        staging.mkdir(parents=True)
        if resume:
            print(
                f"Resume: no prior staging dir at {staging}, starting fresh",
                flush=True,
            )

    # Scan staging for previously-staged prefixes.
    resume_done: Dict[str, Dict] = {}
    if resume:
        for sentinel in staging.glob(f'*{STAGED_SENTINEL_SUFFIX}'):
            data = _load_sentinel(sentinel)
            if not data:
                continue
            tar_file = staging / data['filename']
            try:
                actual_size = tar_file.stat().st_size
            except OSError:
                continue
            if actual_size != data['size_bytes']:
                continue
            resume_done[data['prefix']] = data
        if resume_done:
            print(
                f"Resume: {len(resume_done)} of {len(prefix_dirs)} "
                f"prefix(es) already staged; will re-tar the rest.",
                flush=True,
            )

    inner_tars: Dict[str, InnerTar] = {}
    success = False
    try:
        for pname, data in resume_done.items():
            inner_tars[pname] = InnerTar(
                filename=data['filename'],
                size_bytes=data['size_bytes'],
                sha256=data['sha256'],
                n_objects=data['n_objects'],
            )
        prefix_dirs_todo = [p for p in prefix_dirs if p.name not in resume_done]

        print(
            f"Stage: building {len(prefix_dirs_todo)} inner tarball(s) "
            f"with {n_jobs} worker(s); staging at {staging}"
            + (
                f" (skipping {len(resume_done)} already done)"
                if resume_done else ""
            ),
            flush=True,
        )
        if n_jobs == 1 and len(prefix_dirs_todo) > 1:
            print(
                "  note: --jobs=1 means prefixes are tarred serially; "
                "set $PBS_NCPUS (or pass --jobs N) to parallelise.",
                flush=True,
            )
        phase1_start = time.monotonic()

        with ProcessPoolExecutor(max_workers=n_jobs) as pool:
            futures = {
                pool.submit(
                    build_prefix_tarball,
                    str(source_remote),
                    p.name,
                    str(staging),
                    compress,
                    stats[p.name][0],
                ): p.name
                for p in prefix_dirs_todo
            }
            done = len(resume_done)
            total = len(prefix_dirs)
            for fut in as_completed(futures):
                prefix = futures[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    raise ArchiveError(
                        f"Worker for prefix {prefix} failed: {e}"
                    ) from e
                inner_tars[row['prefix']] = InnerTar(
                    filename=row['filename'],
                    size_bytes=row['size_bytes'],
                    sha256=row['sha256'],
                    n_objects=row['n_objects'],
                )
                done += 1
                size_str = utils.format_size(row['size_bytes'])
                if verbose:
                    elapsed = time.monotonic() - phase1_start
                    print(
                        f"  [{done:>3}/{total}] {prefix} "
                        f"({row['n_objects']:>7} obj, {size_str:>10}) "
                        f"  t+{_format_duration(elapsed)}",
                        flush=True,
                    )
                else:
                    print(
                        f"  [{done:>3}/{total}] {prefix} ({size_str})",
                        flush=True,
                    )

        phase1_elapsed = time.monotonic() - phase1_start
        print(
            f"Stage: done in {_format_duration(phase1_elapsed)}",
            flush=True,
        )

        manifest = ArchiveManifest(
            archive_name=name,
            source_remote=str(source_remote),
            backend=backend,
            backend_dir=bdir,
            layout=LAYOUT_FOLDER_PER_PREFIX,
            total_objects=total_objects,
            total_bytes=total_bytes,
            compression=compress,
            inner_tars=inner_tars,
            extras_at_archive_time=extras,
            created_at=now_iso(),
            created_by=_current_user(),
            git_ref=_git_ref(repo_root),
            dt_version=_dt_version(),
        )
        path_written = save_manifest(manifest, repo_root=repo_root)
        print(f"Stage: wrote manifest {path_written}", flush=True)
        success = True
        return CreateResult(manifest=manifest, manifest_path=path_written)

    finally:
        if not success and staging.exists():
            print(
                f"  staging dir preserved at {staging} — "
                f"rerun with --resume to continue.",
                file=sys.stderr,
                flush=True,
            )


# --------------------------------------------------------------------------- #
# deposit
# --------------------------------------------------------------------------- #

def deposit_archive(
    name: str,
    *,
    staging_dir: Optional[str] = None,
    jobs: Optional[int] = None,
    dry_run: bool = False,
    resume: bool = False,
    keep_staging: bool = False,
    verbose: bool = False,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
) -> CreateResult:
    """Upload staged inner tarballs to the backend folder.

    Reads ``.dvc/archives/<name>.yaml`` (written by :func:`stage_archive`),
    uploads each ``<prefix>.tar[.ext]`` to ``<backend_dir>/<filename>``
    in parallel, then uploads a copy of the manifest as
    ``<backend_dir>/<name>.manifest.yaml`` last as the completion
    sentinel.

    Each successful per-file upload writes a
    ``<filename>.deposited.json`` sentinel in staging, so a re-run with
    ``resume=True`` skips already-uploaded files.

    On success and unless ``keep_staging`` is set, the staging dir is
    removed.
    """
    repo_root = repo_root or utils.find_project_root()
    manifest = load_manifest(name, repo_root=repo_root)
    if not manifest.backend_dir:
        raise ArchiveError(
            f"Manifest for '{name}' has no backend_dir — was it produced "
            f"by an older dt version? Re-run stage."
        )

    be = backend_override if backend_override is not None else get_backend(manifest.backend)
    staging_root = resolve_staging_dir(staging_dir)
    staging = staging_root / name
    if not staging.is_dir():
        raise ArchiveError(
            f"Staging directory {staging} not found. Has stage been run?"
        )

    # Sanity-check the staged inner tars match the manifest.
    missing = []
    for prefix, inner in manifest.inner_tars.items():
        if not (staging / inner.filename).is_file():
            missing.append(f"  {inner.filename}")
    if missing:
        raise ArchiveError(
            "Staging dir is missing inner tarballs listed in the manifest:\n"
            + "\n".join(missing)
            + "\nRe-run stage with --resume."
        )

    n_jobs = jobs if jobs and jobs > 0 else default_deposit_jobs()
    bdir = manifest.backend_dir.rstrip('/') + '/'

    # Identify already-uploaded prefixes (deposit sentinels).
    already: set = set()
    if resume:
        for prefix, inner in manifest.inner_tars.items():
            sentinel = _sentinel_path_for(
                staging, inner.filename, DEPOSITED_SENTINEL_SUFFIX,
            )
            if sentinel.exists():
                already.add(prefix)
        if already:
            print(
                f"Resume: {len(already)} of {len(manifest.inner_tars)} "
                f"file(s) already deposited.",
                flush=True,
            )

    todo = [
        (prefix, inner)
        for prefix, inner in manifest.inner_tars.items()
        if prefix not in already
    ]

    if dry_run:
        print(f"[dry-run] would deposit {len(todo)} file(s) "
              f"(of {len(manifest.inner_tars)}) to "
              f"{manifest.backend}:{bdir}")
        for prefix, inner in todo[:10]:
            print(f"  {inner.filename} ({utils.format_size(inner.size_bytes)})")
        if len(todo) > 10:
            print(f"  ... and {len(todo) - 10} more")
        sidecar = bdir + sidecar_name(name)
        print(f"[dry-run] then sidecar {sidecar} (completion sentinel)")
        return CreateResult(manifest=manifest, manifest_path=manifest_path(name, repo_root=repo_root))

    print(
        f"Deposit: uploading {len(todo)} of {len(manifest.inner_tars)} "
        f"file(s) to {manifest.backend}:{bdir} with {n_jobs} worker(s)",
        flush=True,
    )

    deposit_start = time.monotonic()
    uploaded_bytes = 0
    success = False

    def _upload_one(prefix: str, inner: InnerTar) -> Tuple[str, InnerTar, int]:
        local = staging / inner.filename
        remote = bdir + inner.filename
        be.put_file(local, remote)
        sentinel = _sentinel_path_for(
            staging, inner.filename, DEPOSITED_SENTINEL_SUFFIX,
        )
        data = {
            'prefix': prefix,
            'filename': inner.filename,
            'size_bytes': inner.size_bytes,
            'sha256': inner.sha256,
            'n_objects': inner.n_objects,
            'deposited_at': now_iso(),
        }
        _write_sentinel(sentinel, data)
        return prefix, inner, inner.size_bytes

    try:
        with ThreadPoolExecutor(max_workers=n_jobs) as pool:
            futures = {
                pool.submit(_upload_one, prefix, inner): (prefix, inner)
                for prefix, inner in todo
            }
            done = len(already)
            total = len(manifest.inner_tars)
            for fut in as_completed(futures):
                prefix, inner = futures[fut]
                try:
                    _, _, n = fut.result()
                except Exception as e:
                    raise ArchiveError(
                        f"Upload failed for {inner.filename}: {e}"
                    ) from e
                done += 1
                uploaded_bytes += n
                elapsed = time.monotonic() - deposit_start
                rate = uploaded_bytes / elapsed if elapsed > 0 else 0
                print(
                    f"  [{done:>3}/{total}] {inner.filename} "
                    f"({utils.format_size(inner.size_bytes)}) "
                    f"  t+{_format_duration(elapsed)} "
                    f"({utils.format_size(int(rate))}/s avg)",
                    flush=True,
                )

        # Upload the sidecar last — its presence is what marks the
        # archive complete.
        local_manifest = manifest_path(name, repo_root=repo_root)
        sidecar = bdir + sidecar_name(name)
        print(f"Deposit: uploading sidecar {sidecar}", flush=True)
        be.put_file(local_manifest, sidecar)

        deposit_elapsed = time.monotonic() - deposit_start
        total_size = sum(i.size_bytes for i in manifest.inner_tars.values())
        rate = total_size / deposit_elapsed if deposit_elapsed > 0 else 0
        print(
            f"Deposit: shipped {utils.format_size(total_size)} in "
            f"{_format_duration(deposit_elapsed)} "
            f"({utils.format_size(int(rate))}/s overall)",
            flush=True,
        )
        success = True
        return CreateResult(
            manifest=manifest,
            manifest_path=local_manifest,
        )

    finally:
        if success and not keep_staging and staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        elif not success and staging.exists():
            print(
                f"  staging dir preserved at {staging} — "
                f"rerun with --resume to continue.",
                file=sys.stderr,
                flush=True,
            )


# --------------------------------------------------------------------------- #
# create (wrapper)
# --------------------------------------------------------------------------- #

def create_archive(
    name: str,
    source_remote: Path,
    *,
    backend: str = 'mdss',
    backend_path: Optional[str] = None,
    backend_dir: Optional[str] = None,
    staging_dir: Optional[str] = None,
    jobs: Optional[int] = None,
    deposit_jobs: Optional[int] = None,
    compress: Optional[str] = None,
    dry_run: bool = False,
    force: bool = False,
    resume: bool = False,
    keep_staging: bool = False,
    verbose: bool = False,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
) -> CreateResult:
    """Stage and deposit in one go.

    Convenience wrapper around :func:`stage_archive` + :func:`deposit_archive`.
    Useful for small archives that finish in one walltime; for multi-TB
    workloads prefer running ``stage`` on a compute node and ``deposit``
    on a data mover.

    ``backend_path`` is accepted as an alias for ``backend_dir`` for
    backwards-compatibility with the older single-tar layout.
    """
    if backend_path and not backend_dir:
        backend_dir = backend_path

    staged = stage_archive(
        name=name,
        source_remote=source_remote,
        backend=backend,
        backend_dir=backend_dir,
        staging_dir=staging_dir,
        jobs=jobs,
        compress=compress,
        dry_run=dry_run,
        force=force,
        resume=resume,
        verbose=verbose,
        repo_root=repo_root,
    )
    if dry_run:
        return staged

    return deposit_archive(
        name=name,
        staging_dir=staging_dir,
        jobs=deposit_jobs,
        resume=resume,
        keep_staging=keep_staging,
        verbose=verbose,
        backend_override=backend_override,
        repo_root=repo_root,
    )


# --------------------------------------------------------------------------- #
# list
# --------------------------------------------------------------------------- #

def list_archives(repo_root: Optional[Path] = None) -> List[ArchiveManifest]:
    """Return all manifests under ``.dvc/archives/``."""
    return list_manifests(repo_root=repo_root)


# --------------------------------------------------------------------------- #
# verify
# --------------------------------------------------------------------------- #

def verify_archive(
    name: str,
    *,
    deep: bool = False,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
) -> VerifyResult:
    """Verify an archive against its manifest.

    Checks:
    - The manifest sidecar exists at ``<backend_dir>/<name>.manifest.yaml``
      (the completion sentinel).
    - Every inner tar in the manifest exists at the expected backend path
      with the expected size.
    - With ``deep=True``, also downloads each inner tar to a temp file
      and hashes it.
    """
    manifest = load_manifest(name, repo_root=repo_root)
    if manifest.version < 2 or not manifest.backend_dir:
        raise ArchiveError(
            f"Archive '{name}' uses an older manifest schema "
            f"(version {manifest.version}) that this dt cannot verify. "
            f"Re-create the archive."
        )
    be = backend_override or get_backend(manifest.backend)
    bdir = manifest.backend_dir.rstrip('/') + '/'
    messages: List[str] = []

    sidecar_remote = bdir + sidecar_name(name)
    sidecar_ok = be.exists(sidecar_remote)
    if not sidecar_ok:
        messages.append(
            f"completion sentinel missing: {sidecar_remote} "
            f"(archive may be incomplete on backend)"
        )

    files_ok = True
    for prefix, inner in sorted(manifest.inner_tars.items()):
        remote_path = bdir + inner.filename
        if not be.exists(remote_path):
            messages.append(f"missing: {remote_path}")
            files_ok = False
            continue
        try:
            info = be.stat(remote_path)
        except ArchiveError as e:
            messages.append(f"stat failed for {remote_path}: {e}")
            files_ok = False
            continue
        actual = info.get('size_bytes')
        if actual != inner.size_bytes:
            messages.append(
                f"size mismatch for {remote_path}: "
                f"backend reports {actual}, manifest expects {inner.size_bytes}"
            )
            files_ok = False

    deep_ok: Optional[bool] = None
    if deep:
        deep_ok = _deep_verify(manifest, be, messages)

    return VerifyResult(
        archive_name=name,
        backend=manifest.backend,
        backend_dir=bdir,
        sidecar_ok=sidecar_ok,
        files_ok=files_ok,
        deep_ok=deep_ok,
        messages=messages,
    )


def _deep_verify(manifest: ArchiveManifest, be: ArchiveBackend,
                 messages: List[str]) -> bool:
    """Download each inner tar to a temp file and hash it."""
    import tempfile
    bdir = manifest.backend_dir.rstrip('/') + '/'
    ok = True
    with tempfile.TemporaryDirectory(prefix='dt-verify-') as tmpdir:
        tmp = Path(tmpdir)
        for prefix, inner in sorted(manifest.inner_tars.items()):
            remote_path = bdir + inner.filename
            local = tmp / inner.filename
            try:
                be.get_file(remote_path, local)
            except ArchiveError as e:
                messages.append(f"deep: download failed for {remote_path}: {e}")
                ok = False
                continue
            try:
                actual = sha256_of_file(local)
            except OSError as e:
                messages.append(
                    f"deep: failed to hash local copy of {remote_path}: {e}"
                )
                ok = False
                continue
            if actual != inner.sha256:
                messages.append(
                    f"deep: sha256 mismatch for {inner.filename}: "
                    f"backend has {actual}, manifest expects {inner.sha256}"
                )
                ok = False
            local.unlink(missing_ok=True)
    return ok


# --------------------------------------------------------------------------- #
# restore
# --------------------------------------------------------------------------- #

def restore_archive(
    name: str,
    to_path: Path,
    *,
    object_hash: Optional[str] = None,
    prefix: Optional[str] = None,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
    verbose: bool = False,
) -> List[Path]:
    """Restore from an archive.

    Modes:
    - ``object_hash`` set: extract just that md5 from the matching prefix.
    - ``prefix`` set: extract all objects in that prefix.
    - neither set: full restore (downloads every inner tar and extracts).

    Returns a list of paths written under ``to_path``.
    """
    if object_hash and prefix:
        raise ArchiveError("--object and --prefix are mutually exclusive")

    manifest = load_manifest(name, repo_root=repo_root)
    if manifest.version < 2 or not manifest.backend_dir:
        raise ArchiveError(
            f"Archive '{name}' uses an older manifest schema; "
            f"this dt cannot restore it."
        )
    be = backend_override or get_backend(manifest.backend)
    to_path = to_path.expanduser().resolve()
    to_path.mkdir(parents=True, exist_ok=True)

    if object_hash:
        target_prefix = object_hash[:2]
        return _restore_single_object(
            manifest, be, to_path, target_prefix, object_hash, verbose=verbose,
        )
    if prefix:
        return _restore_prefix(manifest, be, to_path, prefix, verbose=verbose)
    return _restore_full(manifest, be, to_path, verbose=verbose)


def _inner_tar(manifest: ArchiveManifest, prefix: str) -> InnerTar:
    inner = manifest.inner_tars.get(prefix)
    if inner is None:
        raise ArchiveError(
            f"Archive '{manifest.archive_name}' has no record of prefix {prefix}."
        )
    return inner


def _tar_decompress_flag(compression: str) -> List[str]:
    if compression == 'gzip':
        return ['-z']
    if compression == 'zstd':
        return ['--zstd']
    return []


def _extract_inner(local_tar: Path, to_path: Path, compression: str,
                   members: Optional[List[str]] = None) -> None:
    decompress = _tar_decompress_flag(compression)
    cmd = ['tar', '-xf', str(local_tar), '-C', str(to_path)] + decompress
    if members:
        cmd.extend(members)
    rc = subprocess.run(cmd).returncode
    if rc != 0:
        raise ArchiveError(f"tar extraction failed for {local_tar.name}")


def _restore_full(manifest: ArchiveManifest, be: ArchiveBackend,
                  to_path: Path, *, verbose: bool) -> List[Path]:
    bdir = manifest.backend_dir.rstrip('/') + '/'
    written: List[Path] = []
    import tempfile
    with tempfile.TemporaryDirectory(prefix='dt-restore-') as tmpdir:
        tmp = Path(tmpdir)
        for prefix, inner in sorted(manifest.inner_tars.items()):
            remote_path = bdir + inner.filename
            local = tmp / inner.filename
            if verbose:
                print(f"  fetching {remote_path}")
            be.get_file(remote_path, local)
            if verbose:
                print(f"  extracting {inner.filename}")
            _extract_inner(local, to_path, manifest.compression)
            local.unlink(missing_ok=True)
            written.append(to_path / 'files' / 'md5' / prefix)
    return written


def _restore_prefix(manifest: ArchiveManifest, be: ArchiveBackend,
                    to_path: Path, prefix: str, *, verbose: bool) -> List[Path]:
    inner = _inner_tar(manifest, prefix)
    bdir = manifest.backend_dir.rstrip('/') + '/'
    remote_path = bdir + inner.filename
    import tempfile
    with tempfile.TemporaryDirectory(prefix='dt-restore-') as tmpdir:
        local = Path(tmpdir) / inner.filename
        if verbose:
            print(f"  fetching {remote_path}")
        be.get_file(remote_path, local)
        if verbose:
            print(f"  extracting {inner.filename}")
        _extract_inner(local, to_path, manifest.compression)
    return [to_path / 'files' / 'md5' / prefix]


def _restore_single_object(manifest: ArchiveManifest, be: ArchiveBackend,
                           to_path: Path, prefix: str, object_hash: str,
                           *, verbose: bool) -> List[Path]:
    inner = _inner_tar(manifest, prefix)
    bdir = manifest.backend_dir.rstrip('/') + '/'
    remote_path = bdir + inner.filename
    entry = f"files/md5/{prefix}/{object_hash[2:]}"
    import tempfile
    with tempfile.TemporaryDirectory(prefix='dt-restore-') as tmpdir:
        local = Path(tmpdir) / inner.filename
        if verbose:
            print(f"  fetching {remote_path}")
        be.get_file(remote_path, local)
        if verbose:
            print(f"  extracting {entry}")
        _extract_inner(local, to_path, manifest.compression, members=[entry])

    written_path = to_path / entry
    if not written_path.exists():
        raise ArchiveError(
            f"Object {object_hash} was not produced by tar extraction "
            f"(expected at {written_path}). Is the hash correct?"
        )
    return [written_path]


# --------------------------------------------------------------------------- #
# prune
# --------------------------------------------------------------------------- #

def prune_archive(
    name: str,
    *,
    yes: bool = False,
    force: bool = False,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
    confirm_callback=None,
) -> PruneResult:
    """Delete ``<source_remote>/files/md5/`` after verifying the archive.

    Refuses to run if:
    - The archive doesn't verify (sidecar + file existence/sizes).
    - The source remote has files outside ``files/md5/`` (extras).

    ``--force`` bypasses the extras check (NOT the verify check).
    ``--yes`` suppresses the interactive confirm.
    """
    manifest = load_manifest(name, repo_root=repo_root)
    source_remote = Path(manifest.source_remote)
    files_md5 = source_remote / 'files' / 'md5'

    if not files_md5.is_dir():
        raise ArchiveError(
            f"Nothing to prune: {files_md5} doesn't exist "
            f"(already pruned?)"
        )

    vres = verify_archive(
        name,
        deep=False,
        backend_override=backend_override,
        repo_root=repo_root,
    )
    if not vres.ok:
        details = '\n  '.join(vres.messages) or '(no detail)'
        raise ArchiveError(
            f"Refusing to prune: archive '{name}' did not verify.\n  {details}"
        )

    extras = scan_extras(source_remote)
    if extras and not force:
        listing = '\n  '.join(
            f"{e.path}  ({utils.format_size(e.size)})" for e in extras[:20]
        )
        more = (
            f"\n  ... and {len(extras) - 20} more"
            if len(extras) > 20 else ''
        )
        raise ArchiveError(
            f"Refusing to prune: {len(extras)} file(s) under {source_remote} "
            f"are outside files/md5/ and NOT covered by archive '{name}'.\n"
            f"  {listing}{more}\n"
            f"Resolve those files (delete, move, or rerun create with "
            f"--include-extras when implemented), or rerun prune with --force."
        )

    if not yes:
        prompt = (
            f"This will permanently delete {files_md5}\n"
            f"(verified against archive '{name}' on backend "
            f"'{manifest.backend}'). Type 'yes' to continue: "
        )
        if confirm_callback is None:
            try:
                response = input(prompt)
            except EOFError:
                response = ''
        else:
            response = confirm_callback(prompt)
        if response.strip().lower() != 'yes':
            raise ArchiveError("Aborted by user.")

    _, stats = scan_files_md5(source_remote)
    bytes_freed = sum(b for _, b in stats.values())

    shutil.rmtree(files_md5)
    return PruneResult(
        archive_name=name,
        deleted_path=files_md5,
        bytes_freed=bytes_freed,
    )
