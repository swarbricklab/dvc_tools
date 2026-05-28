"""High-level archive operations: create / list / verify / restore / prune.

These functions are the entry points the CLI binds. They are also safe
to use programmatically.

Design notes
------------
- The inner tarballs (one per ``files/md5/XX/`` prefix) are built in
  parallel by ``ProcessPoolExecutor`` workers calling
  :func:`build_prefix_tarball`. That function is intentionally
  side-effect-free given its inputs so a future multi-node mode can
  invoke it as the unit of work in a ``qxub monitor`` job graph.

- The outer tarball is a plain ``tar`` of the inner tarballs (sorted
  ``00.tar`` through ``ff.tar``). The outer is always uncompressed —
  ``--compress`` controls only the inner level.

- Streaming the outer tar to the backend goes through
  :class:`HashingReader` so we never spool the full archive to disk
  twice and never recompute sha256 in a second pass.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, IO, List, Optional, Tuple

from .. import config as cfg
from .. import utils
from ..errors import ArchiveError
from . import backends as _backends
from .backends import (
    ArchiveBackend,
    LocalDirBackend,
    get_backend,
    sha256_of_remote,
)
from .manifest import (
    ArchiveManifest,
    ExtraFile,
    InnerTar,
    LAYOUT_NESTED_PREFIX,
    archives_dir,
    list_manifests,
    load_manifest,
    manifest_path,
    now_iso,
    save_manifest,
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
    backend_path: str
    size_ok: bool
    sha256_ok: bool
    deep_ok: Optional[bool]  # None when --deep wasn't run
    messages: List[str]

    @property
    def ok(self) -> bool:
        if not (self.size_ok and self.sha256_ok):
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


def default_jobs() -> int:
    """Sensible default for ``--jobs``.

    Uses ``$PBS_NCPUS`` when present (so users inside a PBS allocation
    don't accidentally over-parallelise) and caps at 8 because past
    that, Lustre OST contention generally erases any further benefit.
    """
    pbs_ncpus = os.environ.get('PBS_NCPUS')
    if pbs_ncpus:
        try:
            return max(1, min(int(pbs_ncpus), 8))
        except ValueError:
            pass
    return max(1, min(os.cpu_count() or 1, 8))


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
    cumulative_objects = 0
    cumulative_bytes = 0
    total = len(prefix_dirs)
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


def _format_duration(seconds: float) -> str:
    """Compact h/m/s formatter for elapsed times in progress lines."""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


def _stream_heartbeat(
    hr: "HashingReader",
    started: float,
    stop: threading.Event,
    interval: float,
) -> None:
    """Print bytes streamed every ``interval`` seconds until ``stop`` is set.

    Used during Phase 2 so a multi-hour ``put_stream`` to tape shows it
    is making progress instead of looking hung.
    """
    while not stop.wait(interval):
        sent = hr.bytes_read
        elapsed = time.monotonic() - started
        rate = sent / elapsed if elapsed > 0 else 0
        print(
            f"  ... sent {utils.format_size(sent)} "
            f"in {_format_duration(elapsed)} "
            f"({utils.format_size(int(rate))}/s)",
            flush=True,
        )


def _sha256_of_file(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            data = f.read(chunk)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


class HashingReader(IO[bytes]):
    """Wrap a file-like object, hash everything that flows through ``read()``.

    Used to compute the outer tar's sha256 in a single pass while we
    stream it to the backend.
    """

    def __init__(self, stream, algo: str = 'sha256') -> None:
        self._stream = stream
        self._h = hashlib.new(algo)
        self._bytes = 0

    def read(self, n: int = -1):  # type: ignore[override]
        data = self._stream.read(n) if n is not None and n >= 0 else self._stream.read()
        if data:
            self._h.update(data)
            self._bytes += len(data)
        return data

    @property
    def hexdigest(self) -> str:
        return self._h.hexdigest()

    @property
    def bytes_read(self) -> int:
        return self._bytes


# --------------------------------------------------------------------------- #
# Inner-tar worker (process-pool-safe)
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
    multi-node mode) as an independent ``qxub monitor`` task.

    Returns a dict compatible with :class:`InnerTar`.
    """
    _validate_compression(compress)
    remote = Path(remote_dir)
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)

    ext = _COMPRESSION_EXT[compress]
    filename = f"{prefix}.tar{ext}"
    target = staging / filename

    cmd = ['tar', '-C', str(remote), '-cf', str(target)]
    cmd.extend(_COMPRESSION_TAR_FLAGS[compress])
    cmd.extend(['files/md5/' + prefix])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise ArchiveError(
            f"tar failed for prefix {prefix}: {result.stderr.strip() or '(no output)'}"
        )

    sha = _sha256_of_file(target)
    size = target.stat().st_size
    return {
        'prefix': prefix,
        'filename': filename,
        'size_bytes': size,
        'sha256': sha,
        'n_objects': n_objects,
    }


# --------------------------------------------------------------------------- #
# create
# --------------------------------------------------------------------------- #

def _default_backend_path(name: str, source_remote: Path) -> str:
    """Where on the backend should we put the outer tar by default?

    We use ``<source_remote_name>/<archive_name>.tar`` so multiple
    archives of the same remote land beside each other on tape.
    """
    return f"dt-archive/{source_remote.name}/{name}.tar"


def _check_jobfs_headroom(staging_dir: Path, estimated_bytes: int,
                          force: bool = False, verbose: bool = False) -> None:
    """Warn (or fail) if staging dir doesn't have ~1× headroom."""
    try:
        du = shutil.disk_usage(staging_dir)
    except OSError:
        # If we can't tell, just keep going.
        return
    if du.free < estimated_bytes:
        msg = (
            f"Staging dir {staging_dir} has only "
            f"{utils.format_size(du.free)} free; estimated archive size is "
            f"{utils.format_size(estimated_bytes)}."
        )
        if force:
            if verbose:
                print(f"warning: {msg} (continuing because --force)")
        else:
            raise ArchiveError(
                msg + "\nFree space, choose a different --staging-dir, "
                "or rerun with --force to override."
            )


def create_archive(
    name: str,
    source_remote: Path,
    *,
    backend: str = 'mdss',
    backend_path: Optional[str] = None,
    staging_dir: Optional[str] = None,
    jobs: Optional[int] = None,
    compress: str = 'none',
    dry_run: bool = False,
    force: bool = False,
    keep_staging: bool = False,
    verbose: bool = False,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
) -> CreateResult:
    """Create an archive of ``source_remote/files/md5/`` and ship it.

    See ``dt remote archive create --help`` for usage. The function is
    the single source of truth for the create pipeline; the CLI is a
    thin wrapper.
    """
    _validate_compression(compress)
    repo_root = repo_root or utils.find_project_root()
    source_remote = source_remote.expanduser().resolve()
    if not source_remote.is_dir():
        raise ArchiveError(f"Source remote not found: {source_remote}")

    # Refuse to overwrite an existing manifest unless --force.
    target_manifest = manifest_path(name, repo_root=repo_root)
    if target_manifest.exists() and not force:
        raise ArchiveError(
            f"Archive manifest already exists at {target_manifest}.\n"
            f"Choose a different name or rerun with --force."
        )

    # Scan content. Progress is on by default — on a real remote, both
    # of these walks can take minutes and the user deserves a sign of
    # life. Tests calling scan_files_md5/scan_extras directly default to
    # silent.
    prefix_dirs, stats = scan_files_md5(source_remote, progress=True)
    extras = scan_extras(source_remote, progress=True)
    total_objects = sum(n for n, _ in stats.values())
    total_bytes = sum(b for _, b in stats.values())

    # Warn about extras (do not block).
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
            print(
                f"  ... and {len(extras) - 20} more",
                file=sys.stderr,
            )
        print(
            "  These will be recorded in the manifest under "
            "'extras_at_archive_time' for forensics.",
            file=sys.stderr,
        )

    # Resolve staging dir + check headroom.
    staging_root = resolve_staging_dir(staging_dir)
    staging_root.mkdir(parents=True, exist_ok=True)
    staging = staging_root / name
    _check_jobfs_headroom(staging_root, total_bytes, force=force, verbose=verbose)

    # Resolve backend name + default backend path *without* instantiating
    # the backend yet — dry-run must work on dev machines where mdss is
    # not installed.
    backend_name = (
        getattr(backend_override, 'name', backend)
        if backend_override is not None else backend
    )
    bpath = backend_path or _default_backend_path(name, source_remote)
    n_jobs = jobs if jobs and jobs > 0 else default_jobs()

    if dry_run:
        print(f"[dry-run] would archive: {source_remote}")
        print(f"[dry-run] prefixes:      {len(prefix_dirs)}")
        print(f"[dry-run] objects:       {total_objects}")
        print(f"[dry-run] total size:    {utils.format_size(total_bytes)}")
        print(f"[dry-run] compression:   {compress}")
        print(f"[dry-run] jobs:          {n_jobs}")
        print(f"[dry-run] staging:       {staging}")
        print(f"[dry-run] backend:       {backend_name}")
        print(f"[dry-run] backend path:  {bpath}")
        print(f"[dry-run] manifest:      {target_manifest}")
        # Reuse the existing manifest type just so the caller gets a
        # consistent return value, but never write it.
        manifest = ArchiveManifest(
            archive_name=name,
            source_remote=str(source_remote),
            backend=backend_name,
            backend_path=bpath,
            tarball_filename=Path(bpath).name,
            tarball_size_bytes=0,
            tarball_sha256='',
            layout=LAYOUT_NESTED_PREFIX,
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

    # Past this point we actually run the pipeline — instantiate the backend.
    if backend_override is not None:
        be = backend_override
    else:
        be = get_backend(backend)

    # Real run: prepare staging directory.
    if staging.exists():
        if not force:
            raise ArchiveError(
                f"Staging directory {staging} already exists. "
                f"Delete it or rerun with --force."
            )
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    inner_tars: Dict[str, InnerTar] = {}
    try:
        # Phase 1: parallel inner tarballs ----------------------------------
        print(
            f"Phase 1/3: building {len(prefix_dirs)} inner tarball(s) "
            f"with {n_jobs} worker(s); staging at {staging}",
            flush=True,
        )
        if n_jobs == 1 and len(prefix_dirs) > 1:
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
                for p in prefix_dirs
            }
            done = 0
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
                        f"  [{done:>3}/{len(prefix_dirs)}] {prefix} "
                        f"({row['n_objects']:>7} obj, {size_str:>10}) "
                        f"  t+{_format_duration(elapsed)}",
                        flush=True,
                    )
                else:
                    print(
                        f"  [{done:>3}/{len(prefix_dirs)}] {prefix} "
                        f"({size_str})",
                        flush=True,
                    )

        phase1_elapsed = time.monotonic() - phase1_start
        print(
            f"Phase 1/3: done in {_format_duration(phase1_elapsed)}",
            flush=True,
        )

        # Phase 2: stream outer tar -> backend ------------------------------
        sorted_inner_names = [
            inner_tars[k].filename
            for k in sorted(inner_tars)
        ]
        print(
            f"Phase 2/3: streaming outer tar of {len(sorted_inner_names)} "
            f"inner tarball(s) to {backend_name}:{bpath}",
            flush=True,
        )

        tar_cmd = ['tar', '-C', str(staging), '-cf', '-'] + sorted_inner_names
        proc = subprocess.Popen(
            tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        assert proc.stdout is not None
        hr = HashingReader(proc.stdout)
        phase2_start = time.monotonic()
        heartbeat_stop = threading.Event()
        heartbeat = threading.Thread(
            target=_stream_heartbeat,
            args=(hr, phase2_start, heartbeat_stop, 30.0),
            daemon=True,
        )
        heartbeat.start()
        try:
            be.put_stream(hr, bpath)
        finally:
            heartbeat_stop.set()
            heartbeat.join(timeout=1.0)
            if proc.stdout:
                proc.stdout.close()
        rc = proc.wait()
        if rc != 0:
            stderr = (proc.stderr.read() if proc.stderr else b'').decode('utf-8', 'replace')
            raise ArchiveError(
                f"outer tar failed (exit {rc}): "
                f"{stderr.strip() or '(no output)'}"
            )

        outer_sha = hr.hexdigest
        outer_size = hr.bytes_read
        phase2_elapsed = time.monotonic() - phase2_start
        rate = outer_size / phase2_elapsed if phase2_elapsed > 0 else 0
        print(
            f"Phase 2/3: sent {utils.format_size(outer_size)} in "
            f"{_format_duration(phase2_elapsed)} "
            f"({utils.format_size(int(rate))}/s)",
            flush=True,
        )

        # Phase 3: write manifest -------------------------------------------
        manifest = ArchiveManifest(
            archive_name=name,
            source_remote=str(source_remote),
            backend=backend_name,
            backend_path=bpath,
            tarball_filename=Path(bpath).name,
            tarball_size_bytes=outer_size,
            tarball_sha256=outer_sha,
            layout=LAYOUT_NESTED_PREFIX,
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
        print(f"Phase 3/3: wrote manifest {path_written}", flush=True)
        return CreateResult(manifest=manifest, manifest_path=path_written)

    finally:
        if not keep_staging and staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


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
    """Verify an archive against its manifest."""
    manifest = load_manifest(name, repo_root=repo_root)
    be = backend_override or get_backend(manifest.backend)
    messages: List[str] = []

    if not be.exists(manifest.backend_path):
        return VerifyResult(
            archive_name=name,
            backend=manifest.backend,
            backend_path=manifest.backend_path,
            size_ok=False,
            sha256_ok=False,
            deep_ok=None,
            messages=[f"backend object missing: {manifest.backend_path}"],
        )

    info = be.stat(manifest.backend_path)
    size_ok = info.get('size_bytes') == manifest.tarball_size_bytes
    if not size_ok:
        messages.append(
            f"size mismatch: backend reports "
            f"{info.get('size_bytes')} bytes, manifest expects "
            f"{manifest.tarball_size_bytes}"
        )

    actual_sha = sha256_of_remote(be, manifest.backend_path)
    sha256_ok = actual_sha == manifest.tarball_sha256
    if not sha256_ok:
        messages.append(
            f"sha256 mismatch: backend has {actual_sha}, "
            f"manifest expects {manifest.tarball_sha256}"
        )

    deep_ok: Optional[bool] = None
    if deep:
        deep_ok = _deep_verify(manifest, be, messages)

    return VerifyResult(
        archive_name=name,
        backend=manifest.backend,
        backend_path=manifest.backend_path,
        size_ok=size_ok,
        sha256_ok=sha256_ok,
        deep_ok=deep_ok,
        messages=messages,
    )


def _deep_verify(manifest: ArchiveManifest, be: ArchiveBackend,
                 messages: List[str]) -> bool:
    """Enumerate inner tars by streaming ``tar -t`` over the outer tar.

    Confirms each inner ``XX.tar[.zst]`` recorded in the manifest is
    present in the outer tar and that its size matches.
    """
    proc = be.get_stream(manifest.backend_path)
    try:
        assert proc.stdout is not None
        tar = subprocess.Popen(
            ['tar', '-tvf', '-'],
            stdin=proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        out, err = tar.communicate()
        if tar.returncode != 0:
            messages.append(
                f"tar -t failed during deep verify: "
                f"{err.strip() or '(no output)'}"
            )
            return False
    finally:
        rc = proc.wait()
        if rc != 0:
            messages.append(
                f"backend stream exited non-zero ({rc}) during deep verify"
            )

    # Parse `tar -tvf -` output: each line is `mode owner/group size date time name`.
    sizes: Dict[str, int] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 6:
            continue
        try:
            size = int(parts[2])
        except ValueError:
            continue
        name = parts[-1]
        sizes[name] = size

    ok = True
    for prefix, inner in manifest.inner_tars.items():
        actual = sizes.get(inner.filename)
        if actual is None:
            messages.append(f"deep: missing inner {inner.filename}")
            ok = False
        elif actual != inner.size_bytes:
            messages.append(
                f"deep: size mismatch for {inner.filename}: "
                f"outer has {actual}, manifest expects {inner.size_bytes}"
            )
            ok = False
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
    - neither set: full restore (downloads the outer tar and extracts all).

    Returns a list of paths written under ``to_path``.
    """
    if object_hash and prefix:
        raise ArchiveError("--object and --prefix are mutually exclusive")

    manifest = load_manifest(name, repo_root=repo_root)
    be = backend_override or get_backend(manifest.backend)
    to_path = to_path.expanduser().resolve()
    to_path.mkdir(parents=True, exist_ok=True)

    if object_hash:
        target_prefix = object_hash[:2]
        return _restore_single_object(manifest, be, to_path, target_prefix,
                                      object_hash, verbose=verbose)
    if prefix:
        return _restore_prefix(manifest, be, to_path, prefix, verbose=verbose)
    return _restore_full(manifest, be, to_path, verbose=verbose)


def _inner_tar_name(manifest: ArchiveManifest, prefix: str) -> str:
    inner = manifest.inner_tars.get(prefix)
    if inner is None:
        raise ArchiveError(
            f"Archive '{manifest.archive_name}' has no record of prefix {prefix}."
        )
    return inner.filename


def _tar_decompress_flag(compression: str) -> List[str]:
    if compression == 'gzip':
        return ['-z']
    if compression == 'zstd':
        return ['--zstd']
    return []


def _restore_full(manifest: ArchiveManifest, be: ArchiveBackend,
                  to_path: Path, *, verbose: bool) -> List[Path]:
    # Download then extract twice (outer, then each inner). For a full
    # restore there's no win to streaming, and downloading first lets
    # us reuse outer for retries.
    outer_tmp = to_path / manifest.tarball_filename
    if verbose:
        print(f"Downloading outer tar to {outer_tmp}")
    be.get_file(manifest.backend_path, outer_tmp)

    # Extract outer (always uncompressed).
    if verbose:
        print(f"Extracting outer tar into {to_path}")
    rc = subprocess.run(
        ['tar', '-xf', str(outer_tmp), '-C', str(to_path)],
    ).returncode
    if rc != 0:
        raise ArchiveError("Outer tar extraction failed")

    written: List[Path] = []
    decompress = _tar_decompress_flag(manifest.compression)
    for prefix, inner in sorted(manifest.inner_tars.items()):
        inner_path = to_path / inner.filename
        if verbose:
            print(f"Extracting inner tar {inner.filename}")
        rc = subprocess.run(
            ['tar', '-xf', str(inner_path), '-C', str(to_path)] + decompress,
        ).returncode
        if rc != 0:
            raise ArchiveError(f"Inner tar extraction failed: {inner.filename}")
        inner_path.unlink()
        written.append(to_path / 'files' / 'md5' / prefix)

    outer_tmp.unlink()
    return written


def _restore_prefix(manifest: ArchiveManifest, be: ArchiveBackend,
                    to_path: Path, prefix: str, *, verbose: bool) -> List[Path]:
    inner_name = _inner_tar_name(manifest, prefix)
    # Stream: outer tar -> tar -xO XX.tar -> tar -x -C to_path
    decompress = _tar_decompress_flag(manifest.compression)
    if verbose:
        print(
            f"Streaming outer tar from backend; extracting {inner_name} "
            f"into {to_path}"
        )

    src = be.get_stream(manifest.backend_path)
    try:
        assert src.stdout is not None
        mid = subprocess.Popen(
            ['tar', '-xOf', '-', inner_name],
            stdin=src.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        dst = subprocess.Popen(
            ['tar', '-xf', '-', '-C', str(to_path)] + decompress,
            stdin=mid.stdout, stderr=subprocess.PIPE,
        )
        # Close our refs so EOF propagates correctly when upstream finishes.
        if src.stdout:
            src.stdout.close()
        if mid.stdout:
            mid.stdout.close()
        dst.wait()
        mid.wait()
    finally:
        src.wait()

    for proc, label in [(src, 'backend stream'), (mid, 'tar -xO outer'),
                        (dst, 'tar -x inner')]:
        if proc.returncode not in (0, None):
            err = ''
            if proc.stderr is not None:
                try:
                    err = proc.stderr.read().decode('utf-8', 'replace')
                except Exception:
                    err = ''
            raise ArchiveError(
                f"{label} failed (exit {proc.returncode}): "
                f"{err.strip() or '(no output)'}"
            )

    return [to_path / 'files' / 'md5' / prefix]


def _restore_single_object(manifest: ArchiveManifest, be: ArchiveBackend,
                           to_path: Path, prefix: str, object_hash: str,
                           *, verbose: bool) -> List[Path]:
    inner_name = _inner_tar_name(manifest, prefix)
    # Within the inner tar, the entry path is files/md5/<prefix>/<rest>.
    entry = f"files/md5/{prefix}/{object_hash[2:]}"
    decompress = _tar_decompress_flag(manifest.compression)
    if verbose:
        print(
            f"Streaming outer tar; extracting {entry} from {inner_name} "
            f"into {to_path}"
        )

    src = be.get_stream(manifest.backend_path)
    try:
        assert src.stdout is not None
        mid = subprocess.Popen(
            ['tar', '-xOf', '-', inner_name],
            stdin=src.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        dst = subprocess.Popen(
            ['tar', '-xf', '-', '-C', str(to_path), entry] + decompress,
            stdin=mid.stdout, stderr=subprocess.PIPE,
        )
        if src.stdout:
            src.stdout.close()
        if mid.stdout:
            mid.stdout.close()
        dst.wait()
        mid.wait()
    finally:
        src.wait()

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
    - The archive doesn't verify (size + sha256).
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

    # Verify first.
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

    # Extras check.
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

    # Confirm.
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

    # Compute freed bytes before deletion (cheap-ish; we just summed
    # these in scan_files_md5 below).
    _, stats = scan_files_md5(source_remote)
    bytes_freed = sum(b for _, b in stats.values())

    shutil.rmtree(files_md5)
    return PruneResult(
        archive_name=name,
        deleted_path=files_md5,
        bytes_freed=bytes_freed,
    )
