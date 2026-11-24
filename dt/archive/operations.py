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
import stat
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .. import config as cfg
from .. import hpc
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
    LAYOUT_DVC_MIXED,
    LAYOUT_DVC_V2,
    LAYOUT_DVC_V3,
    LAYOUT_FOLDER_PER_PREFIX,
    SUPPORTED_SOURCE_LAYOUTS,
    archives_dir,
    is_plain_name,
    list_manifests,
    load_manifest,
    manifest_path,
    now_iso,
    save_manifest,
    require_plain_name,
    sidecar_name,
)
from . import registry as _registry
from . import signpost as _signpost


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


@dataclass
class DestroyResult:
    archive_name: str
    backend: str
    backend_dir: str
    files_deleted: int
    bytes_freed: int
    manifest_deleted: bool


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


def _own_setting(key: str):
    """*key* from user or system config; a repository's own value for where
    archives are staged or stored is ignored (with a warning)."""
    value = cfg.get_value(key)
    scopes = cfg.scopes_defining(key)
    if scopes and scopes[0] in cfg.REPO_SCOPES:
        trusted = cfg.get_trusted_value(key)
        if trusted is None or not any(s in cfg.TRUSTED_SCOPES for s in scopes):
            print(f"Warning: ignoring {key} from {scopes[0]} config "
                  f"(set it in user or system config)", file=sys.stderr)
            return None
        return trusted
    return value

def resolve_staging_dir(staging_dir: Optional[str]) -> Path:
    """Resolve the staging directory using the agreed precedence.

    Order: ``--staging-dir`` argument → ``archive.staging_dir`` config →
    error with a helpful hint.
    """
    if staging_dir:
        return Path(staging_dir).expanduser().resolve()
    configured = _own_setting('archive.staging_dir')
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


def default_scan_jobs() -> int:
    """Default thread-pool size for the preflight remote scan.

    Reads ``archive.scan_jobs`` config, else ``32``. The scan is
    metadata-bound (``stat()`` per object on Lustre), not CPU-bound,
    so threads work and a higher fan-out than ``stage_jobs`` is fine.
    """
    configured = cfg.get_value('archive.scan_jobs')
    if configured:
        try:
            return max(1, int(configured))
        except (TypeError, ValueError):
            pass
    return 32


def default_compression() -> str:
    """Default compression for inner tarballs.

    Reads ``archive.compress`` config, else ``'none'``. DVC blobs are
    almost always already-compressed scientific data (h5ad, parquet,
    image binaries) and observed savings are ~5–10% — not worth the
    hours of CPU time on a single-core stage worker. Set to ``gzip``
    or ``zstd`` per-archive if your data is genuinely compressible.
    """
    configured = cfg.get_value('archive.compress')
    if configured and str(configured) in _COMPRESSION_TAR_FLAGS:
        return str(configured)
    return 'none'


def default_backend_root() -> str:
    """Default base path on the backend.

    Reads ``archive.backend_root`` config, else ``'dt-archive'``.
    """
    configured = _own_setting('archive.backend_root')
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


def _git_url(repo_root: Path) -> str:
    """Return the project's ``origin`` git URL, or empty string."""
    try:
        out = subprocess.check_output(
            ['git', 'remote', 'get-url', 'origin'],
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

def _is_hex_prefix(name: str) -> bool:
    """``True`` for a 2-char lowercase hex string like ``'00'`` or ``'ff'``."""
    return len(name) == 2 and all(c in '0123456789abcdef' for c in name)


def detect_source_layout(remote_dir: Path) -> str:
    """Infer the DVC layout in ``remote_dir``.

    Returns one of :data:`LAYOUT_DVC_V3`, :data:`LAYOUT_DVC_V2`,
    :data:`LAYOUT_DVC_MIXED`. Raises :class:`ArchiveError` if no DVC
    layout is detected.
    """
    files_md5 = remote_dir / 'files' / 'md5'
    has_v3 = False
    if _real_dir(remote_dir / 'files') and _real_dir(files_md5):
        try:
            for p in files_md5.iterdir():
                if _real_dir(p) and _is_hex_prefix(p.name):
                    has_v3 = True
                    break
        except OSError:
            pass
    has_v2 = False
    try:
        for p in remote_dir.iterdir():
            if _real_dir(p) and _is_hex_prefix(p.name):
                has_v2 = True
                break
    except OSError:
        pass
    if has_v2 and has_v3:
        return LAYOUT_DVC_MIXED
    if has_v3:
        return LAYOUT_DVC_V3
    if has_v2:
        return LAYOUT_DVC_V2
    raise ArchiveError(
        f"No DVC blob layout detected in {remote_dir}. "
        f"Expected either files/md5/<prefix>/ (v3) or <prefix>/ at "
        f"the top level (v2)."
    )


def _real_dir(p: Path) -> bool:
    """A directory at ``p`` itself, not a link to one."""
    return p.is_dir() and not p.is_symlink()


def _enumerate_prefix_dirs(
    remote_dir: Path, layout: str, include_links: bool = False,
) -> List[Tuple[str, Path]]:
    """Return ``[(manifest_key, dir_path), ...]`` for the given layout.

    Pure layouts use bare 2-char hex keys (``'00'`` … ``'ff'``); mixed
    uses prefixed keys (``'v3-00'``, ``'v2-00'``) to disambiguate.

    Only real directories are returned: a link in a shared remote could
    point anywhere. ``include_links=True`` also returns linked prefix
    entries (for callers that report them).
    """
    def ok(p: Path) -> bool:
        if not _is_hex_prefix(p.name):
            return False
        return (p.is_symlink() if include_links else False) or _real_dir(p)

    if layout == LAYOUT_DVC_V3:
        base = remote_dir / 'files' / 'md5'
        if not (_real_dir(remote_dir / 'files') and _real_dir(base)):
            raise ArchiveError(
                f"No files/md5 directory found at {base}. "
                f"Is {remote_dir} actually a DVC v3 remote?"
            )
        return [(p.name, p) for p in sorted(base.iterdir()) if ok(p)]
    if layout == LAYOUT_DVC_V2:
        return [(p.name, p) for p in sorted(remote_dir.iterdir()) if ok(p)]
    if layout == LAYOUT_DVC_MIXED:
        entries: List[Tuple[str, Path]] = []
        v3_base = remote_dir / 'files' / 'md5'
        if _real_dir(remote_dir / 'files') and _real_dir(v3_base):
            for p in sorted(v3_base.iterdir()):
                if ok(p):
                    entries.append((f'v3-{p.name}', p))
        for p in sorted(remote_dir.iterdir()):
            if ok(p):
                entries.append((f'v2-{p.name}', p))
        return entries
    raise ArchiveError(
        f"Unknown source layout {layout!r}. "
        f"Expected one of {SUPPORTED_SOURCE_LAYOUTS}."
    )


def _key_to_paths(key: str, layout: str) -> Tuple[str, str]:
    """Translate a manifest ``key`` into ``(tar_member_path, base_filename)``.

    ``tar_member_path`` is the relative path inside ``remote_dir`` that
    the worker tars; ``base_filename`` is the inner-tar basename (without
    a compression extension).
    """
    if layout == LAYOUT_DVC_V3:
        return f'files/md5/{key}', key
    if layout == LAYOUT_DVC_V2:
        return key, key
    if layout == LAYOUT_DVC_MIXED:
        if key.startswith('v3-'):
            return f'files/md5/{key[3:]}', key
        if key.startswith('v2-'):
            return key[3:], key
        raise ArchiveError(
            f"Mixed-layout key must start with 'v2-' or 'v3-': {key!r}"
        )
    raise ArchiveError(f"Unknown source layout {layout!r}")


_OBJECT_NAME_RE = None


def _is_object_name(name: str) -> bool:
    import re
    global _OBJECT_NAME_RE
    if _OBJECT_NAME_RE is None:
        _OBJECT_NAME_RE = re.compile(r'[0-9a-f]{30}(\.dir)?')
    return _OBJECT_NAME_RE.fullmatch(name) is not None


def _prefix_entries(prefix_dir: Path) -> Tuple[List[Tuple[str, int]], List[str]]:
    """``([(object_name, size), ...], [problem, ...])`` for one prefix dir.

    Entries are classified without following links: only regular files with
    DVC-object names are archived; links, subdirectories and other names are
    problems (a shared remote can hold others' links to anything).
    """
    objects: List[Tuple[str, int]] = []
    problems: List[str] = []
    try:
        fd = os.open(prefix_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as e:
        return objects, [f"{prefix_dir}: {e}"]
    try:
        with os.scandir(fd) as it:
            for entry in it:
                if entry.name.endswith('.tmp'):
                    continue  # an unfinished upload, as elsewhere
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                import stat as stat_mod
                if stat_mod.S_ISREG(st.st_mode) and _is_object_name(entry.name):
                    objects.append((entry.name, st.st_size))
                else:
                    kind = ('link' if stat_mod.S_ISLNK(st.st_mode) else
                            'directory' if stat_mod.S_ISDIR(st.st_mode) else
                            'not an object' if stat_mod.S_ISREG(st.st_mode) else 'special')
                    problems.append(f"{prefix_dir / entry.name} ({kind})")
    finally:
        os.close(fd)
    return objects, problems


def _scan_one_prefix(p: Path) -> Tuple[int, int]:
    """Worker for :func:`scan_prefixes`. Returns ``(n_objects, bytes)``
    over exactly the entries the archive will contain."""
    objects, _problems = _prefix_entries(p)
    return len(objects), sum(size for _name, size in objects)


def scan_prefixes(
    remote_dir: Path,
    layout: str,
    progress: bool = False,
    jobs: Optional[int] = None,
) -> Tuple[List[str], Dict[str, Tuple[int, int]]]:
    """Walk per-layout prefix dirs in parallel.

    Returns ``(keys, stats)`` where ``keys`` is the ordered list of
    manifest keys (e.g. ``['00', '01', ...]`` for pure layouts or
    ``['v3-00', ..., 'v2-00', ...]`` for mixed), and
    ``stats[key] = (n_objects, total_bytes)``.
    """
    entries = _enumerate_prefix_dirs(remote_dir, layout)
    linked = [str(p) for _k, p in
              _enumerate_prefix_dirs(remote_dir, layout, include_links=True)
              if p.is_symlink()]
    if linked:
        raise ArchiveError(
            f"{len(linked)} blob prefix director{'ies are' if len(linked) != 1 else 'y is a'} "
            f"symbolic link{'s' if len(linked) != 1 else ''} (e.g. {linked[0]}); "
            f"replace them with real directories before archiving.")
    n_jobs = jobs if jobs and jobs > 0 else default_scan_jobs()

    if progress:
        print(
            f"Scanning {remote_dir} ({layout}) with {n_jobs} worker(s) ...",
            file=sys.stderr, flush=True,
        )

    keys = [k for k, _ in entries]
    stats: Dict[str, Tuple[int, int]] = {}
    total = len(entries)
    cumulative_objects = 0
    cumulative_bytes = 0
    done = 0

    with ThreadPoolExecutor(max_workers=n_jobs) as pool:
        futures = {pool.submit(_scan_one_prefix, p): key for key, p in entries}
        for fut in as_completed(futures):
            key = futures[fut]
            n, size_sum = fut.result()
            stats[key] = (n, size_sum)
            cumulative_objects += n
            cumulative_bytes += size_sum
            done += 1
            if progress:
                print(
                    f"  [{done:>3}/{total}] {key}: {n:>9} object(s), "
                    f"{utils.format_size(size_sum):>10}  "
                    f"(running total: {cumulative_objects:>9} obj, "
                    f"{utils.format_size(cumulative_bytes):>10})",
                    file=sys.stderr,
                    flush=True,
                )

    return keys, stats


def scan_files_md5(
    remote_dir: Path,
    progress: bool = False,
    jobs: Optional[int] = None,
) -> Tuple[List[Path], Dict[str, Tuple[int, int]]]:
    """DVC v3 prefix scan — back-compat shim returning Path objects.

    New code should call :func:`scan_prefixes` with an explicit layout.
    Kept for callers that still expect ``List[Path]`` and the
    ``files/md5`` layout (existing tests + a couple of internal call
    sites).
    """
    entries = _enumerate_prefix_dirs(remote_dir, LAYOUT_DVC_V3)
    prefix_dirs = [p for _, p in entries]
    _, stats = scan_prefixes(remote_dir, LAYOUT_DVC_V3, progress=progress, jobs=jobs)
    return prefix_dirs, stats


def scan_extras(
    remote_dir: Path,
    layout: str = LAYOUT_DVC_V3,
    progress: bool = False,
) -> List[ExtraFile]:
    """Find files in ``remote_dir`` outside the DVC blob layout.

    For DVC v3 prunes descent into ``files/md5/``; for DVC v2 prunes
    descent into top-level 2-char hex prefix dirs; for mixed, both.
    Returns files only — empty dirs are ignored.
    """
    if progress:
        print(
            f"Scanning {remote_dir} ({layout}) for files outside "
            f"the DVC blob layout ...",
            file=sys.stderr, flush=True,
        )
    extras: List[ExtraFile] = []
    for root, dirs, files in os.walk(remote_dir):
        root_path = Path(root)
        try:
            rel = root_path.relative_to(remote_dir)
        except ValueError:
            continue
        # Prune the walk so we don't descend into archived prefix dirs.
        if rel == Path('.'):
            if layout in (LAYOUT_DVC_V2, LAYOUT_DVC_MIXED):
                dirs[:] = [d for d in dirs if not _is_hex_prefix(d)]
        elif rel == Path('files') and layout in (LAYOUT_DVC_V3, LAYOUT_DVC_MIXED):
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
            f"  found {len(extras)} extra file(s)",
            file=sys.stderr, flush=True,
        )
    return extras


# --------------------------------------------------------------------------- #
# Helpers: tar / hashing
# --------------------------------------------------------------------------- #

# Walltime for a single stage worker. See _archive_qxub_config.
DEFAULT_STAGE_WALLTIME = '04:00:00'

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


def _require_private_staging(staging: Path) -> None:
    """A per-archive staging directory must be this user's own and not
    writable by others (it may sit under a shared staging root)."""
    import stat as stat_mod
    try:
        fd = os.open(staging, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as e:
        raise ArchiveError(f"Staging directory {staging} is not usable: {e}")
    try:
        st = os.fstat(fd)
        if st.st_uid != os.geteuid():
            raise ArchiveError(f"Staging directory {staging} belongs to another user")
        if st.st_mode & (stat_mod.S_IWGRP | stat_mod.S_IWOTH):
            os.fchmod(fd, stat_mod.S_IMODE(st.st_mode) & ~0o022)
    finally:
        os.close(fd)


def _write_sentinel(sentinel: Path, data: Dict) -> None:
    """Atomic sentinel write: private tmp then rename."""
    utils.write_text_atomic(sentinel, json.dumps(data), mode=0o600)


# --------------------------------------------------------------------------- #
# qxub orchestration for stage
# --------------------------------------------------------------------------- #

QXUB_JOB_CONFIG_FILENAME = '.stage-args.json'


def _save_qxub_job_config(
    staging: Path,
    source_remote: Path,
    compress: str,
    stats: Dict[str, Tuple[int, int]],
    layout: str,
) -> Path:
    """Write a small per-archive job config to staging so qxub workers
    can read everything they need from a single file. Avoids enormous
    command lines and re-derives nothing in the worker.
    """
    config_path = staging / QXUB_JOB_CONFIG_FILENAME
    utils.write_text_atomic(config_path, json.dumps({
        'source_remote': str(source_remote),
        'compress': compress,
        'layout': layout,
        'n_objects_by_prefix': {p: n for p, (n, _) in stats.items()},
    }), mode=0o600)
    return config_path


def _load_qxub_job_config(staging: Path) -> Dict:
    config_path = staging / QXUB_JOB_CONFIG_FILENAME
    if not config_path.is_file():
        raise ArchiveError(
            f"qxub worker config not found at {config_path}. "
            f"Was this prefix dispatched by `dt remote archive stage --via-qxub`?"
        )
    return json.loads(config_path.read_text())


def build_prefix_from_config(staging_dir: Path, prefix: str) -> Dict:
    """Worker entry point: read the saved job config, build one prefix.

    Invoked on a compute node by the qxub-dispatched worker subcommand
    ``dt remote archive _build-prefix``. Calls :func:`build_prefix_tarball`
    after looking up the source remote / compression / layout /
    n_objects from the staging-dir job config.
    """
    cfg_data = _load_qxub_job_config(staging_dir)
    n_objects = int(cfg_data['n_objects_by_prefix'].get(prefix, 0))
    return build_prefix_tarball(
        cfg_data['source_remote'],
        prefix,
        str(staging_dir),
        cfg_data['compress'],
        n_objects,
        cfg_data.get('layout', LAYOUT_DVC_V3),
    )


def _normalize_walltime(value: str) -> str:
    """Accept ``HH:MM:SS`` or a bare-integer count of seconds.

    PBS on some Gadi configurations is happy with seconds, on others
    misreads a bare integer (treating it as minutes or worse), so we
    always send ``HH:MM:SS``. Strings already containing ``:`` are
    passed through unchanged.
    """
    s = str(value).strip()
    if not s or ':' in s:
        return s
    try:
        seconds = int(s)
    except ValueError:
        return s  # let qxub / qsub complain about whatever this is
    if seconds < 0:
        return s
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _archive_qxub_config() -> Dict[str, str]:
    """Resolve qxub settings for archive stage workers.

    Each worker is a single-CPU job that tars one md5 prefix on a
    compute node. The right queue is *not* copyq (the default for the
    generic `qxub.*` keys) — copyq is a data-mover queue with no
    compute parallelism. Falls back to the generic `qxub.*` keys when
    archive-specific keys are unset, so users with an existing qxub
    config can still override per-call.
    """
    # A stage worker tars one md5 prefix, so it is far shorter-lived than a
    # generic dt job; 4h asks the scheduler for less and starts sooner. This
    # is deliberately below the 10h generic default -- set
    # `archive.qxub_walltime` if your prefixes are unusually large.
    walltime_raw = (
        cfg.get_value('archive.qxub_walltime')
        or cfg.get_value('qxub.walltime', DEFAULT_STAGE_WALLTIME)
    )
    return {
        'env':      str(cfg.get_own_value('archive.qxub_env')
                        or cfg.get_own_value('qxub.env', 'dt')),
        'queue':    str(cfg.get_value('archive.qxub_queue')
                        or cfg.get_value('qxub.queue', 'normal')),
        'walltime': _normalize_walltime(walltime_raw),
        'mem':      str(cfg.get_value('archive.qxub_mem')
                        or cfg.get_value('qxub.mem', '4GB')),
    }


def _build_archive_qxub_command(job_name: str, worker_cmd: List[str]) -> List[str]:
    """Build a qxub exec command using archive-specific defaults."""
    config = _archive_qxub_config()
    return [
        'qxub', 'exec', '--terse',
        '--env', config['env'],
        '--queue', config['queue'],
        '--time', config['walltime'],
        '--mem', config['mem'],
        '-N', job_name,
        '--',
        *worker_cmd,
    ]


def _submit_prefix_jobs(
    archive_name: str,
    staging: Path,
    prefix_names: List[str],
    verbose: bool,
) -> List[str]:
    """Submit one qxub job per prefix; return the job IDs.

    On a per-prefix failure, raise :class:`ArchiveError` with the full
    submission command + qxub stdout + stderr + exit code so the user
    can reproduce manually. Some qxub builds print diagnostic info to
    stdout rather than stderr, so we surface both.
    """
    hpc.require_qxub()
    job_ids: List[str] = []
    repo_root = Path.cwd()
    for prefix in prefix_names:
        worker_cmd = [
            'dt', 'remote', 'archive', '_build-prefix',
            archive_name, prefix,
            '--staging-dir', str(staging.parent),
        ]
        job_name = f'dt-stage-{archive_name}-{prefix}'
        cmd = _build_archive_qxub_command(job_name, worker_cmd)
        if verbose:
            print(f"  submit {prefix}: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, cwd=repo_root,
            )
        except FileNotFoundError as e:
            raise ArchiveError(f"qxub not found: {e}") from e
        if result.returncode != 0:
            stdout = (result.stdout or '').strip()
            stderr = (result.stderr or '').strip()
            detail = (
                stderr or stdout or '(qxub printed nothing to stdout or stderr)'
            )
            raise ArchiveError(
                f"qxub submit failed for prefix {prefix} (exit "
                f"{result.returncode}):\n"
                f"  command:  {' '.join(cmd)}\n"
                f"  stdout:   {stdout or '(empty)'}\n"
                f"  stderr:   {stderr or '(empty)'}\n"
                f"detail: {detail}"
            )
        job_id = result.stdout.strip().split('\n')[0]
        if not job_id:
            raise ArchiveError(
                f"qxub submit for prefix {prefix} returned 0 but no job ID:\n"
                f"  command:  {' '.join(cmd)}\n"
                f"  stdout:   {(result.stdout or '').strip() or '(empty)'}\n"
                f"  stderr:   {(result.stderr or '').strip() or '(empty)'}"
            )
        job_ids.append(job_id)
    return job_ids


# --------------------------------------------------------------------------- #
# Phase 1 worker: build one inner tarball
# --------------------------------------------------------------------------- #

def build_prefix_tarball(
    remote_dir: str,
    prefix_key: str,
    staging_dir: str,
    compress: str,
    n_objects: int,
    layout: str = LAYOUT_DVC_V3,
) -> Dict:
    """Create one inner tarball for a single prefix in the source remote.

    Pure function: takes only picklable inputs and returns a dict. Safe
    to invoke as a ProcessPoolExecutor task and as an independent qsub
    job dispatched by ``--via-qxub``.

    ``prefix_key`` is the manifest key — bare 2-char hex for pure
    layouts (``'00'`` … ``'ff'``) or prefixed (``'v3-00'`` / ``'v2-00'``)
    for mixed remotes. ``layout`` selects the source-side path mapping.

    Writes ``<key>.tar[.ext]`` atomically (``.tmp`` + rename) and drops
    a ``<key>.tar[.ext].done.json`` sentinel beside it.

    Returns a dict compatible with :class:`InnerTar`.
    """
    _validate_compression(compress)
    remote = Path(remote_dir)
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)
    _require_private_staging(staging)

    member_path, base = _key_to_paths(prefix_key, layout)
    ext = _COMPRESSION_EXT[compress]
    filename = f"{base}.tar{ext}"
    target = staging / filename
    target_tmp = staging / f"{filename}.tmp"
    sentinel = _sentinel_path_for(staging, filename, STAGED_SENTINEL_SUFFIX)

    # Wipe any leftover from a previous killed attempt at this prefix —
    # the .done.json sentinel is the source of truth, not the presence of
    # a .tar file on disk.
    for p in (target, target_tmp, sentinel):
        if os.path.lexists(p):  # a dangling link too
            p.unlink()

    # Archive exactly the regular object files found (without following
    # links) -- never a link's target: a shared remote can hold others' links
    # to anything the archiver can read. tar gets the explicit list, does not
    # recurse and does not dereference, so an entry swapped for a link after
    # this check is stored as a link (and refused on restore), not followed.
    objects, problems = _prefix_entries(remote / member_path)
    if problems:
        raise ArchiveError(
            f"Refusing to archive prefix {prefix_key}: {len(problems)} entr"
            f"{'y is' if len(problems) == 1 else 'ies are'} not plain DVC objects:\n  "
            + '\n  '.join(problems[:20])
            + ('\n  ...' if len(problems) > 20 else ''))
    list_path = staging / f".{filename}.members"
    utils.write_text_atomic(
        list_path, ''.join(f"{member_path}/{name}\0" for name, _ in objects), mode=0o600)
    cmd = ['tar', '-C', str(remote), '-cf', str(target_tmp),
           '--no-recursion', '--null', '-T', str(list_path)]
    cmd.extend(_COMPRESSION_TAR_FLAGS[compress])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    finally:
        try:
            list_path.unlink()
        except OSError:
            pass
    if result.returncode != 0:
        if target_tmp.exists():
            target_tmp.unlink()
        raise ArchiveError(
            f"tar failed for prefix {prefix_key}: {result.stderr.strip() or '(no output)'}"
        )

    # Atomic publish: rename .tmp → final, then write the sentinel.
    target_tmp.rename(target)
    sha = sha256_of_file(target)
    size = target.stat().st_size

    data = {
        'prefix': prefix_key,
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
    source_layout: Optional[str] = None,
    dry_run: bool = False,
    force: bool = False,
    resume: bool = False,
    via_qxub: bool = False,
    git_url: Optional[str] = None,
    verbose: bool = False,
    repo_root: Optional[Path] = None,
) -> CreateResult:
    """Build all inner tarballs and write the manifest locally.

    Does not contact the backend. Safe to run on any compute node with
    parallel CPUs — the data-mover-only step is :func:`deposit_archive`.

    On success the manifest exists at ``.dt/archives/<name>.yaml`` with
    ``backend_dir`` and ``source_layout`` recorded, and every prefix has
    a valid ``<key>.tar[.ext].done.json`` sentinel in the staging dir.

    ``source_layout`` is auto-detected from the remote when ``None``;
    pass ``'dvc-v2'``, ``'dvc-v3'``, or ``'dvc-mixed'`` to override.
    """
    repo_root = repo_root or utils.find_project_root()
    source_remote = source_remote.expanduser().resolve()
    if not source_remote.is_dir():
        raise ArchiveError(f"Source remote not found: {source_remote}")

    compress = compress if compress is not None else default_compression()
    _validate_compression(compress)
    compress = _resolve_compression(compress)

    if source_layout is None:
        source_layout = detect_source_layout(source_remote)
        print(
            f"Detected source layout: {source_layout}",
            file=sys.stderr, flush=True,
        )
    elif source_layout not in SUPPORTED_SOURCE_LAYOUTS:
        raise ArchiveError(
            f"Unsupported source layout {source_layout!r}. "
            f"Expected one of {SUPPORTED_SOURCE_LAYOUTS}."
        )

    target_manifest = manifest_path(name, repo_root=repo_root)
    if target_manifest.exists() and not (force or resume):
        raise ArchiveError(
            f"Archive manifest already exists at {target_manifest}.\n"
            f"Choose a different name, or rerun with --force / --resume."
        )

    prefix_keys, stats = scan_prefixes(
        source_remote, source_layout, progress=True,
    )
    # Before anything is built: links, subdirectories or other names inside
    # the blob tree are not archived silently.
    problems: List[str] = []
    for _key, pdir in _enumerate_prefix_dirs(source_remote, source_layout,
                                             include_links=True):
        if pdir.is_symlink():
            problems.append(f"{pdir} (link)")
        else:
            problems.extend(_prefix_entries(pdir)[1])
    if problems:
        raise ArchiveError(
            f"Refusing to archive {source_remote}: {len(problems)} entr"
            f"{'y' if len(problems) == 1 else 'ies'} in the blob tree "
            f"{'is' if len(problems) == 1 else 'are'} not plain DVC objects:\n  "
            + '\n  '.join(problems[:20])
            + ('\n  ...' if len(problems) > 20 else '')
            + "\nRemove or replace them (a link is usually left by an older tool), "
              "then retry.")
    extras = scan_extras(source_remote, source_layout, progress=True)
    total_objects = sum(n for n, _ in stats.values())
    total_bytes = sum(b for _, b in stats.values())

    if extras:
        print(
            f"warning: {len(extras)} file(s) in {source_remote} are outside "
            f"the DVC blob layout and will NOT be archived:",
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
    staging = staging_root / require_plain_name(name)
    _check_jobfs_headroom(staging_root, total_bytes, force=force, verbose=verbose)

    bdir = backend_dir or _default_backend_dir(name, source_remote)
    if not bdir.endswith('/'):
        bdir = bdir + '/'
    n_jobs = jobs if jobs and jobs > 0 else default_stage_jobs()

    if dry_run:
        print(f"[dry-run] would stage:   {source_remote}")
        print(f"[dry-run] source layout: {source_layout}")
        print(f"[dry-run] prefixes:      {len(prefix_keys)}")
        print(f"[dry-run] objects:       {total_objects}")
        print(f"[dry-run] total size:    {utils.format_size(total_bytes)}")
        print(f"[dry-run] compression:   {compress}")
        print(f"[dry-run] jobs:          {n_jobs}")
        print(f"[dry-run] staging:       {staging}")
        print(f"[dry-run] backend:       {backend}")
        print(f"[dry-run] backend dir:   {bdir}")
        print(f"[dry-run] manifest:      {target_manifest}")
        manifest = ArchiveManifest(
            archive_name=name,
            source_remote=str(source_remote),
            backend=backend,
            backend_dir=bdir,
            layout=LAYOUT_FOLDER_PER_PREFIX,
            source_layout=source_layout,
            total_objects=total_objects,
            total_bytes=total_bytes,
            compression=compress,
            inner_tars={},
            extras_at_archive_time=extras,
            created_at=now_iso(),
            created_by=_current_user(),
            git_ref=_git_ref(repo_root),
            git_url=utils.strip_url_credentials(
                git_url if git_url is not None else _git_url(repo_root)),
            dt_version=_dt_version(),
        )
        return CreateResult(manifest=manifest, manifest_path=target_manifest)

    # Prepare staging dir.
    if staging.exists():
        if force:
            _require_private_staging(staging)
            shutil.rmtree(staging)
            staging.mkdir(parents=True, mode=0o700)
        elif resume:
            _require_private_staging(staging)
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
        staging.mkdir(parents=True, mode=0o700)
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
            # Only a sentinel for a prefix being archived, naming the tar dt
            # would have written for it, counts; anything else is re-staged.
            if not data or data.get('prefix') not in prefix_keys:
                continue
            _member, base = _key_to_paths(data['prefix'], source_layout)
            if data.get('filename') != f"{base}.tar{_COMPRESSION_EXT[compress]}":
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
                f"Resume: {len(resume_done)} of {len(prefix_keys)} "
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
        keys_todo = [k for k in prefix_keys if k not in resume_done]

        if via_qxub:
            # Multi-node dispatch: one qxub job per prefix. The orchestrator
            # waits via `qxub monitor`, then assembles the manifest from the
            # sentinels each worker leaves behind.
            print(
                f"Stage (qxub): dispatching {len(keys_todo)} "
                f"prefix job(s)"
                + (
                    f" (skipping {len(resume_done)} already done)"
                    if resume_done else ""
                ),
                flush=True,
            )
            _save_qxub_job_config(
                staging, source_remote, compress, stats, source_layout,
            )
            phase1_start = time.monotonic()

            if keys_todo:
                job_ids = _submit_prefix_jobs(
                    name, staging, keys_todo, verbose=verbose,
                )
                print(
                    f"  monitoring {len(job_ids)} qxub job(s) ...",
                    flush=True,
                )
                all_ok = hpc.monitor_jobs(job_ids, verbose=verbose)
            else:
                all_ok = True

            # Collect sentinels for every key expected.
            missing: List[str] = []
            ext = _COMPRESSION_EXT[compress]
            for key in keys_todo:
                _member, base = _key_to_paths(key, source_layout)
                expected = staging / f"{base}.tar{ext}{STAGED_SENTINEL_SUFFIX}"
                data = _load_sentinel(expected) if expected.exists() else None
                if not data or data.get('filename') != f"{base}.tar{ext}":
                    missing.append(key)
                    continue
                inner_tars[key] = InnerTar(
                    filename=data['filename'],
                    size_bytes=data['size_bytes'],
                    sha256=data['sha256'],
                    n_objects=data['n_objects'],
                )

            if missing or not all_ok:
                raise ArchiveError(
                    f"qxub stage failed: {len(missing)} prefix(es) "
                    f"missing sentinels after qxub monitor: "
                    f"{', '.join(missing[:10])}"
                    + (' ...' if len(missing) > 10 else '')
                )

            phase1_elapsed = time.monotonic() - phase1_start
            print(
                f"Stage (qxub): done in {_format_duration(phase1_elapsed)}",
                flush=True,
            )
        else:
            print(
                f"Stage: building {len(keys_todo)} inner tarball(s) "
                f"with {n_jobs} worker(s); staging at {staging}"
                + (
                    f" (skipping {len(resume_done)} already done)"
                    if resume_done else ""
                ),
                flush=True,
            )
            if n_jobs == 1 and len(keys_todo) > 1:
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
                        key,
                        str(staging),
                        compress,
                        stats[key][0],
                        source_layout,
                    ): key
                    for key in keys_todo
                }
                done = len(resume_done)
                total = len(prefix_keys)
                for fut in as_completed(futures):
                    key = futures[fut]
                    try:
                        row = fut.result()
                    except Exception as e:
                        raise ArchiveError(
                            f"Worker for prefix {key} failed: {e}"
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
                            f"  [{done:>3}/{total}] {key} "
                            f"({row['n_objects']:>7} obj, {size_str:>10}) "
                            f"  t+{_format_duration(elapsed)}",
                            flush=True,
                        )
                    else:
                        print(
                            f"  [{done:>3}/{total}] {key} ({size_str})",
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
            source_layout=source_layout,
            total_objects=total_objects,
            total_bytes=total_bytes,
            compression=compress,
            inner_tars=inner_tars,
            extras_at_archive_time=extras,
            created_at=now_iso(),
            created_by=_current_user(),
            git_ref=_git_ref(repo_root),
            git_url=utils.strip_url_credentials(
                git_url if git_url is not None else _git_url(repo_root)),
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

    Reads ``.dt/archives/<name>.yaml`` (written by :func:`stage_archive`),
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
    require_plain_name(name)
    staging = staging_root / name
    if staging.is_dir():
        _require_private_staging(staging)
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
        # Rewrite it first so a manifest staged by an older dt goes up with
        # its origin URL cleaned.
        save_manifest(manifest, repo_root=repo_root)
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

        # Record in the central register if configured. Best-effort —
        # never let a register write failure surface as an archive
        # failure since the canonical record is the per-project manifest.
        registered = _registry.record_created(manifest, repo_root)
        if registered is not None and verbose:
            print(f"Deposit: recorded in {registered}", flush=True)

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
    source_layout: Optional[str] = None,
    dry_run: bool = False,
    force: bool = False,
    resume: bool = False,
    via_qxub: bool = False,
    git_url: Optional[str] = None,
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
        source_layout=source_layout,
        dry_run=dry_run,
        force=force,
        resume=resume,
        via_qxub=via_qxub,
        git_url=git_url,
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
    """Return all manifests under ``.dt/archives/`` (and legacy
    ``.dvc/archives/``)."""
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

    result = VerifyResult(
        archive_name=name,
        backend=manifest.backend,
        backend_dir=bdir,
        sidecar_ok=sidecar_ok,
        files_ok=files_ok,
        deep_ok=deep_ok,
        messages=messages,
    )
    repo = repo_root or utils.find_project_root()
    _registry.record_verified(name, repo, result.ok, now_iso())
    return result


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
    to_path: Optional[Path] = None,
    *,
    object_hash: Optional[str] = None,
    prefix: Optional[str] = None,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
    verbose: bool = False,
    confirm_callback=None,
) -> List[Path]:
    """Restore from an archive.

    Modes:
    - ``object_hash`` set: extract just that md5 from the matching prefix.
    - ``prefix`` set: extract all objects in that prefix.
    - neither set: full restore (downloads every inner tar and extracts).

    If ``to_path`` is not given, defaults to the manifest's recorded
    ``source_remote`` — i.e. restores the data back to the directory it
    came from. Useful after :func:`prune_archive` left an
    ``ARCHIVED.yaml`` signpost there. That path comes from the manifest,
    so the user has to type it back before anything is written there.

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

    if to_path is None:
        if not manifest.source_remote:
            raise ArchiveError(
                f"Archive '{name}' has no source_remote recorded — "
                f"pass --to <path> explicitly."
            )
        to_path = _manifest_remote_path(manifest, 'restore into')
        _require_project_remote(to_path, repo_root, 'restore into')
        _refuse_project_cache(to_path, repo_root, 'restore into')
        _confirm_named_target(
            name, to_path, 'restore into', confirm_callback,
            hint="pass --to <path> to choose where to restore")
        if verbose:
            print(
                f"  no --to given; restoring to manifest.source_remote: "
                f"{to_path}",
                flush=True,
            )
    to_path = to_path.expanduser().resolve()
    to_path.mkdir(parents=True, exist_ok=True)

    if object_hash:
        target_prefix = object_hash[:2]
        return _restore_single_object(
            manifest, be, to_path, target_prefix, object_hash, verbose=verbose,
        )
    if prefix:
        return _restore_prefix(manifest, be, to_path, prefix, verbose=verbose)

    written = _restore_full(manifest, be, to_path, verbose=verbose)

    # The data is back — the "this remote is archived" signpost at the
    # restore root is no longer accurate, so clear it. Partial restores
    # leave it alone (the archive is still the canonical copy of
    # everything the user didn't ask for).
    stale_signpost = to_path / _signpost.SIGNPOST_FILENAME
    try:
        root_fd = os.open(str(to_path), _OPEN_DIR)
    except OSError:
        return written
    try:
        st = os.stat(_signpost.SIGNPOST_FILENAME, dir_fd=root_fd,
                     follow_symlinks=False)
        if stat.S_ISREG(st.st_mode):
            os.unlink(_signpost.SIGNPOST_FILENAME, dir_fd=root_fd)
            if verbose:
                print(f"  removed stale signpost {stale_signpost}", flush=True)
    except OSError:
        pass
    finally:
        os.close(root_fd)
    return written


def _inner_tar(manifest: ArchiveManifest, prefix: str) -> InnerTar:
    inner = manifest.inner_tars.get(prefix)
    if inner is None:
        raise ArchiveError(
            f"Archive '{manifest.archive_name}' has no record of prefix {prefix}."
        )
    return inner


def _resolve_prefix_keys(
    manifest: ArchiveManifest, prefix: str,
) -> List[str]:
    """Map a user-given ``--prefix`` to one-or-more manifest keys.

    Lookup order:
      1. Exact match in ``manifest.inner_tars`` — handles both bare
         hex (``'00'``) in pure layouts and namespaced keys
         (``'v3-00'``, ``'v2-00'``) explicitly given in mixed mode.
      2. Mixed-layout convenience: a bare hex prefix expands to
         ``['v3-XX', 'v2-XX']`` (whichever keys actually exist),
         so ``--prefix 00`` restores both halves without the user
         needing to know the namespace.

    Raises :class:`ArchiveError` if nothing matches.
    """
    if prefix in manifest.inner_tars:
        return [prefix]
    if (manifest.source_layout == LAYOUT_DVC_MIXED
            and _is_hex_prefix(prefix.lower())):
        candidates = [f'v3-{prefix}', f'v2-{prefix}']
        matches = [k for k in candidates if k in manifest.inner_tars]
        if matches:
            return matches
    raise ArchiveError(
        f"Archive '{manifest.archive_name}' has no record of prefix "
        f"{prefix!r}. Known keys (first 10): "
        f"{sorted(manifest.inner_tars)[:10]}"
        + ("..." if len(manifest.inner_tars) > 10 else "")
    )


def _entry_path_for_object(
    object_hash: str, source_layout: str, key: str,
) -> str:
    """Return the tar member path for a single object in a given key."""
    if source_layout == LAYOUT_DVC_V3:
        return f"files/md5/{object_hash[:2]}/{object_hash[2:]}"
    if source_layout == LAYOUT_DVC_V2:
        return f"{object_hash[:2]}/{object_hash[2:]}"
    if source_layout == LAYOUT_DVC_MIXED:
        if key.startswith('v3-'):
            return f"files/md5/{object_hash[:2]}/{object_hash[2:]}"
        if key.startswith('v2-'):
            return f"{object_hash[:2]}/{object_hash[2:]}"
    raise ArchiveError(
        f"Cannot resolve tar entry path for layout={source_layout!r}, key={key!r}"
    )


def _tar_decompress_flag(compression: str) -> List[str]:
    if compression == 'gzip':
        return ['-z']
    if compression == 'zstd':
        return ['--zstd']
    return []


_BLOB_NAME_RE = None


def _extract_inner(local_tar: Path, to_path: Path, compression: str,
                   key: str, layout: str,
                   members: Optional[List[str]] = None) -> List[Path]:
    """Extract the blobs of inner tar *key* into *to_path*.

    Only regular files named like DVC objects inside that key's own prefix
    directory (``files/md5/<xx>/<hex>`` or ``<xx>/<hex>``) whose content
    matches their name are written; anything else in the tar -- other
    paths, links, devices, mismatching content -- is skipped and reported.
    An object already present is left as it is. Directories are entered
    without following links and files are written through private
    temporaries renamed into place.
    """
    import re
    import tarfile
    global _BLOB_NAME_RE
    if _BLOB_NAME_RE is None:
        _BLOB_NAME_RE = re.compile(r'[0-9a-f]{30}(\.dir)?')
    member_dir, _base = _key_to_paths(key, layout)
    dir_parts = member_dir.split('/')
    prefix = dir_parts[-1]
    if not _is_hex_prefix(prefix):
        raise ArchiveError(f"Unexpected prefix {prefix!r} in archive key {key!r}")
    if compression not in _COMPRESSION_EXT:
        raise ArchiveError(f"Unknown compression {compression!r}")
    allowed_dirs = {'/'.join(dir_parts[:i]) for i in range(1, len(dir_parts) + 1)}
    wanted = set(members) if members else None

    proc = None
    if compression == 'zstd':
        try:
            proc = subprocess.Popen(['zstd', '-dc', '--', str(local_tar)],
                                    stdout=subprocess.PIPE)
        except OSError as e:
            raise ArchiveError(f"Cannot run zstd for {local_tar.name}: {e}")
    written: List[Path] = []
    skipped: List[str] = []
    dir_fd = None
    ok = False
    try:
        if proc is not None:
            tf = tarfile.open(fileobj=proc.stdout, mode='r|')
        else:
            tf = tarfile.open(str(local_tar),
                              mode='r|gz' if compression == 'gzip' else 'r|')
        with tf:
            for m in tf:
                name = m.name.rstrip('/')
                if m.isdir() and name in allowed_dirs:
                    continue
                parent, _, leaf = name.rpartition('/')
                if not (m.isreg() and parent == member_dir
                        and _BLOB_NAME_RE.fullmatch(leaf)):
                    skipped.append(f"{m.name} (not an object of prefix {prefix})")
                    continue
                if wanted is not None and name not in wanted:
                    continue
                if dir_fd is None:
                    dir_fd = _open_blob_dir(to_path, dir_parts)
                expected = prefix + leaf[:30]
                v2 = layout == LAYOUT_DVC_V2 or key.startswith('v2-')
                result = _write_blob(dir_fd, leaf, tf.extractfile(m), m, expected,
                                     dos2unix=v2)
                if result == 'mismatch':
                    skipped.append(f"{m.name} (content does not match its name)")
                elif result == 'written':
                    written.append(to_path / name)
                if wanted is not None:
                    wanted.discard(name)
                    if not wanted:
                        break
        ok = True
    except (tarfile.TarError, OSError) as e:
        raise ArchiveError(f"tar extraction failed for {local_tar.name}: {e}")
    finally:
        if dir_fd is not None:
            os.close(dir_fd)
        if proc is not None:
            proc.stdout.close()
            if ok:
                if proc.wait() != 0 and wanted is None:
                    raise ArchiveError(f"zstd failed for {local_tar.name}")
            else:
                proc.kill()
                proc.wait()
    if skipped:
        print(f"Warning: {local_tar.name}: skipped {len(skipped)} entr"
              f"{'y' if len(skipped) == 1 else 'ies'}:\n  "
              + '\n  '.join(skipped[:20])
              + ('\n  ...' if len(skipped) > 20 else ''), file=sys.stderr)
    return written


def _open_blob_dir(to_path: Path, parts: List[str]) -> int:
    """Open ``to_path/<parts>``, creating missing parts, never following a link."""
    flags = os.O_RDONLY | os.O_DIRECTORY
    fd = os.open(to_path, flags | os.O_NOFOLLOW)
    try:
        for part in parts:
            try:
                os.mkdir(part, 0o777, dir_fd=fd)
            except FileExistsError:
                pass
            nfd = os.open(part, flags | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = nfd
        return fd
    except Exception:
        os.close(fd)
        raise


def _current_umask() -> int:
    try:
        with open('/proc/self/status') as f:
            for line in f:
                if line.startswith('Umask:'):
                    return int(line.split()[1], 8)
    except (OSError, ValueError):
        pass
    return 0o022


def _write_blob(dir_fd: int, name: str, fobj, member, expected_md5: str,
                dos2unix: bool = False) -> str:
    """Write one object; ``'written'``, ``'exists'`` or ``'mismatch'``.

    The bytes go to a private temporary file and are hashed on the way; only
    content that matches the object's name is renamed into place, and an
    object already there is never replaced. With *dos2unix* (DVC 2 objects)
    content whose CRLF-normalised md5 matches also counts.
    """
    import hashlib
    import secrets
    try:
        os.lstat(name, dir_fd=dir_fd)
        return 'exists'
    except FileNotFoundError:
        pass
    tmp = f'.{name}.{secrets.token_hex(8)}.tmp'
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                 0o600, dir_fd=dir_fd)
    try:
        digest = hashlib.md5()
        d2u = hashlib.md5() if dos2unix else None
        carry = b''
        try:
            while True:
                chunk = fobj.read(1 << 20)
                if not chunk:
                    break
                digest.update(chunk)
                if d2u is not None:
                    data = carry + chunk
                    carry = b'\r' if data.endswith(b'\r') else b''
                    if carry:
                        data = data[:-1]
                    d2u.update(data.replace(b'\r\n', b'\n'))
                view = memoryview(chunk)
                while view:
                    view = view[os.write(fd, view):]
            if d2u is not None and carry:
                d2u.update(carry)
            os.fchmod(fd, 0o444 & ~_current_umask())
            os.utime(fd, (member.mtime, member.mtime))
        finally:
            os.close(fd)
        if expected_md5 not in (digest.hexdigest(),
                                (d2u.hexdigest() if d2u is not None else None)):
            os.unlink(tmp, dir_fd=dir_fd)
            return 'mismatch'
        try:
            os.link(tmp, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd,
                    follow_symlinks=False)
        except FileExistsError:
            return 'exists'
        finally:
            os.unlink(tmp, dir_fd=dir_fd)
        return 'written'
    except BaseException:
        try:
            os.unlink(tmp, dir_fd=dir_fd)
        except OSError:
            pass
        raise


def _require_tar_digest(local: Path, inner: InnerTar) -> None:
    """A downloaded inner tar must be the one the manifest recorded."""
    if inner.sha256 and sha256_of_file(local) != inner.sha256:
        raise ArchiveError(
            f"{local.name} does not match the checksum recorded in the manifest")


def _manifest_remote_path(manifest: ArchiveManifest, action: str) -> Path:
    """The manifest's ``source_remote`` as the one absolute, resolved path
    that every later check and file operation uses."""
    raw = os.path.expanduser(str(manifest.source_remote or ''))
    if not raw or not os.path.isabs(raw):
        raise ArchiveError(
            f"Refusing to {action} {manifest.source_remote!r}: the archive "
            f"manifest's source_remote is not an absolute path.")
    return Path(os.path.realpath(raw))


_OPEN_DIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, 'O_CLOEXEC', 0)


def _dir_identity(st) -> Tuple[int, int]:
    if not stat.S_ISDIR(st.st_mode):
        raise ArchiveError("not a directory")
    return (st.st_dev, st.st_ino)


def _require_same_dir(path: Path, expect: Tuple[int, int]) -> None:
    """*path* must still be the directory that was checked."""
    try:
        same = _dir_identity(os.lstat(path)) == expect
    except (OSError, ArchiveError):
        same = False
    if not same:
        raise ArchiveError(
            f"Refusing to continue: {path} changed while it was being checked.")


_MAX_PRUNE_DEPTH = 8


def _remove_tree_under(root: Path, parts, expect=None) -> None:
    """Delete ``root/<parts...>`` and everything in it, reaching each
    directory through a handle opened without following links, so a link
    planted anywhere below *root* cannot redirect the delete. With
    *expect*, *root* must still be that ``(st_dev, st_ino)``."""
    *parents, last = parts
    fd = os.open(str(root), _OPEN_DIR)
    try:
        if expect is not None and _dir_identity(os.fstat(fd)) != expect:
            raise ArchiveError(
                f"Refusing to continue: {root} changed while it was being checked.")
        for part in parents:
            nfd = os.open(part, _OPEN_DIR, dir_fd=fd)
            os.close(fd)
            fd = nfd
        _remove_entry_tree(fd, last)
    finally:
        os.close(fd)


def _remove_entry_tree(parent_fd: int, name: str, depth: int = 0) -> None:
    if depth > _MAX_PRUNE_DEPTH:
        raise OSError(f"directories nested deeper than a DVC remote's at {name!r}")
    fd = os.open(name, _OPEN_DIR, dir_fd=parent_fd)
    try:
        with os.scandir(fd) as it:
            entries = [(e.name, e.is_dir(follow_symlinks=False)) for e in it]
        for entry, is_dir in entries:
            if is_dir:
                _remove_entry_tree(fd, entry, depth + 1)
            else:
                os.unlink(entry, dir_fd=fd)
    finally:
        os.close(fd)
    os.rmdir(name, dir_fd=parent_fd)


def _same_path(a, b) -> bool:
    return (os.path.realpath(os.path.expanduser(str(a)))
            == os.path.realpath(os.path.expanduser(str(b))))


def _confirm_named_target(name: str, target: Path, action: str,
                          confirm_callback, hint: str) -> None:
    """Have the user type *target* (taken from the manifest) back."""
    prompt = (f"Archive '{name}' names {target} as its source remote.\n"
              f"Type that path to {action} it: ")
    if confirm_callback is not None:
        response = confirm_callback(prompt)
    else:
        if not sys.stdin.isatty():
            raise ArchiveError(
                f"Refusing to {action} {target} without confirmation: {hint}.")
        try:
            response = input(prompt)
        except EOFError:
            response = ''
    response = (response or '').strip()
    if not response or not _same_path(response, target):
        raise ArchiveError(f"Aborted: the path typed is not {target}.")


def _refuse_project_cache(target: Path, repo_root: Optional[Path],
                          action: str) -> None:
    """*target* must not be (or hold, or sit inside) the project's own
    DVC cache."""
    root = Path(repo_root or utils.find_project_root())
    try:
        from dvc.repo import Repo
        with Repo(str(root), uninitialized=True) as repo:
            odb = repo.cache.local.path
    except Exception as exc:
        if not (root / '.dvc').exists():
            return  # not a DVC project: no cache of its own
        raise ArchiveError(
            f"Refusing to {action} {target}: could not read this project's "
            f"DVC cache location ({exc}).")
    if not odb:
        return
    cache = os.path.realpath(utils.cache_root_from_odb_path(Path(odb)))
    path = os.path.realpath(os.path.expanduser(str(target)))
    if (path == cache or path.startswith(cache + os.sep)
            or cache.startswith(path + os.sep)):
        raise ArchiveError(
            f"Refusing to {action} {target}: it overlaps this project's DVC "
            f"cache ({cache}).")


def _require_project_remote(source_remote: Path, repo_root: Optional[Path],
                            action: str) -> None:
    """*source_remote* (from a committed manifest) must be one of this
    project's configured local DVC remotes before dt writes or deletes
    there on its own say-so."""
    from .. import remote as remote_mod
    root = Path(repo_root or utils.find_project_root())
    target = os.path.realpath(os.path.expanduser(str(source_remote)))
    try:
        remotes = remote_mod.list_remotes(repo_path=root)
    except Exception:
        remotes = []
    for _name, url, *_rest in remotes:
        url = str(url)
        if url.startswith('file://'):
            url = url[len('file://'):]
        if '://' in url:
            continue
        path = Path(os.path.expanduser(url))
        if not path.is_absolute():
            path = root / '.dvc' / path
        if os.path.realpath(path) == target:
            return
    raise ArchiveError(
        f"Refusing to {action} {source_remote}: the archive manifest names it, "
        f"but it is not one of this project's DVC remotes. "
        + ("Pass --to <path> to choose where to restore." if action == 'restore into'
           else "Check the manifest and the project's remotes."))


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
            _require_tar_digest(local, inner)
            if verbose:
                print(f"  extracting {inner.filename}")
            _extract_inner(local, to_path, manifest.compression,
                           prefix, manifest.source_layout or LAYOUT_DVC_V3)
            local.unlink(missing_ok=True)
            written.append(to_path / 'files' / 'md5' / prefix)
    return written


def _restore_prefix(manifest: ArchiveManifest, be: ArchiveBackend,
                    to_path: Path, prefix: str, *, verbose: bool) -> List[Path]:
    keys = _resolve_prefix_keys(manifest, prefix)
    bdir = manifest.backend_dir.rstrip('/') + '/'
    written: List[Path] = []
    import tempfile
    with tempfile.TemporaryDirectory(prefix='dt-restore-') as tmpdir:
        tmp = Path(tmpdir)
        for key in keys:
            inner = manifest.inner_tars[key]
            remote_path = bdir + inner.filename
            local = tmp / inner.filename
            if verbose:
                print(f"  fetching {remote_path}")
            be.get_file(remote_path, local)
            _require_tar_digest(local, inner)
            if verbose:
                print(f"  extracting {inner.filename}")
            _extract_inner(local, to_path, manifest.compression,
                           key, manifest.source_layout or LAYOUT_DVC_V3)
            local.unlink(missing_ok=True)
            # Restored path depends on this key's layout.
            if (manifest.source_layout == LAYOUT_DVC_MIXED
                    and key.startswith('v2-')) \
                    or manifest.source_layout == LAYOUT_DVC_V2:
                bare = key[3:] if key.startswith('v2-') else key
                written.append(to_path / bare)
            else:
                bare = key[3:] if key.startswith('v3-') else key
                written.append(to_path / 'files' / 'md5' / bare)
    return written


def _restore_single_object(manifest: ArchiveManifest, be: ArchiveBackend,
                           to_path: Path, prefix: str, object_hash: str,
                           *, verbose: bool) -> List[Path]:
    keys = _resolve_prefix_keys(manifest, prefix)
    bdir = manifest.backend_dir.rstrip('/') + '/'
    import tempfile

    last_error: Optional[Exception] = None
    for key in keys:
        inner = manifest.inner_tars[key]
        remote_path = bdir + inner.filename
        entry = _entry_path_for_object(object_hash, manifest.source_layout, key)
        with tempfile.TemporaryDirectory(prefix='dt-restore-') as tmpdir:
            local = Path(tmpdir) / inner.filename
            if verbose:
                print(f"  fetching {remote_path}")
            be.get_file(remote_path, local)
            _require_tar_digest(local, inner)
            if verbose:
                print(f"  extracting {entry}")
            try:
                _extract_inner(
                    local, to_path, manifest.compression,
                    key, manifest.source_layout or LAYOUT_DVC_V3, members=[entry],
                )
            except ArchiveError as e:
                last_error = e
                continue
        written_path = to_path / entry
        if written_path.exists():
            return [written_path]

    raise ArchiveError(
        f"Object {object_hash} not found in any candidate inner tar "
        f"({', '.join(keys)})"
        + (f": {last_error}" if last_error else "")
    )


# --------------------------------------------------------------------------- #
# prune
# --------------------------------------------------------------------------- #

def _uncovered_prefixes(source_remote: Path, layout: str,
                        inner_tars: Dict[str, InnerTar]) -> List[str]:
    """Non-empty blob prefix directories with no matching inner tar."""
    missing: List[str] = []

    def _check(base: Path, keys: List[str], label: str) -> None:
        if not base.is_dir():
            return
        for p in sorted(base.iterdir()):
            if not (_is_hex_prefix(p.name) and p.is_dir()):
                continue
            try:
                empty = not any(os.scandir(p))
            except OSError:
                empty = False
            if not empty and not any(k.format(p.name) in inner_tars for k in keys):
                missing.append(f"{label}{p.name}")

    if layout == LAYOUT_DVC_V3:
        _check(source_remote / 'files' / 'md5', ['{}'], 'files/md5/')
    elif layout == LAYOUT_DVC_V2:
        _check(source_remote, ['{}'], '')
    elif layout == LAYOUT_DVC_MIXED:
        _check(source_remote / 'files' / 'md5', ['v3-{}'], 'files/md5/')
        _check(source_remote, ['v2-{}'], '')
    return missing


def prune_archive(
    name: str,
    *,
    yes: bool = False,
    force: bool = False,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
    confirm_callback=None,
    source: Optional[Path] = None,
) -> PruneResult:
    """Delete ``<source_remote>/files/md5/`` after verifying the archive.

    Refuses to run if:
    - The archive doesn't verify (sidecar + file existence/sizes).
    - The source remote has files outside ``files/md5/`` (extras).

    ``--force`` bypasses the extras check (NOT the verify check).
    ``--yes`` suppresses the interactive confirm. The remote's path comes
    from the manifest, so the user names it too: ``source`` (``--source``)
    must be that path, or, without it, the path is typed at a prompt that
    ``--yes`` does not skip.
    """
    manifest = load_manifest(name, repo_root=repo_root)
    source_remote = _manifest_remote_path(manifest, 'prune')
    layout = manifest.source_layout or LAYOUT_DVC_V3
    try:
        root_id = _dir_identity(os.lstat(source_remote))
    except OSError as exc:
        raise ArchiveError(f"Nothing to prune: cannot read {source_remote} ({exc})")
    _require_project_remote(source_remote, repo_root, 'prune')
    _refuse_project_cache(source_remote, repo_root, 'prune')
    if source is not None:
        if not _same_path(source, source_remote):
            raise ArchiveError(
                f"Refusing to prune: --source {source} is not the remote "
                f"archive '{name}' was made from ({source_remote}).")
    else:
        _confirm_named_target(
            name, source_remote, 'prune', confirm_callback,
            hint="pass --source <path> naming the remote to prune")

    # Identify what to delete based on the source layout.
    prune_targets: List[Path] = []
    if layout in (LAYOUT_DVC_V3, LAYOUT_DVC_MIXED):
        files_md5 = source_remote / 'files' / 'md5'
        if _real_dir(files_md5.parent) and _real_dir(files_md5):
            prune_targets.append(files_md5)
    if layout in (LAYOUT_DVC_V2, LAYOUT_DVC_MIXED):
        for p in source_remote.iterdir() if source_remote.is_dir() else []:
            if _is_hex_prefix(p.name) and _real_dir(p):
                prune_targets.append(p)

    if not prune_targets:
        raise ArchiveError(
            f"Nothing to prune: no DVC blob directories present under "
            f"{source_remote} (already pruned?)"
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

    if not manifest.inner_tars:
        raise ArchiveError(f"Refusing to prune: archive '{name}' lists no inner tars")
    # Every non-empty prefix on disk must have its own inner tar, holding
    # (at least) as many objects as are there now.
    _keys, disk_stats = scan_prefixes(source_remote, layout)
    short = [f"{k} ({n} on disk, {manifest.inner_tars[k].n_objects} archived)"
             for k, (n, _b) in sorted(disk_stats.items())
             if k in manifest.inner_tars and n > manifest.inner_tars[k].n_objects]
    if short:
        raise ArchiveError(
            f"Refusing to prune: objects on disk are not all in archive '{name}' "
            f"(pushed since it was made?): {', '.join(short[:10])}"
            f"{' ...' if len(short) > 10 else ''}")
    uncovered = _uncovered_prefixes(source_remote, layout, manifest.inner_tars)
    if uncovered:
        raise ArchiveError(
            f"Refusing to prune: {len(uncovered)} prefix director"
            f"{'y' if len(uncovered) == 1 else 'ies'} under {source_remote} "
            f"have no inner tar in archive '{name}': "
            f"{', '.join(uncovered[:10])}{' ...' if len(uncovered) > 10 else ''}")

    extras = scan_extras(source_remote, layout)
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
            f"are outside the DVC blob layout and NOT covered by archive "
            f"'{name}'.\n"
            f"  {listing}{more}\n"
            f"Resolve those files (delete, move, or rerun create with "
            f"--include-extras when implemented), or rerun prune with --force."
        )

    if not yes:
        targets_str = '\n  '.join(str(t) for t in prune_targets)
        prompt = (
            f"This will permanently delete:\n  {targets_str}\n"
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

    _, stats = scan_prefixes(source_remote, layout)
    bytes_freed = sum(b for _, b in stats.values())

    # Drop the signpost BEFORE rmtree so anyone who lists the remote
    # mid-delete sees the explanation rather than an empty-ish dir.
    _require_same_dir(source_remote, root_id)
    signpost_path = _signpost.write_signpost(
        source_remote, manifest,
        pruned_by=_current_user(),
        pruned_at=now_iso(),
    )
    print(f"Wrote signpost {signpost_path}", flush=True)

    try:
        for target in prune_targets:
            _remove_tree_under(source_remote,
                               target.relative_to(source_remote).parts,
                               expect=root_id)
    except (OSError, RecursionError) as exc:
        raise ArchiveError(
            f"Prune stopped part-way through deleting under {source_remote} "
            f"({exc}); the signpost is in place and the archive is intact. "
            f"Check the directory, then rerun prune.")
    repo = repo_root or utils.find_project_root()
    _registry.record_pruned(name, repo, now_iso())
    return PruneResult(
        archive_name=name,
        deleted_path=prune_targets[0] if len(prune_targets) == 1 else source_remote,
        bytes_freed=bytes_freed,
    )


# --------------------------------------------------------------------------- #
# destroy
# --------------------------------------------------------------------------- #

def destroy_archive(
    name: str,
    *,
    yes: bool = False,
    keep_manifest: bool = False,
    backend_override: Optional[ArchiveBackend] = None,
    repo_root: Optional[Path] = None,
    confirm_callback=None,
) -> DestroyResult:
    """Delete the archive copy from the backend.

    Use this to roll back an archive you didn't actually want (wrong
    source, empty content, mistaken name). Unlike :func:`prune_archive`,
    destroy *never* touches the source remote — only the archive copy
    on the backend, the local manifest, and the central register entry.

    Deletes happen sidecar-first so that an interrupted destroy leaves
    the archive marked incomplete on the backend (no sidecar present),
    not falsely complete with missing inner tars.

    Args:
        name: Archive name.
        yes: Skip the interactive confirmation prompt.
        keep_manifest: Don't delete ``.dt/archives/<name>.yaml`` or
            the registry entry — useful if you want to retry deposit
            after wiping the backend copy.
        backend_override: Inject a backend (used by tests).
        repo_root: Override project root.
        confirm_callback: Callable for non-interactive confirmation.
    """
    repo_root = repo_root or utils.find_project_root()
    manifest = load_manifest(name, repo_root=repo_root)
    be = backend_override if backend_override is not None else get_backend(manifest.backend)
    bdir = manifest.backend_dir.rstrip('/') + '/'

    sidecar = bdir + sidecar_name(name)
    inner_paths = [bdir + inner.filename for inner in manifest.inner_tars.values()]
    expected_bytes = sum(i.size_bytes for i in manifest.inner_tars.values())

    if not yes:
        prompt = (
            f"This will permanently delete the archive copy on the backend:\n"
            f"  {manifest.backend}:{bdir}\n"
            f"  - 1 manifest sidecar ({sidecar_name(name)})\n"
            f"  - {len(inner_paths)} inner tar(s) "
            f"(~{utils.format_size(expected_bytes)})\n"
            + (
                ""
                if keep_manifest
                else f"  - 1 local manifest (.dt/archives/{name}.yaml)\n"
                     f"  - 1 central register entry (if configured)\n"
            )
            + "\n"
            f"The SOURCE remote ({manifest.source_remote}) is NOT touched.\n"
            f"Type 'yes' to continue: "
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

    # Sidecar first: a partial destroy then leaves the archive
    # indistinguishable from one whose deposit never finished.
    files_deleted = 0
    print(f"Deleting sidecar {sidecar}", flush=True)
    try:
        be.delete_file(sidecar)
        files_deleted += 1
    except ArchiveError as e:
        print(f"  warning: {e}", file=sys.stderr, flush=True)

    for path in inner_paths:
        print(f"Deleting {path}", flush=True)
        try:
            be.delete_file(path)
            files_deleted += 1
        except ArchiveError as e:
            print(f"  warning: {e}", file=sys.stderr, flush=True)

    # Best-effort: drop the now-empty backend directory.
    try:
        be.rmdir(bdir.rstrip('/'))
    except (ArchiveError, AttributeError):
        pass

    manifest_deleted = False
    if not keep_manifest:
        local_manifest_path = manifest_path(name, repo_root=repo_root)
        if local_manifest_path.exists():
            local_manifest_path.unlink()
            manifest_deleted = True
            print(f"Deleted local manifest {local_manifest_path}", flush=True)
        slug = _registry.project_slug(repo_root)
        if _registry.delete_entry(slug, name):
            print(f"Removed registry entry for {name}", flush=True)

    return DestroyResult(
        archive_name=name,
        backend=manifest.backend,
        backend_dir=bdir,
        files_deleted=files_deleted,
        bytes_freed=expected_bytes,
        manifest_deleted=manifest_deleted,
    )
