"""Sweep abandoned ``.tmp`` files from DVC caches and remotes.

DVC uploads a blob by writing it under a random temporary name in the
destination's prefix directory and then ``os.replace``-ing it into its final
hash-named slot::

    def tmp_fname(prefix: str = "") -> str:
        return f"{prefix}.{token_urlsafe(16)}.tmp"

If the transfer dies before the rename -- an interrupted push, a killed PBS
job, a dropped connection -- the partial file is left behind forever. Nothing
in DVC ever cleans these up, and because they are dotfiles they are invisible
to a casual ``ls``. On a shared lab remote they accumulate into hundreds of
gigabytes.

Two properties make this safe to automate:

* The name shape is exact, so matching never has to guess. A loose ``*.tmp``
  glob would happily eat someone's ``notes.tmp``.
* Deleting a temp file that is still being written cannot corrupt the remote.
  The writer keeps writing to the now-unlinked inode and its final
  ``os.replace`` fails with ``ENOENT``, so the worst case is a failed transfer
  that gets rerun -- never a damaged blob.

Only files are ever removed. Empty prefix directories are deliberately left in
place: DVC recreates a missing prefix directory using the writing user's umask,
which may not grant group write, silently locking every other group member out
of that prefix.
"""

import errno
import os
import re
import stat
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import remote as remote_mod
from . import utils
from .errors import CleanError


# Exactly the shape produced by dvc_objects.fs.utils.tmp_fname(): a leading
# dot, 22 urlsafe-base64 characters from token_urlsafe(16), then '.tmp'.
TMP_RE = re.compile(r'^\.[A-Za-z0-9_-]{22}\.tmp$')

# Transfers are given a generous grace period. Anything younger is reported but
# never touched, since a long copy on a data-mover node can legitimately run for
# hours and its mtime advances only as blocks land.
DEFAULT_MIN_AGE_DAYS = 7

KIND_REMOTE = 'remote'
KIND_CACHE = 'cache'


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class TmpFile:
    """One abandoned temporary file."""
    path: Path
    rel: str
    size: int
    mtime: float
    age_days: float
    owner_uid: int
    owner: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            'path': str(self.path),
            'rel': self.rel,
            'size': self.size,
            'age_days': round(self.age_days, 2),
            'owner': self.owner,
        }


@dataclass
class DeleteFailure:
    """A temp file we could not remove, and why."""
    tmp: TmpFile
    reason: str
    errno_code: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {**self.tmp.to_dict(),
                'reason': self.reason, 'errno': self.errno_code}


@dataclass
class SweepReport:
    """Outcome of sweeping one cache or remote."""
    root: Path
    kind: str
    name: str = ''
    layout: str = ''
    prefixes_scanned: int = 0
    candidates: List[TmpFile] = field(default_factory=list)
    too_recent: List[TmpFile] = field(default_factory=list)
    deleted: List[TmpFile] = field(default_factory=list)
    failed: List[DeleteFailure] = field(default_factory=list)
    skipped_changed: List[TmpFile] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def bytes_candidate(self) -> int:
        return sum(t.size for t in self.candidates)

    @property
    def bytes_reclaimed(self) -> int:
        return sum(t.size for t in self.deleted)

    @property
    def bytes_failed(self) -> int:
        return sum(f.tmp.size for f in self.failed)

    def by_owner(self, items: Sequence[TmpFile]) -> Dict[str, Tuple[int, int]]:
        """``{owner: (count, bytes)}`` for the given files."""
        out: Dict[str, Tuple[int, int]] = {}
        for t in items:
            n, b = out.get(t.owner, (0, 0))
            out[t.owner] = (n + 1, b + t.size)
        return out

    def to_dict(self) -> Dict[str, Any]:
        return {
            'root': str(self.root),
            'kind': self.kind,
            'name': self.name,
            'layout': self.layout,
            'prefixes_scanned': self.prefixes_scanned,
            'error': self.error,
            'candidates': {
                'count': len(self.candidates),
                'bytes': self.bytes_candidate,
                'by_owner': {o: {'count': n, 'bytes': b}
                             for o, (n, b) in
                             sorted(self.by_owner(self.candidates).items())},
            },
            'too_recent': {'count': len(self.too_recent),
                           'bytes': sum(t.size for t in self.too_recent)},
            'deleted': {'count': len(self.deleted),
                        'bytes': self.bytes_reclaimed},
            'skipped_changed': [t.to_dict() for t in self.skipped_changed],
            'failed': [f.to_dict() for f in self.failed],
        }


# =============================================================================
# Helpers
# =============================================================================

def _fmt_size(n: int) -> str:
    """Format a byte count, keeping a unit on zero.

    ``utils.format_size(0)`` yields a bare ``'0'``, which reads as a missing
    value in a size column. Zero-byte temp files are common (a transfer that
    died immediately), so they show up often enough to matter.
    """
    return '0B' if n == 0 else utils.format_size(n)


def _username(uid: int) -> str:
    """Resolve a uid to a login name, falling back to the numeric id."""
    try:
        import pwd
        return pwd.getpwuid(uid).pw_name
    except (KeyError, ImportError):
        return str(uid)


def is_tmp_name(name: str) -> bool:
    """True if ``name`` is exactly a DVC temporary-transfer filename."""
    return bool(TMP_RE.match(name))


def _scan_prefix(
    pdir: Path,
    root: Path,
    min_age_days: float,
    now: float,
) -> Tuple[List[TmpFile], List[TmpFile]]:
    """Return ``(old_enough, too_recent)`` temp files in one prefix directory."""
    old: List[TmpFile] = []
    recent: List[TmpFile] = []
    try:
        entries = list(os.scandir(pdir))
    except OSError:
        return old, recent

    for entry in entries:
        if not is_tmp_name(entry.name):
            continue
        try:
            # Never follow symlinks: stat the entry itself, since that is what
            # would be unlinked.
            st = entry.stat(follow_symlinks=False)
        except OSError:
            continue
        if not stat.S_ISREG(st.st_mode) and not stat.S_ISLNK(st.st_mode):
            continue

        path = Path(entry.path)
        try:
            rel = str(path.relative_to(root))
        except ValueError:
            rel = str(path)

        age_days = (now - st.st_mtime) / 86400.0
        tmp = TmpFile(
            path=path,
            rel=rel,
            size=st.st_size,
            mtime=st.st_mtime,
            age_days=age_days,
            owner_uid=st.st_uid,
            owner=_username(st.st_uid),
        )
        (old if age_days >= min_age_days else recent).append(tmp)

    return old, recent


# =============================================================================
# Scanning
# =============================================================================

def scan(
    root: Path,
    kind: str = KIND_REMOTE,
    name: str = '',
    min_age_days: float = DEFAULT_MIN_AGE_DAYS,
    jobs: int = 8,
    now: Optional[float] = None,
) -> SweepReport:
    """Find abandoned temp files under a cache or remote root.

    Only blob prefix directories are walked, so nothing outside DVC's own
    content-addressed layout is ever considered -- the verify ledger and
    quarantine areas are not prefix directories and are skipped implicitly.

    Args:
        root: Cache or remote directory.
        kind: ``remote`` or ``cache``, for reporting.
        name: Display name (e.g. the DVC remote name).
        min_age_days: Files younger than this are reported but not returned as
            deletion candidates.
        jobs: Concurrent prefix scanners.
        now: Reference time, injectable for testing.

    Returns:
        A :class:`SweepReport` with candidates populated and nothing deleted.
    """
    from .archive import operations as ops
    from .errors import ArchiveError

    root = Path(root)
    report = SweepReport(root=root, kind=kind, name=name)

    if not root.is_dir():
        report.error = f"not a directory: {root}"
        return report

    try:
        layout = ops.detect_source_layout(root)
    except ArchiveError as e:
        report.error = str(e)
        return report

    report.layout = layout
    entries = ops._enumerate_prefix_dirs(root, layout)
    report.prefixes_scanned = len(entries)

    if now is None:
        now = time.time()

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        results = list(pool.map(
            lambda item: _scan_prefix(item[1], root, min_age_days, now),
            entries,
        ))

    for old, recent in results:
        report.candidates.extend(old)
        report.too_recent.extend(recent)

    report.candidates.sort(key=lambda t: -t.size)
    return report


# =============================================================================
# Deletion
# =============================================================================

def delete(report: SweepReport, verbose: bool = False) -> SweepReport:
    """Remove the candidates recorded in ``report``, in place.

    Each file is re-stat'ed immediately before removal and skipped if its mtime
    moved since the scan, which closes the (already remote) window where a
    transfer resumed mid-sweep.

    Permissions are discovered by attempting the removal rather than by
    pre-computing them: whether a file can be unlinked depends on write and
    execute permission on its *containing directory*, not on ownership of the
    file, and a pre-flight check would both race and misread ACLs.
    """
    for tmp in report.candidates:
        try:
            st = os.lstat(tmp.path)
        except FileNotFoundError:
            continue  # someone else cleaned it up; nothing to report
        except OSError as e:
            report.failed.append(DeleteFailure(
                tmp, f"could not stat: {e.strerror}",
                errno.errorcode.get(e.errno),
            ))
            continue

        if st.st_mtime != tmp.mtime:
            # Written since the scan -- a transfer may be live again.
            report.skipped_changed.append(tmp)
            if verbose:
                print(f"  skip (changed since scan): {tmp.rel}")
            continue

        try:
            os.unlink(tmp.path)
        except FileNotFoundError:
            continue
        except OSError as e:
            reason = e.strerror or 'unknown error'
            if e.errno in (errno.EACCES, errno.EPERM):
                reason = 'permission denied'
            report.failed.append(DeleteFailure(
                tmp, reason, errno.errorcode.get(e.errno),
            ))
            continue

        report.deleted.append(tmp)
        if verbose:
            print(f"  removed {tmp.rel} "
                  f"({_fmt_size(tmp.size)}, {tmp.owner})")

    return report


def sweep(
    root: Path,
    kind: str = KIND_REMOTE,
    name: str = '',
    min_age_days: float = DEFAULT_MIN_AGE_DAYS,
    do_delete: bool = False,
    jobs: int = 8,
    verbose: bool = False,
) -> SweepReport:
    """Scan a root and, if ``do_delete``, remove what it found."""
    report = scan(root, kind=kind, name=name,
                  min_age_days=min_age_days, jobs=jobs)
    if do_delete and not report.error:
        delete(report, verbose=verbose)
    return report


# =============================================================================
# Target resolution
# =============================================================================

def looks_like_dvc_store(path: Path) -> bool:
    """True if ``path`` holds a DVC blob layout."""
    from .archive import operations as ops
    from .errors import ArchiveError
    try:
        ops.detect_source_layout(path)
        return True
    except (ArchiveError, OSError):
        return False


def label_stores(stores: Sequence[Path]) -> List[Tuple[str, Path]]:
    """Name each store as briefly as stays unambiguous.

    Store names collide across roots -- one site has 18 names appearing under
    more than one root, some under three -- so a bare name would quietly merge
    distinct remotes in the reader's mind. Qualifying everything with its root
    is not enough either, since two roots can share a basename
    (``a56/dvc/analysis`` and ``px14/dvc/analysis``).

    So: use the bare name where it is unique, else add parent directories one
    at a time until it is, falling back to the full path.
    """
    labels: Dict[Path, str] = {}
    remaining = list(stores)

    for depth in range(0, 4):
        if not remaining:
            break
        candidates: Dict[str, List[Path]] = {}
        for p in remaining:
            parts = p.parts[-(depth + 1):]
            candidates.setdefault('/'.join(parts), []).append(p)
        still: List[Path] = []
        for label, paths in candidates.items():
            if len(paths) == 1:
                labels[paths[0]] = label
            else:
                still.extend(paths)
        remaining = still

    for p in remaining:  # pathological: fall back to the full path
        labels[p] = str(p)

    return [(labels[p], p) for p in stores]


def enumerate_stores(roots: Sequence[Path]) -> List[Tuple[str, Path]]:
    """Find every DVC store across the given roots.

    A root that is missing or holds nothing contributes nothing rather than
    aborting the run: with several roots configured, one stale entry should not
    stop the others being swept.
    """
    stores: List[Path] = []
    seen = set()
    for base in roots:
        if not base.is_dir():
            continue
        try:
            children = sorted(base.iterdir())
        except OSError:
            continue
        for child in children:
            if not child.is_dir():
                continue
            resolved = child.resolve()
            if resolved in seen:
                continue
            if looks_like_dvc_store(child):
                seen.add(resolved)
                stores.append(child)

    if not stores:
        listed = ', '.join(str(r) for r in roots)
        raise CleanError(f"No DVC remotes found under: {listed}")

    return label_stores(stores)


def resolve_remote_targets(
    remote_name: Optional[str] = None,
    path: Optional[str] = None,
    all_remotes: bool = False,
    root: Optional[Sequence[str]] = None,
) -> List[Tuple[str, Path]]:
    """Work out which remote directories to sweep.

    Returns:
        ``[(display_name, directory), ...]``.

    Raises:
        CleanError: if nothing can be resolved, with a message saying how to
            supply what is missing.
    """
    if path:
        return [(str(path), Path(path).resolve())]

    if all_remotes:
        roots = remote_mod.remote_roots(root)
        if not roots:
            raise CleanError(
                "--all needs a remote root, and remote.root is not configured.\n"
                "Either pass --root /path/to/remotes or set it:\n"
                "  dt config set remote.root /path/to/remotes"
            )
        return enumerate_stores(roots)

    # Default: the current repo's remote(s).

    remotes = remote_mod.list_remotes()
    if not remotes:
        raise CleanError(
            "No DVC remotes configured in this repository.\n"
            "Use --path to sweep a directory directly."
        )

    if remote_name:
        match = [(n, u) for n, u, _d in remotes if n == remote_name]
        if not match:
            names = ', '.join(n for n, _u, _d in remotes) or 'none'
            raise CleanError(
                f"No remote named '{remote_name}'. Available: {names}"
            )
        name, url = match[0]
    else:
        default = [(n, u) for n, u, d in remotes if d]
        name, url = (default or [(remotes[0][0], remotes[0][1])])[0]

    local = remote_mod.extract_local_path(url)
    if not local:
        raise CleanError(
            f"Remote '{name}' ({url}) is not on a locally reachable "
            f"filesystem.\n"
            f"Cloud remotes store partial uploads as multipart uploads rather "
            f"than stray files; clear those with a bucket lifecycle rule.\n"
            f"For a remote on another host, run dt remote clean there, or pass "
            f"--path if it is mounted locally."
        )
    return [(name, Path(local).resolve())]


def _cache_store_root(path: Path) -> Path:
    """Normalise a cache path to the directory holding the blob layout.

    ``utils.get_cache_dir()`` returns DVC's local odb path, which is already
    ``<cache>/files/md5``. Stepping up to its parent lets layout detection see
    the same v3 structure it sees on a remote, instead of reading the bare
    prefix directories as a v2 layout.
    """
    if path.name == 'md5' and path.parent.name == 'files':
        return path.parent.parent
    return path


def resolve_cache_target(path: Optional[str] = None) -> Tuple[str, Path]:
    """Work out which cache directory to sweep."""
    if path:
        return (str(path), _cache_store_root(Path(path).resolve()))

    cache_dir = utils.get_cache_dir()
    if not cache_dir:
        raise CleanError(
            "Could not determine the DVC cache directory.\n"
            "Run from inside a DVC repository, or pass --path."
        )
    resolved = _cache_store_root(Path(cache_dir).resolve())
    return (resolved.name, resolved)


# =============================================================================
# Reporting
# =============================================================================

def has_findings(report: SweepReport) -> bool:
    """True if this root is worth printing at all."""
    return bool(
        report.candidates or report.too_recent or report.error
        or report.deleted or report.failed
    )


def format_report(
    report: SweepReport,
    deleted_mode: bool,
    verbose: bool = False,
) -> str:
    """Render one sweep as human-readable text.

    Returns an empty string for a clean root unless ``verbose``, so that
    sweeping a remote root with a hundred healthy remotes does not bury the few
    that need attention.
    """
    if not has_findings(report) and not verbose:
        return ''

    lines: List[str] = []
    label = report.name or str(report.root)
    lines.append(f"{report.kind}: {label}")
    lines.append(f"  {report.root}")

    if report.error:
        lines.append(f"  skipped: {report.error}")
        return "\n".join(lines)

    if not report.candidates and not report.too_recent:
        lines.append("  no abandoned .tmp files found")
        return "\n".join(lines)

    if report.candidates:
        n = len(report.candidates)
        lines.append(
            f"  {n} abandoned .tmp file{'s' if n != 1 else ''}, "
            f"{_fmt_size(report.bytes_candidate)}"
        )
        owners = report.by_owner(report.candidates)
        if len(owners) > 1 or not deleted_mode:
            for owner, (count, size) in sorted(
                owners.items(), key=lambda kv: -kv[1][1]
            ):
                lines.append(
                    f"      {owner:<12} {count:>5} file"
                    f"{'s' if count != 1 else ' '}  "
                    f"{_fmt_size(size)}"
                )

    if verbose:
        for tmp in report.candidates:
            lines.append(f"      {tmp.rel}  "
                         f"{_fmt_size(tmp.size)}  {tmp.owner}  "
                         f"{tmp.age_days:.0f}d")

    if report.too_recent:
        n = len(report.too_recent)
        lines.append(
            f"  {n} newer than the age limit, left alone "
            f"({_fmt_size(sum(t.size for t in report.too_recent))})"
        )

    if deleted_mode:
        lines.append(
            f"  removed {len(report.deleted)}, "
            f"reclaimed {_fmt_size(report.bytes_reclaimed)}"
        )
        if report.skipped_changed:
            lines.append(
                f"  {len(report.skipped_changed)} skipped: written to since "
                f"the scan (transfer may be live)"
            )
        if report.failed:
            lines.append(f"  {len(report.failed)} could not be removed "
                         f"({_fmt_size(report.bytes_failed)})")
            lines.append(_format_failures(report))

    return "\n".join(lines)


def _format_failures(report: SweepReport) -> str:
    """Group failures by directory, since that is what governs deletion."""
    by_dir: Dict[Path, List[DeleteFailure]] = {}
    for f in report.failed:
        by_dir.setdefault(f.tmp.path.parent, []).append(f)

    lines: List[str] = []
    for d, items in sorted(by_dir.items()):
        try:
            st = os.stat(d)
            mode = stat.filemode(st.st_mode)
            dir_owner = _username(st.st_uid)
        except OSError:
            mode, dir_owner = '?', '?'
        size = sum(i.tmp.size for i in items)
        lines.append(
            f"      {d}  {mode}  dir owner: {dir_owner}"
        )
        lines.append(
            f"        {len(items)} file(s), {_fmt_size(size)} "
            f"-- {items[0].reason}"
        )

    owners: Dict[str, int] = {}
    for f in report.failed:
        owners[f.tmp.owner] = owners.get(f.tmp.owner, 0) + 1
    if owners:
        summary = ', '.join(f"{o} {n}" for o, n in sorted(owners.items()))
        lines.append(f"      files owned by: {summary}")
    return "\n".join(lines)


def format_summary(
    reports: Sequence[SweepReport],
    deleted_mode: bool,
    min_age_days: float,
) -> str:
    """Render the totals across every swept root."""
    total_files = sum(len(r.candidates) for r in reports)
    total_bytes = sum(r.bytes_candidate for r in reports)
    n_roots = len(reports)
    lines: List[str] = ['']

    if not total_files and not deleted_mode:
        first = reports[0]
        if n_roots != 1:
            scope = f"{n_roots} {first.kind}s"
        elif first.name and first.name != first.kind:
            scope = f"{first.kind} {first.name}"
        else:
            scope = f"{first.kind} {first.root}"
        skipped = sum(1 for r in reports if r.error)
        line = f"No abandoned .tmp files older than {min_age_days:g} days in {scope}"
        if skipped:
            line += f" ({skipped} skipped: no DVC layout)"
        lines.append(line)
        return "\n".join(lines)

    if deleted_mode:
        removed = sum(len(r.deleted) for r in reports)
        reclaimed = sum(r.bytes_reclaimed for r in reports)
        failed = sum(len(r.failed) for r in reports)
        lines.append(
            f"Removed {removed} file{'s' if removed != 1 else ''}, "
            f"reclaimed {_fmt_size(reclaimed)}"
        )
        if failed:
            lines.append(
                f"{failed} file{'s' if failed != 1 else ''} could not be "
                f"removed ({_fmt_size(sum(r.bytes_failed for r in reports))})"
            )
    else:
        lines.append(
            f"Found {total_files} abandoned .tmp file"
            f"{'s' if total_files != 1 else ''} older than "
            f"{min_age_days:g} day{'s' if min_age_days != 1 else ''}, "
            f"{_fmt_size(total_bytes)}"
        )
        if total_files:
            owners: Dict[str, Tuple[int, int]] = {}
            for r in reports:
                for owner, (n, b) in r.by_owner(r.candidates).items():
                    on, ob = owners.get(owner, (0, 0))
                    owners[owner] = (on + n, ob + b)
            lines.append('')
            lines.append('By owner:')
            for owner, (n, b) in sorted(owners.items(), key=lambda kv: -kv[1][1]):
                lines.append(f"  {owner:<12} {n:>5} file"
                             f"{'s' if n != 1 else ' '}  "
                             f"{_fmt_size(b)}")
            lines.append('')
            lines.append('Nothing was deleted. Re-run with --delete to remove them.')

    return "\n".join(lines)
