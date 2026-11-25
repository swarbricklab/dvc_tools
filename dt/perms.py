"""Check and repair directory permissions on shared DVC caches and remotes.

A shared store only works if its blob directories stay group-writable and
setgid. Two things erode that:

* ``dt remote init`` / ``dt cache init`` pre-create all 256 prefix directories
  precisely so DVC never has to. Where that has not run, DVC creates each
  prefix on demand under the writing user's umask, which frequently leaves it
  unwritable by the rest of the group.
* An empty prefix directory that gets removed is recreated the same way, with
  the same result.

Either way the damage is silent until somebody else's push fails.

By default the directories also get the sticky bit. On a directory it means a
file may be removed or renamed only by the file's owner, the directory's owner,
or root -- so everyone can still create objects and push, but nobody can delete
another user's data by accident. Creation is unaffected. World access is off by
default too, so shared research data is not readable outside the owning group.

Only *missing* bits count as deviations, so a directory more restrictive than
the policy is left alone: ``--allow-other`` tolerates world-readable
directories but never grants world access to one that lacks it. Loosening
permissions is not something a repair tool should do silently.

One asymmetry drives the whole design: ``unlink`` is governed by write
permission on the containing *directory*, but ``chmod`` is governed by
*ownership*. So a group member can delete another user's blob yet cannot repair
another user's directory. Repair is therefore inherently per-owner, and the
report is grouped that way -- its job is to tell each person what only they can
run.
"""

import errno
import os
import stat
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Directory modes. setgid keeps the group on newly created entries; the sticky
# bit restricts deletion to owners without touching create permission.
MODE_SHARED = 0o2775          # setgid, group write, world read/execute
MODE_SHARED_STICKY = 0o3775   # ... plus sticky
MODE_PRIVATE = 0o2770         # no access for others
MODE_PRIVATE_STICKY = 0o3770

# The full set of blob prefix directories a store should hold.
EXPECTED_PREFIXES = tuple(f'{i:02x}' for i in range(256))

KIND_REMOTE = 'remote'
KIND_CACHE = 'cache'


# Defaults. Sticky is on so that everyone can push but nobody can delete
# another user's data by accident, and world access is off so shared research
# data is not readable outside the owning group.
DEFAULT_STICKY = True
DEFAULT_ALLOW_OTHER = False


def wanted_mode(
    sticky: bool = DEFAULT_STICKY,
    allow_other: bool = DEFAULT_ALLOW_OTHER,
) -> int:
    """The directory mode implied by a policy."""
    if allow_other:
        return MODE_SHARED_STICKY if sticky else MODE_SHARED
    return MODE_PRIVATE_STICKY if sticky else MODE_PRIVATE


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class DirFinding:
    """A directory whose mode deviates from the policy."""
    path: Path
    rel: str
    current_mode: int
    wanted: int
    owner_uid: int
    owner: str
    issues: List[str] = field(default_factory=list)
    # identity of the inspected directory and its location relative to the
    # store root, so fix() reaches and changes exactly it
    dev: int = 0
    ino: int = 0
    rel_parts: Tuple[str, ...] = ()
    # populated by fix()
    fixed: bool = False
    failure: Optional[str] = None

    @property
    def mine(self) -> bool:
        return self.owner_uid == os.geteuid()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'path': str(self.path),
            'rel': self.rel,
            'current': oct(self.current_mode),
            'current_symbolic': stat.filemode(stat.S_IFDIR | self.current_mode),
            'wanted': oct(self.wanted),
            'owner': self.owner,
            'issues': self.issues,
            'fixed': self.fixed,
            'failure': self.failure,
        }


@dataclass
class MissingPrefix:
    """A blob prefix directory that does not exist yet."""
    path: Path
    name: str
    created: bool = False
    failure: Optional[str] = None
    base_parts: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {'path': str(self.path), 'name': self.name,
                'created': self.created, 'failure': self.failure}


@dataclass
class PermsReport:
    """Permission state of one cache or remote."""
    root: Path
    kind: str
    name: str = ''
    wanted: int = MODE_PRIVATE_STICKY
    dirs_checked: int = 0
    findings: List[DirFinding] = field(default_factory=list)
    missing: List[MissingPrefix] = field(default_factory=list)
    # Bases holding none of the 256 prefixes: never pre-created, rather than
    # drifted. Usually just a remote nobody has pushed to yet.
    uninitialised: List[Path] = field(default_factory=list)
    # Symbolic links where store directories are expected; never followed.
    links: List[Path] = field(default_factory=list)
    # Directories that could not be inspected (path, reason).
    inaccessible: List[Tuple[Path, str]] = field(default_factory=list)
    error: Optional[str] = None
    # identity of the store root as scanned, re-checked by fix()
    root_id: Tuple[int, int] = (0, 0)
    uninitialised_parts: List[Tuple[str, ...]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.findings or self.missing or self.uninitialised
                    or self.links or self.inaccessible or self.error)

    @property
    def fixed(self) -> List[DirFinding]:
        return [f for f in self.findings if f.fixed]

    @property
    def unfixed(self) -> List[DirFinding]:
        return [f for f in self.findings if not f.fixed]

    def by_owner(self) -> Dict[str, List[DirFinding]]:
        """Deviations grouped by owner -- only they can chmod them."""
        out: Dict[str, List[DirFinding]] = {}
        for f in self.findings:
            out.setdefault(f.owner, []).append(f)
        return out

    def to_dict(self) -> Dict[str, Any]:
        return {
            'root': str(self.root),
            'kind': self.kind,
            'name': self.name,
            'wanted': oct(self.wanted),
            'dirs_checked': self.dirs_checked,
            'error': self.error,
            'deviations': [f.to_dict() for f in self.findings],
            'missing_prefixes': [m.to_dict() for m in self.missing],
            'uninitialised': [str(p) for p in self.uninitialised],
            'links': [str(p) for p in self.links],
            'inaccessible': [{'path': str(p), 'reason': why}
                             for p, why in self.inaccessible],
            'by_owner': {
                owner: len(items) for owner, items in
                sorted(self.by_owner().items())
            },
        }


# =============================================================================
# Inspection
# =============================================================================

def _username(uid: int) -> str:
    """Resolve a uid to a login name, falling back to the numeric id."""
    try:
        import pwd
        return pwd.getpwuid(uid).pw_name
    except (KeyError, ImportError):
        return str(uid)


def describe_issues(mode: int, wanted: int) -> List[str]:
    """Name the ways ``mode`` falls short of ``wanted``.

    Only missing bits count. A directory that is more permissive than the
    policy in some unrelated respect is left alone.
    """
    issues = []
    if wanted & stat.S_IWGRP and not mode & stat.S_IWGRP:
        issues.append('not group-writable')
    if wanted & stat.S_ISGID and not mode & stat.S_ISGID:
        issues.append('not setgid')
    if wanted & stat.S_ISVTX and not mode & stat.S_ISVTX:
        issues.append('not sticky')
    if not wanted & stat.S_IROTH and mode & (stat.S_IROTH | stat.S_IXOTH):
        issues.append('readable by others')
    return issues


# O_PATH needs only search permission on the parent, so directories this user
# cannot read are still reached (and reported).
# Some Python builds don't expose os.O_PATH; its Linux value is fixed.
_O_PATH = getattr(os, 'O_PATH', 0o10000000 if sys.platform.startswith('linux') else os.O_RDONLY)
_DIR_FLAGS = _O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW


def _repaired_mode(mode: int, wanted: int) -> int:
    """``mode`` with the policy's missing owner/group/setgid/sticky bits added.

    Access for others is never added; under a policy without it, it is
    removed (tightening only).
    """
    other = stat.S_IRWXO
    new = mode | (wanted & ~other)
    if not wanted & stat.S_IROTH:
        new &= ~other
    return new


def _open_root(root: Path) -> int:
    """Open the store root as a directory fd.

    The root is configuration, so a link *at* the root is followed -- but only
    one owned by this user or root; one planted by someone else (e.g. a store
    swapped for a link after ``--all`` listed it) is refused.
    """
    path = os.path.abspath(root)
    for _ in range(40):
        st = os.lstat(path)
        if not stat.S_ISLNK(st.st_mode):
            break
        if st.st_uid not in (os.geteuid(), 0):
            raise OSError(errno.ELOOP, "store path is a link owned by another user")
        path = os.path.join(os.path.realpath(os.path.dirname(path)), os.readlink(path))
        path = os.path.abspath(path)
    else:
        raise OSError(errno.ELOOP, "too many levels of links at the store path")
    return os.open(path, _DIR_FLAGS)


def _fchmod_path_fd(fd: int, mode: int) -> None:
    """chmod the inode ``fd`` refers to (``fd`` may be an O_PATH descriptor).

    ``/proc/self/fd/N`` names the open inode itself, so nothing is re-resolved.
    """
    try:
        os.fchmod(fd, mode)
        return
    except OSError as e:
        if e.errno != errno.EBADF:
            raise
    os.chmod(f'/proc/self/fd/{fd}', mode)


def _open_rel(root_fd: int, parts: Sequence[str]) -> int:
    """Open ``root/parts...`` one component at a time, never following links."""
    fd = os.dup(root_fd)
    try:
        for part in parts:
            nfd = os.open(part, _DIR_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = nfd
        return fd
    except Exception:
        os.close(fd)
        raise


def _entry_kind(dir_fd: int, name: str) -> Optional[str]:
    """``'dir'``, ``'link'``, ``'other'`` or None (absent) for ``name``."""
    try:
        st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError:
        return 'error'
    if stat.S_ISLNK(st.st_mode):
        return 'link'
    return 'dir' if stat.S_ISDIR(st.st_mode) else 'other'


def _check_dir(root_fd: int, root: Path, parts: Tuple[str, ...],
               wanted: int):
    """A finding if ``root/parts`` deviates from ``wanted``; None if it
    matches; ``(path, reason)`` if it could not be inspected."""
    try:
        fd = _open_rel(root_fd, parts)
    except OSError as e:
        return (root.joinpath(*parts), e.strerror or str(e))
    try:
        st = os.fstat(fd)
    finally:
        os.close(fd)

    mode = stat.S_IMODE(st.st_mode)
    issues = describe_issues(mode, wanted)
    if not issues:
        return None

    return DirFinding(
        path=root.joinpath(*parts),
        rel='/'.join(parts) or '.',
        current_mode=mode,
        wanted=wanted,
        owner_uid=st.st_uid,
        owner=_username(st.st_uid),
        issues=issues,
        dev=st.st_dev,
        ino=st.st_ino,
        rel_parts=parts,
    )


def _prefix_bases(root_fd: int, kind: str,
                  links: List[Tuple[str, ...]]) -> List[Tuple[Tuple[str, ...], bool]]:
    """Directories holding 00..ff prefixes, as ``(parts, expect_full_set)``.

    Derived from what is present rather than from a declared layout, so a store
    partway through a v2-to-v3 migration is handled without special-casing.
    Links where these directories belong are added to ``links`` and skipped.

    ``expect_full_set`` marks the bases that should hold all 256 prefixes:

    * ``files/md5`` always -- this is what ``dt remote init`` pre-creates.
    * A v2 root never; it legitimately holds only the prefixes in use, mixed in
      among ``files/``, ``runs/`` and the verify ledger.
    * ``runs`` only for a cache. ``dt cache init`` pre-creates the run cache,
      but a remote may carry an empty ``runs/`` with nothing owed.
    """
    bases: List[Tuple[Tuple[str, ...], bool]] = []

    files_kind = _entry_kind(root_fd, 'files')
    if files_kind == 'link':
        links.append(('files',))
    elif files_kind == 'dir':
        try:
            ffd = _open_rel(root_fd, ('files',))
        except OSError:
            ffd = None
        if ffd is not None:
            try:
                md5_kind = _entry_kind(ffd, 'md5')
            finally:
                os.close(ffd)
            if md5_kind == 'link':
                links.append(('files', 'md5'))
            elif md5_kind == 'dir':
                bases.append((('files', 'md5'), True))

    # v2 keeps prefixes at the top level. Probe every expected name (needs
    # only search permission, unlike listing the directory).
    if any(_entry_kind(root_fd, p) == 'dir' for p in EXPECTED_PREFIXES):
        bases.append(((), False))

    runs_kind = _entry_kind(root_fd, 'runs')
    if runs_kind == 'link':
        links.append(('runs',))
    elif runs_kind == 'dir':
        bases.append((('runs',), kind == KIND_CACHE))
    return bases


def scan(
    root: Path,
    kind: str = KIND_REMOTE,
    name: str = '',
    sticky: bool = DEFAULT_STICKY,
    allow_other: bool = DEFAULT_ALLOW_OTHER,
    jobs: int = 8,
) -> PermsReport:
    """Inspect a cache or remote against the directory policy.

    Everything below the root is reached through directory descriptors opened
    without following links, so nothing outside the store is inspected.

    Args:
        root: Cache or remote directory.
        kind: ``remote`` or ``cache``, for reporting.
        name: Display name.
        sticky: Require the sticky bit as well.
        allow_other: Permit world read/execute (the default policy).
        jobs: Concurrent stat workers.

    Returns:
        A :class:`PermsReport`; nothing is modified.
    """
    root = Path(os.path.abspath(root))
    want = wanted_mode(sticky=sticky, allow_other=allow_other)
    report = PermsReport(root=root, kind=kind, name=name, wanted=want)

    try:
        root_fd = _open_root(root)
    except OSError as e:
        report.error = f"not a directory: {root} ({e.strerror or e})"
        return report
    try:
        rst = os.fstat(root_fd)
        report.root_id = (rst.st_dev, rst.st_ino)

        link_parts: List[Tuple[str, ...]] = []
        bases = _prefix_bases(root_fd, kind, link_parts)
        report.links.extend(root.joinpath(*p) for p in link_parts)
        if not bases:
            report.error = (
                f"no DVC blob layout found in {root} "
                f"(expected files/md5/<prefix>/ or <prefix>/)"
            )
            return report

        # The chain above the prefixes matters too: a non-writable files/md5
        # stops anyone creating a missing prefix in the first place.
        targets: List[Tuple[str, ...]] = [()]
        if _entry_kind(root_fd, 'files') == 'dir':
            targets.append(('files',))
        targets.extend(b for b, _full in bases)
        # A v2 root is both the store root and a prefix base; drop the repeat.
        targets = list(dict.fromkeys(targets))

        existing: List[Tuple[str, ...]] = []
        for base, expect_full_set in bases:
            try:
                bfd = _open_rel(root_fd, base)
            except OSError as e:
                report.inaccessible.append((root.joinpath(*base), e.strerror or str(e)))
                continue
            present, absent = [], []
            try:
                for prefix in EXPECTED_PREFIXES:
                    k = _entry_kind(bfd, prefix)
                    if k == 'error':
                        report.inaccessible.append(
                            (root.joinpath(*base, prefix), 'cannot be inspected'))
                    elif k == 'link':
                        report.links.append(root.joinpath(*base, prefix))
                    elif k == 'dir':
                        present.append(prefix)
                    elif k is None:
                        absent.append(prefix)
            finally:
                os.close(bfd)
            existing.extend(base + (p,) for p in present)

            if not expect_full_set:
                continue
            if not present:
                # Nothing at all: the store was never pre-created. That is a
                # different (and much less alarming) statement than a store
                # that has drifted, so it is reported separately rather than as
                # 256 individual gaps.
                report.uninitialised.append(root.joinpath(*base))
                report.uninitialised_parts.append(base)
            elif absent:
                report.missing.extend(
                    MissingPrefix(path=root.joinpath(*base, n), name=n,
                                  base_parts=base)
                    for n in absent
                )

        targets.extend(existing)
        report.dirs_checked = len(targets)

        with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
            results = list(pool.map(
                lambda p: _check_dir(root_fd, root, p, want), targets))
    finally:
        os.close(root_fd)

    report.findings = [r for r in results if isinstance(r, DirFinding)]
    report.findings.sort(key=lambda f: f.rel)
    report.inaccessible.extend(r for r in results if isinstance(r, tuple))
    return report


# =============================================================================
# Repair
# =============================================================================

def fix(report: PermsReport, verbose: bool = False) -> PermsReport:
    """Apply the policy where possible, recording what could not be applied.

    ``chmod`` requires ownership, so deviations on another user's directories
    cannot be repaired here at all -- they are reported for that user to fix.
    Creating a *missing* prefix directory only needs write permission on its
    parent, so that part often succeeds even on someone else's remote.

    Every directory is reached from the same store root that was scanned, one
    component at a time without following links, and changed through its own
    descriptor only if it is the very directory that was inspected.
    """
    try:
        root_fd = _open_root(report.root)
        rst = os.fstat(root_fd)
    except OSError as e:
        report.error = f"cannot open {report.root}: {e.strerror or e}"
        return report
    if (rst.st_dev, rst.st_ino) != report.root_id:
        os.close(root_fd)
        report.error = f"{report.root} changed since it was scanned; nothing changed"
        return report

    try:
        # A store that was never pre-created gets the full set, which is what
        # `dt remote init` would have done and what stops DVC creating them.
        for base in report.uninitialised_parts:
            report.missing.extend(
                MissingPrefix(path=report.root.joinpath(*base, n), name=n,
                              base_parts=base)
                for n in EXPECTED_PREFIXES
            )

        for missing in report.missing:
            try:
                bfd = _open_rel(root_fd, missing.base_parts)
            except OSError as e:
                missing.failure = _reason(e)
                continue
            try:
                # Create, never adopt: anything already at the name (a link
                # included) makes this fail rather than be changed.
                try:
                    os.mkdir(missing.name, 0o700, dir_fd=bfd)
                except FileExistsError:
                    missing.failure = 'already exists (not created by dt); left unchanged'
                    continue
                except OSError as e:
                    missing.failure = _reason(e)
                    continue
                try:
                    fd = os.open(missing.name, _DIR_FLAGS, dir_fd=bfd)
                    try:
                        _fchmod_path_fd(fd, report.wanted)
                    finally:
                        os.close(fd)
                    missing.created = True
                    if verbose:
                        print(f"  created {missing.path}")
                except OSError as e:
                    missing.failure = _reason(e)
            finally:
                os.close(bfd)

        for finding in report.findings:
            try:
                fd = _open_rel(root_fd, finding.rel_parts)
            except OSError as e:
                finding.failure = _reason(e)
                continue
            try:
                st = os.fstat(fd)
                if (st.st_dev, st.st_ino) != (finding.dev, finding.ino):
                    finding.failure = 'changed since it was inspected; left unchanged'
                    continue
                new_mode = _repaired_mode(stat.S_IMODE(st.st_mode), finding.wanted)
                _fchmod_path_fd(fd, new_mode)
            except OSError as e:
                finding.failure = _reason(e)
                continue
            finally:
                os.close(fd)
            finding.fixed = True
            if verbose:
                print(f"  fixed {finding.rel} "
                      f"({oct(finding.current_mode)} -> {oct(new_mode)})")
    finally:
        os.close(root_fd)

    return report


def _reason(e: OSError) -> str:
    if e.errno in (errno.EPERM, errno.EACCES):
        return 'permission denied (only the owner can change this)'
    return e.strerror or 'unknown error'


def check(
    root: Path,
    kind: str = KIND_REMOTE,
    name: str = '',
    sticky: bool = DEFAULT_STICKY,
    allow_other: bool = DEFAULT_ALLOW_OTHER,
    do_fix: bool = False,
    jobs: int = 8,
    verbose: bool = False,
) -> PermsReport:
    """Scan a store and, if ``do_fix``, repair what this user is able to."""
    report = scan(root, kind=kind, name=name, sticky=sticky,
                  allow_other=allow_other, jobs=jobs)
    if do_fix and not report.error:
        fix(report, verbose=verbose)
    return report


# =============================================================================
# Target resolution (shared with dt remote clean / dt cache clean)
# =============================================================================

def resolve_remote_targets(*args, **kwargs) -> List[Tuple[str, Path]]:
    """Delegate to the sweep module so both commands target identically."""
    from . import tmp_sweep
    return tmp_sweep.resolve_remote_targets(*args, **kwargs)


def resolve_cache_target(*args, **kwargs) -> Tuple[str, Path]:
    from . import tmp_sweep
    return tmp_sweep.resolve_cache_target(*args, **kwargs)


# =============================================================================
# Reporting
# =============================================================================

def format_report(
    report: PermsReport,
    fixed_mode: bool,
    verbose: bool = False,
) -> str:
    """Render one store's permission state as text.

    A compliant store prints nothing unless ``verbose``, so sweeping a whole
    remote root surfaces only what needs attention.
    """
    if report.ok and not verbose:
        return ''

    lines: List[str] = []
    label = report.name or str(report.root)
    lines.append(f"{report.kind}: {label}")
    lines.append(f"  {report.root}")

    if report.error:
        lines.append(f"  skipped: {report.error}")
        return "\n".join(lines)

    if report.ok:
        lines.append(f"  all {report.dirs_checked} directories match "
                     f"{oct(report.wanted)}")
        return "\n".join(lines)

    if report.inaccessible:
        lines.append(f"  {len(report.inaccessible)} director"
                     f"{'ies' if len(report.inaccessible) != 1 else 'y'} could not be inspected:")
        for p, why in report.inaccessible[:10]:
            lines.append(f"      {p} ({why})")

    if report.links:
        lines.append(f"  {len(report.links)} symbolic link(s) where store "
                     f"directories belong (not followed; remove them):")
        for p in report.links[:10]:
            lines.append(f"      {p}")

    if report.uninitialised:
        created = sum(1 for m in report.missing if m.created)
        lines.append(
            "  prefix directories not pre-created"
            + (f" ({created} created)" if fixed_mode else "")
        )
        if not fixed_mode:
            lines.append("      nothing pushed here yet; the first push will "
                         "create them under its own umask")

    if report.missing:
        n = len(report.missing)
        created = sum(1 for m in report.missing if m.created)
        failed = [m for m in report.missing if m.failure]
        lines.append(
            f"  {n} of 256 prefix director"
            f"{'ies' if n != 1 else 'y'} missing"
            + (f" ({created} created)" if fixed_mode else "")
        )
        if failed and fixed_mode:
            lines.append(f"      {len(failed)} could not be created: "
                         f"{failed[0].failure}")
        if not fixed_mode:
            lines.append("      DVC will recreate these under the writing "
                         "user's umask")

    if report.findings:
        n = len(report.findings)
        lines.append(
            f"  {n} director{'ies deviate' if n != 1 else 'y deviates'} from "
            f"{oct(report.wanted)}"
        )
        counts: Dict[str, int] = {}
        for f in report.findings:
            for issue in f.issues:
                counts[issue] = counts.get(issue, 0) + 1
        for issue, count in sorted(counts.items(), key=lambda kv: -kv[1]):
            lines.append(f"      {count:>5}  {issue}")

        for owner, items in sorted(report.by_owner().items()):
            mine = items[0].mine
            suffix = '' if mine else '  <- only this user can fix these'
            lines.append(f"      owner {owner}: {len(items)}{suffix}")

    if verbose:
        for f in report.findings[:50]:
            lines.append(f"      {f.rel}  "
                         f"{stat.filemode(stat.S_IFDIR | f.current_mode)}  "
                         f"{f.owner}  ({', '.join(f.issues)})")
        if len(report.findings) > 50:
            lines.append(f"      ... and {len(report.findings) - 50} more")

    if fixed_mode:
        lines.append(f"  fixed {len(report.fixed)}")
        unfixed = report.unfixed
        if unfixed:
            owners = sorted({f.owner for f in unfixed})
            lines.append(
                f"  {len(unfixed)} could not be fixed "
                f"(owned by {', '.join(owners)})"
            )

    return "\n".join(lines)


def format_summary(
    reports: Sequence[PermsReport],
    fixed_mode: bool,
    sticky: bool,
) -> str:
    """Render totals, plus the per-owner worklist."""
    lines: List[str] = ['']
    total_dev = sum(len(r.findings) for r in reports)
    total_missing = sum(len(r.missing) for r in reports)
    total_uninit = sum(1 for r in reports if r.uninitialised)
    total_links = sum(len(r.links) for r in reports)
    total_inacc = sum(len(r.inaccessible) for r in reports)
    n_roots = len(reports)

    if total_links:
        lines.append(f"{total_links} symbolic link(s) in place of store directories "
                     f"(not followed; remove them)")
    if total_inacc:
        lines.append(f"{total_inacc} director{'ies' if total_inacc != 1 else 'y'} "
                     f"could not be inspected")

    if not (total_dev or total_missing or total_uninit or total_links or total_inacc):
        scope = (f"{n_roots} {reports[0].kind}s" if n_roots != 1
                 else f"{reports[0].kind} "
                      f"{reports[0].name or reports[0].root}")
        policy = 'sticky' if sticky else 'shared'
        lines.append(f"All directories match the {policy} policy in {scope}")
        return "\n".join(lines)

    if fixed_mode:
        fixed = sum(len(r.fixed) for r in reports)
        created = sum(1 for r in reports for m in r.missing if m.created)
        lines.append(f"Fixed {fixed} director"
                     f"{'ies' if fixed != 1 else 'y'}"
                     + (f", created {created} missing prefix"
                        f"{'es' if created != 1 else ''}" if created else ""))
    else:
        bits = []
        if total_dev:
            bits.append(f"{total_dev} director"
                        f"{'ies deviate' if total_dev != 1 else 'y deviates'} "
                        f"from policy")
        if total_missing:
            bits.append(f"{total_missing} prefix directories missing")
        if total_uninit:
            bits.append(f"{total_uninit} store"
                        f"{'s' if total_uninit != 1 else ''} never pre-created")
        lines.append(', '.join(bits))

    # Only the owner can chmod, so this is the actionable output: who has to
    # run what.
    outstanding: Dict[str, int] = {}
    for r in reports:
        for f in r.findings:
            if fixed_mode and f.fixed:
                continue
            outstanding[f.owner] = outstanding.get(f.owner, 0) + 1

    if outstanding:
        me = _username(os.geteuid())
        lines.append('')
        lines.append('Needs the owner to run it:')
        for owner, count in sorted(outstanding.items(), key=lambda kv: -kv[1]):
            marker = '  (you)' if owner == me else ''
            lines.append(f"  {owner:<12} {count:>5} director"
                         f"{'ies' if count != 1 else 'y '}{marker}")
        if not fixed_mode:
            # No policy flags: the same defaults and config apply to their run.
            lines.append('')
            lines.append("Each owner: dt remote perms --all --fix")

    return "\n".join(lines)
