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

Optionally the sticky bit can be requested as well. On a directory it means a
file may be removed or renamed only by the file's owner, the directory's owner,
or root -- so everyone can still create objects and push, but nobody can delete
another user's data by accident. Creation is unaffected.

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


def wanted_mode(sticky: bool = False, allow_other: bool = True) -> int:
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

    def to_dict(self) -> Dict[str, Any]:
        return {'path': str(self.path), 'name': self.name,
                'created': self.created, 'failure': self.failure}


@dataclass
class PermsReport:
    """Permission state of one cache or remote."""
    root: Path
    kind: str
    name: str = ''
    wanted: int = MODE_SHARED
    dirs_checked: int = 0
    findings: List[DirFinding] = field(default_factory=list)
    missing: List[MissingPrefix] = field(default_factory=list)
    # Bases holding none of the 256 prefixes: never pre-created, rather than
    # drifted. Usually just a remote nobody has pushed to yet.
    uninitialised: List[Path] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return not (self.findings or self.missing or self.uninitialised
                    or self.error)

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


def _check_dir(path: Path, root: Path, wanted: int) -> Optional[DirFinding]:
    """Return a finding if ``path`` deviates from ``wanted``, else None."""
    try:
        st = os.stat(path)
    except OSError:
        return None

    mode = stat.S_IMODE(st.st_mode)
    issues = describe_issues(mode, wanted)
    if not issues:
        return None

    try:
        rel = str(path.relative_to(root)) or '.'
    except ValueError:
        rel = str(path)

    return DirFinding(
        path=path,
        rel=rel,
        current_mode=mode,
        wanted=wanted,
        owner_uid=st.st_uid,
        owner=_username(st.st_uid),
        issues=issues,
    )


def _prefix_bases(root: Path, kind: str) -> List[Tuple[Path, bool]]:
    """Directories holding 00..ff prefixes, as ``(path, expect_full_set)``.

    Derived from what is present rather than from a declared layout, so a store
    partway through a v2-to-v3 migration is handled without special-casing.

    ``expect_full_set`` marks the bases that should hold all 256 prefixes:

    * ``files/md5`` always -- this is what ``dt remote init`` pre-creates.
    * A v2 root never; it legitimately holds only the prefixes in use, mixed in
      among ``files/``, ``runs/`` and the verify ledger.
    * ``runs`` only for a cache. ``dt cache init`` pre-creates the run cache,
      but a remote may carry an empty ``runs/`` with nothing owed.
    """
    bases: List[Tuple[Path, bool]] = []
    v3 = root / 'files' / 'md5'
    if v3.is_dir():
        bases.append((v3, True))
    if any((root / p).is_dir() for p in EXPECTED_PREFIXES[:4]):
        bases.append((root, False))
    runs = root / 'runs'
    if runs.is_dir():
        bases.append((runs, kind == KIND_CACHE))
    return bases


def scan(
    root: Path,
    kind: str = KIND_REMOTE,
    name: str = '',
    sticky: bool = False,
    allow_other: bool = True,
    jobs: int = 8,
) -> PermsReport:
    """Inspect a cache or remote against the directory policy.

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
    root = Path(root)
    want = wanted_mode(sticky=sticky, allow_other=allow_other)
    report = PermsReport(root=root, kind=kind, name=name, wanted=want)

    if not root.is_dir():
        report.error = f"not a directory: {root}"
        return report

    bases = _prefix_bases(root, kind)
    if not bases:
        report.error = (
            f"no DVC blob layout found in {root} "
            f"(expected files/md5/<prefix>/ or <prefix>/)"
        )
        return report

    # The chain above the prefixes matters too: a non-writable files/md5 stops
    # anyone creating a missing prefix in the first place.
    targets: List[Path] = [root]
    if (root / 'files').is_dir():
        targets.append(root / 'files')
    targets.extend(b for b, _full in bases)

    existing: List[Path] = []
    for base, expect_full_set in bases:
        present, absent = [], []
        for prefix in EXPECTED_PREFIXES:
            p = base / prefix
            (present if p.is_dir() else absent).append((prefix, p))
        existing.extend(p for _n, p in present)

        if not expect_full_set:
            continue
        if not present:
            # Nothing at all: the store was never pre-created. That is a
            # different (and much less alarming) statement than a store that
            # has drifted, so it is reported separately rather than as 256
            # individual gaps.
            report.uninitialised.append(base)
        elif absent:
            report.missing.extend(
                MissingPrefix(path=p, name=n) for n, p in absent
            )

    targets.extend(existing)
    report.dirs_checked = len(targets)

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        results = list(pool.map(lambda p: _check_dir(p, root, want), targets))

    report.findings = [r for r in results if r is not None]
    report.findings.sort(key=lambda f: f.rel)
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
    """
    # A store that was never pre-created gets the full set, which is what
    # `dt remote init` would have done and what stops DVC creating them itself.
    for base in report.uninitialised:
        report.missing.extend(
            MissingPrefix(path=base / name, name=name)
            for name in EXPECTED_PREFIXES
        )

    for missing in report.missing:
        try:
            missing.path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            missing.failure = _reason(e)
            continue
        # We just created it, so we own it and this chmod will stick.
        try:
            os.chmod(missing.path, report.wanted)
            missing.created = True
            if verbose:
                print(f"  created {missing.path}")
        except OSError as e:
            missing.failure = _reason(e)

    for finding in report.findings:
        try:
            os.chmod(finding.path, finding.wanted)
        except OSError as e:
            finding.failure = _reason(e)
            continue
        finding.fixed = True
        if verbose:
            print(f"  fixed {finding.rel} "
                  f"({oct(finding.current_mode)} -> {oct(finding.wanted)})")

    return report


def _reason(e: OSError) -> str:
    if e.errno in (errno.EPERM, errno.EACCES):
        return 'permission denied (only the owner can change this)'
    return e.strerror or 'unknown error'


def check(
    root: Path,
    kind: str = KIND_REMOTE,
    name: str = '',
    sticky: bool = False,
    allow_other: bool = True,
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
    n_roots = len(reports)

    if not total_dev and not total_missing and not total_uninit:
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
            flag = ' --sticky' if sticky else ''
            lines.append('')
            lines.append(f"Each owner: dt remote perms --all --fix{flag}")

    return "\n".join(lines)
