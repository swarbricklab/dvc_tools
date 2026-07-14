"""Scan a DVC remote for symlinked blobs and optionally repair them.

A healthy remote holds only real files. Symlinks can appear in a remote when
an older ``dt update`` seeded a rebuilt ``.dir`` manifest into a shared remote
through the cache link-ladder: on the common cross-filesystem layout (a
per-machine local cache vs a shared remote) reflink and hardlink both fail and
it fell back to ``os.symlink``, planting ``<remote>/files/md5/<xx>/<hash>.dir``
as a symlink pointing at a machine-local cache path. Such links dangle for
every other user, make the object invisible to ``dt find`` (the ``.dir`` can't
be loaded), and would be baked into archives.

This module walks the remote's blob layout, reports every symlink, and — when
``repair=True`` — replaces each *resolvable* symlink with a real, read-only
copy of its target, but only after confirming the target's content actually
hashes to the object's path-implied md5 (so a repair can never write wrong
content into a hash-named slot). Dangling symlinks, and links whose target
fails that hash check, cannot be safely recovered from the link alone and are
reported for manual rebuild (e.g. ``dt update`` from a repo that has the data).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from . import utils
from .errors import RemoteError


def _username(uid: int) -> str:
    """Resolve a uid to a login name, falling back to the numeric id."""
    try:
        import pwd
        return pwd.getpwuid(uid).pw_name
    except (KeyError, ImportError):
        return str(uid)


@dataclass
class SymlinkFinding:
    """One symlinked blob discovered in a remote."""
    path: Path                      # absolute path of the symlink in the remote
    rel: str                        # path relative to the remote root
    target: str                     # raw (possibly relative) symlink target
    expected_md5: Optional[str]     # md5 implied by the blob's path (None if unparseable)
    is_dir_object: bool             # True if a .dir manifest object
    owner_uid: int = -1             # uid that owns the symlink (from lstat)
    owner: str = ""                 # login name for owner_uid
    resolves: bool = False          # target exists and is a regular file
    md5_ok: Optional[bool] = None   # target content matches expected_md5 (None if not checked)
    # Populated by repair():
    repaired: bool = False
    outcome: str = ""               # human-readable result of a repair attempt


@dataclass
class FsckReport:
    remote_name: str
    remote_path: str
    layout: str
    current_uid: int = -1           # uid that ran the scan (owns what it can repair)
    findings: List[SymlinkFinding] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.findings)

    @property
    def resolvable(self) -> List[SymlinkFinding]:
        return [f for f in self.findings if f.resolves]

    @property
    def dangling(self) -> List[SymlinkFinding]:
        return [f for f in self.findings if not f.resolves]

    @property
    def owned_by_others(self) -> List[SymlinkFinding]:
        """Symlinks the current user cannot repair (owned by someone else)."""
        return [f for f in self.findings if f.owner_uid != self.current_uid]

    def other_owners(self) -> Dict[str, Dict[str, int]]:
        """Per other-user summary: ``{user: {'total': n, 'repairable': m}}``.

        ``repairable`` counts the user's symlinks whose target still resolves —
        i.e. the ones they could fix by running ``dt remote fsck --repair``
        themselves.
        """
        summary: Dict[str, Dict[str, int]] = {}
        for f in self.owned_by_others:
            slot = summary.setdefault(f.owner, {'total': 0, 'repairable': 0})
            slot['total'] += 1
            if f.resolves:
                slot['repairable'] += 1
        return summary

    def to_dict(self) -> dict:
        return {
            'remote': self.remote_name,
            'path': self.remote_path,
            'layout': self.layout,
            'run_by': _username(self.current_uid) if self.current_uid >= 0 else None,
            'total_symlinks': self.total,
            'resolvable': len(self.resolvable),
            'dangling': len(self.dangling),
            'repaired': sum(1 for f in self.findings if f.repaired),
            'other_owners': self.other_owners(),
            'findings': [
                {
                    'rel': f.rel,
                    'target': f.target,
                    'expected_md5': f.expected_md5,
                    'is_dir_object': f.is_dir_object,
                    'owner': f.owner,
                    'resolves': f.resolves,
                    'md5_ok': f.md5_ok,
                    'repaired': f.repaired,
                    'outcome': f.outcome,
                }
                for f in self.findings
            ],
        }


def _implied_md5(prefix_hex: str, filename: str) -> Optional[str]:
    """md5 implied by a blob's location, or None if the path isn't a blob.

    A v3 blob at ``files/md5/<xx>/<rest>`` (or a ``<rest>.dir`` object) implies
    md5 == ``<xx><rest>`` with any ``.dir`` suffix stripped.
    """
    name = filename[:-4] if filename.endswith('.dir') else filename
    candidate = (prefix_hex + name).lower()
    if len(candidate) == 32 and all(c in '0123456789abcdef' for c in candidate):
        return candidate
    return None


def scan_remote_symlinks(
    remote_path: Path,
    verbose: bool = False,
) -> tuple:
    """Walk ``remote_path`` blob prefixes and return ``(layout, findings)``.

    Every symlink found in a blob-prefix directory is reported (both real
    ``.dir`` objects and any other symlinked blob, e.g. from a stray
    ``dt fetch --destination <remote>``). Each finding records whether its
    target currently resolves to a regular file.
    """
    from .archive import operations as ops
    from .errors import ArchiveError

    try:
        layout = ops.detect_source_layout(remote_path)
    except ArchiveError as e:
        raise RemoteError(str(e))

    entries = ops._enumerate_prefix_dirs(remote_path, layout)
    findings: List[SymlinkFinding] = []

    for _key, pdir in entries:
        prefix_hex = pdir.name  # always the bare 2-char hex, even for mixed keys
        try:
            scan = list(os.scandir(pdir))
        except OSError:
            continue
        for entry in scan:
            if not entry.is_symlink():
                continue
            p = Path(entry.path)
            try:
                target = os.readlink(p)
            except OSError:
                target = "<unreadable>"
            # lstat (follow_symlinks=False) → ownership of the *link* itself,
            # which is what governs who may replace it in the remote dir.
            try:
                owner_uid = entry.stat(follow_symlinks=False).st_uid
            except OSError:
                owner_uid = -1
            # is_file() follows the link → True only if it resolves to a file.
            resolves = p.is_file()
            finding = SymlinkFinding(
                path=p,
                rel=str(p.relative_to(remote_path)),
                target=target,
                expected_md5=_implied_md5(prefix_hex, entry.name),
                is_dir_object=entry.name.endswith('.dir'),
                owner_uid=owner_uid,
                owner=_username(owner_uid) if owner_uid >= 0 else "?",
                resolves=resolves,
            )
            findings.append(finding)
            if verbose:
                state = "ok" if resolves else "DANGLING"
                print(f"  symlink [{state}] {finding.rel} -> {target}")

    if verbose:
        print(
            f"Found {len(findings)} symlink(s): "
            f"{sum(1 for f in findings if f.resolves)} resolvable, "
            f"{sum(1 for f in findings if not f.resolves)} dangling"
        )

    return layout, findings


def _repair_one(
    finding: SymlinkFinding,
    current_uid: int,
    verbose: bool = False,
) -> None:
    """Replace one resolvable symlink with a verified real copy in place.

    Writes the target's bytes to a temp file beside the link, confirms the copy
    hashes to ``expected_md5`` (when known), then atomically replaces the
    symlink. On any failure the link is left untouched.

    A symlink owned by another user is left alone: on shared storage this user
    typically cannot replace it, and even where it could, the owner should fix
    their own objects (their local cache is the authoritative source).
    """
    p = finding.path
    if finding.owner_uid != current_uid:
        finding.outcome = (
            f"skipped: owned by {finding.owner} — ask them to run "
            f"`dt remote fsck --repair`"
        )
        return
    if not finding.resolves:
        finding.outcome = "dangling: target missing, cannot recover from link"
        return

    real = p.resolve()
    tmp = p.with_name(p.name + ".fsck-tmp")
    try:
        # Copy the dereferenced content to a temp file in the same directory.
        with open(real, 'rb') as src, open(tmp, 'wb') as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)

        # Verify the copy matches the md5 implied by the object's path before
        # we let it stand in a hash-named slot.
        if finding.expected_md5 is not None:
            actual = utils.md5_file(tmp)
            finding.md5_ok = (actual == finding.expected_md5)
            if not finding.md5_ok:
                tmp.unlink(missing_ok=True)
                finding.outcome = (
                    f"skipped: target content md5 {actual[:12]}… != "
                    f"path-implied {finding.expected_md5[:12]}…"
                )
                return

        # Read-only, like every other remote blob.
        os.chmod(tmp, 0o444)
        # Atomic swap: replaces the symlink with the real file.
        os.replace(tmp, p)
        finding.repaired = True
        finding.outcome = "repaired: replaced symlink with real copy"
        if verbose:
            print(f"  repaired {finding.rel}")
    except OSError as e:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        finding.outcome = f"error: {e}"
        if verbose:
            print(f"  FAILED {finding.rel}: {e}")


def fsck_remote(
    remote_name: Optional[str],
    repair: bool = False,
    verbose: bool = False,
) -> FsckReport:
    """Scan (and optionally repair) symlinked blobs in a remote.

    Resolves ``remote_name`` to a locally-accessible path, scans for symlinked
    blobs, and — when ``repair`` is set — replaces each resolvable, hash-verified
    symlink with a real copy.
    """
    from . import remote_verify as remote_verify_mod

    name, url, path = remote_verify_mod.resolve_local_remote(remote_name)

    if verbose:
        print(f"Scanning remote '{name}' at {path} ...")

    current_uid = os.getuid()
    layout, findings = scan_remote_symlinks(path, verbose=verbose)
    report = FsckReport(
        remote_name=name, remote_path=str(path), layout=layout,
        current_uid=current_uid, findings=findings,
    )

    if repair and findings:
        mine = [f for f in findings if f.owner_uid == current_uid and f.resolves]
        if verbose:
            print(f"Repairing {len(mine)} symlink(s) owned by you ...")
        for finding in findings:
            _repair_one(finding, current_uid, verbose=verbose)

    return report
