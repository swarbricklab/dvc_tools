"""Quarantine corrupt/incomplete blobs in a DVC remote so ``dvc push`` re-uploads them.

``dt remote verify`` finds bad blobs; this moves them aside so a repo whose
cache still holds a good copy can re-push them. Move (not delete) keeps the
corrupt bytes for forensics and is fully reversible via the restore manifest.

**The .dir subtlety.** ``dvc push`` does not examine a directory's member files
if the directory's ``.dir`` object is already present in the remote — it treats
"``.dir`` present" as "directory complete". So quarantining a bad *member* blob
alone is not enough; the enclosing ``.dir`` object must also be quarantined
(even though it is intact) to force the re-push. ``.dir`` blobs are tiny, so
re-pushing them is cheap. We therefore build a ``child_md5 -> [.dir paths]``
reverse map by scanning the remote's (few, small) ``.dir`` objects and quarantine
every enclosing ``.dir`` alongside its bad members. Handles v2/v3/mixed layouts
and the case where the bad blob *is* a ``.dir``.

Quarantined blobs move to ``<remote>/.dt-verify/quarantine/<timestamp>/<rel-path>``
with a ``manifest.json`` recording each original path, md5, status and reason.
"""

import datetime as _dt
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import remote_verify
from . import utils
from .errors import RemoteError

QUARANTINE_DIRNAME = 'quarantine'
MANIFEST_FILENAME = 'manifest.json'
REASON_DIR_ENCLOSING = 'enclosing .dir (forces dvc push to re-examine members)'

# A bare md5 token in an external list: 32 lowercase hex, optionally with a
# trailing ``.dir`` (DVC directory manifest objects). Inline trailing comments
# (``<token>  # note``) are stripped before matching.
_MD5_TOKEN_RE = re.compile(r'^([0-9a-f]{32})(\.dir)?$')
# Externally-supplied entries carry the verifier's 'mismatch' status: the key
# is correct but the content is known-wrong (truncated/corrupt), which is
# exactly what the enclosing-.dir quarantine logic keys off of.
STATUS_EXTERNAL = remote_verify.STATUS_MISMATCH


def read_list_file(path: Path) -> List[str]:
    """Read an external bad-blob list into raw tokens.

    Format: one md5 or blob path per line. Blank lines and ``#`` comments are
    ignored, as are inline trailing comments (``<token>  # note``). Only the
    first whitespace-delimited field on each line is kept, so output from tools
    that emit ``<md5> <size>`` columns feeds in directly (DVC blob paths never
    contain spaces).
    """
    try:
        text = Path(path).read_text()
    except OSError as exc:
        raise RemoteError(f"Cannot read bad-blob list {path}: {exc}")
    tokens: List[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith('#'):
            continue
        s = re.split(r'\s+#', s, maxsplit=1)[0]  # strip inline comment
        s = s.split()[0] if s.split() else ''    # first field only
        if s:
            tokens.append(s)
    return tokens


def _md5_blob_path(remote_dir: Path, md5: str, is_dir: bool,
                   layout: str) -> str:
    """Remote-relative blob path for an md5 under the detected layout.

    For a mixed remote the same md5 could live under either the v3
    (``files/md5/..``) or v2 (``<prefix>/..``) tree, so prefer whichever path
    actually exists, defaulting to v3.
    """
    prefix, rest = md5[:2], md5[2:]
    suffix = '.dir' if is_dir else ''
    v3 = f'files/md5/{prefix}/{rest}{suffix}'
    v2 = f'{prefix}/{rest}{suffix}'
    if layout == 'dvc-v2':
        return v2
    if layout == 'dvc-mixed':
        if not (remote_dir / v3).exists() and (remote_dir / v2).exists():
            return v2
        return v3
    return v3  # dvc-v3 (and the sensible default)


def entries_from_list(
    remote_dir: Path,
    tokens: List[str],
    layout: Optional[str] = None,
) -> Tuple[List[dict], List[Tuple[str, str]]]:
    """Normalise external tokens into the verifier's bad-entry schema.

    Each token is either a bare md5 (32 lowercase hex, optional ``.dir``
    suffix) or a remote-relative/absolute-under-remote blob path. Every
    accepted token becomes a ``{path, expected_md5, status}`` dict identical in
    shape to what ``dt remote verify`` records in ``bad.json`` — so the same
    ``plan_quarantine`` machinery (including pulling in enclosing ``.dir``
    objects for bad members) drives it, with no forked logic.

    Returns ``(entries, rejected)``; ``rejected`` is ``[(token, reason), ..]``
    for tokens that aren't a valid md5 or a path reconstructing a valid md5.
    Entries are deduplicated by path.
    """
    from .archive import operations as ops

    if layout is None:
        try:
            layout = ops.detect_source_layout(remote_dir)
        except Exception:
            layout = 'dvc-v3'

    entries: List[dict] = []
    rejected: List[Tuple[str, str]] = []
    seen_paths = set()

    def _emit(rel: str, md5: Optional[str]):
        if rel in seen_paths:
            return
        seen_paths.add(rel)
        entries.append({'path': rel, 'expected_md5': md5,
                        'status': STATUS_EXTERNAL})

    for raw in tokens:
        tok = raw.strip()
        if not tok:
            continue
        m = _MD5_TOKEN_RE.match(tok.lower())
        if m:
            md5 = m.group(1)
            is_dir = bool(m.group(2))
            rel = _md5_blob_path(remote_dir, md5, is_dir, layout)
            _emit(rel, md5)
            continue

        # Otherwise treat it as a blob path.
        rel = tok
        if os.path.isabs(rel):
            try:
                rel = str(Path(rel).resolve().relative_to(
                    remote_dir.resolve()))
            except ValueError:
                rejected.append((raw, 'absolute path outside the remote'))
                continue
        rel = rel.lstrip('./').rstrip('/')
        if not rel:
            rejected.append((raw, 'empty path'))
            continue
        name = rel.split('/')[-1]
        hex_prefix = remote_verify._prefix_key_for_rel(rel, layout)[0][-2:]
        expected = remote_verify.expected_md5_for_blob(hex_prefix, name)
        if not remote_verify._HASH_RE.match(expected):
            rejected.append(
                (raw, 'not an md5 and not a blob path reconstructing an md5'))
            continue
        _emit(rel, expected)

    return entries, rejected


def _quarantine_root(remote_dir: Path) -> Path:
    return remote_dir / remote_verify.LEDGER_DIRNAME / QUARANTINE_DIRNAME


def build_dir_reverse_map(
    remote_dir: Path, layout: Optional[str] = None,
) -> Dict[str, List[str]]:
    """Map ``child_md5 -> [.dir blob rel-paths]`` by scanning the remote's .dir objects.

    ``.dir`` objects are DVC directory manifests (JSON arrays of
    ``{"md5", "relpath"}``). Finding them requires listing every prefix
    directory (cost scales with total object count), but only ``.dir`` files
    are read and parsed — those are few and small. Unreadable/malformed
    manifests are skipped (they surface as bad blobs in their own right).
    """
    from .archive import operations as ops

    layout = layout or ops.detect_source_layout(remote_dir)
    rev: Dict[str, List[str]] = {}
    for _key, _hexp, dir_path in remote_verify._prefix_dirs(remote_dir, layout):
        try:
            children = list(dir_path.iterdir())
        except OSError:
            continue
        for f in children:
            if not f.name.endswith('.dir') or not f.is_file():
                continue
            rel = str(f.relative_to(remote_dir))
            try:
                entries = json.loads(f.read_bytes())
            except (OSError, ValueError):
                continue
            if not isinstance(entries, list):
                continue
            for e in entries:
                child = e.get('md5') if isinstance(e, dict) else None
                if child:
                    rev.setdefault(child, []).append(rel)
    return rev


def plan_quarantine(
    remote_dir: Path,
    entries: List[dict],
    layout: Optional[str] = None,
) -> List[dict]:
    """Compute the full set of blobs to quarantine for the given bad entries.

    For each bad blob this includes the blob itself plus every enclosing ``.dir``
    object (found via the reverse map) — quarantining the ``.dir`` is what forces
    ``dvc push`` to re-examine the directory's members. Deduplicated by path.

    Returns a list of ``{path, md5, status, reason}`` dicts (sorted, unique).
    """
    from .archive import operations as ops

    layout = layout or ops.detect_source_layout(remote_dir)

    bad_paths = {e['path'] for e in entries if e.get('path')}
    # Only an md5-bearing bad *member* (not a .dir, not a stray with no md5)
    # needs the reverse map. Skip the whole-remote .dir scan otherwise — e.g.
    # when the only finding is a stray *.tmp with no md5.
    need_reverse = any(
        e.get('expected_md5') and not e['path'].endswith('.dir')
        for e in entries if e.get('path'))
    reverse = build_dir_reverse_map(remote_dir, layout) if need_reverse else {}

    planned: Dict[str, dict] = {}

    def _add(path: str, md5: Optional[str], status: str, reason: str):
        if path not in planned:
            planned[path] = {'path': path, 'md5': md5, 'status': status,
                             'reason': reason}

    for e in entries:
        rel = e.get('path')
        if not rel:
            continue
        md5 = e.get('expected_md5')
        _add(rel, md5, e.get('status', 'bad'), 'corrupt/incomplete blob')

        # A bad member: also quarantine every .dir that references it, so
        # dvc push re-examines the directory rather than trusting the .dir.
        if md5 and not rel.endswith('.dir'):
            for dir_rel in reverse.get(md5, []):
                if dir_rel in bad_paths:
                    continue  # already covered as a bad entry
                dir_name = dir_rel.split('/')[-1]
                # Reconstruct the .dir object's plain 32-hex md5, matching how
                # verify records a directly-bad .dir entry (expected_md5_for_blob
                # strips the .dir suffix), so the manifest is internally
                # consistent.
                dir_md5 = remote_verify.expected_md5_for_blob(
                    _dir_prefix(dir_rel), dir_name)
                _add(dir_rel, dir_md5, 'ok', REASON_DIR_ENCLOSING)

    return sorted(planned.values(), key=lambda d: d['path'])


def _dir_prefix(rel_path: str) -> str:
    """The 2-char hex prefix from a blob's remote-relative path (v2 or v3).

    Shares the path→prefix logic with the verifier's ledger-key derivation so
    the two commands can't diverge on a future layout change.
    """
    return remote_verify._prefix_key_for_rel(rel_path, None)[0][-2:]


def quarantine(
    remote_dir: Path,
    entries: List[dict],
    layout: Optional[str] = None,
    timestamp: Optional[str] = None,
    verbose: bool = False,
    dry_run: bool = False,
) -> dict:
    """Move bad blobs (and their enclosing ``.dir`` objects) into quarantine.

    Returns a result dict with the quarantine dir, the moved entries, and any
    that were missing. On ``dry_run`` nothing is moved.
    """
    from .archive import operations as ops

    layout = layout or ops.detect_source_layout(remote_dir)
    plan = plan_quarantine(remote_dir, entries, layout)
    if not plan:
        return {'quarantine_dir': None, 'moved': [], 'missing': [], 'plan': []}

    ts = timestamp or _dt.datetime.now(_dt.timezone.utc).strftime(
        '%Y%m%dT%H%M%SZ')

    moved: List[dict] = []
    missing: List[dict] = []

    if dry_run:
        qdir = _quarantine_root(remote_dir) / ts
        for item in plan:
            src = remote_dir / item['path']
            (moved if src.exists() else missing).append(item)
        return {'quarantine_dir': str(qdir), 'moved': moved,
                'missing': missing, 'plan': plan, 'dry_run': True}

    # Ensure a fresh batch dir: two runs in the same UTC second must not share
    # a dir (that would orphan the first run's blobs and overwrite its
    # manifest). Suffix -1, -2, ... until we find an unused name.
    root = _quarantine_root(remote_dir)
    qdir = root / ts
    n = 1
    while qdir.exists():
        qdir = root / f'{ts}-{n}'
        n += 1

    try:
        qdir.mkdir(parents=True, exist_ok=True)
        utils.set_group_writable(_quarantine_root(remote_dir), setgid=True)
    except OSError as exc:
        raise RemoteError(
            f"Cannot create quarantine dir {qdir}: {exc}")

    for item in plan:
        rel = item['path']
        src = remote_dir / rel
        dst = qdir / rel
        if not src.exists():
            missing.append(item)
            continue
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            # The parent dir is writable, so os.rename works even though the
            # blob file itself is read-only. Falls back to a copy+unlink across
            # filesystems (quarantine lives under the same remote, so rename
            # normally succeeds).
            try:
                os.rename(src, dst)
            except OSError:
                import shutil
                shutil.copy2(src, dst)
                try:
                    os.chmod(src, 0o644)
                except OSError:
                    pass
                src.unlink()
            moved.append({**item, 'quarantined_to': str(Path(ts) / rel)})
            if verbose:
                print(f"  quarantined {rel}")
        except OSError as exc:
            raise RemoteError(f"Failed to quarantine {rel}: {exc}")

    manifest = {
        'version': 1,
        'quarantined_at': _dt.datetime.now(_dt.timezone.utc).isoformat(
            timespec='seconds'),
        'remote_dir': str(remote_dir),
        'layout': layout,
        'entries': moved,
        'missing': missing,
    }
    try:
        with open(qdir / MANIFEST_FILENAME, 'w') as f:
            json.dump(manifest, f, indent=2)
    except OSError as exc:
        raise RemoteError(f"Quarantined blobs but failed to write manifest: {exc}")

    return {'quarantine_dir': str(qdir), 'moved': moved,
            'missing': missing, 'plan': plan}


def list_quarantines(remote_dir: Path) -> List[dict]:
    """List quarantine batches under the remote, newest first."""
    root = _quarantine_root(remote_dir)
    if not root.is_dir():
        return []
    out = []
    for d in sorted(root.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        manifest = d / MANIFEST_FILENAME
        info = {'timestamp': d.name, 'path': str(d), 'count': None,
                'quarantined_at': None}
        try:
            with open(manifest) as f:
                data = json.load(f)
            info['count'] = len(data.get('entries', []))
            info['quarantined_at'] = data.get('quarantined_at')
        except (OSError, ValueError):
            pass
        out.append(info)
    return out


def restore_quarantine(
    remote_dir: Path,
    timestamp: str,
    verbose: bool = False,
) -> dict:
    """Move a quarantine batch's blobs back to their original remote paths.

    Skips any blob whose original path is now occupied (e.g. it was already
    re-pushed) — that copy wins; the quarantined one stays put for forensics.
    """
    qdir = _quarantine_root(remote_dir) / timestamp
    manifest_path = qdir / MANIFEST_FILENAME
    # Distinguish an absent manifest from a present-but-corrupt one so the
    # operator isn't told a physically-present batch "doesn't exist".
    if not manifest_path.exists():
        raise RemoteError(f"No quarantine manifest at {manifest_path}")
    try:
        with open(manifest_path) as f:
            data = json.load(f)
    except (OSError, ValueError) as exc:
        raise RemoteError(
            f"Quarantine manifest at {manifest_path} is unreadable/corrupt: "
            f"{exc}")

    restored, skipped, absent = [], [], []
    for item in data.get('entries', []):
        rel = item['path']
        src = qdir / rel
        dst = remote_dir / rel
        if not src.exists():
            absent.append(rel)  # quarantined blob is gone (already restored?)
            continue
        if dst.exists():
            skipped.append(rel)  # already re-pushed; leave quarantine copy
            continue
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.rename(src, dst)
            restored.append(rel)
            if verbose:
                print(f"  restored {rel}")
        except OSError as exc:
            raise RemoteError(f"Failed to restore {rel}: {exc}")

    return {'restored': restored, 'skipped': skipped, 'absent': absent,
            'quarantine_dir': str(qdir)}
