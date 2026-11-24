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
import errno
import json
import os
import stat
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
    prefix, rest = utils.oid_path_parts(md5)  # raises on a malformed id
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
        m = _MD5_TOKEN_RE.fullmatch(tok.lower())
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
        if not remote_verify.safe_blob_rel(rel):
            rejected.append((raw, 'not a blob path inside the remote'))
            continue
        name = rel.split('/')[-1]
        hex_prefix = remote_verify._prefix_key_for_rel(rel, layout)[0][-2:]
        expected = remote_verify.expected_md5_for_blob(hex_prefix, name)
        if not remote_verify._HASH_RE.fullmatch(expected):
            rejected.append(
                (raw, 'not an md5 and not a blob path reconstructing an md5'))
            continue
        _emit(rel, expected)

    return entries, rejected


def confirm_bad(remote_dir: Path, entries: List[dict]) -> Tuple[List[dict], List[dict]]:
    """Re-check report entries against the remote before acting on them.

    A report can be edited by other users, so it is not proof on its own.
    An entry is confirmed bad if it is a link, a stray (non-blob-named) file,
    or a regular blob whose content does not hash to its path's md5; absent
    entries are kept (reported as missing). An intact blob is refused.

    Returns ``(confirmed, refused)``.
    """
    confirmed: List[dict] = []
    refused: List[dict] = []
    for e in entries:
        rel = e.get('path') if isinstance(e, dict) else None
        if not remote_verify.safe_blob_rel(rel) or not remote_verify._dirs_are_real(
                remote_dir, rel.rsplit('/', 1)[0]):
            refused.append({'path': str(rel), 'reason': 'not a blob path inside the remote'})
            continue
        f = remote_dir / rel
        try:
            st = os.lstat(f)
        except FileNotFoundError:
            confirmed.append(e)
            continue
        if stat.S_ISLNK(st.st_mode):
            confirmed.append(e)
            continue
        hex_prefix = remote_verify._prefix_key_for_rel(rel, 'dvc-mixed')[0][-2:]
        expected = remote_verify.expected_md5_for_blob(hex_prefix, rel.rsplit('/', 1)[-1])
        if not remote_verify._HASH_RE.fullmatch(expected or ''):
            confirmed.append(e)  # stray file in a prefix dir
            continue
        try:
            _size, _mtime, actual = remote_verify._hash_blob(f)
        except OSError:
            confirmed.append(e)  # unreadable: as the verifier would report it
            continue
        if actual == expected:
            refused.append({'path': rel, 'reason': 'intact (content matches its name)'})
        else:
            confirmed.append(e)
    return confirmed, refused


def _quarantine_root(remote_dir: Path) -> Path:
    return remote_dir / remote_verify.LEDGER_DIRNAME / QUARANTINE_DIRNAME


_RDIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _open_remote(remote_dir: Path) -> int:
    """Open the remote root once (a link at the root itself only if it is
    this user's or root's, as for ``dt remote perms``)."""
    from . import perms
    try:
        pfd = perms._open_root(remote_dir)
        try:
            return os.open('.', _RDIR, dir_fd=pfd)
        finally:
            os.close(pfd)
    except OSError as exc:
        raise RemoteError(f"Cannot open remote {remote_dir}: {exc}")


def _walk(dir_fd: int, rel_dir: str, create: bool = False,
          mode: int = 0o777) -> int:
    """Open ``rel_dir`` under ``dir_fd`` one component at a time, never
    following a link. With ``create``, missing components are made (with
    ``mode``, less the umask). Returns a new directory fd."""
    fd = os.dup(dir_fd)
    try:
        for part in [p for p in rel_dir.split('/') if p]:
            if part in ('.', '..'):
                raise OSError(errno.EINVAL, f"bad path component {part!r}")
            if create:
                try:
                    os.mkdir(part, mode, dir_fd=fd)
                except FileExistsError:
                    pass
            nfd = os.open(part, _RDIR, dir_fd=fd)
            os.close(fd)
            fd = nfd
        return fd
    except Exception:
        os.close(fd)
        raise


def _move_entry(sdir: int, name: str, ddir: int) -> None:
    """Move entry ``name`` from directory fd ``sdir`` to ``ddir`` (same name).

    A plain rename moves the entry itself (a link stays a link). Only a
    genuine cross-filesystem rename falls back to copy+unlink: a regular file
    is copied through a descriptor opened without following links, a link is
    recreated as a link; nothing's mode is changed. Any other error (e.g. a
    sticky directory) is raised.
    """
    try:
        os.rename(name, name, src_dir_fd=sdir, dst_dir_fd=ddir)
        return
    except OSError as e:
        if e.errno != errno.EXDEV:
            raise
    st = os.lstat(name, dir_fd=sdir)
    if stat.S_ISLNK(st.st_mode):
        os.symlink(os.readlink(name, dir_fd=sdir), name, dir_fd=ddir)
    elif stat.S_ISREG(st.st_mode):
        sfd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=sdir)
        try:
            if not stat.S_ISREG(os.fstat(sfd).st_mode):
                raise OSError(errno.EINVAL, f"{name} is not a regular file")
            dfd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                          stat.S_IMODE(st.st_mode), dir_fd=ddir)
            ok = False
            try:
                while True:
                    chunk = os.read(sfd, 1 << 20)
                    if not chunk:
                        break
                    view = memoryview(chunk)
                    while view:
                        view = view[os.write(dfd, view):]
                ok = True
            finally:
                os.close(dfd)
                if not ok:
                    try:
                        os.unlink(name, dir_fd=ddir)
                    except OSError:
                        pass
        finally:
            os.close(sfd)
    else:
        raise OSError(errno.EINVAL, f"{name} is not a regular file or link")
    os.unlink(name, dir_fd=sdir)


def _lexists_in(dir_fd: int, name: str) -> bool:
    try:
        os.lstat(name, dir_fd=dir_fd)
        return True
    except FileNotFoundError:
        return False


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

    # Only plain blob paths inside the remote; report entries are editable
    # by other users.
    entries = [e for e in entries if remote_verify.safe_blob_rel(e.get('path'))]
    bad_paths = {e['path'] for e in entries}
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
    confirm: bool = True,
) -> dict:
    """Move bad blobs (and their enclosing ``.dir`` objects) into quarantine.

    With ``confirm`` (the default) each entry is re-checked against the
    remote first (see :func:`confirm_bad`); pass False only for a list the
    operator produced or a verify run made just now.

    Returns a result dict with the quarantine dir, the moved entries, and any
    that were missing. On ``dry_run`` nothing is moved.
    """
    from .archive import operations as ops

    layout = layout or ops.detect_source_layout(remote_dir)
    refused: List[dict] = []
    if confirm:
        entries, refused = confirm_bad(remote_dir, entries)
    plan = plan_quarantine(remote_dir, entries, layout)
    if not plan:
        return {'quarantine_dir': None, 'moved': [], 'missing': [], 'plan': [],
                'refused': refused}

    ts = timestamp or _dt.datetime.now(_dt.timezone.utc).strftime(
        '%Y%m%dT%H%M%SZ')

    moved: List[dict] = []
    missing: List[dict] = []

    if dry_run:
        qdir = _quarantine_root(remote_dir) / ts
        for item in plan:
            src = remote_dir / item['path']
            (moved if os.path.lexists(src) else missing).append(item)
        return {'quarantine_dir': str(qdir), 'moved': moved,
                'missing': missing, 'plan': plan, 'dry_run': True,
                'refused': refused}

    if not ts or '/' in ts or ts in ('.', '..'):
        raise RemoteError(f"Invalid quarantine batch name: {ts!r}")

    manifest = {
        'version': 1,
        'remote_dir': str(remote_dir),
        'layout': layout,
    }
    rfd = _open_remote(remote_dir)
    try:
        # Ensure a fresh batch dir: two runs in the same UTC second must not
        # share a dir (that would orphan the first run's blobs and overwrite
        # its manifest). Suffix -1, -2, ... until a new one is made. All of it
        # is reached from the remote root without following links.
        name = ts
        try:
            qroot = _walk(rfd, f'{remote_verify.LEDGER_DIRNAME}/{QUARANTINE_DIRNAME}',
                          create=True)
            try:
                try:
                    os.fchmod(qroot, utils.shared_dir_mode())
                except OSError:
                    pass  # not ours to chmod
                n = 1
                while True:
                    try:
                        os.mkdir(name, 0o755, dir_fd=qroot)  # fresh, ours
                        break
                    except FileExistsError:
                        name = f'{ts}-{n}'
                        n += 1
                bfd = os.open(name, _RDIR, dir_fd=qroot)
            finally:
                os.close(qroot)
            st = os.fstat(bfd)
            if st.st_uid != os.getuid() or st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
                os.close(bfd)
                raise OSError(errno.EPERM, f"new batch {name} was replaced")
        except OSError as exc:
            raise RemoteError(
                f"Cannot create quarantine dir under {_quarantine_root(remote_dir)} "
                f"(each part must be a real directory, not a link): {exc}")
        qdir = _quarantine_root(remote_dir) / name
        try:
            for item in plan:
                rel = item['path']
                if not remote_verify.safe_blob_rel(rel):
                    missing.append(item)
                    continue
                rel_dir, entry = rel.rsplit('/', 1)
                try:
                    sdir = _walk(rfd, rel_dir)
                except OSError:
                    missing.append(item)  # absent, or reached through a link
                    continue
                try:
                    if not _lexists_in(sdir, entry):
                        missing.append(item)
                        continue
                    ddir = _walk(bfd, rel_dir, create=True, mode=0o755)
                    try:
                        # The parent dir is writable, so a rename works even
                        # though the blob file itself is read-only.
                        _move_entry(sdir, entry, ddir)
                    finally:
                        os.close(ddir)
                    moved.append({**item, 'quarantined_to': str(Path(name) / rel)})
                    if verbose:
                        print(f"  quarantined {rel}")
                except OSError as exc:
                    raise RemoteError(f"Failed to quarantine {rel}: {exc}")
                finally:
                    os.close(sdir)
        finally:
            # Record whatever was moved, even if a later move failed, so the
            # batch can still be restored.
            manifest.update({
                'quarantined_at': _dt.datetime.now(_dt.timezone.utc).isoformat(
                    timespec='seconds'),
                'entries': moved,
                'missing': missing,
            })
            try:
                _write_manifest(bfd, manifest)
            finally:
                os.close(bfd)
    finally:
        os.close(rfd)

    return {'quarantine_dir': str(qdir), 'moved': moved,
            'missing': missing, 'plan': plan, 'refused': refused}


def _write_manifest(bfd: int, manifest: dict) -> None:
    try:
        mfd = os.open(MANIFEST_FILENAME,
                      os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                      0o644, dir_fd=bfd)
        with os.fdopen(mfd, 'w') as f:
            json.dump(manifest, f, indent=2)
    except OSError as exc:
        raise RemoteError(f"Quarantined blobs but failed to write manifest: {exc}")


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
            fd = os.open(manifest, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'r') as f:
                if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
                    raise ValueError('not a regular file')
                data = json.load(f)
            info['count'] = len(data.get('entries', []))
            info['quarantined_at'] = data.get('quarantined_at')
        except (OSError, ValueError):
            pass
        out.append(info)
    return out


def _check_batch_owner(fd: int, path: Path) -> None:
    """A batch is restored only by the user who made it, and only while
    nobody else can have changed it."""
    import pwd
    st = os.fstat(fd)
    if st.st_uid != os.getuid():
        try:
            owner = pwd.getpwuid(st.st_uid).pw_name
        except KeyError:
            owner = str(st.st_uid)
        raise RemoteError(
            f"{path} belongs to another user ({owner}); only they can restore it")
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise RemoteError(
            f"{path} is writable by others; check its contents, then "
            f"'chmod -R go-w' the batch and retry")


def _restore_batch(rfd: int, qfd: int, qdir: Path, manifest_path: Path,
                   verbose: bool) -> Tuple[List[str], List[str], List[str]]:
    _check_batch_owner(qfd, qdir)
    # Distinguish an absent manifest from a present-but-corrupt one so the
    # operator isn't told a physically-present batch "doesn't exist".
    try:
        mfd = os.open(MANIFEST_FILENAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                      dir_fd=qfd)
    except FileNotFoundError:
        raise RemoteError(f"No quarantine manifest at {manifest_path}")
    except OSError as exc:
        raise RemoteError(
            f"Quarantine manifest at {manifest_path} is unreadable/corrupt: {exc}")
    try:
        if not stat.S_ISREG(os.fstat(mfd).st_mode):
            raise RemoteError(f"{manifest_path} is not a regular file")
        _check_batch_owner(mfd, manifest_path)
        with os.fdopen(mfd, 'r') as f:
            mfd = None
            data = json.load(f)
    except (OSError, ValueError) as exc:
        raise RemoteError(
            f"Quarantine manifest at {manifest_path} is unreadable/corrupt: {exc}")
    finally:
        if mfd is not None:
            os.close(mfd)
    if not isinstance(data, dict):
        raise RemoteError(f"Quarantine manifest at {manifest_path} is corrupt")

    restored, skipped, absent = [], [], []
    for item in data.get('entries', []):
        rel = item.get('path') if isinstance(item, dict) else None
        # Only plain blob paths, reached through real directories on both
        # sides (opened without following links).
        if not remote_verify.safe_blob_rel(rel):
            absent.append(str(rel))
            continue
        rel_dir, name = rel.rsplit('/', 1)
        try:
            sdir = _walk(qfd, rel_dir)
        except OSError:
            absent.append(rel)
            continue
        try:
            if not _lexists_in(sdir, name):
                absent.append(rel)  # quarantined blob is gone (already restored?)
                continue
            ddir = _walk(rfd, rel_dir, create=True)
            try:
                if _lexists_in(ddir, name):
                    skipped.append(rel)  # already re-pushed; leave quarantine copy
                    continue
                _move_entry(sdir, name, ddir)
                restored.append(rel)
                if verbose:
                    print(f"  restored {rel}")
            finally:
                os.close(ddir)
        except OSError as exc:
            raise RemoteError(f"Failed to restore {rel}: {exc}")
        finally:
            os.close(sdir)
    return restored, skipped, absent


def restore_quarantine(
    remote_dir: Path,
    timestamp: str,
    verbose: bool = False,
) -> dict:
    """Move a quarantine batch's blobs back to their original remote paths.

    Skips any blob whose original path is now occupied (e.g. it was already
    re-pushed) — that copy wins; the quarantined one stays put for forensics.
    """
    if not timestamp or '/' in timestamp or timestamp in ('.', '..'):
        raise RemoteError(f"Invalid quarantine batch name: {timestamp!r}")
    qdir = _quarantine_root(remote_dir) / timestamp
    manifest_path = qdir / MANIFEST_FILENAME
    rfd = _open_remote(remote_dir)
    try:
        try:
            qfd = _walk(rfd, '/'.join(
                (remote_verify.LEDGER_DIRNAME, QUARANTINE_DIRNAME, timestamp)))
        except OSError:
            raise RemoteError(f"No quarantine batch at {qdir}")
        try:
            restored, skipped, absent = _restore_batch(
                rfd, qfd, qdir, manifest_path, verbose)
        finally:
            os.close(qfd)
    finally:
        os.close(rfd)

    return {'restored': restored, 'skipped': skipped, 'absent': absent,
            'quarantine_dir': str(qdir)}
