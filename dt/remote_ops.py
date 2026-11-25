"""Move and copy DVC remotes between filesystem locations.

Operations on a locally-accessible DVC remote's blob tree:

- :func:`copy_remote` — duplicate a remote. Same-filesystem copies prefer
  reflink → hardlink → copy (space-efficient dedup); ``independent=True``
  forces a real byte copy (required cross-filesystem and when the duplicate
  must carry different/broader permissions than the source).
- :func:`move_remote` — relocate a remote. Same filesystem is an instant
  ``os.rename``; cross filesystem copies, **verifies the copy**, and only
  then deletes the source. Afterwards every ``.dvc/config`` remote whose URL
  resolved to the old path is repointed to the new one.
- :func:`diff_remotes` — report which blobs each of two remotes holds that
  the other lacks (diagnoses a half-finished migration).
- :func:`merge_remotes` — top-up one remote from another: copy blobs present
  in the source but missing from the destination, so the destination becomes
  a superset. Additive (never deletes/overwrites), atomic (a temporary file
  linked into place only while the name is free), idempotent/resumable and
  optionally re-hash-verifying.

The bulk transfer is parallelised per md5 prefix (like :mod:`dt.remote_verify`),
reusing the layout-aware prefix enumerator from :mod:`dt.archive.operations`.
"""

import errno
import fcntl
import os
import shutil
import stat
import subprocess
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import cache_ops
from . import remote as remote_mod
from . import remote_verify
from . import utils
from .errors import RemoteError


# --------------------------------------------------------------------------- #
# Filesystem helpers
# --------------------------------------------------------------------------- #

def _existing_ancestor(p: Path) -> Path:
    """Return the nearest existing ancestor of ``p`` (``p`` itself if it exists)."""
    p = Path(p)
    while not p.exists() and p.parent != p:
        p = p.parent
    return p


def same_filesystem(src: Path, dst: Path) -> bool:
    """True if ``src`` and ``dst`` live on the same filesystem (same ``st_dev``).

    ``dst`` need not exist yet; its nearest existing ancestor is used.
    """
    try:
        return src.stat().st_dev == _existing_ancestor(dst).stat().st_dev
    except OSError:
        return False


def _is_regular(p) -> bool:
    """A regular file at ``p`` itself (a link to one does not count)."""
    try:
        return stat.S_ISREG(os.lstat(p).st_mode)
    except OSError:
        return False


# --------------------------------------------------------------------------- #
# Transfer (descriptor-based; the source is a shared tree)
# --------------------------------------------------------------------------- #

def _prefix_entries(src: Path):
    """Return ``[(key, dir_path), ...]`` blob-prefix dirs, or [] if no layout."""
    from .archive import operations as ops
    from .errors import ArchiveError
    try:
        layout = ops.detect_source_layout(src)
    except ArchiveError:
        return None, []
    return layout, ops._enumerate_prefix_dirs(src, layout)


_RDIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _open_src_root(src: Path) -> int:
    """Open the source remote root (a link at the root itself must be the
    caller's or root's, as for ``dt remote perms``)."""
    from . import perms
    fd = perms._open_root(src)
    try:
        # _open_root yields an O_PATH fd; reopen it readable for listing.
        return os.open('.', _RDIR, dir_fd=fd)
    finally:
        os.close(fd)


def _open_rel(root_fd: int, parts) -> int:
    """Open ``root/parts...`` one component at a time without following links."""
    fd = os.dup(root_fd)
    try:
        for part in parts:
            nfd = os.open(part, _RDIR, dir_fd=fd)
            os.close(fd)
            fd = nfd
        return fd
    except Exception:
        os.close(fd)
        raise


def _blob_mode(dst_root: Path) -> int:
    """Read-only mode for copied objects: world-readable only if an existing
    destination root already grants world read."""
    try:
        if os.stat(dst_root).st_mode & stat.S_IROTH:
            return 0o444
    except OSError:
        pass
    return 0o440


def _blob_mode_fd(root_fd: int) -> int:
    """:func:`_blob_mode` for an open destination root."""
    try:
        if os.fstat(root_fd).st_mode & stat.S_IROTH:
            return 0o444
    except OSError:
        pass
    return 0o440


def _copy_from_fd(src_fd: int, dst: Path, mode: int) -> bool:
    """Copy the open source into a newly created ``dst`` (exclusive)."""
    try:
        dfd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        return False
    ok = False
    try:
        os.lseek(src_fd, 0, os.SEEK_SET)
        while True:
            chunk = os.read(src_fd, 1 << 20)
            if not chunk:
                break
            view = memoryview(chunk)
            while view:  # a write may be partial (network filesystems)
                n = os.write(dfd, view)
                view = view[n:]
        st = os.fstat(src_fd)
        os.utime(dfd, ns=(st.st_atime_ns, st.st_mtime_ns))
        os.fchmod(dfd, mode)
        ok = True
    finally:
        os.close(dfd)
        if not ok:
            try:
                os.unlink(dst)
            except OSError:
                pass
    return True


def _reflink_from_fd(src_fd: int, dst: Path, mode: int) -> bool:
    try:
        dfd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except OSError:
        return False
    try:
        fcntl.ioctl(dfd, cache_ops._FICLONE, src_fd)
        os.fchmod(dfd, mode)
        return True
    except OSError:
        try:
            os.unlink(dst)
        except OSError:
            pass
        return False
    finally:
        os.close(dfd)


def _transfer_entry(dir_fd: int, name: str, dst: Path, independent: bool,
                    mode: int) -> Tuple[str, int]:
    """Transfer the regular file ``name`` in ``dir_fd`` to ``dst``.

    The source is opened relative to its (already verified) directory and
    without following links. Returns ``(method, size)``; method is
    ``'skipped'`` if ``dst`` exists and ``'not-regular'`` if the entry is not
    a regular file.
    """
    if os.path.lexists(dst):
        return 'skipped', 0
    try:
        sfd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=dir_fd)
    except OSError as e:
        if e.errno == errno.ELOOP:
            return 'not-regular', 0
        raise RemoteError(f"Failed to open {name}: {e}")
    try:
        st = os.fstat(sfd)
        if not stat.S_ISREG(st.st_mode):
            return 'not-regular', 0
        if independent:
            if _copy_from_fd(sfd, dst, mode):
                return 'copy', st.st_size
            return 'skipped', 0
        if _reflink_from_fd(sfd, dst, mode):
            return 'reflink', st.st_size
        try:
            os.link(name, dst, src_dir_fd=dir_fd, follow_symlinks=False)
            # Make sure the name still referred to the inode we checked.
            if os.lstat(dst).st_ino == st.st_ino:
                return 'hardlink', st.st_size
            os.unlink(dst)
        except FileExistsError:
            return 'skipped', 0
        except OSError:
            pass
        if _copy_from_fd(sfd, dst, mode):
            return 'copy', st.st_size
        return 'skipped', 0
    finally:
        os.close(sfd)


def _transfer_file(src: Path, dst: Path, independent: bool,
                   mode: int = 0o444) -> str:
    """Transfer one file, returning the method used (or ``'skipped'``).

    ``independent=True`` forces a real byte copy (an independent inode).
    Otherwise the space-efficient chain reflink → hardlink → copy is tried;
    symlinks are deliberately never used (a symlinked remote would break the
    moment the source is removed and could not carry different permissions).
    The source must be a regular file; links are never followed.
    """
    if dst.exists():
        return 'skipped'
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        pfd = os.open(src.parent, _RDIR)
    except OSError as e:
        raise RemoteError(f"Cannot open {src.parent}: {e.strerror or e}")
    try:
        method, _size = _transfer_entry(pfd, src.name, dst, independent, mode)
    finally:
        os.close(pfd)
    if method == 'not-regular':
        raise RemoteError(f"Not a regular file: {src}")
    return method


# --------------------------------------------------------------------------- #
# Tree copy
# --------------------------------------------------------------------------- #

def _walk_layout(root_fd: int):
    """Yield ``(parts, kind)`` for the blob-holding dirs of a remote.

    ``kind`` is ``'dir'`` for a real prefix dir or ``'link'`` for a link where
    one belongs (also ``files``/``files/md5`` links). Uses only directory
    descriptors opened without following links.
    """
    from .archive import operations as ops

    def entries(fd):
        with os.scandir(fd) as it:
            return [(e.name, e.is_symlink(), e.is_dir(follow_symlinks=False)) for e in it]

    top = entries(root_fd)
    for name, is_link, is_dir in top:
        if ops._is_hex_prefix(name):
            if is_link:
                yield (name,), 'link'
            elif is_dir:
                yield (name,), 'dir'
    files = next(((l, d) for n, l, d in top if n == 'files'), None)
    if files is None:
        return
    if files[0]:
        yield ('files',), 'link'
        return
    if not files[1]:
        return
    ffd = _open_rel(root_fd, ('files',))
    try:
        sub_entries = entries(ffd)
    finally:
        os.close(ffd)
    md5 = next(((l, d) for n, l, d in sub_entries if n == 'md5'), None)
    if md5 is None:
        return
    if md5[0]:
        yield ('files', 'md5'), 'link'
        return
    if not md5[1]:
        return
    mfd = _open_rel(root_fd, ('files', 'md5'))
    try:
        for name, is_link, is_dir in entries(mfd):
            if ops._is_hex_prefix(name):
                if is_link:
                    yield ('files', 'md5', name), 'link'
                elif is_dir:
                    yield ('files', 'md5', name), 'dir'
    finally:
        os.close(mfd)


def _copy_one_prefix(
    root_fd: int, parts, dst: Path, independent: bool, mode: int,
) -> Tuple[int, int, int, List[str]]:
    """Copy every regular file in one prefix dir (reached from ``root_fd``).

    Returns ``(files, bytes, skipped, links)``; links are listed, not copied.
    """
    rel = '/'.join(parts)
    target = dst.joinpath(*parts)
    target.mkdir(parents=True, exist_ok=True)  # preserve empty prefix dirs
    files = total_bytes = skipped = 0
    links: List[str] = []
    try:
        pfd = _open_rel(root_fd, parts)
    except OSError:
        # Replaced since it was listed: treat as a link, copy nothing.
        return 0, 0, 0, [rel]
    try:
        with os.scandir(pfd) as it:
            names = [(e.name, e.is_symlink()) for e in it]
        for name, is_link in names:
            if is_link:
                links.append(f"{rel}/{name}")
                continue
            method, size = _transfer_entry(pfd, name, target / name, independent, mode)
            if method == 'skipped':
                skipped += 1
            elif method == 'not-regular':
                try:
                    if stat.S_ISLNK(os.stat(name, dir_fd=pfd, follow_symlinks=False).st_mode):
                        links.append(f"{rel}/{name}")
                except OSError:
                    pass
            else:
                files += 1
                total_bytes += size
    finally:
        os.close(pfd)
    return files, total_bytes, skipped, links


def copy_tree(
    src: Path,
    dst: Path,
    independent: bool = False,
    jobs: Optional[int] = None,
    group_writable: bool = False,
    progress: bool = False,
) -> dict:
    """Replicate ``src``'s blob tree into ``dst`` (parallel per prefix).

    Copies the per-prefix blob dirs plus any top-level regular files (e.g.
    ``ARCHIVED.yaml``). The ``.dt-verify`` ledger is intentionally not copied
    — it is verification cache and regenerates on the next verify.

    The source is a shared tree, so it is walked through directory
    descriptors opened without following links: only regular files that
    physically live in it are copied, and links are listed in ``links``.
    """
    from .archive import operations as ops

    layout, _entries = _prefix_entries(src)
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()
    mode = _blob_mode(dst)
    dst.mkdir(parents=True, exist_ok=True)

    totals = {'files': 0, 'bytes': 0, 'skipped': 0, 'links': []}
    root_fd = _open_src_root(src)
    try:
        dirs = []
        for parts, kind in _walk_layout(root_fd):
            if kind == 'link':
                totals['links'].append('/'.join(parts))
            else:
                dirs.append(parts)

        if progress:
            print(f"Copying {src} -> {dst} ({layout or 'empty'}) across "
                  f"{len(dirs)} prefix(es) with {n_jobs} worker(s) ...",
                  file=sys.stderr, flush=True)

        done = 0
        with ThreadPoolExecutor(max_workers=n_jobs) as pool:
            futures = {
                pool.submit(_copy_one_prefix, root_fd, parts, dst, independent, mode): parts
                for parts in dirs
            }
            for fut in as_completed(futures):
                files, total_bytes, skipped, links = fut.result()
                totals['links'].extend(links)
                totals['files'] += files
                totals['bytes'] += total_bytes
                totals['skipped'] += skipped
                done += 1
                if progress and done % 16 == 0:
                    print(f"  [{done:>3}/{len(dirs)}] {totals['files']} files, "
                          f"{utils.format_size(totals['bytes'])}",
                          file=sys.stderr, flush=True)

        # Top-level regular files (signpost etc.); skip the ledger dir / blob dirs.
        with os.scandir(root_fd) as it:
            top = [(e.name, e.is_symlink()) for e in it]
        for name, is_link in top:
            if is_link:
                totals['links'].append(name)
                continue
            method, _size = _transfer_entry(root_fd, name, dst / name, independent, mode)
            if method not in ('skipped', 'not-regular'):
                totals['files'] += 1
    finally:
        os.close(root_fd)

    if group_writable:
        from . import perms
        mode = utils.shared_dir_mode()
        utils.set_group_writable(dst)
        # Walk by directory handle; a link among the entries is never followed.
        for _root, dirs_, _files, root_fd in os.fwalk(dst, follow_symlinks=False):
            for d in dirs_:
                try:
                    dfd = os.open(d, perms._DIR_FLAGS, dir_fd=root_fd)
                except OSError:
                    continue
                try:
                    perms._fchmod_path_fd(dfd, mode)
                except OSError:
                    pass
                finally:
                    os.close(dfd)

    totals['links'] = sorted(set(totals['links']))
    totals['layout'] = layout
    return totals


# --------------------------------------------------------------------------- #
# Integrity checks for move
# --------------------------------------------------------------------------- #

def _size_presence_check(src: Path, dst: Path) -> List[str]:
    """Confirm every source blob exists at ``dst`` with matching size.

    Cheaper than a checksum verify (no hashing); used by ``move --quick``.
    Returns a list of human-readable problems (empty if all good).
    """
    _layout, entries = _prefix_entries(src)
    problems: List[str] = []
    for _key, pdir in entries:
        rel = pdir.relative_to(src)
        for entry in pdir.iterdir():
            if not _is_regular(entry):
                continue
            target = dst / rel / entry.name
            try:
                if not _is_regular(target):
                    problems.append(f"missing at destination: {rel / entry.name}")
                elif os.lstat(target).st_size != os.lstat(entry).st_size:
                    problems.append(f"size mismatch: {rel / entry.name}")
            except OSError as e:
                problems.append(f"unreadable: {rel / entry.name} ({e})")
    return problems


# --------------------------------------------------------------------------- #
# Config repointing
# --------------------------------------------------------------------------- #

def repoint_remotes(
    repo_path: Path, old_path: str, new_path: str, verbose: bool = True,
) -> List[Tuple[str, str, str]]:
    """Repoint every configured remote whose URL resolves to ``old_path``.

    Rewrites the path component in place (preserving scheme/host) via
    ``dvc remote modify``, in whichever scope (project or local) the remote
    is defined. Returns ``[(name, old_url, new_url), ...]``.
    """
    problem = utils.dvc_local_config_problem(repo_path)
    if problem:
        raise RemoteError(f"Refusing to write DVC config in {repo_path}: {problem}")
    old_resolved = str(Path(old_path))
    project_names = {n for n, _, _ in remote_mod.list_remotes(project_only=True)}
    changed: List[Tuple[str, str, str]] = []

    for name, url, _is_default in remote_mod.list_remotes(project_only=False):
        _host, path = remote_mod.parse_remote_url(url)
        if not path or str(Path(path)) != old_resolved:
            continue
        new_url = url.replace(path, new_path)
        cmd = ['dvc', 'remote', 'modify']
        if name not in project_names:
            cmd.append('--local')
        cmd += [name, 'url', new_url]
        proc = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RemoteError(
                f"Failed to repoint remote '{name}': {proc.stderr.strip()}")
        if verbose:
            print(f"  Repointed remote '{name}': {url} -> {new_url}")
        changed.append((name, url, new_url))
    return changed


# --------------------------------------------------------------------------- #
# High-level operations
# --------------------------------------------------------------------------- #

def copy_remote(
    remote_name: Optional[str],
    new_path: str,
    as_name: Optional[str] = None,
    independent: bool = False,
    group_writable: bool = False,
    jobs: Optional[int] = None,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
) -> dict:
    """Duplicate a remote's blob tree at ``new_path``.

    Optionally registers the duplicate as a new DVC remote named ``as_name``.
    """
    name, url, src = remote_verify.resolve_local_remote(remote_name)
    dst = Path(new_path).resolve()
    if dst.exists() and any(dst.iterdir()):
        raise RemoteError(f"Destination {dst} exists and is not empty.")

    cross_fs = not same_filesystem(src, dst)
    if cross_fs and not independent:
        # reflink/hardlink can't cross filesystems; the chain falls back to a
        # real copy anyway, but say so.
        if verbose:
            print(f"Note: {dst} is on a different filesystem; using real copies.")

    totals = copy_tree(src, dst, independent=independent, jobs=jobs,
                       group_writable=group_writable, progress=verbose)

    registered = None
    if as_name:
        repo_path = repo_path or utils.find_project_root()
        proc = subprocess.run(
            ['dvc', 'remote', 'add', as_name, str(dst)],
            cwd=repo_path, capture_output=True, text=True)
        if proc.returncode != 0 and 'already exists' not in proc.stderr:
            raise RemoteError(
                f"Copy completed, but failed to register remote "
                f"'{as_name}': {proc.stderr.strip()}")
        registered = as_name

    return {
        'source': name, 'source_path': str(src), 'dest': str(dst),
        'independent': independent, 'cross_fs': cross_fs,
        'registered': registered, **totals,
    }


def move_remote(
    remote_name: Optional[str],
    new_path: str,
    quick: bool = False,
    jobs: Optional[int] = None,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
) -> dict:
    """Relocate a remote to ``new_path`` and repoint ``.dvc/config``.

    Same filesystem → atomic ``os.rename``. Cross filesystem → copy, verify
    the copy (full checksum, or size+presence with ``quick``), then delete
    the source. The source is only removed once verification passes.
    """
    name, url, src = remote_verify.resolve_local_remote(remote_name)
    src_path = str(src)
    dst = Path(new_path).resolve()
    if dst.exists() and any(dst.iterdir()):
        raise RemoteError(f"Destination {dst} exists and is not empty.")
    repo_path = repo_path or utils.find_project_root()

    if same_filesystem(src, dst):
        if verbose:
            print(f"Moving (rename) {src} -> {dst}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.rename(src, dst)
        method = 'rename'
    else:
        if verbose:
            print(f"Cross-filesystem move: copy -> verify -> delete")
        # Fail fast on links at the layout level, before a long copy.
        root_fd = _open_src_root(src)
        try:
            early = ['/'.join(p) for p, kind in _walk_layout(root_fd) if kind == 'link']
        finally:
            os.close(root_fd)
        if early:
            raise RemoteError(
                f"Source contains {len(early)} symbolic link(s) in its blob "
                f"layout (e.g. {early[0]}); nothing was copied.")
        copied = copy_tree(src, dst, independent=True, jobs=jobs, progress=verbose)
        if copied['links']:
            raise RemoteError(
                f"Source contains {len(copied['links'])} symbolic link(s) where "
                f"blobs belong (not copied; e.g. {copied['links'][0]}). Source "
                f"left intact at {src}; the partial copy at {dst} should be "
                f"reviewed and removed.")

        if quick:
            problems = _size_presence_check(src, dst)
            if problems:
                raise RemoteError(
                    f"Destination failed size/presence check "
                    f"({len(problems)} problem(s)); source left intact. "
                    f"The copy at {dst} should be reviewed and removed.\n  "
                    + "\n  ".join(problems[:10]))
        else:
            totals, bad, incomplete, _layout = remote_verify.verify_remote(
                dst, jobs=jobs, use_ledger=False, full=True, progress=verbose)
            if incomplete:
                print(f"Note: {len(incomplete)} non-blob file(s) were copied to "
                      f"{dst} (e.g. {incomplete[0].get('path')}); review them.",
                      file=sys.stderr)
            if bad:
                raise RemoteError(
                    f"Destination failed checksum verification "
                    f"({len(bad)} bad blob(s)); source left intact at {src}. "
                    f"The copy at {dst} contains them and should be reviewed "
                    f"and removed.")

        if verbose:
            print(f"Verification passed; removing old remote at {src}")
        shutil.rmtree(src)
        method = 'copy+verify+delete'

    repointed = repoint_remotes(repo_path, src_path, str(dst), verbose=verbose)
    return {
        'source': name, 'old_path': src_path, 'dest': str(dst),
        'method': method, 'quick': quick, 'repointed': repointed,
    }


# --------------------------------------------------------------------------- #
# Diff and top-up (merge) of two local remotes (issue #166)
# --------------------------------------------------------------------------- #
#
# A half-finished migration leaves blobs split across an old and a new remote
# with neither a superset of the other. ``dvc pull`` against the configured
# (new) remote then fails for the blobs that only ever made it to the old
# location, and they look simply "missing".
#
# A remote holds two object namespaces, and DVC 3 reads them separately:
# legacy (``md5-dos2unix``) objects at the root (``ab/cdef...``) and ``md5``
# objects under ``files/md5/ab/cdef...``. The same name in the two is not
# interchangeable -- a root name hashes the text with CRLF folded to LF, and a
# reader looks only in the namespace its ``.dvc`` file's hash names. So blobs
# are keyed by their path in the remote (namespace + prefix + name), compared
# namespace by namespace, and copied to the same path in the destination.


def _namespace(parts) -> str:
    """``'legacy'`` for a root prefix dir, ``'md5'`` for ``files/md5/..``."""
    return 'md5' if tuple(parts[:-1]) == ('files', 'md5') else 'legacy'


def _index_one_prefix(
    root_fd: int, parts: Tuple[str, ...],
) -> Tuple[Dict[str, int], List[str]]:
    """Index every real blob in one prefix dir (reached from ``root_fd``).

    Returns ``({rel_path: size}, unreadable)``. Only regular files that live
    in the dir count; links and stray non-blob artefacts (e.g. a leftover
    ``*.tmp``) are skipped. ``unreadable`` lists the dir or entries that could
    not be read, so a caller never takes them for absent.
    """
    out: Dict[str, int] = {}
    bad: List[str] = []
    rel_dir = '/'.join(parts)
    hex_prefix = parts[-1]
    try:
        pfd = _open_rel(root_fd, parts)
    except OSError:
        return out, [rel_dir]
    try:
        try:
            with os.scandir(pfd) as it:
                entries = list(it)
        except OSError:
            return out, [rel_dir]
        for ent in entries:
            expected = remote_verify.expected_md5_for_blob(hex_prefix, ent.name)
            if not remote_verify._HASH_RE.fullmatch(expected):
                continue  # stray / *.tmp — not a real blob
            try:
                if not ent.is_file(follow_symlinks=False):
                    continue
                size = ent.stat(follow_symlinks=False).st_size
            except OSError:
                bad.append(f"{rel_dir}/{ent.name}")
                continue
            out[f"{rel_dir}/{ent.name}"] = size
    finally:
        os.close(pfd)
    return out, bad


def index_remote(
    remote_dir: Path,
    jobs: Optional[int] = None,
    progress: bool = False,
    allow_empty: bool = True,
) -> Tuple[Dict[str, int], str, List[str]]:
    """Index a remote's blobs by their path in the remote.

    Returns ``(index, layout, unreadable)``; ``index`` maps each blob's
    remote-relative path to its size. Prefix dirs are walked in parallel
    through handles opened without following links; a link where a prefix
    dir belongs is not entered.

    A location with no DVC layout yields ``'empty'`` when ``allow_empty``
    (a destination that has not been written to yet) and only if it is
    absent or a readable, empty directory; anything else is an error, so a
    mistyped or unreadable source is never taken for an empty one.
    """
    from .archive import operations as ops
    from .errors import ArchiveError

    try:
        layout = ops.detect_source_layout(remote_dir)
    except ArchiveError:
        if allow_empty:
            try:
                with os.scandir(remote_dir) as it:
                    empty = next(it, None) is None
            except FileNotFoundError:
                empty = True
            except OSError as e:
                raise RemoteError(f"Cannot read {remote_dir}: {e.strerror or e}")
            if empty:
                return {}, 'empty', []
        raise RemoteError(f"{remote_dir} does not look like a DVC remote "
                          f"(no object directories found).")
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()

    index: Dict[str, int] = {}
    unreadable: List[str] = []
    root_fd = _open_src_root(remote_dir)
    try:
        dirs = [parts for parts, kind in _walk_layout(root_fd) if kind == 'dir']
        if progress:
            print(f"Indexing {remote_dir} ({layout}) across {len(dirs)} "
                  f"prefix(es) with {n_jobs} worker(s) ...",
                  file=sys.stderr, flush=True)
        with ThreadPoolExecutor(max_workers=n_jobs) as pool:
            futures = [pool.submit(_index_one_prefix, root_fd, tuple(parts))
                       for parts in dirs]
            for fut in as_completed(futures):
                part, bad = fut.result()
                index.update(part)
                unreadable.extend(bad)
    finally:
        os.close(root_fd)
    return index, layout, sorted(unreadable)


def diff_remotes(
    a_dir: Path,
    b_dir: Path,
    jobs: Optional[int] = None,
    progress: bool = False,
    b_may_be_empty: bool = True,
) -> dict:
    """Compare two remotes' blob sets, namespace by namespace.

    Returns per-side counts/bytes, the sorted remote-relative paths only in
    A, only in B, and in common, and any paths either side could not read.
    ``_a_index`` / ``_b_index`` carry the raw indexes so :func:`merge_remotes`
    can reuse them without re-walking.
    """
    a_index, a_layout, a_bad = index_remote(a_dir, jobs=jobs, progress=progress,
                                            allow_empty=False)
    b_index, b_layout, b_bad = index_remote(b_dir, jobs=jobs, progress=progress,
                                            allow_empty=b_may_be_empty)

    a_keys, b_keys = set(a_index), set(b_index)
    only_a = a_keys - b_keys
    only_b = b_keys - a_keys
    common = a_keys & b_keys
    size_differs = sorted(k for k in common if a_index[k] != b_index[k])

    def _bytes(index, keys):
        return sum(index[k] for k in keys)

    return {
        'a_dir': str(a_dir), 'b_dir': str(b_dir),
        'a_layout': a_layout, 'b_layout': b_layout,
        'a_objects': len(a_keys), 'b_objects': len(b_keys),
        'a_bytes': _bytes(a_index, a_keys), 'b_bytes': _bytes(b_index, b_keys),
        'only_in_a': sorted(only_a), 'only_in_b': sorted(only_b),
        'only_in_a_count': len(only_a), 'only_in_b_count': len(only_b),
        'only_in_a_bytes': _bytes(a_index, only_a),
        'only_in_b_bytes': _bytes(b_index, only_b),
        'common_count': len(common), 'common_bytes': _bytes(a_index, common),
        'size_differs': size_differs,
        'a_unreadable': a_bad, 'b_unreadable': b_bad,
        '_a_index': a_index, '_b_index': b_index,
    }


def _shared_new_dirs(dst_root_fd: int, group_writable: bool) -> Optional[int]:
    """Mode for directories the merge creates, or None to leave the umask's.

    New prefix dirs follow a shared destination: if its root is group
    writable or setgid they get the shared-directory mode, as with
    ``--group-writable``, so other members' pushes into them keep working.
    """
    if group_writable:
        return utils.shared_dir_mode()
    try:
        st = os.fstat(dst_root_fd)
    except OSError:
        return None
    if st.st_mode & (stat.S_IWGRP | stat.S_ISGID):
        return utils.shared_dir_mode()
    return None


def _open_dest_dir(root_fd: int, parts, new_mode: Optional[int]) -> int:
    """Open (creating as needed) ``root/parts...`` one component at a time.

    Nothing is followed: a link where a directory belongs is an error. Only
    directories created here, and still this user's own when opened, get
    ``new_mode``.
    """
    from . import perms
    fd = os.dup(root_fd)
    try:
        for part in parts:
            created = False
            try:
                os.mkdir(part, 0o777, dir_fd=fd)
                created = True
            except FileExistsError:
                pass
            nfd = os.open(part, _RDIR, dir_fd=fd)
            os.close(fd)
            fd = nfd
            if created and new_mode is not None \
                    and os.fstat(fd).st_uid == os.geteuid():
                perms._fchmod_path_fd(fd, new_mode)
        return fd
    except Exception:
        os.close(fd)
        raise


def _entry_kind(dir_fd: int, name: str) -> Optional[str]:
    """``None`` if nothing is at ``name``; else ``'file'`` or ``'other'``."""
    try:
        st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    return 'file' if stat.S_ISREG(st.st_mode) else 'other'


def _link_open_file(fd: int, dir_fd: int, name: str) -> bool:
    """Give the open file ``fd`` the name ``name`` unless something has it.

    Links exactly the inode behind ``fd`` (through ``/proc/self/fd``), so
    whatever is at the temporary name by now does not matter. Returns False,
    leaving the existing entry untouched, if ``name`` exists. Raises if the
    filesystem cannot link -- there is no fallback that could replace.
    """
    try:
        os.link(f'/proc/self/fd/{fd}', name, dst_dir_fd=dir_fd,
                follow_symlinks=True)
        return True
    except FileExistsError:
        return False


def _md5_matches(dfd: int, plain: str, expected: str, namespace: str) -> bool:
    """Whether the written bytes carry the name they will be published under.

    Root (legacy) names are ``md5-dos2unix`` -- CRLF folded to LF for text --
    so those are also checked that way before being called a mismatch.
    """
    if plain == expected:
        return True
    if namespace != 'legacy':
        return False
    try:
        return utils._dvc_file_md5(f'/proc/self/fd/{dfd}',
                                   name='md5-dos2unix') == expected
    except Exception:
        return False


def _copy_blob(src_dir_fd: int, dst_dir_fd: int, name: str, expected: str,
               namespace: str, verify: bool, mode: int,
               ) -> Tuple[str, int, Optional[str]]:
    """Copy blob ``name`` between two directory handles.

    The source is opened without following links and must be a regular
    file. The bytes go to an exclusively created temporary file (hashed on
    the way through when ``verify``) whose inode is then linked under
    ``name`` only if nothing is there yet. Returns ``(status, size,
    actual_md5)`` with status ``'copied'``, ``'skipped'``, ``'occupied'``
    (something other than a file holds the name), ``'mismatch'``,
    ``'short'`` (fewer bytes than the source's size) or ``'not-regular'``.
    """
    import hashlib
    import secrets

    kind = _entry_kind(dst_dir_fd, name)
    if kind == 'file':
        return 'skipped', 0, None
    if kind == 'other':
        return 'occupied', 0, None
    try:
        sfd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                      dir_fd=src_dir_fd)
    except OSError as e:
        if e.errno == errno.ELOOP:
            return 'not-regular', 0, None
        raise
    try:
        st = os.fstat(sfd)
        if not stat.S_ISREG(st.st_mode):
            return 'not-regular', 0, None
        tmp = f".{secrets.token_urlsafe(16)}.tmp"  # the shape `dt remote clean` sweeps
        dfd = os.open(tmp, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                      0o600, dir_fd=dst_dir_fd)
        try:
            h = hashlib.md5() if verify else None
            size = 0
            while True:
                chunk = os.read(sfd, 1 << 20)
                if not chunk:
                    break
                if h is not None:
                    h.update(chunk)
                size += len(chunk)
                view = memoryview(chunk)
                while view:  # a write may be partial (network filesystems)
                    n = os.write(dfd, view)
                    view = view[n:]
            if size != st.st_size:
                return 'short', size, None
            if h is not None and not _md5_matches(dfd, h.hexdigest(), expected,
                                                  namespace):
                return 'mismatch', size, h.hexdigest()
            os.fchmod(dfd, mode)
            if not _link_open_file(dfd, dst_dir_fd, name):
                return 'skipped', 0, None
            # Stamp after publishing: a finished temporary file must not look
            # old enough for a concurrent sweep while it is still in use.
            try:
                os.utime(dfd, ns=(st.st_atime_ns, st.st_mtime_ns))
            except OSError:
                pass
            return 'copied', size, None
        finally:
            os.close(dfd)
            try:
                os.unlink(tmp, dir_fd=dst_dir_fd)
            except OSError:
                pass  # left for `dt remote clean`; never the published name
    finally:
        os.close(sfd)


def _topup_one_prefix(
    keys: List[str],
    src_root_fd: int,
    dst_root_fd: int,
    verify: bool,
    new_dir_mode: Optional[int],
    mode: int,
    stop,
) -> Tuple[Dict[str, int], List[dict], List[dict]]:
    """Copy one prefix's worth of missing blobs between the two remote roots.

    Additive: a blob already at the destination is skipped, never
    overwritten. Returns ``(counts, mismatches, errors)``.
    """
    counts = {'copied': 0, 'copied_bytes': 0, 'skipped': 0,
              'mismatch': 0, 'errors': 0}
    mismatches: List[dict] = []
    errors: List[dict] = []
    src_fds: Dict[Tuple[str, ...], int] = {}
    dst_fds: Dict[Tuple[str, ...], int] = {}
    broken: Dict[Tuple[str, ...], str] = {}

    def _err(rel, msg):
        counts['errors'] += 1
        errors.append({'path': rel, 'error': msg})

    try:
        for rel in keys:
            if stop.is_set():
                break
            parts = tuple(rel.split('/'))
            dir_parts, name = parts[:-1], parts[-1]
            # The plan names a remote path; accept only the shapes a remote uses.
            if (not dir_parts or dir_parts[:-1] not in ((), ('files', 'md5'))
                    or not remote_verify._HASH_RE.fullmatch(
                        remote_verify.expected_md5_for_blob(dir_parts[-1], name))):
                _err(rel, 'unexpected blob path')
                continue
            expected = remote_verify.expected_md5_for_blob(dir_parts[-1], name)
            if dir_parts in broken:
                _err(rel, broken[dir_parts])
                continue
            try:
                if dir_parts not in src_fds:
                    src_fds[dir_parts] = _open_rel(src_root_fd, dir_parts)
                if dir_parts not in dst_fds:
                    dst_fds[dir_parts] = _open_dest_dir(
                        dst_root_fd, dir_parts, new_dir_mode)
            except OSError as e:
                broken[dir_parts] = str(e)
                _err(rel, str(e))
                continue
            try:
                status, size, actual = _copy_blob(
                    src_fds[dir_parts], dst_fds[dir_parts], name, expected,
                    _namespace(dir_parts), verify, mode)
            except OSError as e:
                _err(rel, str(e))
                continue
            if status == 'copied':
                counts['copied'] += 1
                counts['copied_bytes'] += size
            elif status == 'skipped':
                counts['skipped'] += 1
            elif status == 'mismatch':
                counts['mismatch'] += 1
                mismatches.append({'path': rel, 'expected_md5': expected,
                                   'actual_md5': actual})
            elif status == 'occupied':
                _err(rel, 'destination name is held by something other than a file')
            elif status == 'short':
                _err(rel, 'source changed while copying (short read)')
            else:
                _err(rel, 'not a regular file')
    finally:
        for fd in list(src_fds.values()) + list(dst_fds.values()):
            os.close(fd)

    return counts, mismatches, errors


def merge_remotes(
    src_dir: Path,
    dst_dir: Path,
    dry_run: bool = False,
    verify: bool = False,
    jobs: Optional[int] = None,
    group_writable: bool = False,
    progress: bool = False,
    diff: Optional[dict] = None,
) -> dict:
    """Top-up ``dst_dir`` with every blob present in ``src_dir`` but missing from it.

    Additive: never deletes or overwrites a destination blob. Each copy is
    written to a temporary file and linked into place only if the name is
    still free, and (with ``verify``) re-hashed against its content-addressed
    name first, so corruption is reported rather than propagated. Idempotent
    and resumable — a second run, or a run after an interruption, only copies
    what is still missing. Blobs are copied to the same path (and so the same
    namespace) they have in the source.

    With ``dry_run`` nothing is written; the returned dict reports the counts
    and bytes that *would* transfer. ``diff`` may carry a precomputed
    :func:`diff_remotes` result to avoid re-walking both remotes.
    """
    import threading

    diff = diff or diff_remotes(src_dir, dst_dir, jobs=jobs, progress=progress)
    to_copy = diff['only_in_a']
    planned_bytes = diff['only_in_a_bytes']

    result = {
        'source': str(src_dir), 'dest': str(dst_dir),
        'src_layout': diff['a_layout'], 'dst_layout': diff['b_layout'],
        'dry_run': dry_run, 'verify': verify,
        'missing_in_dest': len(to_copy),
        'missing_in_dest_bytes': planned_bytes,
        'unreadable': (list(diff.get('a_unreadable', []))
                       + list(diff.get('b_unreadable', []))),
        'copied': 0, 'copied_bytes': 0, 'skipped': 0,
        'mismatch': 0, 'errors': 0,
        'mismatches': [], 'error_entries': [],
    }

    if dry_run or not to_copy:
        return result

    # Group the missing blobs by prefix dir so each worker owns a disjoint
    # subtree (bounded fan-out: at most 512 tasks regardless of count).
    by_prefix: Dict[str, List[str]] = defaultdict(list)
    for rel in to_copy:
        by_prefix[rel.rsplit('/', 1)[0]].append(rel)

    from .archive import operations as ops
    from . import perms
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()

    if progress:
        print(f"Topping up {dst_dir} from {src_dir}: {len(to_copy)} blob(s), "
              f"{utils.format_size(planned_bytes)} across {len(by_prefix)} "
              f"prefix(es) with {n_jobs} worker(s) ...",
              file=sys.stderr, flush=True)

    done = 0
    stop = threading.Event()
    if not os.path.lexists(dst_dir):
        Path(dst_dir).mkdir(parents=True)
    src_root_fd = _open_src_root(src_dir)
    try:
        dst_root_fd = perms._open_root(dst_dir)
    except OSError as e:
        os.close(src_root_fd)
        raise RemoteError(f"Cannot open {dst_dir}: {e.strerror or e}")
    pool = ThreadPoolExecutor(max_workers=n_jobs)
    try:
        mode = _blob_mode_fd(dst_root_fd)
        new_dir_mode = _shared_new_dirs(dst_root_fd, group_writable)
        futures = [
            pool.submit(_topup_one_prefix, keys, src_root_fd, dst_root_fd,
                        verify, new_dir_mode, mode, stop)
            for keys in by_prefix.values()
        ]
        for fut in as_completed(futures):
            counts, mismatches, errors = fut.result()
            for k in ('copied', 'copied_bytes', 'skipped', 'mismatch', 'errors'):
                result[k] += counts[k]
            result['mismatches'].extend(mismatches)
            result['error_entries'].extend(errors)
            done += 1
            if progress and done % 16 == 0:
                print(f"  [{done:>3}/{len(by_prefix)}] {result['copied']} copied, "
                      f"{utils.format_size(result['copied_bytes'])}",
                      file=sys.stderr, flush=True)
    except BaseException:
        # Interrupted (e.g. Ctrl-C): stop queued and running prefixes, and
        # keep the shared handles open until every worker has returned.
        stop.set()
        pool.shutdown(wait=True, cancel_futures=True)
        raise
    finally:
        pool.shutdown(wait=True)
        os.close(src_root_fd)
        os.close(dst_root_fd)

    result['mismatches'].sort(key=lambda e: e['path'])
    result['error_entries'].sort(key=lambda e: e['path'])
    return result


def resolve_remote_arg(
    arg: str, must_exist: bool = True,
) -> Tuple[str, Path]:
    """Resolve a diff/merge argument to ``(label, local_path)``.

    A bare token that matches a configured remote name resolves to that
    remote's local path (via :func:`remote_verify.resolve_local_remote`, which
    rejects cloud/non-local/archived remotes). Anything that looks like a path
    — contains ``/``, starts with ``.``/``~``, or is not a known remote name —
    is treated as a filesystem path. This lets ``merge``/``diff`` operate on an
    *old* location that is not (or no longer) a configured remote.
    """
    arg_str = str(arg)
    if '/' not in arg_str and not arg_str.startswith(('.', '~')):
        try:
            names = {n for n, _, _ in remote_mod.list_remotes(project_only=False)}
        except Exception:
            names = set()
        if arg_str in names:
            name, _url, path = remote_verify.resolve_local_remote(arg_str)
            return name, path

    # Absolute, but links are left for the root check when it is opened
    # (a link at a remote root must be this user's or root's own).
    path = Path(os.path.abspath(os.path.expanduser(arg_str)))
    if not os.path.lexists(path):
        if must_exist:
            raise RemoteError(
                f"'{arg_str}' is neither a configured remote nor an existing path.")
        if '/' not in arg_str and not arg_str.startswith(('.', '~')):
            # A bare word is a remote name; never turn a typo into a new
            # directory beside the current one.
            raise RemoteError(
                f"'{arg_str}' is not a configured remote. To create a new "
                f"directory, give it as a path (e.g. ./{arg_str}).")
    return str(path), path
