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
  a superset. Additive (never deletes/overwrites), atomic (tmp + ``os.replace``),
  idempotent/resumable and optionally re-hash-verifying.

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
# A blob's identity is content-addressed and layout-independent: its md5 is the
# 2-char prefix directory name concatenated with the filename (``.dir`` objects
# carry that suffix on both the filename and nothing else). The same blob stored
# in a v2 remote (``ab/cdef...``) and a v3 remote (``files/md5/ab/cdef...``)
# shares the identity ``ab/cdef...`` even though the on-disk paths differ. We key
# every blob by that **logical key** (``"<prefix>/<name>"``) so the diff treats
# v2 and v3 copies of one blob as the same object and never re-copies it.


def _logical_key(hex_prefix: str, name: str) -> str:
    """Layout-independent content identity for a blob: ``"<prefix>/<name>"``."""
    return f"{hex_prefix}/{name}"


def _index_one_prefix(
    hex_prefix: str, dir_path: Path, remote_dir: Path,
) -> Dict[str, Tuple[str, int]]:
    """Index every real blob under one prefix dir.

    Returns ``{logical_key: (rel_path, size)}`` where ``rel_path`` is the blob's
    path relative to ``remote_dir`` (its actual on-disk, layout-specific path).
    Stray non-blob artefacts (e.g. leftover ``*.tmp`` whose name does not
    reconstruct a valid md5) are skipped so they are never diffed or copied.
    """
    out: Dict[str, Tuple[str, int]] = {}
    try:
        children = list(dir_path.iterdir())
    except OSError:
        return out
    for f in children:
        if not f.is_file():
            continue
        name = f.name
        expected = remote_verify.expected_md5_for_blob(hex_prefix, name)
        if not remote_verify._HASH_RE.match(expected):
            continue  # stray / *.tmp — not a real blob
        try:
            size = f.stat().st_size
        except OSError:
            size = 0
        out[_logical_key(hex_prefix, name)] = (str(f.relative_to(remote_dir)), size)
    return out


def index_remote(
    remote_dir: Path,
    jobs: Optional[int] = None,
    progress: bool = False,
) -> Tuple[Dict[str, Tuple[str, int]], str]:
    """Index a remote's blobs by layout-independent content identity.

    Returns ``(index, layout)`` where ``index`` maps each blob's logical key
    (``"<prefix>/<name>"``) to ``(rel_path, size)``. Walks the layout-aware
    prefix dirs in parallel.
    """
    from .archive import operations as ops
    from .errors import ArchiveError

    # An absent or never-pushed-to remote simply has no blobs — a legitimate
    # endpoint for a diff/merge (e.g. topping-up a freshly-created destination),
    # not an error.
    try:
        layout = ops.detect_source_layout(remote_dir)
    except ArchiveError:
        return {}, 'empty'
    entries = remote_verify._prefix_dirs(remote_dir, layout)
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()

    if progress:
        print(f"Indexing {remote_dir} ({layout}) across {len(entries)} "
              f"prefix(es) with {n_jobs} worker(s) ...",
              file=sys.stderr, flush=True)

    index: Dict[str, Tuple[str, int]] = {}
    with ThreadPoolExecutor(max_workers=n_jobs) as pool:
        futures = [
            pool.submit(_index_one_prefix, hexp, p, remote_dir)
            for _key, hexp, p in entries
        ]
        for fut in as_completed(futures):
            index.update(fut.result())
    return index, layout


def diff_remotes(
    a_dir: Path,
    b_dir: Path,
    jobs: Optional[int] = None,
    progress: bool = False,
) -> dict:
    """Compare two remotes' blob sets by content identity.

    Returns a dict with per-side counts/bytes and the sorted lists of logical
    keys only in A, only in B, and in common. ``_a_index`` / ``_b_index`` carry
    the raw indexes so :func:`merge_remotes` can reuse them without re-walking.
    """
    a_index, a_layout = index_remote(a_dir, jobs=jobs, progress=progress)
    b_index, b_layout = index_remote(b_dir, jobs=jobs, progress=progress)

    a_keys, b_keys = set(a_index), set(b_index)
    only_a = a_keys - b_keys
    only_b = b_keys - a_keys
    common = a_keys & b_keys

    def _bytes(index, keys):
        return sum(index[k][1] for k in keys)

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
        '_a_index': a_index, '_b_index': b_index,
    }


def _topup_one_prefix(
    keys: List[str],
    a_index: Dict[str, Tuple[str, int]],
    src_dir: Path,
    dst_dir: Path,
    verify: bool,
    group_writable: bool,
) -> Tuple[Dict[str, int], List[dict], List[dict]]:
    """Copy one prefix's worth of missing blobs from ``src_dir`` into ``dst_dir``.

    Each copy is atomic (write a ``.tmp`` sibling, optionally re-hash it, then
    ``os.replace`` onto the final name) and additive (an already-present
    destination blob is skipped, never overwritten). Returns
    ``(counts, mismatches, errors)``.
    """
    counts = {'copied': 0, 'copied_bytes': 0, 'skipped': 0,
              'mismatch': 0, 'errors': 0}
    mismatches: List[dict] = []
    errors: List[dict] = []

    for key in keys:
        rel, size = a_index[key]
        src = src_dir / rel
        dst = dst_dir / rel

        # Additive + idempotent: never clobber a blob already at the destination.
        if dst.exists():
            counts['skipped'] += 1
            continue

        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            if group_writable:
                utils.set_group_writable(dst.parent, setgid=True)
        except OSError as e:
            counts['errors'] += 1
            errors.append({'path': rel, 'error': str(e)})
            continue

        hex_prefix, name = key.split('/', 1)
        expected = remote_verify.expected_md5_for_blob(hex_prefix, name)
        tmp = dst.parent / f".{name}.{os.getpid()}.tmp"

        try:
            shutil.copyfile(src, tmp)
        except OSError as e:
            _unlink_quietly(tmp)
            counts['errors'] += 1
            errors.append({'path': rel, 'error': str(e)})
            continue

        if verify:
            try:
                actual = utils.md5_file(tmp)
            except OSError:
                actual = None
            if actual != expected:
                _unlink_quietly(tmp)
                counts['mismatch'] += 1
                mismatches.append({'path': rel, 'expected_md5': expected,
                                   'actual_md5': actual})
                continue

        cache_ops._make_readonly(tmp)
        try:
            os.replace(tmp, dst)
        except OSError as e:
            _unlink_quietly(tmp)
            counts['errors'] += 1
            errors.append({'path': rel, 'error': str(e)})
            continue

        counts['copied'] += 1
        counts['copied_bytes'] += size

    return counts, mismatches, errors


def _unlink_quietly(path: Path) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


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
    atomic and (with ``verify``) re-hashed against its content-addressed name
    before being published, so corruption is reported rather than propagated.
    Idempotent and resumable — a second run, or a run after an interruption,
    only copies what is still missing.

    With ``dry_run`` nothing is written; the returned dict reports the counts
    and bytes that *would* transfer. ``diff`` may carry a precomputed
    :func:`diff_remotes` result to avoid re-walking both remotes.
    """
    diff = diff or diff_remotes(src_dir, dst_dir, jobs=jobs, progress=progress)
    a_index = diff['_a_index']
    to_copy = diff['only_in_a']
    planned_bytes = diff['only_in_a_bytes']

    result = {
        'source': str(src_dir), 'dest': str(dst_dir),
        'src_layout': diff['a_layout'], 'dst_layout': diff['b_layout'],
        'dry_run': dry_run, 'verify': verify,
        'missing_in_dest': len(to_copy),
        'missing_in_dest_bytes': planned_bytes,
        'copied': 0, 'copied_bytes': 0, 'skipped': 0,
        'mismatch': 0, 'errors': 0,
        'mismatches': [], 'error_entries': [],
    }

    if dry_run or not to_copy:
        return result

    # Group the missing blobs by their 2-char prefix so each worker owns a
    # disjoint subtree (bounded fan-out: at most 256 tasks regardless of count).
    by_prefix: Dict[str, List[str]] = defaultdict(list)
    for key in to_copy:
        by_prefix[key.split('/', 1)[0]].append(key)

    from .archive import operations as ops
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()
    dst_dir.mkdir(parents=True, exist_ok=True)

    if progress:
        print(f"Topping up {dst_dir} from {src_dir}: {len(to_copy)} blob(s), "
              f"{utils.format_size(planned_bytes)} across {len(by_prefix)} "
              f"prefix(es) with {n_jobs} worker(s) ...",
              file=sys.stderr, flush=True)

    done = 0
    with ThreadPoolExecutor(max_workers=n_jobs) as pool:
        futures = {
            pool.submit(_topup_one_prefix, keys, a_index, src_dir, dst_dir,
                        verify, group_writable): prefix
            for prefix, keys in by_prefix.items()
        }
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

    path = Path(arg_str).expanduser().resolve()
    if must_exist and not path.exists():
        raise RemoteError(
            f"'{arg_str}' is neither a configured remote nor an existing path.")
    return str(path), path
