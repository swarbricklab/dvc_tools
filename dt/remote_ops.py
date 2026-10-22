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

import os
import shutil
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


def _transfer_file(src: Path, dst: Path, independent: bool) -> str:
    """Transfer one file, returning the method used (or ``'skipped'``).

    ``independent=True`` forces a real byte copy (an independent inode).
    Otherwise the space-efficient chain reflink → hardlink → copy is tried;
    symlinks are deliberately never used (a symlinked remote would break the
    moment the source is removed and could not carry different permissions).
    """
    if dst.exists():
        return 'skipped'
    dst.parent.mkdir(parents=True, exist_ok=True)

    if independent:
        if cache_ops._try_copy(src, dst):
            cache_ops._make_readonly(dst)
            return 'copy'
        raise RemoteError(f"Failed to copy {src} -> {dst}")

    if cache_ops._try_reflink(src, dst):
        cache_ops._make_readonly(dst)
        return 'reflink'
    if cache_ops._try_hardlink(src, dst):
        return 'hardlink'
    if cache_ops._try_copy(src, dst):
        cache_ops._make_readonly(dst)
        return 'copy'
    raise RemoteError(f"Failed to transfer {src} -> {dst}")


# --------------------------------------------------------------------------- #
# Tree copy
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


def _copy_one_prefix(
    key: str, pdir: Path, src: Path, dst: Path, independent: bool,
) -> Tuple[int, int, int]:
    """Copy every file under one prefix dir. Returns (files, bytes, skipped)."""
    rel = pdir.relative_to(src)
    target = dst / rel
    target.mkdir(parents=True, exist_ok=True)  # preserve empty prefix dirs
    files = total_bytes = skipped = 0
    for entry in pdir.iterdir():
        if not entry.is_file():
            continue
        method = _transfer_file(entry, target / entry.name, independent)
        if method == 'skipped':
            skipped += 1
        else:
            files += 1
            try:
                total_bytes += entry.stat().st_size
            except OSError:
                pass
    return files, total_bytes, skipped


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
    """
    from .archive import operations as ops

    layout, entries = _prefix_entries(src)
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()
    dst.mkdir(parents=True, exist_ok=True)

    totals = {'files': 0, 'bytes': 0, 'skipped': 0}
    if progress:
        print(f"Copying {src} -> {dst} ({layout or 'empty'}) across "
              f"{len(entries)} prefix(es) with {n_jobs} worker(s) ...",
              file=sys.stderr, flush=True)

    done = 0
    with ThreadPoolExecutor(max_workers=n_jobs) as pool:
        futures = {
            pool.submit(_copy_one_prefix, key, p, src, dst, independent): key
            for key, p in entries
        }
        for fut in as_completed(futures):
            files, total_bytes, skipped = fut.result()
            totals['files'] += files
            totals['bytes'] += total_bytes
            totals['skipped'] += skipped
            done += 1
            if progress and done % 16 == 0:
                print(f"  [{done:>3}/{len(entries)}] {totals['files']} files, "
                      f"{utils.format_size(totals['bytes'])}",
                      file=sys.stderr, flush=True)

    # Top-level regular files (signpost etc.); skip the ledger dir / blob dirs.
    for entry in src.iterdir():
        if entry.is_file():
            method = _transfer_file(entry, dst / entry.name, independent)
            if method != 'skipped':
                totals['files'] += 1

    if group_writable:
        utils.set_group_writable(dst)
        for root, dirs, _files in os.walk(dst):
            for d in dirs:
                utils.set_group_writable(Path(root) / d)

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
            if not entry.is_file():
                continue
            target = dst / rel / entry.name
            try:
                if not target.is_file():
                    problems.append(f"missing at destination: {rel / entry.name}")
                elif target.stat().st_size != entry.stat().st_size:
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
        copy_tree(src, dst, independent=True, jobs=jobs, progress=verbose)

        if quick:
            problems = _size_presence_check(src, dst)
            if problems:
                raise RemoteError(
                    f"Destination failed size/presence check "
                    f"({len(problems)} problem(s)); source left intact.\n  "
                    + "\n  ".join(problems[:10]))
        else:
            totals, bad, incomplete, _layout = remote_verify.verify_remote(
                dst, jobs=jobs, use_ledger=False, full=True, progress=verbose)
            if bad:
                raise RemoteError(
                    f"Destination failed checksum verification "
                    f"({len(bad)} bad blob(s)); source left intact at {src}.")

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
