"""Move and copy DVC remotes between filesystem locations.

Two operations on a locally-accessible DVC remote's blob tree:

- :func:`copy_remote` — duplicate a remote. Same-filesystem copies prefer
  reflink → hardlink → copy (space-efficient dedup); ``independent=True``
  forces a real byte copy (required cross-filesystem and when the duplicate
  must carry different/broader permissions than the source).
- :func:`move_remote` — relocate a remote. Same filesystem is an instant
  ``os.rename``; cross filesystem copies, **verifies the copy**, and only
  then deletes the source. Afterwards every ``.dvc/config`` remote whose URL
  resolved to the old path is repointed to the new one.

The bulk transfer is parallelised per md5 prefix (like :mod:`dt.remote_verify`),
reusing the layout-aware prefix enumerator from :mod:`dt.archive.operations`.
"""

import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

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
