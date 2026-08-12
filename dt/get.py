"""Materialise DVC-tracked data without creating tracking files.

``dt get`` is to ``dvc get`` what ``dt import`` is to ``dvc import``: it copies
data out of a source repository, but writes no ``.dvc`` file, so the result is
plain data with no provenance and no way to ``dvc update`` it later. That is the
point -- it exists for handing a subset of a dataset to someone outside the
group, who is running their own pipeline and does not want our tracking files.

Two things make this better than a shell loop over ``dvc get``:

*Resolve once, fetch many.* ``dvc get`` re-clones the source repository's git
metadata on every invocation, so 82 samples cost 82 clones. Here the clone
happens once and every row is resolved against it.

*Subpaths inside a tracked directory.* ``dvc list`` resolves a path within the
tree regardless of how it is tracked, so a row may name a subdirectory of a
single large ``.dir`` output -- which is what makes it possible to hand over 82
of 412 samples without transferring the other 330.
"""

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from . import cache_ops
from . import remote as remote_mod
from . import tmp as tmp_mod
from . import utils
from .errors import GetError

# What to try, in order, when the source repo has no cache.type configured --
# which is the normal case for someone outside our setup. Deliberately excludes
# symlink: a symlink into a cache the recipient cannot read is not a copy of the
# data, it is a dangling pointer, and it would "succeed" silently.
DEFAULT_LINK_TYPES = ('reflink', 'hardlink', 'copy')

VALID_LINK_TYPES = ('reflink', 'hardlink', 'symlink', 'copy')


def resolve_link_types(link: Optional[str] = None) -> List[str]:
    """Decide the link-type preference order.

    Honours DVC's ``cache.type`` when we are inside a repo that sets it, so a
    ``dt get`` on NCI behaves like everything else here. Off NCI there is
    usually no config and no shared filesystem, and the chain falls through to a
    real copy on its own -- hardlink fails with ``EXDEV`` across filesystems.

    Args:
        link: Explicit ``--link`` override, or None to auto-detect.

    Returns:
        Ordered list of link types to attempt.

    Raises:
        GetError: If *link* names an unknown type.
    """
    if link:
        types = [t.strip() for t in link.split(',') if t.strip()]
        unknown = [t for t in types if t not in VALID_LINK_TYPES]
        if unknown:
            raise GetError(
                f"Unknown link type(s): {', '.join(unknown)}. "
                f"Choose from: {', '.join(VALID_LINK_TYPES)}"
            )
        return types

    try:
        result = subprocess.run(
            ['dvc', 'config', 'cache.type'],
            capture_output=True, text=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return list(DEFAULT_LINK_TYPES)

    configured = result.stdout.strip() if result.returncode == 0 else ''
    if not configured:
        return list(DEFAULT_LINK_TYPES)

    # cache.type is a preference list, not a single value (ours is
    # "hardlink,symlink"), so honour every entry in order.
    types = [t.strip() for t in configured.split(',') if t.strip() in VALID_LINK_TYPES]
    return types or list(DEFAULT_LINK_TYPES)


def list_source_files(
    source: Path,
    path: str,
    rev: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List the tracked files under *path*, with hashes.

    Uses ``dvc list`` rather than reading the ``.dir`` manifest directly,
    because it resolves a path inside a tracked directory without us having to
    know whether *path* is its own output or a subtree of a larger one.

    Args:
        source: Local clone of the source repository.
        path: Path within the repository. May be a subpath of a tracked dir.
        rev: Git revision to resolve against.

    Returns:
        List of ``{'relpath': str, 'md5': str, 'size': int}``, with *relpath*
        relative to *path*.

    Raises:
        GetError: If the listing fails or the path is not tracked.
    """
    cmd = ['dvc', 'list', '--json', '--show-hash', '--size', '-R', str(source), path]
    if rev:
        cmd += ['--rev', rev]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip().splitlines()
        detail = message[-1] if message else 'unknown error'
        # DVC reports the miss against our internal clone directory, which is
        # noise to the reader -- they asked for a path in a repository, not in
        # .dt/tmp/clones/<mangled-url>/.
        if 'No such file or directory' in detail or 'does not exist' in detail:
            raise GetError(f"Not found in the source repository: {path}")
        raise GetError(f"Could not list {path}: {detail.replace(str(source), '<source>')}")

    try:
        entries = json.loads(result.stdout or '[]')
    except json.JSONDecodeError as e:
        raise GetError(f"Could not parse dvc list output for {path}: {e}")

    files = []
    for entry in entries:
        if entry.get('isout') and entry.get('isdir'):
            # The directory output itself; its children are listed separately.
            continue
        md5 = entry.get('md5')
        relpath = entry.get('path')
        if not md5 or not relpath or md5.endswith('.dir'):
            continue
        files.append({
            'relpath': relpath,
            'md5': md5,
            'size': entry.get('size') or 0,
        })

    if not files:
        raise GetError(f"No tracked files found at {path}")
    return files


def _place_one(
    entry: Dict[str, object],
    cache_root: Path,
    dest_root: Path,
    link_types: Sequence[str],
    force: bool,
) -> Tuple[str, bool, str]:
    """Materialise a single object. Returns (relpath, ok, message)."""
    relpath = str(entry['relpath'])
    md5 = str(entry['md5'])
    dest = dest_root / relpath

    if dest.exists() and not force:
        return relpath, True, 'exists (use -f to overwrite)'

    source = cache_ops.find_source_file(md5, cache_root)
    if source is None:
        return relpath, False, f'object {md5[:8]} not in source cache'

    if dest.exists() and force:
        try:
            dest.unlink()
        except OSError as e:
            return relpath, False, f'could not replace: {e}'

    last = 'no link type succeeded'
    for link_type in link_types:
        ok, kind = cache_ops.link_file(source, dest, cache_type=link_type)
        if ok:
            # link_file protects cache entries with 0o444. That is right for a
            # cache and wrong here: the recipient owns this data and will want
            # to work with it. Hardlinks share the cache inode, so leave those
            # alone -- widening them would make the cache object writable too.
            if kind in ('reflink', 'copy'):
                try:
                    dest.chmod(0o644)
                except OSError:
                    pass
            return relpath, True, kind
        last = f'{link_type} failed'
    return relpath, False, last


def materialise(
    entries: List[Dict[str, object]],
    cache_root: Path,
    dest_root: Path,
    link_types: Sequence[str],
    jobs: int = 8,
    force: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Place every entry under *dest_root*, in parallel.

    Args:
        entries: Output of :func:`list_source_files`.
        cache_root: Cache holding the source objects.
        dest_root: Directory to materialise into.
        link_types: Ordered link types to attempt.
        jobs: Worker threads.
        force: Overwrite existing destination files.

    Returns:
        List of (relpath, success, message) tuples.
    """
    dest_root.mkdir(parents=True, exist_ok=True)

    def place(entry):
        return _place_one(entry, cache_root, dest_root, link_types, force)

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        return list(pool.map(place, entries))


def _resolve_source(
    repository: str,
    owner: Optional[str],
    rev: Optional[str],
    refresh: bool,
    verbose: bool,
) -> Tuple[Path, Path]:
    """Clone the source once and locate its cache. Returns (clone, cache_root)."""
    clone = tmp_mod.clone_repo(
        repository, owner=owner, refresh=refresh, verbose=verbose, rev=rev
    )

    found = remote_mod.find_local_remote_from_repo(repository, owner=owner)
    if not found:
        raise GetError(
            f"No locally-accessible cache or remote found for {repository}. "
            f"dt get copies from storage this machine can already reach; if the "
            f"data is only in a remote you have no mount for, use dvc get."
        )
    _, cache_path = found
    return clone, Path(cache_path)


def get_data(
    repository: str,
    path: str,
    out: Optional[str] = None,
    owner: Optional[str] = None,
    rev: Optional[str] = None,
    jobs: int = 8,
    force: bool = False,
    link: Optional[str] = None,
    refresh: bool = True,
    verbose: bool = False,
) -> Tuple[int, int]:
    """Materialise a single path from a source repository.

    Args:
        repository: Repository name, alias, or URL.
        path: Path within the repository, possibly inside a tracked directory.
        out: Destination. Defaults to the basename of *path*.
        owner: Owner override for short repository names.
        rev: Git revision to resolve against.
        jobs: Parallel workers.
        force: Overwrite existing files.
        link: Link-type override, e.g. ``copy`` or ``hardlink,copy``.
        refresh: Refresh the cached clone before resolving.
        verbose: Print per-file progress.

    Returns:
        (files_written, files_failed).

    Raises:
        GetError: If the source cannot be resolved.
    """
    clone, cache_root = _resolve_source(repository, owner, rev, refresh, verbose)
    link_types = resolve_link_types(link)
    dest_root = _dest_for(out, path)

    entries = list_source_files(clone, path, rev=rev)
    results = materialise(entries, cache_root, dest_root, link_types, jobs, force)
    return _tally(results, verbose)


def get_from_csv(
    csv_path: str,
    repository: str,
    out: Optional[str] = None,
    owner: Optional[str] = None,
    rev: Optional[str] = None,
    jobs: int = 8,
    force: bool = False,
    link: Optional[str] = None,
    path_col: str = 'path',
    filters: Optional[List[str]] = None,
    refresh: bool = True,
    verbose: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Materialise every path listed in a CSV, resolving the source once.

    Args:
        csv_path: CSV file listing paths to fetch.
        repository: Repository name, alias, or URL.
        out: Fallback destination directory for rows with no ``output`` cell.
        owner: Owner override for short repository names.
        rev: Git revision to resolve against.
        jobs: Parallel workers, shared across all rows.
        force: Overwrite existing files.
        link: Link-type override.
        path_col: CSV column holding the source path.
        filters: ``COL=VALUE`` / ``COL!=VALUE`` row filters, ANDed.
        refresh: Refresh the cached clone before resolving.
        verbose: Print per-file progress.

    Returns:
        List of (path, success, message) tuples, one per selected row.

    Raises:
        GetError: If the CSV or the source cannot be read.
    """
    try:
        targets = utils.read_csv_targets(csv_path, path_col, out, filters)
    except ValueError as e:
        raise GetError(str(e))

    if not targets:
        raise GetError(
            f"No rows selected from {csv_path}"
            + (f" with filter(s): {', '.join(filters)}" if filters else "")
        )

    clone, cache_root = _resolve_source(repository, owner, rev, refresh, verbose)
    link_types = resolve_link_types(link)

    results: List[Tuple[str, bool, str]] = []
    for i, (row_path, row_out) in enumerate(targets, 1):
        if not row_path:
            results.append(('(empty)', False, f'Missing {path_col}'))
            continue

        if verbose:
            print(f"\n[{i}/{len(targets)}] {row_path}")

        try:
            entries = list_source_files(clone, row_path, rev=rev)
            dest_root = _dest_for(row_out, row_path)
            placed = materialise(
                entries, cache_root, dest_root, link_types, jobs, force
            )
            written, failed = _tally(placed, verbose)
            if failed:
                results.append((
                    row_path, False, f'{written} written, {failed} failed'
                ))
            else:
                plural = '' if written == 1 else 's'
                results.append((
                    row_path, True, f'{written} file{plural} -> {dest_root}'
                ))
        except GetError as e:
            results.append((row_path, False, str(e)))

    return results


def _dest_for(out: Optional[str], path: str) -> Path:
    """Destination directory for a source path.

    ``-o fastqs/`` collects every row under ``fastqs/<basename>``, which is what
    makes a CSV of 82 sample directories land as 82 sibling directories.
    """
    if out is None:
        return Path(Path(path).name)
    out_p = Path(out)
    if out_p.is_dir() or out.endswith('/'):
        return out_p / Path(path).name
    return out_p


def _tally(results: List[Tuple[str, bool, str]], verbose: bool) -> Tuple[int, int]:
    """Count successes and failures, printing detail when asked."""
    written = sum(1 for _, ok, _ in results if ok)
    failed = len(results) - written
    if verbose:
        for relpath, ok, message in results:
            if not ok:
                print(f"  ✗ {relpath}: {message}")
    return written, failed
