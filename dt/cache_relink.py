"""Relink the local DVC cache onto a local remote.

Replaces every regular file in the primary DVC cache with a symlink to the
corresponding content-addressed object in a locally-accessible remote. On a
shared-filesystem HPC setup the remote already holds a good copy of every
pushed object, so keeping a second physical copy in each clone's cache is pure
duplication. Relinking reclaims that space while leaving the cache fully
functional: ``dvc checkout`` still finds each object at its usual cache path,
it is just a symlink now.

The mapping is trivial because both stores are content-addressed by the same
md5: a cache blob and its remote twin share the md5 that names them, so the
relink is *existence-only* -- we never re-hash. That is only safe if the remote
is known good, which is why :func:`relink_cache` runs ``dt remote verify``
first by default (incremental via the remote's ledger, so repeat runs are
cheap) and refuses to relink onto a remote with bad blobs unless forced.

Objects present in the cache but absent from the remote are left untouched:
they may not have been pushed yet, and symlinking them would dangle.
"""

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from . import remote as remote_mod
from . import remote_verify as remote_verify_mod
from . import utils
from .errors import RemoteError


@dataclass
class RelinkStats:
    """Outcome of a relink sweep."""

    relinked: int = 0
    already_linked: int = 0
    missing: int = 0
    failed: int = 0
    bytes_reclaimed: int = 0
    missing_md5s: List[str] = field(default_factory=list)
    failures: List[Tuple[str, str]] = field(default_factory=list)


def _iter_cache_blobs(cache_root: Path, layout: str) -> Iterator[Tuple[str, Path]]:
    """Yield ``(md5, blob_path)`` for every object file in the cache.

    ``md5`` keeps a trailing ``.dir`` for directory manifests. Prefix
    directories (the two-char ``ab/`` level) are skipped; only the object
    files inside them are yielded.
    """
    from .archive import operations as ops

    for _key, hex_prefix, prefix_dir in remote_verify_mod._prefix_dirs(
            cache_root, layout):
        try:
            names = sorted(os.listdir(prefix_dir))
        except OSError:
            continue
        for name in names:
            blob = prefix_dir / name
            # Object files only; ignore stray subdirs / ledger dirs.
            if not blob.is_file() and not os.path.islink(blob):
                continue
            yield hex_prefix + name, blob


def _remote_blob_path(remote_root: Path, remote_layout: str,
                      md5: str) -> Optional[Path]:
    """Path of the object named ``md5`` in the remote, or None if absent.

    Checks the layout(s) the remote actually uses. For a mixed remote an
    object may live under either ``files/md5/`` (v3) or the flat ``ab/``
    (v2) tree, so both are probed.
    """
    from .archive import operations as ops

    prefix, rest = md5[:2], md5[2:]
    candidates: List[Path] = []
    if remote_layout in (ops.LAYOUT_DVC_V3, ops.LAYOUT_DVC_MIXED):
        candidates.append(remote_root / 'files' / 'md5' / prefix / rest)
    if remote_layout in (ops.LAYOUT_DVC_V2, ops.LAYOUT_DVC_MIXED):
        candidates.append(remote_root / prefix / rest)
    for cand in candidates:
        if cand.exists():
            return cand
    return None


def _relink_one(blob: Path, target: Path) -> int:
    """Replace regular file ``blob`` with a symlink to ``target``.

    Returns the number of bytes freed (the size of the file that was
    replaced). Written atomically via a temp symlink + rename so an
    interrupted run never leaves a half-written cache object. Raises OSError
    on failure.
    """
    reclaimed = blob.stat().st_size  # follows to the regular file itself
    tmp = blob.parent / (blob.name + '.dt-relink.tmp')
    try:
        os.unlink(tmp)
    except OSError:
        pass
    os.symlink(os.path.abspath(target), tmp)
    os.replace(tmp, blob)  # atomic overwrite of the old regular file
    return reclaimed


def relink_cache(
    remote_name: Optional[str] = None,
    *,
    skip_verify: bool = False,
    force: bool = False,
    jobs: Optional[int] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> RelinkStats:
    """Relink every cache object onto its twin in a local remote.

    Args:
        remote_name: DVC remote to relink onto (default: the default remote).
        skip_verify: Skip the ``dt remote verify`` integrity pass.
        force: Relink even if verification found bad blobs.
        jobs: Hashing threads for the verify pass.
        dry_run: Report what would change without touching the cache.
        verbose: List every missing/failed object instead of just counting.

    Returns:
        A :class:`RelinkStats` describing the sweep.

    Raises:
        RemoteError: if no local remote exists or verification fails and the
            caller did not pass ``force``.
    """
    from .archive import operations as ops

    name, url, remote_root = remote_verify_mod.resolve_local_remote(remote_name)
    print(f"Local remote: {name} -> {remote_root}")

    if not skip_verify:
        print(f"Verifying remote '{name}' (incremental)...")
        totals, bad, incomplete, _layout = remote_verify_mod.verify_remote(
            remote_root, jobs=jobs, progress=verbose)
        n_bad = len(bad) + len(incomplete)
        if n_bad:
            msg = (f"Remote '{name}' has {n_bad} bad/incomplete blob(s); "
                   f"relinking onto it would point the cache at corrupt data. "
                   f"Fix with 'dt remote quarantine' then re-push, or pass "
                   f"--force to relink anyway.")
            if not force:
                raise RemoteError(msg)
            print(f"WARNING: {msg}", file=sys.stderr)
        else:
            print(f"  verify OK: {totals.get('objects', 0)} object(s) checked")

    cache_root = utils.get_cache_root()
    if cache_root is None or not Path(cache_root).exists():
        raise RemoteError("No local DVC cache found (not in a DVC repo?).")
    cache_root = Path(cache_root)

    try:
        cache_layout = ops.detect_source_layout(cache_root)
    except Exception as e:
        raise RemoteError(f"Could not read cache layout at {cache_root}: {e}")
    try:
        remote_layout = ops.detect_source_layout(remote_root)
    except Exception:
        # An empty remote (no blobs yet) has no detectable layout. Fall back
        # to probing both trees; every cache object then reports as missing.
        remote_layout = ops.LAYOUT_DVC_MIXED

    stats = RelinkStats()
    action = "Would relink" if dry_run else "Relinking"
    print(f"{action} cache at {cache_root} (layout: {cache_layout})")

    for md5, blob in _iter_cache_blobs(cache_root, cache_layout):
        if os.path.islink(blob):
            stats.already_linked += 1
            continue
        target = _remote_blob_path(remote_root, remote_layout, md5)
        if target is None:
            stats.missing += 1
            if verbose:
                stats.missing_md5s.append(md5)
                print(f"  missing in remote: {md5}")
            continue
        if dry_run:
            stats.relinked += 1
            try:
                stats.bytes_reclaimed += blob.stat().st_size
            except OSError:
                pass
            if verbose:
                print(f"  would relink: {md5}")
            continue
        try:
            stats.bytes_reclaimed += _relink_one(blob, target)
            stats.relinked += 1
            if verbose:
                print(f"  relinked: {md5}")
        except OSError as e:
            stats.failed += 1
            stats.failures.append((md5, str(e)))
            print(f"  FAILED {md5}: {e}", file=sys.stderr)

    return stats


def format_stats(stats: RelinkStats, dry_run: bool = False) -> str:
    """One-block human summary of a relink sweep."""
    verb = "would be relinked" if dry_run else "relinked"
    reclaim = "would reclaim" if dry_run else "reclaimed"
    lines = [
        "",
        f"  {stats.relinked} object(s) {verb}",
        f"  {stats.already_linked} already symlinked (skipped)",
        f"  {stats.missing} not in remote (left as-is)",
    ]
    if stats.failed:
        lines.append(f"  {stats.failed} failed")
    lines.append(f"  {reclaim} {utils.format_size(stats.bytes_reclaimed)}")
    return "\n".join(lines)
