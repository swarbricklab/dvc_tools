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

import dataclasses
import json
import re
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from . import cache_ops
from . import get_dest
from . import hpc
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
    # "hardlink,symlink"), so honour every entry in order -- except symlink.
    #
    # A symlink is right for a workspace, whose whole point is to stay attached
    # to the cache, and wrong for a hand-off, which has to stand alone. It is
    # also the most dangerous entry to honour blindly: linking across
    # filesystems (/scratch to /g/data here) fails EXDEV at hardlink and lands
    # on symlink, which "succeeds" having moved no bytes at all. The result
    # reports N fetched, then rsyncs to the recipient as dangling pointers.
    # Ask for it with --link symlink if you genuinely want it.
    types = [
        t.strip() for t in configured.split(',')
        if t.strip() in VALID_LINK_TYPES and t.strip() != 'symlink'
    ]
    if not types:
        return list(DEFAULT_LINK_TYPES)
    # Whatever the config prefers, copy has to terminate the chain: a
    # config of just "hardlink" would otherwise fail outright across filesystems.
    if 'copy' not in types:
        types.append('copy')
    return types


def _dvc_error_detail(result: subprocess.CompletedProcess) -> str:
    """Pull the real error out of DVC's stderr.

    DVC signs off with "Having any troubles? Hit us up at ..." on its own line,
    so taking the last line of stderr reports the support footer and throws the
    actual message away. Prefer the ERROR: line.
    """
    lines = [
        line.strip()
        for line in (result.stderr or result.stdout or '').strip().splitlines()
        if line.strip() and 'dvc.org/support' not in line
    ]
    if not lines:
        return 'unknown error'
    for line in lines:
        if line.startswith('ERROR:'):
            return line[len('ERROR:'):].strip()
    return lines[0]


def _run_dvc_list(cmd: List[str], attempts: int = 4) -> subprocess.CompletedProcess:
    """Run a ``dvc list``, retrying while its state database is locked.

    DVC keeps a SQLite state db inside the repo it is reading. Several `dvc
    list` processes against the same clone contend on it, and on a *cold* clone
    -- where the db does not exist yet and they all try to create it -- that
    reliably fails one of them with "database is locked".
    """
    delay = 0.5
    for attempt in range(attempts):
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return result
        if 'database is locked' not in (result.stderr or ''):
            return result
        if attempt < attempts - 1:
            time.sleep(delay)
            delay *= 2
    return result


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

    result = _run_dvc_list(cmd)
    if result.returncode != 0:
        detail = _dvc_error_detail(result)
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


def _resolve_rows(
    clone: Path,
    row_paths: Sequence[str],
    rev: Optional[str],
    jobs: int,
    path_col: str = 'path',
) -> List[Tuple[Optional[List[Dict[str, object]]], Optional[str]]]:
    """Resolve each row's path to a file listing, concurrently.

    Returns one ``(entries, error)`` pair per input path, in input order;
    exactly one of the two is ever set. Destinations are deliberately *not*
    computed here -- a local row lands in a directory and an S3 row lands under
    a key prefix, and the callers differ on that while agreeing on this.

    Each row costs a ``dvc list`` subprocess, so they run concurrently: on a
    long manifest the resolves are otherwise a serial prologue to all the real
    work.

    ...but the first row is resolved on its own first. DVC keeps a SQLite state
    db for the repository it is reading, and on a freshly-cloned one it does not
    exist yet; fan out immediately and the workers race to create it, which
    fails all but one with "database is locked". One serial call builds it,
    after which concurrent readers are fine.
    """
    def resolve(row_path: str):
        if not row_path:
            return None, f'Missing {path_col}'
        try:
            return list_source_files(clone, row_path, rev=rev), None
        except GetError as e:
            return None, str(e)

    resolved: List[Tuple[Optional[List[Dict[str, object]]], Optional[str]]] = []
    if row_paths:
        resolved.append(resolve(row_paths[0]))
    if len(row_paths) > 1:
        with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
            resolved.extend(pool.map(resolve, row_paths[1:]))
    return resolved


def verify_file(dest: Path, md5: str) -> bool:
    """Does the file on disk hash to the value DVC recorded for it?"""
    try:
        return utils.md5_file(dest) == md5
    except OSError:
        return False


def decide(
    dest,
    md5: str,
    force: bool,
    resume: bool,
    check: bool,
) -> Tuple[str, str]:
    """Decide what to do with one destination file. Returns (action, message).

    Action is ``fetch``, ``skip``, or ``bad``. Shared by every path -- local
    cache, network download, and S3 upload -- so a resumed transfer behaves
    identically whichever way the bytes arrive.

    *dest* is either a :class:`~pathlib.Path` or an object exposing
    ``exists()`` and ``verify(md5)``; a Path is wrapped so this logic is
    written once rather than once per destination type.

    The combination that matters is ``--resume`` without ``--check``: a
    transfer interrupted mid-file leaves a *truncated* file behind, and by size
    or existence alone it is indistinguishable from a complete one. Resuming
    over it silently keeps the corruption. That is why ``--check`` exists.
    (On S3 that failure cannot arise -- an interrupted multipart upload is
    never committed -- but the rule is the same either way.)
    """
    if isinstance(dest, Path):
        dest = get_dest.LocalDestFile(dest)

    if not dest.exists():
        return 'fetch', 'missing'

    if force:
        return 'fetch', 're-fetching'

    if check:
        if dest.verify(md5):
            return 'skip', 'verified'
        # A file that fails its checksum is worse than a missing one, because
        # nothing else will ever look at it again. Replace it when resuming;
        # report it when only checking.
        return ('fetch', 'checksum mismatch, re-fetching') if resume \
            else ('bad', 'CHECKSUM MISMATCH')

    if resume:
        return 'skip', 'already present'

    return 'skip', 'exists (use -f to overwrite)'


def _place_one(
    entry: Dict[str, object],
    cache_root: Path,
    dest_root: Path,
    link_types: Sequence[str],
    force: bool,
    resume: bool = False,
    check: bool = False,
) -> Tuple[str, bool, str]:
    """Materialise a single object. Returns (relpath, ok, message)."""
    relpath = str(entry['relpath'])
    md5 = str(entry['md5'])
    dest = dest_root / relpath

    action, note = decide(dest, md5, force, resume, check)
    if action == 'skip':
        return relpath, True, note
    if action == 'bad':
        return relpath, False, note
    force = force or dest.exists()

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


def _place_all(
    tasks: Sequence[Tuple[Path, Dict[str, object]]],
    cache_root: Path,
    link_types: Sequence[str],
    jobs: int,
    force: bool,
    resume: bool = False,
    check: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Place ``(dest_root, entry)`` pairs through one pool, preserving order.

    Taking a flat task list rather than one call per destination is what lets
    the CSV path spend its whole worker budget on the job as a whole. Batching
    per row instead would idle most workers on rows with few files -- and a row
    is often a sample directory holding two fastqs.
    """
    def place(task):
        dest_root, entry = task
        return _place_one(
            entry, cache_root, dest_root, link_types, force, resume, check
        )

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        return list(pool.map(place, tasks))


def materialise(
    entries: List[Dict[str, object]],
    cache_root: Path,
    dest_root: Path,
    link_types: Sequence[str],
    jobs: int = 8,
    force: bool = False,
    resume: bool = False,
    check: bool = False,
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
    tasks = [(dest_root, entry) for entry in entries]
    return _place_all(tasks, cache_root, link_types, jobs, force, resume, check)


def fetch_via_remote(
    clone: Path,
    path: str,
    dest_root: Path,
    rev: Optional[str] = None,
    jobs: int = 8,
    force: bool = False,
    resume: bool = False,
    check: bool = False,
) -> Tuple[int, str]:
    """Download a path over the network, for machines with no local cache.

    Hands the *local clone* to ``dvc get`` as the repository. That keeps the
    resolve-once property -- DVC does not re-clone a path it can already read --
    while letting it pull the objects from whichever remote the source repo
    configures (object storage, typically). Subpaths inside a tracked directory
    work here for the same reason they do locally.

    Args:
        clone: Local clone of the source repository.
        path: Path within the repository.
        dest_root: Directory to download into.
        rev: Git revision to resolve against.
        jobs: Parallel download threads, passed to ``dvc get``.
        force: Overwrite an existing destination.

    Returns:
        (files_written, message).

    Raises:
        GetError: If the download fails.
    """
    entries = list_source_files(clone, path, rev=rev)

    written = skipped = 0
    problems: List[str] = []
    for entry in entries:
        relpath = str(entry['relpath'])
        md5 = str(entry['md5'])
        dest = dest_root / relpath

        action, note = decide(dest, md5, force, resume, check)
        if action == 'skip':
            skipped += 1
            continue
        if action == 'bad':
            problems.append(f'{relpath}: {note}')
            continue

        # Fetch this one file rather than the whole path. That is the
        # difference between resuming a 358 GiB transfer and restarting it.
        source_path = f'{path}/{relpath}' if relpath != '.' else path
        if dest.exists():
            try:
                dest.unlink()
            except OSError as e:
                problems.append(f'{relpath}: could not replace: {e}')
                continue
        dest.parent.mkdir(parents=True, exist_ok=True)

        cmd = ['dvc', 'get', str(clone), source_path,
               '--out', str(dest), '--jobs', str(jobs)]
        if rev:
            cmd += ['--rev', rev]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            problems.append(f'{relpath}: {_dvc_error_detail(result)}')
            continue
        if not dest.exists():
            problems.append(f'{relpath}: download produced no file')
            continue
        if check and not verify_file(dest, md5):
            problems.append(f'{relpath}: CHECKSUM MISMATCH after download')
            continue
        written += 1

    if problems:
        raise GetError('; '.join(problems[:3]) +
                       (f' (+{len(problems) - 3} more)' if len(problems) > 3 else ''))

    note = f'{written} downloaded'
    if skipped:
        note += f', {skipped} already present'
    return written + skipped, note


def _open_source_odb(clone: Path):
    """Open the source repository's DVC remote as an object database.

    This is the whole trick for S3 destinations. ``dvc get`` would download to
    a local path; the object database hands us the *filesystem* instead --
    already carrying the remote's credentials and endpoint from the clone's
    ``.dvc/config`` -- so we can open a content-addressed object and stream it
    somewhere else without ever materialising it.

    Returns ``(repo, odb)``; the caller must close *repo*.
    """
    try:
        from dvc.repo import Repo
    except ImportError:
        raise GetError("dvc is required to read the source remote")

    try:
        repo = Repo(str(clone))
    except Exception as e:
        raise GetError(f"Could not open the source repository: {e}")

    try:
        remote = repo.cloud.get_remote()
    except Exception as e:
        repo.close()
        raise GetError(
            f"The source repository has no usable DVC remote: {e}\n"
            f"An S3 destination streams from the remote, so one is required."
        )
    return repo, remote.odb


def _upload_one(
    entry: Dict[str, object],
    odb,
    dest: 'get_dest.S3Dest',
    force: bool,
    resume: bool,
    check: bool,
    chunk_size: int,
) -> Tuple[str, bool, str]:
    """Stream a single object from the source remote to S3."""
    relpath = str(entry['relpath'])
    md5 = str(entry['md5'])
    target = dest.child(relpath)

    action, note = decide(target, md5, force, resume, check)
    if action == 'skip':
        return relpath, True, note
    if action == 'bad':
        return relpath, False, note

    source = odb.oid_to_path(md5)
    try:
        if not odb.fs.exists(source):
            return relpath, False, f'object {md5[:8]} not on the source remote'
    except Exception as e:
        return relpath, False, f'could not reach the source remote: {e}'

    try:
        transferred = target.write_from(odb.fs, source, md5, chunk_size)
    except Exception as e:
        # An interrupted multipart upload is never committed, so there is no
        # partial object to clean up here.
        return relpath, False, f'upload failed: {e}'

    if transferred != md5:
        # The bytes that arrived are not the bytes DVC recorded. Delete rather
        # than leave a plausible-looking wrong object behind -- it would carry
        # our md5 metadata and pass --check forever after.
        try:
            target.remove()
        except GetError:
            pass
        return relpath, False, (
            f'CHECKSUM MISMATCH in flight '
            f'(got {transferred[:8]}, expected {md5[:8]})'
        )

    return relpath, True, 'uploaded'


def _upload_all(
    tasks: Sequence[Tuple[Dict[str, object], 'get_dest.S3Dest']],
    odb,
    jobs: int,
    force: bool,
    resume: bool = False,
    check: bool = False,
    chunk_size: int = get_dest.DEFAULT_CHUNK_SIZE,
) -> List[Tuple[str, bool, str]]:
    """Stream ``(entry, dest)`` pairs through one pool, preserving order.

    The S3 counterpart to :func:`_place_all`, and flat for the same reason: a
    row is often a sample directory of two fastqs, so a pool per row would idle
    most of the budget.
    """
    def place(task):
        entry, row_dest = task
        return _upload_one(entry, odb, row_dest, force, resume, check, chunk_size)

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        return list(pool.map(place, tasks))


def upload_to_s3(
    clone: Path,
    targets: Sequence[Tuple[str, 'get_dest.S3Dest']],
    rev: Optional[str] = None,
    jobs: int = 8,
    force: bool = False,
    resume: bool = False,
    check: bool = False,
    chunk_size: int = get_dest.DEFAULT_CHUNK_SIZE,
    verbose: bool = False,
) -> Tuple[List[Tuple[str, bool, str]], List[Tuple[str, bool, str]]]:
    """Stream every ``(source path, destination)`` pair into object storage.

    Returns ``(rows, files)``: one entry per requested path, and one per object
    placed. The single-path caller wants file counts; the CSV caller wants row
    outcomes. Both come from the same run.

    Resolves all rows first, then places every file from every row through one
    pool, for the same reason the local path does: a row is often a sample
    directory of two fastqs, and batching per row would idle most workers.

    Unlike the local network path there is no ``dvc get`` subprocess, so nothing
    here forces rows to run serially.
    """
    repo, odb = _open_source_odb(clone)
    try:
        resolved = _resolve_rows(
            clone, [row_path for row_path, _ in targets], rev, jobs
        )

        tasks: List[Tuple[Dict[str, object], 'get_dest.S3Dest']] = []
        spans: Dict[int, Tuple[int, int]] = {}
        for i, ((_, row_dest), (entries, _)) in enumerate(zip(targets, resolved)):
            if entries is None:
                continue
            start = len(tasks)
            tasks.extend((entry, row_dest) for entry in entries)
            spans[i] = (start, len(tasks))

        if verbose and tasks:
            mib = get_dest.memory_estimate(jobs, chunk_size) // (1024 * 1024)
            print(f"Streaming {len(tasks)} objects across {max(1, jobs)} workers "
                  f"(~{mib} MiB peak memory)")

        placed = _upload_all(
            tasks, odb, jobs, force, resume, check, chunk_size
        )

        results: List[Tuple[str, bool, str]] = []
        for i, ((row_path, row_dest), (entries, error)) in enumerate(
            zip(targets, resolved)
        ):
            if entries is None:
                results.append((row_path or '(empty)', False, error))
                continue
            start, end = spans[i]
            written, failed = _tally(placed[start:end], verbose)
            if failed:
                results.append((row_path, False, f'{written} uploaded, {failed} failed'))
            else:
                plural = '' if written == 1 else 's'
                results.append(
                    (row_path, True, f'{written} file{plural} -> {row_dest}')
                )
        return results, placed
    finally:
        repo.close()


# =============================================================================
# Distributed streaming to S3 via qxub
# =============================================================================
#
# Only the S3 destination has a distributed mode, and the reason is bandwidth
# rather than convenience. Streaming from a DVC remote into object storage is
# network-bound, so N nodes genuinely multiply throughput -- and on NCI `copyq`,
# already hpc.DEFAULT_TRANSFER_QUEUE, is the queue with outbound network access.
# A local destination is bound by the filesystem both ends share, where a second
# node adds contention rather than bandwidth; `-j` is the knob there.
#
# What makes this cheap to build is that the submitter has to resolve every row
# anyway, because LPT bin-packing needs the byte sizes and `list_source_files`
# is where sizes come from. Having paid for that, it ships the resolved
# (md5, size, key) tuples in the manifest -- so no worker ever runs `dvc list`,
# which is what keeps N nodes off DVC's SQLite state db for the shared clone.
# Each worker opens the clone once, read-only, for one thing only: the source
# remote's filesystem and credentials out of its .dvc/config.


def _worker_tasks(
    targets: Sequence[Tuple[str, Optional[str]]],
    resolved: Sequence[Tuple[Optional[List[Dict[str, object]]], Optional[str]]],
) -> Tuple[List[Dict[str, object]], Dict[int, int]]:
    """Flatten resolved rows into JSON-serialisable per-file tasks.

    The partition unit is a *file*, not a row. A row keeps its identity through
    the ``row`` tag, so reporting stays per-row either way, and per-file
    balances properly in the two cases per-row cannot: a single row holding
    thousands of files, and rows whose sizes differ by an order of magnitude --
    which fastq sample directories reliably do.

    Returns the task list, and a row index -> file count map so collation can
    tell "this row's files failed" from "nothing came back for this row".
    """
    tasks: List[Dict[str, object]] = []
    counts: Dict[int, int] = {}
    for i, ((_, name), (entries, _)) in enumerate(zip(targets, resolved)):
        if entries is None:
            continue
        counts[i] = len(entries)
        for entry in entries:
            tasks.append({
                'row': i,
                'name': name,
                'relpath': str(entry['relpath']),
                'md5': str(entry['md5']),
                'size': int(entry.get('size') or 0),
            })
    return tasks, counts


def _write_worker_results(
    manifest_dir: Path,
    worker_id: int,
    results: List[Dict[str, object]],
) -> Path:
    """Record one worker's per-file outcomes for the submitter to collate.

    Written even for an empty partition, so that a *missing* file means "this
    worker did not finish" rather than "this worker had nothing to do".
    """
    path = manifest_dir / f'result_{worker_id}.json'
    with open(path, 'w') as f:
        json.dump({'results': results}, f)
    return path


def _collate_worker_results(
    manifest_dir: Path,
    targets: Sequence[Tuple[str, Optional[str]]],
    resolved: Sequence[Tuple[Optional[List[Dict[str, object]]], Optional[str]]],
    counts: Dict[int, int],
    root: 'get_dest.S3Dest',
) -> List[Tuple[str, bool, str]]:
    """Rebuild per-row outcomes, in CSV order, from the workers' result files.

    Because the partition unit is a file, one row's files may be spread across
    several workers; the ``row`` tag each task carries is what puts them back
    together.

    A row whose files do not all come back is reported as a failure even when
    every result that *did* arrive succeeded. A worker killed mid-partition --
    walltime, most likely -- would otherwise read as a clean run of a short row.
    """
    per_row: Dict[int, List[Tuple[str, bool, str]]] = {}
    for part in sorted(manifest_dir.glob('result_*.json')):
        try:
            with open(part) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            # A truncated result file is itself a worker that did not finish,
            # and the per-row count check below is what reports it.
            continue
        for item in data.get('results', []):
            per_row.setdefault(item.get('row'), []).append((
                str(item.get('relpath')),
                bool(item.get('ok')),
                str(item.get('message')),
            ))

    results: List[Tuple[str, bool, str]] = []
    for i, ((row_path, name), (entries, error)) in enumerate(
        zip(targets, resolved)
    ):
        if entries is None:
            results.append((row_path or '(empty)', False, error))
            continue
        placed = per_row.get(i, [])
        written = sum(1 for _, ok, _ in placed if ok)
        failed = len(placed) - written
        missing = counts.get(i, 0) - len(placed)
        if failed or missing:
            note = f'{written} uploaded, {failed} failed'
            if missing:
                note += f', {missing} unreported (a worker did not finish)'
            results.append((row_path, False, note))
        else:
            dest = root.under(name) if name else root
            plural = '' if written == 1 else 's'
            results.append((row_path, True, f'{written} file{plural} -> {dest}'))
    return results


def distributed_upload_to_s3(
    repository: str,
    targets: Sequence[Tuple[str, Optional[str]]],
    root: 'get_dest.S3Dest',
    num_workers: int,
    owner: Optional[str] = None,
    rev: Optional[str] = None,
    jobs: int = 8,
    force: bool = False,
    resume: bool = False,
    check: bool = False,
    chunk_size: int = get_dest.DEFAULT_CHUNK_SIZE,
    refresh: bool = True,
    path_col: str = 'path',
    qxub_args: Optional[List[str]] = None,
    wait: bool = True,
    verbose: bool = False,
) -> Tuple[List[str], Optional[Path], Optional[List[Tuple[str, bool, str]]]]:
    """Spread a stream-to-S3 transfer across ``num_workers`` compute nodes.

    Args:
        repository: Repository name, alias, or URL.
        targets: ``(source path, destination name)`` pairs, one per row. The
            name is the sub-prefix under *root* the row lands in, or None to
            use *root* itself.
        root: The destination prefix, already preflighted by the caller.
        num_workers: Number of qxub jobs to submit.
        owner: Owner override for short repository names.
        rev: Git revision to resolve against.
        jobs: Streaming threads *within* each worker.
        force: Overwrite existing objects.
        resume: Skip objects already present.
        check: Verify existing objects against their recorded checksum.
        chunk_size: Streaming chunk size in bytes.
        refresh: Refresh the cached clone before resolving.
        path_col: CSV column name, for error messages only.
        qxub_args: Additional arguments for qxub exec.
        wait: Wait for the jobs and collate their results.
        verbose: Print progress.

    Returns:
        ``(job_ids, manifest_dir, results)``. *results* is the per-row report
        when *wait* is True, and None when the jobs were left running.

    Raises:
        GetError: If qxub is unavailable, nothing resolves, or no job submits.
    """
    try:
        hpc.require_qxub()
    except hpc.HPCError as e:
        raise GetError(str(e))

    clone = tmp_mod.clone_repo(
        repository, owner=owner, refresh=refresh, verbose=verbose, rev=rev
    )

    resolved = _resolve_rows(
        clone, [row_path for row_path, _ in targets], rev, jobs, path_col
    )
    tasks, counts = _worker_tasks(targets, resolved)
    if not tasks:
        problems = [error for _, error in resolved if error]
        detail = f" First error: {problems[0]}" if problems else ""
        raise GetError(f"No row resolved to a tracked file.{detail}")

    partitions = hpc.partition_by_size(
        tasks,
        num_workers,
        weight=lambda task: task['size'],
        # (row, relpath) is unique across the manifest and independent of the
        # order the rows happened to resolve in, so the same CSV always
        # partitions the same way.
        tiebreak=lambda task: (task['row'], task['relpath']),
        verbose=verbose,
    )

    job_id = str(uuid.uuid4())[:8]
    manifest_dir = hpc.save_manifest(
        {'files': tasks, 'repo_root': str(Path.cwd())},
        partitions,
        job_id,
        operation='get',
        metadata={
            # Resolved on the submitting node and passed through, not
            # re-derived per worker. resolve_link_types() reads `dvc config`
            # from the cwd and preflight() resolves a credential chain; both
            # could answer differently on a compute node, and a transfer that
            # silently changes its mind halfway is worse than one that fails.
            'clone': str(clone),
            'rev': rev,
            'root_url': root.url,
            'dest_config': dataclasses.asdict(root.config),
            'chunk_size': chunk_size,
            'jobs': jobs,
            'force': force,
            'resume': resume,
            'check': check,
        },
    )

    if verbose:
        print(f"Manifest saved to {manifest_dir}")

    try:
        job_ids = hpc.submit_workers(
            manifest_dir,
            num_workers,
            operation='get',
            qxub_args=qxub_args,
            verbose=verbose,
        )
    except hpc.HPCError as e:
        raise GetError(str(e))

    if not job_ids:
        raise GetError(
            f"No worker jobs were submitted. The manifest is at {manifest_dir} "
            f"if you want to inspect the partitioning."
        )

    if not wait:
        return job_ids, manifest_dir, None

    try:
        succeeded = hpc.monitor_jobs(job_ids, verbose=verbose)
    except hpc.HPCError as e:
        raise GetError(str(e))
    if not succeeded and verbose:
        print("Some worker jobs reported failure; collating what landed.")

    return job_ids, manifest_dir, _collate_worker_results(
        manifest_dir, targets, resolved, counts, root
    )


def worker_upload_to_s3(
    manifest_dir: Path,
    worker_id: int,
    jobs: Optional[int] = None,
    verbose: bool = False,
) -> Tuple[int, int]:
    """Stream one worker's partition into S3. Called inside a submitted job.

    The partition arrives already resolved -- md5, size and destination for
    every file -- so this never runs ``dvc list``, and the only thing it needs
    the clone for is the source remote's filesystem and credentials.

    Args:
        manifest_dir: Manifest directory written by the submitter.
        worker_id: Worker index.
        jobs: Streaming threads. None takes the value the submitter chose,
            which is the authoritative one.
        verbose: Print progress.

    Returns:
        (uploaded_count, failed_count) for this partition.
    """
    metadata, tasks = hpc.load_worker_partition(manifest_dir, worker_id)

    root = get_dest.S3Dest(
        metadata['root_url'],
        get_dest.S3DestConfig(**(metadata.get('dest_config') or {})),
    )

    # Every worker preflights for itself rather than trusting the submitter's
    # check. --dest-account-id exists for unattended runs and a worker is the
    # most unattended thing here: a credential chain that resolves differently
    # on a compute node than on the login node is exactly what it must catch.
    identity = root.preflight()
    if verbose:
        rows = len({task.get('row') for task in tasks})
        print(f"Worker {worker_id}: {len(tasks)} object(s) from {rows} "
              f"row{'' if rows == 1 else 's'} under {root}")
        print(f"Writing as:  {identity}")

    if not tasks:
        _write_worker_results(manifest_dir, worker_id, [])
        return 0, 0

    jobs = jobs or int(metadata.get('jobs') or 8)
    chunk_size = int(metadata.get('chunk_size') or get_dest.DEFAULT_CHUNK_SIZE)

    # Build the filesystem once up front: under() copies it into each child, so
    # touching it here means every row shares one client rather than each
    # relying on fsspec's instance cache to hand back the same one.
    _ = root.fs
    dests: Dict[Optional[str], 'get_dest.S3Dest'] = {}
    for task in tasks:
        name = task.get('name')
        if name not in dests:
            dests[name] = root.under(name) if name else root

    repo, odb = _open_source_odb(Path(metadata['clone']))
    try:
        placed = _upload_all(
            [
                ({'relpath': task['relpath'], 'md5': task['md5']},
                 dests[task.get('name')])
                for task in tasks
            ],
            odb,
            jobs,
            bool(metadata.get('force')),
            bool(metadata.get('resume')),
            bool(metadata.get('check')),
            chunk_size,
        )
    finally:
        repo.close()

    _write_worker_results(manifest_dir, worker_id, [
        {'row': task['row'], 'relpath': relpath, 'ok': ok, 'message': message}
        for task, (relpath, ok, message) in zip(tasks, placed)
    ])
    return _tally(placed, verbose)


def _prepare_s3_dest(
    out: str,
    name: Optional[str],
    config: Optional['get_dest.S3DestConfig'],
) -> 'get_dest.S3Dest':
    """Build the destination, verify we can write to it, and say where.

    Mirrors the local trailing-slash rule: ``s3://bucket/fastqs/`` collects
    under ``fastqs/<basename>``, while ``s3://bucket/fastqs`` is used verbatim.
    There is no ``is_dir()`` to consult on object storage, so the slash is the
    only signal available -- which is why it means something here and is merely
    one of two signals locally.

    The identity is printed unconditionally, not only under ``--verbose``.
    Omitting ``--dest-profile`` is legal so that an EC2 instance role needs no
    configuration, and the only thing between that and writing to the wrong
    account is stating which account it is.
    """
    root = get_dest.S3Dest(out, config)
    identity = root.preflight()
    dest = root.under(name) if (out.endswith('/') and name) else root
    print(f"Destination: {dest}")
    print(f"Writing as:  {identity}")
    return dest


def _resolve_source(
    repository: str,
    owner: Optional[str],
    rev: Optional[str],
    refresh: bool,
    verbose: bool,
    allow_remote: bool = True,
) -> Tuple[Path, Optional[Path]]:
    """Clone the source once and locate its cache.

    Returns ``(clone, cache_root)``, where *cache_root* is None when no cache is
    reachable on this filesystem -- the normal situation for anyone outside our
    setup, and the case the remote fallback exists for.
    """
    clone = tmp_mod.clone_repo(
        repository, owner=owner, refresh=refresh, verbose=verbose, rev=rev
    )

    found = remote_mod.find_local_remote_from_repo(repository, owner=owner)
    if found:
        return clone, Path(found[1])

    if not allow_remote:
        raise GetError(
            f"No locally-accessible cache or remote found for {repository}, and "
            f"--no-remote-fallback was given."
        )
    if verbose:
        print("No locally-accessible cache; downloading from the source's remote")
    return clone, None


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
    allow_remote: bool = True,
    resume: bool = False,
    check: bool = False,
    dest_config: Optional['get_dest.S3DestConfig'] = None,
    chunk_size: int = get_dest.DEFAULT_CHUNK_SIZE,
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
    if out is not None and get_dest.is_s3_url(out):
        dest = _prepare_s3_dest(out, Path(path).name, dest_config)
        clone = tmp_mod.clone_repo(
            repository, owner=owner, refresh=refresh, verbose=verbose, rev=rev
        )
        _, placed = upload_to_s3(
            clone, [(path, dest)], rev, jobs, force, resume, check,
            chunk_size, verbose,
        )
        return _tally(placed, verbose)

    clone, cache_root = _resolve_source(
        repository, owner, rev, refresh, verbose, allow_remote
    )
    dest_root = _dest_for(out, path)

    if cache_root is None:
        written, _ = fetch_via_remote(
            clone, path, dest_root, rev, jobs, force, resume, check
        )
        return written, 0

    link_types = resolve_link_types(link)
    entries = list_source_files(clone, path, rev=rev)
    results = materialise(
        entries, cache_root, dest_root, link_types, jobs, force, resume, check
    )
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
    refresh: bool = True,
    allow_remote: bool = True,
    resume: bool = False,
    check: bool = False,
    dest_config: Optional['get_dest.S3DestConfig'] = None,
    chunk_size: int = get_dest.DEFAULT_CHUNK_SIZE,
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
        refresh: Refresh the cached clone before resolving.
        verbose: Print per-file progress.

    Returns:
        List of (path, success, message) tuples, one per selected row.

    Raises:
        GetError: If the CSV or the source cannot be read.
    """
    to_s3 = out is not None and get_dest.is_s3_url(out)

    try:
        # For S3 the fallback is applied here rather than in the CSV reader, so
        # a row's ``output`` cell can be read as a sub-prefix under ``-o``
        # instead of replacing it outright. Replacing it would mean one row
        # could silently redirect to a local path mid-transfer.
        targets = utils.read_csv_targets(
            csv_path, path_col, None if to_s3 else out
        )
    except ValueError as e:
        raise GetError(str(e))

    if not targets:
        raise GetError(f"No rows selected from {csv_path}")

    if to_s3:
        root = get_dest.S3Dest(out, dest_config)
        identity = root.preflight()
        print(f"Destination: {root}")
        print(f"Writing as:  {identity}")
        clone = tmp_mod.clone_repo(
            repository, owner=owner, refresh=refresh, verbose=verbose, rev=rev
        )
        s3_targets = [
            (row_path, root.under(row_out or Path(row_path or '.').name))
            for row_path, row_out in targets
        ]
        rows, _ = upload_to_s3(
            clone, s3_targets, rev, jobs, force, resume, check,
            chunk_size, verbose,
        )
        return rows

    clone, cache_root = _resolve_source(
        repository, owner, rev, refresh, verbose, allow_remote
    )

    if cache_root is None:
        return _download_rows(
            clone, targets, path_col, rev, jobs, force, resume, check, verbose
        )

    link_types = resolve_link_types(link)

    # Phase 1: resolve every row to a file listing.
    resolved = _resolve_rows(
        clone, [row_path for row_path, _ in targets], rev, jobs, path_col
    )

    # Phase 2: place every file from every row through a single pool, so the
    # worker budget goes to the transfer as a whole rather than being re-divided
    # per row. Spans let each row's outcome be recovered from the flat results.
    tasks: List[Tuple[Path, Dict[str, object]]] = []
    spans: Dict[int, Tuple[Path, int, int]] = {}
    for i, ((row_path, row_out), (entries, _)) in enumerate(
        zip(targets, resolved)
    ):
        if entries is None:
            continue
        dest_root = _dest_for(row_out, row_path)
        start = len(tasks)
        tasks.extend((dest_root, entry) for entry in entries)
        spans[i] = (dest_root, start, len(tasks))

    if verbose and tasks:
        rows = len(spans)
        print(f"Placing {len(tasks)} files from {rows} row{'' if rows == 1 else 's'} "
              f"across {max(1, jobs)} workers")

    placed = _place_all(tasks, cache_root, link_types, jobs, force, resume, check)

    # Phase 3: attribute the flat results back to their rows, in CSV order.
    results: List[Tuple[str, bool, str]] = []
    for i, (target, (entries, error)) in enumerate(zip(targets, resolved)):
        row_path = target[0]
        if entries is None:
            results.append((row_path or '(empty)', False, error))
            continue
        dest_root, start, end = spans[i]
        written, failed = _tally(placed[start:end], verbose)
        if failed:
            results.append((row_path, False, f'{written} written, {failed} failed'))
        else:
            plural = '' if written == 1 else 's'
            results.append((row_path, True, f'{written} file{plural} -> {dest_root}'))

    return results


def _download_rows(
    clone: Path,
    targets: Sequence[Tuple[str, Optional[str]]],
    path_col: str,
    rev: Optional[str],
    jobs: int,
    force: bool,
    resume: bool,
    check: bool,
    verbose: bool,
) -> List[Tuple[str, bool, str]]:
    """Batch path for machines with no local cache.

    Rows run one at a time rather than fanned out. The bottleneck here is the
    network, which ``dvc get -j`` already saturates from within a single row,
    and concurrent ``dvc get`` against one clone would contend on its SQLite
    state db -- the same lock that broke the local path.
    """
    results: List[Tuple[str, bool, str]] = []
    for i, (row_path, row_out) in enumerate(targets, 1):
        if not row_path:
            results.append(('(empty)', False, f'Missing {path_col}'))
            continue
        dest_root = _dest_for(row_out, row_path)
        if verbose:
            print(f"[{i}/{len(targets)}] downloading {row_path} -> {dest_root}")
        try:
            written, note = fetch_via_remote(
                clone, row_path, dest_root, rev, jobs, force, resume, check
            )
            plural = '' if written == 1 else 's'
            results.append((
                row_path, True, f'{written} file{plural} -> {dest_root} ({note})'
            ))
        except GetError as e:
            results.append((row_path, False, str(e)))
    return results


# A URL scheme, per RFC 3986: letter followed by letters/digits/+/-/. then "://".
# Anchored, so a relative path containing a colon ("foo:bar/baz") is not a URL.
_URL_SCHEME_RE = re.compile(r'^[a-zA-Z][a-zA-Z0-9+.\-]*://')


def _reject_url_destination(out: str) -> None:
    """Fail on a URL destination instead of silently writing a local directory.

    ``Path('s3://bucket/prefix/')`` collapses the double slash to
    ``s3:/bucket/prefix``, which is a perfectly valid *relative* path -- so
    ``-o s3://bucket/prefix/`` used to create a local directory literally named
    ``s3:`` and report success. Bytes went to the wrong filesystem entirely and
    nothing said so.
    """
    match = _URL_SCHEME_RE.match(out)
    if not match:
        return
    scheme = match.group(0)[:-3]
    raise GetError(
        f"Destination must be a local path, not a {scheme}:// URL: {out}\n"
        f"Fetch to a local directory, then upload it separately."
    )


def _dest_for(out: Optional[str], path: str) -> Path:
    """Destination directory for a source path.

    ``-o fastqs/`` collects every row under ``fastqs/<basename>``, which is what
    makes a CSV of 82 sample directories land as 82 sibling directories.

    Raises:
        GetError: If *out* is a URL. The destination is a local filesystem path.
    """
    if out is None:
        return Path(Path(path).name)
    _reject_url_destination(out)
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
