"""Exhaustive checksum verification of a DVC remote.

Walks the blob tree of a locally-accessible DVC remote and confirms that
each object's content hashes to the md5 implied by its path. This catches
silent corruption and partial/truncated blobs left behind when a transfer
is interrupted (e.g. a push job hitting a walltime limit).

Two execution modes, mirroring :mod:`dt.push`:

- **Single node** — hash blobs with a thread pool (``--jobs``).
- **Distributed** — partition the 256 md5 prefixes (``00``-``ff``) across
  ``--workers`` compute nodes via qxub; each worker writes a partial JSON
  report which the parent merges.

Verification is exact for DVC v3 remotes (plain md5, no line-ending
normalisation). For legacy v2 / mixed layouts the same plain-md5 check is
applied — it reliably catches truncation, but a *mismatch* on a text blob
hashed under DVC's old dos2unix algorithm could be a false positive, so the
report carries a ``legacy_hash_caveat`` flag for those layouts.
"""

import datetime as _dt
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from . import hpc
from . import remote as remote_mod
from . import utils
from .errors import RemoteError

STATUS_MISMATCH = 'mismatch'
STATUS_UNREADABLE = 'unreadable'

REPORT_VERSION = 1


def expected_md5_for_blob(prefix: str, name: str) -> str:
    """Reconstruct the md5 implied by a blob's path.

    ``prefix`` is the 2-char hex directory name and ``name`` is the file
    within it. DVC directory objects carry a ``.dir`` suffix that is not
    part of the hash, so it is stripped.
    """
    base = name[:-4] if name.endswith('.dir') else name
    return prefix + base


def _prefix_dirs(
    remote_dir: Path,
    layout: str,
    only_prefixes: Optional[Set[str]] = None,
) -> List[Tuple[str, Path]]:
    """Return ``[(hex_prefix, dir_path), ...]`` for the given layout.

    Reuses the archive enumerator, which understands v2/v3/mixed, and maps
    its manifest keys (``'00'`` or ``'v3-00'``) back to the bare 2-char hex
    prefix. ``only_prefixes`` restricts the result to a worker's partition.
    """
    from .archive import operations as ops

    entries = []
    for key, path in ops._enumerate_prefix_dirs(remote_dir, layout):
        hex_prefix = key[-2:]
        if only_prefixes is not None and hex_prefix not in only_prefixes:
            continue
        entries.append((hex_prefix, path))
    return entries


def _verify_one_prefix(
    hex_prefix: str,
    dir_path: Path,
    remote_dir: Path,
) -> Tuple[int, int, int, int, List[dict]]:
    """Verify every blob under one prefix directory.

    Returns ``(objects, ok, bad, bytes, bad_entries)``. ``bad_entries`` is a
    list of report dicts for the mismatched / unreadable blobs only.
    """
    objects = ok = bad = total_bytes = 0
    bad_entries: List[dict] = []

    try:
        children = list(dir_path.iterdir())
    except OSError:
        return 0, 0, 0, 0, []

    for f in children:
        if not f.is_file():
            continue
        objects += 1
        expected = expected_md5_for_blob(hex_prefix, f.name)
        rel = str(f.relative_to(remote_dir))
        try:
            actual = utils.md5_file(f)
            size = f.stat().st_size
        except OSError:
            bad += 1
            bad_entries.append({
                'path': rel,
                'expected_md5': expected,
                'actual_md5': None,
                'size_bytes': None,
                'status': STATUS_UNREADABLE,
            })
            continue

        total_bytes += size
        if actual == expected:
            ok += 1
        else:
            bad += 1
            bad_entries.append({
                'path': rel,
                'expected_md5': expected,
                'actual_md5': actual,
                'size_bytes': size,
                'status': STATUS_MISMATCH,
            })

    return objects, ok, bad, total_bytes, bad_entries


def verify_remote(
    remote_dir: Path,
    layout: Optional[str] = None,
    jobs: Optional[int] = None,
    only_prefixes: Optional[Set[str]] = None,
    progress: bool = False,
) -> Tuple[Dict[str, int], List[dict], str]:
    """Hash-and-compare every blob in ``remote_dir``.

    Returns ``(totals, bad_entries, layout)`` where ``totals`` has keys
    ``objects``/``ok``/``bad``/``bytes``. ``only_prefixes`` restricts the
    walk to a worker's partition of the ``00``-``ff`` prefix space.
    """
    from .archive import operations as ops

    layout = layout or ops.detect_source_layout(remote_dir)
    entries = _prefix_dirs(remote_dir, layout, only_prefixes)
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()

    if progress:
        print(
            f"Verifying {remote_dir} ({layout}) across {len(entries)} "
            f"prefix(es) with {n_jobs} worker(s) ...",
            file=sys.stderr, flush=True,
        )

    totals = {'objects': 0, 'ok': 0, 'bad': 0, 'bytes': 0}
    bad_entries: List[dict] = []
    done = 0

    with ThreadPoolExecutor(max_workers=n_jobs) as pool:
        futures = {
            pool.submit(_verify_one_prefix, hexp, p, remote_dir): hexp
            for hexp, p in entries
        }
        for fut in as_completed(futures):
            objects, ok, bad, total_bytes, entries_bad = fut.result()
            totals['objects'] += objects
            totals['ok'] += ok
            totals['bad'] += bad
            totals['bytes'] += total_bytes
            bad_entries.extend(entries_bad)
            done += 1
            if progress:
                print(
                    f"  [{done:>3}/{len(entries)}] {futures[fut]}: "
                    f"{objects} object(s), {bad} bad "
                    f"(running bad total: {totals['bad']})",
                    file=sys.stderr, flush=True,
                )

    # Stable ordering makes reports diffable and tests deterministic.
    bad_entries.sort(key=lambda e: e['path'])
    return totals, bad_entries, layout


def build_report(
    remote_name: Optional[str],
    url: str,
    layout: str,
    totals: Dict[str, int],
    bad_entries: List[dict],
    jobs: int,
) -> dict:
    """Assemble the machine-readable verification report."""
    return {
        'report_version': REPORT_VERSION,
        'remote': remote_name,
        'url': url,
        'layout': layout,
        'legacy_hash_caveat': layout in ('dvc-v2', 'dvc-mixed'),
        'scanned_at': _dt.datetime.now(_dt.timezone.utc).isoformat(
            timespec='seconds'),
        'jobs': jobs,
        'totals': totals,
        'bad': bad_entries,
    }


def format_report_summary(report: dict, show_bad: bool = True) -> str:
    """Render a verification report as a human-readable summary."""
    t = report['totals']
    lines = [
        f"Remote: {report.get('remote') or report.get('url')}  "
        f"({report['layout']})",
        f"  Objects:  {t['objects']:,}",
        f"  OK:       {t['ok']:,}",
        f"  Bad:      {t['bad']:,}",
        f"  Size:     {utils.format_size(t['bytes'])}",
    ]
    if report.get('legacy_hash_caveat'):
        lines.append(
            "  Note:     legacy v2/mixed layout — a mismatch on a text blob "
            "may be a\n            dos2unix-hash false positive (truncation "
            "is still detected reliably).")
    if show_bad and report['bad']:
        lines.append("  Corrupt / unreadable objects:")
        for e in report['bad']:
            lines.append(
                f"    [{e['status']}] {e['path']}  "
                f"expected={e['expected_md5']} actual={e['actual_md5']}")
    return "\n".join(lines)


def resolve_local_remote(remote_name: Optional[str]) -> Tuple[str, str, Path]:
    """Resolve a remote name to ``(name, url, local_path)``.

    Verification reads blobs from disk, so the remote must be on a
    locally-accessible filesystem. Raises :class:`RemoteError` otherwise,
    or if the named remote does not exist or has been archived.
    """
    remotes = remote_mod.list_remotes(project_only=False)
    if not remotes:
        raise RemoteError("No remotes configured.")

    if remote_name:
        matches = [r for r in remotes if r[0] == remote_name]
        if not matches:
            raise RemoteError(f"No remote named '{remote_name}'")
        name, url, _ = matches[0]
    else:
        defaults = [r for r in remotes if r[2]]
        if not defaults:
            raise RemoteError(
                "No default remote configured; pass a remote name.")
        name, url, _ = defaults[0]

    local_path = remote_mod.extract_local_path(url)
    if not local_path:
        raise RemoteError(
            f"Remote '{name}' ({url}) is not on a locally-accessible "
            f"filesystem; verify requires direct file access.")

    path = Path(local_path)
    if not path.exists():
        raise RemoteError(
            f"Remote '{name}' path not accessible: {local_path}")

    # A pruned/archived remote has no blobs left to verify.
    from .archive import signpost as signpost_mod
    if signpost_mod.detect(path) is not None:
        raise RemoteError(
            f"Remote '{name}' has been archived to cold storage "
            f"(ARCHIVED.yaml present); nothing on disk to verify.")

    return name, url, path


# =============================================================================
# Distributed verification via qxub
# =============================================================================

def _report_dir(job_id: str) -> Path:
    d = hpc.get_transfer_dir('remote-verify') / job_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def worker_verify(
    remote_dir: Path,
    worker_id: int,
    num_workers: int,
    report_dir: Path,
    jobs: Optional[int] = None,
    verbose: bool = False,
) -> Path:
    """Verify one worker's prefix partition and write a partial report.

    Called inside a submitted qxub job. The partial report is merged by the
    parent once all workers finish.
    """
    prefixes = hpc.get_prefixes_for_worker(worker_id, num_workers)
    totals, bad_entries, _layout = verify_remote(
        remote_dir, jobs=jobs, only_prefixes=prefixes, progress=verbose)

    part_path = report_dir / f'part_{worker_id}.json'
    with open(part_path, 'w') as f:
        json.dump({'totals': totals, 'bad': bad_entries}, f)
    return part_path


def _merge_parts(report_dir: Path) -> Tuple[Dict[str, int], List[dict]]:
    """Combine all ``part_*.json`` partial reports in ``report_dir``."""
    totals = {'objects': 0, 'ok': 0, 'bad': 0, 'bytes': 0}
    bad_entries: List[dict] = []
    for part in sorted(report_dir.glob('part_*.json')):
        with open(part) as f:
            data = json.load(f)
        for k in totals:
            totals[k] += data.get('totals', {}).get(k, 0)
        bad_entries.extend(data.get('bad', []))
    bad_entries.sort(key=lambda e: e['path'])
    return totals, bad_entries


def parallel_verify(
    remote_name: Optional[str],
    num_workers: int,
    jobs: Optional[int] = None,
    qxub_args: Optional[List[str]] = None,
    wait: bool = True,
    verbose: bool = False,
) -> Tuple[List[str], Path, Optional[dict]]:
    """Distribute verification across ``num_workers`` compute nodes via qxub.

    Returns ``(job_ids, report_dir, report)``. ``report`` is the merged
    report dict when ``wait`` is True, else ``None`` (jobs still running).
    """
    import uuid

    name, url, remote_dir = resolve_local_remote(remote_name)
    layout = _detect_layout(remote_dir)

    job_id = str(uuid.uuid4())[:8]
    report_dir = _report_dir(job_id)

    try:
        hpc.require_qxub()
    except hpc.HPCError as e:
        raise RemoteError(str(e))

    job_ids: List[str] = []
    for worker_id in range(num_workers):
        worker_cmd = [
            'dt', 'remote', 'verify',
            '--worker', str(worker_id),
            '--num-workers', str(num_workers),
            '--remote-dir', str(remote_dir),
            '--report-dir', str(report_dir),
        ]
        if jobs:
            worker_cmd += ['--jobs', str(jobs)]
        if verbose:
            worker_cmd.append('--verbose')

        cmd = hpc.build_qxub_command(
            f'dt-remote-verify-{job_id}-w{worker_id}', worker_cmd, qxub_args)
        if verbose:
            print(f"Submitting worker {worker_id}: {' '.join(cmd)}")

        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            job_ids.append(result.stdout.strip().split('\n')[0])
        else:
            print(f"Warning: failed to submit worker {worker_id}: "
                  f"{result.stderr}", file=sys.stderr)

    if not job_ids:
        raise RemoteError("No verification jobs were submitted.")

    if not wait:
        return job_ids, report_dir, None

    try:
        ok = hpc.monitor_jobs(job_ids, verbose=verbose)
    except hpc.HPCError as e:
        raise RemoteError(str(e))
    if not ok:
        print("Warning: some verification jobs reported failure.",
              file=sys.stderr)

    totals, bad_entries = _merge_parts(report_dir)
    report = build_report(name, url, layout, totals, bad_entries,
                          jobs or 0)
    return job_ids, report_dir, report


def _detect_layout(remote_dir: Path) -> str:
    from .archive import operations as ops
    return ops.detect_source_layout(remote_dir)
