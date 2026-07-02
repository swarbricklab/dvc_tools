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

**Incremental verification.** Re-hashing a multi-terabyte remote is
expensive, so verified blobs are recorded in a per-prefix ledger under
``<remote>/.dt-verify/``. On a later run a blob is skipped if its size and
mtime still match the ledger entry; anything changed (e.g. a re-run of an
interrupted transfer) is re-hashed. The ledger lives in the remote so it is
shared across clones and users; ``dt remote init`` makes remotes
group-writable. ``--full`` ignores and rebuilds the ledger; ``--no-ledger``
disables it entirely (also the fallback for read-only remotes).
"""

import datetime as _dt
import json
import os
import re
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
STATUS_INCOMPLETE = 'incomplete'
STATUS_MISSING = 'missing'

REPORT_VERSION = 2

LEDGER_DIRNAME = '.dt-verify'

# Persisted bad-blob report: the last scan's bad/incomplete findings, so they
# survive between runs and can feed `--recheck` and `dt remote quarantine`.
BAD_REPORT_FILENAME = 'bad.json'

# A valid md5 reconstructed from a blob path is exactly 32 lowercase hex
# chars. Anything else (e.g. a leftover ``*.tmp`` from a killed transfer) is
# not a real blob and is reported as 'incomplete' rather than 'mismatch'.
_HASH_RE = re.compile(r'^[0-9a-f]{32}$')

_TOTALS_KEYS = ('objects', 'ok', 'bad', 'incomplete', 'skipped', 'bytes')


def _zero_totals() -> Dict[str, int]:
    return {k: 0 for k in _TOTALS_KEYS}


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
) -> List[Tuple[str, str, Path]]:
    """Return ``[(key, hex_prefix, dir_path), ...]`` for the given layout.

    Reuses the archive enumerator, which understands v2/v3/mixed. ``key`` is
    the unique manifest key (``'00'`` for pure layouts, ``'v3-00'``/``'v2-00'``
    for mixed) and is used to name the per-prefix ledger file so the two
    halves of a mixed remote never collide. ``hex_prefix`` is the bare 2-char
    prefix used for worker partitioning. ``only_prefixes`` restricts the
    result to a worker's partition.
    """
    from .archive import operations as ops

    entries = []
    for key, path in ops._enumerate_prefix_dirs(remote_dir, layout):
        hex_prefix = key[-2:]
        if only_prefixes is not None and hex_prefix not in only_prefixes:
            continue
        entries.append((key, hex_prefix, path))
    return entries


def _ledger_file(ledger_dir: Path, key: str) -> Path:
    return ledger_dir / f'{key}.json'


def _load_prefix_ledger(ledger_dir: Path, key: str) -> Dict[str, list]:
    """Load a per-prefix ledger ``{name: [size, mtime_ns]}`` (empty if none)."""
    try:
        with open(_ledger_file(ledger_dir, key)) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_prefix_ledger(
    ledger_dir: Path, key: str, mapping: Dict[str, list],
) -> bool:
    """Atomically write a per-prefix ledger. Best-effort: returns success."""
    final = _ledger_file(ledger_dir, key)
    tmp = ledger_dir / f'.{key}.json.tmp'
    try:
        with open(tmp, 'w') as f:
            json.dump(mapping, f)
        os.replace(tmp, final)
        try:
            os.chmod(final, 0o664)  # group-writable for shared remotes
        except OSError:
            pass
        return True
    except OSError:
        return False


# =============================================================================
# Persisted bad-blob report (issue #152)
# =============================================================================

def bad_report_file(remote_dir: Path) -> Path:
    """Path to the persisted bad-blob report inside the remote's ledger dir."""
    return remote_dir / LEDGER_DIRNAME / BAD_REPORT_FILENAME


def write_bad_report(remote_dir: Path, report: dict) -> bool:
    """Persist the bad/incomplete findings to ``<remote>/.dt-verify/bad.json``.

    Best-effort: needs the ledger dir to be writable. Overwriting on each scan
    naturally prunes entries that now pass. Returns success.
    """
    ledger_dir = _setup_ledger_dir(remote_dir)
    if ledger_dir is None:
        return False
    payload = {
        'report_version': report.get('report_version', REPORT_VERSION),
        'remote': report.get('remote'),
        'url': report.get('url'),
        'layout': report.get('layout'),
        'scanned_at': report.get('scanned_at'),
        'totals': report.get('totals'),
        'bad': report.get('bad', []),
        'incomplete': report.get('incomplete', []),
    }
    final = bad_report_file(remote_dir)
    tmp = ledger_dir / f'.{BAD_REPORT_FILENAME}.tmp'
    try:
        with open(tmp, 'w') as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, final)
        try:
            os.chmod(final, 0o664)
        except OSError:
            pass
        return True
    except OSError:
        return False


def load_bad_report(path: Path) -> Optional[dict]:
    """Load a persisted bad report (bad.json or a --report file). None if absent."""
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def _verify_one_prefix(
    key: str,
    hex_prefix: str,
    dir_path: Path,
    remote_dir: Path,
    ledger_dir: Optional[Path],
    full: bool,
) -> Tuple[Dict[str, int], List[dict], List[dict]]:
    """Verify every blob under one prefix directory.

    Returns ``(counts, bad_entries, incomplete_entries)``. When ``ledger_dir``
    is set, blobs whose size+mtime match the ledger are skipped, and the
    ledger is rebuilt from this run's results (pruning deleted blobs).
    """
    counts = _zero_totals()
    bad_entries: List[dict] = []
    incomplete_entries: List[dict] = []

    use_ledger = ledger_dir is not None
    old_ledger = ({} if (full or not use_ledger)
                  else _load_prefix_ledger(ledger_dir, key))
    new_ledger: Dict[str, list] = {}

    try:
        children = list(dir_path.iterdir())
    except OSError:
        return counts, bad_entries, incomplete_entries

    for f in children:
        if not f.is_file():
            continue
        counts['objects'] += 1
        name = f.name
        rel = str(f.relative_to(remote_dir))
        expected = expected_md5_for_blob(hex_prefix, name)

        # A name that doesn't reconstruct to a valid md5 (e.g. a *.tmp left
        # behind by a killed transfer) is an incomplete artefact, not a blob.
        if not _HASH_RE.match(expected):
            counts['incomplete'] += 1
            incomplete_entries.append({
                'path': rel,
                'status': STATUS_INCOMPLETE,
            })
            continue

        try:
            st = f.stat()
            size, mtime_ns = st.st_size, st.st_mtime_ns
        except OSError:
            counts['bad'] += 1
            bad_entries.append({
                'path': rel, 'expected_md5': expected, 'actual_md5': None,
                'size_bytes': None, 'status': STATUS_UNREADABLE,
            })
            continue

        counts['bytes'] += size
        sig = [size, mtime_ns]

        # Incremental skip: unchanged since a previous successful verify.
        if use_ledger and old_ledger.get(name) == sig:
            counts['skipped'] += 1
            new_ledger[name] = sig
            continue

        try:
            actual = utils.md5_file(f)
        except OSError:
            counts['bad'] += 1
            bad_entries.append({
                'path': rel, 'expected_md5': expected, 'actual_md5': None,
                'size_bytes': size, 'status': STATUS_UNREADABLE,
            })
            continue

        if actual == expected:
            counts['ok'] += 1
            new_ledger[name] = sig
        else:
            counts['bad'] += 1
            bad_entries.append({
                'path': rel, 'expected_md5': expected, 'actual_md5': actual,
                'size_bytes': size, 'status': STATUS_MISMATCH,
            })

    if use_ledger:
        _write_prefix_ledger(ledger_dir, key, new_ledger)

    return counts, bad_entries, incomplete_entries


def _setup_ledger_dir(remote_dir: Path) -> Optional[Path]:
    """Create ``<remote>/.dt-verify`` (group-writable), or None if read-only."""
    ledger_dir = remote_dir / LEDGER_DIRNAME
    try:
        ledger_dir.mkdir(parents=True, exist_ok=True)
        utils.set_group_writable(ledger_dir, setgid=True)
        return ledger_dir
    except OSError:
        return None


def verify_remote(
    remote_dir: Path,
    layout: Optional[str] = None,
    jobs: Optional[int] = None,
    only_prefixes: Optional[Set[str]] = None,
    progress: bool = False,
    use_ledger: bool = True,
    full: bool = False,
) -> Tuple[Dict[str, int], List[dict], List[dict], str]:
    """Hash-and-compare every blob in ``remote_dir``.

    Returns ``(totals, bad_entries, incomplete_entries, layout)``.
    ``only_prefixes`` restricts the walk to a worker's partition of the
    ``00``-``ff`` prefix space. With ``use_ledger`` (and not ``full``),
    blobs unchanged since a previous successful verify are skipped.
    """
    from .archive import operations as ops

    layout = layout or ops.detect_source_layout(remote_dir)
    entries = _prefix_dirs(remote_dir, layout, only_prefixes)
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()

    ledger_dir = _setup_ledger_dir(remote_dir) if use_ledger else None
    if use_ledger and ledger_dir is None and progress:
        print(f"  (ledger disabled: {remote_dir} is not writable)",
              file=sys.stderr, flush=True)

    if progress:
        print(
            f"Verifying {remote_dir} ({layout}) across {len(entries)} "
            f"prefix(es) with {n_jobs} worker(s) ...",
            file=sys.stderr, flush=True,
        )

    totals = _zero_totals()
    bad_entries: List[dict] = []
    incomplete_entries: List[dict] = []
    done = 0

    with ThreadPoolExecutor(max_workers=n_jobs) as pool:
        futures = {
            pool.submit(_verify_one_prefix, key, hexp, p, remote_dir,
                        ledger_dir, full): key
            for key, hexp, p in entries
        }
        for fut in as_completed(futures):
            counts, ent_bad, ent_incomplete = fut.result()
            for k in _TOTALS_KEYS:
                totals[k] += counts[k]
            bad_entries.extend(ent_bad)
            incomplete_entries.extend(ent_incomplete)
            done += 1
            if progress:
                print(
                    f"  [{done:>3}/{len(entries)}] {futures[fut]}: "
                    f"{counts['objects']} object(s), {counts['skipped']} "
                    f"skipped, {counts['bad']} bad "
                    f"(running bad total: {totals['bad']})",
                    file=sys.stderr, flush=True,
                )

    # Stable ordering makes reports diffable and tests deterministic.
    bad_entries.sort(key=lambda e: e['path'])
    incomplete_entries.sort(key=lambda e: e['path'])
    return totals, bad_entries, incomplete_entries, layout


def build_report(
    remote_name: Optional[str],
    url: str,
    layout: str,
    totals: Dict[str, int],
    bad_entries: List[dict],
    incomplete_entries: List[dict],
    jobs: int,
    ledger_used: bool = True,
) -> dict:
    """Assemble the machine-readable verification report."""
    return {
        'report_version': REPORT_VERSION,
        'remote': remote_name,
        'url': url,
        'layout': layout,
        'legacy_hash_caveat': layout in ('dvc-v2', 'dvc-mixed'),
        'ledger_used': ledger_used,
        'scanned_at': _dt.datetime.now(_dt.timezone.utc).isoformat(
            timespec='seconds'),
        'jobs': jobs,
        'totals': totals,
        'bad': bad_entries,
        'incomplete': incomplete_entries,
    }


def format_report_summary(report: dict, show_bad: bool = True) -> str:
    """Render a verification report as a human-readable summary."""
    t = report['totals']
    lines = [
        f"Remote: {report.get('remote') or report.get('url')}  "
        f"({report['layout']})",
        f"  Objects:  {t['objects']:,}",
        f"  Verified: {t['ok']:,}",
    ]
    if report.get('ledger_used') and t.get('skipped'):
        lines.append(f"  Skipped:  {t['skipped']:,} (unchanged since last "
                     f"verify)")
    lines.append(f"  Bad:      {t['bad']:,}")
    if t.get('incomplete'):
        lines.append(f"  Incomplete: {t['incomplete']:,} (stray/*.tmp files — "
                     f"possible interrupted transfer)")
    lines.append(f"  Size:     {utils.format_size(t['bytes'])}")

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
    if show_bad and report.get('incomplete'):
        lines.append("  Incomplete / stray files:")
        for e in report['incomplete']:
            lines.append(f"    [{e['status']}] {e['path']}")
    return "\n".join(lines)


# =============================================================================
# Targeted re-check of previously-bad blobs (issue #152)
# =============================================================================

def _prefix_key_for_rel(rel_path: str, layout: str) -> Tuple[str, str]:
    """Return ``(ledger_key, name)`` for a blob's remote-relative path.

    Mirrors the manifest keys used by the ledger so a re-checked blob that now
    passes is recorded under the same ``<key>.json`` a full scan would use
    (``'00'`` for pure layouts, ``'v3-00'`` / ``'v2-00'`` for mixed).
    """
    parts = rel_path.split('/')
    name = parts[-1]
    if parts[:2] == ['files', 'md5']:
        prefix = parts[2] if len(parts) > 3 else ''
        key = f'v3-{prefix}' if layout == 'dvc-mixed' else prefix
    else:
        prefix = parts[0] if len(parts) > 1 else ''
        key = f'v2-{prefix}' if layout == 'dvc-mixed' else prefix
    return key, name


def recheck_entries(
    remote_dir: Path,
    entries: List[dict],
    layout: Optional[str] = None,
    use_ledger: bool = True,
) -> Tuple[Dict[str, int], List[dict], List[dict], str]:
    """Re-hash only the given (previously-bad) blobs to confirm a fix.

    ``entries`` are report entries carrying at least ``path`` (remote-relative).
    Hashes just those blobs — no full-store walk. Blobs that now pass are added
    to the ledger (so a later incremental run skips them); still-bad ones are
    returned. A blob that has vanished (e.g. quarantined but not yet re-pushed)
    is reported as ``missing``.

    Returns ``(totals, still_bad, still_incomplete, layout)``.
    """
    from .archive import operations as ops

    ledger_dir = _setup_ledger_dir(remote_dir) if use_ledger else None

    # Layout is only needed to compute the ledger key for now-OK blobs, so
    # detect it lazily — recheck must work even when every listed blob has
    # vanished (an empty remote has no detectable layout).
    def _layout() -> str:
        nonlocal layout
        if layout is None:
            try:
                layout = ops.detect_source_layout(remote_dir)
            except Exception:
                layout = 'dvc-v3'
        return layout

    totals = _zero_totals()
    still_bad: List[dict] = []
    still_incomplete: List[dict] = []
    # Group now-OK blobs by ledger key so we touch each ledger file once.
    now_ok: Dict[str, Dict[str, list]] = {}

    for entry in entries:
        rel = entry.get('path')
        if not rel:
            continue
        totals['objects'] += 1
        f = remote_dir / rel
        name = rel.split('/')[-1]

        # Stray / non-blob artefact (name doesn't reconstruct a valid md5).
        expected = entry.get('expected_md5')
        if expected is None:
            hex_prefix = _prefix_key_for_rel(rel, layout)[0][-2:]
            expected = expected_md5_for_blob(hex_prefix, name)
        if not _HASH_RE.match(expected or ''):
            # Stray file: gone means the fix (deletion) worked; else still stray.
            if not f.exists():
                totals['ok'] += 1
                continue
            totals['incomplete'] += 1
            still_incomplete.append({'path': rel, 'status': STATUS_INCOMPLETE})
            continue

        try:
            st = f.stat()
        except OSError as exc:
            # ENOENT => the blob is genuinely gone (e.g. quarantined, not yet
            # re-pushed). Any other errno (EACCES, EIO, ...) is a present but
            # unreadable blob — don't misreport a permission problem as loss.
            import errno as _errno
            is_missing = getattr(exc, 'errno', None) == _errno.ENOENT
            totals['bad'] += 1
            still_bad.append({
                'path': rel, 'expected_md5': expected, 'actual_md5': None,
                'size_bytes': None,
                'status': STATUS_MISSING if is_missing else STATUS_UNREADABLE,
            })
            continue

        try:
            actual = utils.md5_file(f)
        except OSError:
            totals['bad'] += 1
            still_bad.append({
                'path': rel, 'expected_md5': expected, 'actual_md5': None,
                'size_bytes': st.st_size, 'status': STATUS_UNREADABLE,
            })
            continue

        totals['bytes'] += st.st_size
        if actual == expected:
            totals['ok'] += 1
            if ledger_dir is not None:
                key, _ = _prefix_key_for_rel(rel, _layout())
                now_ok.setdefault(key, {})[name] = [st.st_size, st.st_mtime_ns]
        else:
            totals['bad'] += 1
            still_bad.append({
                'path': rel, 'expected_md5': expected, 'actual_md5': actual,
                'size_bytes': st.st_size, 'status': STATUS_MISMATCH,
            })

    # Promote now-OK blobs into the ledger (merge, don't clobber other entries).
    if ledger_dir is not None:
        for key, sigs in now_ok.items():
            merged = _load_prefix_ledger(ledger_dir, key)
            merged.update(sigs)
            _write_prefix_ledger(ledger_dir, key, merged)

    still_bad.sort(key=lambda e: e['path'])
    still_incomplete.sort(key=lambda e: e['path'])
    return totals, still_bad, still_incomplete, _layout()


# =============================================================================
# Prompt status report from the ledger + stat-only scan (issue #152)
# =============================================================================

def _status_one_prefix(
    key: str, dir_path: Path, ledger_dir: Optional[Path], full_ledger: dict,
) -> Dict[str, int]:
    """Stat (no hashing) every blob under one prefix; classify vs the ledger."""
    counts = {'objects': 0, 'verified_unchanged': 0, 'unscanned_or_changed': 0,
              'bytes': 0}
    old = full_ledger.get(key, {})
    try:
        children = list(dir_path.iterdir())
    except OSError:
        return counts
    for f in children:
        try:
            if not f.is_file():
                continue
            st = f.stat()
        except OSError:
            continue
        counts['objects'] += 1
        counts['bytes'] += st.st_size
        if old.get(f.name) == [st.st_size, st.st_mtime_ns]:
            counts['verified_unchanged'] += 1
        else:
            counts['unscanned_or_changed'] += 1
    return counts


def status_summary(
    remote_dir: Path,
    layout: Optional[str] = None,
    jobs: Optional[int] = None,
) -> dict:
    """Instant status: ledger + bad.json + a stat-only enumeration (no hashing).

    Reports how many blobs passed the previous scan (and when), how many were
    bad at that scan, and — via a stat-only walk — the total blob count and how
    many are unscanned or changed since the ledger and would be re-hashed next.
    """
    from .archive import operations as ops

    layout = layout or ops.detect_source_layout(remote_dir)
    ledger_dir = remote_dir / LEDGER_DIRNAME
    entries = _prefix_dirs(remote_dir, layout)
    n_jobs = jobs if jobs and jobs > 0 else ops.default_scan_jobs()

    # Load every per-prefix ledger once (keyed by manifest key), and record the
    # most recent ledger write as the "last scan" time.
    full_ledger: Dict[str, dict] = {}
    ledger_ok = 0
    last_scan_ns = 0
    if ledger_dir.is_dir():
        for key, _hexp, _p in entries:
            data = _load_prefix_ledger(ledger_dir, key)
            full_ledger[key] = data
            ledger_ok += len(data)
            try:
                mt = _ledger_file(ledger_dir, key).stat().st_mtime_ns
                last_scan_ns = max(last_scan_ns, mt)
            except OSError:
                pass

    totals = {'objects': 0, 'verified_unchanged': 0,
              'unscanned_or_changed': 0, 'bytes': 0}
    with ThreadPoolExecutor(max_workers=n_jobs) as pool:
        futures = [pool.submit(_status_one_prefix, key, p, ledger_dir,
                               full_ledger) for key, _hexp, p in entries]
        for fut in as_completed(futures):
            c = fut.result()
            for k in totals:
                totals[k] += c[k]

    # Bad count + when, from the persisted bad report (if any).
    bad_report = load_bad_report(bad_report_file(remote_dir))
    bad_count = 0
    bad_scanned_at = None
    if bad_report:
        bad_count = len(bad_report.get('bad', []))
        bad_scanned_at = bad_report.get('scanned_at')

    last_scan_at = None
    if last_scan_ns:
        last_scan_at = _dt.datetime.fromtimestamp(
            last_scan_ns / 1e9, _dt.timezone.utc).isoformat(timespec='seconds')

    return {
        'report_version': REPORT_VERSION,
        'layout': layout,
        'ledger_ok': ledger_ok,
        'ledger_last_scan': last_scan_at,
        'bad': bad_count,
        'bad_scanned_at': bad_scanned_at,
        'total_objects': totals['objects'],
        'verified_unchanged': totals['verified_unchanged'],
        'unscanned_or_changed': totals['unscanned_or_changed'],
        'bytes': totals['bytes'],
    }


def format_status_summary(status: dict, remote_label: str = '') -> str:
    """Render the instant status report as a human-readable summary."""
    header = f"Remote: {remote_label}  ({status['layout']})" if remote_label \
        else f"Layout: {status['layout']}"
    last = status.get('ledger_last_scan') or 'never'
    lines = [
        header,
        f"  Verified OK: {status['ledger_ok']:,} (last scan {last})",
        f"  Bad:         {status['bad']:,}"
        + (f" (as of {status['bad_scanned_at']})"
           if status.get('bad_scanned_at') else ''),
        f"  Total blobs: {status['total_objects']:,} "
        f"({utils.format_size(status['bytes'])})",
        f"  Unscanned/changed since ledger: "
        f"{status['unscanned_or_changed']:,} (would be re-hashed next run)",
    ]
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
    use_ledger: bool = True,
    full: bool = False,
    verbose: bool = False,
) -> Path:
    """Verify one worker's prefix partition and write a partial report.

    Called inside a submitted qxub job. The partial report is merged by the
    parent once all workers finish. Each worker owns a disjoint set of
    prefixes, so per-prefix ledger files in the remote are written without
    contention.
    """
    prefixes = hpc.get_prefixes_for_worker(worker_id, num_workers)
    totals, bad_entries, incomplete_entries, _layout = verify_remote(
        remote_dir, jobs=jobs, only_prefixes=prefixes, progress=verbose,
        use_ledger=use_ledger, full=full)

    part_path = report_dir / f'part_{worker_id}.json'
    with open(part_path, 'w') as f:
        json.dump({'totals': totals, 'bad': bad_entries,
                   'incomplete': incomplete_entries}, f)
    return part_path


def _merge_parts(
    report_dir: Path,
) -> Tuple[Dict[str, int], List[dict], List[dict]]:
    """Combine all ``part_*.json`` partial reports in ``report_dir``."""
    totals = _zero_totals()
    bad_entries: List[dict] = []
    incomplete_entries: List[dict] = []
    for part in sorted(report_dir.glob('part_*.json')):
        with open(part) as f:
            data = json.load(f)
        for k in _TOTALS_KEYS:
            totals[k] += data.get('totals', {}).get(k, 0)
        bad_entries.extend(data.get('bad', []))
        incomplete_entries.extend(data.get('incomplete', []))
    bad_entries.sort(key=lambda e: e['path'])
    incomplete_entries.sort(key=lambda e: e['path'])
    return totals, bad_entries, incomplete_entries


def parallel_verify(
    remote_name: Optional[str],
    num_workers: int,
    jobs: Optional[int] = None,
    use_ledger: bool = True,
    full: bool = False,
    qxub_args: Optional[List[str]] = None,
    wait: bool = True,
    verbose: bool = False,
) -> Tuple[List[str], Path, Optional[dict]]:
    """Distribute verification across ``num_workers`` compute nodes via qxub.

    Returns ``(job_ids, report_dir, report)``. ``report`` is the merged
    report dict when ``wait`` is True, else ``None`` (jobs still running).
    """
    import subprocess
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
        if full:
            worker_cmd.append('--full')
        if not use_ledger:
            worker_cmd.append('--no-ledger')
        if verbose:
            worker_cmd.append('--verbose')

        cmd = hpc.build_qxub_command(
            f'dt-remote-verify-{job_id}-w{worker_id}', worker_cmd, qxub_args)
        if verbose:
            print(f"Submitting worker {worker_id}: {' '.join(cmd)}")

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

    totals, bad_entries, incomplete_entries = _merge_parts(report_dir)
    report = build_report(name, url, layout, totals, bad_entries,
                          incomplete_entries, jobs or 0, ledger_used=use_ledger)
    return job_ids, report_dir, report


def _detect_layout(remote_dir: Path) -> str:
    from .archive import operations as ops
    return ops.detect_source_layout(remote_dir)
