"""Org-wide import index for DVC Tools.

Scans every repository in a GitHub org once and caches the resulting repo-level
edges, so that both upstream and downstream queries become in-memory lookups
rather than clone-and-scan work.

The design rests on one property: **an edge set is a pure function of a commit
sha**. Cached scan records are therefore immutable and never need invalidating.
All the volatility lives in a thin mutable layer -- which sha each ref currently
points at -- and GitHub hands that to us for the whole org in a single API call
via each repo's ``pushed_at`` timestamp. Repos whose ``pushed_at`` has not moved
since the last scan are skipped entirely, without any git operation at all.

Because the index is org-wide rather than project-specific it lives under the
user cache directory, not inside any one project's ``.dt/``.
"""

import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import config as cfg
from . import dvc_deps
from . import repo_graph
from . import tmp as tmp_mod
from . import utils
from .dvc_deps import RepoEdge
from .errors import DepsError


# Bumped when the on-disk format changes, so stale caches are ignored rather
# than mis-parsed.
SCHEMA_VERSION = 1

# Scan records retained per repo before the oldest are pruned.
MAX_SCANS_PER_REPO = 3

DEFAULT_HOST = 'github.com'


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class ScanRecord:
    """Edges found in one repo at one commit. Immutable once written."""
    sha: str
    ref: str
    n_dvc_files: int
    n_imports: int
    edges: List[RepoEdge] = field(default_factory=list)
    scanned_at: Optional[str] = None
    # 'clone' = repo was cloned and read; 'tree-api' = GitHub reported it has
    # no .dvc files, so it was never cloned. Recorded so the provenance of a
    # zero-edge result is never ambiguous.
    method: str = 'clone'

    def to_dict(self) -> Dict[str, Any]:
        return {
            'sha': self.sha,
            'ref': self.ref,
            'n_dvc_files': self.n_dvc_files,
            'n_imports': self.n_imports,
            'edges': [e.to_dict() for e in self.edges],
            'scanned_at': self.scanned_at,
            'method': self.method,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScanRecord':
        return cls(
            sha=data['sha'],
            ref=data.get('ref', ''),
            n_dvc_files=data.get('n_dvc_files', 0),
            n_imports=data.get('n_imports', 0),
            edges=[RepoEdge.from_dict(e) for e in data.get('edges') or []],
            scanned_at=data.get('scanned_at'),
            method=data.get('method', 'clone'),
        )


@dataclass
class RepoEntry:
    """One repository's place in the index."""
    repo_id: str
    url: str
    name: str = ''
    default_branch: Optional[str] = None
    pushed_at: Optional[str] = None
    status: str = repo_graph.STATUS_OK
    detail: Optional[str] = None
    last_checked: Optional[str] = None
    archived: bool = False
    fork: bool = False
    scans: Dict[str, ScanRecord] = field(default_factory=dict)

    @property
    def latest_scan(self) -> Optional[ScanRecord]:
        """Most recently written scan record."""
        if not self.scans:
            return None
        return max(
            self.scans.values(), key=lambda s: (s.scanned_at or '', s.sha)
        )

    @property
    def edges(self) -> List[RepoEdge]:
        scan = self.latest_scan
        return scan.edges if scan else []

    def manifest_dict(self) -> Dict[str, Any]:
        """Everything except scan records, which live in a separate file."""
        return {
            'repo_id': self.repo_id,
            'url': self.url,
            'name': self.name,
            'default_branch': self.default_branch,
            'pushed_at': self.pushed_at,
            'status': self.status,
            'detail': self.detail,
            'last_checked': self.last_checked,
            'archived': self.archived,
            'fork': self.fork,
        }

    @classmethod
    def from_manifest(cls, data: Dict[str, Any]) -> 'RepoEntry':
        return cls(
            repo_id=data['repo_id'],
            url=data.get('url', ''),
            name=data.get('name', ''),
            default_branch=data.get('default_branch'),
            pushed_at=data.get('pushed_at'),
            status=data.get('status', repo_graph.STATUS_OK),
            detail=data.get('detail'),
            last_checked=data.get('last_checked'),
            archived=data.get('archived', False),
            fork=data.get('fork', False),
        )


@dataclass
class OrgIndex:
    """The cached import index for one org."""
    org: str
    host: str = DEFAULT_HOST
    schema_version: int = SCHEMA_VERSION
    generated_at: Optional[str] = None
    repos: Dict[str, RepoEntry] = field(default_factory=dict)

    @property
    def scanned_repos(self) -> List[RepoEntry]:
        return [r for r in self.repos.values() if r.scans]

    @property
    def gaps(self) -> List[RepoEntry]:
        """Repos in the org we could not read."""
        return sorted(
            (r for r in self.repos.values()
             if r.status in repo_graph.GAP_STATUSES),
            key=lambda r: r.repo_id,
        )

    def all_edges(self) -> List[RepoEdge]:
        edges: List[RepoEdge] = []
        for entry in self.repos.values():
            edges.extend(entry.edges)
        return edges

    def summary(self) -> Dict[str, Any]:
        edges = self.all_edges()
        return {
            'org': self.org,
            'host': self.host,
            'generated_at': self.generated_at,
            'n_repos': len(self.repos),
            'n_scanned': len(self.scanned_repos),
            'n_gaps': len(self.gaps),
            'n_edges': len(edges),
            'n_importing_repos': len({e.target for e in edges}),
        }


# =============================================================================
# Cache location and persistence
# =============================================================================

def cache_root() -> Path:
    """Root directory for org indexes.

    Honours the ``deps.cache_dir`` config key, which lets a site point every
    user at one shared index instead of each rebuilding their own.
    """
    configured = cfg.get_value('deps.cache_dir')
    if configured:
        return Path(configured).expanduser()

    from platformdirs import user_cache_dir
    return Path(user_cache_dir('dvc-tools')) / 'repo-deps'


def org_cache_dir(org: str, host: str = DEFAULT_HOST) -> Path:
    return cache_root() / host / org


def _safe_filename(repo_id: str) -> str:
    return repo_id.replace('/', '__') + '.json'


def _write_atomic(path: Path, text: str) -> None:
    """Write via a temp file and rename, so a crash cannot truncate the cache."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(text)
    os.replace(tmp, path)


def load_index(org: str, host: str = DEFAULT_HOST) -> OrgIndex:
    """Load a cached index, or return an empty one if absent or stale.

    A schema mismatch discards the cache rather than risking a mis-parse.
    """
    index = OrgIndex(org=org, host=host)
    manifest_path = org_cache_dir(org, host) / 'index.json'
    if not manifest_path.exists():
        return index

    try:
        data = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError):
        return index

    if data.get('schema_version') != SCHEMA_VERSION:
        return index

    index.generated_at = data.get('generated_at')
    edges_dir = org_cache_dir(org, host) / 'edges'

    for entry_data in data.get('repos') or []:
        entry = RepoEntry.from_manifest(entry_data)
        scan_path = edges_dir / _safe_filename(entry.repo_id)
        if scan_path.exists():
            try:
                scans = json.loads(scan_path.read_text())
                entry.scans = {
                    sha: ScanRecord.from_dict(rec)
                    for sha, rec in scans.items()
                }
            except (OSError, json.JSONDecodeError, KeyError):
                entry.scans = {}
        index.repos[entry.repo_id] = entry

    return index


def save_index(index: OrgIndex) -> Path:
    """Persist the index.

    Scan records go in one file per repo rather than a single blob so that
    concurrent refreshes cannot clobber each other and one unreadable repo
    cannot poison the whole cache.
    """
    base = org_cache_dir(index.org, index.host)
    edges_dir = base / 'edges'
    edges_dir.mkdir(parents=True, exist_ok=True)

    index.generated_at = _now()
    manifest = {
        'schema_version': SCHEMA_VERSION,
        'org': index.org,
        'host': index.host,
        'generated_at': index.generated_at,
        'repos': [e.manifest_dict() for e in sorted(
            index.repos.values(), key=lambda r: r.repo_id
        )],
    }
    _write_atomic(base / 'index.json', json.dumps(manifest, indent=2))

    for entry in index.repos.values():
        if not entry.scans:
            continue
        pruned = _prune_scans(entry.scans)
        _write_atomic(
            edges_dir / _safe_filename(entry.repo_id),
            json.dumps(
                {sha: rec.to_dict() for sha, rec in pruned.items()}, indent=2
            ),
        )

    return base / 'index.json'


def _prune_scans(scans: Dict[str, ScanRecord]) -> Dict[str, ScanRecord]:
    """Keep only the most recent scan records for a repo."""
    if len(scans) <= MAX_SCANS_PER_REPO:
        return scans
    keep = sorted(
        scans.values(), key=lambda s: (s.scanned_at or '', s.sha), reverse=True
    )[:MAX_SCANS_PER_REPO]
    return {s.sha: s for s in keep}


def clear_index(org: str, host: str = DEFAULT_HOST) -> bool:
    """Delete a cached org index. Returns True if anything was removed."""
    import shutil
    base = org_cache_dir(org, host)
    if not base.exists():
        return False
    shutil.rmtree(base)
    return True


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


# =============================================================================
# GitHub access
# =============================================================================

def _check_gh() -> None:
    try:
        result = subprocess.run(
            ['gh', 'auth', 'status'], capture_output=True, text=True, timeout=30,
        )
    except (OSError, FileNotFoundError):
        raise DepsError(
            "GitHub CLI (gh) not found. Install it from https://cli.github.com "
            "-- it is required to enumerate repositories in an org."
        )
    except subprocess.TimeoutExpired:
        raise DepsError("gh auth status timed out")

    if result.returncode != 0:
        raise DepsError(
            "GitHub CLI is not authenticated. Run: gh auth login\n"
            "The 'read:org' scope is needed to list an organisation's repos."
        )


def list_org_repos(
    org: str,
    include_archived: bool = False,
    include_forks: bool = False,
) -> List[Dict[str, Any]]:
    """List an org's repositories in a single paginated API call.

    Falls back to the user endpoint when ``org`` is actually a user account.

    Returns:
        Dicts with name, ssh_url, default_branch, pushed_at, archived, fork,
        and size. ``pushed_at`` is the staleness signal the refresh relies on.
    """
    _check_gh()

    fields = ('.[] | {name, ssh_url, default_branch, pushed_at, archived, '
              'fork, size}')

    for endpoint in (f'orgs/{org}/repos', f'users/{org}/repos'):
        result = subprocess.run(
            ['gh', 'api', endpoint, '--paginate', '--jq', fields],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            repos = []
            for line in result.stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    repo = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if repo.get('archived') and not include_archived:
                    continue
                if repo.get('fork') and not include_forks:
                    continue
                if not repo.get('size'):
                    continue  # empty repo -- nothing to scan
                repos.append(repo)
            return repos

    raise DepsError(
        f"Could not list repositories for '{org}'.\n"
        f"Check the name and that your token has 'read:org'.\n"
        f"{result.stderr.strip()}"
    )


@dataclass
class TreeProbe:
    """Result of asking GitHub what files a repo contains, without cloning."""
    tree_sha: Optional[str]
    n_dvc_files: Optional[int]  # None when unknown -- caller must clone
    truncated: bool = False


def probe_dvc_files(org: str, name: str, branch: str) -> TreeProbe:
    """Count a repo's .dvc files via the git trees API, without cloning it.

    Most repos in an org have no DVC data at all, and cloning every one of them
    to discover that is the single most expensive part of building an index.
    One API call per repo answers it instead.

    A truncated response (very large trees) yields ``n_dvc_files=None``, which
    the caller must treat as "unknown, clone it" rather than "none".
    """
    result = subprocess.run(
        ['gh', 'api', f'repos/{org}/{name}/git/trees/{branch}?recursive=1',
         '--jq', '{sha: .sha, truncated: .truncated, '
                 'n: ([.tree[].path | select(endswith(".dvc"))] | length)}'],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return TreeProbe(tree_sha=None, n_dvc_files=None)

    try:
        data = json.loads(result.stdout.strip() or '{}')
    except json.JSONDecodeError:
        return TreeProbe(tree_sha=None, n_dvc_files=None)

    if data.get('truncated'):
        return TreeProbe(
            tree_sha=data.get('sha'), n_dvc_files=None, truncated=True,
        )
    return TreeProbe(tree_sha=data.get('sha'), n_dvc_files=data.get('n'))


def ls_remote_heads(url: str) -> Dict[str, str]:
    """Resolve every branch of a remote to a sha without cloning.

    Not used by the default refresh -- ``pushed_at`` covers the whole org in one
    API call -- but useful for checking a single repo's freshness precisely.
    """
    result = subprocess.run(
        ['git', 'ls-remote', '--heads', url],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return {}

    heads = {}
    for line in result.stdout.splitlines():
        parts = line.split('\t')
        if len(parts) != 2:
            continue
        sha, ref = parts[0].strip(), parts[1].strip()
        if ref.startswith('refs/heads/'):
            heads[ref[len('refs/heads/'):]] = sha
    return heads


# =============================================================================
# Refresh
# =============================================================================

def _scan_repo_for_index(
    entry: RepoEntry,
    refresh: bool,
    verbose: bool,
) -> RepoEntry:
    """Clone/refresh one repo and record its edges at the default branch."""
    repo_path, status, detail = repo_graph._clone_or_refresh(
        entry.url, entry.repo_id, refresh=refresh, verbose=verbose,
    )
    entry.last_checked = _now()

    if repo_path is None:
        entry.status = status
        entry.detail = detail
        return entry

    ref = dvc_deps.default_ref(repo_path)
    if not ref:
        entry.status = repo_graph.STATUS_CLONE_FAILED
        entry.detail = 'no readable branch in clone'
        return entry

    sha = dvc_deps.resolve_ref(ref, repo_path)
    if not sha:
        entry.status = repo_graph.STATUS_CLONE_FAILED
        entry.detail = f'could not resolve {ref}'
        return entry

    imports, _ = dvc_deps.scan_imports(repo_path, ref=ref)
    n_dvc = dvc_deps.count_dvc_files(repo_path, ref=ref)
    edges = dvc_deps.aggregate_edges(imports, entry.repo_id)

    entry.status = repo_graph.STATUS_OK if n_dvc else repo_graph.STATUS_NO_DVC
    entry.detail = None
    entry.scans[sha] = ScanRecord(
        sha=sha,
        ref=ref,
        n_dvc_files=n_dvc,
        n_imports=len(imports),
        edges=edges,
        scanned_at=_now(),
    )
    return entry


def refresh_index(
    org: str,
    host: str = DEFAULT_HOST,
    jobs: int = 8,
    force: bool = False,
    include_archived: bool = False,
    include_forks: bool = False,
    prefilter: bool = True,
    limit: Optional[int] = None,
    verbose: bool = False,
) -> Tuple[OrgIndex, Dict[str, int]]:
    """Build or update the org index.

    Two filters keep this cheap. Repos whose ``pushed_at`` has not moved since
    the last scan are skipped with no git operation at all, and of the rest,
    only those the trees API says contain ``.dvc`` files are cloned.

    Args:
        org: GitHub organisation or user.
        host: Git host (only github.com is supported for listing).
        jobs: Concurrent clone/scan workers.
        force: Rescan every repo, ignoring ``pushed_at``.
        include_archived: Include archived repos.
        include_forks: Include forks.
        prefilter: Use the trees API to skip repos with no .dvc files.
        limit: Scan at most this many repos this run (the rest are left for a
            later refresh and reported in stats, never silently dropped).
        verbose: Print progress.

    Returns:
        ``(index, stats)`` where stats counts scanned/skipped/failed repos.
    """
    index = load_index(org, host)
    listed = list_org_repos(
        org, include_archived=include_archived, include_forks=include_forks,
    )

    # Created once, before any threads start.
    utils.ensure_dt_gitignore()

    if verbose:
        print(f"{len(listed)} repo(s) in {org}")
        print(f"clones under {tmp_mod.get_tmp_dir()}")

    stats = {
        'listed': len(listed), 'scanned': 0, 'skipped': 0, 'failed': 0,
        'no_dvc': 0, 'limit_dropped': 0,
    }
    to_scan: List[RepoEntry] = []
    target_pushed_at: Dict[str, Optional[str]] = {}
    seen_ids = set()

    for repo in listed:
        url = repo.get('ssh_url') or ''
        repo_id = dvc_deps.normalize_repo_id(url) if url else \
            f"{host}/{org}/{repo['name']}".lower()
        seen_ids.add(repo_id)

        entry = index.repos.get(repo_id) or RepoEntry(repo_id=repo_id, url=url)
        entry.url = url or entry.url
        entry.name = repo.get('name', entry.name)
        entry.default_branch = repo.get('default_branch')
        entry.archived = bool(repo.get('archived'))
        entry.fork = bool(repo.get('fork'))
        index.repos[repo_id] = entry

        unchanged = (
            not force
            and entry.scans
            and entry.pushed_at
            and entry.pushed_at == repo.get('pushed_at')
        )
        if unchanged:
            stats['skipped'] += 1
            continue

        # The new pushed_at is only committed once a scan succeeds. Recording
        # it up front would make a transient clone failure permanent: the next
        # refresh would see an unchanged timestamp and skip the retry.
        target_pushed_at[repo_id] = repo.get('pushed_at')
        to_scan.append(entry)

    # Repos that vanished from the org listing are dropped from the index.
    for repo_id in list(index.repos):
        if repo_id not in seen_ids:
            del index.repos[repo_id]

    if limit is not None and len(to_scan) > limit:
        dropped = len(to_scan) - limit
        to_scan = to_scan[:limit]
        stats['limit_dropped'] = dropped
        if verbose:
            print(f"--limit {limit}: skipping {dropped} repo(s) this run")

    if verbose:
        print(f"{stats['skipped']} unchanged, {len(to_scan)} to check")

    # Ask GitHub which of these actually contain .dvc files. Most repos in an
    # org have none, and this turns "clone 225 repos" into "clone the few that
    # matter" at the cost of one API call each.
    if to_scan and prefilter:
        with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
            probes = list(pool.map(
                lambda e: probe_dvc_files(
                    org, e.name or e.repo_id.split('/')[-1],
                    e.default_branch or 'HEAD',
                ),
                to_scan,
            ))

        needs_clone = []
        for entry, probe in zip(to_scan, probes):
            if probe.n_dvc_files == 0 and probe.tree_sha:
                # Definitively empty of DVC data -- record that without cloning.
                entry.status = repo_graph.STATUS_NO_DVC
                entry.detail = None
                entry.last_checked = _now()
                entry.pushed_at = target_pushed_at.get(entry.repo_id)
                entry.scans[probe.tree_sha] = ScanRecord(
                    sha=probe.tree_sha,
                    ref=entry.default_branch or '',
                    n_dvc_files=0,
                    n_imports=0,
                    edges=[],
                    scanned_at=_now(),
                    method='tree-api',
                )
                index.repos[entry.repo_id] = entry
                stats['no_dvc'] += 1
            else:
                needs_clone.append(entry)

        if verbose:
            print(f"{stats['no_dvc']} have no .dvc files (skipped without "
                  f"cloning), {len(needs_clone)} to clone")
        to_scan = needs_clone

    if to_scan:
        with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
            scanned = list(pool.map(
                lambda e: _scan_repo_for_index(e, True, verbose), to_scan,
            ))
        for entry in scanned:
            index.repos[entry.repo_id] = entry
            if entry.status in repo_graph.GAP_STATUSES:
                stats['failed'] += 1
                # Leave pushed_at untouched so the next refresh retries.
            else:
                entry.pushed_at = target_pushed_at.get(entry.repo_id)
                stats['scanned'] += 1

    save_index(index)
    return index, stats


# =============================================================================
# Downstream queries
# =============================================================================

def invert_edges(index: OrgIndex) -> Dict[str, List[RepoEdge]]:
    """Map each source repo to the edges pointing *out* of it.

    The index stores edges per importing repo; downstream queries need the
    opposite direction.
    """
    inverted: Dict[str, List[RepoEdge]] = {}
    for entry in index.repos.values():
        for edge in entry.edges:
            inverted.setdefault(edge.source, []).append(edge)
    return inverted


def downstream_graph(
    index: OrgIndex,
    root_id: str,
    depth: Optional[int] = None,
) -> repo_graph.RepoGraph:
    """Build the graph of repos that import *from* ``root_id``.

    Pure lookup over the cached index -- no cloning, no network.
    """
    inverted = invert_edges(index)

    graph = repo_graph.RepoGraph(root=root_id)
    graph.nodes[root_id] = repo_graph.RepoNode(
        repo_id=root_id, depth=0, is_root=True,
        status=_status_for(index, root_id),
    )

    frontier = [root_id]
    current_depth = 1
    while frontier:
        if depth is not None and current_depth > depth:
            break

        next_frontier = []
        for source in frontier:
            for edge in inverted.get(source, []):
                graph.edges[(edge.source, edge.target)] = edge
                if edge.target in graph.nodes or edge.is_self_loop:
                    continue
                graph.nodes[edge.target] = repo_graph.RepoNode(
                    repo_id=edge.target,
                    url=_url_for(index, edge.target),
                    depth=current_depth,
                    status=_status_for(index, edge.target),
                )
                next_frontier.append(edge.target)

        frontier = next_frontier
        current_depth += 1

    graph.cycles = repo_graph.detect_cycles(graph.edges)
    return graph


def _status_for(index: OrgIndex, repo_id: str) -> str:
    entry = index.repos.get(repo_id)
    return entry.status if entry else repo_graph.STATUS_OK


def _url_for(index: OrgIndex, repo_id: str) -> Optional[str]:
    entry = index.repos.get(repo_id)
    return entry.url if entry else None


def merge_graphs(
    upstream: repo_graph.RepoGraph,
    downstream: repo_graph.RepoGraph,
) -> repo_graph.RepoGraph:
    """Combine an upstream and a downstream graph sharing the same root."""
    merged = repo_graph.RepoGraph(root=upstream.root, mode=upstream.mode)
    merged.nodes = dict(upstream.nodes)
    merged.truncated_revs = dict(upstream.truncated_revs)

    for repo_id, node in downstream.nodes.items():
        if repo_id not in merged.nodes:
            merged.nodes[repo_id] = node

    merged.edges = dict(upstream.edges)
    merged.edges.update(downstream.edges)
    merged.cycles = repo_graph.detect_cycles(merged.edges)
    return merged


def format_index_summary(index: OrgIndex, stats: Optional[Dict] = None) -> str:
    """Human-readable summary of an index."""
    s = index.summary()
    lines = [
        f"Import index for {s['org']} ({s['host']})",
        "",
        f"  repos listed:     {s['n_repos']}",
        f"  repos scanned:    {s['n_scanned']}",
        f"  repos unreadable: {s['n_gaps']}",
        f"  edges:            {s['n_edges']}",
        f"  repos importing:  {s['n_importing_repos']}",
        f"  generated:        {s['generated_at']}",
    ]
    if stats:
        lines += [
            "",
            f"  this run: {stats.get('scanned', 0)} cloned+scanned, "
            f"{stats.get('skipped', 0)} unchanged, "
            f"{stats.get('no_dvc', 0)} no .dvc files (not cloned), "
            f"{stats.get('failed', 0)} failed",
        ]
        if stats.get('limit_dropped'):
            lines.append(
                f"  {stats['limit_dropped']} repo(s) left unscanned by --limit"
                f" -- re-run to continue"
            )
    if index.gaps:
        lines += ["", "Unreadable repos:"]
        for entry in index.gaps:
            lines.append(f"  {entry.repo_id}  [{entry.status}]")
    lines += ["", f"  cached at {org_cache_dir(index.org, index.host)}"]
    return "\n".join(lines)
