"""Recursive repo-level dependency graph for DVC Tools.

Walks the import edges produced by :mod:`dt.dvc_deps` outward from a root repo,
cloning each source repo into the shared ``.dt/tmp/clones/`` area and scanning it
in turn.

The result is deliberately *not* assumed to be acyclic. Two repos can import
from each other, so cycles are detected and reported as back-edges rather than
treated as an error, and nothing here topologically sorts.

Repos that cannot be reached are recorded as nodes with a non-``ok`` status
instead of being dropped, so an incomplete graph never renders as a complete
one.
"""

import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from . import dvc_deps
from . import tmp as tmp_mod
from . import utils
from .dvc_deps import RepoEdge
from .errors import DepsError


# How the revision of each source repo is chosen.
MODE_HEAD = 'head'      # follow each source repo's default branch
MODE_PINNED = 'pinned'  # follow the rev_lock we import at

# Node resolution outcomes. Everything except 'ok' and 'no_dvc' is an access gap.
STATUS_OK = 'ok'
STATUS_NO_DVC = 'no_dvc'
STATUS_NO_ACCESS = 'no_access'
STATUS_NOT_FOUND = 'not_found'
STATUS_CLONE_FAILED = 'clone_failed'
STATUS_NOT_SCANNED = 'not_scanned'

# A genuine gap is a repo we tried and failed to reach. Depth truncation is
# something the user asked for, so it is tracked separately -- reporting it as a
# gap would bury real access problems in expected noise.
GAP_STATUSES = frozenset({
    STATUS_NO_ACCESS, STATUS_NOT_FOUND, STATUS_CLONE_FAILED,
})

STATUS_HINTS = {
    STATUS_NO_ACCESS: "no read access (or the repo does not exist -- GitHub "
                      "does not distinguish). Try: dt auth request --repo <url>",
    STATUS_NOT_FOUND: "repository does not exist (renamed, deleted, or wrong owner)",
    STATUS_CLONE_FAILED: "clone or fetch failed (network, timeout, or git error)",
}

# Distinct pinned revisions scanned per source repo before truncating. Bounded
# because a repo imported at many revs would otherwise multiply the work.
MAX_PINNED_REVS = 3


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class RepoNode:
    """One repository in the graph."""
    repo_id: str
    url: Optional[str] = None
    depth: int = 0
    status: str = STATUS_OK
    is_root: bool = False
    scanned_revs: List[str] = field(default_factory=list)
    n_dvc_files: int = 0
    detail: Optional[str] = None  # raw error text, for honest gap reporting

    @property
    def is_gap(self) -> bool:
        return self.status in GAP_STATUSES

    def to_dict(self) -> Dict[str, Any]:
        return {
            'repo_id': self.repo_id,
            'url': self.url,
            'depth': self.depth,
            'status': self.status,
            'is_root': self.is_root,
            'scanned_revs': self.scanned_revs,
            'n_dvc_files': self.n_dvc_files,
            'detail': self.detail,
        }


@dataclass
class RepoGraph:
    """A repo-level dependency graph. May contain cycles."""
    root: str
    nodes: Dict[str, RepoNode] = field(default_factory=dict)
    edges: Dict[Tuple[str, str], RepoEdge] = field(default_factory=dict)
    cycles: List[List[str]] = field(default_factory=list)
    mode: str = MODE_HEAD
    truncated_revs: Dict[str, int] = field(default_factory=dict)

    @property
    def gaps(self) -> List[RepoNode]:
        """Nodes we tried and failed to reach, worst first.

        Excludes depth-truncated nodes -- see :attr:`truncated`.
        """
        order = {
            STATUS_NO_ACCESS: 0, STATUS_NOT_FOUND: 1, STATUS_CLONE_FAILED: 2,
        }
        return sorted(
            (n for n in self.nodes.values() if n.is_gap),
            key=lambda n: (order.get(n.status, 9), n.repo_id),
        )

    @property
    def truncated(self) -> List[RepoNode]:
        """Nodes left unexpanded because of an explicit depth limit."""
        return sorted(
            (n for n in self.nodes.values() if n.status == STATUS_NOT_SCANNED),
            key=lambda n: n.repo_id,
        )

    def sources_of(self, repo_id: str) -> List[str]:
        """Repos that ``repo_id`` imports from."""
        return sorted(s for (s, t) in self.edges if t == repo_id)

    def targets_of(self, repo_id: str) -> List[str]:
        """Repos that import from ``repo_id``."""
        return sorted(t for (s, t) in self.edges if s == repo_id)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'root': self.root,
            'mode': self.mode,
            'nodes': [n.to_dict() for n in sorted(
                self.nodes.values(), key=lambda n: (n.depth, n.repo_id)
            )],
            'edges': [e.to_dict() for e in sorted(
                self.edges.values(), key=lambda e: (e.target, e.source)
            )],
            'cycles': self.cycles,
            'gaps': [n.to_dict() for n in self.gaps],
            'truncated': [n.repo_id for n in self.truncated],
            'truncated_revs': self.truncated_revs,
        }


# =============================================================================
# Cloning and error classification
# =============================================================================

def _classify_git_error(stderr: str) -> Tuple[str, str]:
    """Map git stderr onto a node status.

    Note that over SSH GitHub deliberately reports "Repository not found" for a
    private repo you cannot read as well as for one that does not exist, so that
    case is treated as ``no_access`` -- the actionable reading -- with the raw
    text preserved.
    """
    text = (stderr or '').strip()
    low = text.lower()

    if 'permission denied' in low or 'access rights' in low \
            or 'authentication failed' in low or '403' in low:
        return STATUS_NO_ACCESS, text
    if 'repository not found' in low or 'could not read from remote' in low:
        return STATUS_NO_ACCESS, text
    if 'does not exist' in low or '404' in low:
        return STATUS_NOT_FOUND, text
    return STATUS_CLONE_FAILED, text


def _clone_or_refresh(
    url: str,
    repo_id: str,
    refresh: bool = True,
    verbose: bool = False,
) -> Tuple[Optional[Path], str, Optional[str]]:
    """Clone or update a source repo under ``.dt/tmp/clones/``.

    Shares the location used by ``dt tmp`` so clones are reused and remain
    manageable with ``dt tmp list`` / ``dt tmp clean``. Unlike ``dt tmp``'s own
    refresh this fetches every branch (not ``--depth 1``) and never raises --
    failures come back as a status for the caller to record.

    Returns:
        ``(path, status, detail)``. ``path`` is None unless status is ``ok``.
    """
    repo_path = tmp_mod.get_tmp_dir() / repo_id

    if repo_path.exists():
        if not refresh:
            return repo_path, STATUS_OK, None
        result = subprocess.run(
            ['git', 'fetch', '--all', '--prune', '--quiet'],
            cwd=str(repo_path), capture_output=True, text=True,
        )
        if result.returncode != 0:
            # A stale clone is still usable for scanning; report the failure but
            # keep going rather than losing a whole subtree.
            if verbose:
                print(f"  warning: fetch failed for {repo_id}, using cached clone")
            return repo_path, STATUS_OK, result.stderr.strip() or None
        return repo_path, STATUS_OK, None

    repo_path.parent.mkdir(parents=True, exist_ok=True)
    if verbose:
        print(f"  cloning {repo_id}...")

    result = subprocess.run(
        ['git', 'clone', '--quiet', url, str(repo_path)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        status, detail = _classify_git_error(result.stderr)
        return None, status, detail

    return repo_path, STATUS_OK, None


def _ensure_rev(repo_path: Path, rev: str) -> bool:
    """Make ``rev`` readable in the clone, fetching it if needed."""
    if dvc_deps.resolve_ref(rev, repo_path):
        return True
    subprocess.run(
        ['git', 'fetch', '--quiet', 'origin', rev],
        cwd=str(repo_path), capture_output=True, text=True,
    )
    return dvc_deps.resolve_ref(rev, repo_path) is not None


# =============================================================================
# Scanning one source repo
# =============================================================================

@dataclass
class _ScanOutcome:
    repo_id: str
    url: str
    status: str
    detail: Optional[str] = None
    edges: List[RepoEdge] = field(default_factory=list)
    scanned_revs: List[str] = field(default_factory=list)
    n_dvc_files: int = 0
    truncated_revs: int = 0


def _scan_source(
    repo_id: str,
    url: str,
    pinned_revs: Sequence[str],
    mode: str,
    refresh: bool,
    owner: Optional[str],
    max_paths: int,
    verbose: bool,
) -> _ScanOutcome:
    """Clone one source repo and collect its outgoing edges."""
    repo_path, status, detail = _clone_or_refresh(
        url, repo_id, refresh=refresh, verbose=verbose
    )
    if repo_path is None:
        return _ScanOutcome(repo_id, url, status, detail)

    # Work out which revisions of this repo to read.
    truncated = 0
    if mode == MODE_PINNED and pinned_revs:
        revs = list(dict.fromkeys(pinned_revs))
        if len(revs) > MAX_PINNED_REVS:
            truncated = len(revs) - MAX_PINNED_REVS
            revs = revs[:MAX_PINNED_REVS]
        revs = [r for r in revs if _ensure_rev(repo_path, r)]
        if not revs:
            head = dvc_deps.default_ref(repo_path)
            revs = [head] if head else []
    else:
        head = dvc_deps.default_ref(repo_path)
        revs = [head] if head else []

    if not revs:
        return _ScanOutcome(
            repo_id, url, STATUS_CLONE_FAILED,
            'no readable branch in clone',
        )

    imports = []
    for rev in revs:
        found, _ = dvc_deps.scan_imports(repo_path, ref=rev, owner=owner)
        imports.extend(found)

    # Count across every rev scanned, not just the first: with several pinned
    # revs a repo can have .dvc files at one and none at another, and reporting
    # "no .dvc files" for a repo whose edges we just collected is a lie.
    n_dvc = max(
        (dvc_deps.count_dvc_files(repo_path, ref=rev) for rev in revs),
        default=0,
    )
    edges = dvc_deps.aggregate_edges(imports, repo_id, max_paths=max_paths)

    return _ScanOutcome(
        repo_id=repo_id,
        url=url,
        status=STATUS_OK if n_dvc else STATUS_NO_DVC,
        edges=edges,
        scanned_revs=[r for r in revs if r],
        n_dvc_files=n_dvc,
        truncated_revs=truncated,
    )


# =============================================================================
# Cycle detection
# =============================================================================

def detect_cycles(edges: Dict[Tuple[str, str], RepoEdge]) -> List[List[str]]:
    """Find cycles via Tarjan's strongly connected components.

    Returns every SCC with more than one member, plus any self-loop, each as a
    sorted list of repo ids. Cycles are reported, not raised -- a mutual import
    between two repos is unusual but legitimate.
    """
    adjacency: Dict[str, List[str]] = {}
    for source, target in edges:
        # Edge source -> target means target depends on source, so traverse
        # from the importer outward to its sources.
        adjacency.setdefault(target, []).append(source)
        adjacency.setdefault(source, [])

    index_of: Dict[str, int] = {}
    low: Dict[str, int] = {}
    on_stack: Set[str] = set()
    stack: List[str] = []
    counter = [0]
    found: List[List[str]] = []

    def strongconnect(node: str) -> None:
        # Iterative to avoid a recursion limit on deep chains.
        work: List[Tuple[str, int]] = [(node, 0)]
        while work:
            current, child_i = work[-1]
            if child_i == 0:
                index_of[current] = low[current] = counter[0]
                counter[0] += 1
                stack.append(current)
                on_stack.add(current)

            recursed = False
            neighbours = adjacency.get(current, [])
            for i in range(child_i, len(neighbours)):
                nxt = neighbours[i]
                work[-1] = (current, i + 1)
                if nxt not in index_of:
                    work.append((nxt, 0))
                    recursed = True
                    break
                if nxt in on_stack:
                    low[current] = min(low[current], index_of[nxt])
            if recursed:
                continue

            if low[current] == index_of[current]:
                component = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == current:
                        break
                if len(component) > 1:
                    found.append(sorted(component))
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[current])

    for node in list(adjacency):
        if node not in index_of:
            strongconnect(node)

    for source, target in edges:
        if source == target:
            found.append([source])

    return sorted(found)


# =============================================================================
# Graph construction
# =============================================================================

def build_upstream(
    root: Optional[Path] = None,
    depth: Optional[int] = None,
    mode: str = MODE_HEAD,
    jobs: int = 4,
    refresh: bool = True,
    all_branches: bool = False,
    owner: Optional[str] = None,
    max_paths: int = 5,
    verbose: bool = False,
) -> RepoGraph:
    """Build the upstream repo dependency graph rooted at ``root``.

    Args:
        root: Repository directory (defaults to the current project root).
        depth: Maximum node depth to expand. None means unlimited. Depth 1
            yields the root's direct sources without cloning anything.
        mode: ``head`` scans each source repo's default branch; ``pinned``
            scans the rev_lock we import at. ``head`` describes the current
            shape of the ecosystem; ``pinned`` describes the provenance of the
            data actually in the workspace.
        jobs: Concurrent clone/scan workers.
        refresh: Fetch updates into existing clones.
        all_branches: Scan every branch of the *root* repo. Source repos are
            always read at a single revision, since scanning every branch of
            every upstream repo is rarely worth the wall-clock.
        owner: Optional owner for expanding short repo names.
        max_paths: Example paths retained per edge.
        verbose: Print progress.

    Returns:
        A :class:`RepoGraph`, possibly containing cycles.
    """
    if mode not in (MODE_HEAD, MODE_PINNED):
        raise DepsError(f"Unknown mode '{mode}' (expected 'head' or 'pinned')")

    if root is None:
        root = utils.find_project_root()
    root = Path(root)

    # Create .dt/.gitignore once, before any threads start.
    utils.ensure_dt_gitignore()

    root_result = dvc_deps.list_imports(
        root=root, all_branches=all_branches, owner=owner, max_paths=max_paths,
    )
    root_id = root_result.target

    graph = RepoGraph(root=root_id, mode=mode)
    graph.nodes[root_id] = RepoNode(
        repo_id=root_id,
        depth=0,
        status=STATUS_OK,
        is_root=True,
        scanned_revs=list(root_result.scanned_refs),
        n_dvc_files=dvc_deps.count_dvc_files(root),
    )

    # url and pinned revs for each repo we still need to visit
    pending: Dict[str, Tuple[str, List[str]]] = {}

    def absorb(edges: Sequence[RepoEdge], at_depth: int) -> None:
        """Record edges and queue any newly discovered source repos."""
        for edge in edges:
            key = (edge.source, edge.target)
            existing = graph.edges.get(key)
            if existing is None:
                graph.edges[key] = edge
            else:
                # Same pair seen from another revision -- keep the richer view.
                existing.n_imports = max(existing.n_imports, edge.n_imports)
                existing.revs = sorted(set(existing.revs) | set(edge.revs))
                existing.refs = sorted(set(existing.refs) | set(edge.refs))

            if edge.source in graph.nodes or edge.is_self_loop:
                continue
            url, revs = pending.get(edge.source, ('', []))
            if not url:
                url = edge.source_url or url_for_repo_id(edge.source)
            pending[edge.source] = (url, sorted(set(revs) | set(edge.revs)))

    absorb(root_result.edges, 0)

    current_depth = 1
    while pending:
        if depth is not None and current_depth >= depth:
            for repo_id, (url, _revs) in pending.items():
                graph.nodes[repo_id] = RepoNode(
                    repo_id=repo_id, url=url, depth=current_depth,
                    status=STATUS_NOT_SCANNED,
                    detail=f'depth limit {depth} reached',
                )
            break

        frontier = list(pending.items())
        pending = {}

        if verbose:
            print(f"Level {current_depth}: {len(frontier)} repo"
                  f"{'s' if len(frontier) != 1 else ''}")

        with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
            outcomes = list(pool.map(
                lambda item: _scan_source(
                    item[0], item[1][0], item[1][1], mode, refresh,
                    owner, max_paths, verbose,
                ),
                frontier,
            ))

        for outcome in outcomes:
            graph.nodes[outcome.repo_id] = RepoNode(
                repo_id=outcome.repo_id,
                url=outcome.url,
                depth=current_depth,
                status=outcome.status,
                scanned_revs=outcome.scanned_revs,
                n_dvc_files=outcome.n_dvc_files,
                detail=outcome.detail,
            )
            if outcome.truncated_revs:
                graph.truncated_revs[outcome.repo_id] = outcome.truncated_revs
            if outcome.status in (STATUS_OK, STATUS_NO_DVC):
                absorb(outcome.edges, current_depth)

        # Anything already visited must not be queued again -- this is what
        # terminates traversal when the graph contains a cycle.
        pending = {k: v for k, v in pending.items() if k not in graph.nodes}
        current_depth += 1

    graph.cycles = detect_cycles(graph.edges)
    return graph


def url_for_repo_id(repo_id: str) -> str:
    """Reconstruct an SSH clone URL from a normalised repo id.

    Only a fallback for when an edge carries no source URL. Since ids are
    lowercased for known hosts, this relies on those hosts treating owner/repo
    case-insensitively -- which is exactly why they are on that list.
    """
    parts = repo_id.split('/')
    if len(parts) >= 3:
        return f"git@{parts[0]}:{'/'.join(parts[1:])}.git"
    return repo_id
