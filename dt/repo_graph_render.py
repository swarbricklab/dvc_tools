"""Renderers for the repo-level dependency graph.

Text, Mermaid, Graphviz and JSON output for :class:`dt.repo_graph.RepoGraph`.

Every renderer assumes the graph may contain cycles, so none of them
topologically sorts. The text renderer marks a repeat visit and stops
descending; the graph formats emit back-edges normally, which both Mermaid and
Graphviz handle.

Unresolved nodes are always rendered distinctly. An incomplete graph that looks
complete is worse than no graph.
"""

import json
from typing import Dict, List, Optional, Set

from .repo_graph import (
    GAP_STATUSES,
    STATUS_HINTS,
    STATUS_NO_ACCESS,
    STATUS_NO_DVC,
    STATUS_NOT_SCANNED,
    STATUS_OK,
    RepoGraph,
    RepoNode,
)


# Marker shown against a node whose subtree was already printed.
CYCLE_MARK = '(cycle)'
SEEN_MARK = '(seen above)'


def _short(repo_id: str) -> str:
    """Drop the host prefix for display: github.com/org/repo -> org/repo."""
    parts = repo_id.split('/')
    return '/'.join(parts[1:]) if len(parts) >= 3 else repo_id


def _node_suffix(node: Optional[RepoNode]) -> str:
    """Status annotation shown after a node label."""
    if node is None:
        return ''
    if node.status == STATUS_NO_DVC:
        return '  [no .dvc files]'
    if node.status == STATUS_NOT_SCANNED:
        return '  ...'
    if node.status in GAP_STATUSES:
        return f'  [{node.status.upper()}]'
    return ''


# =============================================================================
# Text
# =============================================================================

def render_text(
    graph: RepoGraph,
    include_paths: bool = False,
    show_gaps: bool = True,
    direction: str = 'up',
) -> str:
    """Render the graph as an indented tree rooted at ``graph.root``.

    Args:
        graph: The graph to render.
        include_paths: Show example import paths under each edge.
        show_gaps: Append the access-gap report.
        direction: ``up`` walks to the repos this one imports from, ``down``
            to the repos that import from it, ``both`` prints two trees.
    """
    if direction not in ('up', 'down', 'both'):
        raise ValueError(
            f"Unknown direction '{direction}' (expected up, down, or both)"
        )

    lines: List[str] = []
    root_node = graph.nodes.get(graph.root)

    if direction == 'down':
        lines.append(f"Repos importing from {graph.root}")
    else:
        mode_note = ('source repos at their default branch'
                     if graph.mode == 'head'
                     else 'source repos at the pinned rev_lock')
        lines.append(f"Repo dependency graph for {graph.root}")
        lines.append(f"  ({mode_note})")
    lines.append("")

    # A repo graph is a DAG-with-cycles, not a tree: shared neighbours would
    # otherwise be reprinted under every parent, turning 32 edges into hundreds
    # of lines. Each subtree is expanded once and referenced after.
    if direction in ('up', 'both'):
        if direction == 'both':
            lines.append("Upstream (repos this one imports from):")
            lines.append("")
        lines.append(f"{_short(graph.root)}{_node_suffix(root_node)}")
        _render_children(
            graph, graph.root, set(), '', lines, include_paths, set(),
        )

    if direction in ('down', 'both'):
        if direction == 'both':
            lines.append("")
            lines.append("Downstream (repos that import from this one):")
            lines.append("")
        lines.append(f"{_short(graph.root)}{_node_suffix(root_node)}")
        _render_children(
            graph, graph.root, set(), '', lines, include_paths, set(),
            direction='targets',
        )
        if not graph.targets_of(graph.root):
            lines.append("  (no importing repos found in the index)")

    lines.append("")
    n_edges = len(graph.edges)
    resolved = sum(1 for n in graph.nodes.values()
                   if n.status in (STATUS_OK, STATUS_NO_DVC))
    lines.append(
        f"  {len(graph.nodes)} repos ({resolved} resolved), {n_edges} edges"
    )

    if graph.cycles:
        lines.append("")
        lines.append("Cycles:")
        for cycle in graph.cycles:
            if len(cycle) == 1:
                lines.append(f"  {_short(cycle[0])} (imports from itself)")
            else:
                lines.append("  " + " <-> ".join(_short(c) for c in cycle))

    if graph.truncated:
        n = len(graph.truncated)
        lines.append("")
        lines.append(
            f"  {n} repo{'s' if n != 1 else ''} not expanded (depth limit); "
            f"marked ... above"
        )

    if graph.truncated_revs:
        lines.append("")
        lines.append("Truncated revisions (scanned a subset):")
        for repo_id, count in sorted(graph.truncated_revs.items()):
            lines.append(
                f"  {_short(repo_id)}: {count} further rev(s) not scanned"
            )

    if show_gaps and graph.gaps:
        lines.append("")
        lines.append(format_gaps(graph))

    return "\n".join(lines)


def _render_children(
    graph: RepoGraph,
    repo_id: str,
    ancestors: Set[str],
    prefix: str,
    lines: List[str],
    include_paths: bool,
    expanded: Set[str],
    direction: str = 'sources',
) -> None:
    """Recursively print the neighbours of ``repo_id`` in one direction.

    ``sources`` walks upstream (what this repo imports from); ``targets`` walks
    downstream (what imports from it).
    """
    if direction == 'targets':
        sources = graph.targets_of(repo_id)
    else:
        sources = graph.sources_of(repo_id)
    if not sources:
        return

    branch_ancestors = ancestors | {repo_id}
    for i, source in enumerate(sources):
        last = (i == len(sources) - 1)
        connector = '└── ' if last else '├── '
        child_prefix = prefix + ('    ' if last else '│   ')

        node = graph.nodes.get(source)
        if direction == 'targets':
            edge = graph.edges.get((repo_id, source))
        else:
            edge = graph.edges.get((source, repo_id))
        label = _short(source)

        detail = ''
        if edge:
            detail = f"  ({edge.n_imports} import"
            detail += "s)" if edge.n_imports != 1 else ")"

        if source in branch_ancestors:
            # Following this would loop forever.
            lines.append(f"{prefix}{connector}{label}{detail}  {CYCLE_MARK}")
            continue

        has_children = bool(
            graph.targets_of(source) if direction == 'targets'
            else graph.sources_of(source)
        )
        if source in expanded and has_children:
            lines.append(f"{prefix}{connector}{label}{detail}  {SEEN_MARK}")
            continue

        lines.append(f"{prefix}{connector}{label}{detail}{_node_suffix(node)}")
        expanded.add(source)

        if include_paths and edge:
            for sample in edge.sample_paths:
                lines.append(f"{child_prefix}    {sample}")

        _render_children(
            graph, source, branch_ancestors, child_prefix, lines,
            include_paths, expanded, direction,
        )


def format_gaps(graph: RepoGraph) -> str:
    """Render the access-gap report."""
    gaps = graph.gaps
    if not gaps:
        return "Access gaps: none -- every repo in the graph was resolved."

    lines = [f"Access gaps ({len(gaps)} repo"
             f"{'s' if len(gaps) != 1 else ''} not fully resolved):"]

    by_status: Dict[str, List[RepoNode]] = {}
    for node in gaps:
        by_status.setdefault(node.status, []).append(node)

    for status, nodes in by_status.items():
        hint = STATUS_HINTS.get(status, '')
        lines.append("")
        lines.append(f"  {status} -- {hint}")
        for node in nodes:
            lines.append(f"    {_short(node.repo_id)}")
            if node.detail and status != STATUS_NOT_SCANNED:
                lines.append(f"      {node.detail.splitlines()[0]}")

    if any(n.status == STATUS_NO_ACCESS for n in gaps):
        lines.append("")
        lines.append("  The graph below these repos is incomplete.")

    return "\n".join(lines)


# =============================================================================
# Mermaid
# =============================================================================

def _mermaid_id(repo_id: str, ids: Dict[str, str]) -> str:
    """Stable, syntax-safe node id for mermaid."""
    if repo_id not in ids:
        ids[repo_id] = f"n{len(ids)}"
    return ids[repo_id]


def render_mermaid(graph: RepoGraph, include_counts: bool = True) -> str:
    """Render as a Mermaid flowchart.

    Arrows point in the direction data flows: from the source repo into the
    repo that imports it.
    """
    ids: Dict[str, str] = {}
    lines = ["```mermaid", "flowchart LR"]

    for repo_id in sorted(graph.nodes):
        node = graph.nodes[repo_id]
        nid = _mermaid_id(repo_id, ids)
        label = _short(repo_id)
        if node.status in GAP_STATUSES:
            label += f"<br/>{node.status}"
        elif node.status == STATUS_NO_DVC:
            label += "<br/>no .dvc"
        lines.append(f'    {nid}["{label}"]')

    for (source, target), edge in sorted(graph.edges.items()):
        sid = _mermaid_id(source, ids)
        tid = _mermaid_id(target, ids)
        if include_counts:
            lines.append(f"    {sid} -->|{edge.n_imports}| {tid}")
        else:
            lines.append(f"    {sid} --> {tid}")

    root_id = ids.get(graph.root)
    if root_id:
        lines.append(
            f"    style {root_id} stroke-width:3px"
        )
    for repo_id in sorted(graph.nodes):
        if graph.nodes[repo_id].status in GAP_STATUSES:
            lines.append(
                f"    style {ids[repo_id]} stroke-dasharray: 5 5"
            )

    lines.append("```")
    return "\n".join(lines)


# =============================================================================
# Graphviz
# =============================================================================

def render_dot(graph: RepoGraph) -> str:
    """Render as a Graphviz digraph, for graphs too large for Mermaid."""
    lines = ["digraph repo_deps {", "    rankdir=LR;",
             '    node [shape=box, fontname="Helvetica"];']

    for repo_id in sorted(graph.nodes):
        node = graph.nodes[repo_id]
        attrs = [f'label="{_short(repo_id)}"']
        if node.is_root:
            attrs.append('penwidth=3')
        if node.status in GAP_STATUSES:
            attrs.append('style=dashed')
            attrs.append(f'xlabel="{node.status}"')
        elif node.status == STATUS_NO_DVC:
            attrs.append('style=dotted')
        lines.append(f'    "{repo_id}" [{", ".join(attrs)}];')

    for (source, target), edge in sorted(graph.edges.items()):
        lines.append(
            f'    "{source}" -> "{target}" [label="{edge.n_imports}"];'
        )

    lines.append("}")
    return "\n".join(lines)


# =============================================================================
# JSON
# =============================================================================

def render_json(graph: RepoGraph) -> str:
    """Render the full graph as JSON, gaps and cycles included."""
    return json.dumps(graph.to_dict(), indent=2)


RENDERERS = {
    'text': render_text,
    'mermaid': render_mermaid,
    'dot': render_dot,
    'json': render_json,
}


def render(graph: RepoGraph, fmt: str, **kwargs) -> str:
    """Render ``graph`` in the named format."""
    if fmt not in RENDERERS:
        raise ValueError(
            f"Unknown format '{fmt}' (expected one of {sorted(RENDERERS)})"
        )
    if fmt == 'text':
        return render_text(graph, **kwargs)
    return RENDERERS[fmt](graph)
