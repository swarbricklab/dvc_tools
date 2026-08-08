"""Repo-level dependency scanning for DVC Tools.

Extracts repo-to-repo import edges from ``.dvc`` files.

Imports live *only* in ``.dvc`` files. ``dvc.lock`` never carries a
``deps[].repo`` section: ``to_single_stage_lockfile`` opens with
``assert stage.cmd`` and import stages have no command, and even on that path
the lockfile serialiser builds its dep dicts by hand rather than calling
``RepoDependency.dumpd()``. ``dvc.yaml`` stage deps are plain path strings with
no ``repo`` key in the schema. So ``.dvc`` files are the complete source of
repo-level edges.

Scanning reads from either a working tree or an arbitrary git ref, so edges can
be collected across branches without checking anything out.
"""

import json
import re
import subprocess
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import yaml

from . import tmp as tmp_mod
from . import utils
from .errors import DepsError


# Hosts that treat owner/repo case-insensitively, so ``Swarbricklab/metadata``
# and ``swarbricklab/metadata`` are the same node. Both spellings occur in real
# .dvc files, so this is load-bearing rather than cosmetic.
CASE_INSENSITIVE_HOSTS = frozenset({'github.com', 'gitlab.com', 'bitbucket.org'})

# Assumed host when a bare owner/repo is given with no host.
DEFAULT_HOST = 'github.com'

# Schemes that indicate an `dvc import-url` external source rather than a repo.
_URL_SCHEME_RE = re.compile(r'^[a-z][a-z0-9+.-]*://', re.IGNORECASE)


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class ImportRef:
    """A single ``.dvc`` file's dependency on a path in another repo."""
    dvc_file: str          # repo-relative path to the .dvc file
    repo_url: str          # url exactly as written in the .dvc file
    repo_id: str           # normalised, e.g. github.com/swarbricklab/metadata
    path: str              # deps[].path -- path inside the source repo
    rev: Optional[str]     # deps[].repo.rev (branch/tag, if pinned loosely)
    rev_lock: Optional[str]  # deps[].repo.rev_lock (commit sha)
    out_path: str          # outs[].path in our repo
    size: Optional[int]
    nfiles: Optional[int]
    is_directory: bool
    md5: Optional[str] = None  # outs[].md5 in our repo
    ref: Optional[str] = None  # git ref this was found on (None = working tree)

    @property
    def locked_rev(self) -> str:
        """The most specific revision recorded, preferring the locked sha."""
        return self.rev_lock or self.rev or ''


@dataclass
class ExternalRef:
    """A ``dvc import-url`` dependency on a non-repo source (s3://, https://)."""
    dvc_file: str
    url: str
    scheme: str
    out_path: str
    size: Optional[int]
    ref: Optional[str] = None


@dataclass
class RepoEdge:
    """Aggregated edge: everything ``target`` imports from ``source``."""
    source: str                          # repo_id imported FROM
    target: str                          # repo_id doing the importing
    n_imports: int                       # distinct (src path, out path) pairs
    revs: List[str] = field(default_factory=list)         # distinct rev_locks
    sample_paths: List[str] = field(default_factory=list)  # src -> out samples
    total_size: Optional[int] = None
    refs: List[str] = field(default_factory=list)          # git refs seen on
    is_self_loop: bool = False
    source_url: Optional[str] = None  # clone URL as written in the .dvc file

    def to_dict(self) -> Dict[str, Any]:
        return {
            'source': self.source,
            'source_url': self.source_url,
            'target': self.target,
            'n_imports': self.n_imports,
            'revs': self.revs,
            'sample_paths': self.sample_paths,
            'total_size': self.total_size,
            'refs': self.refs,
            'is_self_loop': self.is_self_loop,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RepoEdge':
        """Rebuild an edge from :meth:`to_dict` output (for cached indexes)."""
        return cls(
            source=data['source'],
            target=data['target'],
            n_imports=data.get('n_imports', 0),
            revs=list(data.get('revs') or []),
            sample_paths=list(data.get('sample_paths') or []),
            total_size=data.get('total_size'),
            refs=list(data.get('refs') or []),
            is_self_loop=data.get('is_self_loop', False),
            source_url=data.get('source_url'),
        )


# =============================================================================
# Repo identity
# =============================================================================

def _fold_case(repo_id: str) -> str:
    """Apply the case and suffix rules that make two spellings one id."""
    repo_id = repo_id.rstrip('/')
    if repo_id.endswith('.git'):
        repo_id = repo_id[:-4]

    host, sep, rest = repo_id.partition('/')
    host = host.lower()
    if not sep:
        return host
    if host in CASE_INSENSITIVE_HOSTS:
        rest = rest.lower()
    return f"{host}/{rest}"


def normalize_repo_id(url: str, owner: Optional[str] = None) -> str:
    """Normalise a repository URL to a stable ``host/owner/repo`` identifier.

    All node identity comparisons go through here. This is the single point at
    which ``git@github.com:Swarbricklab/metadata.git`` and
    ``https://github.com/swarbricklab/metadata`` collapse to one node.

    Accepts URLs from .dvc files as well as the bare ``owner/repo`` and
    ``host/owner/repo`` forms people type on the command line.

    Args:
        url: Repository URL, SSH path, ``owner/repo``, or short name.
        owner: Optional owner used to expand short names.

    Returns:
        Normalised identifier, e.g. ``github.com/swarbricklab/metadata``.
    """
    if not url:
        return ''

    raw = url.strip()

    # Bare path forms are not URLs and would otherwise be mangled by the
    # fallback sanitiser, since they contain a '/' but no scheme or host.
    if '://' not in raw and '@' not in raw and ':' not in raw:
        parts = [p for p in raw.strip('/').split('/') if p]
        if len(parts) == 2:
            return _fold_case(f"{DEFAULT_HOST}/{parts[0]}/{parts[1]}")
        if len(parts) >= 3 and '.' in parts[0]:
            return _fold_case('/'.join(parts))

    try:
        repo_id = tmp_mod.get_repo_id(raw, owner)
    except Exception:
        # Short name with no owner configured, or an unparseable URL. Fall back
        # to a sanitised form rather than failing the whole scan.
        safe = re.sub(r'[^\w\-./]', '-', raw)
        repo_id = re.sub(r'-+', '-', safe).strip('-/')

    return _fold_case(repo_id)


def current_repo_id(root: Optional[Path] = None) -> Optional[str]:
    """Normalised repo id of the repo at ``root``, from its ``origin`` remote.

    Returns None if there is no origin remote (e.g. a repo that was never
    pushed), in which case the caller should fall back to a local label.
    """
    result = _git(['remote', 'get-url', 'origin'], root, check=False)
    if result is None or not result.strip():
        return None
    return normalize_repo_id(result.strip())


# =============================================================================
# Git plumbing
# =============================================================================

def _git(
    args: Sequence[str],
    root: Optional[Path] = None,
    check: bool = True,
) -> Optional[str]:
    """Run a git command and return stdout, or None on failure when not checking."""
    try:
        result = subprocess.run(
            ['git', *args],
            cwd=str(root) if root else None,
            capture_output=True,
            text=True,
        )
    except (OSError, FileNotFoundError) as e:
        if check:
            raise DepsError(f"Failed to run git: {e}")
        return None

    if result.returncode != 0:
        if check:
            raise DepsError(
                f"git {' '.join(args)} failed: {result.stderr.strip()}"
            )
        return None
    return result.stdout


def list_refs(
    root: Optional[Path] = None,
    include_local: bool = True,
    include_remote: bool = True,
) -> List[str]:
    """List branch refs available in the repo at ``root``.

    Remote-tracking branches are included because ``dt tmp`` clones keep their
    branches under ``refs/remotes/origin/`` rather than as local heads.

    Returns:
        Short ref names (e.g. ``main``, ``origin/feature-x``). Symbolic HEAD
        refs are excluded. Refs are *not* deduplicated by commit here -- callers
        should rely on the sha dedup in :func:`scan_refs` for that.
    """
    patterns = []
    if include_local:
        patterns.append('refs/heads')
    if include_remote:
        patterns.append('refs/remotes/origin')
    if not patterns:
        return []

    # Filter on the full refname: refs/remotes/origin/HEAD shortens to plain
    # "origin", so filtering the short form would let it through.
    out = _git(
        ['for-each-ref', '--format=%(refname)\t%(refname:short)', *patterns],
        root,
        check=False,
    )
    if out is None:
        return []

    refs = []
    for line in out.splitlines():
        full, tab, short = line.strip().partition('\t')
        if not tab or not short:
            continue
        if full.endswith('/HEAD'):
            continue
        refs.append(short)
    return refs


def default_ref(root: Optional[Path] = None) -> Optional[str]:
    """Best guess at the repo's default branch ref."""
    out = _git(
        ['symbolic-ref', '--short', 'refs/remotes/origin/HEAD'],
        root,
        check=False,
    )
    if out and out.strip():
        return out.strip()

    out = _git(['symbolic-ref', '--short', 'HEAD'], root, check=False)
    if out and out.strip():
        return out.strip()
    return None


def resolve_ref(ref: str, root: Optional[Path] = None) -> Optional[str]:
    """Resolve a ref to a commit sha, or None if it does not exist.

    Edge sets are a pure function of the commit sha, so this is what a cache
    layer keys on.
    """
    out = _git(['rev-parse', '--verify', f'{ref}^{{commit}}'], root, check=False)
    return out.strip() if out and out.strip() else None


def _cat_file_batch(
    oids: Sequence[str],
    root: Optional[Path] = None,
) -> Dict[str, bytes]:
    """Read many blobs in one ``git cat-file --batch`` pass.

    One subprocess per blob is far too slow on repos with thousands of .dvc
    files, so this batches them into a single call.

    Args:
        oids: Blob object ids to read.
        root: Repository directory.

    Returns:
        Mapping of oid to blob content. Missing oids are simply absent.
    """
    if not oids:
        return {}

    try:
        proc = subprocess.run(
            ['git', 'cat-file', '--batch'],
            cwd=str(root) if root else None,
            input=('\n'.join(oids) + '\n').encode(),
            capture_output=True,
        )
    except (OSError, FileNotFoundError) as e:
        raise DepsError(f"Failed to run git cat-file: {e}")

    if proc.returncode != 0:
        raise DepsError(
            f"git cat-file failed: {proc.stderr.decode(errors='replace').strip()}"
        )

    blobs: Dict[str, bytes] = {}
    buf = proc.stdout
    pos = 0
    for _ in oids:
        nl = buf.find(b'\n', pos)
        if nl == -1:
            break
        header = buf[pos:nl].decode(errors='replace').split()
        # Missing objects report "<oid> missing" and consume no body.
        if len(header) < 3:
            pos = nl + 1
            continue
        oid, size_s = header[0], header[2]
        try:
            size = int(size_s)
        except ValueError:
            pos = nl + 1
            continue
        start = nl + 1
        blobs[oid] = buf[start:start + size]
        pos = start + size + 1  # skip the trailing newline git appends
    return blobs


# =============================================================================
# Source adapters -- (path, text) pairs from a working tree or a git ref
# =============================================================================

def iter_dvc_files_worktree(
    root: Optional[Path] = None,
) -> Iterator[Tuple[str, str]]:
    """Yield ``(repo_relative_path, text)`` for .dvc files in the working tree."""
    root_path = Path(root) if root else Path('.')

    out = _git(['ls-files', '-z', '*.dvc'], root_path, check=False)
    if out is not None:
        paths = [p for p in out.split('\0') if p]
    else:
        paths = [
            str(p.relative_to(root_path))
            for p in root_path.rglob('*.dvc')
        ]

    for rel in paths:
        full = root_path / rel
        try:
            yield rel, full.read_text()
        except (OSError, UnicodeDecodeError):
            continue


def iter_dvc_files_ref(
    ref: str,
    root: Optional[Path] = None,
) -> Iterator[Tuple[str, str]]:
    """Yield ``(repo_relative_path, text)`` for .dvc files at a git ref.

    Reads the tree directly -- no checkout, so the working tree is untouched and
    scanning a second ref on an already-cloned repo costs milliseconds.
    """
    out = _git(['ls-tree', '-r', '-z', ref], root, check=False)
    if out is None:
        raise DepsError(f"Cannot read ref '{ref}' (does it exist?)")

    entries: List[Tuple[str, str]] = []  # (oid, path)
    for record in out.split('\0'):
        if not record:
            continue
        meta, tab, path = record.partition('\t')
        if not tab or not path.endswith('.dvc'):
            continue
        parts = meta.split()
        if len(parts) < 3 or parts[1] != 'blob':
            continue
        entries.append((parts[2], path))

    if not entries:
        return

    blobs = _cat_file_batch([oid for oid, _ in entries], root)
    for oid, path in entries:
        content = blobs.get(oid)
        if content is None:
            continue
        try:
            yield path, content.decode()
        except UnicodeDecodeError:
            continue


# =============================================================================
# Parsing
# =============================================================================

def _out_for_dep(data: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Pick the output entry corresponding to dep ``index``.

    Import .dvc files have exactly one dep and one out, but pair them
    positionally when counts match and fall back to the first out otherwise.
    """
    outs = data.get('outs') or []
    if not outs:
        return {}
    deps = data.get('deps') or []
    if len(outs) == len(deps) and index < len(outs):
        return outs[index] or {}
    return outs[0] or {}


def count_dvc_files(root: Optional[Path] = None, ref: Optional[str] = None) -> int:
    """Count .dvc files in a working tree or at a ref.

    Distinguishes "scanned, genuinely imports nothing" from "not a DVC repo" --
    without it a leaf node and a hole in the graph look identical.
    """
    if ref:
        out = _git(['ls-tree', '-r', '-z', '--name-only', ref], root, check=False)
    else:
        out = _git(['ls-files', '-z', '*.dvc'], root, check=False)
    if out is None:
        return 0
    return sum(1 for p in out.split('\0') if p.endswith('.dvc'))


def parse_import_refs(
    text: str,
    dvc_file: str,
    ref: Optional[str] = None,
    owner: Optional[str] = None,
) -> Tuple[List[ImportRef], List[ExternalRef]]:
    """Parse one ``.dvc`` file's text into repo imports and external sources.

    Pure function -- no filesystem or git access -- so it is cheap to test
    exhaustively against fixture text.

    Args:
        text: Contents of the .dvc file.
        dvc_file: Repo-relative path, recorded on each result.
        ref: Git ref the text came from (None for the working tree).
        owner: Optional owner for expanding short repo names.

    Returns:
        ``(import_refs, external_refs)``. Both are empty for a plain
        (non-import) .dvc file.
    """
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError:
        return [], []

    if not isinstance(data, dict):
        return [], []

    deps = data.get('deps')
    if not isinstance(deps, list):
        return [], []

    imports: List[ImportRef] = []
    externals: List[ExternalRef] = []

    for i, dep in enumerate(deps):
        if not isinstance(dep, dict):
            continue

        out = _out_for_dep(data, i)
        out_path = out.get('path', '') or ''
        size = out.get('size')
        md5 = out.get('md5') or ''

        repo = dep.get('repo')
        if isinstance(repo, dict) and repo.get('url'):
            url = repo.get('url', '')
            imports.append(ImportRef(
                dvc_file=dvc_file,
                repo_url=url,
                repo_id=normalize_repo_id(url, owner),
                path=dep.get('path', '') or '',
                rev=repo.get('rev'),
                rev_lock=repo.get('rev_lock'),
                out_path=out_path,
                size=size,
                nfiles=out.get('nfiles'),
                is_directory=bool(md5.endswith('.dir')),
                md5=md5 or None,
                ref=ref,
            ))
            continue

        # No repo key: `dvc import-url` external source, or a plain local dep.
        dep_path = dep.get('path', '') or ''
        scheme_match = _URL_SCHEME_RE.match(dep_path)
        if scheme_match:
            externals.append(ExternalRef(
                dvc_file=dvc_file,
                url=dep_path,
                scheme=scheme_match.group(0)[:-3].lower(),
                out_path=out_path,
                size=size,
                ref=ref,
            ))

    return imports, externals


# =============================================================================
# Scanning
# =============================================================================

def scan_imports(
    root: Optional[Path] = None,
    ref: Optional[str] = None,
    owner: Optional[str] = None,
) -> Tuple[List[ImportRef], List[ExternalRef]]:
    """Scan one working tree or ref for repo imports and external sources.

    Args:
        root: Repository directory (defaults to cwd).
        ref: Git ref to read from. None scans the working tree.
        owner: Optional owner for expanding short repo names.

    Returns:
        ``(import_refs, external_refs)``.
    """
    source = (
        iter_dvc_files_ref(ref, root) if ref
        else iter_dvc_files_worktree(root)
    )

    imports: List[ImportRef] = []
    externals: List[ExternalRef] = []
    for path, text in source:
        i, e = parse_import_refs(text, path, ref=ref, owner=owner)
        imports.extend(i)
        externals.extend(e)
    return imports, externals


def scan_refs(
    refs: Sequence[str],
    root: Optional[Path] = None,
    owner: Optional[str] = None,
) -> Tuple[List[ImportRef], List[ExternalRef]]:
    """Scan several refs, skipping refs that point at an already-scanned commit.

    Branches routinely share a commit, and an edge set is a pure function of the
    commit sha, so resolving first avoids rescanning identical trees.
    """
    imports: List[ImportRef] = []
    externals: List[ExternalRef] = []
    by_sha: Dict[str, Tuple[List[ImportRef], List[ExternalRef]]] = {}

    for ref in refs:
        sha = resolve_ref(ref, root)
        if sha is None:
            continue

        if sha in by_sha:
            # Same tree as a ref we already scanned -- replay it under this ref
            # so edge annotations still list every branch the import appears on.
            cached_i, cached_e = by_sha[sha]
            imports.extend(_with_ref(r, ref) for r in cached_i)
            externals.extend(_with_ref(r, ref) for r in cached_e)
            continue

        i, e = scan_imports(root, ref=ref, owner=owner)
        by_sha[sha] = (i, e)
        imports.extend(i)
        externals.extend(e)

    return imports, externals


def _with_ref(item, ref: str):
    """Copy a ref dataclass with a different ``ref`` field."""
    return replace(item, ref=ref)


# =============================================================================
# Aggregation
# =============================================================================

def aggregate_edges(
    refs: Sequence[ImportRef],
    target: str,
    max_paths: int = 5,
) -> List[RepoEdge]:
    """Collapse many individual imports into one edge per source repo.

    This is where the "thousands of imports, a handful of repos" reduction
    happens -- everything downstream works on edges, not individual imports.

    Args:
        refs: Import references, typically from :func:`scan_imports`.
        target: Normalised repo id of the importing repo.
        max_paths: How many example paths to retain per edge.

    Returns:
        Edges sorted by source repo id.
    """
    grouped: Dict[str, List[ImportRef]] = {}
    for r in refs:
        if not r.repo_id:
            continue
        grouped.setdefault(r.repo_id, []).append(r)

    edges: List[RepoEdge] = []
    for source, items in sorted(grouped.items()):
        # Count distinct imports, not (import, ref) pairs -- otherwise scanning
        # N branches would multiply the count by N.
        pairs = {(i.path, i.out_path) for i in items}

        revs = sorted({i.locked_rev for i in items if i.locked_rev})
        seen_refs = sorted({i.ref for i in items if i.ref})

        sizes = [i.size for i in items if i.size is not None]
        total_size = sum(sizes) if sizes else None

        samples = []
        for path, out_path in sorted(pairs)[:max_paths]:
            samples.append(f"{path} -> {out_path}" if out_path else path)

        edges.append(RepoEdge(
            source=source,
            target=target,
            n_imports=len(pairs),
            revs=revs,
            sample_paths=samples,
            total_size=total_size,
            refs=seen_refs,
            is_self_loop=(source == target),
            # Keep the URL as written so cloning works for hosts whose id
            # cannot be turned back into a URL (self-hosted, ssh aliases, file://).
            source_url=next((i.repo_url for i in items if i.repo_url), None),
        ))

    return edges


def aggregate_external(
    refs: Sequence[ExternalRef],
) -> Dict[str, List[ExternalRef]]:
    """Group external (``import-url``) sources by scheme."""
    grouped: Dict[str, List[ExternalRef]] = {}
    for r in refs:
        grouped.setdefault(r.scheme, []).append(r)
    return grouped


# =============================================================================
# Reporting
# =============================================================================

def format_edges(
    edges: Sequence[RepoEdge],
    target: str,
    externals: Optional[Dict[str, List[ExternalRef]]] = None,
    include_paths: bool = False,
    scanned_refs: Optional[Sequence[str]] = None,
    default_ref: Optional[str] = None,
    show_refs: bool = False,
    max_refs: int = 5,
) -> str:
    """Render edges as human-readable text.

    When many refs are scanned the full ref list per edge is noise, so the
    default is a ``n/m branches`` count plus a marker on edges missing from the
    default branch -- those are the interesting ones, since they are imports
    that exist only on unmerged work.
    """
    lines: List[str] = []
    n_scanned = len(scanned_refs) if scanned_refs else 0

    scope = ''
    if n_scanned == 1:
        scope = f" (ref: {scanned_refs[0]})"
    elif n_scanned > 1:
        scope = f" ({n_scanned} branches, local + origin)"
    lines.append(f"Imports in {target}{scope}")
    lines.append("")

    if not edges:
        lines.append("  No repo imports found.")
    else:
        width = max(len(e.source) for e in edges)
        off_default = 0
        for e in edges:
            bits = [f"{e.n_imports} import" + ("s" if e.n_imports != 1 else "")]
            if len(e.revs) == 1:
                bits.append(f"rev {e.revs[0][:8]}")
            elif len(e.revs) > 1:
                bits.append(f"{len(e.revs)} revs")
            if e.total_size:
                bits.append(utils.format_size(e.total_size))
            if n_scanned > 1:
                bits.append(f"{len(e.refs)}/{n_scanned} branches")
            if e.is_self_loop:
                bits.append("self")

            missing_from_default = (
                default_ref is not None
                and n_scanned > 1
                and default_ref not in e.refs
            )
            if missing_from_default:
                off_default += 1
                bits.append(f"NOT on {default_ref}")

            lines.append(f"  {e.source:<{width}}  {'  '.join(bits)}")

            if include_paths:
                for s in e.sample_paths:
                    lines.append(f"      {s}")
                remaining = e.n_imports - len(e.sample_paths)
                if remaining > 0:
                    lines.append(f"      ... and {remaining} more")
            if show_refs and e.refs:
                shown = ', '.join(e.refs[:max_refs])
                extra = len(e.refs) - max_refs
                if extra > 0:
                    shown += f", ... and {extra} more"
                lines.append(f"      refs: {shown}")

        total = sum(e.n_imports for e in edges)
        lines.append("")
        lines.append(
            f"  {len(edges)} source repo{'s' if len(edges) != 1 else ''}, "
            f"{total} import{'s' if total != 1 else ''}"
        )
        if off_default:
            lines.append(
                f"  {off_default} source repo"
                f"{'s' if off_default != 1 else ''} not present on "
                f"{default_ref} (unmerged imports)"
            )

    if externals:
        lines.append("")
        lines.append("External sources (import-url):")
        for scheme, items in sorted(externals.items()):
            lines.append(f"  {scheme}://  {len(items)} import"
                         f"{'s' if len(items) != 1 else ''}")

    return "\n".join(lines)


def edges_to_json(
    edges: Sequence[RepoEdge],
    target: str,
    externals: Optional[Dict[str, List[ExternalRef]]] = None,
    scanned_refs: Optional[Sequence[str]] = None,
    default_ref: Optional[str] = None,
) -> str:
    """Render edges as JSON."""
    payload: Dict[str, Any] = {
        'target': target,
        'refs': list(scanned_refs) if scanned_refs else [],
        'default_ref': default_ref,
        'edges': [e.to_dict() for e in edges],
    }
    if externals is not None:
        payload['external'] = {
            scheme: [
                {'dvc_file': r.dvc_file, 'url': r.url, 'out_path': r.out_path}
                for r in items
            ]
            for scheme, items in sorted(externals.items())
        }
    return json.dumps(payload, indent=2)


# =============================================================================
# Top-level entry point
# =============================================================================

@dataclass
class ScanResult:
    """Aggregated outgoing edges for one repo."""
    target: str
    edges: List[RepoEdge]
    externals: Dict[str, List[ExternalRef]]
    scanned_refs: List[str]
    default_ref: Optional[str] = None


def list_imports(
    root: Optional[Path] = None,
    ref: Optional[str] = None,
    all_branches: bool = False,
    owner: Optional[str] = None,
    max_paths: int = 5,
) -> ScanResult:
    """Scan a repo and return its aggregated outgoing edges.

    Args:
        root: Repository directory (defaults to the current project root).
        ref: Single git ref to scan. None means the working tree.
        all_branches: Scan every local and origin branch instead.
        owner: Optional owner for expanding short repo names.
        max_paths: Example paths retained per edge.

    Returns:
        A :class:`ScanResult`.
    """
    if root is None:
        root = utils.find_project_root()
    root = Path(root)

    if all_branches:
        refs = list_refs(root)
        if not refs:
            raise DepsError(f"No branches found in {root}")
        imports, externals = scan_refs(refs, root, owner=owner)
        scanned = refs
    elif ref:
        imports, externals = scan_imports(root, ref=ref, owner=owner)
        scanned = [ref]
    else:
        imports, externals = scan_imports(root, owner=owner)
        scanned = []

    target = current_repo_id(root) or root.name
    edges = aggregate_edges(imports, target, max_paths=max_paths)
    return ScanResult(
        target=target,
        edges=edges,
        externals=aggregate_external(externals),
        scanned_refs=scanned,
        default_ref=default_ref(root),
    )
