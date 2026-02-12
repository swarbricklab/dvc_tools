"""Discover and verify access to storage backends used by a DVC project.

Scans DVC config, .dvc files, dt config, and git remotes to build a
complete picture of every storage endpoint the current project depends on.
"""

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from . import config as cfg
from . import remote as remote_mod
from . import utils
from .errors import AuthError


# =============================================================================
# Endpoint types
# =============================================================================

#: Valid endpoint type strings for --type filtering
ENDPOINT_TYPES = frozenset({
    'filesystem',
    'ssh',
    's3',
    'gs',
    'http',
    'git',
})


@dataclass
class Endpoint:
    """A single storage endpoint discovered by ``dt auth list``.

    Attributes:
        type: One of the :data:`ENDPOINT_TYPES` strings.
        url: The canonical URL or path for this endpoint.
        source: Human-readable description of where this was discovered
            (e.g. ``"cache.root"``, ``"DVC remote 'origin'"``).
        local_path: For SSH remotes on a local host, the equivalent
            local filesystem path.
        children: Secondary endpoints discovered *through* this one
            (e.g. remotes of an import source repository).
    """

    type: str
    url: str
    source: str
    local_path: Optional[str] = None
    children: List['Endpoint'] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Deduplication helpers
    # ------------------------------------------------------------------

    @property
    def key(self) -> Tuple[str, str]:
        """Stable deduplication key (type, canonical url)."""
        return (self.type, self.url)

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Convert to a JSON-serialisable dictionary."""
        d: dict = {
            'type': self.type,
            'url': self.url,
            'source': self.source,
        }
        if self.local_path:
            d['local_path'] = self.local_path
        if self.children:
            d['children'] = [c.to_dict() for c in self.children]
        return d


# =============================================================================
# URL classification
# =============================================================================

def classify_url(url: str) -> str:
    """Return the endpoint type string for a URL.

    >>> classify_url('/g/data/a56/dvc_cache')
    'filesystem'
    >>> classify_url('ssh://gadi.nci.org.au/data')
    'ssh'
    >>> classify_url('s3://bucket/prefix')
    's3'
    >>> classify_url('gs://bucket/prefix')
    'gs'
    >>> classify_url('https://example.com/data')
    'http'
    >>> classify_url('git@github.com:org/repo.git')
    'git'
    """
    url = url.strip()

    if url.startswith('/') or url.startswith('file://'):
        return 'filesystem'

    if url.startswith('ssh://'):
        return 'ssh'

    if url.startswith('s3://'):
        return 's3'

    if url.startswith('gs://'):
        return 'gs'

    if url.startswith('http://') or url.startswith('https://'):
        return 'http'

    # SCP-style SSH: user@host:/path  (has colon after host, before path)
    if ':' in url and not '://' in url:
        # Could be SCP-style git or SSH
        # git@github.com:org/repo.git  → git
        # user@host:/some/path          → ssh
        after_colon = url.split(':', 1)[1]
        if after_colon.startswith('/'):
            return 'ssh'
        return 'git'

    return 'filesystem'  # fallback for bare paths


# =============================================================================
# Discovery: individual sources
# =============================================================================

def _discover_dt_config() -> List[Endpoint]:
    """Endpoints from dt configuration (cache.root, remote.root)."""
    endpoints: List[Endpoint] = []

    cache_root = cfg.get_value('cache.root')
    if cache_root:
        endpoints.append(Endpoint(
            type='filesystem',
            url=cache_root,
            source='cache.root',
        ))

    remote_root = cfg.get_value('remote.root')
    if remote_root:
        # Append project name to match the actual remote path
        project_name = utils.get_project_name()
        full_path = str(Path(remote_root) / project_name)
        endpoints.append(Endpoint(
            type='filesystem',
            url=full_path,
            source='remote.root',
        ))

    return endpoints


def _discover_dvc_remotes(repo_path: Optional[Path] = None) -> List[Endpoint]:
    """Endpoints from ``dvc remote list --project``.

    Uses project scope only so that local overrides (e.g. a convenience
    local-remote) are excluded.
    """
    endpoints: List[Endpoint] = []

    remotes = remote_mod.list_remotes(repo_path, project_only=True)
    for name, url, is_default in remotes:
        if not url:
            continue

        ep_type = classify_url(url)
        label = f"DVC remote '{name}'"
        if is_default:
            label += ' (default)'

        ep = Endpoint(type=ep_type, url=url, source=label)

        # For SSH remotes, check local-host equivalence
        if ep_type == 'ssh':
            local_path = remote_mod.extract_local_path(url)
            if local_path:
                ep.local_path = local_path

        endpoints.append(ep)

    return endpoints


def _discover_git_remotes(repo_path: Optional[Path] = None) -> List[Endpoint]:
    """Endpoints from ``git remote -v`` (fetch URLs only)."""
    endpoints: List[Endpoint] = []
    cwd = str(repo_path) if repo_path else None

    try:
        result = subprocess.run(
            ['git', 'remote', '-v'],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=cwd,
        )
        if result.returncode != 0:
            return endpoints
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return endpoints

    seen: Set[str] = set()
    for line in result.stdout.strip().splitlines():
        if not line or '(fetch)' not in line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        name, url = parts[0], parts[1]
        if url in seen:
            continue
        seen.add(url)

        endpoints.append(Endpoint(
            type=classify_url(url),
            url=url,
            source=f"git remote '{name}'",
        ))

    return endpoints


def _discover_import_sources(
    repo_path: Optional[Path] = None,
    verbose: bool = False,
) -> List[Endpoint]:
    """Endpoints from import ``.dvc`` files (``deps.repo.url``).

    For each unique source repository URL, also discovers its DVC remotes
    via the tmp-clone infrastructure.
    """
    import yaml

    endpoints: List[Endpoint] = []
    search_root = repo_path or Path.cwd()

    # Collect unique import URLs from .dvc files
    import_urls: Dict[str, str] = {}  # url -> first dvc file that uses it
    for dvc_file in sorted(search_root.rglob('*.dvc')):
        # Skip .dvc directory itself and .dt/tmp clones
        rel = str(dvc_file.relative_to(search_root))
        if rel.startswith('.dvc') or rel.startswith('.dt'):
            continue

        try:
            with open(dvc_file) as f:
                data = yaml.safe_load(f)
        except Exception:
            continue

        if not isinstance(data, dict):
            continue

        deps = data.get('deps')
        if not deps or not isinstance(deps, list):
            continue

        for dep in deps:
            repo = dep.get('repo') if isinstance(dep, dict) else None
            if repo and isinstance(repo, dict):
                url = repo.get('url')
                if url and url not in import_urls:
                    import_urls[url] = rel

    # Build endpoints for each unique import source
    for url, first_file in import_urls.items():
        ep = Endpoint(
            type=classify_url(url),
            url=url,
            source=f"import source ({first_file})",
        )

        # Try to discover DVC remotes of the source repo via tmp clone
        try:
            source_remotes = remote_mod.list_remotes_from_repo(
                        url, project_only=True)
            for rname, rurl, r_default in source_remotes:
                if not rurl:
                    continue
                child_type = classify_url(rurl)
                child_label = f"DVC remote '{rname}' of {_short_repo_name(url)}"
                if r_default:
                    child_label += ' (default)'

                child = Endpoint(type=child_type, url=rurl, source=child_label)

                # Check local-host equivalence for SSH children
                if child_type == 'ssh':
                    local_path = remote_mod.extract_local_path(rurl)
                    if local_path:
                        child.local_path = local_path

                ep.children.append(child)

            if verbose and ep.children:
                print(f"  Discovered {len(ep.children)} remote(s) for {_short_repo_name(url)}")
        except Exception as exc:
            if verbose:
                print(f"  Could not discover remotes for {url}: {exc}")

        endpoints.append(ep)

    return endpoints


# =============================================================================
# Top-level discovery
# =============================================================================

def discover_endpoints(
    repo_path: Optional[Path] = None,
    type_filter: Optional[Set[str]] = None,
    verbose: bool = False,
) -> List[Endpoint]:
    """Discover every storage endpoint the current project relies on.

    Scans dt config, DVC remotes, git remotes, and import ``.dvc`` files.

    Args:
        repo_path: Project root (defaults to cwd).
        type_filter: If given, only return endpoints whose ``type`` is in
            this set.  Children are also filtered.
        verbose: Print progress information.

    Returns:
        Deduplicated list of :class:`Endpoint` objects.
    """
    project_name = utils.get_project_name()
    print(f"Scanning endpoints for project '{project_name}'...")

    all_eps: List[Endpoint] = []

    # 1. dt config (cache.root, remote.root)
    dt_eps = _discover_dt_config()
    all_eps.extend(dt_eps)
    if verbose and dt_eps:
        print(f"  dt config: {len(dt_eps)} endpoint(s)")

    # 2. DVC remotes (project scope only)
    dvc_eps = _discover_dvc_remotes(repo_path)
    all_eps.extend(dvc_eps)
    if verbose and dvc_eps:
        print(f"  DVC remotes (project scope): {len(dvc_eps)} endpoint(s)")

    # 3. Git remotes
    git_eps = _discover_git_remotes(repo_path)
    all_eps.extend(git_eps)
    if verbose and git_eps:
        print(f"  git remotes: {len(git_eps)} endpoint(s)")

    # 4. Import sources (+ their DVC remotes via tmp clone)
    import_eps = _discover_import_sources(repo_path, verbose=verbose)
    all_eps.extend(import_eps)
    if verbose and import_eps:
        print(f"  import sources: {len(import_eps)} endpoint(s)")

    # Deduplicate by (type, url) keeping first occurrence
    seen: Set[Tuple[str, str]] = set()
    unique: List[Endpoint] = []
    for ep in all_eps:
        if ep.key not in seen:
            seen.add(ep.key)
            unique.append(ep)
        else:
            # Merge children from duplicates
            for existing in unique:
                if existing.key == ep.key:
                    _merge_children(existing, ep)
                    break

    # Deduplicate children within each endpoint
    for ep in unique:
        if ep.children:
            child_seen: Set[Tuple[str, str]] = set()
            deduped: List[Endpoint] = []
            for child in ep.children:
                if child.key not in child_seen:
                    child_seen.add(child.key)
                    deduped.append(child)
            ep.children = deduped

    # Apply type filter
    if type_filter:
        unique = _apply_type_filter(unique, type_filter)

    return unique


# =============================================================================
# Formatting
# =============================================================================

def format_endpoints(endpoints: List[Endpoint]) -> str:
    """Format endpoints for human-readable terminal output.

    Groups by type, shows children indented with ``→`` prefix.
    """
    project_name = utils.get_project_name()
    lines: List[str] = [f"Endpoints for project '{project_name}':"]

    # Group by type, preserving discovery order within each group
    groups: Dict[str, List[Endpoint]] = {}
    for ep in endpoints:
        groups.setdefault(ep.type, []).append(ep)

    # Display in a stable order
    type_order = ['filesystem', 'ssh', 's3', 'gs', 'http', 'git']
    for t in type_order:
        eps = groups.get(t)
        if not eps:
            continue

        lines.append('')
        lines.append(f'  {t}')
        for ep in eps:
            source_tag = f'({ep.source})'
            lines.append(f'    {ep.url:<50s} {source_tag}')
            if ep.local_path:
                lines.append(f'      → local equivalent: {ep.local_path} (host is local)')
            for child in ep.children:
                child_tag = f'({child.source})'
                lines.append(f'      → remote: {child.url:<44s} {child_tag}')
                if child.local_path:
                    lines.append(f'          → local equivalent: {child.local_path} (host is local)')

    if not any(groups.values()):
        lines.append('')
        lines.append('  (no endpoints discovered)')

    return '\n'.join(lines)


def format_endpoints_json(endpoints: List[Endpoint]) -> str:
    """Format endpoints as a JSON string."""
    return json.dumps(
        [ep.to_dict() for ep in endpoints],
        indent=2,
    )


# =============================================================================
# Helpers
# =============================================================================

def _short_repo_name(url: str) -> str:
    """Short display name from a repository URL."""
    if not url:
        return 'unknown'
    name = url.rstrip('/').split('/')[-1]
    if name.endswith('.git'):
        name = name[:-4]
    return name


def _merge_children(target: Endpoint, source: Endpoint) -> None:
    """Merge children from *source* into *target*."""
    existing_keys = {c.key for c in target.children}
    for child in source.children:
        if child.key not in existing_keys:
            target.children.append(child)
            existing_keys.add(child.key)


def _apply_type_filter(
    endpoints: List[Endpoint],
    type_filter: Set[str],
) -> List[Endpoint]:
    """Keep only endpoints (and children) whose type is in *type_filter*."""
    filtered: List[Endpoint] = []
    for ep in endpoints:
        if ep.type in type_filter:
            # Also filter children
            ep.children = [c for c in ep.children if c.type in type_filter]
            filtered.append(ep)
        else:
            # Even if parent is excluded, promote matching children
            for child in ep.children:
                if child.type in type_filter:
                    # Promote child to top level, note its origin
                    child.source = f"{child.source} (via {ep.source})"
                    filtered.append(child)
    return filtered
