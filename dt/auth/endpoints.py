"""Endpoint discovery and classification.

Scans DVC config, .dvc files, dt config, and git remotes to build a
complete picture of every storage endpoint the current project depends on.
"""

import json
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .. import config as cfg
from .. import remote as remote_mod
from .. import tmp as tmp_mod
from .. import utils
from ..errors import AuthError


def resolve_repo_url(repo: str) -> str:
    """Resolve a repository short name or URL to a full git URL.

    Supports short names (e.g. ``neochemo``) when the ``owner`` config
    key is set, as well as full SSH/HTTPS URLs.

    Raises:
        AuthError: If the short name cannot be resolved.
    """
    try:
        return tmp_mod.resolve_repository_url(repo)
    except tmp_mod.TmpError as e:
        raise AuthError(str(e))


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
        source: Human-readable description of where this was discovered.
        local_path: For SSH remotes on a local host, the equivalent
            local filesystem path.
        children: Secondary endpoints discovered *through* this one.
    """

    type: str
    url: str
    source: str
    local_path: Optional[str] = None
    children: List['Endpoint'] = field(default_factory=list)

    @property
    def key(self) -> Tuple[str, str]:
        """Stable deduplication key (type, canonical url)."""
        return (self.type, self.url)

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
    if ':' in url and '://' not in url:
        after_colon = url.split(':', 1)[1]
        if after_colon.startswith('/'):
            return 'ssh'
        return 'git'

    return 'filesystem'  # fallback for bare paths


# =============================================================================
# Discovery: individual sources
# =============================================================================

def _discover_dt_config() -> List[Endpoint]:
    """Endpoints from dt config and DVC cache configuration."""
    endpoints: List[Endpoint] = []

    cache_dir = utils.get_cache_dir()
    if cache_dir:
        cache_root = cache_dir.parent.parent
        endpoints.append(Endpoint(
            type='filesystem',
            url=str(cache_root),
            source='DVC cache (dvc cache dir)',
        ))
    else:
        cache_root_cfg = cfg.get_value('cache.root')
        if cache_root_cfg:
            endpoints.append(Endpoint(
                type='filesystem',
                url=cache_root_cfg,
                source='cache.root (dt config)',
            ))

    remote_root = cfg.get_value('remote.root')
    if remote_root:
        project_name = utils.get_project_name()
        full_path = str(Path(remote_root) / project_name)
        endpoints.append(Endpoint(
            type='filesystem',
            url=full_path,
            source='remote.root',
        ))

    return endpoints


def _discover_dvc_remotes(repo_path: Optional[Path] = None) -> List[Endpoint]:
    """Endpoints from ``dvc remote list --project``."""
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
    """Endpoints from import ``.dvc`` files (``deps.repo.url``)."""
    from ._helpers import _get_import_urls, _short_repo_name

    endpoints: List[Endpoint] = []
    import_urls = _get_import_urls(repo_path)

    for url, dvc_files in import_urls.items():
        if len(dvc_files) == 1:
            source_label = f"import source ({dvc_files[0]})"
        else:
            source_label = f"import source ({len(dvc_files)} files)"

        ep = Endpoint(
            type=classify_url(url),
            url=url,
            source=source_label,
        )

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
    """Discover every storage endpoint the current project relies on."""
    from ._helpers import _apply_type_filter, _merge_children

    project_name = utils.get_project_name()
    print(f"Scanning endpoints for project '{project_name}'...")

    all_eps: List[Endpoint] = []

    dt_eps = _discover_dt_config()
    all_eps.extend(dt_eps)
    if verbose and dt_eps:
        print(f"  dt config: {len(dt_eps)} endpoint(s)")

    dvc_eps = _discover_dvc_remotes(repo_path)
    all_eps.extend(dvc_eps)
    if verbose and dvc_eps:
        print(f"  DVC remotes (project scope): {len(dvc_eps)} endpoint(s)")

    git_eps = _discover_git_remotes(repo_path)
    all_eps.extend(git_eps)
    if verbose and git_eps:
        print(f"  git remotes: {len(git_eps)} endpoint(s)")

    import_eps = _discover_import_sources(repo_path, verbose=verbose)
    all_eps.extend(import_eps)
    if verbose and import_eps:
        print(f"  import sources: {len(import_eps)} endpoint(s)")

    # Deduplicate
    seen: Set[Tuple[str, str]] = set()
    unique: List[Endpoint] = []
    for ep in all_eps:
        if ep.key not in seen:
            seen.add(ep.key)
            unique.append(ep)
        else:
            for existing in unique:
                if existing.key == ep.key:
                    _merge_children(existing, ep)
                    break

    for ep in unique:
        if ep.children:
            child_seen: Set[Tuple[str, str]] = set()
            deduped: List[Endpoint] = []
            for child in ep.children:
                if child.key not in child_seen:
                    child_seen.add(child.key)
                    deduped.append(child)
            ep.children = deduped

    if type_filter:
        unique = _apply_type_filter(unique, type_filter)

    return unique


def discover_endpoints_from_repo(
    repo_url: str,
    type_filter: Optional[Set[str]] = None,
    verbose: bool = False,
) -> List[Endpoint]:
    """Discover endpoints for a remote repository via a temporary clone."""
    import tempfile

    repo_url = resolve_repo_url(repo_url)

    tmpdir = tempfile.mkdtemp(prefix='dt-auth-')
    clone_path = Path(tmpdir) / 'repo'

    try:
        if verbose:
            print(f"Cloning {repo_url} (shallow)...")

        result = subprocess.run(
            ['git', 'clone', '--depth', '1', '--single-branch',
             repo_url, str(clone_path)],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            raise AuthError(
                f"Failed to clone {repo_url}: {result.stderr.strip()}"
            )

        endpoints = discover_endpoints(
            repo_path=clone_path,
            type_filter=type_filter,
            verbose=verbose,
        )
        return endpoints
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# =============================================================================
# Formatting
# =============================================================================

def format_endpoints(endpoints: List[Endpoint]) -> str:
    """Format endpoints for human-readable terminal output."""
    import click

    project_name = utils.get_project_name()
    lines: List[str] = [
        click.style(f"\nEndpoints for '{project_name}'", bold=True),
    ]

    groups: Dict[str, List[Endpoint]] = {}
    for ep in endpoints:
        groups.setdefault(ep.type, []).append(ep)

    _type_colour: Dict[str, str] = {
        'filesystem': 'yellow',
        'ssh': 'cyan',
        's3': 'green',
        'gs': 'green',
        'http': 'magenta',
        'git': 'blue',
    }

    type_order = ['filesystem', 'ssh', 's3', 'gs', 'http', 'git']
    for t in type_order:
        eps = groups.get(t)
        if not eps:
            continue

        colour = _type_colour.get(t, 'white')
        lines.append('')
        lines.append(click.style(f'  [{t}]', fg=colour, bold=True))

        for ep in eps:
            url_str = click.style(ep.url, fg=colour)
            src_str = click.style(ep.source, dim=True)
            lines.append(f'    {url_str}')
            lines.append(f'      {src_str}')

            if ep.local_path:
                local_str = click.style(ep.local_path, fg='yellow')
                lines.append(f'      ↳ local path: {local_str}')

            for child in ep.children:
                child_colour = _type_colour.get(child.type, 'white')
                arrow = click.style('├─', dim=True)
                child_url = click.style(child.url, fg=child_colour)
                child_src = click.style(child.source, dim=True)
                lines.append(f'      {arrow} {child_url}')
                lines.append(f'      │  {child_src}')
                if child.local_path:
                    local_str = click.style(child.local_path, fg='yellow')
                    lines.append(f'      │  ↳ local path: {local_str}')

    if not any(groups.values()):
        lines.append('')
        lines.append(click.style('  (no endpoints discovered)', dim=True))

    lines.append('')
    return '\n'.join(lines)


def format_endpoints_json(endpoints: List[Endpoint]) -> str:
    """Format endpoints as a JSON string."""
    return json.dumps(
        [ep.to_dict() for ep in endpoints],
        indent=2,
    )
