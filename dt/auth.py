"""Discover and verify access to storage backends used by a DVC project.

Scans DVC config, .dvc files, dt config, and git remotes to build a
complete picture of every storage endpoint the current project depends on.
"""

import getpass
import json
import os
import platform
import shutil
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import date
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
    """Endpoints from dt config and DVC cache configuration.

    Discovers the DVC cache directory via ``utils.get_cache_dir()``
    (which reads ``Repo().cache.local.path``, the same value reported
    by ``dvc cache dir``).  Falls back to ``cache.root`` from dt
    config only if the DVC cache cannot be determined.

    Also discovers ``remote.root`` from dt config.
    """
    endpoints: List[Endpoint] = []

    # Cache directory — prefer DVC's own cache path
    cache_dir = utils.get_cache_dir()
    if cache_dir:
        # get_cache_dir() returns the files/md5 subdirectory; report
        # the cache root (its grandparent) for permission checking.
        cache_root = cache_dir.parent.parent
        endpoints.append(Endpoint(
            type='filesystem',
            url=str(cache_root),
            source='DVC cache (dvc cache dir)',
        ))
    else:
        # Fallback: dt config cache.root (may not be set)
        cache_root_cfg = cfg.get_value('cache.root')
        if cache_root_cfg:
            endpoints.append(Endpoint(
                type='filesystem',
                url=cache_root_cfg,
                source='cache.root (dt config)',
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
    import_urls: Dict[str, List[str]] = {}  # url -> list of dvc files
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
                if url:
                    import_urls.setdefault(url, []).append(rel)

    # Build endpoints for each unique import source
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

    Groups by type with coloured headers.  Uses ``click.style`` for
    ANSI colours so the output degrades gracefully when piped.
    """
    import click

    project_name = utils.get_project_name()
    lines: List[str] = [
        click.style(f"\nEndpoints for '{project_name}'", bold=True),
    ]

    # Group by type, preserving discovery order within each group
    groups: Dict[str, List[Endpoint]] = {}
    for ep in endpoints:
        groups.setdefault(ep.type, []).append(ep)

    # Colour per type
    _type_colour: Dict[str, str] = {
        'filesystem': 'yellow',
        'ssh': 'cyan',
        's3': 'green',
        'gs': 'green',
        'http': 'magenta',
        'git': 'blue',
    }

    # Display in a stable order
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

    lines.append('')  # trailing newline for breathing room
    return '\n'.join(lines)


def format_endpoints_json(endpoints: List[Endpoint]) -> str:
    """Format endpoints as a JSON string."""
    return json.dumps(
        [ep.to_dict() for ep in endpoints],
        indent=2,
    )


# =============================================================================
# Access checking
# =============================================================================

#: Result status constants
STATUS_PASS = 'pass'
STATUS_FAIL = 'fail'
STATUS_WARN = 'warn'
STATUS_SKIP = 'skip'


@dataclass
class CheckResult:
    """Outcome of a single access check.

    Attributes:
        endpoint: The endpoint that was checked.
        status: One of ``'pass'``, ``'fail'``, ``'warn'``, ``'skip'``.
        summary: Short human-readable result line (e.g. ``"read/write"``).
        details: Optional list of verbose detail lines.
        hints: Suggested remediation steps for failures.
    """

    endpoint: Endpoint
    status: str  # pass | fail | warn | skip
    summary: str
    details: List[str] = field(default_factory=list)
    hints: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = {
            'endpoint': self.endpoint.to_dict(),
            'status': self.status,
            'summary': self.summary,
        }
        if self.details:
            d['details'] = self.details
        if self.hints:
            d['hints'] = self.hints
        return d


# ---------------------------------------------------------------------
# Per-type checkers
# ---------------------------------------------------------------------

def _check_filesystem(ep: Endpoint, verbose: bool = False) -> CheckResult:
    """Check a filesystem endpoint for read/write access.

    Walks immediate subdirectories and reports per-subdir status.
    """
    path = Path(ep.url)

    if not path.exists():
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='path does not exist',
            hints=[f'Create the directory or mount the filesystem: {ep.url}'],
        )

    if not path.is_dir():
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='not a directory',
        )

    # Check root readability/writability
    readable = os.access(path, os.R_OK)
    writable = os.access(path, os.W_OK)

    if not readable:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='not readable',
            hints=[f'Check permissions: ls -la {ep.url}'],
        )

    # Walk immediate subdirectories
    try:
        subdirs = sorted([d for d in path.iterdir() if d.is_dir()])
    except PermissionError:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='cannot list directory contents',
            hints=[f'Check permissions: ls -la {ep.url}'],
        )

    if not subdirs:
        # Empty dir or flat — just report root status
        status = STATUS_PASS if writable else STATUS_FAIL
        summary = 'read/write' if writable else 'read-only'
        result = CheckResult(endpoint=ep, status=status, summary=summary)
        if not writable:
            result.hints.append(f'Check permissions: ls -la {ep.url}')
        return result

    ok_count = 0
    fail_count = 0
    detail_lines: List[str] = []
    failed_dirs: List[Path] = []

    for d in subdirs:
        d_read = os.access(d, os.R_OK)
        d_write = os.access(d, os.W_OK)
        if d_read and d_write:
            ok_count += 1
            if verbose:
                detail_lines.append(f'{d.name}  r/w')
        else:
            fail_count += 1
            failed_dirs.append(d)
            perms = []
            if not d_read:
                perms.append('not readable')
            if not d_write:
                perms.append('not writable')
            detail_lines.append(f'{d.name}  {", ".join(perms)}')

    total = ok_count + fail_count
    if fail_count == 0:
        status = STATUS_PASS if writable else STATUS_FAIL
        summary = f'read/write ({total}/{total} subdirs OK)'
        details = detail_lines if verbose else []
    else:
        status = STATUS_FAIL
        summary = f'{fail_count} of {total} subdirectories not accessible'
        details = detail_lines  # always show failures
        hints = []
        for d in failed_dirs[:3]:  # limit hints
            hints.append(
                f'Fix permissions: chmod -R g+rw {d}'
            )
        return CheckResult(
            endpoint=ep, status=status, summary=summary,
            details=details, hints=hints,
        )

    result = CheckResult(endpoint=ep, status=status, summary=summary, details=details)
    if not writable:
        result.hints.append(f'Root directory is read-only: {ep.url}')
    return result


def _check_ssh(ep: Endpoint, verbose: bool = False) -> CheckResult:
    """Check an SSH endpoint.

    If the host is local, delegates to :func:`_check_filesystem` via
    ``local_path``.  Otherwise tests the SSH connection directly.
    """
    if ep.local_path:
        # Host is local — check the filesystem path directly
        fs_ep = Endpoint(
            type='filesystem', url=ep.local_path, source=ep.source,
        )
        result = _check_filesystem(fs_ep, verbose=verbose)
        result.endpoint = ep  # restore original endpoint
        result.summary = f'checked as local path — {result.summary}'
        return result

    # Remote SSH — test connection
    # Extract host from ssh://[user@]host/path
    url = ep.url.strip()
    if url.startswith('ssh://'):
        netloc = url[6:].split('/')[0]
        host = netloc.split('@')[-1]
    elif '@' in url and ':' in url:
        # SCP-style user@host:/path
        host = url.split('@')[1].split(':')[0]
    else:
        host = url

    try:
        result_proc = subprocess.run(
            ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
             host],
            capture_output=True,
            text=True,
            timeout=10,
        )
        # SSH -T exits 0 on success, but some servers return 1 even when
        # connection works (e.g. GitHub).  A connection failure is typically
        # exit code 255.
        if result_proc.returncode != 255:
            return CheckResult(
                endpoint=ep, status=STATUS_PASS,
                summary='connection OK',
            )
        else:
            return CheckResult(
                endpoint=ep, status=STATUS_FAIL,
                summary='connection failed',
                hints=[
                    'Check your SSH agent has keys loaded: ssh-add -l',
                    'Ensure you connected with agent forwarding: ssh -A <host>',
                ],
            )
    except subprocess.TimeoutExpired:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='connection timed out',
            hints=[
                'Check your SSH agent has keys loaded: ssh-add -l',
                f'Test manually: ssh -T {host}',
            ],
        )
    except FileNotFoundError:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='ssh command not found',
            hints=['Install OpenSSH or ensure ssh is in your PATH'],
        )


def _get_dvc_remote_config(remote_name: str, key: str) -> Optional[str]:
    """Read a single DVC remote config value.

    Runs ``dvc config remote.<name>.<key>`` and returns the value,
    or ``None`` if not set.
    """
    try:
        result = subprocess.run(
            ['dvc', 'config', f'remote.{remote_name}.{key}'],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass
    return None


def _extract_remote_name(source: str) -> Optional[str]:
    """Extract the DVC remote name from an endpoint's source string.

    The source string is like ``"DVC remote 'nci' (default)"`` — this
    returns ``"nci"``.
    """
    import re
    m = re.search(r"DVC remote '([^']+)'", source)
    return m.group(1) if m else None


def _check_s3(ep: Endpoint) -> CheckResult:
    """Check an S3-compatible endpoint (AWS, R2, MinIO, …).

    Uses ``aws`` CLI with ``--endpoint-url`` if configured in DVC.
    """
    import shutil

    if not shutil.which('aws'):
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary='aws CLI not installed',
            hints=['Install the AWS CLI: pip install awscli'],
        )

    # Try to get endpoint URL from DVC remote config
    endpoint_url = None
    remote_name = _extract_remote_name(ep.source)
    if remote_name:
        endpoint_url = _get_dvc_remote_config(remote_name, 'endpointurl')

    extra_args: List[str] = []
    if endpoint_url:
        extra_args = ['--endpoint-url', endpoint_url]

    # 1. Check credentials
    try:
        cred_result = subprocess.run(
            ['aws', 'sts', 'get-caller-identity'] + extra_args,
            capture_output=True, text=True, timeout=10,
        )
    except (subprocess.TimeoutExpired, OSError):
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='credentials check timed out',
        )

    if cred_result.returncode != 0:
        hint = 'Configure AWS credentials in ~/.aws/credentials or environment variables'
        if endpoint_url:
            hint += f' for endpoint {endpoint_url}'
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='credentials not configured',
            hints=[hint],
        )

    # 2. Check bucket access
    bucket_prefix = ep.url  # s3://bucket/prefix
    try:
        ls_result = subprocess.run(
            ['aws', 's3', 'ls', bucket_prefix] + extra_args,
            capture_output=True, text=True, timeout=15,
        )
    except (subprocess.TimeoutExpired, OSError):
        return CheckResult(
            endpoint=ep, status=STATUS_WARN,
            summary='credentials OK, bucket check timed out',
        )

    if ls_result.returncode != 0:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='credentials OK, bucket not accessible',
            hints=[f'Check bucket exists and your credentials have access: {bucket_prefix}'],
        )

    return CheckResult(
        endpoint=ep, status=STATUS_PASS,
        summary='credentials OK, bucket accessible',
    )


def _check_gs(ep: Endpoint) -> CheckResult:
    """Check a GCS endpoint.

    Reports warnings rather than errors during the service-account
    to IAM transition.
    """
    import shutil

    if not shutil.which('gcloud'):
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary='gcloud CLI not installed',
            hints=['Install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install'],
        )

    # Check if any account is authenticated
    try:
        auth_result = subprocess.run(
            ['gcloud', 'auth', 'list', '--format=value(account)',
             '--filter=status:ACTIVE'],
            capture_output=True, text=True, timeout=10,
        )
    except (subprocess.TimeoutExpired, OSError):
        return CheckResult(
            endpoint=ep, status=STATUS_WARN,
            summary='gcloud auth check timed out',
        )

    if auth_result.returncode != 0 or not auth_result.stdout.strip():
        return CheckResult(
            endpoint=ep, status=STATUS_WARN,
            summary='no gcloud auth configured',
            hints=['Run: gcloud auth login'],
        )

    # Check bucket access
    if not shutil.which('gsutil'):
        return CheckResult(
            endpoint=ep, status=STATUS_WARN,
            summary=f'authenticated as {auth_result.stdout.strip()}, gsutil not available',
        )

    try:
        ls_result = subprocess.run(
            ['gsutil', 'ls', ep.url],
            capture_output=True, text=True, timeout=15,
        )
    except (subprocess.TimeoutExpired, OSError):
        return CheckResult(
            endpoint=ep, status=STATUS_WARN,
            summary='authenticated, bucket check timed out',
        )

    if ls_result.returncode != 0:
        return CheckResult(
            endpoint=ep, status=STATUS_WARN,
            summary='authenticated, bucket not accessible',
            hints=[f'Check bucket permissions: {ep.url}'],
        )

    return CheckResult(
        endpoint=ep, status=STATUS_PASS,
        summary='authenticated, bucket accessible',
    )


def _check_git(ep: Endpoint) -> CheckResult:
    """Check a git endpoint via ``git ls-remote``."""
    try:
        result = subprocess.run(
            ['git', 'ls-remote', '--exit-code', ep.url],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            return CheckResult(
                endpoint=ep, status=STATUS_PASS,
                summary='reachable',
            )
        else:
            hints = []
            if 'git@' in ep.url or ep.url.startswith('ssh://'):
                hints = [
                    'Check your SSH agent has keys loaded: ssh-add -l',
                    'Ensure you connected with agent forwarding: ssh -A <host>',
                ]
            return CheckResult(
                endpoint=ep, status=STATUS_FAIL,
                summary='not reachable',
                hints=hints,
            )
    except subprocess.TimeoutExpired:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='connection timed out',
            hints=[f'Test manually: git ls-remote {ep.url}'],
        )
    except FileNotFoundError:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='git command not found',
        )


def _check_http(ep: Endpoint) -> CheckResult:
    """Check an HTTP(S) endpoint via ``curl``."""
    import shutil

    if not shutil.which('curl'):
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary='curl not installed',
        )

    try:
        result = subprocess.run(
            ['curl', '-sf', '--head', '--max-time', '10', ep.url],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            return CheckResult(
                endpoint=ep, status=STATUS_PASS,
                summary='reachable',
            )
        else:
            return CheckResult(
                endpoint=ep, status=STATUS_FAIL,
                summary='not reachable',
                hints=[f'Test manually: curl -sf --head {ep.url}'],
            )
    except subprocess.TimeoutExpired:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='connection timed out',
        )


# The dispatcher used by check_endpoints
_CHECKERS = {
    'filesystem': _check_filesystem,
    'ssh': _check_ssh,
    's3': _check_s3,
    'gs': _check_gs,
    'git': _check_git,
    'http': _check_http,
}


def _check_dvc_remote(ep: Endpoint, remote_name: str,
                      verbose: bool = False) -> Optional[CheckResult]:
    """Check a DVC remote via DVC's own storage API.

    Uses ``Repo().cloud.get_remote_odb(name)`` to obtain a
    remote object-database, then ``odb.fs.exists(odb.path)`` to
    verify the remote is reachable using whatever credentials DVC
    has configured (service-account JSON, endpoint URLs, SSH keys,
    environment variables, etc.).

    This is authoritative: if DVC can reach the remote, access is
    confirmed regardless of whether external CLIs (``aws``, ``gcloud``,
    etc.) are installed or configured.

    Returns a :class:`CheckResult` on success or failure, or ``None``
    if the DVC Repo cannot be opened (caller should fall back to
    per-type checkers).
    """
    return _check_dvc_remote_impl(ep, remote_name, verbose=verbose)


def _check_dvc_remote_impl(ep: Endpoint, remote_name: str,
                           verbose: bool = False,
                           _repo_factory=None) -> Optional[CheckResult]:
    """Implementation of :func:`_check_dvc_remote`.

    Accepts an optional *_repo_factory* for testing.  In production
    this defaults to ``dvc.repo.Repo``.
    """
    if _repo_factory is None:
        try:
            from dvc.repo import Repo as DvcRepo
        except ImportError:
            return None
        _repo_factory = DvcRepo

    try:
        repo = _repo_factory()
    except Exception:
        return None

    try:
        odb = repo.cloud.get_remote_odb(remote_name)
    except Exception as exc:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary=f'DVC cannot initialise remote: {exc}',
            hints=[f'Check DVC remote config: dvc remote list'],
        )

    try:
        reachable = odb.fs.exists(odb.path)
    except Exception as exc:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary=f'remote not reachable: {exc}',
            hints=[f'Check credentials for {ep.url}'],
        )

    if not reachable:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='remote path does not exist',
            hints=[f'Check remote URL: dvc remote modify {remote_name} url <url>'],
        )

    # Remote is reachable — optionally list contents to confirm read access
    detail_lines: List[str] = []
    try:
        items = odb.fs.ls(odb.path)
        n_items = len(items)
        summary = f'accessible via DVC ({n_items} entries)'
        if verbose:
            for item in items[:20]:
                name = item.rsplit('/', 1)[-1] if isinstance(item, str) else str(item)
                detail_lines.append(name)
            if n_items > 20:
                detail_lines.append(f'... and {n_items - 20} more')
    except Exception:
        summary = 'reachable via DVC'

    return CheckResult(
        endpoint=ep, status=STATUS_PASS,
        summary=summary,
        details=detail_lines,
    )


# ---------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------

def check_endpoints(
    endpoints: Optional[List[Endpoint]] = None,
    type_filter: Optional[Set[str]] = None,
    verbose: bool = False,
) -> List[CheckResult]:
    """Run access checks on every endpoint.

    If *endpoints* is ``None``, calls :func:`discover_endpoints` first.

    For DVC remotes (endpoints whose source matches ``"DVC remote '...'"``),
    the check is performed via DVC's own storage API first
    (:func:`_check_dvc_remote`).  This uses whatever credentials DVC has
    configured (service-account JSON, endpoint URLs, SSH keys, etc.) and
    is authoritative.  Per-type CLI checkers are used as a fallback for
    non-DVC endpoints or if the DVC API is unavailable.

    Children of each endpoint are checked recursively (e.g. DVC remotes
    of an import source).

    Args:
        endpoints: Pre-discovered endpoints, or None to discover now.
        type_filter: Passed through to :func:`discover_endpoints`.
        verbose: Show verbose per-subdirectory filesystem detail.

    Returns:
        List of :class:`CheckResult` objects.
    """
    if endpoints is None:
        endpoints = discover_endpoints(type_filter=type_filter, verbose=verbose)

    results: List[CheckResult] = []
    for ep in endpoints:
        result = _try_check(ep, verbose=verbose)
        results.append(result)

        # Check children too
        for child in ep.children:
            results.append(_try_check(child, verbose=verbose))

    return results


def _try_check(ep: Endpoint, verbose: bool = False) -> CheckResult:
    """Check a single endpoint, preferring DVC-native access where possible.

    For endpoints sourced from a DVC remote (source matches
    ``"DVC remote '...'"``), tries :func:`_check_dvc_remote` first.
    Falls back to per-type CLI checkers.
    """
    remote_name = _extract_remote_name(ep.source)

    # For DVC remotes in the current project (not children of import
    # sources), try the DVC-native check first.
    if remote_name and ' of ' not in ep.source:
        dvc_result = _check_dvc_remote(ep, remote_name, verbose=verbose)
        if dvc_result is not None:
            return dvc_result

    # Fall back to per-type checker
    checker = _CHECKERS.get(ep.type)
    if checker:
        if ep.type in ('filesystem', 'ssh'):
            return checker(ep, verbose=verbose)
        return checker(ep)

    return CheckResult(
        endpoint=ep, status=STATUS_SKIP,
        summary=f'no checker for type {ep.type!r}',
    )


# ---------------------------------------------------------------------
# Check output formatting
# ---------------------------------------------------------------------

def format_check_results(results: List[CheckResult]) -> str:
    """Format check results for human-readable terminal output."""
    import click

    _status_icon = {
        STATUS_PASS: click.style('✓', fg='green', bold=True),
        STATUS_FAIL: click.style('✗', fg='red', bold=True),
        STATUS_WARN: click.style('⚠', fg='yellow', bold=True),
        STATUS_SKIP: click.style('–', dim=True),
    }

    _status_colour = {
        STATUS_PASS: 'green',
        STATUS_FAIL: 'red',
        STATUS_WARN: 'yellow',
        STATUS_SKIP: 'white',
    }

    lines: List[str] = []

    for r in results:
        icon = _status_icon.get(r.status, '?')
        colour = _status_colour.get(r.status, 'white')
        url_str = click.style(r.endpoint.url, fg=colour)
        summary_str = click.style(r.summary, dim=(r.status == STATUS_SKIP))
        lines.append(f'  {icon} {url_str}')
        lines.append(f'    {summary_str}')

        for detail in r.details:
            lines.append(click.style(f'      {detail}', dim=True))

        for hint in r.hints:
            hint_str = click.style(f'    Hint: {hint}', fg='yellow')
            lines.append(hint_str)

    # Summary line
    counts = {STATUS_PASS: 0, STATUS_FAIL: 0, STATUS_WARN: 0, STATUS_SKIP: 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    parts: List[str] = []
    if counts[STATUS_PASS]:
        parts.append(click.style(f'{counts[STATUS_PASS]} passed', fg='green'))
    if counts[STATUS_FAIL]:
        parts.append(click.style(f'{counts[STATUS_FAIL]} failed', fg='red'))
    if counts[STATUS_WARN]:
        parts.append(click.style(f'{counts[STATUS_WARN]} warning(s)', fg='yellow'))
    if counts[STATUS_SKIP]:
        parts.append(click.style(f'{counts[STATUS_SKIP]} skipped', dim=True))

    lines.append('')
    lines.append(', '.join(parts) + '.')
    lines.append('')

    return '\n'.join(lines)


def format_check_results_json(results: List[CheckResult]) -> str:
    """Format check results as a JSON string."""
    return json.dumps(
        [r.to_dict() for r in results],
        indent=2,
    )


# =============================================================================
# Access request generation
# =============================================================================

@dataclass
class AccessRequest:
    """An access-request template generated from check failures.

    Attributes:
        user: Username of the requester.
        project: Project name.
        platform_name: Hostname / platform identifier.
        dt_version: Installed dt version string.
        request_date: Date the request was generated.
        items: Failed/warned :class:`CheckResult` objects that need
            attention.
    """

    user: str
    project: str
    platform_name: str
    dt_version: str
    request_date: str
    items: List[CheckResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'user': self.user,
            'project': self.project,
            'platform': self.platform_name,
            'dt_version': self.dt_version,
            'date': self.request_date,
            'items': [r.to_dict() for r in self.items],
        }


def _get_dt_version() -> str:
    """Return the installed dt version string."""
    try:
        from importlib.metadata import version as pkg_version
        return pkg_version('dvc-tools')
    except Exception:
        return 'unknown'


def generate_request(
    type_filter: Optional[Set[str]] = None,
    verbose: bool = False,
    include_warnings: bool = True,
) -> AccessRequest:
    """Run access checks and collect failures into an :class:`AccessRequest`.

    Calls :func:`check_endpoints` internally, then filters to results
    with status ``'fail'`` (and optionally ``'warn'``).

    Args:
        type_filter: Passed through to :func:`check_endpoints`.
        verbose: Passed through to :func:`check_endpoints`.
        include_warnings: Whether to include ``'warn'`` results as well
            as ``'fail'`` results.  Defaults to ``True``.

    Returns:
        An :class:`AccessRequest` populated with metadata and the
        failing / warning items.
    """
    results = check_endpoints(type_filter=type_filter, verbose=verbose)

    target_statuses = {STATUS_FAIL}
    if include_warnings:
        target_statuses.add(STATUS_WARN)

    items = [r for r in results if r.status in target_statuses]

    return AccessRequest(
        user=getpass.getuser(),
        project=utils.get_project_name(),
        platform_name=platform.node(),
        dt_version=_get_dt_version(),
        request_date=date.today().isoformat(),
        items=items,
    )


def format_request_text(req: AccessRequest) -> str:
    """Format an access request as plain text."""
    lines: List[str] = []

    lines.append(f"Access request for user '{req.user}' on project '{req.project}'")
    lines.append('')

    if not req.items:
        lines.append('All endpoints are accessible — no request needed.')
        lines.append('')
        return '\n'.join(lines)

    lines.append('The following resources are not accessible:')
    lines.append('')

    for i, r in enumerate(req.items, 1):
        ep = r.endpoint
        type_label = ep.type.capitalize()
        lines.append(f'  {i}. {type_label}: {ep.url}')
        lines.append(f'     Status: {r.summary}')

        # Determine required access level
        if ep.type == 'filesystem':
            lines.append('     Required: read/write access')
        elif ep.type in ('s3', 'gs'):
            lines.append('     Required: read access (at minimum)')
        elif ep.type in ('ssh', 'git'):
            lines.append('     Required: connection access')
        elif ep.type == 'http':
            lines.append('     Required: HTTP reachability')

        for hint in r.hints:
            lines.append(f'     Suggested fix: {hint}')
        lines.append('')

    lines.append(f'Platform: {req.platform_name}')
    lines.append(f'dt version: {req.dt_version}')
    lines.append(f'Date: {req.request_date}')
    lines.append('')

    return '\n'.join(lines)


def format_request_markdown(req: AccessRequest) -> str:
    """Format an access request as Markdown (for tickets / emails)."""
    lines: List[str] = []

    lines.append(f"# Access request — {req.project}")
    lines.append('')
    lines.append(f'**User:** {req.user}  ')
    lines.append(f'**Platform:** {req.platform_name}  ')
    lines.append(f'**dt version:** {req.dt_version}  ')
    lines.append(f'**Date:** {req.request_date}')
    lines.append('')

    if not req.items:
        lines.append('All endpoints are accessible — no request needed.')
        lines.append('')
        return '\n'.join(lines)

    lines.append('## Resources requiring access')
    lines.append('')

    for i, r in enumerate(req.items, 1):
        ep = r.endpoint
        status_icon = '🔴' if r.status == STATUS_FAIL else '🟡'
        lines.append(f'### {i}. {ep.type} — `{ep.url}`')
        lines.append('')
        lines.append(f'- **Status:** {status_icon} {r.summary}')
        lines.append(f'- **Source:** {ep.source}')

        if ep.type == 'filesystem':
            lines.append('- **Required:** read/write access')
        elif ep.type in ('s3', 'gs'):
            lines.append('- **Required:** read access (at minimum)')
        elif ep.type in ('ssh', 'git'):
            lines.append('- **Required:** connection access')
        elif ep.type == 'http':
            lines.append('- **Required:** HTTP reachability')

        if r.hints:
            lines.append('')
            lines.append('**Suggested fix:**')
            for hint in r.hints:
                lines.append(f'- {hint}')

        lines.append('')

    return '\n'.join(lines)


def format_request_json(req: AccessRequest) -> str:
    """Format an access request as JSON."""
    return json.dumps(req.to_dict(), indent=2)


# =============================================================================
# Sending access requests
# =============================================================================

def _format_slack_blocks(req: AccessRequest) -> dict:
    """Build a Slack message payload from an *AccessRequest*.

    Uses Slack's ``mrkdwn`` formatting via Block Kit so the message
    renders nicely in channels and DMs.
    """
    blocks: List[dict] = [
        {
            'type': 'header',
            'text': {
                'type': 'plain_text',
                'text': f'Access request — {req.project}',
            },
        },
        {
            'type': 'section',
            'fields': [
                {'type': 'mrkdwn', 'text': f'*User:* {req.user}'},
                {'type': 'mrkdwn', 'text': f'*Platform:* {req.platform_name}'},
                {'type': 'mrkdwn', 'text': f'*dt version:* {req.dt_version}'},
                {'type': 'mrkdwn', 'text': f'*Date:* {req.request_date}'},
            ],
        },
        {'type': 'divider'},
    ]

    if not req.items:
        blocks.append({
            'type': 'section',
            'text': {
                'type': 'mrkdwn',
                'text': ':white_check_mark: All endpoints are accessible — no request needed.',
            },
        })
    else:
        for i, r in enumerate(req.items, 1):
            ep = r.endpoint
            icon = ':red_circle:' if r.status == STATUS_FAIL else ':large_yellow_circle:'
            text = f'{icon} *{i}. {ep.type}* — `{ep.url}`\n_{r.summary}_'
            if r.hints:
                text += '\n' + '\n'.join(f'> :bulb: {h}' for h in r.hints)
            blocks.append({
                'type': 'section',
                'text': {'type': 'mrkdwn', 'text': text},
            })

    return {'blocks': blocks}


def send_request_slack(req: AccessRequest, webhook_url: str) -> None:
    """Send an access request to a Slack incoming-webhook URL.

    Uses :mod:`urllib.request` from stdlib — zero extra dependencies.

    Raises:
        AuthError: If the webhook POST fails.
    """
    payload = json.dumps(_format_slack_blocks(req)).encode()
    http_req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={'Content-Type': 'application/json'},
    )

    try:
        with urllib.request.urlopen(http_req, timeout=30) as resp:
            body = resp.read().decode()
            if resp.status != 200 or body != 'ok':
                raise AuthError(
                    f'Slack webhook returned unexpected response: '
                    f'{resp.status} {body}'
                )
    except urllib.error.URLError as exc:
        raise AuthError(f'Failed to send Slack notification: {exc}') from exc


def send_request_email(
    req: AccessRequest,
    admin_email: str,
) -> None:
    """Send an access request via the local ``mail`` command.

    Pipes the text-format request to ``mail -s <subject> <admin_email>``.
    This relies on a working MTA (e.g. ``sendmail``, ``postfix``) which
    is standard on NCI HPC nodes.

    Raises:
        AuthError: If ``mail`` is not found or exits non-zero.
    """
    if not shutil.which('mail'):
        raise AuthError(
            "The 'mail' command is not available on this system.\n"
            "Try 'dt auth request --send slack' or copy the output "
            "manually."
        )

    subject = f'dt access request — {req.project} ({req.user})'
    body = format_request_text(req)

    result = subprocess.run(
        ['mail', '-s', subject, admin_email],
        input=body,
        capture_output=True,
        text=True,
        timeout=30,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise AuthError(f'mail command failed (exit {result.returncode}): {stderr}')


def send_request(
    req: AccessRequest,
    method: Optional[str] = None,
) -> str:
    """Send an access request using the configured delivery method.

    Args:
        req: The access request to send.
        method: ``'slack'``, ``'email'``, or ``None`` for auto-detect.
            Auto-detect tries Slack first, then email, based on which
            config values are set.

    Returns:
        A human-friendly message describing where the request was sent.

    Raises:
        AuthError: If no delivery method is configured or sending fails.
    """
    slack_url = None
    admin_email = None

    try:
        slack_url = cfg.get_value('auth.slack_webhook')
    except Exception:
        pass

    try:
        admin_email = cfg.get_value('auth.admin_email')
    except Exception:
        pass

    if method == 'slack':
        if not slack_url:
            raise AuthError(
                "Slack webhook not configured.\n"
                "Set it with: dt config set --system auth.slack_webhook "
                "'https://hooks.slack.com/services/...'"
            )
        send_request_slack(req, slack_url)
        return 'Access request sent to Slack.'

    if method == 'email':
        if not admin_email:
            raise AuthError(
                "Admin email not configured.\n"
                "Set it with: dt config set --system auth.admin_email "
                "'admin@example.com'"
            )
        send_request_email(req, admin_email)
        return f'Access request emailed to {admin_email}.'

    # Auto-detect: prefer Slack, fall back to email
    if slack_url:
        send_request_slack(req, slack_url)
        return 'Access request sent to Slack.'

    if admin_email:
        send_request_email(req, admin_email)
        return f'Access request emailed to {admin_email}.'

    raise AuthError(
        "No delivery method configured.\n"
        "Set one of:\n"
        "  dt config set --system auth.slack_webhook "
        "'https://hooks.slack.com/services/...'\n"
        "  dt config set --system auth.admin_email 'admin@example.com'"
    )


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
