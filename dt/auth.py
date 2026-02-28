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
from . import tmp as tmp_mod
from . import utils
from .errors import AuthError


def resolve_repo_url(repo: str) -> str:
    """Resolve a repository short name or URL to a full git URL.

    Supports short names (e.g. ``neochemo``) when the ``owner`` config
    key is set, as well as full SSH/HTTPS URLs.

    Args:
        repo: Repository URL or short name.

    Returns:
        Full git URL.

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
    endpoints: List[Endpoint] = []

    # Use shared helper to collect import URLs
    import_urls = _get_import_urls(repo_path)

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


def discover_endpoints_from_repo(
    repo_url: str,
    type_filter: Optional[Set[str]] = None,
    verbose: bool = False,
) -> List[Endpoint]:
    """Discover endpoints for a remote repository via a temporary clone.

    Creates a shallow clone of *repo_url* in a temporary directory,
    runs :func:`discover_endpoints` inside it, then cleans up.

    The clone is just deep enough to read ``.dvc/config``, ``*.dvc``
    files, and git remotes — no data is fetched.

    Args:
        repo_url: Git clone URL, or short name (resolved via ``owner`` config).
        type_filter: Passed through to :func:`discover_endpoints`.
        verbose: Print progress information.

    Returns:
        Deduplicated list of :class:`Endpoint` objects.
    """
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

        # Run discovery inside the cloned repo
        endpoints = discover_endpoints(
            repo_path=clone_path,
            type_filter=type_filter,
            verbose=verbose,
        )
        return endpoints
    finally:
        # Clean up the temp clone
        shutil.rmtree(tmpdir, ignore_errors=True)


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


def _get_owner_info(path: Path) -> Tuple[str, str]:
    """Return ``(owner_name, group_name)`` for *path*.

    Falls back to numeric uid/gid strings if names cannot be resolved.
    """
    import grp
    import pwd

    try:
        st = path.stat()
    except (PermissionError, FileNotFoundError, OSError):
        return ('?', '?')

    try:
        owner = pwd.getpwuid(st.st_uid).pw_name
    except KeyError:
        owner = str(st.st_uid)

    try:
        group = grp.getgrgid(st.st_gid).gr_name
    except KeyError:
        group = str(st.st_gid)

    return owner, group


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
            owner, group = _get_owner_info(d)
            perms = []
            if not d_read:
                perms.append('not readable')
            if not d_write:
                perms.append('not writable')
            detail_lines.append(
                f'{d.name}  {", ".join(perms)}  (owner: {owner}, group: {group})'
            )

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
        # Collect unique owners of failed dirs for actionable hints
        owners = set()
        for d in failed_dirs:
            owner, _ = _get_owner_info(d)
            if owner != '?':
                owners.add(owner)
        for d in failed_dirs[:3]:  # limit hints
            hints.append(
                f'Fix permissions: setfacl -R -m u:{getpass.getuser()}:rwx {d}'
            )
        if owners:
            owner_list = ', '.join(sorted(owners))
            hints.append(f'Ask {owner_list} to run the setfacl command(s) above')
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


# ---------------------------------------------------------------------
# Per-user checkers (for --user flag)
# ---------------------------------------------------------------------

def _get_user_info(username: str) -> Optional[Tuple[int, int, List[int]]]:
    """Get uid, primary gid, and supplementary gids for *username*.

    Returns ``None`` if the user does not exist on this system.
    """
    import grp
    import pwd
    try:
        pw = pwd.getpwnam(username)
    except KeyError:
        return None

    uid = pw.pw_uid
    gid = pw.pw_gid
    groups = [g.gr_gid for g in grp.getgrall() if username in g.gr_mem]
    if gid not in groups:
        groups.append(gid)
    return uid, gid, groups


def _stat_check_user(
    path: Path,
    uid: int,
    gids: List[int],
    need_write: bool = True,
) -> Tuple[bool, bool]:
    """Simulate permission check for a given uid/gids.

    Returns ``(readable, writable)`` booleans.  Uses the file's
    mode bits and owner/group from :func:`os.stat`, plus ACLs via
    ``getfacl`` if available.
    """
    try:
        st = path.stat()
    except (PermissionError, FileNotFoundError):
        return False, False

    mode = st.st_mode
    file_uid = st.st_uid
    file_gid = st.st_gid

    if uid == file_uid:
        readable = bool(mode & 0o400)
        writable = bool(mode & 0o200)
    elif file_gid in gids:
        readable = bool(mode & 0o040)
        writable = bool(mode & 0o020)
    else:
        readable = bool(mode & 0o004)
        writable = bool(mode & 0o002)

    # Try ACLs as a supplementary check (may grant extra access)
    if not (readable and writable):
        acl_r, acl_w = _check_acl_for_user(path, uid, gids)
        readable = readable or acl_r
        writable = writable or acl_w

    return readable, writable


def _check_acl_for_user(
    path: Path,
    uid: int,
    gids: List[int],
) -> Tuple[bool, bool]:
    """Check POSIX ACLs for a user via ``getfacl``.

    Returns ``(readable, writable)`` — both False if getfacl is
    unavailable or errors out.
    """
    import grp
    import pwd

    try:
        username = pwd.getpwuid(uid).pw_name
    except KeyError:
        return False, False

    try:
        result = subprocess.run(
            ['getfacl', '-p', str(path)],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0:
            return False, False
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False, False

    readable = False
    writable = False

    for line in result.stdout.splitlines():
        line = line.strip()
        # user:<name>:rwx
        if line.startswith(f'user:{username}:'):
            perms = line.split(':')[2]
            if 'r' in perms:
                readable = True
            if 'w' in perms:
                writable = True
        # group:<name>:rwx — check all groups
        elif line.startswith('group:'):
            parts = line.split(':')
            if len(parts) >= 3:
                gname = parts[1]
                try:
                    gr = grp.getgrnam(gname)
                    if gr.gr_gid in gids:
                        perms = parts[2]
                        if 'r' in perms:
                            readable = True
                        if 'w' in perms:
                            writable = True
                except KeyError:
                    pass

    return readable, writable


def _check_filesystem_for_user(
    ep: Endpoint,
    username: str,
    verbose: bool = False,
) -> CheckResult:
    """Check filesystem access from *username*'s perspective.

    Simulates permission checks using ``stat()`` + ``getfacl``
    without needing ``sudo``.
    """
    user_info = _get_user_info(username)
    if user_info is None:
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary=f"user '{username}' not found on this system",
        )

    uid, _, gids = user_info
    path = Path(ep.url)

    if not path.exists():
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='path does not exist',
        )

    readable, writable = _stat_check_user(path, uid, gids)

    if not readable:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary=f'not readable by {username}',
            hints=[f'Grant access: setfacl -R -m u:{username}:rwx {ep.url}'],
        )

    if not writable:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary=f'read-only for {username}',
            hints=[f'Grant write: setfacl -R -m u:{username}:rwx {ep.url}'],
        )

    # Check subdirectories
    try:
        subdirs = sorted([d for d in path.iterdir() if d.is_dir()])
    except PermissionError:
        return CheckResult(
            endpoint=ep, status=STATUS_PASS,
            summary=f'root accessible by {username} (cannot enumerate subdirs)',
        )

    if not subdirs:
        return CheckResult(
            endpoint=ep, status=STATUS_PASS,
            summary=f'read/write for {username}',
        )

    fail_count = 0
    detail_lines: List[str] = []
    failed_dirs: List[Path] = []

    for d in subdirs:
        d_r, d_w = _stat_check_user(d, uid, gids)
        if d_r and d_w:
            if verbose:
                detail_lines.append(f'{d.name}  r/w')
        else:
            fail_count += 1
            failed_dirs.append(d)
            perms = []
            if not d_r:
                perms.append('not readable')
            if not d_w:
                perms.append('not writable')
            detail_lines.append(f'{d.name}  {", ".join(perms)}')

    total = len(subdirs)
    if fail_count == 0:
        return CheckResult(
            endpoint=ep, status=STATUS_PASS,
            summary=f'read/write for {username} ({total}/{total} subdirs OK)',
            details=detail_lines if verbose else [],
        )

    hints = [f'setfacl -R -m u:{username}:rwx {d}' for d in failed_dirs[:3]]
    return CheckResult(
        endpoint=ep, status=STATUS_FAIL,
        summary=f'{fail_count} of {total} subdirs not accessible by {username}',
        details=detail_lines,
        hints=hints,
    )


def _check_github_for_user(
    ep: Endpoint,
    username: str,
) -> CheckResult:
    """Check GitHub repository access for a specific user.

    Uses ``gh api`` to check:
    1. Whether the user is a collaborator on the repo.
    2. Their permission level.

    Requires the caller to have admin/maintain access to the repo
    (or org membership).
    """
    # Extract owner/repo from git URL
    url = ep.url
    repo_path = None
    for prefix in ('git@github.com:', 'https://github.com/'):
        if url.startswith(prefix):
            repo_path = url[len(prefix):]
            break

    if not repo_path:
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary='not a GitHub URL, cannot check per-user access',
        )

    repo_path = repo_path.rstrip('/').removesuffix('.git')

    try:
        result = subprocess.run(
            ['gh', 'api', f'repos/{repo_path}/collaborators/{username}/permission',
             '-q', '.permission'],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            perm = result.stdout.strip()
            if perm in ('admin', 'maintain', 'write', 'read'):
                return CheckResult(
                    endpoint=ep, status=STATUS_PASS,
                    summary=f'{username} has {perm} access',
                )
            else:
                return CheckResult(
                    endpoint=ep, status=STATUS_FAIL,
                    summary=f'{username} has {perm} access (insufficient)',
                    hints=[f'Invite {username} to {repo_path} or add to a team with access'],
                )
        elif result.returncode != 0:
            stderr = result.stderr.strip()
            if '404' in stderr or 'Not Found' in stderr:
                return CheckResult(
                    endpoint=ep, status=STATUS_FAIL,
                    summary=f'{username} is not a collaborator',
                    hints=[f'Invite {username} to {repo_path} or add to a team with access'],
                )
            return CheckResult(
                endpoint=ep, status=STATUS_SKIP,
                summary=f'cannot check: {stderr[:80]}',
            )
    except FileNotFoundError:
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary='gh CLI not available',
        )
    except subprocess.TimeoutExpired:
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary='gh API timed out',
        )

    return CheckResult(
        endpoint=ep, status=STATUS_SKIP,
        summary='could not determine access',
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
# GitHub team management
# ---------------------------------------------------------------------

def _parse_github_owner_repo(url: str) -> Optional[Tuple[str, str]]:
    """Extract ``(owner, repo)`` from a GitHub URL.

    Handles ``git@github.com:owner/repo.git``,
    ``https://github.com/owner/repo``, etc.

    Returns ``None`` if the URL is not a GitHub URL.
    """
    import re

    url = url.strip().rstrip('/')

    # SSH: git@github.com:owner/repo.git
    m = re.match(r'git@github\.com:([^/]+)/([^/]+?)(?:\.git)?$', url)
    if m:
        return m.group(1), m.group(2)

    # HTTPS: https://github.com/owner/repo
    m = re.match(
        r'https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?$', url,
    )
    if m:
        return m.group(1), m.group(2)

    return None


@dataclass
class TeamInfo:
    """A GitHub team with its repo permission level."""
    org: str
    slug: str
    name: str
    permission: str  # pull, push, admin, maintain, triage


def list_repo_teams(repo_url: str) -> List[TeamInfo]:
    """List GitHub teams that have access to a repository.

    Args:
        repo_url: GitHub repository URL or short name.

    Returns:
        List of :class:`TeamInfo` objects.

    Raises:
        AuthError: If the URL is not a GitHub URL or ``gh`` fails.
    """
    repo_url = resolve_repo_url(repo_url)
    parsed = _parse_github_owner_repo(repo_url)
    if parsed is None:
        raise AuthError(f'Not a GitHub URL: {repo_url}')

    owner, repo = parsed

    try:
        result = subprocess.run(
            ['gh', 'api', f'repos/{owner}/{repo}/teams', '--paginate',
             '-q', '.[] | [.slug, .name, .permission] | @tsv'],
            capture_output=True, text=True, timeout=15,
        )
    except FileNotFoundError:
        raise AuthError('gh CLI is not installed')

    if result.returncode != 0:
        raise AuthError(
            f'Failed to list teams for {owner}/{repo}: '
            f'{result.stderr.strip()}'
        )

    teams: List[TeamInfo] = []
    for line in result.stdout.strip().splitlines():
        parts = line.split('\t')
        if len(parts) >= 3:
            teams.append(TeamInfo(
                org=owner, slug=parts[0], name=parts[1],
                permission=parts[2],
            ))

    return teams


def list_user_teams(
    username: str,
    org: Optional[str] = None,
) -> List[TeamInfo]:
    """List GitHub teams that *username* belongs to.

    If *org* is given, only returns teams in that organisation.

    Args:
        username: GitHub username.
        org: Optional organisation filter.

    Returns:
        List of :class:`TeamInfo` objects.

    Raises:
        AuthError: If ``gh`` fails.
    """
    # We need to search org teams for membership.
    # gh api doesn't have a direct "list teams for a user" endpoint
    # for other users, so we list org teams and check membership.
    if org is None:
        raise AuthError(
            'An organisation name is required to list teams '
            '(pass --org or use a repo URL to infer it)'
        )

    try:
        result = subprocess.run(
            ['gh', 'api', f'orgs/{org}/teams', '--paginate',
             '-q', '.[] | [.slug, .name] | @tsv'],
            capture_output=True, text=True, timeout=15,
        )
    except FileNotFoundError:
        raise AuthError('gh CLI is not installed')

    if result.returncode != 0:
        raise AuthError(
            f'Failed to list teams for org {org}: '
            f'{result.stderr.strip()}'
        )

    user_teams: List[TeamInfo] = []
    for line in result.stdout.strip().splitlines():
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        slug, name = parts[0], parts[1]

        # Check if user is a member of this team
        mem_result = subprocess.run(
            ['gh', 'api', f'orgs/{org}/teams/{slug}/memberships/{username}',
             '-q', '.state'],
            capture_output=True, text=True, timeout=10,
        )
        if mem_result.returncode == 0 and mem_result.stdout.strip() == 'active':
            user_teams.append(TeamInfo(
                org=org, slug=slug, name=name, permission='member',
            ))

    return user_teams


def add_team_to_repo(
    repo_url: str,
    team_slug: str,
    permission: str = 'push',
) -> str:
    """Add a GitHub team to a repository.

    Args:
        repo_url: GitHub repository URL or short name.
        team_slug: Team slug (e.g. ``data-team``).
        permission: Permission level: ``pull``, ``push``, ``admin``,
            ``maintain``, or ``triage``.

    Returns:
        Confirmation message.

    Raises:
        AuthError: If the operation fails.
    """
    repo_url = resolve_repo_url(repo_url)
    parsed = _parse_github_owner_repo(repo_url)
    if parsed is None:
        raise AuthError(f'Not a GitHub URL: {repo_url}')

    owner, repo = parsed

    try:
        result = subprocess.run(
            ['gh', 'api', f'orgs/{owner}/teams/{team_slug}/repos/{owner}/{repo}',
             '-X', 'PUT', '-f', f'permission={permission}'],
            capture_output=True, text=True, timeout=15,
        )
    except FileNotFoundError:
        raise AuthError('gh CLI is not installed')

    if result.returncode != 0:
        raise AuthError(
            f'Failed to add {team_slug} to {owner}/{repo}: '
            f'{result.stderr.strip()}'
        )

    return f"Team '{team_slug}' granted '{permission}' access to {owner}/{repo}"


def add_user_to_team(
    org: str,
    team_slug: str,
    username: str,
) -> str:
    """Add a GitHub user to a team.

    Args:
        org: Organisation name.
        team_slug: Team slug.
        username: GitHub username to add.

    Returns:
        Confirmation message.

    Raises:
        AuthError: If the operation fails.
    """
    try:
        result = subprocess.run(
            ['gh', 'api', f'orgs/{org}/teams/{team_slug}/memberships/{username}',
             '-X', 'PUT'],
            capture_output=True, text=True, timeout=15,
        )
    except FileNotFoundError:
        raise AuthError('gh CLI is not installed')

    if result.returncode != 0:
        raise AuthError(
            f"Failed to add '{username}' to {org}/{team_slug}: "
            f'{result.stderr.strip()}'
        )

    return f"User '{username}' added to team '{org}/{team_slug}'"


def format_teams(teams: List[TeamInfo], header: str = 'Teams') -> str:
    """Format a list of teams for terminal output."""
    import click

    if not teams:
        return f'No teams found.'

    lines = [click.style(f'\n{header}', bold=True)]
    for t in teams:
        perm_colour = {
            'admin': 'red', 'maintain': 'yellow', 'push': 'green',
            'pull': 'cyan', 'triage': 'magenta', 'member': 'white',
        }.get(t.permission, 'white')
        perm_label = click.style(t.permission, fg=perm_colour)
        lines.append(f'  {t.org}/{t.slug}  ({t.name})  {perm_label}')

    return '\n'.join(lines)


def format_teams_json(teams: List[TeamInfo]) -> str:
    """Format teams as JSON."""
    return json.dumps(
        [{'org': t.org, 'slug': t.slug, 'name': t.name,
          'permission': t.permission} for t in teams],
        indent=2,
    )


# ---------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------

def check_endpoints(
    endpoints: Optional[List[Endpoint]] = None,
    type_filter: Optional[Set[str]] = None,
    verbose: bool = False,
    user: Optional[str] = None,
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
        user: If set, check access from this user's perspective
            (filesystem via stat/ACL, git via GitHub API).

    Returns:
        List of :class:`CheckResult` objects.
    """
    if endpoints is None:
        endpoints = discover_endpoints(type_filter=type_filter, verbose=verbose)

    results: List[CheckResult] = []
    for ep in endpoints:
        result = _try_check(ep, verbose=verbose, user=user)
        results.append(result)

        # Check children too
        for child in ep.children:
            results.append(_try_check(child, verbose=verbose, user=user))

    return results


def _try_check(ep: Endpoint, verbose: bool = False,
               user: Optional[str] = None) -> CheckResult:
    """Check a single endpoint, preferring DVC-native access where possible.

    For endpoints sourced from a DVC remote (source matches
    ``"DVC remote '...'"``), tries :func:`_check_dvc_remote` first.
    Falls back to per-type CLI checkers.

    If *user* is set, uses per-user checkers for filesystem and git
    endpoints (simulating access from another user's perspective).
    """
    # --user mode: use per-user checkers where available
    if user:
        if ep.type == 'filesystem':
            return _check_filesystem_for_user(ep, user, verbose=verbose)
        if ep.type == 'git':
            return _check_github_for_user(ep, user)
        if ep.type == 'ssh' and ep.local_path:
            # SSH with local path — check the local filesystem
            local_ep = Endpoint(
                type='filesystem', url=ep.local_path,
                source=ep.source, local_path=ep.local_path,
            )
            return _check_filesystem_for_user(local_ep, user, verbose=verbose)
        # For s3/gs/http/ssh-remote: can't check per-user
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary=f'cannot check {ep.type} access for another user',
        )

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
        identities: User identities across systems.
        items: Failed/warned :class:`CheckResult` objects that need
            attention.
    """

    user: str
    project: str
    platform_name: str
    dt_version: str
    request_date: str
    identities: List['Identity'] = field(default_factory=list)
    items: List[CheckResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'user': self.user,
            'project': self.project,
            'platform': self.platform_name,
            'dt_version': self.dt_version,
            'date': self.request_date,
            'identities': [i.to_dict() for i in self.identities],
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
    endpoints: Optional[List[Endpoint]] = None,
) -> AccessRequest:
    """Run access checks and collect failures into an :class:`AccessRequest`.

    Calls :func:`check_endpoints` internally, then filters to results
    with status ``'fail'`` (and optionally ``'warn'``).

    Args:
        type_filter: Passed through to :func:`check_endpoints`.
        verbose: Passed through to :func:`check_endpoints`.
        include_warnings: Whether to include ``'warn'`` results as well
            as ``'fail'`` results.  Defaults to ``True``.
        endpoints: Pre-discovered endpoints (e.g. from ``--repo``).
            If ``None``, discovery runs from the current project.

    Returns:
        An :class:`AccessRequest` populated with metadata and the
        failing / warning items.
    """
    results = check_endpoints(
        endpoints=endpoints,
        type_filter=type_filter,
        verbose=verbose,
    )

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
        identities=get_identities(),
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

    if req.identities:
        lines.append('')
        lines.append('Identities:')
        for ident in req.identities:
            lines.append(f'  {ident.system}: {ident.value}')

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

    if req.identities:
        lines.append('**Identities:**')
        lines.append('')
        for ident in req.identities:
            lines.append(f'- **{ident.system}:** {ident.value}')
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
    ]

    # Add identities (skip NCI username — already shown as User)
    id_lines = [f'*{i.system}:* {i.value}'
                for i in req.identities if i.system != 'NCI username']
    if id_lines:
        blocks.append({
            'type': 'section',
            'text': {
                'type': 'mrkdwn',
                'text': '\n'.join(id_lines),
            },
        })

    blocks.append({'type': 'divider'})

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
    """Send an access request via the local mail system.

    Tries ``sendmail`` first (non-interactive, reliable for scripted use),
    then falls back to ``mail -s``.  On NCI, ``sendmail`` is the standard
    MTA interface and avoids the stdin-blocking issues that ``mail``/
    ``mailx`` can have with piped input.

    Raises:
        AuthError: If neither ``sendmail`` nor ``mail`` is found, or
            the command exits non-zero.
    """
    import getpass

    subject = f'dt access request — {req.project} ({req.user})'
    body = format_request_text(req)
    sender = getpass.getuser()

    sendmail = shutil.which('sendmail')
    if sendmail:
        # Build a minimal RFC 2822 message so sendmail knows the
        # recipient, subject, and From header without flags.
        message = (
            f'To: {admin_email}\n'
            f'From: {sender}\n'
            f'Subject: {subject}\n'
            f'\n'
            f'{body}'
        )
        result = subprocess.run(
            [sendmail, '-t'],
            input=message,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise AuthError(
                f'sendmail failed (exit {result.returncode}): {stderr}'
            )
        return

    # Fallback: mail command
    if not shutil.which('mail'):
        raise AuthError(
            "Neither 'sendmail' nor 'mail' is available on this system.\n"
            "Try 'dt auth request --send slack' or copy the output "
            "manually."
        )

    result = subprocess.run(
        ['mail', '-s', subject, admin_email],
        input=body,
        capture_output=True,
        text=True,
        timeout=60,
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


# =============================================================================
# Identity / whoami
# =============================================================================

#: Config keys for stored identities and their display labels.
_IDENTITY_KEYS: List[Tuple[str, str]] = [
    ('auth.github_user', 'GitHub user'),
    ('auth.github_teams', 'GitHub teams'),
    ('auth.gcp_email', 'GCP email'),
    ('auth.aws_identity', 'AWS identity'),
]


@dataclass
class Identity:
    """A single identity on a particular system.

    Attributes:
        system: Short label (e.g. ``'NCI username'``, ``'GitHub user'``).
        value: The identity value (username, email, ARN, etc.).
        source: How it was obtained — ``'detected'``, ``'config'``, or
            ``'detected via <tool>'``.
    """

    system: str
    value: str
    source: str = 'config'

    def to_dict(self) -> dict:
        return {
            'system': self.system,
            'value': self.value,
            'source': self.source,
        }


def get_identities() -> List[Identity]:
    """Read stored identities from config + local username.

    Always includes the local (NCI / HPC) username via
    :func:`getpass.getuser`.  Other identities come from dt config
    keys under ``auth.*``.

    Returns:
        List of :class:`Identity` objects.
    """
    ids: List[Identity] = [
        Identity(
            system='NCI username',
            value=getpass.getuser(),
            source='detected',
        ),
    ]

    for key, label in _IDENTITY_KEYS:
        try:
            val = cfg.get_value(key)
            if val:
                ids.append(Identity(system=label, value=str(val), source='config'))
        except Exception:
            pass

    return ids


def _detect_github_user() -> Optional[str]:
    """Detect GitHub username via ``gh api``."""
    try:
        result = subprocess.run(
            ['gh', 'api', 'user', '-q', '.login'],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _detect_github_teams() -> Optional[str]:
    """Detect GitHub team slugs via ``gh api``."""
    try:
        result = subprocess.run(
            ['gh', 'api', 'user/teams', '-q', '.[].slug'],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            teams = ', '.join(result.stdout.strip().splitlines())
            return teams
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _detect_gcp_email() -> Optional[str]:
    """Detect active GCP account via ``gcloud``."""
    try:
        result = subprocess.run(
            ['gcloud', 'auth', 'list',
             '--filter=status:ACTIVE', "--format=value(account)"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip().splitlines()[0]
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _detect_aws_identity() -> Optional[str]:
    """Detect AWS caller identity via ``aws sts``."""
    try:
        result = subprocess.run(
            ['aws', 'sts', 'get-caller-identity', '--query', 'Arn',
             '--output', 'text'],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


#: Mapping from config key to (display label, detector function).
_DETECTORS: Dict[str, Tuple[str, str]] = {
    'auth.github_user': ('GitHub user', 'gh api'),
    'auth.github_teams': ('GitHub teams', 'gh api'),
    'auth.gcp_email': ('GCP email', 'gcloud'),
    'auth.aws_identity': ('AWS identity', 'aws sts'),
}

_DETECT_FNS: Dict[str, object] = {
    'auth.github_user': _detect_github_user,
    'auth.github_teams': _detect_github_teams,
    'auth.gcp_email': _detect_gcp_email,
    'auth.aws_identity': _detect_aws_identity,
}


def detect_identities() -> List[Identity]:
    """Auto-detect identities by probing external tools.

    Runs CLI commands (``gh``, ``gcloud``, ``aws``) to discover what
    accounts are currently active.  Falls back gracefully when tools
    are not installed.

    Returns:
        List of :class:`Identity` objects with ``source`` set to
        ``'detected via <tool>'``.
    """
    ids: List[Identity] = [
        Identity(
            system='NCI username',
            value=getpass.getuser(),
            source='detected',
        ),
    ]

    for key, (label, tool) in _DETECTORS.items():
        fn = _DETECT_FNS[key]
        val = fn()
        if val:
            ids.append(Identity(
                system=label,
                value=val,
                source=f'detected via {tool}',
            ))

    return ids


def compare_identities(
    stored: List[Identity],
    detected: List[Identity],
) -> List[Tuple[Identity, Optional[Identity], str]]:
    """Compare stored and detected identities.

    Returns a list of ``(best_identity, other_identity, note)`` tuples
    where *note* is one of:

    - ``'match'`` — stored and detected agree
    - ``'config only'`` — stored but not detected (tool missing?)
    - ``'detected only'`` — detected but not in config
    - ``'mismatch'`` — both exist but values differ

    The first element is always the "best" identity to display.
    """
    stored_by_sys = {i.system: i for i in stored}
    detected_by_sys = {i.system: i for i in detected}

    all_systems: List[str] = []
    seen: Set[str] = set()
    for i in stored + detected:
        if i.system not in seen:
            all_systems.append(i.system)
            seen.add(i.system)

    results: List[Tuple[Identity, Optional[Identity], str]] = []

    for sys in all_systems:
        s = stored_by_sys.get(sys)
        d = detected_by_sys.get(sys)

        if s and d:
            if s.value == d.value:
                results.append((s, d, 'match'))
            else:
                results.append((d, s, 'mismatch'))
        elif s:
            results.append((s, None, 'config only'))
        elif d:
            results.append((d, None, 'detected only'))

    return results


def format_identities(identities: List[Identity]) -> str:
    """Format identities for terminal output."""
    import click

    if not identities:
        return '  No identities found.\n'

    lines: List[str] = []

    # Find the longest system label for alignment
    max_label = max(len(i.system) for i in identities)

    for i in identities:
        label = click.style(f'{i.system}:', bold=True).ljust(max_label + 15)
        value = click.style(i.value, fg='cyan')
        source = click.style(f'({i.source})', dim=True)
        lines.append(f'  {label} {value}  {source}')

    return '\n'.join(lines) + '\n'


def format_identities_json(identities: List[Identity]) -> str:
    """Format identities as JSON."""
    return json.dumps([i.to_dict() for i in identities], indent=2)


def format_whoami_comparison(
    comparisons: List[Tuple[Identity, Optional[Identity], str]],
) -> str:
    """Format the comparison output from ``whoami --detect``."""
    import click

    lines: List[str] = []
    max_label = max(len(c[0].system) for c in comparisons) if comparisons else 0

    _note_style = {
        'match': ('✓', 'green'),
        'config only': ('?', 'yellow'),
        'detected only': ('●', 'cyan'),
        'mismatch': ('✗', 'red'),
    }

    for best, other, note in comparisons:
        icon, colour = _note_style.get(note, ('?', 'white'))
        icon_str = click.style(icon, fg=colour, bold=True)
        label = click.style(f'{best.system}:', bold=True).ljust(max_label + 15)
        value = click.style(best.value, fg='cyan')
        source = click.style(f'({best.source})', dim=True)
        line = f'  {icon_str} {label} {value}  {source}'

        if note == 'mismatch' and other:
            line += '\n' + click.style(
                f'      config has: {other.value}',
                fg='yellow',
            )
        elif note == 'match':
            line += '  ' + click.style('matches config', fg='green', dim=True)
        elif note == 'config only':
            line += '  ' + click.style('not detected (tool missing?)', fg='yellow', dim=True)
        elif note == 'detected only':
            line += '  ' + click.style('not in config', dim=True)

        lines.append(line)

    return '\n'.join(lines) + '\n'


def save_detected_identities(detected: List[Identity]) -> int:
    """Save detected identities to user-scope config.

    Skips the NCI username (always auto-detected) and identities that
    already match config.

    Returns:
        Number of values saved.
    """
    # Map display labels back to config keys
    label_to_key = {label: key for key, label in _IDENTITY_KEYS}

    saved = 0
    for ident in detected:
        key = label_to_key.get(ident.system)
        if not key:
            continue  # NCI username — skip

        # Check if config already has this value
        try:
            existing = cfg.get_value(key)
            if str(existing) == ident.value:
                continue  # Already saved
        except Exception:
            pass

        cfg.set_value(key, ident.value, scope='user')
        saved += 1

    return saved


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


# =============================================================================
# Credentials management
# =============================================================================

def _get_secret_backend():
    """Get the configured secret backend.
    
    Reads configuration from .dt/config.yaml to determine which
    secret manager to use and how to connect to it.
    
    Returns:
        A SecretBackend instance.
        
    Raises:
        AuthError: If no backend is configured or configuration is invalid.
    """
    from .secrets import GCPSecretBackend, SecretError
    
    backend_type = cfg.get_value('secrets.backend')
    if not backend_type:
        raise AuthError(
            "No secret backend configured.\n"
            "Add to .dt/config.yaml:\n"
            "  secrets:\n"
            "    backend: gcp\n"
            "    gcp:\n"
            "      project: <your-gcp-project>"
        )
    
    if backend_type == 'gcp':
        project = cfg.get_value('secrets.gcp.project')
        if not project:
            raise AuthError(
                "GCP project not configured for secrets.\n"
                "Add to .dt/config.yaml:\n"
                "  secrets:\n"
                "    gcp:\n"
                "      project: <your-gcp-project>"
            )
        prefix = cfg.get_value('secrets.prefix') or 'dvc-remote-'
        return GCPSecretBackend(project=project, prefix=prefix)
    else:
        raise AuthError(
            f"Unknown secret backend: {backend_type}\n"
            f"Supported backends: gcp"
        )


def _get_dvc_config_local_path() -> Path:
    """Get path to .dvc/config.local."""
    return Path.cwd() / '.dvc' / 'config.local'


def _get_dvc_global_config_path() -> Path:
    """Get path to DVC global config using platformdirs.
    
    Returns the platform-appropriate path:
    - macOS: ~/Library/Application Support/dvc/config
    - Linux: ~/.config/dvc/config (or $XDG_CONFIG_HOME/dvc/config)
    - Windows: %APPDATA%/dvc/config
    """
    try:
        from platformdirs import user_config_dir
        return Path(user_config_dir('dvc')) / 'config'
    except ImportError:
        # Fallback if platformdirs not available
        import sys
        if sys.platform == 'darwin':
            return Path.home() / 'Library' / 'Application Support' / 'dvc' / 'config'
        elif sys.platform == 'win32':
            appdata = os.environ.get('APPDATA', '')
            return Path(appdata) / 'dvc' / 'config' if appdata else Path.home() / 'dvc' / 'config'
        else:
            xdg_config = os.environ.get('XDG_CONFIG_HOME', '')
            if xdg_config:
                return Path(xdg_config) / 'dvc' / 'config'
            return Path.home() / '.config' / 'dvc' / 'config'


def _ensure_config_local_permissions(path: Path) -> None:
    """Ensure config file has 600 permissions.
    
    Args:
        path: Path to config file.
    """
    if path.exists():
        current_mode = path.stat().st_mode & 0o777
        if current_mode != 0o600:
            path.chmod(0o600)


def _parse_dvc_ini(content: str) -> Dict[str, Dict[str, str]]:
    """Parse DVC INI config into sections.
    
    Handles DVC's special section syntax like ['remote "name"'].
    
    Args:
        content: INI file content.
        
    Returns:
        Dict mapping section names to key-value dicts.
        Section names are normalized (e.g., 'remote "bcarc-wts"').
    """
    import re
    
    sections: Dict[str, Dict[str, str]] = {}
    current_section = None
    
    for line in content.split('\n'):
        line = line.rstrip()
        
        # Skip empty lines and comments
        if not line or line.startswith('#') or line.startswith(';'):
            continue
        
        # Check for section header
        # Matches [section] or ['remote "name"']
        section_match = re.match(r"^\[(.+)\]$", line)
        if section_match:
            current_section = section_match.group(1)
            if current_section not in sections:
                sections[current_section] = {}
            continue
        
        # Parse key-value pairs
        if current_section and '=' in line:
            # Handle indented lines
            line = line.lstrip()
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip()
            sections[current_section][key] = value
    
    return sections


def _format_dvc_ini(sections: Dict[str, Dict[str, str]]) -> str:
    """Format sections back to DVC INI format.
    
    Args:
        sections: Dict mapping section names to key-value dicts.
        
    Returns:
        INI formatted string.
    """
    lines = []
    
    for section_name, values in sections.items():
        lines.append(f'[{section_name}]')
        for key, value in values.items():
            lines.append(f'    {key} = {value}')
        lines.append('')  # Empty line between sections
    
    return '\n'.join(lines)


def _merge_ini_sections(
    existing: Dict[str, Dict[str, str]],
    new: Dict[str, Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    """Merge new INI sections into existing, replacing duplicates.
    
    Args:
        existing: Existing sections dict.
        new: New sections to merge in.
        
    Returns:
        Merged sections dict. Values from 'new' override 'existing'.
    """
    result = {k: dict(v) for k, v in existing.items()}
    
    for section_name, values in new.items():
        if section_name in result:
            # Update existing section with new values
            result[section_name].update(values)
        else:
            # Add new section
            result[section_name] = dict(values)
    
    return result


def _extract_repo_name_from_url(url: str) -> Optional[str]:
    """Extract repository name from a git URL.
    
    Handles:
    - git@github.com:org/repo.git
    - https://github.com/org/repo.git
    - https://github.com/org/repo
    
    Args:
        url: Git URL.
        
    Returns:
        Repository name (without .git suffix) or None.
    """
    import re
    
    # SSH format: git@host:org/repo.git
    ssh_match = re.match(r'^git@[^:]+:(?:[^/]+/)?([^/]+?)(?:\.git)?$', url)
    if ssh_match:
        return ssh_match.group(1)
    
    # HTTPS format: https://host/org/repo.git
    https_match = re.match(r'^https?://[^/]+/(?:[^/]+/)?([^/]+?)(?:\.git)?$', url)
    if https_match:
        return https_match.group(1)
    
    return None


def _get_import_urls(repo_path: Optional[Path] = None) -> Dict[str, List[str]]:
    """Get unique import source URLs from .dvc files.
    
    This is a lightweight scan that just extracts URLs without cloning.
    Shared between _discover_import_sources() and credentials code.
    
    Args:
        repo_path: Project root (defaults to cwd).
    
    Returns:
        Dict mapping URL to list of .dvc files that import from it.
    """
    import yaml
    
    search_root = repo_path or Path.cwd()
    import_urls: Dict[str, List[str]] = {}
    
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
    
    return import_urls


@dataclass
class RepoCredentialInfo:
    """Info about a repo for credential fetching."""
    name: str
    url: Optional[str]  # None for primary repo
    has_s3_remote: bool
    remote_types: List[str]


def _get_repos_needing_credentials(verbose: bool = False) -> List[RepoCredentialInfo]:
    """Get list of repos that may need S3 credentials.
    
    Returns the primary repo plus source repos from imports, filtered to
    those with S3 remotes. Uses _discover_import_sources() to check remote types.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        List of RepoCredentialInfo, filtered to repos with S3 remotes.
    """
    repos: List[RepoCredentialInfo] = []
    seen_names: Set[str] = set()
    
    # Primary repo - check project-level DVC config only (not global)
    primary_name = utils.get_project_name()
    
    primary_has_s3 = False
    primary_types: List[str] = []
    result = subprocess.run(
        ['dvc', 'remote', 'list', '--project'],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                _, url = parts[0], parts[1]
                rtype = classify_url(url)
                if rtype not in primary_types:
                    primary_types.append(rtype)
                if rtype == 's3':
                    primary_has_s3 = True
    
    if primary_has_s3:
        repos.append(RepoCredentialInfo(
            name=primary_name,
            url=None,
            has_s3_remote=True,
            remote_types=primary_types,
        ))
        seen_names.add(primary_name)
        if verbose:
            print(f"Primary repo: {primary_name} (s3)")
    elif verbose:
        types_str = ', '.join(primary_types) if primary_types else 'none'
        print(f"Primary repo: {primary_name} (remotes: {types_str}, no s3)")
    
    # Source repos - use _discover_import_sources() which clones and checks remotes
    source_eps = _discover_import_sources(verbose=False)
    
    for ep in source_eps:
        name = _extract_repo_name_from_url(ep.url)
        if not name or name in seen_names:
            continue
        
        # Check children (DVC remotes) for S3 type
        has_s3 = False
        remote_types: List[str] = []
        for child in ep.children:
            if child.type not in remote_types:
                remote_types.append(child.type)
            if child.type == 's3':
                has_s3 = True
        
        if has_s3:
            repos.append(RepoCredentialInfo(
                name=name,
                url=ep.url,
                has_s3_remote=True,
                remote_types=remote_types,
            ))
            seen_names.add(name)
            if verbose:
                print(f"Found source repo: {name} (s3)")
        elif verbose and ep.children:
            types_str = ', '.join(remote_types)
            print(f"Skipping source repo: {name} (remotes: {types_str}, no s3)")
    
    return repos


def _get_installed_credentials() -> Dict[str, Dict[str, str]]:
    """Get credentials currently installed in global DVC config.
    
    Returns:
        Dictionary mapping remote name to credential keys present.
    """
    import re
    
    config_path = _get_dvc_global_config_path()
    
    if not config_path.exists():
        return {}
    
    sections = _parse_dvc_ini(config_path.read_text())
    
    credential_keys = {'access_key_id', 'secret_access_key', 'endpointurl', 'region'}
    credentials: Dict[str, Dict[str, str]] = {}
    
    for section_name, values in sections.items():
        # Check if this is a remote section
        if not section_name.startswith("'remote "):
            continue
        
        # Extract remote name from section name like 'remote "bcarc-wts"'
        match = re.match(r"'remote \"([^\"]+)\"'", section_name)
        if not match:
            continue
        remote_name = match.group(1)
        
        # Collect credential keys
        for key, value in values.items():
            if key in credential_keys:
                if remote_name not in credentials:
                    credentials[remote_name] = {}
                credentials[remote_name][key] = value
    
    return credentials


def install_credentials(
    verbose: bool = False,
) -> Dict[str, bool]:
    """Install S3 credentials from secret manager into global DVC config.
    
    Fetches credentials for the current repository and all source repos
    from imports that have S3 remotes, then merges them into the global
    DVC config.
    
    Uses INI merging to properly replace existing credentials rather than
    appending duplicates.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        Dict mapping repo names to success status. Empty dict if no S3 remotes
        are found (this is not an error).
        
    Raises:
        AuthError: If secret backend cannot be configured.
    """
    from .secrets import SecretError
    
    # Get repos with S3 remotes
    repos = _get_repos_needing_credentials(verbose=verbose)
    
    if not repos:
        if verbose:
            print("No repositories with S3 remotes found — nothing to install")
        return {}
    
    if verbose:
        print(f"\nFetching credentials for {len(repos)} repo(s) with S3 remotes...")
    
    # Get secret backend
    try:
        backend = _get_secret_backend()
    except AuthError:
        raise
    
    # Fetch secrets for each repo
    results: Dict[str, bool] = {}
    new_sections: Dict[str, Dict[str, str]] = {}
    
    for repo_info in repos:
        try:
            raw_config = backend.get_raw_config(repo_info.name)
            if raw_config and raw_config.strip():
                parsed = _parse_dvc_ini(raw_config)
                new_sections = _merge_ini_sections(new_sections, parsed)
                results[repo_info.name] = True
                if verbose:
                    section_names = list(parsed.keys())
                    print(f"  ✓ {repo_info.name}: {len(section_names)} section(s)")
            else:
                results[repo_info.name] = False
                if verbose:
                    print(f"  ⚠ {repo_info.name}: empty secret")
        except SecretError as e:
            results[repo_info.name] = False
            if verbose:
                print(f"  ⚠ {repo_info.name}: {e}")
    
    if not new_sections:
        raise AuthError("No credentials found for any repository with S3 remotes")
    
    # Get global config path
    config_path = _get_dvc_global_config_path()
    
    if verbose:
        print(f"\nMerging credentials into {config_path}...")
    
    # Ensure parent directory exists
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Read and parse existing config
    existing_sections: Dict[str, Dict[str, str]] = {}
    if config_path.exists():
        existing_content = config_path.read_text()
        existing_sections = _parse_dvc_ini(existing_content)
    
    # Merge new sections (replaces existing sections with same name)
    merged = _merge_ini_sections(existing_sections, new_sections)
    
    # Write back the merged config
    config_path.write_text(_format_dvc_ini(merged))
    
    # Ensure proper permissions (600)
    _ensure_config_local_permissions(config_path)
    
    if verbose:
        print(f"✓ Credentials installed to {config_path}")
        print(f"  Permissions: 600")
    
    return results


def uninstall_credentials(
    remote: Optional[str] = None,
    verbose: bool = False,
) -> List[str]:
    """Remove S3 credentials from global DVC config.
    
    Removes credential keys (access_key_id, secret_access_key, endpointurl)
    from remote sections in the global config. Does not remove the entire
    remote section - leaves url intact.
    
    Args:
        remote: Remove only credentials for this remote. If None, remove all.
        verbose: Print progress messages.
        
    Returns:
        List of remote names whose credentials were removed.
    """
    config_path = _get_dvc_global_config_path()
    
    if not config_path.exists():
        return []
    
    # Parse existing config
    existing_content = config_path.read_text()
    sections = _parse_dvc_ini(existing_content)
    
    credential_keys = {'access_key_id', 'secret_access_key', 'endpointurl', 'region'}
    removed = []
    
    for section_name, values in sections.items():
        # Check if this is a remote section
        if not section_name.startswith("'remote "):
            continue
        
        # Extract remote name from section name like 'remote "bcarc-wts"'
        import re
        match = re.match(r"'remote \"([^\"]+)\"'", section_name)
        if not match:
            continue
        remote_name = match.group(1)
        
        # Skip if we're filtering to a specific remote
        if remote and remote != remote_name:
            continue
        
        # Check if this remote has any credential keys
        cred_keys_present = [k for k in values.keys() if k in credential_keys]
        if not cred_keys_present:
            continue
        
        if verbose:
            print(f"Removing credentials for remote '{remote_name}'...")
        
        # Remove credential keys from this section
        for key in cred_keys_present:
            del sections[section_name][key]
        
        removed.append(remote_name)
    
    if removed:
        # Write back the modified config
        config_path.write_text(_format_dvc_ini(sections))
        _ensure_config_local_permissions(config_path)
    
    return removed


@dataclass
class CredentialStatus:
    """Status of credentials for a remote."""
    remote_name: str
    installed: bool
    keys_present: List[str]


def get_credentials_status(verbose: bool = False) -> List[CredentialStatus]:
    """Get status of credentials for all S3 remotes.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        List of CredentialStatus for each S3 remote.
    """
    # Get installed credentials
    installed = _get_installed_credentials()
    
    # Get S3 remotes from DVC config
    s3_remotes = set()
    result = subprocess.run(
        ['dvc', 'remote', 'list'],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                remote_name, url = parts[0], parts[1]
                if url.startswith('s3://'):
                    s3_remotes.add(remote_name)
    
    # Build status for all known remotes
    all_remotes = s3_remotes | set(installed.keys())
    
    statuses = []
    for remote_name in sorted(all_remotes):
        keys_present = list(installed.get(remote_name, {}).keys())
        statuses.append(CredentialStatus(
            remote_name=remote_name,
            installed=remote_name in installed,
            keys_present=keys_present,
        ))
    
    return statuses


def format_credentials_status(statuses: List[CredentialStatus]) -> str:
    """Format credential status for display.
    
    Args:
        statuses: List of CredentialStatus objects.
        
    Returns:
        Formatted string for terminal output.
    """
    if not statuses:
        return "No S3 remotes found."
    
    lines = ["S3 Remote Credentials:", ""]
    
    for status in statuses:
        # Status indicator
        if status.installed:
            indicator = "✓"
            keys = ', '.join(status.keys_present)
            line = f"  {indicator} {status.remote_name}  (installed: {keys})"
        else:
            indicator = "✗"
            line = f"  {indicator} {status.remote_name}  (no credentials)"
        
        lines.append(line)
    
    lines.append("")
    lines.append("Legend: ✓ installed  ✗ missing")
    
    return '\n'.join(lines)
