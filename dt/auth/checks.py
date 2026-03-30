"""Access checking for discovered endpoints."""

import getpass
import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .endpoints import Endpoint, classify_url, discover_endpoints


# =============================================================================
# Status constants and CheckResult
# =============================================================================

STATUS_PASS = 'pass'
STATUS_FAIL = 'fail'
STATUS_WARN = 'warn'
STATUS_SKIP = 'skip'


@dataclass
class CheckResult:
    """Outcome of a single access check."""

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


# =============================================================================
# Per-type checkers
# =============================================================================


def _get_owner_info(path: Path) -> Tuple[str, str]:
    """Return ``(owner_name, group_name)`` for *path*."""
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
    """Check a filesystem endpoint for read/write access."""
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

    readable = os.access(path, os.R_OK)
    writable = os.access(path, os.W_OK)

    if not readable:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='not readable',
            hints=[f'Check permissions: ls -la {ep.url}'],
        )

    try:
        subdirs = sorted([d for d in path.iterdir() if d.is_dir()])
    except PermissionError:
        return CheckResult(
            endpoint=ep, status=STATUS_FAIL,
            summary='cannot list directory contents',
            hints=[f'Check permissions: ls -la {ep.url}'],
        )

    if not subdirs:
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
        details = detail_lines
        hints = []
        owners = set()
        for d in failed_dirs:
            owner, _ = _get_owner_info(d)
            if owner != '?':
                owners.add(owner)
        for d in failed_dirs[:3]:
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


def _extract_ssh_remote_path(url: str) -> Optional[str]:
    """Extract the remote directory path from an SSH URL.

    Handles ``ssh://[user@]host/path`` and SCP-style ``user@host:/path``.
    Returns *None* if no path component can be extracted.
    """
    import re

    url = url.strip()
    # ssh://[user@]host/path  — host must contain at least one char
    m = re.match(r'ssh://(?:[^@/]+@)?([^/]+)(/.+)', url)
    if m:
        return m.group(2)
    # SCP-style: [user@]host:/path  (skip scheme:// URLs)
    if '://' not in url:
        m = re.match(r'(?:[^@]+@)?[^:]+:(/.+)', url)
        if m:
            return m.group(1)
    return None


def _check_ssh_remote_dir(
    host: str, remote_path: str, verbose: bool = False,
) -> Optional[CheckResult]:
    """Check whether *remote_path* is accessible on *host* via SSH.

    Returns a :class:`CheckResult` on failure/warning, or *None* when the
    directory is fully accessible (caller should report its own PASS).
    """
    try:
        result = subprocess.run(
            ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
             host, 'test', '-d', remote_path, '-a', '-r', remote_path],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        # Cannot determine directory status — don't block on it.
        return None

    if result.returncode == 0:
        return None  # directory exists and is readable

    # The test failed — distinguish "does not exist" from "permission denied".
    # Run a second probe: ``test -e`` tells us if the path exists at all.
    try:
        exists_result = subprocess.run(
            ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
             host, 'test', '-e', remote_path],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None

    if exists_result.returncode != 0:
        # Path does not exist — might be expected for a brand-new remote.
        return CheckResult(
            endpoint=Endpoint(type='ssh', url='', source=''),  # placeholder
            status=STATUS_WARN,
            summary=f'remote directory does not exist: {remote_path}',
            hints=[
                f'The directory may need to be created on {host}.',
                f'Test manually: ssh {host} ls -ld {remote_path}',
            ],
        )

    # Path exists but is not a readable directory — permission problem.
    return CheckResult(
        endpoint=Endpoint(type='ssh', url='', source=''),  # placeholder
        status=STATUS_FAIL,
        summary=f'remote directory not accessible: {remote_path}',
        hints=[
            f'You may lack the required project/group membership to access {remote_path}.',
            f'Test manually: ssh {host} ls -ld {remote_path}',
        ],
    )


def _check_ssh(ep: Endpoint, verbose: bool = False) -> CheckResult:
    """Check an SSH endpoint.

    Performs a two-part check:

    1. SSH host connectivity (can we reach the host at all?).
    2. Remote directory access (can we read the target path?).
    """
    if ep.local_path:
        fs_ep = Endpoint(
            type='filesystem', url=ep.local_path, source=ep.source,
        )
        result = _check_filesystem(fs_ep, verbose=verbose)
        result.endpoint = ep
        result.summary = f'checked as local path — {result.summary}'
        return result

    url = ep.url.strip()
    if url.startswith('ssh://'):
        netloc = url[6:].split('/')[0]
        host = netloc.split('@')[-1]
    elif '@' in url and ':' in url:
        host = url.split('@')[1].split(':')[0]
    else:
        host = url

    # -- Part 1: SSH host connectivity ------------------------------------
    try:
        result_proc = subprocess.run(
            ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
             host],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result_proc.returncode == 255:
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

    # -- Part 2: remote directory access ----------------------------------
    remote_path = _extract_ssh_remote_path(url)
    if remote_path:
        dir_result = _check_ssh_remote_dir(host, remote_path, verbose=verbose)
        if dir_result is not None:
            dir_result.endpoint = ep
            return dir_result

    return CheckResult(
        endpoint=ep, status=STATUS_PASS,
        summary='connection OK' if not remote_path else 'connection OK, remote directory accessible',
    )


def _get_dvc_remote_config(remote_name: str, key: str) -> Optional[str]:
    """Read a single DVC remote config value."""
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
    """Extract the DVC remote name from an endpoint's source string."""
    import re
    m = re.search(r"DVC remote '([^']+)'", source)
    return m.group(1) if m else None


def _check_s3(ep: Endpoint) -> CheckResult:
    """Check an S3-compatible endpoint."""
    import shutil

    if not shutil.which('aws'):
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary='aws CLI not installed',
            hints=['Install the AWS CLI: pip install awscli'],
        )

    endpoint_url = None
    remote_name = _extract_remote_name(ep.source)
    if remote_name:
        endpoint_url = _get_dvc_remote_config(remote_name, 'endpointurl')

    extra_args: List[str] = []
    if endpoint_url:
        extra_args = ['--endpoint-url', endpoint_url]

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

    bucket_prefix = ep.url
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
    """Check a GCS endpoint."""
    import shutil

    if not shutil.which('gcloud'):
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary='gcloud CLI not installed',
            hints=['Install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install'],
        )

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
    """Get uid, primary gid, and supplementary gids for *username*."""
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
    """Simulate permission check for a given uid/gids."""
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
    """Check POSIX ACLs for a user via ``getfacl``."""
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
        if line.startswith(f'user:{username}:'):
            perms = line.split(':')[2]
            if 'r' in perms:
                readable = True
            if 'w' in perms:
                writable = True
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
    """Check filesystem access from *username*'s perspective."""
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
    """Check GitHub repository access for a specific user."""
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
    """Check a DVC remote via DVC's own storage API."""
    return _check_dvc_remote_impl(ep, remote_name, verbose=verbose)


def _check_dvc_remote_impl(ep: Endpoint, remote_name: str,
                           verbose: bool = False,
                           _repo_factory=None) -> Optional[CheckResult]:
    """Implementation of :func:`_check_dvc_remote`."""
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


# =============================================================================
# Check orchestration
# =============================================================================

def check_endpoints(
    endpoints: Optional[List[Endpoint]] = None,
    type_filter: Optional[Set[str]] = None,
    verbose: bool = False,
    user: Optional[str] = None,
) -> List[CheckResult]:
    """Run access checks on every endpoint."""
    if endpoints is None:
        endpoints = discover_endpoints(type_filter=type_filter, verbose=verbose)

    results: List[CheckResult] = []
    for ep in endpoints:
        result = _try_check(ep, verbose=verbose, user=user)
        results.append(result)

        for child in ep.children:
            results.append(_try_check(child, verbose=verbose, user=user))

    return results


def _try_check(ep: Endpoint, verbose: bool = False,
               user: Optional[str] = None) -> CheckResult:
    """Check a single endpoint, preferring DVC-native access where possible."""
    if user:
        if ep.type == 'filesystem':
            return _check_filesystem_for_user(ep, user, verbose=verbose)
        if ep.type == 'git':
            return _check_github_for_user(ep, user)
        if ep.type == 'ssh' and ep.local_path:
            local_ep = Endpoint(
                type='filesystem', url=ep.local_path,
                source=ep.source, local_path=ep.local_path,
            )
            return _check_filesystem_for_user(local_ep, user, verbose=verbose)
        return CheckResult(
            endpoint=ep, status=STATUS_SKIP,
            summary=f'cannot check {ep.type} access for another user',
        )

    remote_name = _extract_remote_name(ep.source)

    if remote_name and ' of ' not in ep.source:
        dvc_result = _check_dvc_remote(ep, remote_name, verbose=verbose)
        if dvc_result is not None:
            return dvc_result

    checker = _CHECKERS.get(ep.type)
    if checker:
        if ep.type in ('filesystem', 'ssh'):
            return checker(ep, verbose=verbose)
        return checker(ep)

    return CheckResult(
        endpoint=ep, status=STATUS_SKIP,
        summary=f'no checker for type {ep.type!r}',
    )


# =============================================================================
# Formatting
# =============================================================================

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
