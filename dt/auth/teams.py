"""GitHub team management for repository access."""

import json
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from ..errors import AuthError
from .endpoints import resolve_repo_url


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
