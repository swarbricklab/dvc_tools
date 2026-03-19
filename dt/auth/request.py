"""Access request generation and delivery."""

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
from typing import Dict, List, Optional, Set

from .. import config as cfg
from .. import utils
from ..errors import AuthError
from .checks import (
    STATUS_FAIL,
    STATUS_WARN,
    CheckResult,
    check_endpoints,
)
from .endpoints import Endpoint
from .identity import Identity, get_identities


@dataclass
class AccessRequest:
    """An access-request template generated from check failures."""

    user: str
    project: str
    platform_name: str
    dt_version: str
    request_date: str
    identities: List[Identity] = field(default_factory=list)
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
    """Run access checks and collect failures into an :class:`AccessRequest`."""
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
        status_icon = '\U0001f534' if r.status == STATUS_FAIL else '\U0001f7e1'
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
    """Build a Slack message payload from an *AccessRequest*."""
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
