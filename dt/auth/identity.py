"""Identity detection and management (whoami)."""

import getpass
import json
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from .. import config as cfg


# =============================================================================
# Identity model
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


# =============================================================================
# Detectors
# =============================================================================

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
    accounts are currently active.

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


# =============================================================================
# Formatting
# =============================================================================

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
