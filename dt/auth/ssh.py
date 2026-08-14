"""SSH key setup and deployment."""

import getpass
import os
import platform
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

import click

from .checks import STATUS_FAIL, check_endpoints
from .endpoints import Endpoint, discover_endpoints


# =============================================================================
# Constants
# =============================================================================

#: Git forge hostnames that use ``gh``/``glab`` for key registration
#: rather than ``ssh-copy-id``.
_FORGE_HOSTS: Dict[str, str] = {
    'github.com': 'gh',
    'gitlab.com': 'glab',
}

_DEFAULT_KEY_TYPE = 'ed25519'
_DEFAULT_KEY_PATH = Path.home() / '.ssh' / f'id_{_DEFAULT_KEY_TYPE}'


# =============================================================================
# Helpers
# =============================================================================

def _extract_ssh_host(url: str) -> Optional[str]:
    """Extract the hostname from an SSH or git URL.

    Handles ``ssh://[user@]host/path`` and SCP-style ``user@host:path``.
    Returns *None* if the URL cannot be parsed as SSH.
    """
    import re

    url = url.strip()
    # ssh://[user@]host/path
    m = re.match(r'ssh://(?:[^@]+@)?([^/:]+)', url)
    if m:
        return m.group(1)
    # Skip non-SSH scheme URLs (s3://, gs://, http://, etc.)
    if re.match(r'[a-zA-Z][a-zA-Z0-9+.-]*://', url):
        return None
    # SCP-style: [user@]host:path  (git remotes)
    m = re.match(r'(?:[^@]+@)?([^:]+):', url)
    if m and '/' not in m.group(1):
        return m.group(1)
    return None


def _extract_ssh_user(url: str) -> Optional[str]:
    """Extract the username from an SSH URL, if present."""
    import re

    url = url.strip()
    m = re.match(r'ssh://([^@]+)@', url)
    if m:
        return m.group(1)
    m = re.match(r'([^@]+)@[^:]+:', url)
    if m:
        return m.group(1)
    return None


def _is_forge_host(host: str) -> bool:
    """True if *host* is a known git forge (GitHub, GitLab, ...)."""
    return host in _FORGE_HOSTS


def _ensure_ssh_dir(verbose: bool = False) -> Path:
    """Ensure ``~/.ssh`` exists with mode 700.  Returns the path."""
    ssh_dir = Path.home() / '.ssh'
    if ssh_dir.exists():
        mode = ssh_dir.stat().st_mode & 0o777
        if mode != 0o700:
            ssh_dir.chmod(0o700)
            if verbose:
                print(f"  Fixed permissions on {ssh_dir} (was {oct(mode)}, now 0700)")
        elif verbose:
            print(f"  {ssh_dir} exists (permissions OK)")
    else:
        ssh_dir.mkdir(mode=0o700)
        if verbose:
            print(f"  Created {ssh_dir} (mode 0700)")
    return ssh_dir


def _find_existing_key() -> Optional[Path]:
    """Return the path to an existing private key, or *None*."""
    ssh_dir = Path.home() / '.ssh'
    for name in ('id_ed25519', 'id_rsa', 'id_ecdsa'):
        candidate = ssh_dir / name
        if candidate.exists():
            return candidate
    return None


def _generate_key(verbose: bool = False) -> Path:
    """Generate an ed25519 keypair and return the private-key path."""
    key_path = _DEFAULT_KEY_PATH
    if key_path.exists():
        return key_path
    subprocess.run(
        ['ssh-keygen', '-t', _DEFAULT_KEY_TYPE, '-f', str(key_path),
         '-N', '', '-C', f'{getpass.getuser()}@{platform.node()}'],
        check=True,
        capture_output=True,
    )
    if verbose:
        print(f"  Generated keypair: {key_path}")
    return key_path


def _key_has_passphrase(key_path: Path) -> bool:
    """Return True if *key_path* is passphrase-protected."""
    result = subprocess.run(
        ['ssh-keygen', '-y', '-P', '', '-f', str(key_path)],
        capture_output=True,
    )
    return result.returncode != 0


def _key_accepted_by_host(
    host: str,
    user: str,
    key_path: Path,
    verbose: bool = False,
) -> bool:
    """Check whether *key_path* is accepted by *host* for public-key auth.

    Disables the SSH agent so that only the specified key file is tested.
    Returns True if the key is accepted, False otherwise.
    """
    try:
        result = subprocess.run(
            [
                'ssh', '-T',
                '-o', 'BatchMode=yes',
                '-o', 'ConnectTimeout=5',
                '-o', 'IdentitiesOnly=yes',
                '-o', f'IdentityFile={key_path}',
                '-o', 'PreferredAuthentications=publickey',
                f'{user}@{host}',
            ],
            capture_output=True,
            text=True,
            timeout=10,
            env={**os.environ, 'SSH_AUTH_SOCK': ''},
        )
        # returncode 0 or 1 = authenticated (1 is normal for ssh -T)
        # returncode 255 = SSH-level failure (auth denied, etc.)
        accepted = result.returncode != 255
        if verbose:
            status = 'accepted' if accepted else 'rejected'
            print(f"  Key {key_path.name} {status} by {host}")
        return accepted
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


def _parse_ssh_config(config_path: Path) -> Dict[str, Dict[str, str]]:
    """Parse an SSH config file into ``{host_alias: {key: value}}``."""
    hosts: Dict[str, Dict[str, str]] = {}
    current_host: Optional[str] = None

    if not config_path.exists():
        return hosts

    for raw_line in config_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        key, _, value = line.partition(' ')
        key = key.strip()
        value = value.strip()
        if key.lower() == 'host':
            current_host = value
            hosts.setdefault(current_host, {})
        elif current_host is not None:
            hosts[current_host][key] = value

    return hosts


def _host_in_ssh_config(host: str, config_path: Path) -> bool:
    """Return True if *host* already has a stanza in the SSH config."""
    hosts = _parse_ssh_config(config_path)
    return host in hosts


def _write_ssh_config_stanza(
    host: str,
    user: Optional[str],
    identity_file: Path,
    config_path: Path,
    extra: Optional[Dict[str, str]] = None,
    verbose: bool = False,
) -> None:
    """Append a ``Host`` stanza to the SSH config file."""
    lines = [f'\nHost {host}']
    lines.append(f'    HostName {host}')
    if user:
        lines.append(f'    User {user}')
    lines.append(f'    IdentityFile {identity_file}')
    lines.append(f'    AddKeysToAgent yes')
    if extra:
        for k, v in extra.items():
            lines.append(f'    {k} {v}')
    lines.append('')

    if not config_path.exists():
        config_path.touch(mode=0o600)
        if verbose:
            print(f"  Created {config_path} (mode 0600)")
    else:
        mode = config_path.stat().st_mode & 0o777
        if mode != 0o600:
            config_path.chmod(0o600)
            if verbose:
                print(f"  Fixed permissions on {config_path} (was {oct(mode)}, now 0600)")

    with open(config_path, 'a') as f:
        f.write('\n'.join(lines))

    if verbose:
        print(f"  Added SSH config stanza for {host}")


#: Appends a key to authorized_keys, creating ~/.ssh if needed and skipping
#: the write when the key is already there. What ssh-copy-id does, minus
#: ssh-copy-id. Runs under the remote /bin/sh, so keep it POSIX.
_AUTHORIZED_KEYS_SH = (
    'set -e; '
    'mkdir -p ~/.ssh; '
    'chmod 700 ~/.ssh; '
    'touch ~/.ssh/authorized_keys; '
    'chmod 600 ~/.ssh/authorized_keys; '
    'if ! grep -qxF "$0" ~/.ssh/authorized_keys; then '
    'printf "%s\\n" "$0" >> ~/.ssh/authorized_keys; '
    'fi'
)


def _deploy_key_ssh_copy_id(
    host: str,
    user: str,
    key_path: Path,
    verbose: bool = False,
) -> bool:
    """Deploy a public key to a remote host.

    Prefers ``ssh-copy-id``, falling back to appending to
    ``authorized_keys`` over plain ``ssh`` when it isn't installed.

    The fallback exists because ``ssh-copy-id`` is not guaranteed to be
    present. It ships in ``openssh-clients`` as a separate file from ``ssh``
    itself, so minimal container images routinely have one and not the other
    -- which is exactly what the containerised ``module load dt`` build hits.
    Previously that raised FileNotFoundError out of the whole SSH phase.

    Returns True on success, False on failure.
    """
    pub_key = Path(f'{key_path}.pub')
    target = f'{user}@{host}'

    if shutil.which('ssh-copy-id'):
        if verbose:
            print(f"  Deploying key to {target} via ssh-copy-id ...")
        result = subprocess.run(
            ['ssh-copy-id', '-i', str(pub_key), target],
            capture_output=False,
            stdin=None,
        )
        return result.returncode == 0

    if verbose:
        print(f"  ssh-copy-id not installed — appending to "
              f"authorized_keys over ssh instead")
        print(f"  Deploying key to {target} via ssh ...")

    try:
        key_text = pub_key.read_text().strip()
    except OSError as exc:
        if verbose:
            print(f"  Could not read {pub_key}: {exc}")
        return False

    if not shutil.which('ssh'):
        if verbose:
            print("  ssh not installed either — cannot deploy the key")
        return False

    # ssh joins its trailing arguments into one string and hands it to the
    # remote *shell*, which parses it again -- so both the script and the key
    # have to survive a round of shell quoting. shlex.quote does that, and
    # passing the key as $0 keeps its content out of the script body entirely.
    #
    # stdin is deliberately left alone: the key isn't deployed yet, so ssh may
    # need to prompt for a password.
    remote = (
        f"sh -c {shlex.quote(_AUTHORIZED_KEYS_SH)} {shlex.quote(key_text)}"
    )
    result = subprocess.run(
        ['ssh', target, remote],
        capture_output=False,
        stdin=None,
    )
    return result.returncode == 0


def _deploy_key_forge(
    host: str,
    key_path: Path,
    verbose: bool = False,
) -> bool:
    """Deploy a public key to a git forge (GitHub/GitLab) via CLI.

    Returns True if the key was registered, False if the user must
    register it manually.
    """
    import socket

    pub_key = Path(f'{key_path}.pub')
    pub_key_text = pub_key.read_text().strip()
    cli_tool = _FORGE_HOSTS.get(host)
    title = f'dt@{socket.gethostname()}'

    if cli_tool and shutil.which(cli_tool):
        if verbose:
            print(f"  Registering key with {host} via {cli_tool} ...")
        result = subprocess.run(
            [cli_tool, 'ssh-key', 'add', str(pub_key), '--title', title],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            if verbose:
                print(f"  \u2713 Key registered with {host}")
            return True
        if 'already' in result.stderr.lower():
            if verbose:
                print(f"  Key already registered with {host}")
            return True
        # Detect missing OAuth scope and auto-refresh
        if 'admin:public_key' in result.stderr:
            if verbose:
                print(f"  Token lacks admin:public_key scope \u2014 requesting it ...")
            refresh_env = {**os.environ, 'GH_BROWSER': 'echo'}
            refresh = subprocess.run(
                [cli_tool, 'auth', 'refresh', '-h', host,
                 '-s', 'admin:public_key'],
                capture_output=False,
                stdin=None,
                env=refresh_env,
            )
            if refresh.returncode == 0:
                retry = subprocess.run(
                    [cli_tool, 'ssh-key', 'add', str(pub_key),
                     '--title', title],
                    capture_output=True,
                    text=True,
                )
                if retry.returncode == 0:
                    if verbose:
                        print(f"  \u2713 Key registered with {host}")
                    return True
                if 'already' in retry.stderr.lower():
                    if verbose:
                        print(f"  Key already registered with {host}")
                    return True
                if verbose:
                    print(f"  Retry failed: {retry.stderr.strip()}")
            else:
                if verbose:
                    print(f"  Scope refresh failed (user may have cancelled)")
        else:
            if verbose:
                print(f"  {cli_tool} failed: {result.stderr.strip()}")

    # Manual fallback
    if host == 'github.com':
        url = 'https://github.com/settings/ssh/new'
    elif host == 'gitlab.com':
        url = 'https://gitlab.com/-/user_settings/ssh_keys'
    else:
        url = f'https://{host}'

    print(f"\n  Your public key (copy this):")
    print(f"    {pub_key_text}")
    print(f"\n  Add it at: {url}")
    return False


# =============================================================================
# Main setup function
# =============================================================================

@dataclass
class SSHSetupResult:
    """Outcome of SSH setup for one host."""
    host: str
    already_ok: bool
    key_generated: bool
    key_deployed: bool
    config_written: bool
    manual_action_needed: bool
    message: str


def _deploy_keys_and_report(
    hosts: List[str],
    host_users: Dict[str, str],
    failing_hosts: Set[str],
    stanzas_written: Dict[str, bool],
    key_path: Path,
    key_generated: bool,
    has_passphrase: bool,
    verbose: bool = False,
) -> List[SSHSetupResult]:
    """Deploy keys to the hosts that need them and summarise the outcome.

    Shared by :func:`ssh_setup` and ``setup._do_ssh_setup``, which had
    byte-identical copies of this loop. They drifted -- a fix applied to one
    silently left the other broken -- so it lives in one place now.

    Each host is isolated: an exception deploying to one must not abandon the
    rest, because the caller's error handling discards partial results and a
    host that already succeeded would go unreported.

    Args:
        hosts: Hostnames to consider, in report order.
        host_users: Username to use per host.
        failing_hosts: Hosts whose key needs deploying.
        stanzas_written: Whether a config stanza was just written, per host.
        key_path: Private key; the ``.pub`` beside it is what gets deployed.
        key_generated: Whether the key was created during this run.
        has_passphrase: Whether the key is passphrase-protected.
        verbose: Print per-host progress.

    Returns:
        One result per host that needed work, or a single ``(all)`` result
        when there was nothing to do.
    """
    setup_results: List[SSHSetupResult] = []

    for host in hosts:
        is_forge = _is_forge_host(host)
        host_needs_key = host in failing_hosts
        config_written = stanzas_written.get(host, False)
        host_user = host_users[host]

        key_deployed = False
        manual_action = False

        if host_needs_key:
            try:
                if is_forge:
                    key_deployed = _deploy_key_forge(
                        host, key_path, verbose=verbose,
                    )
                else:
                    key_deployed = _deploy_key_ssh_copy_id(
                        host, host_user, key_path, verbose=verbose,
                    )
            except Exception as exc:
                key_deployed = False
                if verbose:
                    print(f"  Key deployment to {host} failed: {exc}")
            if not key_deployed:
                manual_action = True
                if verbose and not is_forge:
                    print(f"  ⚠ Could not deploy the key to {host}. "
                          f"You may need to deploy it manually.")

        if not host_needs_key and not config_written:
            continue

        msg_parts = []
        if key_deployed:
            msg_parts.append('key deployed')
        elif manual_action:
            msg_parts.append('key deployment needs manual action')
        if config_written:
            msg_parts.append('config stanza added')
        message = '; '.join(msg_parts) if msg_parts else 'already configured'

        setup_results.append(SSHSetupResult(
            host=host,
            already_ok=not host_needs_key and not config_written,
            key_generated=key_generated,
            key_deployed=key_deployed,
            config_written=config_written,
            manual_action_needed=manual_action,
            message=message,
        ))

    if not setup_results:
        if verbose:
            print("All SSH/git hosts already configured — nothing to do.")
        return [
            SSHSetupResult(
                host='(all)', already_ok=True, key_generated=False,
                key_deployed=False, config_written=False,
                manual_action_needed=False,
                message='All SSH/git hosts already configured',
            )
        ]

    if has_passphrase:
        setup_results[0] = SSHSetupResult(
            **{**setup_results[0].__dict__,
               'message': setup_results[0].message +
               ' (⚠ key is passphrase-protected — run ssh-add)'}
        )

    return setup_results


def ssh_setup(
    username: Optional[str] = None,
    config_file: Optional[Path] = None,
    verbose: bool = False,
) -> List[SSHSetupResult]:
    """Set up SSH access for every discovered SSH and git endpoint.

    1. Discover endpoints and collect unique SSH hosts.
    2. Resolve the username for each non-forge host (prompt if needed).
    3. Ensure ``~/.ssh`` exists with correct permissions and a keypair.
    4. Write SSH config stanzas for all hosts.
    5. Run ``check_endpoints`` to find hosts that still fail.
    6. Deploy the public key to each failing host.
    7. Warn about passphrase-protected keys in batch contexts.

    Args:
        username: Remote username for SSH hosts.
        config_file: Path to SSH config file (default ``~/.ssh/config``).
        verbose: Print progress.

    Returns:
        List of :class:`SSHSetupResult` for each host processed.
    """
    if config_file is None:
        config_file = Path.home() / '.ssh' / 'config'

    # -- 1. Discover endpoints and collect hosts ---------------------------
    ssh_git_types = {'ssh', 'git'}
    endpoints = discover_endpoints(type_filter=ssh_git_types, verbose=verbose)

    all_eps: List[Endpoint] = []
    for ep in endpoints:
        all_eps.append(ep)
        all_eps.extend(ep.children)

    if not all_eps:
        if verbose:
            print("No SSH or git endpoints discovered.")
        return []

    all_hosts: Dict[str, Endpoint] = {}
    for ep in all_eps:
        host = _extract_ssh_host(ep.url)
        if host and host not in all_hosts:
            all_hosts[host] = ep

    if not all_hosts:
        if verbose:
            print("No SSH hosts found in discovered endpoints.")
        return []

    # -- 2. Resolve usernames BEFORE any SSH connections -------------------
    host_users: Dict[str, str] = {}
    for host, ep in all_hosts.items():
        if _is_forge_host(host):
            host_users[host] = 'git'
        elif username:
            host_users[host] = username
        else:
            url_user = _extract_ssh_user(ep.url)
            if url_user:
                host_users[host] = url_user
            else:
                host_users[host] = click.prompt(
                    f"SSH username for {host}",
                    type=str,
                )

    # -- 3. Ensure ~/.ssh and keypair exist --------------------------------
    _ensure_ssh_dir(verbose=verbose)

    key_path = _find_existing_key()
    key_generated = False
    if key_path is None:
        key_path = _generate_key(verbose=verbose)
        key_generated = True
    elif verbose:
        print(f"  Using existing key: {key_path}")

    # -- 4. Check for passphrase-protected key -----------------------------
    has_passphrase = _key_has_passphrase(key_path)
    if has_passphrase and verbose:
        print(f"  \u26a0 Key {key_path} is passphrase-protected.")
        print(f"    Run 'ssh-add {key_path}' to load it into your agent.")
        print(f"    Note: passphrase keys will NOT work in PBS batch jobs")
        print(f"    unless the agent is forwarded (which NCI does not support).")

    # -- 5. Write config stanzas BEFORE checking connectivity --------------
    setup_results: List[SSHSetupResult] = []
    stanzas_written: Dict[str, bool] = {}

    if verbose:
        hosts_needing_stanza = [
            h for h in all_hosts if not _host_in_ssh_config(h, config_file)
        ]
        if hosts_needing_stanza:
            print(f"{len(hosts_needing_stanza)} host(s) need config stanzas:")
            for h in hosts_needing_stanza:
                print(f"  \u2022 {h}")

    for host in all_hosts:
        if not _host_in_ssh_config(host, config_file):
            _write_ssh_config_stanza(
                host=host,
                user=host_users[host],
                identity_file=key_path,
                config_path=config_file,
                verbose=verbose,
            )
            stanzas_written[host] = True
        else:
            stanzas_written[host] = False
            if verbose:
                print(f"  SSH config stanza for {host} already exists \u2014 skipping")

    # -- 6. Check endpoints (now using correct config) ---------------------
    #
    # The general connectivity check uses the system SSH binary, which may
    # succeed via agent-forwarded keys even though our *configured* key
    # (IdentityFile) is not in authorized_keys on the remote host.  DVC's
    # SSH access often doesn't have agent access (e.g. paramiko, sshfs, or
    # PBS batch jobs), so we must also verify that the *specific key file*
    # is accepted — not just that "some" authentication method works.
    results_check = check_endpoints(
        endpoints=endpoints,
        type_filter=ssh_git_types,
        verbose=False,
    )

    failing_hosts: Set[str] = set()
    for cr in results_check:
        if cr.status == STATUS_FAIL:
            host = _extract_ssh_host(cr.endpoint.url)
            if host:
                failing_hosts.add(host)

    # For non-forge hosts that passed the general check, verify the
    # configured key is specifically accepted (agent-less).
    for host in all_hosts:
        if host in failing_hosts:
            continue  # already know it needs key deployment
        if _is_forge_host(host):
            continue  # forge hosts use different deploy path
        if not _key_accepted_by_host(
            host, host_users[host], key_path, verbose=verbose,
        ):
            failing_hosts.add(host)
            if verbose:
                print(f"  {host}: connectivity OK via agent, but "
                      f"configured key {key_path.name} not accepted")

    if verbose and failing_hosts:
        print(f"\n{len(failing_hosts)} host(s) need key deployment:")
        for h in sorted(failing_hosts):
            print(f"  \u2022 {h}")

    # -- 7. Deploy keys for failing hosts ----------------------------------
    return _deploy_keys_and_report(
        hosts=list(all_hosts),
        host_users=host_users,
        failing_hosts=failing_hosts,
        stanzas_written=stanzas_written,
        key_path=key_path,
        key_generated=key_generated,
        has_passphrase=has_passphrase,
        verbose=verbose,
    )
