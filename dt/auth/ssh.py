"""SSH key setup and deployment."""

import getpass
import os
import platform
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


def _deploy_key_ssh_copy_id(
    host: str,
    user: str,
    key_path: Path,
    verbose: bool = False,
) -> bool:
    """Deploy a public key to a remote host via ``ssh-copy-id``.

    Returns True on success, False on failure.
    """
    pub_key = Path(f'{key_path}.pub')
    target = f'{user}@{host}'
    if verbose:
        print(f"  Deploying key to {target} via ssh-copy-id ...")
    result = subprocess.run(
        ['ssh-copy-id', '-i', str(pub_key), target],
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

    if verbose and failing_hosts:
        print(f"\n{len(failing_hosts)} host(s) need key deployment:")
        for h in sorted(failing_hosts):
            print(f"  \u2022 {h}")

    # -- 7. Deploy keys for failing hosts ----------------------------------
    for host, ep in all_hosts.items():
        is_forge = _is_forge_host(host)
        host_needs_key = host in failing_hosts
        config_written = stanzas_written[host]
        host_user = host_users[host]

        key_deployed = False
        manual_action = False

        if host_needs_key:
            if is_forge:
                key_deployed = _deploy_key_forge(host, key_path, verbose=verbose)
                if not key_deployed:
                    manual_action = True
            else:
                key_deployed = _deploy_key_ssh_copy_id(
                    host, host_user, key_path, verbose=verbose,
                )
                if not key_deployed:
                    manual_action = True
                    if verbose:
                        print(f"  \u26a0 ssh-copy-id failed for {host}. "
                              f"You may need to deploy the key manually.")

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
            print("All SSH/git hosts already configured \u2014 nothing to do.")
        return [
            SSHSetupResult(
                host='(all)', already_ok=True, key_generated=False,
                key_deployed=False, config_written=False,
                manual_action_needed=False,
                message='All SSH/git hosts already configured',
            )
        ]

    # -- Passphrase warning in summary ----------------------------------
    if has_passphrase:
        setup_results[0] = SSHSetupResult(
            **{**setup_results[0].__dict__,
               'message': setup_results[0].message +
               ' (\u26a0 key is passphrase-protected \u2014 run ssh-add)'}
        )

    return setup_results
