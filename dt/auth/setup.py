"""Combined ``dt auth setup`` command.

Orchestrates both SSH key setup and S3 credential installation in a
single pass, driven by endpoint discovery.  Accepts an optional
``--config`` YAML file so the command can run non-interactively.

YAML config format::

    hosts:
      gadi-dm.nci.org.au:
        username: jr9959
        password: secret       # optional — only used by ssh-copy-id
      github.com:
        # forge — no username needed
"""

import getpass
import os
import platform
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import click
import yaml

from ..errors import AuthError
from .checks import STATUS_FAIL, check_endpoints
from .credentials import install_credentials
from .endpoints import Endpoint, classify_url, discover_endpoints
from .ssh import (
    SSHSetupResult,
    _DEFAULT_KEY_PATH,
    _deploy_key_forge,
    _deploy_key_ssh_copy_id,
    _ensure_ssh_dir,
    _extract_ssh_host,
    _extract_ssh_user,
    _find_existing_key,
    _generate_key,
    _host_in_ssh_config,
    _is_forge_host,
    _key_has_passphrase,
    _write_ssh_config_stanza,
)


# =============================================================================
# YAML config loader
# =============================================================================

@dataclass
class HostConfig:
    """Per-host settings from the YAML config file."""
    username: Optional[str] = None
    password: Optional[str] = None


def _load_config(config_path: Path) -> Dict[str, HostConfig]:
    """Load a YAML config file and return per-host settings.

    Returns:
        Mapping from hostname to :class:`HostConfig`.

    Raises:
        AuthError: If the file cannot be read or parsed.
    """
    try:
        raw = yaml.safe_load(config_path.read_text())
    except Exception as exc:
        raise AuthError(f"Cannot read config file {config_path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise AuthError(f"Config file must be a YAML mapping, got {type(raw).__name__}")

    hosts_raw = raw.get('hosts', {})
    if not isinstance(hosts_raw, dict):
        raise AuthError("'hosts' key must be a mapping")

    result: Dict[str, HostConfig] = {}
    for host, values in hosts_raw.items():
        if values is None:
            result[str(host)] = HostConfig()
        elif isinstance(values, dict):
            result[str(host)] = HostConfig(
                username=values.get('username'),
                password=values.get('password'),
            )
        else:
            raise AuthError(
                f"Host entry '{host}' must be a mapping or empty, "
                f"got {type(values).__name__}"
            )

    return result


# =============================================================================
# Combined setup orchestrator
# =============================================================================

@dataclass
class SetupReport:
    """Summary of what ``auth_setup`` did."""
    ssh_results: List[SSHSetupResult] = field(default_factory=list)
    credentials_installed: Dict[str, bool] = field(default_factory=dict)
    skipped_ssh: bool = False
    skipped_credentials: bool = False
    errors: List[str] = field(default_factory=list)


def auth_setup(
    config_path: Optional[Path] = None,
    username: Optional[str] = None,
    ssh_config_file: Optional[Path] = None,
    verbose: bool = False,
) -> SetupReport:
    """Combined SSH + credentials setup driven by endpoint discovery.

    Steps:

    1. Load optional YAML ``--config`` file.
    2. Discover all endpoints.
    3. If SSH/git endpoints exist → run SSH key setup.
    4. If S3 endpoints exist → install credentials.

    Args:
        config_path: Optional YAML config file with per-host usernames
            and passwords.
        username: Default SSH username (overridden by config file).
        ssh_config_file: Path to SSH config (default ``~/.ssh/config``).
        verbose: Print progress.

    Returns:
        :class:`SetupReport` summarising what was done.
    """
    report = SetupReport()

    if ssh_config_file is None:
        ssh_config_file = Path.home() / '.ssh' / 'config'

    # -- 1. Load YAML config (if provided) ---------------------------------
    host_configs: Dict[str, HostConfig] = {}
    if config_path is not None:
        host_configs = _load_config(config_path)
        if verbose:
            print(f"Loaded config for {len(host_configs)} host(s) from {config_path}")

    # -- 2. Discover ALL endpoints -----------------------------------------
    if verbose:
        print("\nDiscovering endpoints ...")
    all_endpoints = discover_endpoints(verbose=verbose)

    # Flatten including children
    flat_eps: List[Endpoint] = []
    for ep in all_endpoints:
        flat_eps.append(ep)
        flat_eps.extend(ep.children)

    # Classify what we have
    has_ssh_or_git = any(ep.type in ('ssh', 'git') for ep in flat_eps)
    has_s3 = any(ep.type == 's3' for ep in flat_eps)

    # -- 3. SSH setup (if needed) ------------------------------------------
    if has_ssh_or_git:
        if verbose:
            print("\n--- SSH / git endpoint setup ---")
        try:
            ssh_results = _do_ssh_setup(
                endpoints=all_endpoints,
                host_configs=host_configs,
                default_username=username,
                ssh_config_file=ssh_config_file,
                verbose=verbose,
            )
            report.ssh_results = ssh_results
        except Exception as exc:
            report.errors.append(f"SSH setup error: {exc}")
            if verbose:
                print(f"  ERROR: {exc}")
    else:
        report.skipped_ssh = True
        if verbose:
            print("\nNo SSH/git endpoints found — skipping SSH setup.")

    # -- 4. Credential install (if needed) ---------------------------------
    if has_s3:
        if verbose:
            print("\n--- S3 credential setup ---")
        try:
            cred_results = install_credentials(verbose=verbose)
            report.credentials_installed = cred_results
        except AuthError as exc:
            report.errors.append(f"Credential install error: {exc}")
            if verbose:
                print(f"  ERROR: {exc}")
    else:
        report.skipped_credentials = True
        if verbose:
            print("\nNo S3 endpoints found — skipping credential install.")

    return report


# =============================================================================
# SSH setup (config-aware variant)
# =============================================================================

def _do_ssh_setup(
    endpoints: List[Endpoint],
    host_configs: Dict[str, HostConfig],
    default_username: Optional[str],
    ssh_config_file: Path,
    verbose: bool,
) -> List[SSHSetupResult]:
    """SSH setup that honours the YAML host configs."""

    # Filter to SSH/git
    ssh_git_types = {'ssh', 'git'}
    ssh_eps = [ep for ep in endpoints if ep.type in ssh_git_types]

    all_eps: List[Endpoint] = []
    for ep in ssh_eps:
        all_eps.append(ep)
        all_eps.extend(ep.children)

    if not all_eps:
        return []

    # Collect unique hosts
    all_hosts: Dict[str, Endpoint] = {}
    for ep in all_eps:
        host = _extract_ssh_host(ep.url)
        if host and host not in all_hosts:
            all_hosts[host] = ep

    if not all_hosts:
        return []

    # Resolve usernames (config file > CLI --username > URL > prompt)
    host_users: Dict[str, str] = {}
    for host, ep in all_hosts.items():
        if _is_forge_host(host):
            host_users[host] = 'git'
        elif host in host_configs and host_configs[host].username:
            host_users[host] = host_configs[host].username
        elif default_username:
            host_users[host] = default_username
        else:
            url_user = _extract_ssh_user(ep.url)
            if url_user:
                host_users[host] = url_user
            else:
                host_users[host] = click.prompt(
                    f"SSH username for {host}",
                    type=str,
                )

    # Ensure ~/.ssh and keypair
    _ensure_ssh_dir(verbose=verbose)

    key_path = _find_existing_key()
    key_generated = False
    if key_path is None:
        key_path = _generate_key(verbose=verbose)
        key_generated = True
    elif verbose:
        print(f"  Using existing key: {key_path}")

    has_passphrase = _key_has_passphrase(key_path)
    if has_passphrase and verbose:
        print(f"  \u26a0 Key {key_path} is passphrase-protected.")
        print(f"    Run 'ssh-add {key_path}' to load it into your agent.")

    # Write config stanzas
    stanzas_written: Dict[str, bool] = {}
    for host in all_hosts:
        if not _host_in_ssh_config(host, ssh_config_file):
            _write_ssh_config_stanza(
                host=host,
                user=host_users[host],
                identity_file=key_path,
                config_path=ssh_config_file,
                verbose=verbose,
            )
            stanzas_written[host] = True
        else:
            stanzas_written[host] = False
            if verbose:
                print(f"  SSH config stanza for {host} already exists \u2014 skipping")

    # Check connectivity
    results_check = check_endpoints(
        endpoints=ssh_eps,
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

    # Deploy keys
    setup_results: List[SSHSetupResult] = []
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

    if has_passphrase:
        setup_results[0] = SSHSetupResult(
            **{**setup_results[0].__dict__,
               'message': setup_results[0].message +
               ' (\u26a0 key is passphrase-protected \u2014 run ssh-add)'}
        )

    return setup_results


# =============================================================================
# Formatting
# =============================================================================

def format_setup_report(report: SetupReport) -> str:
    """Format a :class:`SetupReport` for terminal output."""
    lines: List[str] = []

    if report.ssh_results:
        lines.append(click.style("\nSSH setup:", bold=True))
        for r in report.ssh_results:
            if r.already_ok:
                icon = click.style("\u2713", fg='green')
            elif r.manual_action_needed:
                icon = click.style("\u26a0", fg='yellow')
            else:
                icon = click.style("\u2713", fg='green')
            lines.append(f"  {icon} {r.host}: {r.message}")
    elif report.skipped_ssh:
        lines.append("\nNo SSH/git endpoints — SSH setup skipped.")

    if report.credentials_installed:
        lines.append(click.style("\nCredentials:", bold=True))
        for repo, ok in report.credentials_installed.items():
            icon = click.style("\u2713", fg='green') if ok else click.style("\u2717", fg='red')
            lines.append(f"  {icon} {repo}")
    elif report.skipped_credentials:
        lines.append("\nNo S3 endpoints — credential install skipped.")

    if report.errors:
        lines.append(click.style("\nErrors:", fg='red', bold=True))
        for err in report.errors:
            lines.append(f"  {err}")

    lines.append("")
    return '\n'.join(lines)
