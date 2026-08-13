"""Combined ``dt auth setup`` command.

Orchestrates both SSH key setup and S3 credential installation in a
single pass, driven by endpoint discovery.  Accepts an optional
``--config`` YAML file so the command can run non-interactively.

YAML config format::

    hosts:
      gadi-dm.nci.org.au:
        username: jr9959
      github.com:
        # forge — no username needed

Do not put passwords in this file. Nothing reads them: where a password
is genuinely needed, ``ssh-copy-id`` prompts for it interactively.
"""

import getpass
import os
import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import click
import yaml

from ..errors import AuthError
from ._helpers import _extract_repo_name_from_url, _short_repo_name
from .checks import STATUS_FAIL, check_endpoints
from .credentials import install_credentials
from .endpoints import (
    Endpoint,
    classify_url,
    discover_endpoints,
    discover_endpoints_from_repo,
)
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
    _key_accepted_by_host,
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
    repo_url: Optional[str] = None,
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
        repo_url: Set up access to this repository (URL or short name)
            instead of the current directory's project. The repo is
            shallow-cloned to a temp dir purely to read its config, so
            this works from anywhere and without cloning it for real.
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
    if repo_url:
        all_endpoints = discover_endpoints_from_repo(repo_url, verbose=verbose)
    else:
        all_endpoints = discover_endpoints(verbose=verbose)

    # Flatten including children
    flat_eps: List[Endpoint] = []
    for ep in all_endpoints:
        flat_eps.append(ep)
        flat_eps.extend(ep.children)

    # Classify what we have
    has_ssh_or_git = any(ep.type in ('ssh', 'git') for ep in flat_eps)
    has_s3 = any(ep.type == 's3' for ep in flat_eps)

    # -- 2b. Early GCP auth check (fail fast, not after SSH setup) ---------
    if has_s3:
        from ..secrets.gcp import GCPSecretBackend
        if not GCPSecretBackend._has_adc_credentials() \
                and not GCPSecretBackend.check_gcloud_authenticated():
            report.errors.append(
                "No active GCP authentication. "
                "Run 'gcloud auth login' then retry."
            )
            if verbose:
                print("\n\u26a0 No active GCP authentication found.")
                print("  Run 'gcloud auth login' to authenticate, then retry.")
            has_s3 = False  # skip credential install

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
            if repo_url:
                # install_credentials() discovers repos from the current
                # directory, which is the wrong project entirely here.
                # Name the repos explicitly instead.
                report.credentials_installed = _install_credentials_for_repos(
                    _s3_secret_names(repo_url, all_endpoints),
                    report=report,
                    verbose=verbose,
                )
            else:
                report.credentials_installed = install_credentials(verbose=verbose)
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
# Credentials for a named repo (--repo)
# =============================================================================

def _s3_secret_names(repo_url: str, endpoints: List[Endpoint]) -> List[str]:
    """Names of the secrets needed for the S3 endpoints in *endpoints*.

    Secrets are named after the repo that owns the remote, so this maps
    each S3 endpoint back to its owning repo:

    * a top-level S3 endpoint is a remote of ``repo_url`` itself;
    * an S3 endpoint nested under a parent is a remote of that parent,
      which is an import source (``deps.repo.url`` in a ``.dvc`` file).

    A repo whose data is imported from elsewhere needs the source repo's
    credentials too, otherwise the fetch fails on exactly the paths the
    caller came for.

    Returns:
        Repo names, in discovery order, without duplicates.
    """
    names: List[str] = []

    def _add(name: Optional[str]) -> None:
        if name and name not in names:
            names.append(name)

    for ep in endpoints:
        if ep.type == 's3':
            _add(_short_repo_name(repo_url))
        if any(child.type == 's3' for child in ep.children):
            _add(_extract_repo_name_from_url(ep.url) or _short_repo_name(ep.url))

    return names


def _install_credentials_for_repos(
    repo_names: List[str],
    report: SetupReport,
    verbose: bool,
) -> Dict[str, bool]:
    """Install credentials for each named repo, recording per-repo failures.

    One repo failing must not abort the others — ``install_credentials``
    raises when *its* single repo yields nothing usable, so each call is
    guarded separately.
    """
    installed: Dict[str, bool] = {}

    for name in repo_names:
        try:
            installed.update(install_credentials(verbose=verbose, repo_name=name))
        except AuthError as exc:
            installed[name] = False
            report.errors.append(f"Credential install error ({name}): {exc}")
            if verbose:
                print(f"  ERROR: {name}: {exc}")

    return installed


# =============================================================================
# SSH setup (config-aware variant)
# =============================================================================

def _prompt_username(host: str) -> str:
    """Ask for an SSH username, failing usefully when there is nobody to ask.

    ``click.prompt`` aborts with an empty message on a closed stdin, which
    surfaces as a bare "SSH setup error:" telling the caller nothing. Say
    what is missing and how to supply it instead.
    """
    if not sys.stdin.isatty():
        raise AuthError(
            f"No SSH username for {host}, and stdin is not interactive.\n"
            f"Supply one with -u/--username, or give the host an entry in a "
            f"--config file."
        )
    return click.prompt(f"SSH username for {host}", type=str)


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
                host_users[host] = _prompt_username(host)

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

    # Refine failing_hosts: for non-forge hosts that passed the
    # connectivity check, verify whether the configured key is already
    # accepted.  This avoids running ssh-copy-id (which prompts for a
    # password) when the key is already deployed.
    for host in list(all_hosts):
        if host in failing_hosts:
            continue
        if _is_forge_host(host):
            continue
        if not _key_accepted_by_host(
            host, host_users[host], key_path, verbose=verbose,
        ):
            failing_hosts.add(host)
            if verbose:
                print(f"  Key not accepted by {host} — will deploy")

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
