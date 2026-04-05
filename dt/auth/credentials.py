"""DVC credential management (install / uninstall / status)."""

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from .. import utils
from ..errors import AuthError
from .endpoints import Endpoint, classify_url, _discover_import_sources


# =============================================================================
# DVC config helpers
# =============================================================================

def _get_secret_backend():
    """Get the configured secret backend.

    Returns:
        A SecretBackend instance.

    Raises:
        AuthError: If no backend is configured or configuration is invalid.
    """
    from .. import config as cfg
    from ..secrets import GCPSecretBackend, SecretError

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
        # Quick auth check — fail fast instead of hanging on GCP calls
        if not GCPSecretBackend._has_adc_credentials() \
                and not GCPSecretBackend.check_gcloud_authenticated():
            raise AuthError(
                "No active GCP authentication found.\n"
                "Run 'gcloud auth login' to authenticate, then retry."
            )
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
    """Get path to DVC global config using platformdirs."""
    try:
        from platformdirs import user_config_dir
        return Path(user_config_dir('dvc')) / 'config'
    except ImportError:
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
    """Ensure config file has 600 permissions."""
    if path.exists():
        current_mode = path.stat().st_mode & 0o777
        if current_mode != 0o600:
            path.chmod(0o600)


def _parse_dvc_ini(content: str) -> Dict[str, Dict[str, str]]:
    """Parse DVC INI config into sections.

    Handles DVC's special section syntax like ['remote "name"'].
    """
    sections: Dict[str, Dict[str, str]] = {}
    current_section = None

    for line in content.split('\n'):
        line = line.rstrip()

        if not line or line.startswith('#') or line.startswith(';'):
            continue

        section_match = re.match(r"^\[(.+)\]$", line)
        if section_match:
            current_section = section_match.group(1)
            if current_section not in sections:
                sections[current_section] = {}
            continue

        if current_section and '=' in line:
            line = line.lstrip()
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip()
            sections[current_section][key] = value

    return sections


def _format_dvc_ini(sections: Dict[str, Dict[str, str]]) -> str:
    """Format sections back to DVC INI format."""
    lines = []

    for section_name, values in sections.items():
        lines.append(f'[{section_name}]')
        for key, value in values.items():
            lines.append(f'    {key} = {value}')
        lines.append('')

    return '\n'.join(lines)


def _merge_ini_sections(
    existing: Dict[str, Dict[str, str]],
    new: Dict[str, Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    """Merge new INI sections into existing, replacing duplicates."""
    result = {k: dict(v) for k, v in existing.items()}

    for section_name, values in new.items():
        if section_name in result:
            result[section_name].update(values)
        else:
            result[section_name] = dict(values)

    return result


def _extract_repo_name_from_url(url: str) -> Optional[str]:
    """Extract repository name from a git URL."""
    ssh_match = re.match(r'^git@[^:]+:(?:[^/]+/)?([^/]+?)(?:\.git)?$', url)
    if ssh_match:
        return ssh_match.group(1)

    https_match = re.match(r'^https?://[^/]+/(?:[^/]+/)?([^/]+?)(?:\.git)?$', url)
    if https_match:
        return https_match.group(1)

    return None


def _get_import_urls(repo_path: Optional[Path] = None) -> Dict[str, List[str]]:
    """Get unique import source URLs from .dvc files."""
    import yaml

    search_root = repo_path or Path.cwd()
    import_urls: Dict[str, List[str]] = {}

    for dvc_file in sorted(search_root.rglob('*.dvc')):
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


# =============================================================================
# Credential info model
# =============================================================================

@dataclass
class RepoCredentialInfo:
    """Info about a repo for credential fetching."""
    name: str
    url: Optional[str]  # None for primary repo
    has_s3_remote: bool
    remote_types: List[str]


def _get_repos_needing_credentials(verbose: bool = False) -> List[RepoCredentialInfo]:
    """Get list of repos that may need S3 credentials."""
    repos: List[RepoCredentialInfo] = []
    seen_names: Set[str] = set()

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

    source_eps = _discover_import_sources(verbose=False)

    for ep in source_eps:
        name = _extract_repo_name_from_url(ep.url)
        if not name or name in seen_names:
            continue

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
    """Get credentials currently installed in global DVC config."""
    config_path = _get_dvc_global_config_path()

    if not config_path.exists():
        return {}

    sections = _parse_dvc_ini(config_path.read_text())

    credential_keys = {'access_key_id', 'secret_access_key', 'endpointurl', 'region'}
    credentials: Dict[str, Dict[str, str]] = {}

    for section_name, values in sections.items():
        if not section_name.startswith("'remote "):
            continue

        match = re.match(r"'remote \"([^\"]+)\"'", section_name)
        if not match:
            continue
        remote_name = match.group(1)

        for key, value in values.items():
            if key in credential_keys:
                if remote_name not in credentials:
                    credentials[remote_name] = {}
                credentials[remote_name][key] = value

    return credentials


# =============================================================================
# Install / uninstall / status
# =============================================================================

def install_credentials(
    verbose: bool = False,
    repo_name: Optional[str] = None,
) -> Dict[str, bool]:
    """Install S3 credentials from secret manager into global DVC config.

    Args:
        verbose:   Show detailed progress.
        repo_name: If given, only install credentials for this specific repo
                   (skips discovery; fetches the secret by name directly).

    Returns:
        Dict mapping repo names to success status.

    Raises:
        AuthError: If secret backend cannot be configured.
    """
    from ..secrets import SecretError

    if repo_name:
        # Single-repo mode: bypass discovery, fetch the named secret directly
        repos = [RepoCredentialInfo(
            name=repo_name,
            url=None,
            has_s3_remote=True,
            remote_types=['s3'],
        )]
        if verbose:
            print(f"Installing credentials for '{repo_name}' only...")
    else:
        repos = _get_repos_needing_credentials(verbose=verbose)

    if not repos:
        if verbose:
            print("No repositories with S3 remotes found — nothing to install")
        return {}

    if verbose:
        print(f"\nFetching credentials for {len(repos)} repo(s) with S3 remotes...")

    try:
        backend = _get_secret_backend()
    except AuthError:
        raise

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
                    print(f"  \u2713 {repo_info.name}: {len(section_names)} section(s)")
            else:
                results[repo_info.name] = False
                if verbose:
                    print(f"  \u26a0 {repo_info.name}: empty secret")
        except SecretError as e:
            results[repo_info.name] = False
            if verbose:
                print(f"  \u26a0 {repo_info.name}: {e}")

    if not new_sections:
        raise AuthError("No credentials found for any repository with S3 remotes")

    config_path = _get_dvc_global_config_path()

    if verbose:
        print(f"\nMerging credentials into {config_path}...")

    config_path.parent.mkdir(parents=True, exist_ok=True)

    existing_sections: Dict[str, Dict[str, str]] = {}
    if config_path.exists():
        existing_content = config_path.read_text()
        existing_sections = _parse_dvc_ini(existing_content)

    merged = _merge_ini_sections(existing_sections, new_sections)

    config_path.write_text(_format_dvc_ini(merged))

    _ensure_config_local_permissions(config_path)

    if verbose:
        print(f"\u2713 Credentials installed to {config_path}")
        print(f"  Permissions: 600")

    return results


def uninstall_credentials(
    remote: Optional[str] = None,
    verbose: bool = False,
) -> List[str]:
    """Remove S3 credentials from global DVC config.

    Returns:
        List of remote names whose credentials were removed.
    """
    config_path = _get_dvc_global_config_path()

    if not config_path.exists():
        return []

    existing_content = config_path.read_text()
    sections = _parse_dvc_ini(existing_content)

    credential_keys = {'access_key_id', 'secret_access_key', 'endpointurl', 'region'}
    removed = []

    for section_name, values in sections.items():
        if not section_name.startswith("'remote "):
            continue

        match = re.match(r"'remote \"([^\"]+)\"'", section_name)
        if not match:
            continue
        remote_name = match.group(1)

        if remote and remote != remote_name:
            continue

        cred_keys_present = [k for k in values.keys() if k in credential_keys]
        if not cred_keys_present:
            continue

        if verbose:
            print(f"Removing credentials for remote '{remote_name}'...")

        for key in cred_keys_present:
            del sections[section_name][key]

        removed.append(remote_name)

    if removed:
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
    """Get status of credentials for all S3 remotes."""
    installed = _get_installed_credentials()

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
    """Format credential status for display."""
    if not statuses:
        return "No S3 remotes found."

    lines = ["S3 Remote Credentials:", ""]

    for status in statuses:
        if status.installed:
            indicator = "\u2713"
            keys = ', '.join(status.keys_present)
            line = f"  {indicator} {status.remote_name}  (installed: {keys})"
        else:
            indicator = "\u2717"
            line = f"  {indicator} {status.remote_name}  (no credentials)"

        lines.append(line)

    lines.append("")
    lines.append("Legend: \u2713 installed  \u2717 missing")

    return '\n'.join(lines)


# =============================================================================
# Secret management helpers (list / check / set)
# =============================================================================

@dataclass
class SecretInfo:
    """Info about a single DVC remote secret."""
    repo_name: str
    exists: bool
    accessible: bool
    error: Optional[str]
    section_count: int = 0
    sections: Optional[List[str]] = None


def list_repo_secrets(verbose: bool = False) -> List[str]:
    """List all repo names that have a secret in the configured backend.

    Returns:
        Sorted list of repo names (prefix stripped).
    """
    from ..secrets import SecretError

    backend = _get_secret_backend()
    try:
        return backend.list_secrets()
    except SecretError as e:
        raise AuthError(f"Failed to list secrets: {e}") from e


def check_secret(repo_name: str) -> SecretInfo:
    """Check whether a secret exists and parse its content.

    Returns:
        :class:`SecretInfo` with existence, accessibility, and parsed details.
    """
    from ..secrets import SecretError

    try:
        backend = _get_secret_backend()
    except AuthError as e:
        return SecretInfo(
            repo_name=repo_name, exists=False, accessible=False,
            error=str(e),
        )

    exists = False
    try:
        exists = backend.secret_exists(repo_name)
    except SecretError as e:
        return SecretInfo(
            repo_name=repo_name, exists=False, accessible=False,
            error=str(e),
        )

    if not exists:
        return SecretInfo(
            repo_name=repo_name, exists=False, accessible=False,
            error=None,
        )

    try:
        raw = backend.get_raw_config(repo_name)
        sections = _parse_dvc_ini(raw)
        return SecretInfo(
            repo_name=repo_name,
            exists=True,
            accessible=True,
            error=None,
            section_count=len(sections),
            sections=list(sections.keys()),
        )
    except SecretError as e:
        return SecretInfo(
            repo_name=repo_name, exists=True, accessible=False,
            error=str(e),
        )


def set_secret(repo_name: str, content: str) -> None:
    """Create or update the secret for *repo_name* with raw DVC INI *content*.

    Args:
        repo_name: Repository name (without prefix).
        content:   Raw DVC INI config text.

    Raises:
        AuthError: If the backend is not configured or the operation fails.
    """
    from ..secrets import SecretError

    # Validate content is parseable INI before writing
    try:
        sections = _parse_dvc_ini(content)
    except Exception as e:
        raise AuthError(f"Invalid DVC INI content: {e}") from e

    if not sections:
        raise AuthError("No valid INI sections found in content.")

    try:
        backend = _get_secret_backend()
        backend.set_secret(repo_name, content)
    except SecretError as e:
        raise AuthError(f"Failed to set secret for '{repo_name}': {e}") from e

