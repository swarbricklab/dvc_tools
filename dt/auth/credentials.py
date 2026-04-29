"""DVC credential management (install / uninstall / status).

Credentials are stored in AWS shared-credentials files
(``~/.aws/credentials`` and ``~/.aws/config``) using profiles named after
the **repository** (one profile per repo). The committed per-repo
``.dvc/config`` references the profile via ``profile = <reponame>`` and
holds the public ``endpointurl``.

The legacy behaviour wrote credentials into ``~/.config/dvc/config``;
that location is now treated as deprecated. ``install`` removes redundant
credential keys from there for the current project's S3 remotes, and
``migrate`` provides a one-shot conversion of any leftover entries.
"""

import configparser
import io
import os
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

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


# =============================================================================
# AWS shared-credentials file helpers
# =============================================================================

def _get_aws_credentials_path() -> Path:
    """Path to ``~/.aws/credentials`` (override via ``$AWS_SHARED_CREDENTIALS_FILE``)."""
    override = os.environ.get('AWS_SHARED_CREDENTIALS_FILE')
    if override:
        return Path(override)
    return Path.home() / '.aws' / 'credentials'


def _get_aws_config_path() -> Path:
    """Path to ``~/.aws/config`` (override via ``$AWS_CONFIG_FILE``)."""
    override = os.environ.get('AWS_CONFIG_FILE')
    if override:
        return Path(override)
    return Path.home() / '.aws' / 'config'


def _make_aws_parser() -> configparser.RawConfigParser:
    """Build a case-preserving ConfigParser suitable for AWS INI files."""
    parser = configparser.RawConfigParser()
    parser.optionxform = lambda s: s  # preserve case
    return parser


def _load_aws_ini(path: Path) -> configparser.RawConfigParser:
    """Load an AWS INI file (returns empty parser if file missing)."""
    parser = _make_aws_parser()
    if path.exists():
        parser.read(path)
    return parser


def _write_aws_ini(parser: configparser.RawConfigParser, path: Path) -> None:
    """Write an AWS INI file with 600 permissions, creating parent dir."""
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    parser.write(buf)
    path.write_text(buf.getvalue())
    path.chmod(0o600)


def _aws_profile_section_name(profile: str, *, in_config_file: bool) -> str:
    """Section name used by AWS for *profile*.

    In ``~/.aws/credentials``: ``[profile_name]``.
    In ``~/.aws/config``:      ``[profile profile_name]`` (except ``default``).
    """
    if in_config_file and profile != 'default':
        return f'profile {profile}'
    return profile


def _install_aws_profile(
    profile: str,
    access_key_id: str,
    secret_access_key: str,
    region: str = 'auto',
) -> Tuple[Path, Path]:
    """Write/update *profile* in the AWS credentials and config files.

    Returns:
        Tuple of (credentials_path, config_path) that were written.
    """
    creds_path = _get_aws_credentials_path()
    config_path = _get_aws_config_path()

    # ~/.aws/credentials
    creds = _load_aws_ini(creds_path)
    section = _aws_profile_section_name(profile, in_config_file=False)
    if not creds.has_section(section):
        creds.add_section(section)
    creds.set(section, 'aws_access_key_id', access_key_id)
    creds.set(section, 'aws_secret_access_key', secret_access_key)
    _write_aws_ini(creds, creds_path)

    # ~/.aws/config
    cfg = _load_aws_ini(config_path)
    section = _aws_profile_section_name(profile, in_config_file=True)
    if not cfg.has_section(section):
        cfg.add_section(section)
        cfg.set(section, 'region', region)
    elif not cfg.has_option(section, 'region'):
        cfg.set(section, 'region', region)
    _write_aws_ini(cfg, config_path)

    return creds_path, config_path


def _remove_aws_profile(profile: str) -> List[str]:
    """Remove *profile* from both AWS files. Returns list of files modified."""
    modified: List[str] = []

    creds_path = _get_aws_credentials_path()
    if creds_path.exists():
        creds = _load_aws_ini(creds_path)
        section = _aws_profile_section_name(profile, in_config_file=False)
        if creds.has_section(section):
            creds.remove_section(section)
            _write_aws_ini(creds, creds_path)
            modified.append(str(creds_path))

    config_path = _get_aws_config_path()
    if config_path.exists():
        cfg = _load_aws_ini(config_path)
        section = _aws_profile_section_name(profile, in_config_file=True)
        if cfg.has_section(section):
            cfg.remove_section(section)
            _write_aws_ini(cfg, config_path)
            modified.append(str(config_path))

    return modified


def _list_aws_profiles() -> Set[str]:
    """Return set of profiles present in ``~/.aws/credentials``."""
    creds_path = _get_aws_credentials_path()
    if not creds_path.exists():
        return set()
    creds = _load_aws_ini(creds_path)
    return set(creds.sections())


# =============================================================================
# Secret format helpers
# =============================================================================

def _detect_secret_format(content: str) -> str:
    """Classify a secret's INI text.

    Returns:
        ``'aws'`` if the content has a plain section with
        ``aws_access_key_id``; ``'dvc'`` if it has a
        ``'remote "..."'`` section with ``access_key_id``;
        ``'unknown'`` otherwise.
    """
    has_dvc_remote = re.search(
        r"^\[\s*'remote \"[^\"]+\"'\s*\]\s*$", content, re.MULTILINE,
    )
    has_dvc_keys = re.search(r"^\s*access_key_id\s*=", content, re.MULTILINE)
    if has_dvc_remote and has_dvc_keys:
        return 'dvc'

    has_aws_keys = re.search(
        r"^\s*aws_access_key_id\s*=", content, re.MULTILINE,
    )
    if has_aws_keys:
        return 'aws'

    return 'unknown'


def _parse_aws_secret(content: str, repo_name: str) -> Tuple[str, str]:
    """Parse an AWS-INI secret and return ``(access_key_id, secret_access_key)``.

    Fails loudly if multiple sections carry *different* credentials.
    """
    parser = _make_aws_parser()
    try:
        parser.read_string(content)
    except configparser.Error as exc:
        raise AuthError(
            f"Secret for '{repo_name}' is not valid INI: {exc}"
        ) from exc

    if not parser.sections():
        raise AuthError(f"Secret for '{repo_name}' has no sections.")

    pairs: Set[Tuple[str, str]] = set()
    for section in parser.sections():
        if not parser.has_option(section, 'aws_access_key_id'):
            continue
        if not parser.has_option(section, 'aws_secret_access_key'):
            continue
        pairs.add((
            parser.get(section, 'aws_access_key_id'),
            parser.get(section, 'aws_secret_access_key'),
        ))

    if not pairs:
        raise AuthError(
            f"Secret for '{repo_name}' has no aws_access_key_id / "
            f"aws_secret_access_key entries."
        )
    if len(pairs) > 1:
        raise AuthError(
            f"Secret for '{repo_name}' contains {len(pairs)} distinct "
            f"credential pair(s). One profile per repo is required; "
            f"split the repo or unify the keys."
        )
    return next(iter(pairs))


def _build_aws_secret(repo_name: str, access_key_id: str, secret_access_key: str) -> str:
    """Build the canonical AWS-INI secret content for *repo_name*."""
    parser = _make_aws_parser()
    parser.add_section(repo_name)
    parser.set(repo_name, 'aws_access_key_id', access_key_id)
    parser.set(repo_name, 'aws_secret_access_key', secret_access_key)
    buf = io.StringIO()
    parser.write(buf)
    return buf.getvalue()


# =============================================================================
# Project introspection
# =============================================================================

def _get_project_s3_remotes(repo_path: Optional[Path] = None) -> Dict[str, Dict[str, str]]:
    """Return ``{remote_name: {key: value}}`` for S3 remotes in the project.

    Reads ``.dvc/config`` directly so we can see ``profile`` and
    ``endpointurl`` even when the user-side AWS files are missing.
    """
    cfg_path = (repo_path or Path.cwd()) / '.dvc' / 'config'
    if not cfg_path.exists():
        return {}
    sections = _parse_dvc_ini(cfg_path.read_text())
    out: Dict[str, Dict[str, str]] = {}
    for name, values in sections.items():
        m = re.match(r"'remote \"([^\"]+)\"'", name)
        if not m:
            continue
        url = values.get('url', '')
        if not url.startswith('s3://'):
            continue
        out[m.group(1)] = dict(values)
    return out


def _strip_dvc_global_creds(remote_names: Set[str], verbose: bool = False) -> List[str]:
    """Remove ``access_key_id`` / ``secret_access_key`` from DVC global config
    for the given *remote_names*. Returns list of remotes touched.

    Leaves ``endpointurl`` and ``region`` alone.
    """
    config_path = _get_dvc_global_config_path()
    if not config_path.exists() or not remote_names:
        return []

    sections = _parse_dvc_ini(config_path.read_text())
    cred_keys = {'access_key_id', 'secret_access_key'}
    touched: List[str] = []
    changed = False

    for section_name in list(sections.keys()):
        m = re.match(r"'remote \"([^\"]+)\"'", section_name)
        if not m:
            continue
        rname = m.group(1)
        if rname not in remote_names:
            continue
        present = [k for k in sections[section_name] if k in cred_keys]
        if not present:
            continue
        for k in present:
            del sections[section_name][k]
        touched.append(rname)
        changed = True
        if verbose:
            print(f"  Stripped {present} from DVC global config "
                  f"[remote \"{rname}\"]")

    if changed:
        config_path.write_text(_format_dvc_ini(sections))
        _ensure_config_local_permissions(config_path)

    return touched


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
    """Get credentials currently installed in the DVC global config (legacy).

    Kept for ``migrate``. Returns ``{remote_name: {key: value}}``.
    """
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
    """Install S3 credentials from the secret manager into AWS shared files.

    For each repo with S3 remotes:

    1. Fetch its secret from the configured backend.
    2. If the secret is in legacy DVC-INI format → fail loudly and point
       the user at ``dt auth credentials migrate``.
    3. Extract the single ``aws_access_key_id`` / ``aws_secret_access_key``
       pair. Multiple distinct pairs in one secret raise ``AuthError``.
    4. Write a ``[<repo_name>]`` profile to ``~/.aws/credentials`` and an
       ``[profile <repo_name>]`` section to ``~/.aws/config``.
    5. Strip ``access_key_id`` / ``secret_access_key`` from
       ``~/.config/dvc/config`` for any S3 remote in the current project
       (those entries are now redundant).

    Args:
        verbose:   Show detailed progress.
        repo_name: If given, only install credentials for this specific repo
                   (skips discovery; fetches the secret by name directly).

    Returns:
        Dict mapping repo names to success status.

    Raises:
        AuthError: If secret backend cannot be configured, or if a secret
                   is in the legacy format / has invalid content.
    """
    from ..secrets import SecretError

    if repo_name:
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

    backend = _get_secret_backend()

    results: Dict[str, bool] = {}
    legacy_repos: List[str] = []

    for repo_info in repos:
        try:
            raw = backend.get_raw_config(repo_info.name)
        except SecretError as exc:
            results[repo_info.name] = False
            if verbose:
                print(f"  \u26a0 {repo_info.name}: {exc}")
            continue

        if not raw or not raw.strip():
            results[repo_info.name] = False
            if verbose:
                print(f"  \u26a0 {repo_info.name}: empty secret")
            continue

        fmt = _detect_secret_format(raw)
        if fmt == 'dvc':
            legacy_repos.append(repo_info.name)
            results[repo_info.name] = False
            if verbose:
                print(f"  \u26a0 {repo_info.name}: legacy DVC-INI format")
            continue
        if fmt == 'unknown':
            results[repo_info.name] = False
            if verbose:
                print(f"  \u26a0 {repo_info.name}: unrecognised secret format")
            continue

        access_key, secret_key = _parse_aws_secret(raw, repo_info.name)
        creds_path, config_path = _install_aws_profile(
            repo_info.name, access_key, secret_key,
        )
        results[repo_info.name] = True
        if verbose:
            print(f"  \u2713 {repo_info.name}: profile installed "
                  f"({creds_path})")

    if legacy_repos:
        raise AuthError(
            "The following secret(s) are in legacy DVC-INI format and "
            "cannot be installed:\n  - "
            + "\n  - ".join(legacy_repos)
            + "\n\nRun `dt auth credentials migrate` to convert them to the "
            "new AWS-INI format and install."
        )

    if not any(results.values()):
        raise AuthError("No credentials were installed (no usable secrets found).")

    # Strip now-redundant credential keys from DVC global config, scoped to
    # remotes in the current project.
    project_remotes = set(_get_project_s3_remotes().keys())
    if project_remotes:
        stripped = _strip_dvc_global_creds(project_remotes, verbose=verbose)
        if stripped and verbose:
            print(f"\nCleaned legacy credentials from DVC global config for: "
                  f"{', '.join(stripped)}")

    return results


def uninstall_credentials(
    repo_name: Optional[str] = None,
    remote: Optional[str] = None,
    verbose: bool = False,
) -> List[str]:
    """Remove credentials from the user's AWS files and from DVC global config.

    Args:
        repo_name: If given, remove only this profile from ``~/.aws/*``.
                   If omitted, removes profiles for every repo discovered as
                   needing credentials.
        remote:    If given, additionally restrict the DVC global config
                   cleanup to this remote name only.
        verbose:   Show detailed progress.

    Returns:
        Sorted list of identifiers that were touched (profile names and/or
        legacy DVC remote names).
    """
    touched: Set[str] = set()

    # 1. AWS profiles
    if repo_name:
        profiles = [repo_name]
    else:
        repos = _get_repos_needing_credentials(verbose=False)
        profiles = [r.name for r in repos]

    for profile in profiles:
        modified = _remove_aws_profile(profile)
        if modified:
            touched.add(profile)
            if verbose:
                print(f"Removed AWS profile '{profile}' "
                      f"({', '.join(modified)})")

    # 2. Legacy DVC global config
    if remote:
        scope = {remote}
    elif repo_name:
        # Limit to the project's remotes when uninstalling for one repo
        scope = set(_get_project_s3_remotes().keys())
    else:
        scope = set(_get_project_s3_remotes().keys())

    if scope:
        stripped = _strip_dvc_global_creds(scope, verbose=verbose)
        for r in stripped:
            touched.add(r)

    return sorted(touched)


@dataclass
class CredentialStatus:
    """Status of credentials for an S3 remote in the current project."""
    remote_name: str
    profile: Optional[str]
    endpointurl: Optional[str]
    profile_installed: bool
    legacy_creds_in_global: bool


def get_credentials_status(verbose: bool = False) -> List[CredentialStatus]:
    """Status for every S3 remote in the current project.

    Reports whether the configured AWS profile exists in ``~/.aws/credentials``
    and whether legacy credentials linger in the DVC global config.
    """
    project_remotes = _get_project_s3_remotes()
    aws_profiles = _list_aws_profiles()
    legacy = _get_installed_credentials()

    statuses: List[CredentialStatus] = []
    for remote_name in sorted(project_remotes.keys()):
        values = project_remotes[remote_name]
        profile = values.get('profile')
        endpoint = values.get('endpointurl')

        legacy_keys = legacy.get(remote_name, {})
        has_legacy_creds = any(
            k in legacy_keys for k in ('access_key_id', 'secret_access_key')
        )

        statuses.append(CredentialStatus(
            remote_name=remote_name,
            profile=profile,
            endpointurl=endpoint,
            profile_installed=bool(profile and profile in aws_profiles),
            legacy_creds_in_global=has_legacy_creds,
        ))

    return statuses


def format_credentials_status(statuses: List[CredentialStatus]) -> str:
    """Format credential status for display."""
    if not statuses:
        return "No S3 remotes found in this project."

    lines = ["S3 Remote Credentials:", ""]

    for st in statuses:
        if st.profile_installed:
            indicator = "\u2713"
            detail = f"profile '{st.profile}' \u2192 ~/.aws/credentials"
        elif st.profile:
            indicator = "\u2717"
            detail = (f"profile '{st.profile}' configured but missing from "
                      "~/.aws/credentials")
        else:
            indicator = "\u2717"
            detail = "no `profile = ...` set in .dvc/config"

        line = f"  {indicator} {st.remote_name}  ({detail})"
        if st.legacy_creds_in_global:
            line += "  [legacy creds in DVC global config]"
        if not st.endpointurl:
            line += "  [no endpointurl in .dvc/config]"
        lines.append(line)

    lines.append("")
    lines.append("Legend: \u2713 ready  \u2717 needs attention")

    return '\n'.join(lines)


# =============================================================================
# configure-remotes
# =============================================================================

@dataclass
class ConfigureRemotesResult:
    """Result of running ``configure_remotes``."""
    repo_name: str
    updated_remotes: List[str] = field(default_factory=list)
    skipped_remotes: List[str] = field(default_factory=list)  # already configured
    config_path: Optional[Path] = None


def configure_remotes(
    endpoint: Optional[str] = None,
    verbose: bool = False,
) -> ConfigureRemotesResult:
    """Add ``endpointurl`` and ``profile = <reponame>`` to each S3 remote
    in the current project's committed ``.dvc/config``.

    Args:
        endpoint: Endpoint URL to set. If ``None``, falls back to the
                  ``secrets.default_endpointurl`` config item.
        verbose:  Show detailed progress.

    Raises:
        AuthError: If no endpoint is available, or no S3 remotes are found,
                   or ``.dvc/config`` does not exist.
    """
    from .. import config as cfg

    if endpoint is None:
        endpoint = cfg.get_value('secrets.default_endpointurl')
    if not endpoint:
        raise AuthError(
            "No endpoint URL provided and no `secrets.default_endpointurl` "
            "configured.\nPass --endpoint URL or set the dt config key."
        )

    repo_name = utils.get_project_name()
    cfg_path = Path.cwd() / '.dvc' / 'config'
    if not cfg_path.exists():
        raise AuthError(f"No .dvc/config in {Path.cwd()} — not a DVC project?")

    sections = _parse_dvc_ini(cfg_path.read_text())

    result = ConfigureRemotesResult(repo_name=repo_name, config_path=cfg_path)
    changed = False

    for section_name, values in sections.items():
        m = re.match(r"'remote \"([^\"]+)\"'", section_name)
        if not m:
            continue
        rname = m.group(1)
        url = values.get('url', '')
        if not url.startswith('s3://'):
            continue

        existing_endpoint = values.get('endpointurl')
        existing_profile = values.get('profile')

        needs_endpoint = existing_endpoint != endpoint
        needs_profile = existing_profile != repo_name

        if not needs_endpoint and not needs_profile:
            result.skipped_remotes.append(rname)
            if verbose:
                print(f"  \u2713 {rname}: already configured")
            continue

        if needs_endpoint:
            values['endpointurl'] = endpoint
        if needs_profile:
            values['profile'] = repo_name

        result.updated_remotes.append(rname)
        changed = True
        if verbose:
            print(f"  \u2713 {rname}: set endpointurl + profile = {repo_name}")

    if not result.updated_remotes and not result.skipped_remotes:
        raise AuthError("No S3 remotes found in .dvc/config.")

    if changed:
        cfg_path.write_text(_format_dvc_ini(sections))
        # Stage the change for commit
        subprocess.run(
            ['git', 'add', str(cfg_path)],
            cwd=str(cfg_path.parent.parent),
            capture_output=True,
        )

    return result


def format_configure_remotes_result(result: ConfigureRemotesResult) -> str:
    """Format a ``configure_remotes`` result for display."""
    lines = [f"Repo: {result.repo_name}"]
    if result.config_path:
        lines.append(f"Config: {result.config_path}")
    if result.updated_remotes:
        lines.append("")
        lines.append("Updated remotes (staged for commit):")
        for r in result.updated_remotes:
            lines.append(f"  \u2713 {r}")
    if result.skipped_remotes:
        lines.append("")
        lines.append("Already configured:")
        for r in result.skipped_remotes:
            lines.append(f"  \u00b7 {r}")
    if result.updated_remotes:
        lines.append("")
        lines.append(
            "Review with `git diff .dvc/config`, then commit and push."
        )
    return '\n'.join(lines)


# =============================================================================
# migrate
# =============================================================================

@dataclass
class MigrateResult:
    """Result of running ``migrate_credentials``."""
    reuploaded: List[str] = field(default_factory=list)
    installed: List[str] = field(default_factory=list)
    stripped_remotes: List[str] = field(default_factory=list)
    skipped: List[Tuple[str, str]] = field(default_factory=list)  # (name, reason)


def migrate_credentials(
    repo_name: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> MigrateResult:
    """One-shot migration from legacy DVC-INI secrets / global config to AWS files.

    For each repo (the named one, or every repo discovered as needing
    credentials):

    1. Fetch the secret. If it's already AWS-INI, just install it.
    2. If it's legacy DVC-INI, extract the single credential pair, build a
       new AWS-INI secret, upload it back to the backend, then install.
    3. Strip the now-redundant ``access_key_id`` / ``secret_access_key``
       entries for project-scope S3 remotes from the DVC global config.

    Args:
        repo_name: Migrate only this repo. Default: all discovered.
        dry_run:   Print what would happen without changing anything.
        verbose:   Show detailed progress.
    """
    from ..secrets import SecretError

    result = MigrateResult()

    if repo_name:
        repos = [RepoCredentialInfo(
            name=repo_name, url=None, has_s3_remote=True, remote_types=['s3'],
        )]
    else:
        repos = _get_repos_needing_credentials(verbose=verbose)

    if not repos:
        if verbose:
            print("No repositories with S3 remotes found — nothing to migrate.")
        return result

    backend = _get_secret_backend()

    for repo_info in repos:
        name = repo_info.name
        try:
            raw = backend.get_raw_config(name)
        except SecretError as exc:
            result.skipped.append((name, f"fetch failed: {exc}"))
            continue

        if not raw or not raw.strip():
            result.skipped.append((name, "empty secret"))
            continue

        fmt = _detect_secret_format(raw)

        if fmt == 'aws':
            access_key, secret_key = _parse_aws_secret(raw, name)
            if verbose:
                print(f"  \u00b7 {name}: already AWS-INI; installing")
        elif fmt == 'dvc':
            # Legacy: extract from the secret directly
            access_key, secret_key = _extract_creds_from_dvc_secret(raw, name)
            new_content = _build_aws_secret(name, access_key, secret_key)
            if dry_run:
                if verbose:
                    print(f"  [dry-run] would re-upload secret for '{name}' "
                          "in AWS-INI format")
            else:
                try:
                    backend.set_secret(name, new_content)
                except SecretError as exc:
                    result.skipped.append((name, f"reupload failed: {exc}"))
                    continue
            result.reuploaded.append(name)
            if verbose:
                print(f"  \u2713 {name}: re-uploaded as AWS-INI")
        else:
            result.skipped.append((name, "unrecognised secret format"))
            continue

        if dry_run:
            if verbose:
                print(f"  [dry-run] would install AWS profile '{name}'")
        else:
            _install_aws_profile(name, access_key, secret_key)
        result.installed.append(name)

    # Strip legacy creds from DVC global config (project-scope only)
    project_remotes = set(_get_project_s3_remotes().keys())
    if project_remotes:
        if dry_run:
            legacy = _get_installed_credentials()
            scoped = sorted(project_remotes & set(legacy.keys()))
            result.stripped_remotes = scoped
            if verbose and scoped:
                print(f"  [dry-run] would strip legacy creds for: "
                      f"{', '.join(scoped)}")
        else:
            result.stripped_remotes = _strip_dvc_global_creds(
                project_remotes, verbose=verbose,
            )

    return result


def _extract_creds_from_dvc_secret(content: str, repo_name: str) -> Tuple[str, str]:
    """Pull a single ``(access_key_id, secret_access_key)`` from a legacy
    DVC-INI secret. Fails loudly on multiple distinct pairs.
    """
    sections = _parse_dvc_ini(content)
    pairs: Set[Tuple[str, str]] = set()
    for name, values in sections.items():
        if not name.startswith("'remote "):
            continue
        ak = values.get('access_key_id')
        sk = values.get('secret_access_key')
        if ak and sk:
            pairs.add((ak, sk))
    if not pairs:
        raise AuthError(
            f"Legacy secret for '{repo_name}' has no access_key_id / "
            "secret_access_key entries."
        )
    if len(pairs) > 1:
        raise AuthError(
            f"Legacy secret for '{repo_name}' contains {len(pairs)} distinct "
            "credential pair(s); cannot migrate to a single AWS profile."
        )
    return next(iter(pairs))


def format_migrate_result(result: MigrateResult) -> str:
    """Format a ``MigrateResult`` for display."""
    lines: List[str] = []
    if result.reuploaded:
        lines.append(f"Re-uploaded {len(result.reuploaded)} secret(s) "
                     "in AWS-INI format:")
        for n in result.reuploaded:
            lines.append(f"  \u2713 {n}")
    if result.installed:
        if lines:
            lines.append("")
        lines.append(f"Installed {len(result.installed)} AWS profile(s):")
        for n in result.installed:
            lines.append(f"  \u2713 {n}")
    if result.stripped_remotes:
        if lines:
            lines.append("")
        lines.append("Removed legacy credential keys from DVC global config "
                     "for remote(s):")
        for n in result.stripped_remotes:
            lines.append(f"  \u2713 {n}")
    if result.skipped:
        if lines:
            lines.append("")
        lines.append("Skipped:")
        for n, reason in result.skipped:
            lines.append(f"  \u26a0 {n}: {reason}")
    if not lines:
        return "Nothing to migrate."
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
        fmt = _detect_secret_format(raw)
        if fmt == 'aws':
            parser = _make_aws_parser()
            parser.read_string(raw)
            section_names = list(parser.sections())
        else:
            section_names = list(_parse_dvc_ini(raw).keys())
        return SecretInfo(
            repo_name=repo_name,
            exists=True,
            accessible=True,
            error=None,
            section_count=len(section_names),
            sections=section_names,
        )
    except SecretError as e:
        return SecretInfo(
            repo_name=repo_name, exists=True, accessible=False,
            error=str(e),
        )


def set_secret(repo_name: str, content: str) -> None:
    """Create or update the secret for *repo_name* with raw INI *content*.

    Accepts either the new AWS-INI format (preferred) or the legacy
    DVC-INI format (so existing tooling still works). The new format is:

        [<repo_name>]
        aws_access_key_id = ...
        aws_secret_access_key = ...

    Args:
        repo_name: Repository name (without prefix).
        content:   Raw INI text.

    Raises:
        AuthError: If the backend is not configured, the content is
                   unparseable, or the operation fails.
    """
    from ..secrets import SecretError

    fmt = _detect_secret_format(content)
    if fmt == 'unknown':
        raise AuthError(
            "Content is not recognised as AWS-INI or legacy DVC-INI. "
            "Expected an `aws_access_key_id` (AWS) or `access_key_id` "
            "(legacy DVC) entry."
        )

    try:
        backend = _get_secret_backend()
        backend.set_secret(repo_name, content)
    except SecretError as e:
        raise AuthError(f"Failed to set secret for '{repo_name}': {e}") from e

