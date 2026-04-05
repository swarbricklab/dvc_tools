"""Authentication and access management for DVC projects.

This package is split into submodules for maintainability.  All public
names are re-exported here so that existing code using
``from dt.auth import X`` continues to work.
"""

# -- Errors (originally from dt.errors, re-exported for convenience) ---------
from ..errors import AuthError  # noqa: F401

# -- Bring sibling modules into auth namespace so existing patches
#    like ``@patch('dt.auth.utils.get_project_name')`` keep working.
from .. import config as cfg  # noqa: F401
from .. import remote as remote_mod  # noqa: F401
from .. import tmp as tmp_mod  # noqa: F401
from .. import utils  # noqa: F401

# -- Stdlib modules used by submodules — imported here so that
#    ``@patch('dt.auth.subprocess.run')`` still resolves.
import getpass  # noqa: F401
import json  # noqa: F401
import os  # noqa: F401
import platform  # noqa: F401
import shutil  # noqa: F401
import subprocess  # noqa: F401
import urllib.error  # noqa: F401
import urllib.request  # noqa: F401

# -- Endpoints ---------------------------------------------------------------
from .endpoints import (  # noqa: F401
    ENDPOINT_TYPES,
    Endpoint,
    classify_url,
    discover_endpoints,
    discover_endpoints_from_repo,
    format_endpoints,
    format_endpoints_json,
    resolve_repo_url,
)

# -- Checks ------------------------------------------------------------------
from .checks import (  # noqa: F401
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    STATUS_WARN,
    CheckResult,
    check_endpoints,
    format_check_results,
    format_check_results_json,
)

# -- Identity ----------------------------------------------------------------
from .identity import (  # noqa: F401
    Identity,
    compare_identities,
    detect_identities,
    format_identities,
    format_identities_json,
    format_whoami_comparison,
    get_identities,
    save_detected_identities,
)

# -- Teams -------------------------------------------------------------------
from .teams import (  # noqa: F401
    TeamInfo,
    add_team_to_repo,
    add_user_to_team,
    format_teams,
    format_teams_json,
    list_repo_teams,
    list_user_teams,
)

# -- Access request ----------------------------------------------------------
from .request import (  # noqa: F401
    AccessRequest,
    format_request_json,
    format_request_markdown,
    format_request_text,
    generate_request,
    send_request,
    send_request_email,
    send_request_slack,
)

# -- Credentials -------------------------------------------------------------
from .credentials import (  # noqa: F401
    CredentialStatus,
    RepoCredentialInfo,
    SecretInfo,
    check_secret,
    format_credentials_status,
    get_credentials_status,
    install_credentials,
    list_repo_secrets,
    set_secret,
    uninstall_credentials,
)

# -- SSH setup ---------------------------------------------------------------
from .ssh import (  # noqa: F401
    SSHSetupResult,
    ssh_setup,
)

# -- Combined setup ----------------------------------------------------------
from .setup import (  # noqa: F401
    SetupReport,
    auth_setup,
    format_setup_report,
)

# -- Internal helpers used by tests (keep backward compat) -------------------
from .checks import (  # noqa: F401
    _check_dvc_remote,
    _check_dvc_remote_impl,
    _check_filesystem,
    _check_filesystem_for_user,
    _check_git,
    _check_gs,
    _check_github_for_user,
    _check_http,
    _check_s3,
    _check_ssh,
    _check_ssh_remote_dir,
    _extract_ssh_remote_path,
    _CHECKERS,
    _extract_remote_name,
    _get_owner_info,
    _get_user_info,
    _stat_check_user,
    _try_check,
)
from .credentials import (  # noqa: F401
    _ensure_config_local_permissions,
    _extract_repo_name_from_url,
    _format_dvc_ini,
    _get_dvc_config_local_path,
    _get_dvc_global_config_path,
    _get_import_urls,
    _get_installed_credentials,
    _get_repos_needing_credentials,
    _get_secret_backend,
    _merge_ini_sections,
    _parse_dvc_ini,
)
from .endpoints import (  # noqa: F401
    _discover_dt_config,
    _discover_dvc_remotes,
    _discover_git_remotes,
    _discover_import_sources,
)
from .identity import (  # noqa: F401
    _DETECT_FNS,
    _DETECTORS,
    _IDENTITY_KEYS,
    _detect_aws_identity,
    _detect_gcp_email,
    _detect_github_teams,
    _detect_github_user,
)
from .request import (  # noqa: F401
    _format_slack_blocks,
    _get_dt_version,
)
from .ssh import (  # noqa: F401
    _DEFAULT_KEY_PATH,
    _DEFAULT_KEY_TYPE,
    _FORGE_HOSTS,
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
    _parse_ssh_config,
    _write_ssh_config_stanza,
)
from .teams import (  # noqa: F401
    _parse_github_owner_repo,
)
from ._helpers import (  # noqa: F401
    _apply_type_filter,
    _merge_children,
    _short_repo_name,
)
