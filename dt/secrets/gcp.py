"""GCP Secret Manager backend for DVC credentials."""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .. import config as cfg
from .base import SecretBackend, SecretError

#: Seconds to allow ``gcloud auth list`` before giving up.
#:
#: Generous because ``gcloud`` is a large Python application and on NCI it
#: lives on ``/g/data``, so a cold start pays NFS latency for every module it
#: imports. Measured on a Gadi persistent-session node: 20.4s cold, 5.3s warm.
#: The previous 10s budget turned a slow filesystem into a false "you are not
#: logged in", which sent people off to re-run ``gcloud auth login`` for a
#: problem that authentication could not fix.
GCLOUD_AUTH_TIMEOUT = 30.0


def _configured_secret_locations() -> Optional[str]:
    """Return the configured GCP secret location(s), or None.

    Read from the ``secrets.gcp.locations`` config key (a comma-separated
    list of regions, e.g. ``australia-southeast1``). When unset, secrets are
    created with GCP's default automatic (global) replication — preserving the
    original behaviour. Setting it switches to user-managed replication in the
    given region(s), which is required under a ``gcp.resourceLocations`` org
    policy that forbids creating secrets in ``global`` (issue #149).
    """
    value = cfg.get_value('secrets.gcp.locations', None)
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = ','.join(str(v).strip() for v in value if str(v).strip())
    else:
        value = str(value).strip()
    return value or None


def _replication_create_flags() -> List[str]:
    """gcloud ``secrets create`` flags for the configured replication policy."""
    locations = _configured_secret_locations()
    if not locations:
        return []
    return ['--replication-policy=user-managed', f'--locations={locations}']


class GCPSecretBackend(SecretBackend):
    """Fetch DVC config from Google Cloud Secret Manager.
    
    Tries the Python client library first (works with service accounts
    and ``gcloud auth application-default login``).  Falls back to the
    ``gcloud`` CLI automatically, which also honours ``gcloud auth login``.
    
    Args:
        project: GCP project ID containing the secrets.
        prefix: Prefix for secret names (default: 'dvc-remote-').
    """
    
    def __init__(self, project: str, prefix: str = 'dvc-remote-'):
        self.project = project
        self.prefix = prefix
        self._client = None
        self._use_cli = False
    
    @staticmethod
    def gcloud_auth_status() -> Tuple[str, Optional[str]]:
        """Determine why gcloud authentication is or isn't usable.

        Three failures used to collapse into a bare ``None``: gcloud missing,
        the check timing out, and nobody being logged in. Only the last is
        fixed by ``gcloud auth login``, so telling all three to run it sends
        two-thirds of callers down a dead end.

        Returns:
            ``(status, account)`` where *status* is one of ``'ok'``,
            ``'missing'``, ``'timeout'``, ``'error'``, or
            ``'unauthenticated'``. *account* is the active account email
            when *status* is ``'ok'``, otherwise None.
        """
        gcloud = shutil.which('gcloud')
        if not gcloud:
            return 'missing', None
        try:
            result = subprocess.run(
                [gcloud, 'auth', 'list', '--filter=status:ACTIVE',
                 '--format=value(account)'],
                capture_output=True, text=True,
                timeout=GCLOUD_AUTH_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            return 'timeout', None
        except OSError:
            return 'error', None
        account = result.stdout.strip()
        return ('ok', account) if account else ('unauthenticated', None)

    @staticmethod
    def gcloud_auth_hint(status: str) -> str:
        """A message that matches the actual failure."""
        return {
            'missing': (
                "gcloud CLI not found on PATH. Install the Google Cloud SDK, "
                "or load the module providing it."
            ),
            'timeout': (
                f"gcloud did not respond within {GCLOUD_AUTH_TIMEOUT:.0f}s. "
                f"This usually means a cold start over a slow filesystem "
                f"rather than an authentication problem -- run "
                f"'gcloud auth list' once by hand, then retry."
            ),
            'error': "Could not run gcloud to check authentication.",
            'unauthenticated': (
                "No active GCP account. Run 'gcloud auth login' to "
                "authenticate, then retry."
            ),
        }.get(status, "GCP authentication unavailable.")

    @staticmethod
    def check_gcloud_authenticated() -> Optional[str]:
        """Active gcloud account email, or None if unusable for any reason.

        Retained for callers that only need a yes/no. Prefer
        :meth:`gcloud_auth_status` when the answer is shown to a user, so the
        message can name the actual problem.
        """
        status, account = GCPSecretBackend.gcloud_auth_status()
        return account if status == 'ok' else None

    @staticmethod
    def _has_adc_credentials() -> bool:
        """Check if Application Default Credentials exist locally.

        Returns ``True`` if an explicit credential file is set via
        ``GOOGLE_APPLICATION_CREDENTIALS`` or gcloud application-default
        credentials exist on disk.  Does **not** call any GCP API, so
        this is safe to call without risk of hanging.
        """
        explicit = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if explicit and Path(explicit).is_file():
            return True
        adc_path = Path.home() / '.config' / 'gcloud' / 'application_default_credentials.json'
        return adc_path.is_file()

    @property
    def client(self):
        """Lazy-load the Secret Manager client.

        Skips the Python library when no Application Default Credentials
        are available locally, to avoid a slow timeout on the GCE
        metadata server.
        """
        if self._client is None:
            if not self._has_adc_credentials():
                self._use_cli = True
                return None
            try:
                from google.cloud import secretmanager
            except ImportError:
                self._use_cli = True
                return None
            self._client = secretmanager.SecretManagerServiceClient(
                credentials=self._credentials(),
            )
        return self._client

    def _credentials(self):
        """ADC with the configured project attached as the quota project.

        Two different things are called "the project" here. ``self.project``
        names where the *secret* lives, and is baked into the resource path.
        The *quota* project says who gets billed for the API call, and lives
        on the credentials. They hold the same value for us, but nothing
        derives one from the other.

        Left unset, ``gcloud auth application-default login`` credentials
        carry no quota project, so google-auth prints a UserWarning on every
        single call about possible "quota exceeded"/"API not enabled" errors.
        Attaching it here fixes that for everyone at once, rather than each
        user having to run ``gcloud auth application-default
        set-quota-project`` on every machine they touch.

        Returns None if credentials can't be resolved, letting the client
        fall back to its own default lookup rather than failing outright.
        """
        try:
            import google.auth
        except ImportError:
            return None
        try:
            credentials, _ = google.auth.default()
        except Exception:
            return None
        # Service-account and metadata-server credentials bill their own
        # project and have no with_quota_project; only user credentials need
        # this, so treat its absence as "nothing to do".
        if self.project and hasattr(credentials, 'with_quota_project'):
            try:
                return credentials.with_quota_project(self.project)
            except Exception:
                return credentials
        return credentials
    
    def _get_secret_name(self, repo_name: str) -> str:
        """Build the full secret resource name."""
        secret_id = f"{self.prefix}{repo_name}"
        return f"projects/{self.project}/secrets/{secret_id}/versions/latest"
    
    def _get_secret_id(self, repo_name: str) -> str:
        """Build the secret ID (without version)."""
        return f"{self.prefix}{repo_name}"

    # -----------------------------------------------------------------
    # gcloud CLI helpers
    # -----------------------------------------------------------------

    @staticmethod
    def _require_gcloud() -> str:
        """Return path to gcloud or raise."""
        path = shutil.which('gcloud')
        if not path:
            raise SecretError(
                "Neither the google-cloud-secret-manager Python package "
                "nor the gcloud CLI is available.\n"
                "Install one of:\n"
                "  pip install google-cloud-secret-manager\n"
                "  https://cloud.google.com/sdk/docs/install"
            )
        return path

    def _cli_secret_exists(self, repo_name: str) -> bool:
        gcloud = self._require_gcloud()
        secret_id = self._get_secret_id(repo_name)
        result = subprocess.run(
            [gcloud, 'secrets', 'describe', secret_id,
             f'--project={self.project}'],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            return True
        if 'NOT_FOUND' in result.stderr:
            return False
        if 'PERMISSION_DENIED' in result.stderr:
            raise SecretError(
                f"Permission denied accessing secret '{secret_id}'.\n"
                f"Ensure you have secretmanager.secrets.get permission.\n"
                f"Error: {result.stderr.strip()}"
            )
        raise SecretError(f"gcloud error: {result.stderr.strip()}")

    def _cli_list_secrets(self) -> list:
        """List secret IDs matching the prefix via gcloud CLI."""
        gcloud = self._require_gcloud()
        result = subprocess.run(
            [
                gcloud, 'secrets', 'list',
                f'--project={self.project}',
                f'--filter=name:{self.prefix}',
                '--format=value(name)',
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise SecretError(f"gcloud error listing secrets: {result.stderr.strip()}")
        ids = []
        for line in result.stdout.strip().splitlines():
            # Full resource name: projects/<proj>/secrets/<id>
            secret_id = line.rsplit('/', 1)[-1]
            if secret_id.startswith(self.prefix):
                ids.append(secret_id[len(self.prefix):])
        return sorted(ids)

    def _create_secret_hint(self, secret_id: str) -> str:
        """Return the ``gcloud secrets create`` command shown in error hints.

        Includes the configured replication flags so the suggested manual
        command also works under a ``gcp.resourceLocations`` org policy (#149).
        """
        parts = [f'gcloud secrets create {secret_id}', f'--project={self.project}']
        parts.extend(_replication_create_flags())
        return ' '.join(parts)

    def _cli_set_secret(self, repo_name: str, content: str) -> None:
        """Create or update a secret via gcloud CLI."""
        import tempfile
        gcloud = self._require_gcloud()
        secret_id = self._get_secret_id(repo_name)

        # Write to a temp file so we can pipe into gcloud
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write(content)
            tmp_path = f.name

        try:
            # Create secret if it doesn't exist
            if not self._cli_secret_exists(repo_name):
                result = subprocess.run(
                    [gcloud, 'secrets', 'create', secret_id,
                     f'--project={self.project}',
                     *_replication_create_flags(),
                     f'--data-file={tmp_path}'],
                    capture_output=True, text=True,
                )
                if result.returncode != 0:
                    raise SecretError(
                        f"Failed to create secret '{secret_id}': "
                        f"{result.stderr.strip()}"
                    )
            else:
                # Add a new version
                result = subprocess.run(
                    [gcloud, 'secrets', 'versions', 'add', secret_id,
                     f'--project={self.project}',
                     f'--data-file={tmp_path}'],
                    capture_output=True, text=True,
                )
                if result.returncode != 0:
                    raise SecretError(
                        f"Failed to update secret '{secret_id}': "
                        f"{result.stderr.strip()}"
                    )
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def _cli_access_secret(self, repo_name: str) -> str:
        gcloud = self._require_gcloud()
        secret_id = self._get_secret_id(repo_name)
        result = subprocess.run(
            [gcloud, 'secrets', 'versions', 'access', 'latest',
             f'--secret={secret_id}', f'--project={self.project}'],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            return result.stdout
        if 'NOT_FOUND' in result.stderr:
            raise SecretError(
                f"Secret '{secret_id}' not found in project '{self.project}'.\n"
                f"Create it with: {self._create_secret_hint(secret_id)}"
            )
        if 'PERMISSION_DENIED' in result.stderr:
            raise SecretError(
                f"Permission denied accessing secret '{secret_id}'.\n"
                f"Ensure you have secretmanager.versions.access permission.\n"
                f"Error: {result.stderr.strip()}"
            )
        raise SecretError(f"gcloud error: {result.stderr.strip()}")

    # -----------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------
    
    def secret_exists(self, repo_name: str) -> bool:
        """Check if a secret exists for the given repository."""
        if self._use_cli:
            return self._cli_secret_exists(repo_name)

        try:
            from google.api_core import exceptions as gcp_exceptions
        except ImportError:
            self._use_cli = True
            return self._cli_secret_exists(repo_name)
        
        secret_path = f"projects/{self.project}/secrets/{self._get_secret_id(repo_name)}"

        client = self.client
        if client is None:
            return self._cli_secret_exists(repo_name)

        try:
            client.get_secret(request={"name": secret_path})
            return True
        except gcp_exceptions.NotFound:
            return False
        except gcp_exceptions.PermissionDenied:
            # ADC credentials may lack access — fall back to gcloud CLI
            # which honours `gcloud auth login`.
            self._use_cli = True
            return self._cli_secret_exists(repo_name)
        except Exception as e:
            raise SecretError(f"Error checking secret existence: {e}")
    
    def get_raw_config(self, repo_name: str) -> str:
        """Fetch raw DVC config content from GCP Secret Manager.
        
        Tries the Python client library first; falls back to gcloud CLI
        on PermissionDenied (common when authenticated via
        ``gcloud auth login`` rather than application-default credentials).
        
        Args:
            repo_name: Name of the repository.
            
        Returns:
            Raw config text to append to .dvc/config.local.
            
        Raises:
            SecretError: If the secret cannot be fetched.
        """
        if self._use_cli:
            return self._cli_access_secret(repo_name)

        try:
            from google.api_core import exceptions as gcp_exceptions
        except ImportError:
            self._use_cli = True
            return self._cli_access_secret(repo_name)
        
        secret_name = self._get_secret_name(repo_name)

        client = self.client
        if client is None:
            return self._cli_access_secret(repo_name)

        try:
            response = client.access_secret_version(
                request={"name": secret_name}
            )
        except gcp_exceptions.NotFound:
            raise SecretError(
                f"Secret '{self._get_secret_id(repo_name)}' not found in project '{self.project}'.\n"
                f"Create it with: {self._create_secret_hint(self._get_secret_id(repo_name))}"
            )
        except gcp_exceptions.PermissionDenied:
            # Fall back to gcloud CLI for the rest of this session
            self._use_cli = True
            return self._cli_access_secret(repo_name)
        except Exception as e:
            raise SecretError(f"Error fetching secret: {e}")
        
        # Return raw payload as text
        return response.payload.data.decode('utf-8')

    def list_secrets(self) -> list:
        """List secret IDs matching the prefix."""
        # Always use CLI — the Python client requires extra list permissions
        # that users often don't have, whereas gcloud auth login is sufficient.
        return self._cli_list_secrets()

    def set_secret(self, repo_name: str, content: str) -> None:
        """Create or update a secret with raw DVC INI content."""
        self._cli_set_secret(repo_name, content)
