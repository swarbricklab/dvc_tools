"""GCP Secret Manager backend for DVC credentials."""

import shutil
import subprocess
from typing import Dict, Optional

from .base import SecretBackend, SecretError


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
    
    @property
    def client(self):
        """Lazy-load the Secret Manager client."""
        if self._client is None:
            try:
                from google.cloud import secretmanager
            except ImportError:
                self._use_cli = True
                return None
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client
    
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
                f"Create it with: gcloud secrets create {secret_id} --project={self.project}"
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
        
        try:
            self.client.get_secret(request={"name": secret_path})
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
        
        try:
            response = self.client.access_secret_version(
                request={"name": secret_name}
            )
        except gcp_exceptions.NotFound:
            raise SecretError(
                f"Secret '{self._get_secret_id(repo_name)}' not found in project '{self.project}'.\n"
                f"Create it with: gcloud secrets create {self._get_secret_id(repo_name)} --project={self.project}"
            )
        except gcp_exceptions.PermissionDenied:
            # Fall back to gcloud CLI for the rest of this session
            self._use_cli = True
            return self._cli_access_secret(repo_name)
        except Exception as e:
            raise SecretError(f"Error fetching secret: {e}")
        
        # Return raw payload as text
        return response.payload.data.decode('utf-8')
