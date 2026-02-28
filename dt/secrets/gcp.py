"""GCP Secret Manager backend for DVC credentials."""

from typing import Dict, Optional

from .base import SecretBackend, SecretError


class GCPSecretBackend(SecretBackend):
    """Fetch DVC config from Google Cloud Secret Manager.
    
    Requires the google-cloud-secret-manager package and appropriate
    GCP credentials (via gcloud auth or service account).
    
    Args:
        project: GCP project ID containing the secrets.
        prefix: Prefix for secret names (default: 'dvc-remote-').
    """
    
    def __init__(self, project: str, prefix: str = 'dvc-remote-'):
        self.project = project
        self.prefix = prefix
        self._client = None
    
    @property
    def client(self):
        """Lazy-load the Secret Manager client."""
        if self._client is None:
            try:
                from google.cloud import secretmanager
            except ImportError:
                raise SecretError(
                    "google-cloud-secret-manager package not installed.\n"
                    "Install with: pip install google-cloud-secret-manager"
                )
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client
    
    def _get_secret_name(self, repo_name: str) -> str:
        """Build the full secret resource name."""
        secret_id = f"{self.prefix}{repo_name}"
        return f"projects/{self.project}/secrets/{secret_id}/versions/latest"
    
    def _get_secret_id(self, repo_name: str) -> str:
        """Build the secret ID (without version)."""
        return f"{self.prefix}{repo_name}"
    
    def secret_exists(self, repo_name: str) -> bool:
        """Check if a secret exists for the given repository."""
        try:
            from google.api_core import exceptions as gcp_exceptions
        except ImportError:
            raise SecretError(
                "google-cloud-secret-manager package not installed.\n"
                "Install with: pip install google-cloud-secret-manager"
            )
        
        secret_path = f"projects/{self.project}/secrets/{self._get_secret_id(repo_name)}"
        
        try:
            self.client.get_secret(request={"name": secret_path})
            return True
        except gcp_exceptions.NotFound:
            return False
        except gcp_exceptions.PermissionDenied as e:
            raise SecretError(
                f"Permission denied accessing secret '{self._get_secret_id(repo_name)}'.\n"
                f"Ensure you have secretmanager.secrets.get permission.\n"
                f"Error: {e}"
            )
        except Exception as e:
            raise SecretError(f"Error checking secret existence: {e}")
    
    def get_raw_config(self, repo_name: str) -> str:
        """Fetch raw DVC config content from GCP Secret Manager.
        
        The secret should contain INI-format DVC config sections:
        
            ['remote "bcarc-wts"']
                access_key_id = AKIAXXXXXXXX
                secret_access_key = xxxxx
                endpointurl = https://xxx.r2.cloudflarestorage.com
        
        Args:
            repo_name: Name of the repository.
            
        Returns:
            Raw config text to append to .dvc/config.local.
            
        Raises:
            SecretError: If the secret cannot be fetched.
        """
        try:
            from google.api_core import exceptions as gcp_exceptions
        except ImportError:
            raise SecretError(
                "google-cloud-secret-manager package not installed.\n"
                "Install with: pip install google-cloud-secret-manager"
            )
        
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
        except gcp_exceptions.PermissionDenied as e:
            raise SecretError(
                f"Permission denied accessing secret '{self._get_secret_id(repo_name)}'.\n"
                f"Ensure you have secretmanager.versions.access permission.\n"
                f"Error: {e}"
            )
        except Exception as e:
            raise SecretError(f"Error fetching secret: {e}")
        
        # Return raw payload as text
        return response.payload.data.decode('utf-8')
