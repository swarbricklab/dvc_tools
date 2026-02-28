"""Base classes and types for secret manager backends."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional


class SecretError(Exception):
    """Raised when secret operations fail."""
    pass


@dataclass
class S3Credentials:
    """S3/S3-compatible storage credentials.
    
    Attributes:
        access_key_id: AWS access key ID or equivalent.
        secret_access_key: AWS secret access key or equivalent.
        endpoint_url: Custom endpoint URL for S3-compatible storage
            (e.g., Cloudflare R2, MinIO). None for AWS S3.
        region: AWS region. None to use default or 'auto'.
    """
    access_key_id: str
    secret_access_key: str
    endpoint_url: Optional[str] = None
    region: Optional[str] = None
    
    def to_dvc_config(self) -> Dict[str, str]:
        """Convert to DVC config key-value pairs.
        
        Returns:
            Dictionary of config keys and values for .dvc/config.local.
        """
        config = {
            'access_key_id': self.access_key_id,
            'secret_access_key': self.secret_access_key,
        }
        if self.endpoint_url:
            config['endpointurl'] = self.endpoint_url
        if self.region:
            config['region'] = self.region
        return config


class SecretBackend(ABC):
    """Abstract interface for secret managers.
    
    Implementations should fetch credentials from a specific secret
    manager (GCP Secret Manager, AWS Secrets Manager, etc.).
    
    Secrets can contain either:
    1. Raw INI config text (appended directly to .dvc/config.local)
    2. YAML with structured credentials (converted to DVC config)
    """
    
    @abstractmethod
    def get_raw_config(self, repo_name: str) -> str:
        """Fetch raw config content for a repository.
        
        The secret should contain INI-format config sections that can
        be appended directly to .dvc/config.local:
        
            ['remote "cloud"']
                access_key_id = AKIA...
                secret_access_key = ...
                endpointurl = https://...
        
        Args:
            repo_name: Name of the repository (e.g., 'neochemo').
            
        Returns:
            Raw config text to append to .dvc/config.local.
            
        Raises:
            SecretError: If the secret cannot be fetched.
        """
        ...
    
    @abstractmethod
    def secret_exists(self, repo_name: str) -> bool:
        """Check if a secret exists for the given repository.
        
        Args:
            repo_name: Name of the repository.
            
        Returns:
            True if the secret exists, False otherwise.
        """
        ...
