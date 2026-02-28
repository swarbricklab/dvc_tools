"""Secret manager backends for DVC remote credentials.

This package provides a common interface for fetching DVC config
from various secret managers (GCP Secret Manager, AWS Secrets Manager, etc.)
and appending it to .dvc/config.local.
"""

from .base import SecretBackend, SecretError
from .gcp import GCPSecretBackend

__all__ = [
    'SecretBackend',
    'SecretError',
    'GCPSecretBackend',
]
