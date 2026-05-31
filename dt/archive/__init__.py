"""Remote archival for DVC Tools.

Provides commands and machinery to archive DVC remotes (tar + ship to a
cold-storage backend like MDSS on NCI), verify them, restore from them,
and prune the on-disk remote once an archive is verified.
"""

from .manifest import ArchiveManifest, load_manifest, save_manifest, list_manifests
from .backends import ArchiveBackend, get_backend, register_backend
from . import operations

__all__ = [
    'ArchiveManifest',
    'ArchiveBackend',
    'get_backend',
    'register_backend',
    'load_manifest',
    'save_manifest',
    'list_manifests',
    'operations',
]
