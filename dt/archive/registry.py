"""Central archive register.

A lightweight, optional directory of YAML files — one per archive
recorded across all projects on the host (or team-shared storage).
The per-project manifest under ``.dvc/archives/<name>.yaml`` remains
canonical; the register is a derived index that lets you ask "what
archives exist?" without scanning every repo.

Configuration::

    dt config set archive.registry_path /g/data/<project>/dt-archives/registry

When unset, the register is silently disabled — record/list/sync are
no-ops.

Each entry filename is ``<project-slug>__<archive-name>.yaml`` so
``ls`` groups naturally by project. The project slug is the repo-root
basename plus a short hash of the absolute path, which is stable across
machines that mount the same path.
"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .. import config as cfg
from .. import utils
from ..errors import ArchiveError
from .manifest import ArchiveManifest, list_manifests


REGISTRY_ENTRY_VERSION = 1


@dataclass
class RegistryStatus:
    """Lifecycle markers for an archive entry."""
    verified_at: Optional[str] = None
    verified_ok: Optional[bool] = None
    pruned_at: Optional[str] = None


@dataclass
class RegistryEntry:
    """One row in the central register."""
    project_slug: str
    project_path: str
    project_name: str
    archive_name: str
    manifest_path: str  # absolute path on the host that recorded it
    backend: str
    backend_dir: str
    total_objects: int
    total_size_bytes: int
    inner_tars_count: int
    compression: str
    created_at: str
    created_by: str
    git_ref: str
    dt_version: str
    status: RegistryStatus = field(default_factory=RegistryStatus)
    version: int = REGISTRY_ENTRY_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            'version': self.version,
            'project_slug': self.project_slug,
            'project_path': self.project_path,
            'project_name': self.project_name,
            'archive_name': self.archive_name,
            'manifest_path': self.manifest_path,
            'backend': self.backend,
            'backend_dir': self.backend_dir,
            'total_objects': self.total_objects,
            'total_size_bytes': self.total_size_bytes,
            'inner_tars_count': self.inner_tars_count,
            'compression': self.compression,
            'created_at': self.created_at,
            'created_by': self.created_by,
            'git_ref': self.git_ref,
            'dt_version': self.dt_version,
            'status': asdict(self.status),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RegistryEntry':
        status_raw = data.get('status', {}) or {}
        return cls(
            project_slug=data['project_slug'],
            project_path=data['project_path'],
            project_name=data['project_name'],
            archive_name=data['archive_name'],
            manifest_path=data['manifest_path'],
            backend=data['backend'],
            backend_dir=data['backend_dir'],
            total_objects=int(data.get('total_objects', 0)),
            total_size_bytes=int(data.get('total_size_bytes', 0)),
            inner_tars_count=int(data.get('inner_tars_count', 0)),
            compression=data.get('compression', 'none'),
            created_at=data.get('created_at', ''),
            created_by=data.get('created_by', ''),
            git_ref=data.get('git_ref', ''),
            dt_version=data.get('dt_version', ''),
            status=RegistryStatus(
                verified_at=status_raw.get('verified_at'),
                verified_ok=status_raw.get('verified_ok'),
                pruned_at=status_raw.get('pruned_at'),
            ),
            version=int(data.get('version', REGISTRY_ENTRY_VERSION)),
        )


# --------------------------------------------------------------------------- #
# Resolution
# --------------------------------------------------------------------------- #

def registry_path() -> Optional[Path]:
    """Where the central register lives, or None if unconfigured."""
    raw = cfg.get_value('archive.registry_path')
    if not raw:
        return None
    return Path(str(raw)).expanduser()


def project_slug(repo_root: Path) -> str:
    """Stable slug for a project: ``<basename>--<6-char-hash-of-abs-path>``.

    Same path on a different machine produces the same slug, so two
    workers on the same project don't get duplicate entries.
    """
    abs_str = str(repo_root.resolve())
    digest = hashlib.sha256(abs_str.encode()).hexdigest()[:6]
    return f"{repo_root.name}--{digest}"


def entry_filename(slug: str, archive_name: str) -> str:
    return f"{slug}__{archive_name}.yaml"


def _entry_path(slug: str, archive_name: str) -> Optional[Path]:
    base = registry_path()
    if base is None:
        return None
    return base / entry_filename(slug, archive_name)


def _ensure_registry_dir() -> Optional[Path]:
    base = registry_path()
    if base is None:
        return None
    try:
        base.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(
            f"warning: archive registry dir {base} not writable: {e}",
            file=sys.stderr,
        )
        return None
    return base


# --------------------------------------------------------------------------- #
# Entry construction
# --------------------------------------------------------------------------- #

def entry_from_manifest(
    manifest: ArchiveManifest, repo_root: Path,
) -> RegistryEntry:
    """Build a :class:`RegistryEntry` from a loaded manifest."""
    from .manifest import manifest_path as _mp
    return RegistryEntry(
        project_slug=project_slug(repo_root),
        project_path=str(repo_root.resolve()),
        project_name=repo_root.name,
        archive_name=manifest.archive_name,
        manifest_path=str(_mp(manifest.archive_name, repo_root=repo_root)),
        backend=manifest.backend,
        backend_dir=manifest.backend_dir,
        total_objects=manifest.total_objects,
        total_size_bytes=sum(i.size_bytes for i in manifest.inner_tars.values()),
        inner_tars_count=len(manifest.inner_tars),
        compression=manifest.compression,
        created_at=manifest.created_at,
        created_by=manifest.created_by,
        git_ref=manifest.git_ref,
        dt_version=manifest.dt_version,
    )


# --------------------------------------------------------------------------- #
# Read / write
# --------------------------------------------------------------------------- #

def write_entry(entry: RegistryEntry) -> Optional[Path]:
    """Write/overwrite an entry in the register. No-op if unconfigured."""
    base = _ensure_registry_dir()
    if base is None:
        return None
    target = base / entry_filename(entry.project_slug, entry.archive_name)
    tmp = target.with_suffix(target.suffix + '.tmp')
    try:
        with open(tmp, 'w') as f:
            yaml.safe_dump(
                entry.to_dict(), f, default_flow_style=False, sort_keys=False,
            )
        tmp.rename(target)
    except OSError as e:
        print(
            f"warning: failed to update archive registry {target}: {e}",
            file=sys.stderr,
        )
        return None
    return target


def read_entry(slug: str, archive_name: str) -> Optional[RegistryEntry]:
    path = _entry_path(slug, archive_name)
    if path is None or not path.is_file():
        return None
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return RegistryEntry.from_dict(data)
    except (yaml.YAMLError, KeyError, ValueError):
        return None


def list_entries() -> List[RegistryEntry]:
    """Return every entry in the register, sorted by project then name."""
    base = registry_path()
    if base is None or not base.is_dir():
        return []
    entries: List[RegistryEntry] = []
    for p in sorted(base.glob('*.yaml')):
        try:
            with open(p) as f:
                data = yaml.safe_load(f) or {}
            entries.append(RegistryEntry.from_dict(data))
        except (yaml.YAMLError, KeyError, ValueError, OSError):
            continue
    entries.sort(key=lambda e: (e.project_slug, e.archive_name))
    return entries


def delete_entry(slug: str, archive_name: str) -> bool:
    """Remove an entry from the register if it exists."""
    path = _entry_path(slug, archive_name)
    if path is None or not path.is_file():
        return False
    try:
        path.unlink()
        return True
    except OSError:
        return False


# --------------------------------------------------------------------------- #
# Lifecycle hooks (called from operations.py)
# --------------------------------------------------------------------------- #

def record_created(manifest: ArchiveManifest, repo_root: Path) -> Optional[Path]:
    """Hook for create/deposit: write a fresh entry for ``manifest``."""
    if registry_path() is None:
        return None
    entry = entry_from_manifest(manifest, repo_root)
    return write_entry(entry)


def record_verified(
    archive_name: str, repo_root: Path, ok: bool, at_iso: str,
) -> Optional[Path]:
    """Hook for verify: update the verification timestamp."""
    if registry_path() is None:
        return None
    slug = project_slug(repo_root)
    entry = read_entry(slug, archive_name)
    if entry is None:
        return None
    entry.status.verified_at = at_iso
    entry.status.verified_ok = ok
    return write_entry(entry)


def record_pruned(
    archive_name: str, repo_root: Path, at_iso: str,
) -> Optional[Path]:
    """Hook for prune: stamp pruned_at on the entry."""
    if registry_path() is None:
        return None
    slug = project_slug(repo_root)
    entry = read_entry(slug, archive_name)
    if entry is None:
        return None
    entry.status.pruned_at = at_iso
    return write_entry(entry)


# --------------------------------------------------------------------------- #
# sync
# --------------------------------------------------------------------------- #

def sync_from_roots(roots: List[Path]) -> Dict[str, int]:
    """Rebuild register entries from every manifest under each root.

    Walks ``<root>/.dt/archives/*.yaml`` (and legacy
    ``<root>/.dvc/archives/*.yaml``) for each root, reading the manifest
    and overwriting the corresponding register entry. Useful for
    bootstrapping the register across an existing fleet of projects or
    recovering after manual deletes.

    Returns a small stats dict for the caller to surface.
    """
    written = 0
    skipped = 0
    for root in roots:
        root = Path(root).expanduser().resolve()
        has_archives = (
            (root / '.dt' / 'archives').is_dir()
            or (root / '.dvc' / 'archives').is_dir()
        )
        if not has_archives:
            skipped += 1
            continue
        try:
            manifests = list_manifests(repo_root=root)
        except ArchiveError:
            skipped += 1
            continue
        for m in manifests:
            entry = entry_from_manifest(m, root)
            if write_entry(entry) is not None:
                written += 1
    return {'written': written, 'skipped_roots': skipped}
