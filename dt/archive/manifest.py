"""Archive manifest: schema, load/dump, and discovery.

A manifest is a YAML file committed to ``.dvc/archives/<name>.yaml`` that
records everything we need to verify and restore an archive without
contacting the storage backend. Each manifest describes one archived
DVC remote.

Schema version 1::

    version: 1
    archive_name: neochemo-2026-05
    source_remote: /g/data/a56/dvc/neochemo
    created_at: 2026-05-28T12:34:56+00:00
    created_by: jr9959
    git_ref: <sha>
    dt_version: 0.6.0
    backend: mdss
    backend_path: jr9959/archive/neochemo/neochemo-2026-05.tar
    tarball:
      filename: neochemo-2026-05.tar
      size_bytes: 1234567890
      sha256: abc...               # outer (un-streamed) tar sha256
      layout: nested-prefix        # vs future 'flat'
    contents:
      total_objects: 42137
      total_bytes: 1234500000
      compression: none            # none | gzip | zstd
      inner_tars:
        "00":
          filename: 00.tar
          size_bytes: ...
          sha256: ...
          n_objects: ...
        "01": ...
        ...
    extras_at_archive_time:        # informational only
      - {path: "some_file", size: 412}
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .. import utils
from ..errors import ArchiveError


MANIFEST_VERSION = 1
ARCHIVE_DIR_NAME = 'archives'
LAYOUT_NESTED_PREFIX = 'nested-prefix'


def archives_dir(repo_root: Optional[Path] = None) -> Path:
    """Path to the directory holding committed archive manifests.

    ``.dvc/archives/`` sits alongside ``.dvc/config`` so manifests are
    versioned with the project.
    """
    root = repo_root or utils.find_project_root()
    return root / '.dvc' / ARCHIVE_DIR_NAME


def manifest_path(name: str, repo_root: Optional[Path] = None) -> Path:
    """Resolve the on-disk manifest path for an archive name."""
    return archives_dir(repo_root) / f'{name}.yaml'


@dataclass
class InnerTar:
    """One per-prefix inner tarball inside the outer tar."""
    filename: str
    size_bytes: int
    sha256: str
    n_objects: int


@dataclass
class ExtraFile:
    """A file in the remote dir that was NOT archived."""
    path: str
    size: int


@dataclass
class ArchiveManifest:
    """In-memory representation of an archive manifest."""

    archive_name: str
    source_remote: str
    backend: str
    backend_path: str

    # Tarball metadata
    tarball_filename: str
    tarball_size_bytes: int
    tarball_sha256: str
    layout: str = LAYOUT_NESTED_PREFIX

    # Contents
    total_objects: int = 0
    total_bytes: int = 0
    compression: str = 'none'
    inner_tars: Dict[str, InnerTar] = field(default_factory=dict)
    extras_at_archive_time: List[ExtraFile] = field(default_factory=list)

    # Provenance
    created_at: str = ''
    created_by: str = ''
    git_ref: str = ''
    dt_version: str = ''

    version: int = MANIFEST_VERSION

    # ------------------------------------------------------------------ #
    # Serialisation
    # ------------------------------------------------------------------ #

    def to_dict(self) -> Dict[str, Any]:
        return {
            'version': self.version,
            'archive_name': self.archive_name,
            'source_remote': self.source_remote,
            'created_at': self.created_at,
            'created_by': self.created_by,
            'git_ref': self.git_ref,
            'dt_version': self.dt_version,
            'backend': self.backend,
            'backend_path': self.backend_path,
            'tarball': {
                'filename': self.tarball_filename,
                'size_bytes': self.tarball_size_bytes,
                'sha256': self.tarball_sha256,
                'layout': self.layout,
            },
            'contents': {
                'total_objects': self.total_objects,
                'total_bytes': self.total_bytes,
                'compression': self.compression,
                'inner_tars': {
                    prefix: asdict(inner)
                    for prefix, inner in sorted(self.inner_tars.items())
                },
            },
            'extras_at_archive_time': [asdict(e) for e in self.extras_at_archive_time],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ArchiveManifest':
        version = data.get('version', 1)
        if version != MANIFEST_VERSION:
            raise ArchiveError(
                f"Unsupported manifest version: {version} "
                f"(this dt understands version {MANIFEST_VERSION})"
            )

        tarball = data.get('tarball', {}) or {}
        contents = data.get('contents', {}) or {}
        inner_raw = contents.get('inner_tars', {}) or {}
        extras_raw = data.get('extras_at_archive_time', []) or []

        return cls(
            archive_name=data['archive_name'],
            source_remote=data['source_remote'],
            backend=data['backend'],
            backend_path=data['backend_path'],
            tarball_filename=tarball.get('filename', ''),
            tarball_size_bytes=int(tarball.get('size_bytes', 0)),
            tarball_sha256=tarball.get('sha256', ''),
            layout=tarball.get('layout', LAYOUT_NESTED_PREFIX),
            total_objects=int(contents.get('total_objects', 0)),
            total_bytes=int(contents.get('total_bytes', 0)),
            compression=contents.get('compression', 'none'),
            inner_tars={
                prefix: InnerTar(
                    filename=row['filename'],
                    size_bytes=int(row['size_bytes']),
                    sha256=row['sha256'],
                    n_objects=int(row['n_objects']),
                )
                for prefix, row in inner_raw.items()
            },
            extras_at_archive_time=[
                ExtraFile(path=e['path'], size=int(e['size']))
                for e in extras_raw
            ],
            created_at=data.get('created_at', ''),
            created_by=data.get('created_by', ''),
            git_ref=data.get('git_ref', ''),
            dt_version=data.get('dt_version', ''),
            version=version,
        )


def now_iso() -> str:
    """UTC timestamp in ISO-8601 with explicit offset."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec='seconds')


def save_manifest(manifest: ArchiveManifest, repo_root: Optional[Path] = None) -> Path:
    """Write ``manifest`` to ``.dvc/archives/<name>.yaml``.

    Returns the path written to. Does NOT git-add — let the caller decide.
    """
    target = manifest_path(manifest.archive_name, repo_root=repo_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, 'w') as f:
        yaml.safe_dump(
            manifest.to_dict(),
            f,
            default_flow_style=False,
            sort_keys=False,
        )
    return target


def load_manifest(name: str, repo_root: Optional[Path] = None) -> ArchiveManifest:
    """Load and parse a manifest by archive name."""
    path = manifest_path(name, repo_root=repo_root)
    if not path.exists():
        raise ArchiveError(
            f"No archive manifest found for '{name}' at {path}.\n"
            f"Run `dt remote archive list` to see known archives."
        )
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return ArchiveManifest.from_dict(data)


def list_manifests(repo_root: Optional[Path] = None) -> List[ArchiveManifest]:
    """Return all manifests in ``.dvc/archives/`` sorted by archive name."""
    d = archives_dir(repo_root)
    if not d.exists():
        return []
    manifests = []
    for p in sorted(d.glob('*.yaml')):
        try:
            with open(p) as f:
                data = yaml.safe_load(f) or {}
            manifests.append(ArchiveManifest.from_dict(data))
        except (ArchiveError, KeyError, ValueError, yaml.YAMLError):
            # Skip unparseable manifests; surfacing them is for `verify`.
            continue
    return manifests
