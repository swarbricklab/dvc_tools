"""Archive signposts.

When ``dt remote archive prune`` deletes the on-disk DVC blob data,
it leaves an ``ARCHIVED.yaml`` file at the root of the source remote
directory. The signpost serves two audiences:

- **Humans** who `ls` the remote and want to know where the data went —
  there's a comment block at the top of the file explaining what
  happened and how to restore.
- **Tools** (``dt fetch``, ``dt pull``, ``dt status``, ``dt doctor``)
  that would otherwise return "data missing" errors when a user tries
  to use a pruned remote — they detect the signpost and explain that
  the data is archived, not lost.

The signpost is a strict *subset* of the canonical manifest, not a
copy. The full manifest lives in two places already (in-repo at
``.dt/archives/<name>.yaml`` and on the backend as
``<backend_dir>/<name>.manifest.yaml``); the signpost only needs to
point at those.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from ..errors import ArchiveError
from .manifest import ArchiveManifest, manifest_path


SIGNPOST_FILENAME = 'ARCHIVED.yaml'
SIGNPOST_VERSION = 1


@dataclass
class ArchiveSignpost:
    """Parsed contents of an ``ARCHIVED.yaml`` file."""
    archive_name: str
    backend: str
    backend_dir: str
    source_layout: str
    source_remote: str
    git_url: str
    git_ref: str
    manifest_in_repo: str
    pruned_at: str
    pruned_by: str
    path: Path           # where the signpost itself was found


def _comment_block() -> str:
    """Top-of-file explainer rendered into the signpost YAML."""
    return (
        "# This DVC remote was archived to cold storage by "
        "`dt remote archive prune`.\n"
        "# The blob data that previously lived here (files/md5/ for "
        "DVC v3, or top-level\n"
        "# hex prefix directories for DVC v2) has been moved to the "
        "backend below.\n"
        "#\n"
        "# To restore the data:\n"
        "#\n"
        "#   git clone <git_url> proj && cd proj\n"
        "#   dt remote archive restore <archive_name>\n"
        "#\n"
        "# `restore` defaults to putting the data back at `source_remote` "
        "(below); pass\n"
        "# `--to <path>` to restore somewhere else.\n"
        "#\n"
        "# Reference:\n"
        "#   https://github.com/swarbricklab/dvc_tools/blob/main/"
        "docs/archive.md\n"
    )


def write_signpost(
    source_remote: Path,
    manifest: ArchiveManifest,
    *,
    pruned_by: str,
    pruned_at: Optional[str] = None,
) -> Path:
    """Drop ``ARCHIVED.yaml`` at the root of ``source_remote``.

    Returns the path written. Atomic: writes to ``.tmp`` then renames.
    Idempotent — overwrites any existing signpost.
    """
    if pruned_at is None:
        pruned_at = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec='seconds')

    body: Dict[str, Any] = {
        'dt_archive_signpost': SIGNPOST_VERSION,
        'archive_name': manifest.archive_name,
        'backend': manifest.backend,
        'backend_dir': manifest.backend_dir,
        'source_layout': manifest.source_layout,
        'source_remote': manifest.source_remote,
        'git_url': manifest.git_url,
        'git_ref': manifest.git_ref,
        'manifest_in_repo': f'.dt/archives/{manifest.archive_name}.yaml',
        'pruned_at': pruned_at,
        'pruned_by': pruned_by,
    }

    target = source_remote / SIGNPOST_FILENAME
    tmp = target.with_suffix(target.suffix + '.tmp')
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp, 'w') as f:
        f.write(_comment_block())
        f.write('\n')
        yaml.safe_dump(body, f, default_flow_style=False, sort_keys=False)
    tmp.rename(target)
    return target


def detect(remote_path: Path) -> Optional[ArchiveSignpost]:
    """Look for ``ARCHIVED.yaml`` at the root of ``remote_path``.

    Returns the parsed :class:`ArchiveSignpost`, or ``None`` if the
    file is absent, unreadable, or doesn't carry the
    ``dt_archive_signpost`` marker. Never raises — the absence of a
    signpost is the common case and not an error.
    """
    try:
        candidate = Path(remote_path) / SIGNPOST_FILENAME
    except (TypeError, ValueError):
        return None
    if not candidate.is_file():
        return None
    try:
        with open(candidate) as f:
            data = yaml.safe_load(f) or {}
    except (yaml.YAMLError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get('dt_archive_signpost') != SIGNPOST_VERSION:
        return None
    return ArchiveSignpost(
        archive_name=str(data.get('archive_name', '')),
        backend=str(data.get('backend', '')),
        backend_dir=str(data.get('backend_dir', '')),
        source_layout=str(data.get('source_layout', '')),
        source_remote=str(data.get('source_remote', '')),
        git_url=str(data.get('git_url', '')),
        git_ref=str(data.get('git_ref', '')),
        manifest_in_repo=str(data.get('manifest_in_repo', '')),
        pruned_at=str(data.get('pruned_at', '')),
        pruned_by=str(data.get('pruned_by', '')),
        path=candidate,
    )


def detect_in_configured_remotes() -> 'list[ArchiveSignpost]':
    """Walk the project's configured DVC remotes; return signposts found
    at any locally-accessible remote root.

    Used by ``dt fetch`` / ``dt pull`` / ``dt status`` / ``dt doctor`` to
    notice an archived remote before they try to read blobs from it.

    Never raises — if listing the remotes fails for any reason (no
    ``.dvc/config``, malformed entries) we return an empty list and let
    the caller proceed.
    """
    try:
        from .. import remote as _remote
        remotes = _remote.list_remotes()
    except Exception:
        return []
    signposts: 'list[ArchiveSignpost]' = []
    for _name, url, _is_default in remotes:
        try:
            local = _remote.extract_local_path(url)
        except Exception:
            continue
        if not local:
            continue
        sp = detect(Path(local))
        if sp:
            signposts.append(sp)
    return signposts


def format_message(signpost: ArchiveSignpost) -> str:
    """Return a consistent user-facing message about an archived remote.

    Used by ``dt fetch`` / ``dt pull`` / ``dt status`` / ``dt doctor``
    so the wording is identical everywhere.
    """
    pruned_date = signpost.pruned_at[:10] if signpost.pruned_at else '(unknown)'
    lines = [
        f"⚠ This DVC remote was archived on {pruned_date} to "
        f"{signpost.backend}:{signpost.backend_dir}",
        f"  archive name:    {signpost.archive_name}",
        f"  source layout:   {signpost.source_layout}",
        f"  manifest in git: {signpost.manifest_in_repo}",
    ]
    if signpost.git_url:
        lines.append(f"  origin repo:     {signpost.git_url}")
    lines.append("")
    lines.append("To restore the data, run:")
    lines.append(f"  dt remote archive restore {signpost.archive_name}")
    lines.append("")
    return '\n'.join(lines)
