"""Shared low-level helpers used across auth submodules."""

from pathlib import Path
from typing import Dict, List, Optional, Set

from .endpoints import Endpoint


def _short_repo_name(url: str) -> str:
    """Short display name from a repository URL."""
    if not url:
        return 'unknown'
    name = url.rstrip('/').split('/')[-1]
    if name.endswith('.git'):
        name = name[:-4]
    return name


def _merge_children(target: Endpoint, source: Endpoint) -> None:
    """Merge children from *source* into *target*."""
    existing_keys = {c.key for c in target.children}
    for child in source.children:
        if child.key not in existing_keys:
            target.children.append(child)
            existing_keys.add(child.key)


def _apply_type_filter(
    endpoints: List[Endpoint],
    type_filter: Set[str],
) -> List[Endpoint]:
    """Keep only endpoints (and children) whose type is in *type_filter*."""
    filtered: List[Endpoint] = []
    for ep in endpoints:
        if ep.type in type_filter:
            # Also filter children
            ep.children = [c for c in ep.children if c.type in type_filter]
            filtered.append(ep)
        else:
            # Even if parent is excluded, promote matching children
            for child in ep.children:
                if child.type in type_filter:
                    # Promote child to top level, note its origin
                    child.source = f"{child.source} (via {ep.source})"
                    filtered.append(child)
    return filtered


def _extract_repo_name_from_url(url: str) -> Optional[str]:
    """Extract repository name from a git URL.

    Handles:
    - git@github.com:org/repo.git
    - https://github.com/org/repo.git
    - https://github.com/org/repo
    """
    import re

    # SSH format: git@host:org/repo.git
    ssh_match = re.match(r'^git@[^:]+:(?:[^/]+/)?([^/]+?)(?:\.git)?$', url)
    if ssh_match:
        return ssh_match.group(1)

    # HTTPS format: https://host/org/repo.git
    https_match = re.match(r'^https?://[^/]+/(?:[^/]+/)?([^/]+?)(?:\.git)?$', url)
    if https_match:
        return https_match.group(1)

    return None


def _get_import_urls(repo_path: Optional[Path] = None) -> Dict[str, List[str]]:
    """Get unique import source URLs from .dvc files.

    This is a lightweight scan that just extracts URLs without cloning.
    Shared between _discover_import_sources() and credentials code.
    """
    import yaml

    search_root = repo_path or Path.cwd()
    import_urls: Dict[str, List[str]] = {}

    for dvc_file in sorted(search_root.rglob('*.dvc')):
        # Skip .dvc directory itself and .dt/tmp clones
        rel = str(dvc_file.relative_to(search_root))
        if rel.startswith('.dvc') or rel.startswith('.dt'):
            continue

        try:
            with open(dvc_file) as f:
                data = yaml.safe_load(f)
        except Exception:
            continue

        if not isinstance(data, dict):
            continue

        deps = data.get('deps')
        if not deps or not isinstance(deps, list):
            continue

        for dep in deps:
            repo = dep.get('repo') if isinstance(dep, dict) else None
            if repo and isinstance(repo, dict):
                url = repo.get('url')
                if url:
                    import_urls.setdefault(url, []).append(rel)

    return import_urls
