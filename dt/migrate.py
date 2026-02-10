"""Migrate DVC v2 .dvc files to v3 format.

DVC v3 introduces two key changes from v2:

1. **Hash field**: v3 .dvc files include an explicit ``hash: md5`` field
   in each output entry. v2 files omit this field.

2. **Hash algorithm**: v2 used ``md5-dos2unix`` which normalised CRLF to LF
   before hashing. v3 hashes files "as-is" via plain ``md5``.
   For binary files and files created on Unix (no CRLF), the hashes are
   identical. Only text files with Windows-style line endings will differ.

3. **Cache layout**: v2 stored cache files at ``<cache>/XX/YYYYYY...``.
   v3 stores them under ``<cache>/files/md5/XX/YYYYYY...``.
   The ``dvc cache migrate`` command handles relocating cache data.
   This module focuses on updating ``.dvc`` file metadata.

Usage flow:
    1. Run ``dvc cache migrate`` first to relocate cache data to v3 layout.
    2. Run ``dt migrate`` to update ``.dvc`` files one-at-a-time, including
       imports that ``dvc cache migrate --dvc-files`` may choke on.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta
from dvc_data.hashfile.tree import Tree

from . import cache_ops
from .errors import MigrateError
from .utils import md5_file, recompute_dvc_md5


# =============================================================================
# .dvc file analysis
# =============================================================================

def is_v3(dvc_data: Dict[str, Any]) -> bool:
    """Check whether a parsed .dvc file is already in v3 format.

    v3 files have an explicit ``hash: md5`` key in every output entry.

    Args:
        dvc_data: Parsed YAML content of a ``.dvc`` file.

    Returns:
        True if the file is already v3 format.
    """
    outs = dvc_data.get('outs', [])
    if not outs:
        return True
    return all('hash' in out for out in outs)


def is_import(dvc_data: Dict[str, Any]) -> bool:
    """Check whether a parsed .dvc file represents a repo import.

    Imports have a ``deps`` list containing entries with a ``repo`` key.

    Args:
        dvc_data: Parsed YAML content of a ``.dvc`` file.

    Returns:
        True if this is a repo import.
    """
    deps = dvc_data.get('deps', [])
    return any('repo' in dep for dep in deps)


def parse_dvc_file(path: Path) -> Dict[str, Any]:
    """Parse a .dvc file and return its YAML content.

    Args:
        path: Path to the ``.dvc`` file.

    Returns:
        Parsed YAML dictionary.

    Raises:
        MigrateError: If the file cannot be read or parsed.
    """
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        raise MigrateError(f"Cannot read {path}: {e}")
    return data


def write_dvc_file(path: Path, data: Dict[str, Any]) -> None:
    """Write a .dvc file, preserving DVC's key ordering conventions.

    Args:
        path: Path to the ``.dvc`` file.
        data: Dictionary content to write.

    Raises:
        MigrateError: If the file cannot be written.
    """
    try:
        with open(path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    except Exception as e:
        raise MigrateError(f"Cannot write {path}: {e}")


# =============================================================================
# Cache helpers
# =============================================================================

def find_in_cache(md5: str, cache_root: Path) -> Optional[Path]:
    """Find a file in the cache, checking both v3 and v2 layouts.

    Args:
        md5: Hash value (with optional ``.dir`` suffix).
        cache_root: Root of the DVC cache.

    Returns:
        Path to the cached file, or None if not found.
    """
    return cache_ops.find_source_file(md5, cache_root)


def ensure_v3_cache_entry(
    md5: str,
    content: bytes,
    cache_root: Path,
    is_dir: bool = False,
) -> Path:
    """Write content to the v3 cache location if not already present.

    Args:
        md5: The MD5 hash of the content (without ``.dir`` suffix).
        content: The file content.
        cache_root: Root of the DVC cache.
        is_dir: Whether this is a .dir manifest.

    Returns:
        Path to the v3 cache file.
    """
    suffix = '.dir' if is_dir else ''
    dest = cache_ops.get_cache_file_path(
        md5 + suffix, cache_root, use_v3_layout=True,
    )
    if not dest.exists():
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
    return dest


# =============================================================================
# Single-file migration
# =============================================================================

def migrate_single_output(
    out: Dict[str, Any],
    cache_root: Path,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Migrate a single non-directory output entry to v3 format.

    Re-hashes the cache file with plain md5 (no dos2unix normalisation) and
    updates the output dict with the new hash and ``hash: md5``.

    Only reads from the cache — never from the workspace, which may be dirty.

    Args:
        out: The output entry dict from the .dvc file.
        cache_root: DVC cache root.
        verbose: Print progress.

    Returns:
        Updated output dict.

    Raises:
        MigrateError: If the file cannot be found in the cache.
    """
    old_md5 = out.get('md5', '')
    rel_path = out.get('path', '')

    cached = find_in_cache(old_md5, cache_root)
    if cached is None:
        raise MigrateError(
            f"Cannot find {rel_path} in cache "
            f"(hash: {old_md5[:12]}…). Run 'dvc cache migrate' first "
            f"to ensure cache data is available."
        )

    new_md5 = md5_file(cached)
    changed = (new_md5 != old_md5)

    if verbose:
        if changed:
            print(f"  {rel_path}: hash changed {old_md5[:12]}… → {new_md5[:12]}…")
        else:
            print(f"  {rel_path}: hash unchanged, adding hash field")

    # Ensure cache has the v3 entry when hash changed
    if changed:
        _ensure_v3_cache_copy(old_md5, new_md5, cache_root)

    out['md5'] = new_md5
    out['hash'] = 'md5'
    return out


def _ensure_v3_cache_copy(
    old_md5: str,
    new_md5: str,
    cache_root: Path,
) -> None:
    """Ensure a file exists in the v3 cache under its new hash.

    When the hash changes (CRLF → LF normalisation difference), the file
    already exists in the cache under the old hash. We link it under the
    new hash in the v3 layout.

    Args:
        old_md5: Original v2 hash.
        new_md5: New v3 hash.
        cache_root: Root of the DVC cache.
    """
    v3_dest = cache_ops.get_cache_file_path(new_md5, cache_root, use_v3_layout=True)
    if v3_dest.exists():
        return

    source = cache_ops.find_source_file(old_md5, cache_root)
    if source is not None:
        v3_dest.parent.mkdir(parents=True, exist_ok=True)
        cache_ops.link_file(source, v3_dest)


# =============================================================================
# Directory migration
# =============================================================================

def migrate_directory_output(
    out: Dict[str, Any],
    cache_root: Path,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Migrate a directory output entry to v3 format.

    1. Reads the old ``.dir`` manifest from the cache.
    2. For each child file, re-hashes from cache with plain md5.
    3. Builds a new manifest with updated hashes.
    4. Computes the new manifest hash and writes it to the v3 cache.
    5. Updates the output dict.

    Only reads from the cache — never from the workspace, which may be dirty.

    Args:
        out: The output entry dict from the .dvc file.
        cache_root: DVC cache root.
        verbose: Print progress.

    Returns:
        Updated output dict.

    Raises:
        MigrateError: If the .dir manifest or child files cannot be found.
    """
    old_md5 = out.get('md5', '')  # includes .dir suffix
    rel_path = out.get('path', '')

    # Read the old .dir manifest from cache
    dir_cache_path = find_in_cache(old_md5, cache_root)
    if dir_cache_path is None:
        raise MigrateError(
            f"Cannot find .dir manifest for {rel_path} in cache "
            f"(hash: {old_md5}). Run 'dvc cache migrate' first to "
            f"ensure cache data is in v3 layout."
        )

    old_manifest = json.loads(dir_cache_path.read_bytes())

    # Re-hash each child file from cache, building a new Tree
    new_tree = Tree()
    any_changed = False

    for entry in old_manifest:
        child_md5 = entry['md5']
        child_relpath = entry['relpath']

        cached = find_in_cache(child_md5, cache_root)
        if cached is None:
            raise MigrateError(
                f"Cannot find {rel_path}/{child_relpath} in cache "
                f"(hash: {child_md5[:12]}…). Run 'dvc cache migrate' "
                f"first to ensure cache data is available."
            )

        new_child_md5 = md5_file(cached)

        if new_child_md5 != child_md5:
            any_changed = True
            if verbose:
                print(
                    f"  {rel_path}/{child_relpath}: "
                    f"{child_md5[:12]}… → {new_child_md5[:12]}…"
                )
            # Ensure child is in v3 cache under new hash
            _ensure_v3_cache_copy(child_md5, new_child_md5, cache_root)

        key = tuple(child_relpath.split('/'))
        new_tree.add(key, Meta(), HashInfo('md5', new_child_md5))

    # Compute new manifest hash via Tree.digest()
    new_tree.digest()
    new_dir_hash = new_tree.hash_info.value  # already has .dir suffix
    new_dir_md5 = new_dir_hash.replace('.dir', '')

    if verbose:
        if any_changed or new_dir_hash != old_md5:
            print(
                f"  {rel_path} (dir): manifest "
                f"{old_md5.replace('.dir', '')[:12]}… → {new_dir_md5[:12]}…"
            )
        else:
            print(f"  {rel_path} (dir): hashes unchanged, adding hash field")

    # Write new manifest to v3 cache
    ensure_v3_cache_entry(
        new_dir_md5, new_tree.as_bytes(), cache_root, is_dir=True,
    )

    out['md5'] = new_dir_hash
    out['hash'] = 'md5'
    return out


# =============================================================================
# Top-level .dvc file checksum
# =============================================================================

# =============================================================================
# Main migration logic
# =============================================================================

def analyse_dvc_file(
    dvc_path: Path,
    cache_root: Optional[Path] = None,
) -> Dict[str, Any]:
    """Analyse a .dvc file and report its migration status.

    Args:
        dvc_path: Path to the ``.dvc`` file.
        cache_root: DVC cache root (for checking data availability).

    Returns:
        Dict with analysis results:
            - ``path``: The .dvc file path
            - ``is_v3``: Whether already v3 format
            - ``is_import``: Whether this is a repo import
            - ``outputs``: List of output analysis dicts
            - ``can_migrate``: Whether migration is possible
            - ``reason``: Reason if migration is not possible
    """
    data = parse_dvc_file(dvc_path)

    result = {
        'path': str(dvc_path),
        'is_v3': is_v3(data),
        'is_import': is_import(data),
        'outputs': [],
        'can_migrate': True,
        'reason': None,
    }

    if result['is_v3']:
        result['reason'] = 'already v3'
        return result

    outs = data.get('outs', [])
    for out in outs:
        md5 = out.get('md5', '')
        rel_path = out.get('path', '')
        is_dir = md5.endswith('.dir')

        out_info: Dict[str, Any] = {
            'path': rel_path,
            'md5': md5,
            'is_dir': is_dir,
            'in_cache': False,
        }

        if cache_root and md5:
            out_info['in_cache'] = find_in_cache(md5, cache_root) is not None

        if not out_info['in_cache']:
            result['can_migrate'] = False
            result['reason'] = (
                f"Output {rel_path} not found in cache "
                f"(hash: {md5[:12]}…)"
            )

        result['outputs'].append(out_info)

    return result


def migrate_dvc_file(
    dvc_path: Path,
    cache_root: Optional[Path] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Migrate a single .dvc file from v2 to v3 format.

    This updates the ``.dvc`` file in place (unless ``dry_run=True``):
    1. Re-hashes each output with plain md5 (no dos2unix normalisation).
    2. Adds ``hash: md5`` to each output.
    3. Ensures the new hash is present in the v3 cache layout.
    4. Recomputes the top-level ``md5`` checksum (for imports).

    Args:
        dvc_path: Path to the ``.dvc`` file.
        cache_root: DVC cache root. Auto-detected if not provided.
        dry_run: If True, report what would change without modifying files.
        verbose: Print detailed progress.

    Returns:
        Dict with migration results:
            - ``path``: The .dvc file path
            - ``status``: 'migrated', 'skipped', or 'error'
            - ``is_import``: Whether this is a repo import
            - ``changes``: List of changes made
            - ``error``: Error message if status is 'error'

    Raises:
        MigrateError: If a non-recoverable error occurs.
    """
    dvc_path = dvc_path.resolve()

    result: Dict[str, Any] = {
        'path': str(dvc_path),
        'status': 'skipped',
        'is_import': False,
        'changes': [],
        'error': None,
    }

    data = parse_dvc_file(dvc_path)
    result['is_import'] = is_import(data)

    if is_v3(data):
        if verbose:
            print(f"  {dvc_path.name}: already v3, skipping")
        return result

    # Auto-detect cache root
    if cache_root is None:
        cache_root = _detect_cache_root()

    if cache_root is None:
        raise MigrateError(
            f"Cannot migrate {dvc_path.name}: no cache found. "
            f"Use --cache-root or ensure you are in a DVC project."
        )

    outs = data.get('outs', [])
    for out in outs:
        md5 = out.get('md5', '')
        rel_path = out.get('path', '')
        is_dir = md5.endswith('.dir')

        old_md5 = md5

        if is_dir:
            out = migrate_directory_output(
                out, cache_root, verbose=verbose,
            )
        else:
            out = migrate_single_output(
                out, cache_root, verbose=verbose,
            )

        new_md5 = out['md5']
        if new_md5 != old_md5:
            result['changes'].append({
                'path': rel_path,
                'old_md5': old_md5,
                'new_md5': new_md5,
            })
        else:
            result['changes'].append({
                'path': rel_path,
                'old_md5': old_md5,
                'new_md5': new_md5,
                'note': 'hash unchanged, added hash field',
            })

    # Recompute top-level md5 for imports
    if 'md5' in data:
        data = recompute_dvc_md5(data)

    if dry_run:
        result['status'] = 'would_migrate'
        if verbose:
            print(f"  {dvc_path.name}: would migrate (dry run)")
    else:
        write_dvc_file(dvc_path, data)
        result['status'] = 'migrated'
        if verbose:
            print(f"  {dvc_path.name}: migrated to v3")

    return result


def migrate_project(
    targets: Optional[List[str]] = None,
    dry_run: bool = False,
    verbose: bool = False,
    cache_root: Optional[Path] = None,
) -> Dict[str, Any]:
    """Migrate .dvc files in the current project from v2 to v3 format.

    Without targets, finds and processes all ``.dvc`` files in the project.
    With targets, processes only the specified files or directories.

    Args:
        targets: Optional list of ``.dvc`` file paths or directories.
        dry_run: Report changes without modifying files.
        verbose: Print detailed progress.
        cache_root: DVC cache root. Auto-detected if not provided.

    Returns:
        Dict with overall migration summary:
            - ``total``: Number of .dvc files found
            - ``migrated``: Number successfully migrated
            - ``skipped``: Number already v3
            - ``errors``: Number that failed
            - ``files``: List of per-file result dicts
    """
    if cache_root is None:
        cache_root = _detect_cache_root()

    # Collect .dvc files
    dvc_files = _collect_dvc_files(targets)

    summary: Dict[str, Any] = {
        'total': len(dvc_files),
        'migrated': 0,
        'skipped': 0,
        'errors': 0,
        'files': [],
    }

    for dvc_path in dvc_files:
        if verbose:
            print(f"\n{dvc_path}:")

        try:
            file_result = migrate_dvc_file(
                dvc_path,
                cache_root=cache_root,
                dry_run=dry_run,
                verbose=verbose,
            )
        except MigrateError as e:
            file_result = {
                'path': str(dvc_path),
                'status': 'error',
                'is_import': False,
                'changes': [],
                'error': str(e),
            }
            if verbose:
                print(f"  ERROR: {e}")

        summary['files'].append(file_result)

        if file_result['status'] == 'migrated':
            summary['migrated'] += 1
        elif file_result['status'] == 'would_migrate':
            summary['migrated'] += 1  # count as "would migrate" in dry run
        elif file_result['status'] == 'error':
            summary['errors'] += 1
        else:
            summary['skipped'] += 1

    return summary


# =============================================================================
# Internal helpers
# =============================================================================

def _detect_cache_root() -> Optional[Path]:
    """Auto-detect the DVC cache root directory.

    Returns:
        Path to the cache root, or None if not in a DVC project.
    """
    try:
        from dvc.repo import Repo
        repo = Repo()
        cache_path = Path(repo.cache.local.path)
        # The Repo gives us the files/md5 level — we want the root
        # Walk up to find the actual cache root
        if cache_path.name == 'md5' and cache_path.parent.name == 'files':
            return cache_path.parent.parent
        return cache_path
    except Exception:
        # Try .dvc/cache as fallback
        dvc_dir = Path('.dvc')
        if dvc_dir.exists():
            cache = dvc_dir / 'cache'
            if cache.exists():
                return cache
        return None


def _collect_dvc_files(
    targets: Optional[List[str]] = None,
) -> List[Path]:
    """Collect .dvc files from targets or the whole project.

    Args:
        targets: Optional list of file paths or directories.

    Returns:
        Sorted list of .dvc file paths.
    """
    dvc_files: List[Path] = []

    if targets:
        for target in targets:
            target_path = Path(target)
            if target_path.is_file() and target_path.suffix == '.dvc':
                dvc_files.append(target_path.resolve())
            elif target_path.is_dir():
                dvc_files.extend(
                    p.resolve() for p in target_path.rglob('*.dvc')
                )
            else:
                # Maybe it's a data path — try adding .dvc
                dvc_path = Path(str(target_path) + '.dvc')
                if dvc_path.is_file():
                    dvc_files.append(dvc_path.resolve())
                else:
                    raise MigrateError(
                        f"Target not found: {target} "
                        f"(tried {target_path} and {dvc_path})"
                    )
    else:
        # Find all .dvc files in the project, excluding .dvc/ directory
        for p in Path('.').rglob('*.dvc'):
            # Exclude the .dvc config directory itself and anything inside it
            if p.name == '.dvc' or '.dvc' in p.parts[:-1]:
                continue
            if p.is_file():
                dvc_files.append(p.resolve())

    return sorted(set(dvc_files))
