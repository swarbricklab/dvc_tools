"""Update imported DVC data by rebuilding .dir manifests.

Rebuilds .dir files for repo imports where the directory manifest
doesn't exist or is stale. This is distinct from `dvc update` which
downloads data - `dt update` only fixes metadata so `dt fetch` can work.

When --rev is not specified, the command checks if HEAD differs from
the locked revision. If no data changes are detected at the import path,
it safely upgrades to HEAD. If data has changed, it prompts the user
to specify which version they want.
"""

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from . import tmp as tmp_mod
from . import utils
from .errors import UpdateError


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class ImportInfo:
    """Information extracted from an import .dvc file."""
    dvc_path: Path
    repo_url: str
    path: str  # Path within source repo
    locked_rev: str
    current_hash: Optional[str]  # Current outs.md5 (may be None)
    is_directory: bool  # True if hash ends with .dir


@dataclass
class SourceChanges:
    """Result of checking for changes in source repo."""
    has_changes: bool
    head_rev: str
    added: int
    modified: int  
    deleted: int
    diff_summary: str  # Human-readable summary


# =============================================================================
# Helper functions
# =============================================================================

def _parse_import_info(dvc_path: Path) -> Optional[ImportInfo]:
    """Extract import information from a .dvc file.
    
    Args:
        dvc_path: Path to the .dvc file.
        
    Returns:
        ImportInfo if valid import file, None otherwise.
    """
    try:
        with open(dvc_path) as f:
            data = yaml.safe_load(f)
        
        if not data:
            return None
        
        deps = data.get('deps', [])
        if not deps:
            return None
        
        for dep in deps:
            repo = dep.get('repo', {})
            if repo:
                outs = data.get('outs', [])
                current_hash = outs[0].get('md5') if outs else None
                is_directory = current_hash.endswith('.dir') if current_hash else False
                
                # DVC uses rev_lock for the locked revision
                locked_rev = repo.get('rev_lock') or repo.get('rev', '')
                
                return ImportInfo(
                    dvc_path=dvc_path,
                    repo_url=repo.get('url', ''),
                    path=dep.get('path', ''),
                    locked_rev=locked_rev,
                    current_hash=current_hash,
                    is_directory=is_directory,
                )
        
        return None
    except (OSError, yaml.YAMLError):
        return None


def _get_head_rev(clone_path: Path) -> str:
    """Get the HEAD commit hash of a cloned repo."""
    result = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        cwd=clone_path,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise UpdateError(f"Failed to get HEAD revision: {result.stderr}")
    return result.stdout.strip()


def _check_source_changes(
    clone_path: Path,
    path: str,
    locked_rev: str,
    head_rev: str,
) -> SourceChanges:
    """Check if source data has changed between locked_rev and HEAD.
    
    Uses `dvc diff` in the cloned source repo to compare revisions.
    
    Args:
        clone_path: Path to cloned source repo.
        path: Path within repo to check.
        locked_rev: Currently locked revision.
        head_rev: HEAD revision to compare against.
        
    Returns:
        SourceChanges with comparison results.
    """
    if locked_rev == head_rev:
        return SourceChanges(
            has_changes=False,
            head_rev=head_rev,
            added=0,
            modified=0,
            deleted=0,
            diff_summary="Same revision",
        )
    
    # Run dvc diff in the clone
    result = subprocess.run(
        ['dvc', 'diff', '--json', locked_rev, head_rev],
        cwd=clone_path,
        capture_output=True,
        text=True,
    )
    
    # Parse diff output
    try:
        diff_data = json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError:
        # If diff fails or returns non-JSON, assume no changes
        diff_data = {}
    
    # Filter to only changes in our path
    def in_path(item):
        item_path = item.get('path', '')
        return item_path.startswith(path) or item_path == path
    
    added = [i for i in diff_data.get('added', []) if in_path(i)]
    modified = [i for i in diff_data.get('modified', []) if in_path(i)]
    deleted = [i for i in diff_data.get('deleted', []) if in_path(i)]
    
    has_changes = bool(added or modified or deleted)
    
    # Build summary
    parts = []
    if added:
        parts.append(f"+{len(added)} added")
    if modified:
        parts.append(f"~{len(modified)} modified")
    if deleted:
        parts.append(f"-{len(deleted)} deleted")
    
    return SourceChanges(
        has_changes=has_changes,
        head_rev=head_rev,
        added=len(added),
        modified=len(modified),
        deleted=len(deleted),
        diff_summary=', '.join(parts) if parts else "No changes",
    )


def _get_file_listing(
    repo_url: str,
    path: str,
    revision: str,
    verbose: bool = False,
) -> List[Dict[str, str]]:
    """Get file listing with hashes from source repo using dvc list.
    
    Args:
        repo_url: URL of source repository.
        path: Path within repo to list.
        revision: Git revision to list at.
        verbose: Print progress messages.
        
    Returns:
        List of dicts with 'md5' and 'relpath' keys.
        
    Raises:
        UpdateError: If listing fails.
    """
    if verbose:
        print(f"  Querying source: dvc list {repo_url} {path} --rev {revision[:12]}...")
    
    cmd = [
        'dvc', 'list',
        '--json',
        '--show-hash',
        '--recursive',
        repo_url,
        path,
        '--rev', revision,
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise UpdateError(f"dvc list failed: {result.stderr.strip()}")
    
    try:
        files = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise UpdateError(f"Failed to parse dvc list output: {e}")
    
    # Filter to files with hashes
    entries = []
    for f in files:
        if f.get('isdir'):
            continue
        md5 = f.get('md5')
        if not md5:
            continue
        relpath = f.get('path', '')
        if relpath:
            entries.append({'md5': md5, 'relpath': relpath})
    
    if verbose:
        print(f"  Found {len(entries)} files")
    
    return entries


def _build_dir_manifest(entries: List[Dict[str, str]]) -> bytes:
    """Build a .dir manifest file content in exact DVC format.
    
    DVC uses a specific JSON format for .dir files that must be reproduced
    exactly to match the expected hash.
    
    Args:
        entries: List of dicts with 'md5' and 'relpath' keys.
        
    Returns:
        Bytes content of the .dir file.
    """
    # Sort by relpath
    sorted_entries = sorted(entries, key=lambda x: x['relpath'])
    
    # Build JSON with exact DVC format
    parts = []
    for entry in sorted_entries:
        parts.append(f'{{"md5": "{entry["md5"]}", "relpath": "{entry["relpath"]}"}}')
    
    content = '[' + ', '.join(parts) + ']'
    return content.encode('utf-8')


def _write_dir_to_cache(
    manifest_content: bytes,
    dest_cache: str,
    verbose: bool = False,
) -> Tuple[str, Path]:
    """Write .dir manifest to cache and return its hash.
    
    Args:
        manifest_content: Bytes content of the .dir file.
        dest_cache: Path to cache base directory.
        verbose: Print progress messages.
        
    Returns:
        Tuple of (hash_with_dir_suffix, path_to_file).
    """
    # Compute hash
    file_hash = hashlib.md5(manifest_content).hexdigest()
    dir_hash = f"{file_hash}.dir"
    
    # Write to v3 cache layout
    dest_file = Path(dest_cache) / 'files' / 'md5' / file_hash[:2] / f"{file_hash[2:]}.dir"
    
    if not dest_file.exists():
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        dest_file.write_bytes(manifest_content)
        if verbose:
            print(f"  Created .dir file: {dest_file}")
    elif verbose:
        print(f"  .dir file already exists: {dest_file}")
    
    return dir_hash, dest_file


def _update_dvc_file(
    dvc_path: Path,
    new_hash: str,
    new_rev: Optional[str] = None,
    verbose: bool = False,
) -> bool:
    """Update the .dvc file with new hash and optionally new revision.
    
    Args:
        dvc_path: Path to the .dvc file.
        new_hash: New outs.md5 hash (with .dir suffix if directory).
        new_rev: New deps.repo.rev (None to keep current).
        verbose: Print progress messages.
        
    Returns:
        True if file was modified.
    """
    try:
        with open(dvc_path) as f:
            data = yaml.safe_load(f)
        
        modified = False
        
        # Update outs.md5
        if data.get('outs'):
            old_hash = data['outs'][0].get('md5')
            if old_hash != new_hash:
                data['outs'][0]['md5'] = new_hash
                modified = True
                if verbose:
                    print(f"  Updated outs.md5: {old_hash or 'None'} → {new_hash}")
        
        # Update deps.repo.rev_lock if specified
        if new_rev:
            deps = data.get('deps', [])
            for dep in deps:
                repo = dep.get('repo', {})
                if repo:
                    old_rev = repo.get('rev_lock') or repo.get('rev', '')
                    if old_rev != new_rev:
                        repo['rev_lock'] = new_rev
                        modified = True
                        if verbose:
                            old_str = f"{old_rev[:12]}..." if old_rev else "None"
                            print(f"  Updated rev_lock: {old_str} → {new_rev[:12]}...")
        
        if modified:
            with open(dvc_path, 'w') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            
            # Auto-stage if core.autostage is enabled
            if utils.is_autostage_enabled():
                utils.git_stage_file(dvc_path, verbose=verbose)
        
        return modified
    
    except (OSError, yaml.YAMLError) as e:
        raise UpdateError(f"Failed to update {dvc_path}: {e}")


def _push_dir_to_remote(
    dir_file: Path,
    remote_path: Path,
    dir_hash: str,
    verbose: bool = False,
) -> bool:
    """Push .dir file to source remote.
    
    Args:
        dir_file: Path to local .dir file.
        remote_path: Path to source remote.
        dir_hash: Hash of the .dir file (with .dir suffix).
        verbose: Print progress messages.
        
    Returns:
        True if file was pushed, False if already exists.
    """
    from . import cache_ops
    
    # Determine destination path in remote
    hash_only = dir_hash.replace('.dir', '')
    dest_v3 = remote_path / 'files' / 'md5' / hash_only[:2] / f"{hash_only[2:]}.dir"
    dest_v2 = remote_path / hash_only[:2] / f"{hash_only[2:]}.dir"
    
    # Check if already exists
    if dest_v3.exists() or dest_v2.exists():
        if verbose:
            print(f"  .dir already in source remote")
        return False
    
    # Push to v3 layout
    dest_v3.parent.mkdir(parents=True, exist_ok=True)
    success, link_type = cache_ops.link_file(dir_file, dest_v3, verbose=verbose, label=dir_hash)
    
    if success:
        if verbose:
            print(f"  Pushed .dir to source remote ({link_type})")
        return True
    
    return False


# =============================================================================
# Main update function
# =============================================================================

def update(
    targets: Optional[List[str]] = None,
    rev: Optional[str] = None,
    verbose: bool = False,
    push_dir: bool = False,
    no_download: bool = False,
    dry_run: bool = False,
    cache: Optional[str] = None,
) -> List[Tuple[str, bool, str]]:
    """Update import .dvc files by rebuilding .dir manifests.
    
    For directory imports, queries the source repository to get the
    current file listing, builds a .dir manifest, and updates the
    .dvc file with the correct hash.
    
    If --rev is not specified:
    - Checks if data at the import path has changed between locked rev and HEAD
    - If no changes: safely upgrades to HEAD
    - If changes detected: stops and asks user to specify --rev
    
    Args:
        targets: .dvc files to update. If None, updates all import files.
        rev: Git revision to update to. None = smart auto-detection.
        verbose: Show detailed progress.
        push_dir: Push .dir file to source remote after creating.
        no_download: Skip dt fetch after rebuilding .dir.
        dry_run: Show what would be done without making changes.
        cache: Explicit cache path. If None, uses primary cache.
        
    Returns:
        List of (target, success, message) tuples.
        
    Raises:
        UpdateError: If update fails completely.
    """
    # Find import files if no targets specified
    if not targets:
        targets = _find_import_files(verbose=verbose)
        if not targets:
            return [(".", True, "No import .dvc files found")]
    
    results = []
    updated_targets = []
    
    for target in targets:
        target_path = Path(target)
        
        # Validate target exists
        if not target_path.exists():
            if not target.endswith('.dvc'):
                target_path = Path(f"{target}.dvc")
            if not target_path.exists():
                results.append((target, False, "File not found"))
                continue
        
        # Parse import info
        info = _parse_import_info(target_path)
        if not info:
            results.append((target, False, "Not an import .dvc file"))
            continue
        
        if not info.repo_url:
            results.append((target, False, "No source URL in import"))
            continue
        
        print(f"\n{target_path}:")
        print(f"  Source: {info.repo_url}")
        print(f"  Path: {info.path}")
        print(f"  Locked rev: {info.locked_rev[:12]}...")
        
        # Get or create clone
        try:
            clone_path = tmp_mod.clone_repo(info.repo_url, refresh=True, verbose=verbose)
        except Exception as e:
            results.append((str(target_path), False, f"Clone failed: {e}"))
            continue
        
        # Determine target revision
        if rev:
            # Explicit revision specified
            target_rev = rev
            if target_rev == 'HEAD':
                target_rev = _get_head_rev(clone_path)
            print(f"  Target rev: {target_rev[:12]}... (specified)")
        else:
            # Smart detection: check for changes
            head_rev = _get_head_rev(clone_path)
            
            if head_rev == info.locked_rev:
                # Same revision - just refresh .dir
                target_rev = info.locked_rev
                print(f"  HEAD same as locked ({head_rev[:12]}...) - refreshing .dir")
            else:
                # Check for data changes
                print(f"  HEAD rev: {head_rev[:12]}...")
                print(f"  Checking for data changes...")
                
                changes = _check_source_changes(
                    clone_path, info.path, info.locked_rev, head_rev
                )
                
                if changes.has_changes:
                    # Data changed - cannot auto-update
                    print(f"  ⚠ Data has changed: {changes.diff_summary}")
                    print()
                    print(f"  Cannot auto-update when data has changed.")
                    print(f"  Specify which version:")
                    print(f"    dt update --rev {info.locked_rev[:12]} {target_path}  # Keep current")
                    print(f"    dt update --rev HEAD {target_path}  # Get new data")
                    print()
                    print(f"  To see details:")
                    print(f"    (cd {clone_path} && dvc diff {info.locked_rev[:12]} {head_rev[:12]})")
                    results.append((str(target_path), False, 
                        f"Data changed ({changes.diff_summary}). Specify --rev"))
                    continue
                else:
                    # No data changes - safe to upgrade
                    target_rev = head_rev
                    print(f"  ✓ No data changes - upgrading to HEAD")
        
        if dry_run:
            results.append((str(target_path), True, f"Would update to {target_rev[:12]}..."))
            continue
        
        # Get file listing from source
        try:
            entries = _get_file_listing(
                info.repo_url, info.path, target_rev, verbose=verbose
            )
        except UpdateError as e:
            results.append((str(target_path), False, str(e)))
            continue
        
        if not entries:
            # No entries found - error
            results.append((str(target_path), False, "No files found at source path"))
            continue
        
        # Check if this is a single file or directory import
        # Single file: exactly 1 entry where path is just the filename
        source_filename = Path(info.path).name
        is_single_file = (
            len(entries) == 1 and 
            entries[0]['relpath'] == source_filename
        )
        
        if is_single_file:
            # Single file import - use the md5 directly (no .dir)
            file_hash = entries[0]['md5']
            if verbose:
                print(f"  Single file import: {file_hash[:12]}...")
            
            # Check if hash changed
            if file_hash != info.current_hash:
                new_rev = target_rev if target_rev != info.locked_rev else None
                _update_dvc_file(target_path, file_hash, new_rev, verbose)
                results.append((str(target_path), True, f"Updated hash to {file_hash[:12]}..."))
                updated_targets.append(str(target_path))
            elif target_rev != info.locked_rev:
                _update_dvc_file(target_path, file_hash, target_rev, verbose)
                results.append((str(target_path), True, f"Updated rev to {target_rev[:12]}..."))
                updated_targets.append(str(target_path))
            else:
                results.append((str(target_path), True, "Already up to date"))
            continue
        
        # Build .dir manifest
        manifest_content = _build_dir_manifest(entries)
        
        # Get cache path - use explicit or primary
        if cache:
            cache_base = cache
        else:
            cache_dir = utils.get_cache_dir()
            if not cache_dir:
                results.append((str(target_path), False, "DVC cache not configured"))
                continue
            
            cache_base = str(cache_dir)
            if cache_base.endswith('/files/md5') or cache_base.endswith('\\files\\md5'):
                cache_base = str(Path(cache_base).parent.parent)
        
        # Write to cache
        dir_hash, dir_file = _write_dir_to_cache(manifest_content, cache_base, verbose)
        
        # Update .dvc file
        new_rev = target_rev if target_rev != info.locked_rev else None
        _update_dvc_file(target_path, dir_hash, new_rev, verbose)
        
        # Push to source remote if requested
        if push_dir:
            from . import remote as remote_mod
            try:
                local_remote = remote_mod.find_local_remote_from_repo(info.repo_url)
                if local_remote:
                    remote_path = Path(local_remote[1])
                    _push_dir_to_remote(dir_file, remote_path, dir_hash, verbose)
            except Exception as e:
                if verbose:
                    print(f"  Warning: Could not push to source remote: {e}")
        
        updated_targets.append(str(target_path))
        results.append((str(target_path), True, f"Built .dir ({len(entries)} files)"))
    
    # Run dt fetch for updated targets (unless --no-download)
    if updated_targets and not no_download and not dry_run:
        print(f"\nFetching data for {len(updated_targets)} updated import(s)...")
        from . import fetch as fetch_mod
        try:
            fetch_mod.fetch(targets=updated_targets, verbose=verbose, destination=cache)
        except Exception as e:
            print(f"  Warning: fetch failed: {e}")
            print(f"  Run 'dt fetch {' '.join(updated_targets)}' manually")
    
    return results


def _find_import_files(verbose: bool = False) -> List[str]:
    """Find all import .dvc files in the repository.
    
    Returns:
        List of paths to import .dvc files.
    """
    import_files = []
    
    # Find all .dvc files
    try:
        result = subprocess.run(
            ['git', 'ls-files', '*.dvc'],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return []
        
        dvc_files = result.stdout.strip().split('\n')
        dvc_files = [f for f in dvc_files if f]
    except (OSError, FileNotFoundError):
        dvc_files = [str(p) for p in Path('.').rglob('*.dvc')]
    
    for dvc_file in dvc_files:
        info = _parse_import_info(Path(dvc_file))
        if info:
            import_files.append(dvc_file)
            if verbose:
                print(f"  Found import: {dvc_file}")
    
    return import_files
