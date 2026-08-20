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
from dvc.utils.serialize import dump_yaml

from . import cache_ops
from . import dvc_deps
from . import dvc_lock
from . import find as find_mod
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
    rev_spec: Optional[str] = None  # deps.repo.rev as written (branch/tag/sha)


@dataclass
class SourceChanges:
    """Result of checking for changes in source repo."""
    has_changes: bool
    head_rev: str
    added: int
    modified: int  
    deleted: int
    diff_summary: str  # Human-readable summary
    new_path: Optional[str] = None  # If deleted but moved, the new path
    diff_error: Optional[str] = None  # Error message if diff failed
    # Git-tracked paths inside the import that the source repo does not hash.
    # Their presence means no hash we compute can match what dvc records.
    mixed_tree: Optional[List[str]] = None


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
        text = Path(dvc_path).read_text()
    except (OSError, UnicodeDecodeError):
        return None

    imports, _ = dvc_deps.parse_import_refs(text, str(dvc_path))
    if not imports:
        return None

    ref = imports[0]
    return ImportInfo(
        dvc_path=dvc_path,
        repo_url=ref.repo_url,
        path=ref.path,
        locked_rev=ref.locked_rev,
        current_hash=ref.md5,
        is_directory=ref.is_directory,
        rev_spec=ref.rev,
    )


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


def _is_sha_like(rev: str) -> bool:
    """True if ``rev`` looks like a commit sha rather than a branch or tag.

    A sha is immutable, so it pins; a branch name is a moving target that a
    ``rev_lock`` is expected to resolve. The two need opposite treatment when
    ``rev_lock`` advances, and this is the only signal available from the
    ``.dvc`` file alone.
    """
    if not rev or len(rev) < 7 or len(rev) > 40:
        return False
    return all(c in '0123456789abcdefABCDEF' for c in rev)


def _resolve_rev(clone_path: Path, rev: str) -> Optional[str]:
    """Resolve a git revision spec to a full commit sha in the clone.

    Tries the spec as given, then under ``origin/`` -- the tmp clones track
    remote branches, so a bare branch name usually only exists as
    ``origin/<name>``.

    Returns:
        The full 40-char sha, or None if the spec does not resolve.
    """
    for candidate in (rev, f'origin/{rev}'):
        result = subprocess.run(
            ['git', 'rev-parse', '--verify', '--quiet', f'{candidate}^{{commit}}'],
            cwd=clone_path,
            capture_output=True,
            text=True,
        )
        sha = result.stdout.strip()
        if result.returncode == 0 and sha:
            return sha
    return None


def _tracked_tip(clone_path: Path, rev_spec: Optional[str], verbose: bool = False) -> str:
    """Current tip of the revision this import tracks.

    ``dvc update`` advances ``rev_lock`` to the tip of ``deps.repo.rev``, not
    to whatever the clone happens to have checked out. Honouring the recorded
    spec keeps ``rev`` and ``rev_lock`` consistent; ignoring it is how the two
    fields end up asserting different revisions (issue #182).
    """
    if rev_spec and not _is_sha_like(rev_spec):
        resolved = _resolve_rev(clone_path, rev_spec)
        if resolved:
            return resolved
        if verbose:
            print(f"  Recorded rev '{rev_spec}' does not resolve in the clone; using HEAD")
    return _get_head_rev(clone_path)


def _compute_source_hash(
    repo_url: str,
    path: str,
    rev: str,
    verbose: bool = False,
) -> Tuple[Optional[str], int, List[str]]:
    """Compute the DVC hash of source data at a revision.

    Lists the path at ``rev`` and reconstructs the hash exactly the way
    ``dt update`` builds it: the file md5 for a single-file import, or the
    ``.dir`` manifest md5 for a directory. This works for *any* path,
    including a subpath of a tracked directory, because ``dvc list``
    resolves paths within the tree regardless of how they are tracked.
    This is what makes change detection robust where ``dvc diff
    --targets`` (which resolves against stage/out paths) is not.

    Args:
        repo_url: URL of source repository.
        path: Path within repo to hash.
        rev: Git revision to hash at.
        verbose: Print progress messages.

    Returns:
        Tuple of (hash, nfiles, unhashed_paths). Returns (None, 0, []) if the
        path lists nothing or cannot be listed at this revision (e.g. it was
        moved or deleted). ``unhashed_paths`` is non-empty when the path mixes
        git-tracked files with DVC outs, in which case no hash computed from
        hashes alone can match what ``dvc import`` recorded.
    """
    try:
        entries, unhashed = _get_file_listing(repo_url, path, rev, verbose=verbose)
    except UpdateError as e:
        if verbose:
            print(f"  Could not list {path} at {rev[:12]}...: {e}")
        return None, 0, []

    if unhashed:
        return None, 0, unhashed

    if not entries:
        return None, 0, []

    # Single file: exactly one entry whose relpath is just the filename.
    source_filename = Path(path).name
    if len(entries) == 1 and entries[0]['relpath'] == source_filename:
        return entries[0]['md5'], 1, []

    # Directory: hash matches outs.md5 (the .dir manifest md5).
    manifest_content = utils.build_dir_manifest(entries)
    dir_hash = f"{hashlib.md5(manifest_content).hexdigest()}.dir"
    return dir_hash, len(entries), []


def _check_source_changes(
    clone_path: Path,
    path: str,
    locked_rev: str,
    head_rev: str,
    repo_url: str,
    current_hash: Optional[str] = None,
    verbose: bool = False,
) -> SourceChanges:
    """Check if source data has changed between the locked state and HEAD.

    Compares the hash recorded in the .dvc file (``current_hash``) against
    the hash of the source data at ``head_rev``, computed from
    ``dvc list``. This is robust against the import path not lining up with
    a tracked stage/out path, which is where ``dvc diff --targets`` fails.

    If the path lists nothing at HEAD, the data may have moved: the hash is
    searched for elsewhere in the repo and, if found at a new path, the move
    is reported instead of a change.

    Args:
        clone_path: Path to cloned source repo (used for move detection).
        path: Path within repo to check.
        locked_rev: Currently locked revision.
        head_rev: HEAD revision to compare against.
        repo_url: URL of source repository (used to list at HEAD).
        current_hash: Hash recorded in the .dvc file (the locked data).
        verbose: Print progress messages.

    Returns:
        SourceChanges with comparison results, including new_path if moved.
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

    # Without a recorded hash there is nothing to compare against.
    if not current_hash:
        return SourceChanges(
            has_changes=True,  # Cannot verify - treat as changed
            head_rev=head_rev,
            added=0,
            modified=0,
            deleted=0,
            diff_summary="Unknown current hash",
            diff_error="No recorded hash in .dvc file",
        )

    head_hash, _, unhashed = _compute_source_hash(
        repo_url, path, head_rev, verbose=verbose
    )

    if unhashed:
        # Nothing to compare: dt's hash of this path can never equal the one
        # dvc recorded, because dvc counted the git-tracked files too. Report
        # it rather than the "data changed" it would otherwise look like.
        return SourceChanges(
            has_changes=True,
            head_rev=head_rev,
            added=0,
            modified=0,
            deleted=0,
            diff_summary="Path mixes git-tracked files with DVC outs",
            mixed_tree=unhashed,
        )

    if head_hash is None:
        # Path lists nothing at HEAD - data may have moved or been deleted.
        if verbose:
            print(f"  Path absent at HEAD, searching for hash {current_hash[:12]}...")
        new_path = find_mod.find_hash_in_repo(
            current_hash, clone_path, revision=head_rev, verbose=verbose
        )
        if new_path and new_path != path:
            return SourceChanges(
                has_changes=False,  # Data unchanged, just moved
                head_rev=head_rev,
                added=0,
                modified=0,
                deleted=1,
                diff_summary=f"Moved: {path} → {new_path}",
                new_path=new_path,
            )
        if new_path:
            # Same path, same hash - unchanged (rev_lock just advances).
            return SourceChanges(
                has_changes=False,
                head_rev=head_rev,
                added=0,
                modified=0,
                deleted=0,
                diff_summary="No changes",
            )
        # Hash not found anywhere at HEAD - data is gone.
        return SourceChanges(
            has_changes=True,
            head_rev=head_rev,
            added=0,
            modified=0,
            deleted=1,
            diff_summary="Path no longer present at HEAD",
        )

    if head_hash == current_hash:
        return SourceChanges(
            has_changes=False,
            head_rev=head_rev,
            added=0,
            modified=0,
            deleted=0,
            diff_summary="No changes",
        )

    # Hash differs - the data changed between the locked state and HEAD.
    return SourceChanges(
        has_changes=True,
        head_rev=head_rev,
        added=0,
        modified=1,
        deleted=0,
        diff_summary=f"Hash changed: {current_hash[:12]}... → {head_hash[:12]}...",
    )


def _get_file_listing(
    repo_url: str,
    path: str,
    revision: str,
    verbose: bool = False,
) -> Tuple[List[Dict[str, any]], List[str]]:
    """Get file listing with hashes and sizes from source repo using dvc list.

    Args:
        repo_url: URL of source repository.
        path: Path within repo to list.
        revision: Git revision to list at.
        verbose: Print progress messages.

    Returns:
        Tuple of (entries, unhashed_paths). Entries are dicts with 'md5',
        'relpath' and optionally 'size'. ``unhashed_paths`` holds the files the
        source repo records no hash for -- git-tracked files in a directory
        that is not itself an out. They are reported rather than dropped:
        dropping them silently produced a manifest that disagreed with dvc's
        (issue #182).

    Raises:
        UpdateError: If listing fails.
    """
    if verbose:
        print(f"  Querying source: dvc list {repo_url} {path} --rev {revision[:12]}...")
    
    cmd = [
        'dvc', 'list',
        '--json',
        '--show-hash',
        '--size',
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
    
    # Split into hashed entries and the git-tracked files the source repo
    # records no hash for.
    unhashed = utils.unhashed_listing_paths(files)
    entries = []
    for f in files:
        if f.get('isdir'):
            continue
        md5 = f.get('md5')
        if not md5:
            continue
        relpath = f.get('path', '')
        if relpath:
            entry = {'md5': md5, 'relpath': relpath}
            # Include size if available (may be None)
            if f.get('size') is not None:
                entry['size'] = f['size']
            entries.append(entry)

    if verbose:
        total_size = sum(e.get('size', 0) or 0 for e in entries)
        if total_size > 0:
            print(f"  Found {len(entries)} files ({utils.format_size(total_size, human_readable=True)})")
        else:
            print(f"  Found {len(entries)} files")
        if unhashed:
            print(f"  {len(unhashed)} git-tracked file(s) carry no hash")

    return entries, unhashed




def _cache_root(cache: Optional[str]) -> Optional[str]:
    """Root of the cache to work in: the dir that contains ``files/md5``."""
    if cache:
        return cache
    cache_dir = utils.get_cache_dir()
    if not cache_dir:
        return None
    base = str(cache_dir)
    if base.endswith(('/files/md5', '\\files\\md5')):
        return str(Path(base).parent.parent)
    return base


def _find_source_remote(repo_url: str, verbose: bool = False) -> Optional[str]:
    """Path to the source repo's remote, if it is on this filesystem.

    Used both to size objects and to publish a rebuilt manifest, so look it up
    once. Returns None for cloud remotes or when the lookup fails -- neither is
    fatal, it just means sizes have to come from elsewhere.
    """
    from . import remote as remote_mod
    try:
        local_remote = remote_mod.find_local_remote_from_repo(repo_url)
    except Exception as e:
        if verbose:
            print(f"  Could not resolve source remote: {e}")
        return None
    return local_remote[1] if local_remote else None


def _total_size_from_entries(
    entries: List[Dict[str, any]],
    *cache_roots: Optional[str],
    verbose: bool = False,
) -> Optional[int]:
    """Total bytes for a manifest, or None if any entry cannot be accounted for.

    ``dvc list --size`` against a repo *URL* generally reports no sizes at all:
    a ``.dir`` manifest records only md5 and relpath, so the only place a size
    exists is the object itself. Hence the fallback -- stat the object in a
    locally-readable cache or remote, which is authoritative because the object
    *is* the content.

    Returns None if even one entry is unsized, rather than a partial total: an
    undercount reads as fact downstream, where a missing size reads as unknown
    (issue #182).
    """
    total = 0
    unsized = 0
    for entry in entries:
        size = entry.get('size')
        if size is None:
            size = cache_ops.object_size(entry['md5'], *cache_roots)
        if size is None:
            unsized += 1
        else:
            total += size

    if unsized:
        if verbose:
            print(
                f"  Could not size {unsized} of {len(entries)} object(s) "
                f"from the source listing or any local cache"
            )
        return None
    return total


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
    
    # Take the workspace lock around the cache write so a concurrent `dvc`
    # process cannot observe a half-written .dir manifest.
    with dvc_lock.repo_lock():
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
    new_path: Optional[str] = None,
    size: Optional[int] = None,
    clear_size: bool = False,
    nfiles: Optional[int] = None,
    new_rev_spec: Optional[str] = None,
    verbose: bool = False,
) -> bool:
    """Update the .dvc file with new hash, size, nfiles, and optionally new revision/path.

    Args:
        dvc_path: Path to the .dvc file.
        new_hash: New outs.md5 hash (with .dir suffix if directory).
        new_rev: New deps.repo.rev_lock (None to keep current).
        new_path: New deps.path if source path has changed (None to keep current).
        size: Total size in bytes (None to leave the recorded size alone).
        clear_size: Delete ``outs.size`` instead of leaving a value that
            belongs to the previous hash. Set this when the data changed but
            its size could not be determined -- a stale size is a false
            statement that propagates downstream, where a missing one is
            merely unknown (issue #182).
        nfiles: Number of files for directories (None to omit).
        new_rev_spec: New deps.repo.rev, i.e. the revision *spec* the caller
            asked for. When None, a recorded spec that would contradict
            ``new_rev`` is dropped rather than left to roll the import back.
        verbose: Print progress messages.

    Returns:
        True if file was modified.
    """
    try:
        with open(dvc_path) as f:
            data = yaml.safe_load(f)
        
        modified = False
        
        # Update outs section
        if data.get('outs'):
            outs = data['outs'][0]
            old_hash = outs.get('md5')
            
            if old_hash != new_hash:
                outs['md5'] = new_hash
                modified = True
                if verbose:
                    print(f"  Updated outs.md5: {old_hash or 'None'} → {new_hash}")
            
            # Update size if provided
            if size is not None:
                old_size = outs.get('size')
                if old_size != size:
                    outs['size'] = size
                    modified = True
                    if verbose:
                        old_str = utils.format_size(old_size, True) if old_size else 'None'
                        new_str = utils.format_size(size, True)
                        print(f"  Updated size: {old_str} → {new_str}")
            elif clear_size and 'size' in outs:
                # The size we hold describes the old hash. Keeping it would
                # assert something untrue about the new data.
                old_size = outs.pop('size')
                modified = True
                print(
                    f"  Removed outs.size ({utils.format_size(old_size, True)}): "
                    f"could not determine the size of the new data. "
                    f"Run 'dvc update {dvc_path}' or 'dt du' to record it."
                )

            # Update nfiles if provided (for directories)
            if nfiles is not None:
                old_nfiles = outs.get('nfiles')
                if old_nfiles != nfiles:
                    outs['nfiles'] = nfiles
                    modified = True
                    if verbose:
                        print(f"  Updated nfiles: {old_nfiles or 'None'} → {nfiles}")
        
        # Update deps.repo.rev_lock if specified
        if new_rev:
            deps = data.get('deps', [])
            for dep in deps:
                repo = dep.get('repo', {})
                if not repo:
                    continue

                old_rev = repo.get('rev_lock') or repo.get('rev', '')
                if old_rev != new_rev:
                    repo['rev_lock'] = new_rev
                    modified = True
                    if verbose:
                        old_str = f"{old_rev[:12]}..." if old_rev else "None"
                        print(f"  Updated rev_lock: {old_str} → {new_rev[:12]}...")

                # Keep deps.repo.rev consistent with the lock. rev is the spec
                # and rev_lock its resolution, so a rev pinned to a different
                # commit makes the file assert two revisions at once -- and the
                # next `dvc update` believes rev, rolling the import back to
                # the commit we just moved away from (issue #182).
                old_spec = repo.get('rev')
                if new_rev_spec:
                    if old_spec != new_rev_spec:
                        repo['rev'] = new_rev_spec
                        modified = True
                        if verbose:
                            print(f"  Updated rev: {old_spec or 'None'} → {new_rev_spec}")
                elif (old_spec and _is_sha_like(old_spec)
                        and not new_rev.lower().startswith(old_spec.lower())):
                    del repo['rev']
                    modified = True
                    print(
                        f"  Removed pinned rev ({old_spec}): it no longer matches "
                        f"rev_lock {new_rev[:12]}..., and would have rolled this "
                        f"import back on the next 'dvc update'. The import now "
                        f"tracks the source's default branch; re-pin with "
                        f"'dt update --rev <rev>' if that is not what you want."
                    )

        # Update deps.path if source path has changed
        if new_path:
            deps = data.get('deps', [])
            for dep in deps:
                old_path = dep.get('path', '')
                if old_path != new_path:
                    dep['path'] = new_path
                    modified = True
                    if verbose:
                        print(f"  Updated deps.path: {old_path} → {new_path}")
        
        if modified:
            data = utils.recompute_dvc_md5(data)
            # Take the workspace lock around the .dvc file rewrite so a
            # concurrent `dvc` process cannot read a partially-written file.
            with dvc_lock.repo_lock():
                dump_yaml(dvc_path, data)
            
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

    The write must never be a symlink: the remote is shared, but ``dir_file``
    lives in this machine's local cache, so a symlinked ``.dir`` in the remote
    would point at a per-machine path and dangle for every other user (it also
    trips up ``dt find`` and gets baked into archives). We therefore transfer
    via :func:`remote_ops._transfer_file`, which uses reflink → hardlink → copy
    and deliberately never symlinks — the same rule the rest of the remote-write
    machinery follows.

    Args:
        dir_file: Path to local .dir file.
        remote_path: Path to source remote.
        dir_hash: Hash of the .dir file (with .dir suffix).
        verbose: Print progress messages.

    Returns:
        True if file was pushed, False if already exists.
    """
    from . import remote_ops
    from .errors import RemoteError

    # Determine destination path in remote
    hash_only = dir_hash.replace('.dir', '')
    dest_v3 = remote_path / 'files' / 'md5' / hash_only[:2] / f"{hash_only[2:]}.dir"
    dest_v2 = remote_path / hash_only[:2] / f"{hash_only[2:]}.dir"

    # Check if already exists
    if dest_v3.exists() or dest_v2.exists():
        if verbose:
            print(f"  .dir already in source remote")
        return False

    # Push to v3 layout using the symlink-free remote-write helper.
    dest_v3.parent.mkdir(parents=True, exist_ok=True)
    try:
        method = remote_ops._transfer_file(dir_file, dest_v3, independent=False)
    except RemoteError as e:
        if verbose:
            print(f"  Warning: could not push .dir to source remote: {e}")
        return False

    if method == 'skipped':
        return False

    if verbose:
        print(f"  Pushed .dir to source remote ({method})")
    return True


# =============================================================================
# Main update function
# =============================================================================

def update(
    targets: Optional[List[str]] = None,
    rev: Optional[str] = None,
    verbose: bool = False,
    no_download: bool = False,
    dry_run: bool = False,
    cache: Optional[str] = None,
    rebuild: bool = False,
    force: bool = False,
    show_status: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Update import .dvc files by rebuilding .dir manifests.
    
    For directory imports, queries the source repository to get the
    current file listing, builds a .dir manifest, and updates the
    .dvc file with the correct hash.
    
    If --rev is not specified:
    - Checks if data at the import path has changed between locked rev and HEAD
    - If no changes: safely upgrades to HEAD
    - If changes detected: stops and asks user to specify --rev or --force
    - If --rebuild: rebuilds at locked rev (skips change detection)
    - If --force: accepts changes and updates to HEAD
    
    Args:
        targets: .dvc files to update. If None, updates all import files.
        rev: Git revision to update to. None = smart auto-detection.
        verbose: Show detailed progress.
        no_download: Skip dt fetch after rebuilding .dir.
        dry_run: Show what would be done without making changes.
        cache: Explicit cache path. If None, uses primary cache.
        rebuild: Force rebuild at locked rev (skips change detection).
        force: Accept data changes and update to HEAD.
        show_status: Show summary output mode (suppresses per-file details).
        
    Note:
        Rebuilt .dir files are always pushed to the source remote.
        
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
    total_targets = len(targets)
    
    for idx, target in enumerate(targets, 1):
        target_path = Path(target)
        
        # Show progress in status mode
        if show_status:
            print(f"\rChecking {idx}/{total_targets}...", end="", flush=True)
        
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
        
        if not show_status:
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
        # Initialize source_path - may be updated if move is detected
        source_path = info.path
        path_changed = False
        
        # The spec to record in deps.repo.rev. An explicit --rev is what the
        # caller now tracks; 'HEAD' is not a spec worth recording (it means
        # "wherever the source is now"), so it leaves rev to be reconciled
        # against the new lock.
        rev_spec = rev if (rev and rev != 'HEAD') else None

        if rev:
            # Explicit revision specified. Record the spec, but lock to a full
            # sha: rev_lock is the *resolution* of the spec, so writing a
            # branch name there would leave the import unpinned.
            if rev == 'HEAD':
                target_rev = _get_head_rev(clone_path)
            else:
                resolved = _resolve_rev(clone_path, rev)
                if not resolved:
                    results.append((str(target_path), False,
                        f"Revision '{rev}' does not resolve in {info.repo_url}"))
                    continue
                target_rev = resolved
            if not show_status:
                spec_note = f" ({rev} → sha)" if rev_spec and rev_spec != target_rev else ""
                print(f"  Target rev: {target_rev[:12]}...{spec_note} (specified)")
        else:
            # Smart detection: check for changes. Advance along the revision
            # this import tracks, which is deps.repo.rev when it names a
            # branch or tag -- not simply whatever the clone has checked out.
            head_rev = _tracked_tip(clone_path, info.rev_spec, verbose=verbose)
            
            if rebuild:
                # --rebuild: skip change detection, just rebuild at locked rev
                target_rev = info.locked_rev
                if not show_status:
                    print(f"  Rebuilding .dir at locked rev ({target_rev[:12]}...)")
            elif head_rev == info.locked_rev:
                # Same revision - just refresh .dir
                target_rev = info.locked_rev
                if not show_status:
                    print(f"  HEAD same as locked ({head_rev[:12]}...) - refreshing .dir")
            else:
                # Check for data changes
                if not show_status:
                    print(f"  HEAD rev: {head_rev[:12]}...")
                    print(f"  Checking for data changes...")
                
                changes = _check_source_changes(
                    clone_path, info.path, info.locked_rev, head_rev,
                    repo_url=info.repo_url,
                    current_hash=info.current_hash, verbose=verbose
                )
                
                # A path that mixes git-tracked files with DVC outs cannot be
                # rebuilt at all, so say that instead of reporting a change.
                if changes.mixed_tree:
                    if not show_status:
                        print()
                        print(utils.describe_mixed_tree(
                            changes.mixed_tree, info.path, info.repo_url,
                            action='rebuild',
                        ))
                    results.append((str(target_path), False,
                        "Path mixes git-tracked files with DVC outs"))
                    continue

                # Handle diff errors (e.g., unknown revision)
                if changes.diff_error:
                    if not show_status:
                        print(f"  ⚠ {changes.diff_error}")
                        print()
                        print(f"  Cannot auto-detect changes. Specify version explicitly:")
                        print(f"    dt update --rev HEAD {target_path}  # Use latest")
                    results.append((str(target_path), False, 
                        f"Diff failed: {changes.diff_error}. Specify --rev"))
                    continue
                
                # Handle path move detection
                if changes.new_path:
                    if not show_status:
                        print(f"  ✓ Data moved: {info.path} → {changes.new_path}")
                    # Update path for subsequent operations
                    source_path = changes.new_path
                    path_changed = True
                else:
                    source_path = info.path
                    path_changed = False
                
                if changes.has_changes:
                    # Data changed
                    if force:
                        # --force: accept changes and update to HEAD
                        target_rev = head_rev
                        if not show_status:
                            print(f"  ⚠ Data has changed: {changes.diff_summary}")
                            print(f"  → Updating to HEAD (--force)")
                    else:
                        # Stop and show options
                        if not show_status:
                            print(f"  ⚠ Data has changed: {changes.diff_summary}")
                            print()
                            print(f"  Cannot auto-update when data has changed.")
                            print(f"  Options:")
                            print(f"    dt update --force {target_path}  # Accept new data from HEAD")
                            print(f"    dt update --rebuild {target_path}  # Rebuild .dir at current lock")
                            print(f"    dt update --rev <rev> {target_path}  # Update to specific revision")
                            print()
                            print(f"  To inspect changes at source:")
                            print(f"    cd {clone_path}")
                            print(f"    dvc diff {info.locked_rev[:12]} {head_rev[:12]} -- {info.path}")
                        results.append((str(target_path), False, 
                            f"Data changed ({changes.diff_summary})"))
                        continue
                else:
                    # No data changes - safe to upgrade
                    target_rev = head_rev
                    if path_changed:
                        if dry_run:
                            if not show_status:
                                print(f"  ✓ No data changes (path moved) - would update to {head_rev[:12]}...")
                            results.append((str(target_path), True, f"Up to date (path moved)"))
                        else:
                            if not show_status:
                                print(f"  ✓ No data changes (path moved) - updating to {head_rev[:12]}...")
                    else:
                        # Fast path: just update rev_lock without re-listing source
                        if dry_run:
                            if not show_status:
                                print(f"  ✓ No data changes - would update rev_lock to {head_rev[:12]}...")
                            results.append((str(target_path), True, f"Up to date"))
                        else:
                            if not show_status:
                                print(f"  ✓ No data changes - updating rev_lock to {head_rev[:12]}...")
                            _update_dvc_file(
                                target_path, info.current_hash, head_rev,
                                verbose=verbose
                            )
                            results.append((str(target_path), True, f"Updated rev_lock to {head_rev[:12]}..."))
                        continue
        
        if dry_run:
            results.append((str(target_path), True, f"Would update to {target_rev[:12]}..."))
            continue
        
        # Get file listing from source
        try:
            entries, unhashed = _get_file_listing(
                info.repo_url, source_path, target_rev, verbose=verbose
            )
        except UpdateError as e:
            results.append((str(target_path), False, str(e)))
            continue

        # Reached via --rev/--rebuild, which skip change detection. Refuse
        # rather than build a manifest that disagrees with dvc's -- and, since
        # the .dir is pushed upstream, plant that disagreement in a shared
        # remote where no dvc operation would ever produce it (#182).
        if unhashed:
            if not show_status:
                print()
                print(utils.describe_mixed_tree(
                    unhashed, source_path, info.repo_url, action='rebuild'
                ))
            results.append((str(target_path), False,
                "Path mixes git-tracked files with DVC outs"))
            continue

        if not entries:
            # No entries found - error
            results.append((str(target_path), False, "No files found at source path"))
            continue
        
        # Where sizes can be read from: the source repo's remote if it is on
        # this filesystem, then the cache we are writing into.
        cache_base = _cache_root(cache)
        source_remote = _find_source_remote(info.repo_url, verbose=verbose)
        size_roots = (source_remote, cache_base)

        # Check if this is a single file or directory import
        # Single file: exactly 1 entry where path is just the filename
        source_filename = Path(source_path).name
        is_single_file = (
            len(entries) == 1 and
            entries[0]['relpath'] == source_filename
        )

        if is_single_file:
            # Single file import - use the md5 directly (no .dir)
            file_hash = entries[0]['md5']
            file_size = _total_size_from_entries(
                entries, *size_roots, verbose=verbose
            )  # May be None if the object is not locally readable
            if verbose:
                size_str = f" ({utils.format_size(file_size, True)})" if file_size else ""
                print(f"  Single file import: {file_hash[:12]}...{size_str}")
            
            # Check if hash changed or we need to update metadata
            needs_update = (
                file_hash != info.current_hash or 
                target_rev != info.locked_rev or
                file_size is not None or  # Always update if we have size info
                path_changed  # Update if source path changed
            )
            
            if needs_update:
                # Also pass the rev when only the spec changed, so an explicit
                # --rev is recorded even if it resolves to the current lock.
                new_rev = target_rev if (target_rev != info.locked_rev or rev_spec) else None
                _update_dvc_file(
                    target_path, file_hash, new_rev,
                    new_path=source_path if path_changed else None,
                    size=file_size,
                    clear_size=file_size is None and file_hash != info.current_hash,
                    new_rev_spec=rev_spec,
                    verbose=verbose
                )
                if path_changed:
                    results.append((str(target_path), True, f"Moved to {source_path}"))
                elif file_hash != info.current_hash:
                    results.append((str(target_path), True, f"Updated hash to {file_hash[:12]}..."))
                elif target_rev != info.locked_rev:
                    results.append((str(target_path), True, f"Updated rev to {target_rev[:12]}..."))
                else:
                    results.append((str(target_path), True, "Updated metadata"))
                updated_targets.append(str(target_path))
            else:
                results.append((str(target_path), True, "Already up to date"))
            continue
        
        # Directory import
        nfiles = len(entries)

        if not cache_base:
            results.append((str(target_path), False, "DVC cache not configured"))
            continue

        # Total the payload. None means "could not be determined", which is
        # written as an absent size rather than a stale or partial one.
        total_size = _total_size_from_entries(entries, *size_roots, verbose=verbose)

        # Build .dir manifest
        manifest_content = utils.build_dir_manifest(entries)

        # Write to cache
        dir_hash, dir_file = _write_dir_to_cache(manifest_content, cache_base, verbose)

        # Update .dvc file with size, nfiles, and path metadata
        # Also pass the rev when only the spec changed, so an explicit
        # --rev is recorded even if it resolves to the current lock.
        new_rev = target_rev if (target_rev != info.locked_rev or rev_spec) else None
        _update_dvc_file(
            target_path, dir_hash, new_rev,
            new_path=source_path if path_changed else None,
            size=total_size,
            clear_size=total_size is None and dir_hash != info.current_hash,
            nfiles=nfiles,
            new_rev_spec=rev_spec,
            verbose=verbose
        )

        # Always push to source remote so fetch can find it
        if source_remote:
            try:
                _push_dir_to_remote(dir_file, Path(source_remote), dir_hash, verbose)
            except Exception as e:
                if verbose:
                    print(f"  Warning: Could not push to source remote: {e}")

        updated_targets.append(str(target_path))
        size_str = f", {utils.format_size(total_size, True)}" if total_size is not None else ""
        if path_changed:
            results.append((str(target_path), True, f"Moved to {source_path} ({nfiles} files{size_str})"))
        else:
            results.append((str(target_path), True, f"Built .dir ({nfiles} files{size_str})"))
    
    # Run dt fetch for updated targets (unless --no-download)
    if updated_targets and not no_download and not dry_run:
        print(f"\nFetching data for {len(updated_targets)} updated import(s)...")
        from . import fetch as fetch_mod
        try:
            fetch_results = fetch_mod.fetch(
                targets=updated_targets, verbose=verbose, destination=cache
            )
            # Surface fetch failures (including post-fetch cache verification
            # misses) so a silently-incomplete rebuild is not reported as
            # success (issue #151). Previously the return value was discarded
            # and only a raised exception was noticed.
            fetch_failures = [
                (name, msg) for name, ok, msg in (fetch_results or []) if not ok
            ]
            if fetch_failures:
                for name, msg in fetch_failures:
                    print(f"  ✗ fetch: {name}: {msg}")
                results.append((
                    'fetch', False,
                    f"fetch reported {len(fetch_failures)} failure(s) after rebuild"
                ))
                print(f"  Run 'dt fetch {' '.join(updated_targets)}' to retry")
        except Exception as e:
            print(f"  Warning: fetch failed: {e}")
            print(f"  Run 'dt fetch {' '.join(updated_targets)}' manually")
            results.append(('fetch', False, f"fetch raised: {e}"))
        
        # Checkout files to workspace
        print(f"\nChecking out files to workspace...")
        for target in updated_targets:
            try:
                result = subprocess.run(
                    ['dvc', 'checkout', target],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    print(f"  Warning: checkout failed for {target}: {result.stderr.strip()}")
                elif verbose:
                    print(f"  Checked out: {target}")
            except Exception as e:
                print(f"  Warning: checkout failed for {target}: {e}")
    
    # Clear progress line if we were showing status
    if show_status:
        print("\r" + " " * 40 + "\r", end="", flush=True)
    
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
