"""Pull DVC-tracked files, automatically handling imports.

This is the dt equivalent of `dvc pull`. It fetches data to the cache
and checks out to the workspace in one step.

For imports and local-remote scenarios, uses dt fetch for efficient
local cache symlinks. For data that requires network access, falls
back to dvc fetch.
"""

import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from dvc.repo import Repo

from . import utils
from .fetch import fetch
from .errors import FetchError, PullError


# =============================================================================
# Force mode - delete .dir manifests
# =============================================================================


def delete_dir_manifests(
    targets: Optional[List[str]] = None,
    verbose: bool = False,
) -> List[str]:
    """Delete .dir manifest files from cache for specified targets.
    
    This forces DVC to re-fetch directory contents on the next pull,
    useful after fixing corrupted files inside directories.
    
    Args:
        targets: Specific targets to process. If None, finds all directories.
        verbose: Print detailed progress.
        
    Returns:
        List of deleted .dir hashes.
    """
    try:
        repo = Repo()
        cache_dir = Path(repo.cache.local.path)
    except Exception as e:
        raise PullError(f"Not in a DVC repository: {e}")
    
    deleted = []
    repo_root = Path(repo.root_dir)
    
    # Resolve targets to absolute paths
    target_paths = set()
    if targets:
        for target in targets:
            target_path = Path(target).resolve()
            if not target_path.exists():
                target_path = repo_root / target
            target_paths.add(target_path)
    
    # Find matching directory outputs
    for out in repo.index.outs:
        if not out.hash_info or not out.hash_info.isdir:
            continue
        
        out_path = Path(out.fs_path)
        
        # Check if this directory matches targets (or no targets = all)
        if target_paths:
            matches = False
            for target_path in target_paths:
                try:
                    # Check if output matches or is under target
                    if out_path == target_path:
                        matches = True
                        break
                    out_path.relative_to(target_path)
                    matches = True
                    break
                except ValueError:
                    try:
                        target_path.relative_to(out_path)
                        matches = True
                        break
                    except ValueError:
                        pass
            if not matches:
                continue
        
        # Delete the .dir file from cache
        dir_hash = out.hash_info.value
        hash_clean = dir_hash.replace('.dir', '')
        cache_path = cache_dir / hash_clean[:2] / (hash_clean[2:] + '.dir')
        
        if cache_path.exists():
            try:
                cache_path.unlink()
                deleted.append(dir_hash)
                if verbose:
                    print(f"  Deleted .dir manifest: {out.fs_path}")
            except OSError as e:
                if verbose:
                    print(f"  Failed to delete {cache_path}: {e}")
    
    return deleted


# =============================================================================
# Main pull function
# =============================================================================


def pull(
    targets: Optional[List[str]] = None,
    verbose: bool = False,
    force: bool = False,
    update: bool = False,
    network: bool = True,
    dry: bool = False,
) -> Tuple[bool, int, int]:
    """Pull DVC-tracked files: fetch to cache + checkout to workspace.
    
    This is the simple model: dt pull = dt fetch + dvc checkout.
    
    By default, network=True means data will be fetched however possible:
    - Local cache symlinks for imports and local remotes (fast, no network)
    - Network download via dvc fetch for cloud remotes (slower, needs network)
    
    With network=False, only local sources are used. If data requires
    network access, an informative message is shown.
    
    Args:
        targets: Specific targets to pull. Can be:
            - .dvc file paths (e.g., 'data.dvc')
            - Pipeline stage names (e.g., 'transform')
            - Output paths (e.g., 'pipeline/output.txt')
            - None for all stages
        verbose: Print detailed progress.
        force: Delete .dir manifests before pulling to force re-fetch.
        update: If True, rebuild .dir files for imports if missing.
        network: If True (default), fall back to network fetch when
                 local cache is not available. If False, only use local
                 sources and report what would need network access.
        dry: If True, show what would be pulled without pulling.
        
    Returns:
        Tuple of (success, fetched_count, failed_count).
        
    Raises:
        PullError: If pull fails.
    """
    from . import doctor
    
    # Run environment checks
    env = doctor.check_environment()
    env.require_git_repo()
    
    # Also verify DVC is installed
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise PullError(str(e))
    
    # If force mode, delete .dir manifests first
    if force:
        if verbose:
            print("Force mode: deleting .dir manifests from cache...")
        try:
            deleted = delete_dir_manifests(targets=targets, verbose=verbose)
            if deleted:
                print(f"  Deleted {len(deleted)} .dir manifest(s)")
            elif verbose:
                print("  No .dir manifests found for targets")
        except Exception as e:
            print(f"Warning: failed to delete .dir manifests: {e}")
    
    # Phase 1: Fetch (populates cache)
    if verbose:
        print("\n=== Fetch phase ===")
    
    try:
        fetch_results = fetch(
            targets=targets,
            verbose=verbose,
            update=update,
            network=network,
            show_progress=True,
            dry=dry,
        )
    except FetchError as e:
        raise PullError(f"Fetch failed: {e}")
    
    # Count results
    fetched = 0
    failed = 0
    network_needed = []
    
    for source_name, success, message in fetch_results:
        if success:
            fetched += 1
        else:
            failed += 1
            # Track items that failed due to no local source
            if "No local source" in message or "use --network" in message.lower():
                network_needed.append(source_name)
    
    # If network=False and some items needed network, show helpful message
    if not network and network_needed:
        print(f"\n{len(network_needed)} stage(s) require network access:")
        for name in network_needed[:5]:  # Show first 5
            print(f"  - {name}")
        if len(network_needed) > 5:
            print(f"  ... and {len(network_needed) - 5} more")
        print("\nTo fetch these, run: dt pull --network")
        print("Or configure a local remote accessible on this filesystem.")
    
    if dry:
        # Dry run - don't checkout
        return True, 0, 0
    
    # Phase 2: Checkout (links from cache to workspace)
    if verbose:
        print("\n=== Checkout phase ===")
    
    cmd = ['dvc', 'checkout']
    if targets:
        cmd.extend(targets)
    
    if verbose:
        print(f"Running: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=not verbose, text=True)
    
    if result.returncode != 0:
        if not verbose and result.stderr:
            print(f"Checkout error: {result.stderr.strip()}")
        # Don't fail completely - fetch may have succeeded for some
        # User can run dvc checkout manually to see details
    
    success = (failed == 0 and result.returncode == 0)
    
    if verbose:
        print(f"\nPull complete: {fetched} fetched, {failed} failed")
    
    return success, fetched, failed

