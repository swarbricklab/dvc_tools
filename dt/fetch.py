"""Fetch DVC-tracked files into the primary cache.

Populates the primary cache with links/symlinks to files from source caches,
mirroring DVC's fetch concept (remote → cache). After fetch, regular
`dvc checkout` can link files from cache → workspace.

For import .dvc files (those with a deps section containing repo.url), 
automatically clones the source repository to find a locally-accessible cache.

For import-url .dvc files (external URLs like s3://, http://, local paths),
uses `dvc update` to re-download from the source URL.

This is the "dt" equivalent of `dvc fetch`, but works with local caches
(other projects' remotes that are accessible on the same filesystem).
"""

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from . import remote
from . import utils
from .errors import FetchError, HashMismatchError


def _is_autostage_enabled() -> bool:
    """Check if DVC core.autostage is enabled.
    
    Returns:
        True if autostage is enabled, False otherwise.
    """
    try:
        result = subprocess.run(
            ['dvc', 'config', 'core.autostage'],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0 and result.stdout.strip().lower() == 'true'
    except (OSError, FileNotFoundError):
        return False


def _git_stage_file(path: Path, verbose: bool = False) -> None:
    """Stage a file with git add.
    
    Args:
        path: Path to file to stage.
        verbose: Print progress messages.
    """
    try:
        result = subprocess.run(
            ['git', 'add', str(path)],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            if verbose:
                print(f"  Staged {path.name} (core.autostage enabled)")
        elif verbose:
            print(f"  Warning: Could not stage {path.name}: {result.stderr.strip()}")
    except (OSError, FileNotFoundError) as e:
        if verbose:
            print(f"  Warning: Could not stage {path.name}: {e}")


def _update_dvc_hash(dvc_path: Path, old_hash: str, new_hash: str, verbose: bool = False) -> bool:
    """Update the MD5 hash in a .dvc file.
    
    For import files, the hash is in outs with a .dir suffix (e.g., "abc123.dir").
    
    Args:
        dvc_path: Path to the .dvc file.
        old_hash: The old hash to replace (without .dir suffix).
        new_hash: The new hash to use.
        verbose: Print progress messages.
        
    Returns:
        True if the file was modified, False otherwise.
    """
    import yaml
    
    try:
        content = dvc_path.read_text()
        data = yaml.safe_load(content)
        
        # Update the outs section - check for both exact hash and hash.dir format
        modified = False
        for out in data.get('outs', []):
            out_md5 = out.get('md5', '')
            # Handle both "hash" and "hash.dir" formats
            if out_md5 == old_hash:
                out['md5'] = new_hash
                modified = True
            elif out_md5 == f"{old_hash}.dir":
                out['md5'] = f"{new_hash}.dir"
                modified = True
        
        if modified:
            # Write back with same formatting
            with open(dvc_path, 'w') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            if verbose:
                print(f"  Updated .dvc file hash: {old_hash[:12]}... -> {new_hash[:12]}...")
            
            # Stage file if autostage is enabled
            if _is_autostage_enabled():
                _git_stage_file(dvc_path, verbose)
            
            return True
        elif verbose:
            print(f"  Warning: Could not find hash {old_hash[:12]}... in .dvc file to update")
        return False
            
    except Exception as e:
        if verbose:
            print(f"  Warning: Could not update .dvc file: {e}")
        return False


def _populate_cache_from_source(
    dvc_path: Path,
    source_cache: str,
    verbose: bool = False,
    rev_lock: Optional[str] = None,
    source_url: Optional[str] = None,
    update: bool = False,
    show_progress: bool = False,
) -> Tuple[int, int]:
    """Populate the primary cache from a source cache.
    
    Creates symlinks in the primary cache pointing to files in the source cache.
    Respects the .dvc file format (v2 vs v3) when determining cache layout.
    
    For directory imports, if the .dir file doesn't exist in the source cache,
    it will be constructed using 'dvc list' to query the source repository.
    
    Args:
        dvc_path: Path to the .dvc file.
        source_cache: Path to the source cache.
        verbose: Print progress messages.
        rev_lock: Git revision for constructing .dir files via dvc list.
        source_url: URL of the source repository (for dvc list).
        update: If True, create .dir file with computed hash and update .dvc file.
        show_progress: If True (and not verbose), show a progress bar.
        
    Returns:
        Tuple of (files_added, files_failed) counts.
    """
    from . import import_data as import_mod
    
    primary_cache = utils.get_cache_dir()
    if not primary_cache:
        return 0, 0
    
    # Parse the .dvc file to get output info
    dvc_data = utils.parse_dvc_file(dvc_path)
    if not dvc_data:
        return 0, 0
    
    outs = dvc_data.get('outs', [])
    if not outs:
        return 0, 0
    
    out = outs[0]
    md5 = out.get('md5', '')
    
    # Detect v2 vs v3 format: v3 has explicit 'hash' field, v2 doesn't
    # This determines where dvc checkout will look for files
    use_v3_layout = import_mod.is_v3_dvc_file(dvc_data)
    
    # Get base cache directory (without files/md5 suffix)
    # repo.cache.local.path returns .../files/md5, we need the parent
    cache_base = str(primary_cache)
    if cache_base.endswith('/files/md5') or cache_base.endswith('\\files\\md5'):
        cache_base = str(Path(cache_base).parent.parent)
    
    # Show cache destination path in verbose mode
    if use_v3_layout:
        cache_dest_path = Path(cache_base) / 'files' / 'md5'
    else:
        cache_dest_path = Path(cache_base)
    
    if verbose:
        layout = "v3 (files/md5/)" if use_v3_layout else "v2 (legacy)"
        print(f"  DVC file format: {layout}")
        print(f"  Cache destination: {cache_dest_path}")
    
    count = 0
    failed = 0
    
    # Handle the root hash (.dir file or single file)
    if md5:
        result = import_mod.populate_cache_file(
            md5=md5,
            source_cache=source_cache,
            dest_cache=cache_base,
            verbose=verbose,
            use_v3_layout=use_v3_layout,
        )
        if result is True:
            count += 1
        elif result is None:
            # Source file not found
            # For .dir files, we'll try to construct it later - don't count as failure yet
            if not md5.endswith('.dir'):
                if verbose:
                    print(f"  ERROR: Source file not found in cache: {md5}")
                failed += 1
        # result is False means already cached - that's fine
    
    # For directories, also populate individual files
    if md5.endswith('.dir'):
        dir_hash = md5[:-4]  # Remove .dir suffix
        
        # First check if .dir file already exists in destination cache
        # (it may have been created by the original dvc import)
        if use_v3_layout:
            dest_dir_file = Path(cache_base) / 'files' / 'md5' / dir_hash[:2] / f"{dir_hash[2:]}.dir"
        else:
            dest_dir_file = Path(cache_base) / dir_hash[:2] / f"{dir_hash[2:]}.dir"
        
        dir_file = None
        entries = None
        
        if dest_dir_file.exists():
            if verbose:
                print(f"  .dir file already in primary cache: {dest_dir_file}")
            dir_file = dest_dir_file
        else:
            # Try DVC v3 path first, then v2 path in source cache
            dir_file_v3 = Path(source_cache) / 'files' / 'md5' / dir_hash[:2] / f"{dir_hash[2:]}.dir"
            dir_file_v2 = Path(source_cache) / dir_hash[:2] / f"{dir_hash[2:]}.dir"
            
            if dir_file_v3.exists():
                dir_file = dir_file_v3
            elif dir_file_v2.exists():
                dir_file = dir_file_v2
        
        if dir_file is None:
            # .dir file not in source cache or dest cache - construct it using dvc list
            # This works for both regular directories and nested DVC imports
            if source_url:
                deps = dvc_data.get('deps', [])
                if deps:
                    dep_path = deps[0].get('path', '')
                    if dep_path:
                        if verbose:
                            print(f"  .dir file not in cache, using dvc list to build manifest...")
                        elif show_progress:
                            click.echo(f"  Building file manifest...", nl=False)
                        
                        try:
                            result = import_mod.construct_dir_from_dvc_list(
                                repo_url=source_url,
                                path=dep_path,
                                revision=rev_lock,
                                expected_hash=dir_hash,
                                dest_cache=cache_base,
                                use_v3_layout=use_v3_layout,
                                verbose=verbose,
                                update=update,
                                dvc_file=str(dvc_path),
                            )
                        except HashMismatchError:
                            if show_progress and not verbose:
                                click.echo()  # Finish the line
                            # Re-raise to stop processing - user needs --update
                            raise
                        
                        if result is None:
                            if verbose:
                                print(f"  ERROR: Could not construct .dir file from dvc list")
                            elif show_progress:
                                click.echo(" failed")
                            failed += 1
                        else:
                            entries, new_hash = result
                            if show_progress and not verbose:
                                click.echo(f" {len(entries)} files")
                            # If hash changed, update the .dvc file
                            if new_hash and new_hash != dir_hash:
                                _update_dvc_hash(dvc_path, dir_hash, new_hash, verbose)
                                dir_hash = new_hash
            else:
                if verbose:
                    print(f"  .dir file not found in cache and no source URL available")
                failed += 1
        
        # Read entries from existing .dir file
        if dir_file and entries is None:
            try:
                import json
                entries = json.loads(dir_file.read_text())
            except (json.JSONDecodeError, KeyError) as e:
                if verbose:
                    print(f"Warning: Could not parse .dir file: {e}")
                entries = None
        
        # Populate individual files from entries
        if entries:
            # Use progress bar in non-verbose mode
            use_progressbar = show_progress and not verbose and len(entries) > 1
            
            def process_entry(entry):
                nonlocal count, failed
                file_md5 = entry.get('md5', '')
                relpath = entry.get('relpath', file_md5[:12])  # Use relpath if available
                if file_md5:
                    result = import_mod.populate_cache_file(
                        md5=file_md5,
                        source_cache=source_cache,
                        dest_cache=cache_base,
                        verbose=verbose,
                        use_v3_layout=use_v3_layout,
                    )
                    if result is True:
                        count += 1
                    elif result is None:
                        # Source file not found
                        if verbose:
                            print(f"  ERROR: File not found in source cache: {relpath} ({file_md5})")
                        failed += 1
                    # result is False means already cached - that's fine
            
            if use_progressbar:
                with click.progressbar(
                    entries,
                    label=f"  Fetching {len(entries)} files",
                    show_pos=True,
                    show_percent=True,
                ) as bar:
                    for entry in bar:
                        process_entry(entry)
            else:
                for entry in entries:
                    process_entry(entry)
            
            # Show summary
            if verbose:
                already_cached = len(entries) - count - failed
                print(f"  Summary: {len(entries)} files in manifest, {count} fetched, {already_cached} already cached, {failed} missing")
    
    return count, failed


def fetch_import(
    dvc_path: Path,
    verbose: bool = False,
    update: bool = False,
    show_progress: bool = False,
) -> Tuple[str, int, int]:
    """Fetch an import .dvc file by finding and linking from the source cache.
    
    This handles .dvc files created by `dvc import`. It finds a locally-accessible
    cache from the source repository and populates the primary cache with symlinks.
    
    Args:
        dvc_path: Path to the .dvc file.
        verbose: Print progress messages.
        update: If True, create .dir file with computed hash and update .dvc file.
        show_progress: If True (and not verbose), show a progress bar.
        
    Returns:
        Tuple of (source_cache_path, files_added_count, files_failed_count).
        
    Raises:
        FetchError: If fetch fails.
    """
    from . import remote as remote_mod
    
    import_info = utils.get_import_info(dvc_path)
    if not import_info:
        raise FetchError(f"Not an import .dvc file: {dvc_path}")
    
    source_url = import_info['url']
    if not source_url:
        raise FetchError(f"No source URL in import: {dvc_path}")
    
    if verbose:
        print(f"Import from: {source_url}")
        if import_info.get('path'):
            print(f"  Path: {import_info['path']}")
    elif show_progress:
        # Brief status in non-verbose mode
        click.echo(f"  Source: {source_url}", nl=False)
    
    # Step 1: Find a local remote from the source repo (clones if needed)
    if verbose:
        print(f"Looking for local cache...")
    elif show_progress:
        click.echo(" → finding cache...", nl=False)
    
    result = remote_mod.find_local_remote_from_repo(repo_spec=source_url)
    
    if not result:
        if show_progress and not verbose:
            click.echo()  # Finish the line
        raise FetchError(
            f"No locally-accessible cache found for {source_url}.\n"
            f"The source repository's remote may not be on this filesystem.\n"
            f"Options:\n"
            f"  1. Use 'dt import {source_url} <path>' to set up proper tracking\n"
            f"  2. Use 'dvc pull' to fetch from the remote directly"
        )
    
    remote_name, cache_path = result
    
    if verbose:
        print(f"Found local cache: {cache_path} (from remote '{remote_name}')")
    elif show_progress:
        click.echo(" → fetching...")  # Finish the line
    
    # Step 2: Populate primary cache with symlinks
    if verbose:
        print(f"Populating primary cache...")
    
    count, failed = _populate_cache_from_source(
        dvc_path, cache_path, verbose,
        rev_lock=import_info.get('rev'),
        source_url=source_url,
        update=update,
        show_progress=show_progress,
    )
    
    return cache_path, count, failed


def fetch(
    targets: Optional[List[str]] = None,
    verbose: bool = False,
    update: bool = False,
    show_progress: bool = True,
    network: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Fetch DVC-tracked files into the primary cache.
    
    Populates the primary cache with symlinks to files from source caches.
    This is the equivalent of `dvc fetch` but for local caches.
    
    For import .dvc files, automatically discovers the source repository's
    local cache and creates symlinks.
    
    For non-import .dvc files, checks if there's a locally-accessible remote
    and creates symlinks from it. If no local remote is available and network
    is True, falls back to `dvc fetch`.
    
    After fetch, run `dvc checkout` to link files to the workspace.
    
    Args:
        targets: DVC targets to fetch. Can be:
            - .dvc file paths (e.g., 'data.dvc')
            - Pipeline stage names (e.g., 'transform')
            - Output paths (e.g., 'pipeline/output.txt')
            - None for all stages
        verbose: Print progress messages.
        update: If True, create .dir file with computed hash and update .dvc file.
        show_progress: If True (and not verbose), show a progress bar.
        network: If True, fall back to `dvc fetch` when local remote not available.
        
    Returns:
        List of (target, success, message) tuples.
        
    Raises:
        FetchError: If fetch fails.
    """
    from dvc.stage import PipelineStage
    from dvc.stage.exceptions import StageFileDoesNotExistError
    from dvc.scm import SCMError
    
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise FetchError(str(e))
    
    # Collect stages using DVC's internal index
    # Falls back to file-based approach if not in a git repo
    use_stages = True
    stages = []
    
    try:
        stages = utils.collect_stages(targets=targets, verbose=verbose)
    except StageFileDoesNotExistError as e:
        raise FetchError(str(e))
    except SCMError:
        # Not in a git repository - fall back to file-based approach
        use_stages = False
        if verbose:
            print("Not in a git repository, using file-based discovery")
    except Exception as e:
        # Other errors - try fallback
        use_stages = False
        if verbose:
            print(f"Could not use DVC index ({e}), using file-based discovery")
    
    if use_stages:
        if verbose:
            print(f"Found {len(stages)} stage(s) to process")
        return _fetch_from_stages(
            stages=stages,
            verbose=verbose,
            update=update,
            show_progress=show_progress,
            network=network,
        )
    else:
        # Fallback: use file-based approach
        return _fetch_from_files(
            targets=targets,
            verbose=verbose,
            update=update,
            show_progress=show_progress,
            network=network,
        )


def _fetch_from_stages(
    stages: List[Any],
    verbose: bool = False,
    update: bool = False,
    show_progress: bool = True,
    network: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Fetch from DVC Stage objects.
    
    This is the primary implementation using DVC internals.
    """
    from dvc.stage import PipelineStage
    
    results = []
    
    for stage in stages:
        stage_name = stage.addressing
        stage_path = Path(stage.path) if hasattr(stage, 'path') else None
        is_pipeline = isinstance(stage, PipelineStage)
        
        # Show which stage we're processing (in non-verbose mode with progress)
        if show_progress and not verbose:
            click.echo(f"{stage_name}:")
        
        # Check if it's an import from another DVC repo
        if stage.is_repo_import:
            if verbose:
                print(f"Fetching repo import: {stage_name}")
            
            if not stage_path or is_pipeline:
                results.append((stage_name, False, "Repo imports must be .dvc files"))
                continue
            
            try:
                cache_path, count, failed = fetch_import(
                    dvc_path=stage_path,
                    verbose=verbose,
                    update=update,
                    show_progress=show_progress,
                )
                if failed > 0:
                    results.append((stage_name, False, 
                        f"FAILED: {failed} file(s) not found in source cache at {cache_path}"))
                elif count == 0:
                    results.append((stage_name, True, f"Already in cache (from {cache_path})"))
                else:
                    results.append((stage_name, True, f"Fetched {count} files from {cache_path}"))
            except FetchError as e:
                results.append((stage_name, False, str(e)))
        
        # Check if it's a URL import (dvc import-url) - is_import but not is_repo_import
        elif stage.is_import and not stage.is_repo_import:
            if verbose:
                print(f"Fetching URL import: {stage_name}")
            
            if not stage_path or is_pipeline:
                results.append((stage_name, False, "URL imports must be .dvc files"))
                continue
            
            result = _fetch_url_import(
                dvc_path=stage_path,
                verbose=verbose,
            )
            results.append((stage_name, result[0], result[1]))
        
        else:
            # Regular stage (.dvc file or pipeline output) - try to fetch from local remote
            if verbose:
                if is_pipeline:
                    print(f"Fetching pipeline output: {stage_name}")
                else:
                    print(f"Fetching: {stage_name}")
            
            result = _fetch_stage(
                stage=stage,
                verbose=verbose,
                network=network,
            )
            results.append((stage_name, result[0], result[1]))
    
    return results


def _fetch_from_files(
    targets: Optional[List[str]] = None,
    verbose: bool = False,
    update: bool = False,
    show_progress: bool = True,
    network: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Fetch using file-based discovery (fallback for non-git repos).
    
    This preserves the original behavior for environments without git.
    """
    # Find .dvc files using fallback method
    dvc_files = utils.find_dvc_files_fallback(targets=targets, verbose=verbose)
    
    if not dvc_files and targets:
        # If specific targets were given but not found, report errors
        results = []
        for target in targets:
            results.append((target, False, f"File not found: {target}"))
        return results
    
    results = []
    
    for target_path in dvc_files:
        # Show which file we're processing (in non-verbose mode with progress)
        if show_progress and not verbose:
            click.echo(f"{target_path}:")
        
        # Check if it's an import from another DVC repo
        if utils.is_repo_import(target_path):
            if verbose:
                print(f"Fetching repo import: {target_path}")
            
            try:
                cache_path, count, failed = fetch_import(
                    dvc_path=target_path,
                    verbose=verbose,
                    update=update,
                    show_progress=show_progress,
                )
                if failed > 0:
                    results.append((str(target_path), False, 
                        f"FAILED: {failed} file(s) not found in source cache at {cache_path}"))
                elif count == 0:
                    results.append((str(target_path), True, f"Already in cache (from {cache_path})"))
                else:
                    results.append((str(target_path), True, f"Fetched {count} files from {cache_path}"))
            except FetchError as e:
                results.append((str(target_path), False, str(e)))
        
        # Check if it's a URL import (dvc import-url)
        elif utils.is_url_import(target_path):
            if verbose:
                print(f"Fetching URL import: {target_path}")
            
            result = _fetch_url_import(
                dvc_path=target_path,
                verbose=verbose,
            )
            results.append((str(target_path), result[0], result[1]))
        
        else:
            # Regular .dvc file - try to fetch from local remote
            result = _fetch_non_import(
                dvc_path=target_path,
                verbose=verbose,
                network=network,
            )
            results.append((str(target_path), result[0], result[1]))
    
    return results


def _fetch_stage(
    stage: Any,
    verbose: bool = False,
    network: bool = False,
) -> Tuple[bool, str]:
    """Fetch a regular stage (non-import) from a local remote.
    
    Works with both .dvc file stages and pipeline stages from dvc.yaml.
    
    Args:
        stage: A DVC Stage object.
        verbose: Print progress messages.
        network: If True, fall back to `dvc fetch` when local remote not available.
        
    Returns:
        Tuple of (success, message).
    """
    from . import import_data as import_mod
    from dvc.stage import PipelineStage
    
    is_pipeline = isinstance(stage, PipelineStage)
    stage_name = stage.addressing
    
    # Get output info from stage
    if not stage.outs:
        return (False, "No outputs in stage")
    
    out = stage.outs[0]
    if not out.hash_info or not out.hash_info.value:
        return (False, "No hash in stage output")
    
    md5 = out.hash_info.value
    
    primary_cache = utils.get_cache_dir()
    if not primary_cache:
        return (False, "No cache configured")
    
    # Check if already in cache
    hash_clean = md5.replace('.dir', '')
    suffix = '.dir' if md5.endswith('.dir') else ''
    cache_file = primary_cache / hash_clean[:2] / (hash_clean[2:] + suffix)
    if cache_file.exists():
        return (True, "Already in cache")
    
    # Try to find a local remote
    remotes = remote.list_remotes()
    local_remote_info = remote.find_local_remote(remotes)
    
    if local_remote_info:
        remote_name, remote_path = local_remote_info
        if verbose:
            print(f"  Using local remote '{remote_name}': {remote_path}")
        
        # Detect v2 vs v3 format - for pipeline stages, assume v3
        if is_pipeline:
            use_v3_layout = True
        else:
            dvc_data = utils.parse_dvc_file(Path(stage.path))
            use_v3_layout = import_mod.is_v3_dvc_file(dvc_data) if dvc_data else True
        
        # Get cache base directory
        cache_base = str(primary_cache)
        if cache_base.endswith('/files/md5') or cache_base.endswith('\\files\\md5'):
            cache_base = str(primary_cache.parent.parent)
        
        # Fetch the file(s)
        if md5.endswith('.dir'):
            # Directory - need to fetch .dir file and all contents
            count, failed = _fetch_directory_from_remote(
                remote_path=remote_path,
                cache_base=cache_base,
                md5=md5,
                verbose=verbose,
                use_v3_layout=use_v3_layout,
            )
            if failed > 0:
                return (False, f"Failed to fetch {failed} file(s)")
            return (True, f"Fetched {count} files from {remote_name}")
        else:
            # Single file
            result = _populate_cache_file(
                cache_base=cache_base,
                file_md5=md5,
                source_cache_base=remote_path,
                verbose=verbose,
                use_v3_layout=use_v3_layout,
            )
            if result is True:
                return (True, f"Fetched from {remote_name}")
            elif result is None:
                return (False, f"File not in remote '{remote_name}'")
            else:
                return (True, "Already in cache")
    
    # No local remote available
    if not network:
        return (False, "No local remote available (use --network to fetch from remote)")
    
    # Fall back to dvc fetch
    success, msg = _run_dvc_fetch(stage_name, verbose)
    return (success, msg)


def _fetch_non_import(
    dvc_path: Path,
    verbose: bool = False,
    network: bool = False,
) -> Tuple[bool, str]:
    """Fetch a non-import .dvc file from a local remote.
    
    Checks if there's a locally-accessible remote and creates symlinks/reflinks
    from it. If no local remote is available and network is True, falls back
    to `dvc fetch`.
    
    Args:
        dvc_path: Path to the .dvc file.
        verbose: Print progress messages.
        network: If True, fall back to `dvc fetch` when local remote not available.
        
    Returns:
        Tuple of (success, message).
    """
    from . import import_data as import_mod
    
    dvc_data = utils.parse_dvc_file(dvc_path)
    if not dvc_data:
        return (False, "Could not parse .dvc file")
    
    outs = dvc_data.get('outs', [])
    if not outs:
        return (False, "No outputs in .dvc file")
    
    md5 = outs[0].get('md5', '')
    if not md5:
        return (False, "No MD5 hash in .dvc file")
    
    primary_cache = utils.get_cache_dir()
    if not primary_cache:
        return (False, "No cache configured")
    
    # Check if already in cache
    # Note: primary_cache from get_cache_dir() already includes files/md5
    hash_clean = md5.replace('.dir', '')
    suffix = '.dir' if md5.endswith('.dir') else ''
    cache_file = primary_cache / hash_clean[:2] / (hash_clean[2:] + suffix)
    if cache_file.exists():
        return (True, "Already in cache")
    
    # Try to find a local remote
    remotes = remote.list_remotes()
    local_remote_info = remote.find_local_remote(remotes)
    
    if local_remote_info:
        remote_name, remote_path = local_remote_info
        if verbose:
            print(f"  Using local remote '{remote_name}': {remote_path}")
        
        # Detect v2 vs v3 format
        use_v3_layout = import_mod.is_v3_dvc_file(dvc_data)
        
        # Get cache base directory
        cache_base = str(primary_cache)
        if cache_base.endswith('/files/md5') or cache_base.endswith('\\files\\md5'):
            cache_base = str(Path(cache_base).parent.parent)
        
        # Fetch the main file/directory hash
        count = 0
        failed = 0
        
        result = import_mod.populate_cache_file(
            md5=md5,
            source_cache=remote_path,
            dest_cache=cache_base,
            verbose=verbose,
            use_v3_layout=use_v3_layout,
        )
        
        if result is True:
            count += 1
        elif result is None:
            # File not in local remote
            if not md5.endswith('.dir'):
                failed += 1
        
        # For directories, also populate individual files
        if md5.endswith('.dir'):
            dir_hash = md5[:-4]
            
            # Find the .dir file
            if use_v3_layout:
                dest_dir_file = Path(cache_base) / 'files' / 'md5' / dir_hash[:2] / f"{dir_hash[2:]}.dir"
            else:
                dest_dir_file = Path(cache_base) / dir_hash[:2] / f"{dir_hash[2:]}.dir"
            
            dir_file = None
            if dest_dir_file.exists():
                dir_file = dest_dir_file
            else:
                # Try source remote
                dir_file_v3 = Path(remote_path) / 'files' / 'md5' / dir_hash[:2] / f"{dir_hash[2:]}.dir"
                dir_file_v2 = Path(remote_path) / dir_hash[:2] / f"{dir_hash[2:]}.dir"
                if dir_file_v3.exists():
                    dir_file = dir_file_v3
                elif dir_file_v2.exists():
                    dir_file = dir_file_v2
            
            if dir_file:
                try:
                    import json
                    entries = json.loads(dir_file.read_text())
                    for entry in entries:
                        file_md5 = entry.get('md5', '')
                        if file_md5:
                            file_result = import_mod.populate_cache_file(
                                md5=file_md5,
                                source_cache=remote_path,
                                dest_cache=cache_base,
                                verbose=verbose,
                                use_v3_layout=use_v3_layout,
                            )
                            if file_result is True:
                                count += 1
                            elif file_result is None:
                                failed += 1
                except (json.JSONDecodeError, OSError) as e:
                    if verbose:
                        print(f"  Warning: Could not parse .dir file: {e}")
                    failed += 1
            else:
                if verbose:
                    print(f"  Warning: .dir file not found in local remote")
                failed += 1
        
        if failed > 0:
            # Some files not in local remote - fall back to network if enabled
            if network:
                return _run_dvc_fetch(dvc_path, verbose)
            else:
                return (False, f"Not in local remote '{remote_name}' (use --network to fetch)")
        elif count == 0:
            return (True, f"Already in cache (from {remote_name})")
        else:
            return (True, f"Fetched {count} files from local remote '{remote_name}'")
    
    # No local remote available
    if network:
        return _run_dvc_fetch(dvc_path, verbose)
    else:
        return (False, "No local remote available (use --network to fetch)")


def _run_dvc_fetch(dvc_path: Path, verbose: bool = False) -> Tuple[bool, str]:
    """Run dvc fetch for a specific target.
    
    Args:
        dvc_path: Path to the .dvc file.
        verbose: Print progress messages.
        
    Returns:
        Tuple of (success, message).
    """
    cmd = ['dvc', 'fetch', str(dvc_path)]
    if verbose:
        print(f"  Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return (True, "Fetched via dvc fetch (network)")
        else:
            error_msg = result.stderr.strip() if result.stderr else "Unknown error"
            return (False, f"dvc fetch failed: {error_msg}")
    except (OSError, FileNotFoundError) as e:
        return (False, f"dvc fetch failed: {e}")


def _fetch_url_import(
    dvc_path: Path,
    verbose: bool = False,
) -> Tuple[bool, str]:
    """Fetch a URL import by running dvc update.
    
    For .dvc files created by `dvc import-url`, the data is typically not
    pushed to remote storage. Instead, we re-download from the source URL
    using `dvc update`.
    
    If the source has changed, dvc update will update the .dvc file with
    the new hash.
    
    Args:
        dvc_path: Path to the .dvc file.
        verbose: Print progress messages.
        
    Returns:
        Tuple of (success, message).
    """
    # First check if already in cache
    dvc_data = utils.parse_dvc_file(dvc_path)
    if dvc_data:
        outs = dvc_data.get('outs', [])
        if outs:
            md5 = outs[0].get('md5', '')
            if md5:
                primary_cache = utils.get_cache_dir()
                if primary_cache:
                    hash_clean = md5.replace('.dir', '')
                    suffix = '.dir' if md5.endswith('.dir') else ''
                    cache_file = primary_cache / hash_clean[:2] / (hash_clean[2:] + suffix)
                    if cache_file.exists():
                        return (True, "Already in cache (URL import)")
    
    # Get URL info for display
    url_info = utils.get_url_import_info(dvc_path)
    source_url = url_info.get('url', 'unknown') if url_info else 'unknown'
    
    if verbose:
        print(f"  URL import from: {source_url}")
        print(f"  Running: dvc update {dvc_path}")
    
    cmd = ['dvc', 'update', str(dvc_path)]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            # Check if the .dvc file was modified (source changed)
            if 'Importing' in result.stdout or 'importing' in result.stdout.lower():
                return (True, f"Updated from {source_url}")
            else:
                return (True, f"Fetched from {source_url}")
        else:
            error_msg = result.stderr.strip() if result.stderr else "Unknown error"
            # Common errors
            if 'No such file' in error_msg or 'not found' in error_msg.lower():
                return (False, f"Source not accessible: {source_url}")
            return (False, f"dvc update failed: {error_msg}")
    except (OSError, FileNotFoundError) as e:
        return (False, f"dvc update failed: {e}")


# Keep smart_checkout as alias for backwards compatibility during transition
def smart_checkout(
    targets: Optional[List[str]] = None,
    extra_args: Optional[List[str]] = None,
    verbose: bool = False,
    cache: Optional[str] = None,
    update: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Deprecated: Use fetch() + dvc checkout instead.
    
    This function is kept for backwards compatibility during the transition.
    It runs fetch() and then dvc checkout.
    """
    # Run fetch first
    results = fetch(
        targets=targets,
        verbose=verbose,
        update=update,
    )
    
    # Then run dvc checkout
    cmd = ['dvc', 'checkout']
    if targets:
        cmd.extend(targets)
    if extra_args:
        cmd.extend(extra_args)
    
    try:
        checkout_result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        if checkout_result.returncode != 0:
            results.append(('dvc checkout', False, checkout_result.stderr.strip()))
        elif verbose:
            if checkout_result.stdout.strip():
                print(checkout_result.stdout.strip())
    except Exception as e:
        results.append(('dvc checkout', False, str(e)))
    
    return results
