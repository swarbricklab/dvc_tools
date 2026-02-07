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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click

from . import cache_ops
from . import remote
from . import utils
from .errors import FetchError, HashMismatchError


# =============================================================================
# Stage Categorization
# =============================================================================

@dataclass
class RepoImportGroup:
    """Group of repo import stages from the same source repository."""
    url: str
    rev: Optional[str]
    stages: List[Any] = field(default_factory=list)
    local_cache: Optional[Path] = None
    has_local_cache: bool = False
    
    def add_stage(self, stage: Any) -> None:
        """Add a stage to this group."""
        self.stages.append(stage)
    
    @property
    def count(self) -> int:
        """Number of stages in this group."""
        return len(self.stages)
    
    @property
    def short_name(self) -> str:
        """Short display name for this repo (last component of URL)."""
        if not self.url:
            return "unknown"
        # Handle both git URLs and paths
        name = self.url.rstrip('/').split('/')[-1]
        # Remove .git suffix if present
        if name.endswith('.git'):
            name = name[:-4]
        return name


@dataclass
class StageCategorization:
    """Categorized stages for fetch operations.
    
    Divides stages into three categories:
    1. URL imports (dvc import-url) - external URLs
    2. Repo imports (dvc import) - grouped by source repository
    3. Regular stages - non-import .dvc files and pipeline stages
    """
    url_imports: List[Any] = field(default_factory=list)
    repo_imports: Dict[str, RepoImportGroup] = field(default_factory=dict)
    regular_stages: List[Any] = field(default_factory=list)
    has_local_remote: bool = False
    local_remote_name: Optional[str] = None
    
    @property
    def total_stages(self) -> int:
        """Total number of stages across all categories."""
        repo_import_count = sum(g.count for g in self.repo_imports.values())
        return len(self.url_imports) + repo_import_count + len(self.regular_stages)
    
    @property
    def repo_import_count(self) -> int:
        """Total number of repo import stages."""
        return sum(g.count for g in self.repo_imports.values())
    
    def summary_lines(self, verbose: bool = False) -> List[str]:
        """Generate summary lines for display.
        
        Args:
            verbose: If True, include individual stage names.
            
        Returns:
            List of formatted strings for display.
        """
        lines = []
        
        # URL imports
        if self.url_imports:
            lines.append(f"URL imports: {len(self.url_imports)}")
            if verbose:
                for stage in self.url_imports:
                    lines.append(f"    {stage.addressing}")
        
        # Repo imports (grouped by source)
        if self.repo_imports:
            lines.append(f"Repo imports: {self.repo_import_count}")
            for url, group in sorted(self.repo_imports.items(), key=lambda x: x[1].short_name):
                cache_status = "✓ local" if group.has_local_cache else "✗ remote not locally accessible"
                lines.append(f"    {group.short_name}: {group.count} ({cache_status})")
                if verbose:
                    for stage in group.stages:
                        lines.append(f"        {stage.addressing}")
        
        # Regular stages
        if self.regular_stages:
            if self.has_local_remote:
                cache_status = f"✓ local via '{self.local_remote_name}'"
            else:
                cache_status = "✗ remote not locally accessible"
            lines.append(f"Regular stages: {len(self.regular_stages)} ({cache_status})")
            if verbose:
                for stage in self.regular_stages:
                    lines.append(f"    {stage.addressing}")
        
        return lines
    
    def print_summary(self, verbose: bool = False) -> None:
        """Print summary to stdout."""
        lines = self.summary_lines(verbose=verbose)
        for line in lines:
            print(line)


def categorize_stages(
    stages: List[Any],
    verbose: bool = False,
) -> StageCategorization:
    """Categorize stages by type for fetch operations.
    
    Divides stages into:
    1. URL imports (dvc import-url)
    2. Repo imports (dvc import) - grouped by source repository
    3. Regular stages (non-import)
    
    For repo imports, also checks if a local cache is accessible.
    
    Args:
        stages: List of DVC Stage objects.
        verbose: Print progress information.
        
    Returns:
        StageCategorization with stages organized by type.
    """
    result = StageCategorization()
    
    for stage in stages:
        if stage.is_repo_import:
            # Get import info to group by source repo
            stage_path = Path(stage.path) if hasattr(stage, 'path') else None
            if stage_path:
                import_info = utils.get_import_info(stage_path)
                if import_info:
                    url = import_info.get('url', '')
                    rev = import_info.get('rev')
                    
                    # Get or create group for this URL
                    if url not in result.repo_imports:
                        group = RepoImportGroup(url=url, rev=rev)
                        # Check if we have a locally-accessible remote for this repo
                        try:
                            local_remote = remote.find_local_remote_from_repo(url)
                            if local_remote:
                                group.local_cache = Path(local_remote[1])
                                group.has_local_cache = True
                        except Exception:
                            pass
                        result.repo_imports[url] = group
                    
                    result.repo_imports[url].add_stage(stage)
                else:
                    # Couldn't get import info, still a repo import but unknown source
                    if '' not in result.repo_imports:
                        result.repo_imports[''] = RepoImportGroup(url='', rev=None)
                    result.repo_imports[''].add_stage(stage)
            else:
                # No path, unknown source
                if '' not in result.repo_imports:
                    result.repo_imports[''] = RepoImportGroup(url='', rev=None)
                result.repo_imports[''].add_stage(stage)
        elif stage.is_import:
            # URL import (dvc import-url)
            result.url_imports.append(stage)
        else:
            # Regular stage
            result.regular_stages.append(stage)
    
    # Check if there's a locally-accessible remote for regular stages
    if result.regular_stages:
        try:
            remotes = remote.list_remotes()
            local_remote = remote.find_local_remote(remotes)
            if local_remote:
                result.has_local_remote = True
                result.local_remote_name = local_remote[0]
        except Exception:
            pass
    
    if verbose:
        print(f"Categorized {result.total_stages} stages:")
        result.print_summary(verbose=False)
    
    return result


# =============================================================================
# Fetch Plan: Hash-based fetch model
# =============================================================================

@dataclass
class SourceGroup:
    """Group of hashes to fetch from a single source cache."""
    source_path: Path
    source_name: str  # Display name (e.g., remote name or repo short name)
    hashes: set = field(default_factory=set)
    
    def add_hash(self, h: str) -> None:
        """Add a hash to fetch from this source."""
        self.hashes.add(h)
    
    def add_hashes(self, hashes: set) -> None:
        """Add multiple hashes to fetch from this source."""
        self.hashes.update(hashes)


@dataclass 
class FetchPlan:
    """Plan for fetching hashes from multiple sources.
    
    Groups hashes by source cache, allowing efficient bulk fetching.
    URL imports are handled separately (require network download).
    """
    sources: Dict[str, SourceGroup] = field(default_factory=dict)
    url_imports: List[Any] = field(default_factory=list)
    no_source: List[Any] = field(default_factory=list)  # Stages with no local source
    
    def add_source(self, source_path: Path, source_name: str) -> SourceGroup:
        """Get or create a source group."""
        key = str(source_path)
        if key not in self.sources:
            self.sources[key] = SourceGroup(source_path=source_path, source_name=source_name)
        return self.sources[key]
    
    @property
    def total_hashes(self) -> int:
        """Total unique hashes across all sources."""
        return sum(len(g.hashes) for g in self.sources.values())
    
    def summary_lines(self) -> List[str]:
        """Generate summary for display."""
        lines = []
        for source_path, group in self.sources.items():
            lines.append(f"{group.source_name}: {len(group.hashes)} hashes from {source_path}")
        if self.url_imports:
            lines.append(f"URL imports: {len(self.url_imports)} (require network)")
        if self.no_source:
            lines.append(f"No local source: {len(self.no_source)} stages")
        return lines


def _create_source_cache_db(source_path: Path):
    """Create a LocalHashFileDB for reading from a source cache.
    
    Handles both v2 and v3 cache layouts.
    
    Args:
        source_path: Path to the remote/cache root.
        
    Returns:
        LocalHashFileDB instance, or None if path doesn't exist.
    """
    from dvc.fs import LocalFileSystem
    from dvc_data.hashfile.db.local import LocalHashFileDB
    
    if not source_path.exists():
        return None
    
    # Check for v3 layout (files/md5)
    v3_path = source_path / "files" / "md5"
    if v3_path.exists():
        cache_path = v3_path
    else:
        cache_path = source_path
    
    local_fs = LocalFileSystem()
    return LocalHashFileDB(local_fs, cache_path, tmp_dir=source_path / "tmp")


def _expand_dir_hash(
    dir_hash: str,
    source_db,
) -> set:
    """Expand a .dir hash to get all child file hashes.
    
    Uses DVC's Tree.load() to read the directory manifest.
    
    Args:
        dir_hash: The directory hash (with .dir suffix).
        source_db: LocalHashFileDB to read the .dir file from.
        
    Returns:
        Set of child file hashes (not including the .dir hash itself).
    """
    from dvc_data.hashfile.tree import Tree
    from dvc_data.hashfile.hash_info import HashInfo
    
    child_hashes = set()
    
    try:
        hi = HashInfo("md5", dir_hash)
        tree = Tree.load(source_db, hi)
        for key, (meta, hash_info) in tree.iteritems():
            child_hashes.add(hash_info.value)
    except Exception:
        # If we can't load the tree, we'll just fetch the .dir hash
        # and handle expansion later
        pass
    
    return child_hashes


def _collect_hashes_from_stage(stage: Any) -> set:
    """Extract all hashes from a stage's outputs.
    
    Args:
        stage: DVC Stage object.
        
    Returns:
        Set of hash strings (may include .dir hashes).
    """
    hashes = set()
    for out in stage.outs:
        if out.use_cache and out.hash_info and out.hash_info.value:
            hashes.add(out.hash_info.value)
    return hashes


def build_fetch_plan(
    categorization: StageCategorization,
    verbose: bool = False,
) -> FetchPlan:
    """Build a fetch plan from categorized stages.
    
    Groups all hashes by their source cache, expanding .dir hashes
    to include child files.
    
    Args:
        categorization: Result from categorize_stages().
        verbose: Print progress information.
        
    Returns:
        FetchPlan with hashes grouped by source.
    """
    plan = FetchPlan()
    
    # Handle regular stages - all from the primary local remote
    if categorization.regular_stages:
        if categorization.has_local_remote:
            remotes = remote.list_remotes()
            local_remote = remote.find_local_remote(remotes)
            if local_remote:
                remote_name, remote_path = local_remote
                source_path = Path(remote_path)
                group = plan.add_source(source_path, remote_name)
                source_db = _create_source_cache_db(source_path)
                
                for stage in categorization.regular_stages:
                    stage_hashes = _collect_hashes_from_stage(stage)
                    for h in stage_hashes:
                        group.add_hash(h)
                        # Expand directory hashes
                        if h.endswith('.dir') and source_db:
                            child_hashes = _expand_dir_hash(h, source_db)
                            group.add_hashes(child_hashes)
        else:
            # No local remote available
            plan.no_source.extend(categorization.regular_stages)
    
    # Handle repo imports - grouped by source repository
    for url, import_group in categorization.repo_imports.items():
        if import_group.has_local_cache and import_group.local_cache:
            source_path = import_group.local_cache
            group = plan.add_source(source_path, import_group.short_name)
            source_db = _create_source_cache_db(source_path)
            
            for stage in import_group.stages:
                stage_hashes = _collect_hashes_from_stage(stage)
                for h in stage_hashes:
                    group.add_hash(h)
                    # Expand directory hashes
                    if h.endswith('.dir') and source_db:
                        child_hashes = _expand_dir_hash(h, source_db)
                        group.add_hashes(child_hashes)
        else:
            # No local cache for this repo
            plan.no_source.extend(import_group.stages)
    
    # URL imports are handled separately
    plan.url_imports = categorization.url_imports
    
    if verbose:
        print(f"\nFetch plan: {plan.total_hashes} hashes from {len(plan.sources)} sources")
        for line in plan.summary_lines():
            print(f"  {line}")
    
    return plan


def fetch_from_plan(
    plan: FetchPlan,
    verbose: bool = False,
    show_progress: bool = True,
    network: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Execute a fetch plan, linking hashes from sources to primary cache.
    
    Args:
        plan: The FetchPlan to execute.
        verbose: Print detailed progress.
        show_progress: Show progress bar.
        network: Fall back to dvc fetch for stages without local source.
        
    Returns:
        List of (source_name, success, message) tuples.
    """
    results = []
    total_fetched = 0
    total_failed = 0
    
    # Handle stages with no local source first (doesn't need Repo)
    if plan.no_source:
        if network:
            if verbose:
                print(f"\nFalling back to dvc fetch for {len(plan.no_source)} stages...")
            for stage in plan.no_source:
                success, msg = _run_dvc_fetch(stage.addressing, verbose)
                results.append((stage.addressing, success, msg))
        else:
            for stage in plan.no_source:
                results.append((stage.addressing, False, "No local source (use --network)"))
    
    # Handle URL imports (doesn't need local cache)
    if plan.url_imports:
        if verbose:
            print(f"\nProcessing {len(plan.url_imports)} URL imports...")
        for stage in plan.url_imports:
            result = _fetch_url_import_stage(stage, verbose=verbose)
            results.append(result)
    
    # Early return if no sources with hashes to fetch
    if plan.total_hashes == 0:
        if verbose and not results:
            print("No hashes to fetch")
        return results
    
    # Now we need the DVC cache for fetching from sources
    from dvc.repo import Repo
    
    repo = Repo()
    cache = repo.cache.local
    if cache is None:
        raise FetchError("DVC cache not configured.")
    
    # Get cache base path (strip files/md5 suffix if present)
    cache_base = str(cache.path)
    if cache_base.endswith('/files/md5') or cache_base.endswith('\\files\\md5'):
        cache_base = str(Path(cache.path).parent.parent)
    
    # Collect all hashes and check what's already cached
    all_hashes = set()
    for group in plan.sources.values():
        all_hashes.update(group.hashes)
    
    existing = set(cache.oids_exist(all_hashes))
    total_missing = len(all_hashes) - len(existing)
    
    if verbose:
        print(f"\nCache status:")
        print(f"  Total hashes: {len(all_hashes)}")
        print(f"  Already cached: {len(existing)}")
        print(f"  Missing: {total_missing}")
    
    if total_missing == 0:
        results.append(("all", True, f"All {len(all_hashes)} hashes already cached"))
        return results
    
    # Fetch from each source
    for source_path, group in plan.sources.items():
        missing_from_source = group.hashes - existing
        if not missing_from_source:
            continue
        
        source_label = f"{group.source_name} ({len(missing_from_source)} files)"
        
        if verbose:
            print(f"\nFetching from {group.source_name}: {len(missing_from_source)} files")
            print(f"  Source: {source_path}")
        
        fetched = 0
        failed = 0
        
        # Use progress bar for non-verbose mode
        if show_progress and not verbose:
            with click.progressbar(
                sorted(missing_from_source),
                label=source_label,
                show_pos=True,
                show_percent=True,
            ) as bar:
                for h in bar:
                    result = cache_ops.populate_cache_file(
                        md5=h,
                        source_cache=source_path,
                        dest_cache=cache_base,
                        verbose=False,
                        use_v3_layout=True,
                    )
                    if result is True:
                        fetched += 1
                    elif result is None:
                        failed += 1
        else:
            for h in sorted(missing_from_source):
                if verbose:
                    print(f"  {h[:12]}...", end=" ")
                result = cache_ops.populate_cache_file(
                    md5=h,
                    source_cache=source_path,
                    dest_cache=cache_base,
                    verbose=False,
                    use_v3_layout=True,
                )
                if result is True:
                    fetched += 1
                    if verbose:
                        print("✓")
                elif result is None:
                    failed += 1
                    if verbose:
                        print("✗ (not found)")
                else:
                    if verbose:
                        print("(exists)")
        
        total_fetched += fetched
        total_failed += failed
        
        if failed > 0:
            results.append((group.source_name, False, f"Fetched {fetched}, failed {failed}"))
        else:
            results.append((group.source_name, True, f"Fetched {fetched} files"))
    
    # Summary
    if verbose:
        print(f"\nFetch complete: {total_fetched} fetched, {total_failed} failed")
    
    return results


# =============================================================================
# Cache Population (legacy - to be removed)
# =============================================================================

def _update_dvc_hash(dvc_path: Path, old_hash: str, new_hash: str, verbose: bool = False) -> bool:
    """Update the MD5 hash in a .dvc file. Wrapper for utils.update_dvc_hash."""
    return utils.update_dvc_hash(dvc_path, old_hash, new_hash, verbose)


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
        # Brief inline status: "finding cache..."
        click.echo("finding cache...", nl=False)
    
    # Step 1: Find a local remote from the source repo (clones if needed)
    if verbose:
        print(f"Looking for local cache...")
    
    result = remote_mod.find_local_remote_from_repo(repo_spec=source_url)
    
    if not result:
        if show_progress and not verbose:
            click.echo(" failed")  # Finish the line
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
        click.echo(" fetching...", nl=False)  # Continue the line
    
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
    dry: bool = False,
    imports: bool = False,
    urls: bool = False,
    regular: bool = False,
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
        dry: If True, only collect and categorize stages without fetching.
        imports: If True, only fetch repo imports. Can combine with urls/regular.
        urls: If True, only fetch URL imports. Can combine with imports/regular.
        regular: If True, only fetch regular stages. Can combine with imports/urls.
        
    Returns:
        List of (target, success, message) tuples.
        In dry mode, returns empty list (summary is printed instead).
        
    Raises:
        FetchError: If fetch fails.
    """
    from dvc.stage.exceptions import StageFileDoesNotExistError
    from dvc.scm import SCMError
    from . import doctor
    
    # If none of the type filters are specified, fetch all
    fetch_all = not (imports or urls or regular)
    
    # Run environment checks
    env = doctor.check_environment()
    env.require_git_repo()
    
    # Also verify DVC is installed
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise FetchError(str(e))
    
    # Collect stages using DVC's internal index
    try:
        stages = utils.collect_stages(targets=targets, verbose=verbose)
    except StageFileDoesNotExistError as e:
        raise FetchError(str(e))
    except SCMError:
        # This shouldn't happen since we checked above, but handle it anyway
        env.require_git_repo()
    
    if verbose:
        print(f"Found {len(stages)} stage(s) to process")
    
    # Categorize stages
    categorization = categorize_stages(stages, verbose=False)
    
    # Apply type filters
    if not fetch_all:
        if not imports:
            categorization.repo_imports = {}
        if not urls:
            categorization.url_imports = []
        if not regular:
            categorization.regular_stages = []
    
    # In dry mode, just print the summary and return
    if dry:
        print(f"\nStage categorization ({categorization.total_stages} total):")
        categorization.print_summary(verbose=verbose)
        return []
    
    # Build and execute fetch plan (new simplified model)
    plan = build_fetch_plan(categorization, verbose=verbose)
    return fetch_from_plan(
        plan=plan,
        verbose=verbose,
        show_progress=show_progress,
        network=network,
    )


def _fetch_from_stages(
    categorization: StageCategorization,
    verbose: bool = False,
    update: bool = False,
    show_progress: bool = True,
    network: bool = False,
    include_imports: bool = True,
    include_urls: bool = True,
    include_regular: bool = True,
) -> List[Tuple[str, bool, str]]:
    """Fetch from categorized stages using bulk operations.
    
    Uses DVC's bulk cache checking to efficiently determine which
    objects need to be fetched, then fetches only the missing ones.
    
    Args:
        categorization: Pre-categorized stages from categorize_stages().
        verbose: Print progress information.
        update: If True, rebuild .dir files and update .dvc hashes.
        show_progress: If True, show progress bar.
        network: If True, fall back to dvc fetch for stages without local cache.
        include_imports: If True, process repo imports.
        include_urls: If True, process URL imports.
        include_regular: If True, process regular stages.
        
    Returns:
        List of (target, success, message) tuples.
    """
    from . import doctor
    
    results = []
    
    if categorization.total_stages == 0:
        return results
    
    if verbose:
        print(f"Stage breakdown:")
        if include_regular:
            print(f"  Regular stages: {len(categorization.regular_stages)}")
        if include_imports:
            print(f"  Repo imports: {categorization.repo_import_count}")
        if include_urls:
            print(f"  URL imports: {len(categorization.url_imports)}")
    
    # Phase 1: Bulk fetch regular stages
    if include_regular and categorization.regular_stages:
        if verbose:
            print(f"\nPhase 1: Processing {len(categorization.regular_stages)} regular stages...")
        results.extend(_fetch_regular_stages_bulk(
            stages=categorization.regular_stages,
            verbose=verbose,
            show_progress=show_progress,
            network=network,
        ))
        if verbose:
            print(f"  Regular stages complete.")
    
    # Phase 2: Handle repo imports (grouped by source repo)
    if include_imports and categorization.repo_imports:
        if verbose:
            print(f"\nPhase 2: Processing {categorization.repo_import_count} repo imports...")
            print(f"  From {len(categorization.repo_imports)} source repositories")
        
        for url, group in categorization.repo_imports.items():
            if verbose:
                cache_status = "has local cache" if group.has_local_cache else "no local cache"
                print(f"  {group.short_name} ({group.count} stages, {cache_status})")
            
            for i, stage in enumerate(group.stages):
                if verbose:
                    print(f"    [{i+1}/{group.count}] {stage.addressing}")
                result = _fetch_import_stage(
                    stage=stage,
                    verbose=verbose,
                    update=update,
                    show_progress=show_progress,
                )
                results.append(result)
        
        if verbose:
            print(f"  Repo imports complete.")
    
    # Phase 3: Handle URL imports
    if include_urls and categorization.url_imports:
        if verbose:
            print(f"\nPhase 3: Processing {len(categorization.url_imports)} URL imports...")
        # Check network access before attempting URL imports
        has_network = doctor.check_network_connectivity(timeout=1.0)
        if verbose:
            if has_network:
                print("  Network access: available")
            else:
                print("  Network access: not available")
                print("  Note: URL imports may fail without network access.")
        
        for stage in categorization.url_imports:
            result = _fetch_url_import_stage(
                stage=stage,
                verbose=verbose,
            )
            results.append(result)
    
    return results


def _fetch_regular_stages_bulk(
    stages: List[Any],
    verbose: bool = False,
    show_progress: bool = True,
    network: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Fetch regular stages using bulk cache checking.
    
    Collects all needed hashes, checks which are missing from cache
    in one bulk operation, then fetches only the missing files.
    """
    from dvc.repo import Repo
    from dvc.stage import PipelineStage
    from . import import_data as import_mod
    
    results = []
    
    # Collect all hashes and map them back to stages
    hash_to_stages = {}  # hash -> list of (stage, out)
    all_hashes = set()
    
    for stage in stages:
        if not stage.outs:
            results.append((stage.addressing, True, "No outputs"))
            continue
        
        for out in stage.outs:
            if out.use_cache and out.hash_info and out.hash_info.value:
                h = out.hash_info.value
                all_hashes.add(h)
                if h not in hash_to_stages:
                    hash_to_stages[h] = []
                hash_to_stages[h].append((stage, out))
    
    if not all_hashes:
        return results
    
    # Bulk check which hashes are in cache
    try:
        repo = Repo()
        cache = repo.cache.local
        if cache is None:
            raise FetchError(
                "DVC cache not configured.\n"
                "Run 'dvc cache dir' to check your cache configuration."
            )
        existing = set(cache.oids_exist(all_hashes))
        missing = all_hashes - existing
    except FetchError:
        raise
    except Exception as e:
        raise FetchError(
            f"Failed to check cache status: {e}\n"
            f"This may indicate a corrupted DVC cache or configuration issue.\n"
            f"Try running 'dt doctor' to diagnose the problem."
        )
    
    if verbose:
        print(f"\nCache status:")
        print(f"  Total unique hashes: {len(all_hashes)}")
        print(f"  Already in cache: {len(existing)}")
        print(f"  Missing: {len(missing)}")
    
    # Mark fully-cached stages as done
    cached_stages = set()
    for stage in stages:
        if not stage.outs:
            continue
        stage_hashes = {out.hash_info.value for out in stage.outs 
                       if out.use_cache and out.hash_info and out.hash_info.value}
        if stage_hashes and stage_hashes.issubset(existing):
            cached_stages.add(stage.addressing)
            if show_progress and not verbose:
                click.echo(f"{stage.addressing}: ✓ (cached)")
            results.append((stage.addressing, True, "Already in cache"))
    
    if not missing:
        return results
    
    # Find a local remote for fetching
    if verbose:
        print("\nChecking remote access...")
    remotes = remote.list_remotes()
    if verbose:
        print(f"  Found {len(remotes)} remote(s)")
    local_remote_info, access_error = remote.check_remote_access(remotes)
    
    if not local_remote_info and not network:
        # No local remote and network disabled - provide useful error message
        if access_error:
            if verbose:
                print(f"  {access_error}")
                # Check if it looks like an unmounted volume
                if '/g/data/' in access_error or '/scratch/' in access_error:
                    print("  Hint: Check if the volume is mounted on this node.")
            error_msg = access_error
        else:
            if verbose:
                print("  No local remote configured")
            error_msg = "No local remote available (use --network)"
        
        if verbose:
            print(f"\nReporting errors for {len(missing)} missing hashes...")
        
        for h in missing:
            for stage, out in hash_to_stages.get(h, []):
                if stage.addressing not in cached_stages:
                    if show_progress and not verbose:
                        click.echo(f"{stage.addressing}: ✗ ({error_msg})")
                    results.append((stage.addressing, False, error_msg))
                    cached_stages.add(stage.addressing)  # Mark as handled
        
        if verbose:
            print(f"  Done. Returning {len(results)} results.")
        return results
    
    # Fetch missing files
    if local_remote_info:
        remote_name, remote_path = local_remote_info
        if verbose:
            print(f"\nFetching from local remote '{remote_name}': {remote_path}")
        
        # Get cache base path
        cache_base = str(cache.path)
        if cache_base.endswith('/files/md5') or cache_base.endswith('\\files\\md5'):
            cache_base = str(Path(cache.path).parent.parent)
        
        # Detect layout (v3 preferred)
        use_v3_layout = True
        
        # Fetch with progress bar (when not verbose and multiple files)
        fetched = 0
        failed_hashes = set()
        
        if not verbose and show_progress and len(missing) > 1:
            with click.progressbar(
                missing,
                label=f"Fetching {len(missing)} objects",
                show_pos=True,
                show_percent=True,
            ) as bar:
                for h in bar:
                    result = import_mod.populate_cache_file(
                        md5=h,
                        source_cache=remote_path,
                        dest_cache=cache_base,
                        verbose=False,  # Quiet during progress
                        use_v3_layout=use_v3_layout,
                    )
                    if result is True:
                        fetched += 1
                    elif result is None:
                        failed_hashes.add(h)
        else:
            for h in missing:
                if verbose:
                    print(f"  Fetching {h[:12]}...")
                result = import_mod.populate_cache_file(
                    md5=h,
                    source_cache=remote_path,
                    dest_cache=cache_base,
                    verbose=verbose,
                    use_v3_layout=use_v3_layout,
                )
                if result is True:
                    fetched += 1
                elif result is None:
                    failed_hashes.add(h)
        
        if verbose:
            print(f"\nFetch summary: {fetched} fetched, {len(failed_hashes)} failed")
        
        # Build results for stages with missing hashes
        for stage in stages:
            if stage.addressing in cached_stages:
                continue  # Already handled
            
            stage_hashes = {out.hash_info.value for out in stage.outs 
                          if out.use_cache and out.hash_info and out.hash_info.value}
            stage_missing = stage_hashes & missing
            stage_failed = stage_missing & failed_hashes
            
            if stage_failed:
                if show_progress and not verbose:
                    click.echo(f"{stage.addressing}: ✗ ({len(stage_failed)} files not in remote)")
                results.append((stage.addressing, False, 
                    f"Failed: {len(stage_failed)} files not found in remote"))
            else:
                if show_progress and not verbose:
                    click.echo(f"{stage.addressing}: ✓ (fetched)")
                results.append((stage.addressing, True, 
                    f"Fetched {len(stage_missing)} files from {remote_name}"))
    
    elif network:
        # Fall back to dvc fetch for network mode
        if verbose:
            print("\nNo local remote, using dvc fetch...")
        
        for stage in stages:
            if stage.addressing in cached_stages:
                continue
            
            success, msg = _run_dvc_fetch(stage.addressing, verbose)
            if show_progress and not verbose:
                status = "✓" if success else "✗"
                click.echo(f"{stage.addressing}: {status} ({msg})")
            results.append((stage.addressing, success, msg))
    
    return results


def _fetch_import_stage(
    stage: Any,
    verbose: bool = False,
    update: bool = False,
    show_progress: bool = True,
) -> Tuple[str, bool, str]:
    """Fetch a repo import stage."""
    from dvc.stage import PipelineStage
    
    stage_name = stage.addressing
    stage_path = Path(stage.path) if hasattr(stage, 'path') else None
    is_pipeline = isinstance(stage, PipelineStage)
    
    # Check if already cached
    if stage.outs:
        needs_fetch = False
        for out in stage.outs:
            if out.use_cache and out.hash_info:
                if out.changed_cache():
                    needs_fetch = True
                    break
        
        if not needs_fetch:
            if show_progress and not verbose:
                click.echo(f"{stage_name}: ✓ (cached)")
            return (stage_name, True, "Already in cache")
    
    if show_progress and not verbose:
        click.echo(f"{stage_name}: ", nl=False)
    
    if not stage_path or is_pipeline:
        if show_progress and not verbose:
            click.echo("Error (repo imports must be .dvc files)")
        return (stage_name, False, "Repo imports must be .dvc files")
    
    try:
        cache_path, count, failed = fetch_import(
            dvc_path=stage_path,
            verbose=verbose,
            update=update,
            show_progress=show_progress,
        )
        if failed > 0:
            return (stage_name, False, 
                f"FAILED: {failed} file(s) not found in source cache at {cache_path}")
        elif count == 0:
            return (stage_name, True, f"Already in cache (from {cache_path})")
        else:
            return (stage_name, True, f"Fetched {count} files from {cache_path}")
    except FetchError as e:
        if show_progress and not verbose:
            click.echo(f"Error: {e}")
        return (stage_name, False, str(e))


def _fetch_url_import_stage(
    stage: Any,
    verbose: bool = False,
) -> Tuple[str, bool, str]:
    """Fetch a URL import stage."""
    from dvc.stage import PipelineStage
    
    stage_name = stage.addressing
    stage_path = Path(stage.path) if hasattr(stage, 'path') else None
    is_pipeline = isinstance(stage, PipelineStage)
    
    # Check if already cached
    if stage.outs:
        needs_fetch = False
        for out in stage.outs:
            if out.use_cache and out.hash_info:
                if out.changed_cache():
                    needs_fetch = True
                    break
        
        if not needs_fetch:
            click.echo(f"{stage_name}: ✓ (cached)")
            return (stage_name, True, "Already in cache")
    
    click.echo(f"{stage_name}: ", nl=False)
    
    if not stage_path or is_pipeline:
        click.echo("Error (URL imports must be .dvc files)")
        return (stage_name, False, "URL imports must be .dvc files")
    
    result = _fetch_url_import(
        dvc_path=stage_path,
        verbose=verbose,
    )
    if result[0]:
        click.echo(f"✓ ({result[1]})")
    else:
        click.echo(f"Error: {result[1]}")
    return (stage_name, result[0], result[1])


def _fetch_directory_from_remote(
    remote_path: str,
    cache_base: str,
    md5: str,
    verbose: bool = False,
    use_v3_layout: bool = True,
) -> Tuple[int, int]:
    """Fetch a directory and its contents from a remote.
    
    Fetches the .dir manifest file and all individual files listed in it.
    
    Args:
        remote_path: Path to the source remote/cache.
        cache_base: Path to the destination cache base directory.
        md5: The directory hash (with .dir suffix).
        verbose: Print progress messages.
        use_v3_layout: Use v3 cache layout (files/md5/).
        
    Returns:
        Tuple of (files_fetched, files_failed) counts.
    """
    import json
    from . import import_data as import_mod
    
    count = 0
    failed = 0
    
    # First fetch the .dir file itself
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
        # .dir file not found - can't proceed
        if verbose:
            print(f"  ERROR: .dir file not found in remote: {md5}")
        return (0, 1)
    
    # Find the .dir file to read entries
    dir_hash = md5[:-4]  # Remove .dir suffix
    
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
    
    if not dir_file:
        if verbose:
            print(f"  Warning: .dir file not found after fetch")
        return (count, 1)
    
    # Read entries and fetch each file
    try:
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
    
    return (count, failed)


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
    timeout: int = 60,
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
        timeout: Timeout in seconds for the update operation (default 60).
        
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
    
    # Check network access before attempting download
    if source_url.startswith(('http://', 'https://', 's3://', 'gs://')):
        from . import doctor
        if not doctor.check_network_connectivity(timeout=3.0):
            return (False, f"No network access (cannot fetch from {source_url})")
    
    if verbose:
        print(f"  URL import from: {source_url}")
        print(f"  Running: dvc update {dvc_path}")
    
    cmd = ['dvc', 'update', str(dvc_path)]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
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
            if 'timeout' in error_msg.lower():
                return (False, f"Timeout fetching from {source_url}")
            return (False, f"dvc update failed: {error_msg}")
    except subprocess.TimeoutExpired:
        return (False, f"Timeout after {timeout}s fetching from {source_url}")
    except (OSError, FileNotFoundError) as e:
        return (False, f"dvc update failed: {e}")

