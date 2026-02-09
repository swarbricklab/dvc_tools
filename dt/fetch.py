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
    local_cache_error: Optional[str] = None  # Error if remote looks local but not accessible
    
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
    local_remote_error: Optional[str] = None  # Error if remote looks local but not accessible
    
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
                            found, error = remote.check_remote_access_from_repo(url)
                            if found:
                                group.local_cache = Path(found[1])
                                group.has_local_cache = True
                            elif error:
                                # Remote looks local but path doesn't exist
                                group.local_cache_error = error
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
            found, error = remote.check_remote_access(remotes)
            if found:
                result.has_local_remote = True
                result.local_remote_name = found[0]
            elif error:
                # Remote looks local but path doesn't exist (e.g., unmounted volume)
                result.local_remote_error = error
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
    # Track which stage each hash came from (for error reporting)
    hash_stages: Dict[str, str] = field(default_factory=dict)
    # Track the path for each hash (for error reporting)
    hash_paths: Dict[str, str] = field(default_factory=dict)
    
    def add_hash(self, h: str, stage_name: str = None, path: str = None) -> None:
        """Add a hash to fetch from this source."""
        self.hashes.add(h)
        if stage_name:
            self.hash_stages[h] = stage_name
        if path:
            self.hash_paths[h] = path
    
    def add_hashes_with_paths(self, hash_path_pairs: List[Tuple[str, str]], stage_name: str = None) -> None:
        """Add multiple hashes with their paths to fetch from this source."""
        for h, path in hash_path_pairs:
            self.hashes.add(h)
            if stage_name:
                self.hash_stages[h] = stage_name
            if path:
                self.hash_paths[h] = path
    
    def get_stage_for_hash(self, h: str) -> Optional[str]:
        """Get the stage name that a hash came from."""
        return self.hash_stages.get(h)
    
    def get_path_for_hash(self, h: str) -> Optional[str]:
        """Get the path that a hash came from."""
        return self.hash_paths.get(h)


@dataclass 
class FetchPlan:
    """Plan for fetching hashes from multiple sources.
    
    Groups hashes by source cache, allowing efficient bulk fetching.
    URL imports are handled separately (require network download).
    """
    sources: Dict[str, SourceGroup] = field(default_factory=dict)
    url_imports: List[Any] = field(default_factory=list)
    no_source: List[Any] = field(default_factory=list)  # Stages with no local source
    no_source_errors: Dict[str, str] = field(default_factory=dict)  # Stage addressing -> error message
    
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
    base_path: str = None,
) -> List[Tuple[str, str]]:
    """Expand a .dir hash to get all child file hashes with their paths.
    
    Uses DVC's Tree.load() to read the directory manifest.
    
    Args:
        dir_hash: The directory hash (with .dir suffix).
        source_db: LocalHashFileDB to read the .dir file from.
        base_path: Base path to prepend to child paths.
        
    Returns:
        List of (hash, path) tuples for child files.
    """
    from dvc_data.hashfile.tree import Tree
    from dvc_data.hashfile.hash_info import HashInfo
    
    child_files = []
    
    try:
        hi = HashInfo("md5", dir_hash)
        tree = Tree.load(source_db, hi)
        for key, (meta, hash_info) in tree.iteritems():
            # key is a tuple of path components, e.g., ('subdir', 'file.txt')
            rel_path = '/'.join(key) if key else ''
            if base_path:
                full_path = f"{base_path}/{rel_path}" if rel_path else base_path
            else:
                full_path = rel_path
            child_files.append((hash_info.value, full_path))
    except Exception:
        # If we can't load the tree, we'll just fetch the .dir hash
        # and handle expansion later
        pass
    
    return child_files


def _collect_hashes_from_stage(stage: Any) -> List[Tuple[str, str]]:
    """Extract all hashes from a stage's outputs with their paths.
    
    Args:
        stage: DVC Stage object.
        
    Returns:
        List of (hash, path) tuples (may include .dir hashes).
        Paths are relative to the repo root.
    """
    hash_paths = []
    # Get repo root to make paths relative
    repo_root = None
    if hasattr(stage, 'repo') and stage.repo:
        repo_root = Path(stage.repo.root_dir)
    
    for out in stage.outs:
        if out.use_cache and out.hash_info and out.hash_info.value:
            # DVC uses fspath for the filesystem path
            path = None
            if hasattr(out, 'fspath') and out.fspath:
                abs_path = Path(out.fspath)
                # Make relative to repo root if possible
                if repo_root and abs_path.is_absolute():
                    try:
                        path = str(abs_path.relative_to(repo_root))
                    except ValueError:
                        path = str(abs_path)
                else:
                    path = str(abs_path)
            hash_paths.append((out.hash_info.value, path))
    return hash_paths


def build_fetch_plan(
    categorization: StageCategorization,
    verbose: bool = False,
    explicit_source: Optional[Path] = None,
) -> FetchPlan:
    """Build a fetch plan from categorized stages.
    
    Groups all hashes by their source cache, expanding .dir hashes
    to include child files.
    
    Args:
        categorization: Result from categorize_stages().
        verbose: Print progress information.
        explicit_source: Explicit source cache path. If provided, all stages
            use this as the source instead of auto-discovered sources.
        
    Returns:
        FetchPlan with hashes grouped by source.
    """
    plan = FetchPlan()
    
    # If explicit source is provided, use it for all stages
    if explicit_source:
        source_path = explicit_source
        source_name = f"explicit ({source_path.name})"
        group = plan.add_source(source_path, source_name)
        source_db = _create_source_cache_db(source_path)
        
        if verbose:
            print(f"Using explicit source cache: {source_path}")
        
        # Add all stages (regular, repo imports, etc.) to the same source
        all_stages = (
            categorization.regular_stages +
            [stage for import_group in categorization.repo_imports.values() 
             for stage in import_group.stages]
        )
        
        for stage in all_stages:
            stage_hash_paths = _collect_hashes_from_stage(stage)
            stage_name = stage.addressing
            for h, path in stage_hash_paths:
                group.add_hash(h, stage_name=stage_name, path=path)
                # Expand directory hashes
                if h.endswith('.dir') and source_db:
                    child_files = _expand_dir_hash(h, source_db, base_path=path)
                    group.add_hashes_with_paths(child_files, stage_name=stage_name)
        
        # URL imports are still handled separately
        plan.url_imports = categorization.url_imports
        
        if verbose:
            print(f"\nFetch plan: {plan.total_hashes} hashes from explicit source")
            for line in plan.summary_lines():
                print(f"  {line}")
        
        return plan
    
    # Standard auto-discovery mode
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
                    stage_hash_paths = _collect_hashes_from_stage(stage)
                    stage_name = stage.addressing
                    for h, path in stage_hash_paths:
                        group.add_hash(h, stage_name=stage_name, path=path)
                        # Expand directory hashes
                        if h.endswith('.dir') and source_db:
                            child_files = _expand_dir_hash(h, source_db, base_path=path)
                            group.add_hashes_with_paths(child_files, stage_name=stage_name)
        else:
            # No local remote available - track with error message if available
            for stage in categorization.regular_stages:
                plan.no_source.append(stage)
                if categorization.local_remote_error:
                    plan.no_source_errors[stage.addressing] = categorization.local_remote_error
    
    # Handle repo imports - grouped by source repository
    for url, import_group in categorization.repo_imports.items():
        if import_group.has_local_cache and import_group.local_cache:
            source_path = import_group.local_cache
            group = plan.add_source(source_path, import_group.short_name)
            source_db = _create_source_cache_db(source_path)
            
            for stage in import_group.stages:
                stage_hash_paths = _collect_hashes_from_stage(stage)
                stage_name = stage.addressing
                for h, path in stage_hash_paths:
                    group.add_hash(h, stage_name=stage_name, path=path)
                    # Expand directory hashes
                    if h.endswith('.dir') and source_db:
                        child_files = _expand_dir_hash(h, source_db, base_path=path)
                        group.add_hashes_with_paths(child_files, stage_name=stage_name)
        else:
            # No local cache for this repo - track with error message if available
            for stage in import_group.stages:
                plan.no_source.append(stage)
                if import_group.local_cache_error:
                    plan.no_source_errors[stage.addressing] = import_group.local_cache_error
    
    # URL imports are handled separately
    plan.url_imports = categorization.url_imports
    
    if verbose:
        print(f"\nFetch plan: {plan.total_hashes} hashes from {len(plan.sources)} sources")
        for line in plan.summary_lines():
            print(f"  {line}")
    
    return plan


def _recover_dir_failures(
    failures: List[Tuple[str, str]],
    verbose: bool = False,
    show_progress: bool = True,
    destination: Optional[Path] = None,
) -> List[Tuple[str, bool, str]]:
    """Attempt to recover from .dir failures by running dt update.
    
    For each failed .dir, runs dt update with --rev set to the current
    locked revision (no HEAD check), then re-fetches the stage.
    
    Args:
        failures: List of (hash, stage_name) tuples for failed .dir hashes.
        verbose: Print detailed progress.
        show_progress: Show progress bar for re-fetch.
        destination: Explicit destination cache path. If None, uses primary cache.
        
    Returns:
        List of (target, success, message) tuples for recovery attempts.
    """
    from . import update as update_mod
    
    results = []
    
    # Group failures by stage (multiple .dir hashes might come from same stage, though unlikely)
    stages_to_recover = set(stage_name for _, stage_name in failures)
    
    print(f"\nRebuilding {len(stages_to_recover)} missing .dir manifests...")
    
    for stage_name in sorted(stages_to_recover):
        # Get the rev_lock from the .dvc file to avoid HEAD checks
        stage_path = Path(stage_name)
        import_info = utils.get_import_info(stage_path)
        
        if not import_info:
            results.append((stage_name, False, "Could not read import info"))
            continue
        
        rev_lock = import_info.get('rev')
        if not rev_lock:
            results.append((stage_name, False, "No rev_lock in .dvc file"))
            continue
        
        if verbose:
            print(f"  {stage_name}: rebuilding .dir at rev {rev_lock[:12]}...")
        
        # Run dt update with explicit --rev to skip HEAD comparison
        try:
            update_results = update_mod.update(
                targets=[stage_name],
                rev=rev_lock,  # Use locked rev, not HEAD
                verbose=verbose,
                no_download=False,  # Let update call fetch after rebuilding
                dry_run=False,
                cache=str(destination) if destination else None,
            )
            
            # Check if update succeeded
            for target, success, msg in update_results:
                if success:
                    results.append((target, True, f"Rebuilt .dir: {msg}"))
                else:
                    results.append((target, False, f"Failed to rebuild .dir: {msg}"))
                    
        except Exception as e:
            results.append((stage_name, False, f"Failed to rebuild .dir: {e}"))
    
    return results


def fetch_from_plan(
    plan: FetchPlan,
    verbose: bool = False,
    show_progress: bool = True,
    network: bool = False,
    update: bool = False,
    destination: Optional[Path] = None,
    cache_type: Optional[str] = None,
) -> List[Tuple[str, bool, str]]:
    """Execute a fetch plan, linking hashes from sources to primary cache.
    
    Args:
        plan: The FetchPlan to execute.
        verbose: Print detailed progress.
        show_progress: Show progress bar.
        network: Fall back to dvc fetch for stages without local source.
        update: If True, attempt to recover from .dir failures by running dt update.
        destination: Explicit destination cache path. If None, uses primary cache.
        cache_type: Link type for cache population (reflink, hardlink, symlink, copy).
            If None, tries all in order until one succeeds.
        
    Returns:
        List of (source_name, success, message) tuples.
    """
    results = []
    total_fetched = 0
    total_failed = 0
    
    # Track .dir failures for potential recovery with --update
    recoverable_dir_failures = []  # List of (hash, stage_name) tuples
    
    # Handle stages with no local source first (doesn't need Repo)
    if plan.no_source:
        if network:
            if verbose:
                print(f"\nFalling back to dvc fetch for {len(plan.no_source)} stages...")
            for stage in plan.no_source:
                success, msg = _run_dvc_fetch(stage.addressing, verbose)
                results.append((stage.addressing, success, msg))
        else:
            # Show detailed errors for stages that look like they should be local
            shown_errors = set()
            for stage in plan.no_source:
                error = plan.no_source_errors.get(stage.addressing)
                if error:
                    # Show unique errors only once
                    if error not in shown_errors:
                        print(f"Warning: {error}")
                        print("  (Check if the required volume is mounted)")
                        shown_errors.add(error)
                    results.append((stage.addressing, False, error))
                else:
                    results.append((stage.addressing, False, "No local source (use --network)"))
    
    # Handle URL imports (require network access)
    if plan.url_imports:
        if network:
            if verbose:
                print(f"\nProcessing {len(plan.url_imports)} URL imports...")
            for stage in plan.url_imports:
                result = _fetch_url_import_stage(stage, verbose=verbose)
                results.append(result)
        else:
            # Skip URL imports when network=False
            for stage in plan.url_imports:
                results.append((stage.addressing, False, "URL import requires network (use --network)"))
    
    # Early return if no sources with hashes to fetch
    if plan.total_hashes == 0:
        if verbose and not results:
            print("No hashes to fetch")
        return results
    
    # Determine destination cache
    if destination:
        # Use explicit destination cache
        cache_base = str(destination)
        if verbose:
            print(f"Using explicit destination cache: {cache_base}")
        
        # For explicit destination, create a simple DB to check existing hashes
        dest_db = _create_source_cache_db(destination)
    else:
        # Use DVC's primary cache
        from dvc.repo import Repo
        
        repo = Repo()
        cache = repo.cache.local
        if cache is None:
            raise FetchError("DVC cache not configured.")
        
        # Get cache base path (strip files/md5 suffix if present)
        cache_base = str(cache.path)
        if cache_base.endswith('/files/md5') or cache_base.endswith('\\files\\md5'):
            cache_base = str(Path(cache.path).parent.parent)
        
        dest_db = cache
    
    # Collect all hashes and check what's already cached
    all_hashes = set()
    for group in plan.sources.values():
        all_hashes.update(group.hashes)
    
    # Check existing hashes in destination
    if hasattr(dest_db, 'oids_exist'):
        # DVC cache object
        existing = set(dest_db.oids_exist(all_hashes))
    elif dest_db is not None:
        # LocalHashFileDB - check manually
        existing = set()
        for h in all_hashes:
            if cache_ops.find_source_file(h, Path(cache_base)) is not None:
                existing.add(h)
    else:
        # No existing destination - assume empty
        existing = set()
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
        failed_hashes = []  # Track (hash, reason) for verbose reporting
        
        # Always use progress bar when show_progress is True (even in verbose mode)
        if show_progress:
            with click.progressbar(
                sorted(missing_from_source),
                label=source_label,
                show_pos=True,
                show_percent=True,
            ) as bar:
                for h in bar:
                    # Check source exists first so we can report why it failed
                    source_file = cache_ops.find_source_file(h, Path(source_path))
                    if source_file is None:
                        failed += 1
                        failed_hashes.append((h, "not found in source"))
                        continue
                    
                    result = cache_ops.populate_cache_file(
                        md5=h,
                        source_cache=source_path,
                        dest_cache=cache_base,
                        verbose=False,
                        use_v3_layout=True,
                        cache_type=cache_type,
                    )
                    if result is True:
                        fetched += 1
                    elif result is None:
                        failed += 1
                        failed_hashes.append((h, "link failed"))
        else:
            # No progress bar - just process silently
            for h in sorted(missing_from_source):
                source_file = cache_ops.find_source_file(h, Path(source_path))
                if source_file is None:
                    failed += 1
                    failed_hashes.append((h, "not found in source"))
                    continue
                
                result = cache_ops.populate_cache_file(
                    md5=h,
                    source_cache=source_path,
                    dest_cache=cache_base,
                    verbose=False,
                    use_v3_layout=True,
                    cache_type=cache_type,
                )
                if result is True:
                    fetched += 1
                elif result is None:
                    failed += 1
                    failed_hashes.append((h, "link failed"))
        
        total_fetched += fetched
        total_failed += failed
        
        # Report failures (always show, not just verbose mode)
        dir_failures = []
        other_failures = []
        
        if failed_hashes:
            # Separate .dir failures (which may need dt update) from other failures
            dir_failures = [(h, r) for h, r in failed_hashes if h.endswith('.dir')]
            other_failures = [(h, r) for h, r in failed_hashes if not h.endswith('.dir')]
            
            if other_failures:
                print(f"  Failed files ({len(other_failures)}):")
                for h, reason in other_failures:
                    stage_name = group.get_stage_for_hash(h)
                    path = group.get_path_for_hash(h)
                    # Show both stage name and path when available
                    if stage_name and path:
                        print(f"    {h} ({stage_name}: {path}): {reason}")
                    elif path:
                        print(f"    {h} ({path}): {reason}")
                    elif stage_name:
                        print(f"    {h} ({stage_name}): {reason}")
                    else:
                        print(f"    {h}: {reason}")
            
            if dir_failures:
                # Track for potential recovery (only for import .dvc files, not pipeline stages)
                recoverable_dirs = []
                non_recoverable_dirs = []
                for h, reason in dir_failures:
                    stage_name = group.get_stage_for_hash(h)
                    path = group.get_path_for_hash(h)
                    # Only repo imports (ending in .dvc) can be recovered via dt update
                    if stage_name and stage_name.endswith('.dvc'):
                        recoverable_dir_failures.append((h, stage_name))
                        recoverable_dirs.append((h, reason, stage_name, path))
                    else:
                        non_recoverable_dirs.append((h, reason, stage_name, path))
                
                # Always report non-recoverable .dir failures (pipeline stages)
                if non_recoverable_dirs:
                    print(f"  Failed .dir manifests ({len(non_recoverable_dirs)}):")
                    for h, reason, stage_name, path in non_recoverable_dirs:
                        # Show both stage name and path when available
                        if stage_name and path:
                            print(f"    {h} ({stage_name}: {path}): {reason}")
                        elif path:
                            print(f"    {h} ({path}): {reason}")
                        elif stage_name:
                            print(f"    {h} ({stage_name}): {reason}")
                        else:
                            print(f"    {h}: {reason}")
                
                # Report recoverable .dir failures only if --update is not set
                if recoverable_dirs and not update:
                    print(f"  Failed .dir manifests ({len(recoverable_dirs)}):")
                    for h, reason, stage_name, path in recoverable_dirs:
                        # Show both stage name and path when available
                        if stage_name and path:
                            print(f"    {h} ({stage_name}: {path}): {reason}")
                        elif path:
                            print(f"    {h} ({path}): {reason}")
                        else:
                            print(f"    {h} ({stage_name}): {reason}")
                    print(f"  Hint: .dir files may need rebuilding. Try: dt fetch --update")
        
        # Count .dir failures separately if --update will handle them
        effective_failed = failed
        if update and dir_failures:
            # Only subtract recoverable .dir failures, keep non-recoverable ones
            recoverable_count = sum(1 for h, _ in dir_failures 
                                   if group.get_stage_for_hash(h) and 
                                   group.get_stage_for_hash(h).endswith('.dvc'))
            effective_failed = failed - recoverable_count
        
        if effective_failed > 0:
            results.append((group.source_name, False, f"Fetched {fetched}, failed {effective_failed}"))
        else:
            results.append((group.source_name, True, f"Fetched {fetched} files"))
    
    # Attempt recovery for .dir failures if --update is set
    if update and recoverable_dir_failures:
        recovery_results = _recover_dir_failures(
            failures=recoverable_dir_failures,
            verbose=verbose,
            show_progress=show_progress,
            destination=destination,
        )
        results.extend(recovery_results)
    
    # Summary - always show if there were failures, or if verbose
    if total_failed > 0 or verbose:
        print(f"\nFetch complete: {total_fetched} fetched, {total_failed} failed")
    
    return results


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
    source: Optional[str] = None,
    destination: Optional[str] = None,
    cache_type: Optional[str] = None,
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
        source: Explicit source cache path (overrides auto-discovery).
        destination: Explicit destination cache path (overrides primary cache).
        cache_type: Link type for cache population (reflink, hardlink, symlink, copy).
            If None, tries all in order until one succeeds.
        
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
    
    # Show initial status so user knows we're working
    if verbose:
        print("Collecting stages...")
    
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
        print(f"Found {len(stages)} stage(s)")
        print("Categorizing stages...")
    
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
    plan = build_fetch_plan(
        categorization,
        verbose=verbose,
        explicit_source=Path(source) if source else None,
    )
    return fetch_from_plan(
        plan=plan,
        verbose=verbose,
        show_progress=show_progress,
        network=network,
        update=update,
        destination=Path(destination) if destination else None,
        cache_type=cache_type,
    )


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

