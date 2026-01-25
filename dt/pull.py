"""Pull DVC-tracked files, automatically handling imports.

For targets tracked by import .dvc files (those with deps.repo), uses
dt checkout to fetch from the source repo's cache. For other targets,
uses regular dvc pull.
"""

import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from .checkout import (
    CheckoutError,
    is_import_dvc,
    parse_dvc_file,
    smart_checkout,
)


def find_project_root(start: Path) -> Optional[Path]:
    """Find the DVC project root (directory containing .dvc/).
    
    Args:
        start: Starting path for the search.
        
    Returns:
        Path to project root, or None if not in a DVC project.
    """
    current = start.resolve()
    while current != current.parent:
        if (current / ".dvc").is_dir():
            return current
        current = current.parent
    return None


def resolve_to_dvc_file(target: str) -> Optional[Path]:
    """Resolve a target to its tracking .dvc file.
    
    Resolution order:
    1. If target ends with .dvc, return it if it exists
    2. If {target}.dvc exists, return it
    3. Check parent directories for a .dvc file tracking the parent
       (stops at project root - directory containing .dvc/)
    
    Args:
        target: Target path (file, directory, or .dvc file).
        
    Returns:
        Path to the tracking .dvc file, or None if not found.
    """
    target_path = Path(target)
    
    # If it's already a .dvc file
    if target.endswith('.dvc'):
        if target_path.exists():
            return target_path
        return None
    
    # Check if {target}.dvc exists
    dvc_path = Path(f"{target}.dvc")
    if dvc_path.exists():
        return dvc_path
    
    # Find project root to know when to stop
    project_root = find_project_root(Path.cwd())
    if project_root is None:
        return None
    
    # Check parent directories up to project root
    # For data/subdir/file.txt, check data/subdir.dvc, data.dvc, etc.
    current = target_path.resolve()
    while current >= project_root:
        parent_dvc = Path(f"{current}.dvc")
        if parent_dvc.exists():
            return parent_dvc
        if current == project_root:
            break
        current = current.parent
    
    return None


def is_import_target(target: str) -> Tuple[bool, Optional[Path]]:
    """Check if a target is tracked by an import .dvc file.
    
    Args:
        target: Target path to check.
        
    Returns:
        Tuple of (is_import, dvc_file_path).
    """
    dvc_file = resolve_to_dvc_file(target)
    if dvc_file is None:
        return False, None
    
    try:
        dvc_data = parse_dvc_file(dvc_file)
        if is_import_dvc(dvc_data):
            return True, dvc_file
    except CheckoutError:
        pass
    
    return False, dvc_file


def find_all_dvc_files() -> List[Path]:
    """Find all .dvc files in the current directory tree.
    
    Returns:
        List of paths to .dvc files.
    """
    cwd = Path.cwd()
    return sorted(cwd.rglob('*.dvc'))


def separate_targets(
    targets: List[str],
    verbose: bool = False,
) -> Tuple[List[str], List[str]]:
    """Separate targets into import and regular targets.
    
    Args:
        targets: List of target paths.
        verbose: Print progress messages.
        
    Returns:
        Tuple of (import_targets, regular_targets).
    """
    import_targets = []
    regular_targets = []
    
    for target in targets:
        is_import, dvc_file = is_import_target(target)
        if is_import:
            if verbose:
                print(f"  {target} → import ({dvc_file})")
            import_targets.append(target)
        else:
            if verbose:
                print(f"  {target} → regular")
            regular_targets.append(target)
    
    return import_targets, regular_targets


def pull(
    targets: Optional[List[str]] = None,
    verbose: bool = False,
    dvc_args: Optional[List[str]] = None,
) -> bool:
    """Pull DVC-tracked files, handling imports automatically.
    
    Args:
        targets: Specific targets to pull. If None, pulls all.
        verbose: Print detailed progress.
        dvc_args: Additional arguments to pass to dvc pull.
        
    Returns:
        True if all operations succeeded.
    """
    success = True
    
    # If no targets specified, find all .dvc files
    if not targets:
        if verbose:
            print("Discovering .dvc files...")
        all_dvc_files = find_all_dvc_files()
        targets = [str(f) for f in all_dvc_files]
        if verbose:
            print(f"  Found {len(targets)} .dvc files")
    
    if not targets:
        print("No .dvc files found")
        return True
    
    # Separate import targets from regular targets
    if verbose:
        print("Resolving targets...")
    import_targets, regular_targets = separate_targets(targets, verbose)
    
    # Handle imports with dt checkout
    if import_targets:
        if verbose:
            print(f"\nHandling {len(import_targets)} import target(s)...")
        
        for target in import_targets:
            try:
                # Resolve to .dvc file for checkout
                dvc_file = resolve_to_dvc_file(target)
                if dvc_file:
                    if verbose:
                        print(f"  dt checkout {dvc_file}")
                    smart_checkout(
                        targets=[str(dvc_file)],
                        cache=None,
                        verbose=verbose,
                    )
            except CheckoutError as e:
                print(f"Error checking out {target}: {e}")
                success = False
    
    # Handle regular targets with dvc pull
    if regular_targets:
        if verbose:
            print(f"\nPulling {len(regular_targets)} regular target(s)...")
        
        cmd = ['dvc', 'pull']
        if dvc_args:
            cmd.extend(dvc_args)
        cmd.extend(regular_targets)
        
        if verbose:
            print(f"  Running: {' '.join(cmd)}")
        
        result = subprocess.run(cmd)
        if result.returncode != 0:
            success = False
    elif verbose:
        print("\nNo regular targets to pull")
    
    return success
