"""Update imported DVC data to a specific revision.

Wraps `dvc update` with improved revision handling and dt integration.
Updates .dvc files created by `dvc import` or `dt import` to point to
a different revision of the source repository.
"""

import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from .errors import UpdateError


def update(
    targets: Optional[List[str]] = None,
    rev: Optional[str] = None,
    recursive: bool = False,
    no_download: bool = False,
    to_remote: bool = False,
    remote: Optional[str] = None,
    jobs: Optional[int] = None,
    verbose: bool = False,
) -> List[Tuple[str, bool, str]]:
    """Update imported DVC files to a specific revision.
    
    Wraps `dvc update` with better revision handling. For each target,
    updates the .dvc file to reference the specified revision of the
    source repository.
    
    Args:
        targets: .dvc files to update. If None, updates all import files.
        rev: Git revision (commit, branch, tag) to update to. Defaults to HEAD.
        recursive: Update all stages in specified directory.
        no_download: Update .dvc file only, don't download data.
        to_remote: Update data directly on the remote.
        remote: Remote storage to perform updates to.
        jobs: Number of parallel jobs.
        verbose: Show detailed progress.
        
    Returns:
        List of (target, success, message) tuples.
        
    Raises:
        UpdateError: If update fails completely.
    """
    # Find import .dvc files if no targets specified
    if not targets:
        targets = _find_import_files(verbose=verbose)
        if not targets:
            return [(".", True, "No import .dvc files found")]
    
    results = []
    
    for target in targets:
        target_path = Path(target)
        
        # Validate target exists
        if not target_path.exists():
            # Try with .dvc extension
            if not target.endswith('.dvc'):
                target_path = Path(f"{target}.dvc")
            if not target_path.exists():
                results.append((target, False, "File not found"))
                continue
        
        # Check if it's an import file
        if not _is_import_file(target_path):
            results.append((target, False, "Not an import .dvc file (no deps.repo section)"))
            continue
        
        # Build dvc update command
        cmd = ['dvc', 'update']
        
        if rev:
            cmd.extend(['--rev', rev])
        
        if recursive:
            cmd.append('--recursive')
        
        if no_download:
            cmd.append('--no-download')
        
        if to_remote:
            cmd.append('--to-remote')
        
        if remote:
            cmd.extend(['--remote', remote])
        
        if jobs:
            cmd.extend(['--jobs', str(jobs)])
        
        if verbose:
            cmd.append('--verbose')
        
        cmd.append(str(target_path))
        
        if verbose:
            print(f"Running: {' '.join(cmd)}")
        
        # Run dvc update
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            rev_msg = f" to {rev}" if rev else " to HEAD"
            results.append((str(target_path), True, f"Updated{rev_msg}"))
        else:
            error_msg = result.stderr.strip() or result.stdout.strip() or "Unknown error"
            # Clean up DVC error messages
            if "ERROR:" in error_msg:
                error_msg = error_msg.split("ERROR:")[-1].strip()
            results.append((str(target_path), False, error_msg))
    
    return results


def _find_import_files(verbose: bool = False) -> List[str]:
    """Find all import .dvc files in the repository.
    
    Import files have a deps section with a repo URL.
    
    Returns:
        List of paths to import .dvc files.
    """
    import yaml
    
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
        dvc_files = [f for f in dvc_files if f]  # Remove empty strings
    except (OSError, FileNotFoundError):
        # Fall back to glob
        dvc_files = [str(p) for p in Path('.').rglob('*.dvc')]
    
    for dvc_file in dvc_files:
        if _is_import_file(Path(dvc_file)):
            import_files.append(dvc_file)
            if verbose:
                print(f"  Found import: {dvc_file}")
    
    return import_files


def _is_import_file(path: Path) -> bool:
    """Check if a .dvc file is an import (has deps.repo section).
    
    Args:
        path: Path to .dvc file.
        
    Returns:
        True if file is an import, False otherwise.
    """
    import yaml
    
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
        
        if not data:
            return False
        
        # Check for deps section with repo
        deps = data.get('deps', [])
        if not deps:
            return False
        
        for dep in deps:
            if 'repo' in dep:
                return True
        
        return False
    except (OSError, yaml.YAMLError):
        return False


def get_import_info(path: Path) -> dict:
    """Get import information from a .dvc file.
    
    Args:
        path: Path to .dvc file.
        
    Returns:
        Dict with 'repo_url', 'path', 'rev' keys, or empty dict if not an import.
    """
    import yaml
    
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
        
        if not data:
            return {}
        
        deps = data.get('deps', [])
        for dep in deps:
            repo = dep.get('repo', {})
            if repo:
                return {
                    'repo_url': repo.get('url', ''),
                    'path': dep.get('path', ''),
                    'rev': repo.get('rev', ''),
                }
        
        return {}
    except (OSError, yaml.YAMLError):
        return {}
