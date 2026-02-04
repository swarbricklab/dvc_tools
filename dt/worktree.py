"""Create git worktrees with DVC cache properly configured.

Git worktrees allow working on multiple branches simultaneously without
stashing changes. However, DVC's local cache configuration doesn't carry
over to new worktrees, breaking DVC operations.

This module wraps `git worktree add` to:
1. Create the worktree
2. Configure DVC to use the same cache as the main worktree
3. Initialize submodules
"""

import subprocess
from pathlib import Path
from typing import Optional, List

from . import utils
from .errors import WorktreeError


def add(
    path: str,
    branch: Optional[str] = None,
    new_branch: Optional[str] = None,
    verbose: bool = False,
) -> Path:
    """Create a git worktree with DVC cache configured.
    
    Args:
        path: Path where the worktree will be created.
        branch: Existing branch to checkout in the worktree.
        new_branch: Create a new branch with this name.
        verbose: Print progress messages.
        
    Returns:
        Path to the created worktree.
        
    Raises:
        WorktreeError: If worktree creation fails.
    """
    worktree_path = Path(path).resolve()
    
    # Get current DVC cache before creating worktree
    try:
        cache_dir = utils.get_cache_dir()
    except Exception:
        cache_dir = None
    
    if verbose and cache_dir:
        print(f"Current DVC cache: {cache_dir}")
    
    # Build git worktree command
    cmd = ['git', 'worktree', 'add']
    
    if new_branch:
        cmd.extend(['-b', new_branch])
    
    cmd.append(str(worktree_path))
    
    if branch:
        cmd.append(branch)
    elif not new_branch:
        # Neither branch nor new_branch specified - git worktree needs a branch
        raise WorktreeError("Must specify either --branch or --new-branch")
    
    # Create the worktree
    if verbose:
        print(f"Creating worktree: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = result.stderr.strip() or result.stdout.strip()
        raise WorktreeError(f"git worktree add failed: {error_msg}")
    
    if verbose and result.stdout.strip():
        print(result.stdout.strip())
    
    # Configure DVC cache in the new worktree
    if cache_dir:
        if verbose:
            print(f"Configuring DVC cache in worktree...")
        
        result = subprocess.run(
            ['dvc', 'cache', 'dir', '--local', str(cache_dir)],
            cwd=worktree_path,
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            if verbose:
                print(f"Warning: Failed to configure DVC cache: {result.stderr.strip()}")
        elif verbose:
            print(f"Set cache to: {cache_dir}")
    
    # Initialize submodules
    if verbose:
        print("Initializing submodules...")
    
    result = subprocess.run(
        ['git', 'submodule', 'update', '--init', '--recursive'],
        cwd=worktree_path,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        if verbose:
            print(f"Warning: Submodule initialization failed: {result.stderr.strip()}")
    elif verbose and result.stdout.strip():
        print(result.stdout.strip())
    
    return worktree_path


def list_worktrees(verbose: bool = False) -> List[dict]:
    """List all git worktrees.
    
    Args:
        verbose: Print progress messages.
        
    Returns:
        List of worktree info dicts with 'path', 'head', 'branch' keys.
        
    Raises:
        WorktreeError: If listing fails.
    """
    result = subprocess.run(
        ['git', 'worktree', 'list', '--porcelain'],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise WorktreeError(f"git worktree list failed: {result.stderr.strip()}")
    
    worktrees = []
    current = {}
    
    for line in result.stdout.strip().split('\n'):
        if not line:
            if current:
                worktrees.append(current)
                current = {}
        elif line.startswith('worktree '):
            current['path'] = line[9:]
        elif line.startswith('HEAD '):
            current['head'] = line[5:]
        elif line.startswith('branch '):
            current['branch'] = line[7:]
        elif line == 'bare':
            current['bare'] = True
        elif line == 'detached':
            current['detached'] = True
    
    if current:
        worktrees.append(current)
    
    return worktrees


def remove(
    path: str,
    force: bool = False,
    verbose: bool = False,
) -> None:
    """Remove a git worktree.
    
    Args:
        path: Path to the worktree to remove.
        force: Force removal even if worktree is dirty.
        verbose: Print progress messages.
        
    Raises:
        WorktreeError: If removal fails.
    """
    cmd = ['git', 'worktree', 'remove']
    
    if force:
        cmd.append('--force')
    
    cmd.append(path)
    
    if verbose:
        print(f"Removing worktree: {path}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = result.stderr.strip() or result.stdout.strip()
        raise WorktreeError(f"git worktree remove failed: {error_msg}")
    
    if verbose:
        print("Worktree removed")
