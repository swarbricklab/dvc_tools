"""Move or rename DVC-tracked files, preserving import metadata.

This module wraps `dvc mv` to fix a long-standing bug where import .dvc files
lose their `deps` information when moved. For non-imports, it simply calls
`dvc mv`. For imports, it:

1. Saves the `deps` section from the original .dvc file
2. Runs `dvc mv`
3. Restores the `deps` section to the new .dvc file
"""

import subprocess
from pathlib import Path
from typing import Optional, Tuple

from dvc.utils.serialize import dump_yaml

from . import utils
from .errors import MvError


def mv(
    src: str,
    dst: str,
    verbose: bool = False,
) -> Tuple[Path, Path]:
    """Move or rename a DVC-tracked file or directory.
    
    For imports (files created by `dvc import`), preserves the `deps`
    metadata that `dvc mv` incorrectly drops.
    
    Args:
        src: Source path (file, directory, or .dvc file).
        dst: Destination path.
        verbose: Print progress messages.
        
    Returns:
        Tuple of (old_dvc_path, new_dvc_path).
        
    Raises:
        MvError: If move fails.
    """
    src_path = Path(src)
    dst_path = Path(dst)
    
    # Resolve the .dvc file
    if src_path.suffix == '.dvc':
        src_dvc = src_path
    else:
        src_dvc = Path(str(src_path) + '.dvc')
    
    if not src_dvc.exists():
        raise MvError(f"Source .dvc file not found: {src_dvc}")
    
    # Check if this is an import
    is_import = utils.is_repo_import(src_dvc)
    deps_data = None
    
    if is_import:
        # Save the deps section before dvc mv destroys it
        if verbose:
            print(f"Detected import: saving deps metadata...")
        
        try:
            dvc_data = utils.parse_dvc_file(src_dvc)
            deps_data = dvc_data.get('deps')
            if not deps_data:
                # Fallback: not actually an import or no deps
                is_import = False
        except Exception as e:
            raise MvError(f"Failed to read {src_dvc}: {e}")
    
    # Determine what the destination .dvc file will be
    if dst_path.suffix == '.dvc':
        dst_dvc = dst_path
    else:
        # dvc mv creates the .dvc file based on destination
        if dst_path.is_dir() or str(dst).endswith('/'):
            # Moving into a directory
            dst_dvc = dst_path / (src_path.stem + '.dvc')
        else:
            dst_dvc = Path(str(dst_path) + '.dvc')
    
    # Run dvc mv
    if verbose:
        print(f"Running: dvc mv {src} {dst}")
    
    result = subprocess.run(
        ['dvc', 'mv', str(src), str(dst)],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        error_msg = result.stderr.strip() or result.stdout.strip()
        raise MvError(f"dvc mv failed: {error_msg}")
    
    if verbose and result.stdout.strip():
        print(result.stdout.strip())
    
    # For imports, restore the deps section
    if is_import and deps_data:
        if verbose:
            print(f"Restoring deps metadata to {dst_dvc}...")
        
        if not dst_dvc.exists():
            raise MvError(f"Expected .dvc file not found after move: {dst_dvc}")
        
        try:
            # Read the new .dvc file
            new_dvc_data = utils.parse_dvc_file(dst_dvc)
            
            # Restore deps
            new_dvc_data['deps'] = deps_data
            
            # Write back
            dump_yaml(dst_dvc, new_dvc_data)
            
            if verbose:
                print(f"  Restored {len(deps_data)} dep(s)")
                
        except Exception as e:
            raise MvError(f"Failed to restore deps to {dst_dvc}: {e}")
    
    return src_dvc, dst_dvc
