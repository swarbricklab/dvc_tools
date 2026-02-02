"""Project summary generation for DVC Tools.

Generates tree.txt (file listing) and dag.md (pipeline DAG) summaries.
"""

import subprocess
from pathlib import Path
from typing import Optional

from . import config as cfg
from . import utils


class SummaryError(Exception):
    """Raised when summary generation fails."""
    pass


# Default output directory
DEFAULT_OUTPUT_DIR = "docs"


def get_output_dir(output_dir: Optional[str] = None) -> Path:
    """Get the output directory for summaries.
    
    Priority:
    1. Explicit output_dir argument
    2. Config value summary.output_dir
    3. Default: docs/
    
    Args:
        output_dir: Optional explicit output directory
        
    Returns:
        Path to the output directory
    """
    if output_dir:
        return Path(output_dir)
    
    configured = cfg.get_value('summary.output_dir')
    if configured:
        return Path(configured)
    
    return Path(DEFAULT_OUTPUT_DIR)


def generate_tree(
    output_dir: Optional[str] = None,
    filename: str = "tree.txt",
    verbose: bool = True,
) -> Path:
    """Generate tree.txt using dvc list --tree.
    
    Args:
        output_dir: Output directory (defaults to config or docs/)
        filename: Output filename (defaults to tree.txt)
        verbose: Print progress messages
        
    Returns:
        Path to the generated file
        
    Raises:
        SummaryError: If generation fails
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise SummaryError(str(e))
    
    out_dir = get_output_dir(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    
    if verbose:
        print(f"Generating {out_path}...")
    
    result = subprocess.run(
        ['dvc', 'list', '--tree', '.'],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise SummaryError(f"Failed to generate tree: {result.stderr}")
    
    out_path.write_text(result.stdout)
    
    if verbose:
        print(f"  Written to {out_path}")
    
    return out_path


def generate_dag(
    output_dir: Optional[str] = None,
    filename: str = "dag.md",
    verbose: bool = True,
) -> Path:
    """Generate dag.md using dvc dag --md.
    
    Args:
        output_dir: Output directory (defaults to config or docs/)
        filename: Output filename (defaults to dag.md)
        verbose: Print progress messages
        
    Returns:
        Path to the generated file
        
    Raises:
        SummaryError: If generation fails
    """
    try:
        utils.check_dvc()
    except utils.DependencyError as e:
        raise SummaryError(str(e))
    
    out_dir = get_output_dir(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    
    if verbose:
        print(f"Generating {out_path}...")
    
    result = subprocess.run(
        ['dvc', 'dag', '--md'],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise SummaryError(f"Failed to generate DAG: {result.stderr}")
    
    out_path.write_text(result.stdout)
    
    if verbose:
        print(f"  Written to {out_path}")
    
    return out_path


def generate_all(
    output_dir: Optional[str] = None,
    verbose: bool = True,
) -> dict:
    """Generate all summary files.
    
    Args:
        output_dir: Output directory (defaults to config or docs/)
        verbose: Print progress messages
        
    Returns:
        Dict with 'tree' and 'dag' paths
        
    Raises:
        SummaryError: If generation fails
    """
    results = {}
    
    results['tree'] = generate_tree(output_dir=output_dir, verbose=verbose)
    results['dag'] = generate_dag(output_dir=output_dir, verbose=verbose)
    
    return results
