"""Project summary generation for DVC Tools.

Generates tree.txt (file listing) and dag.md (pipeline DAG) summaries.
"""

import os
import secrets
import subprocess
from pathlib import Path
from typing import Optional

from . import config as cfg
from . import utils
from .errors import SummaryError


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
        # The user's own choice: anywhere, as given (relative to here).
        return Path.cwd() / output_dir
    
    configured, scope = cfg.get_trusted_value_with_scope('summary.output_dir')
    if configured:
        path = Path(str(configured))
        # A repository's own config may only name a place inside it.
        if scope in cfg.REPO_SCOPES:
            if path.is_absolute() or '..' in path.parts:
                raise SummaryError(
                    f"summary.output_dir from {scope} config must be a relative path "
                    f"inside the project: {configured!r} (use --out to choose another)")
            return path
        return Path.cwd() / path
    
    return Path(DEFAULT_OUTPUT_DIR)


def _write_report(out_dir: Path, filename: str, text: str) -> Path:
    """Write *text* to ``out_dir/filename``.

    A relative ``out_dir`` is walked from the current directory one
    component at a time without following links (creating what is missing),
    and the file is a fresh private temporary renamed into place -- so a
    link committed at ``docs`` or ``docs/tree.txt`` is never written
    through. An absolute ``out_dir`` (given with ``--out``) is used as is.
    """
    if '/' in filename or filename in ('', '.', '..'):
        raise SummaryError(f"Invalid output file name {filename!r}")
    flags = os.O_RDONLY | os.O_DIRECTORY
    if out_dir.is_absolute():
        out_dir.mkdir(parents=True, exist_ok=True)
        dfd = os.open(out_dir, flags)
        parts = []
    else:
        dfd = os.open('.', flags)
        parts = [p for p in out_dir.parts if p not in ('', '.')]
    try:
        for part in parts:
            if part == '..':
                raise SummaryError(f"Refusing output directory {out_dir}")
            try:
                os.mkdir(part, 0o777, dir_fd=dfd)
            except FileExistsError:
                pass
            try:
                nfd = os.open(part, flags | os.O_NOFOLLOW, dir_fd=dfd)
            except OSError as e:
                raise SummaryError(f"Refusing to write under {out_dir}: {part} "
                                   f"is not a real directory ({e})")
            os.close(dfd)
            dfd = nfd
        tmp = f'.{filename}.{secrets.token_hex(8)}.tmp'
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o666, dir_fd=dfd)
        try:
            with os.fdopen(fd, 'w') as f:
                fd = None
                f.write(text)
            os.rename(tmp, filename, src_dir_fd=dfd, dst_dir_fd=dfd)
        except BaseException:
            if fd is not None:
                os.close(fd)
            try:
                os.unlink(tmp, dir_fd=dfd)
            except OSError:
                pass
            raise
    finally:
        os.close(dfd)
    return out_dir / filename


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
    out_path = out_dir / filename
    
    if verbose:
        print(f"Generating {out_path}...")
    
    result = subprocess.run(
        ['dvc', 'list', '--tree', '--dvc-only', '.'],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        raise SummaryError(f"Failed to generate tree: {result.stderr}")
    
    _write_report(out_dir, filename, result.stdout)
    
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
    
    _write_report(out_dir, filename, result.stdout)
    
    if verbose:
        print(f"  Written to {out_path}")
    
    return out_path


def generate_repo_dag(
    output_dir: Optional[str] = None,
    filename: str = "repo-dag.md",
    depth: Optional[int] = None,
    verbose: bool = True,
) -> Path:
    """Generate repo-dag.md: the graph of imports *between* repos.

    Complements dag.md, which is the stage graph *within* this repo.

    Unlike the other generators this one clones every upstream repo, so it is
    not part of :func:`generate_all` -- it needs network access and takes far
    longer than a `dvc dag` call.

    Args:
        output_dir: Output directory (defaults to config or docs/)
        filename: Output filename (defaults to repo-dag.md)
        depth: Maximum depth to expand (None for unlimited)
        verbose: Print progress messages

    Returns:
        Path to the generated file

    Raises:
        SummaryError: If generation fails
    """
    from . import repo_graph as repo_graph_mod
    from . import repo_graph_render
    from .errors import DepsError

    out_dir = get_output_dir(output_dir)
    out_path = out_dir / filename

    if verbose:
        print(f"Generating {out_path}...")

    try:
        graph = repo_graph_mod.build_upstream(depth=depth, verbose=verbose)
    except DepsError as e:
        raise SummaryError(f"Failed to build repo graph: {e}")

    body = [
        f"# Repo dependencies for {graph.root}",
        "",
        repo_graph_render.render_mermaid(graph),
    ]
    if graph.gaps:
        body += ["", "```", repo_graph_render.format_gaps(graph), "```"]

    _write_report(out_dir, filename, "\n".join(body) + "\n")

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
