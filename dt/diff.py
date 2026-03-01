"""Show differences between versions of DVC-tracked data.

Two modes:
- Tree view: Shows which files changed (wraps dvc diff with tree formatting)
- Content view: Shows what changed inside a specific file (format-specific)

Provides a plugin architecture for format-specific content diffing (CSV, etc.)
with graceful fallback for unsupported formats.
"""

import json
import subprocess
import tempfile
from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from .errors import DiffError


# =============================================================================
# Constants
# =============================================================================

# Target size for auto-level tree output (fits in GH PR comment)
MAX_TREE_CHARS = 60000

# Status symbols for tree output
STATUS_SYMBOLS = {
    'added': '+',
    'deleted': '-',
    'modified': '~',
    'renamed': '→',
}


# =============================================================================
# Tree diff functions
# =============================================================================

def _run_dvc_diff(
    old_rev: str = "HEAD",
    new_rev: Optional[str] = None,
    targets: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run dvc diff and return parsed JSON output.
    
    Args:
        old_rev: Git revision for old version (default: HEAD)
        new_rev: Git revision for new version (default: None = workspace)
        targets: Optional list of paths to filter
        
    Returns:
        Dict with 'added', 'deleted', 'modified', 'renamed' lists
        
    Raises:
        DiffError: If dvc diff fails
    """
    cmd = ["dvc", "diff", "--json"]
    
    # Add revisions
    if new_rev:
        cmd.append(f"{old_rev}...{new_rev}")
    else:
        cmd.append(old_rev)
    
    # Add targets
    if targets:
        cmd.extend(targets)
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = result.stderr.strip() if result.stderr else "Unknown error"
        raise DiffError(f"dvc diff failed: {error_msg}")
    
    if not result.stdout.strip():
        return {'added': [], 'deleted': [], 'modified': [], 'renamed': []}
    
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise DiffError(f"Failed to parse dvc diff output: {e}")


def _build_tree(diff_data: Dict[str, Any]) -> Dict[str, Any]:
    """Build a tree structure from dvc diff output.
    
    Args:
        diff_data: Output from dvc diff --json
        
    Returns:
        Nested dict representing directory tree with file info
    """
    tree: Dict[str, Any] = {'_files': [], '_counts': defaultdict(int)}
    
    for status in ['added', 'deleted', 'modified', 'renamed']:
        items = diff_data.get(status, [])
        for item in items:
            # Handle both dict format and string format
            if isinstance(item, dict):
                path = item.get('path', '')
            else:
                path = str(item)
            
            if not path:
                continue
            
            parts = Path(path).parts
            current = tree
            
            # Navigate/create directories
            for i, part in enumerate(parts[:-1]):
                if part not in current:
                    current[part] = {'_files': [], '_counts': defaultdict(int)}
                current = current[part]
            
            # Add file to leaf directory
            filename = parts[-1] if parts else path
            current['_files'].append({
                'name': filename,
                'status': status,
                'path': path,
            })
            
            # Update counts up the tree
            current['_counts'][status] += 1
            
            # Propagate counts up
            current = tree
            for part in parts[:-1]:
                current['_counts'][status] += 1
                current = current[part]
    
    return tree


def _count_tree_items(tree: Dict[str, Any]) -> Tuple[int, int]:
    """Count files and directories in a tree.
    
    Returns:
        Tuple of (file_count, dir_count)
    """
    files = len(tree.get('_files', []))
    dirs = 0
    
    for key, value in tree.items():
        if key.startswith('_'):
            continue
        dirs += 1
        sub_files, sub_dirs = _count_tree_items(value)
        files += sub_files
        dirs += sub_dirs
    
    return files, dirs


def _format_counts(counts: Dict[str, int]) -> str:
    """Format status counts as a summary string."""
    parts = []
    if counts.get('added', 0):
        parts.append(f"+{counts['added']}")
    if counts.get('modified', 0):
        parts.append(f"~{counts['modified']}")
    if counts.get('deleted', 0):
        parts.append(f"-{counts['deleted']}")
    if counts.get('renamed', 0):
        parts.append(f"→{counts['renamed']}")
    return ', '.join(parts) if parts else ''


def _render_tree(
    tree: Dict[str, Any],
    prefix: str = "",
    max_level: Optional[int] = None,
    current_level: int = 0,
    is_last: bool = True,
    name: str = "",
) -> List[str]:
    """Render tree as formatted lines.
    
    Args:
        tree: Tree structure from _build_tree
        prefix: Current line prefix for indentation
        max_level: Maximum depth to render (None = unlimited)
        current_level: Current depth in tree
        is_last: Whether this is the last item at current level
        name: Name of current directory
        
    Returns:
        List of formatted lines
    """
    lines = []
    
    # Get subdirectories and files
    subdirs = sorted([k for k in tree.keys() if not k.startswith('_')])
    files = tree.get('_files', [])
    counts = tree.get('_counts', {})
    
    # Check if we should collapse this level
    if max_level is not None and current_level >= max_level:
        total = sum(counts.values())
        if total > 0:
            count_str = _format_counts(counts)
            lines.append(f"{prefix}... ({count_str})")
        return lines
    
    # Render files at this level
    for i, file_info in enumerate(sorted(files, key=lambda f: f['name'])):
        is_last_item = (i == len(files) - 1) and not subdirs
        connector = "└── " if is_last_item else "├── "
        status_sym = STATUS_SYMBOLS.get(file_info['status'], '?')
        lines.append(f"{prefix}{connector}[{status_sym}] {file_info['name']}")
    
    # Render subdirectories
    for i, subdir in enumerate(subdirs):
        is_last_subdir = (i == len(subdirs) - 1)
        connector = "└── " if is_last_subdir else "├── "
        
        subdir_counts = tree[subdir].get('_counts', {})
        count_str = _format_counts(subdir_counts)
        count_display = f" ({count_str})" if count_str else ""
        
        lines.append(f"{prefix}{connector}{subdir}/{count_display}")
        
        # Recurse
        new_prefix = prefix + ("    " if is_last_subdir else "│   ")
        sub_lines = _render_tree(
            tree[subdir],
            prefix=new_prefix,
            max_level=max_level,
            current_level=current_level + 1,
            is_last=is_last_subdir,
            name=subdir,
        )
        lines.extend(sub_lines)
    
    return lines


def _find_auto_level(tree: Dict[str, Any], max_chars: int = MAX_TREE_CHARS) -> int:
    """Find the maximum tree depth that fits within character limit.
    
    Args:
        tree: Tree structure from _build_tree
        max_chars: Maximum output characters
        
    Returns:
        Optimal max_level value
    """
    # Try increasing levels until output exceeds limit
    for level in range(1, 50):
        lines = _render_tree(tree, max_level=level)
        output = '\n'.join(lines)
        if len(output) > max_chars:
            return max(1, level - 1)
    
    # Full tree fits
    return 50


def tree_diff(
    old_rev: str = "HEAD",
    new_rev: Optional[str] = None,
    targets: Optional[List[str]] = None,
    level: Union[int, str] = "auto",
    verbose: bool = False,
) -> str:
    """Show which files changed between revisions in tree format.
    
    Args:
        old_rev: Git revision for old version (default: HEAD)
        new_rev: Git revision for new version (default: None = workspace)
        targets: Optional list of paths to filter
        level: Max tree depth (int) or "auto" to fit GH comment
        verbose: Show additional details
        
    Returns:
        Formatted tree string
        
    Raises:
        DiffError: If diff fails
    """
    # Get changes from dvc diff
    diff_data = _run_dvc_diff(old_rev, new_rev, targets)
    
    # Count total changes
    total_changes = sum(len(diff_data.get(s, [])) for s in ['added', 'deleted', 'modified', 'renamed'])
    
    if total_changes == 0:
        return "No changes detected."
    
    # Build tree
    tree = _build_tree(diff_data)
    
    # Determine level
    if level == "auto":
        max_level = _find_auto_level(tree)
        if verbose:
            print(f"Auto-selected level: {max_level}")
    else:
        max_level = int(level)
    
    # Render
    lines = []
    
    # Header with summary
    counts = tree.get('_counts', {})
    summary_parts = []
    if counts.get('added', 0):
        summary_parts.append(f"{counts['added']} added")
    if counts.get('modified', 0):
        summary_parts.append(f"{counts['modified']} modified")
    if counts.get('deleted', 0):
        summary_parts.append(f"{counts['deleted']} deleted")
    if counts.get('renamed', 0):
        summary_parts.append(f"{counts['renamed']} renamed")
    
    summary = ', '.join(summary_parts)
    
    # Revision display
    if new_rev:
        rev_display = f"{old_rev}...{new_rev}"
    else:
        rev_display = f"{old_rev} → workspace"
    
    lines.append(f"Changes ({rev_display}): {summary}")
    lines.append("")
    
    # Tree
    tree_lines = _render_tree(tree, max_level=max_level)
    lines.extend(tree_lines)
    
    return '\n'.join(lines)


# =============================================================================
# Handler base class and registry
# =============================================================================

class DiffHandler(ABC):
    """Base class for format-specific diff handlers."""
    
    # File extensions this handler supports (e.g., ['.csv', '.tsv'])
    extensions: List[str] = []
    
    # Human-readable format name
    format_name: str = "Unknown"
    
    @classmethod
    def can_handle(cls, path: str) -> bool:
        """Check if this handler can process the given file."""
        suffix = Path(path).suffix.lower()
        return suffix in cls.extensions
    
    @abstractmethod
    def diff(
        self,
        old_path: Path,
        new_path: Path,
        output_format: str = "terminal",
    ) -> str:
        """Compute and format the diff between two file versions.
        
        Args:
            old_path: Path to the old version of the file
            new_path: Path to the new version of the file
            output_format: Output format ('terminal', 'json', 'html', 'md')
            
        Returns:
            Formatted diff string
        """
        pass


# Global registry of handlers
_handlers: List[Type[DiffHandler]] = []


def register_handler(handler_class: Type[DiffHandler]) -> Type[DiffHandler]:
    """Decorator to register a diff handler."""
    _handlers.append(handler_class)
    return handler_class


def get_handler(path: str) -> Optional[DiffHandler]:
    """Get the appropriate handler for a file path."""
    for handler_class in _handlers:
        if handler_class.can_handle(path):
            return handler_class()
    return None


def list_handlers() -> List[Dict[str, Any]]:
    """List all registered handlers and their supported extensions."""
    return [
        {
            'name': h.format_name,
            'extensions': h.extensions,
        }
        for h in _handlers
    ]


# =============================================================================
# Built-in handlers
# =============================================================================

@register_handler
class CSVHandler(DiffHandler):
    """Handler for CSV/TSV files using daff."""
    
    extensions = ['.csv', '.tsv', '.txt']
    format_name = "CSV/TSV"
    
    def diff(
        self,
        old_path: Path,
        new_path: Path,
        output_format: str = "terminal",
    ) -> str:
        """Diff CSV files using daff."""
        # Check if daff is available
        import shutil
        if not shutil.which('daff'):
            raise DiffError(
                "daff not found. Install with: pip install daff\n"
                "See: https://github.com/paulfitz/daff"
            )
        
        # Build daff command
        cmd = ["daff"]
        
        if output_format == "html":
            cmd.append("--output-format=html")
        elif output_format == "json":
            cmd.append("--output-format=json")
        # terminal/md use default output
        
        cmd.extend([str(old_path), str(new_path)])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0 and result.stderr:
            raise DiffError(f"daff failed: {result.stderr}")
        
        return result.stdout


@register_handler
class FallbackHandler(DiffHandler):
    """Fallback handler for unsupported formats.
    
    Shows basic metadata comparison (size, hash) rather than content diff.
    """
    
    extensions = []  # Empty = matches nothing via can_handle
    format_name = "Fallback"
    
    @classmethod
    def can_handle(cls, path: str) -> bool:
        """Fallback always returns False - it's used explicitly."""
        return False
    
    def diff(
        self,
        old_path: Path,
        new_path: Path,
        output_format: str = "terminal",
    ) -> str:
        """Show basic metadata comparison."""
        old_size = old_path.stat().st_size if old_path.exists() else 0
        new_size = new_path.stat().st_size if new_path.exists() else 0
        
        size_change = new_size - old_size
        sign = "+" if size_change >= 0 else ""
        
        result = {
            'old_size': old_size,
            'new_size': new_size,
            'size_change': size_change,
            'message': f"Binary/unsupported format: size changed from {old_size:,} to {new_size:,} bytes ({sign}{size_change:,})",
        }
        
        if output_format == "json":
            return json.dumps(result, indent=2)
        
        return result['message']


# =============================================================================
# Content diff function (format-specific file comparison)
# =============================================================================

def content_diff(
    path: str,
    old_rev: str = "HEAD",
    new_rev: Optional[str] = None,
    output_format: str = "terminal",
    verbose: bool = False,
) -> str:
    """Compute the diff between two versions of a DVC-tracked file.
    
    Shows what changed *inside* a specific file using format-specific handlers.
    
    Args:
        path: Path to the DVC-tracked file
        old_rev: Git revision for the old version (default: HEAD)
        new_rev: Git revision for the new version (default: None = workspace)
        output_format: Output format ('terminal', 'json', 'html', 'md')
        verbose: Show additional details
        
    Returns:
        Formatted diff string
        
    Raises:
        DiffError: If the diff cannot be computed
    """
    import dvc.api
    
    path = str(Path(path))
    
    # Get the appropriate handler
    handler = get_handler(path)
    if handler is None:
        handler = FallbackHandler()
        if verbose:
            print(f"No specific handler for {Path(path).suffix}, using fallback")
    elif verbose:
        print(f"Using {handler.format_name} handler")
    
    # Create temp files for the versions
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        old_file = tmpdir / f"old_{Path(path).name}"
        new_file = tmpdir / f"new_{Path(path).name}"
        
        # Fetch old version
        try:
            with dvc.api.open(path, rev=old_rev) as f:
                old_file.write_bytes(f.read() if hasattr(f, 'read') else f)
        except Exception as e:
            raise DiffError(f"Failed to get '{path}' at revision '{old_rev}': {e}")
        
        # Fetch new version
        if new_rev is None:
            # Use workspace version
            workspace_path = Path(path)
            if not workspace_path.exists():
                raise DiffError(f"'{path}' not found in workspace")
            new_file = workspace_path
        else:
            try:
                with dvc.api.open(path, rev=new_rev) as f:
                    new_file.write_bytes(f.read() if hasattr(f, 'read') else f)
            except Exception as e:
                raise DiffError(f"Failed to get '{path}' at revision '{new_rev}': {e}")
        
        # Compute diff
        return handler.diff(old_file, new_file, output_format)


def get_supported_formats() -> str:
    """Get a formatted string of supported formats for help text."""
    handlers = list_handlers()
    lines = []
    for h in handlers:
        if h['extensions']:
            exts = ', '.join(h['extensions'])
            lines.append(f"  {h['name']}: {exts}")
    
    if lines:
        return "Supported formats:\n" + '\n'.join(lines)
    return "No format-specific handlers registered."


# Backward compatibility alias
diff = content_diff
