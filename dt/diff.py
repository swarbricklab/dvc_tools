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
import warnings
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
    cwd: Optional[Path] = None,
) -> Dict[str, Any]:
    """Run dvc diff and return parsed JSON output.
    
    Args:
        old_rev: Git revision for old version (default: HEAD)
        new_rev: Git revision for new version (default: None = workspace)
        targets: Optional list of paths to filter
        cwd: Optional working directory to run command in
        
    Returns:
        Dict with 'added', 'deleted', 'modified', 'renamed' lists
        
    Raises:
        DiffError: If dvc diff fails
    """
    cmd = ["dvc", "diff", "--json"]
    
    # Add targets before --
    if targets:
        cmd.extend(["--targets"] + targets)
    
    # Separator between options/targets and revisions
    cmd.append("--")
    
    # Add revisions after --
    if new_rev:
        cmd.extend([old_rev, new_rev])
    else:
        cmd.append(old_rev)
    
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    
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
                raw_path = item.get('path', '')
                # Renamed items have path as {"old": "...", "new": "..."}
                if isinstance(raw_path, dict):
                    old_path = raw_path.get('old', '')
                    new_path = raw_path.get('new', '') or old_path
                    path = new_path
                else:
                    old_path = None
                    path = raw_path
            else:
                old_path = None
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
            file_entry: Dict[str, Any] = {
                'name': filename,
                'status': status,
                'path': path,
            }
            if old_path:
                file_entry['old_path'] = old_path
            current['_files'].append(file_entry)
            
            # Update counts up the tree
            current['_counts'][status] += 1
            
            # Propagate counts up
            current = tree
            for part in parts[:-1]:
                current['_counts'][status] += 1
                current = current[part]
    
    # Remove leaf file entries that duplicate a subdirectory (DVC reports
    # .dir manifests as both a directory change and a file change).
    _dedup_dir_entries(tree)
    
    return tree


def _dedup_dir_entries(node: Dict[str, Any]) -> None:
    """Remove file entries whose name matches a subdirectory in the same node.

    DVC diff reports directory manifests (e.g. ``results``) as modified
    *files* alongside the individual files inside ``results/``.  This
    causes the directory to appear twice in the tree and inflates counts.

    After removing duplicates, recalculates ``_counts`` bottom-up so that
    all ancestor counts are correct.
    """
    # Recurse into subdirectories first (bottom-up)
    for key, value in node.items():
        if key.startswith('_') or not isinstance(value, dict):
            continue
        _dedup_dir_entries(value)

    # Remove file entries that duplicate a subdirectory name
    subdir_names = {
        k for k in node if not k.startswith('_') and isinstance(node[k], dict)
    }
    if subdir_names:
        node['_files'] = [f for f in node.get('_files', [])
                          if f['name'] not in subdir_names]

    # Recalculate counts from files + child counts
    counts: Dict[str, int] = defaultdict(int)
    for f in node.get('_files', []):
        counts[f['status']] += 1
    for key, value in node.items():
        if key.startswith('_') or not isinstance(value, dict):
            continue
        for status, n in value.get('_counts', {}).items():
            counts[status] += n
    node['_counts'] = counts


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


def _run_dvc_diff_md(
    old_rev: str = "HEAD",
    new_rev: Optional[str] = None,
    targets: Optional[List[str]] = None,
) -> str:
    """Run dvc diff with --md flag for markdown table output.
    
    Args:
        old_rev: Git revision for old version
        new_rev: Git revision for new version
        targets: Optional list of paths to filter
        
    Returns:
        Markdown table string
        
    Raises:
        DiffError: If dvc diff fails
    """
    cmd = ["dvc", "diff", "--md"]
    
    if new_rev:
        cmd.extend([old_rev, new_rev])
    else:
        cmd.append(old_rev)
    
    if targets:
        cmd.extend(targets)
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = result.stderr.strip() if result.stderr else "Unknown error"
        raise DiffError(f"dvc diff failed: {error_msg}")
    
    return result.stdout.strip() if result.stdout else "No changes detected."


def _format_terminal(
    tree: Dict[str, Any],
    diff_data: Dict[str, Any],
    old_rev: str,
    new_rev: Optional[str],
    max_level: int,
) -> str:
    """Format tree diff for terminal output."""
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
    
    if new_rev:
        rev_display = f"{old_rev}...{new_rev}"
    else:
        rev_display = f"{old_rev} → workspace"
    
    lines = [f"Changes ({rev_display}): {summary}", ""]
    tree_lines = _render_tree(tree, max_level=max_level)
    lines.extend(tree_lines)
    
    return '\n'.join(lines)


def _format_json(diff_data: Dict[str, Any]) -> str:
    """Format diff data as JSON."""
    return json.dumps(diff_data, indent=2)


def _format_csv(diff_data: Dict[str, Any]) -> str:
    """Format diff data as CSV with columns: change,path,old_hash,new_hash."""
    lines = ["change,path,old_hash,new_hash"]
    
    for status in ['added', 'deleted', 'modified', 'renamed']:
        items = diff_data.get(status, [])
        for item in items:
            path = item.get('path', '')
            old_hash = item.get('hash', {}).get('old', '') if isinstance(item.get('hash'), dict) else ''
            new_hash = item.get('hash', {}).get('new', '') if isinstance(item.get('hash'), dict) else ''
            
            # Handle renamed items (old path in 'path', new path in different field)
            if status == 'renamed':
                old_path = item.get('path', {}).get('old', '') if isinstance(item.get('path'), dict) else path
                new_path = item.get('path', {}).get('new', '') if isinstance(item.get('path'), dict) else path
                path = f"{old_path} -> {new_path}"
            
            # Escape commas in paths
            if ',' in path:
                path = f'"{path}"'
            
            lines.append(f"{status},{path},{old_hash},{new_hash}")
    
    return '\n'.join(lines)


def _format_md(
    tree: Dict[str, Any],
    diff_data: Dict[str, Any],
    old_rev: str,
    new_rev: Optional[str],
    max_level: int,
) -> str:
    """Format tree diff as markdown with diff code block for coloring."""
    counts = tree.get('_counts', {})
    summary_parts = []
    if counts.get('added', 0):
        summary_parts.append(f"+{counts['added']} added")
    if counts.get('modified', 0):
        summary_parts.append(f"~{counts['modified']} modified")
    if counts.get('deleted', 0):
        summary_parts.append(f"-{counts['deleted']} deleted")
    if counts.get('renamed', 0):
        summary_parts.append(f"→{counts['renamed']} renamed")
    
    summary = ', '.join(summary_parts)
    
    if new_rev:
        rev_display = f"`{old_rev}`...`{new_rev}`"
    else:
        rev_display = f"`{old_rev}` → workspace"
    
    lines = [f"**Changes ({rev_display}):** {summary}", "", "```diff"]
    
    # Render tree with diff-style prefixes for coloring
    tree_lines = _render_tree_diff_style(tree, max_level=max_level)
    lines.extend(tree_lines)
    lines.append("```")
    
    return '\n'.join(lines)


def _render_tree_diff_style(
    tree: Dict[str, Any],
    current_path: str = "",
    prefix: str = "",
    max_level: int = 50,
    current_level: int = 0,
) -> List[str]:
    """Render tree with diff-style prefixes for markdown coloring.
    
    Lines starting with + are green (additions)
    Lines starting with - are red (deletions)
    Other lines are neutral
    """
    lines = []
    
    # Get subdirectories and files
    subdirs = []
    for key, value in tree.items():
        if key.startswith('_'):
            continue
        if isinstance(value, dict) and '_counts' in value:
            subdirs.append((key, value))
    
    files = tree.get('_files', [])
    
    # Sort entries
    subdirs.sort(key=lambda x: x[0])
    files.sort(key=lambda x: x['name'])
    
    items = [(name, 'dir', data) for name, data in subdirs] + \
            [(f['name'], 'file', f) for f in files]
    
    for i, (name, item_type, data) in enumerate(items):
        is_last = i == len(items) - 1
        
        if item_type == 'dir':
            counts = data.get('_counts', {})
            count_str = _format_counts(counts)
            
            # Determine diff prefix based on directory contents
            if counts.get('added', 0) > 0 and counts.get('deleted', 0) == 0 and counts.get('modified', 0) == 0:
                diff_prefix = "+ "
            elif counts.get('deleted', 0) > 0 and counts.get('added', 0) == 0 and counts.get('modified', 0) == 0:
                diff_prefix = "- "
            else:
                diff_prefix = "  "
            
            connector = "└── " if is_last else "├── "
            lines.append(f"{diff_prefix}{prefix}{connector}{name}/ {count_str}")
            
            if current_level < max_level:
                child_prefix = prefix + ("    " if is_last else "│   ")
                child_lines = _render_tree_diff_style(
                    data, 
                    current_path=f"{current_path}{name}/",
                    prefix=child_prefix,
                    max_level=max_level,
                    current_level=current_level + 1,
                )
                lines.extend(child_lines)
        else:
            status = data.get('status', 'modified')
            symbol = STATUS_SYMBOLS.get(status, '?')
            
            # Diff prefix for coloring
            if status == 'added':
                diff_prefix = "+ "
            elif status == 'deleted':
                diff_prefix = "- "
            else:
                diff_prefix = "  "
            
            connector = "└── " if is_last else "├── "
            lines.append(f"{diff_prefix}{prefix}{connector}[{symbol}] {name}")
    
    return lines


def _format_html(
    tree: Dict[str, Any],
    diff_data: Dict[str, Any],
    old_rev: str,
    new_rev: Optional[str],
) -> str:
    """Format tree diff as interactive HTML with collapsible sections."""
    counts = tree.get('_counts', {})
    summary_parts = []
    if counts.get('added', 0):
        summary_parts.append(f'<span class="added">+{counts["added"]} added</span>')
    if counts.get('modified', 0):
        summary_parts.append(f'<span class="modified">~{counts["modified"]} modified</span>')
    if counts.get('deleted', 0):
        summary_parts.append(f'<span class="deleted">-{counts["deleted"]} deleted</span>')
    if counts.get('renamed', 0):
        summary_parts.append(f'<span class="renamed">→{counts["renamed"]} renamed</span>')
    
    summary = ', '.join(summary_parts)
    
    if new_rev:
        rev_display = f"<code>{old_rev}</code>...<code>{new_rev}</code>"
    else:
        rev_display = f"<code>{old_rev}</code> → workspace"
    
    html_tree = _render_tree_html(tree)
    
    return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>DVC Diff</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            line-height: 1.5;
            padding: 20px;
            max-width: 900px;
            margin: 0 auto;
        }}
        .summary {{
            margin-bottom: 20px;
            padding: 10px 15px;
            background: #f6f8fa;
            border-radius: 6px;
        }}
        .added {{ color: #22863a; }}
        .deleted {{ color: #cb2431; }}
        .modified {{ color: #b08800; }}
        .renamed {{ color: #6f42c1; }}
        .tree {{
            font-family: "SF Mono", Consolas, monospace;
            font-size: 13px;
        }}
        details {{
            margin-left: 20px;
        }}
        details > summary {{
            cursor: pointer;
            list-style: none;
        }}
        details > summary::-webkit-details-marker {{
            display: none;
        }}
        details > summary::before {{
            content: "▶ ";
            font-size: 10px;
        }}
        details[open] > summary::before {{
            content: "▼ ";
        }}
        .file {{
            margin-left: 20px;
            padding: 2px 0;
        }}
        .counts {{
            color: #6a737d;
            font-size: 12px;
        }}
        .controls {{
            margin-bottom: 12px;
        }}
        .controls button {{
            font-size: 13px;
            padding: 4px 12px;
            margin-right: 6px;
            border: 1px solid #d1d5da;
            border-radius: 4px;
            background: #f6f8fa;
            cursor: pointer;
        }}
        .controls button:hover {{
            background: #e1e4e8;
        }}
    </style>
</head>
<body>
    <div class="summary">
        <strong>Changes ({rev_display}):</strong> {summary}
    </div>
    <div class="controls">
        <button onclick="document.querySelectorAll('details').forEach(d => d.open = true)">Expand all</button>
        <button onclick="document.querySelectorAll('details').forEach(d => d.open = false)">Collapse all</button>
    </div>
    <div class="tree">
{html_tree}
    </div>
    <script>
        document.querySelectorAll('details').forEach(d => d.open = false);
    </script>
</body>
</html>'''


def _render_tree_html(
    tree: Dict[str, Any],
    indent: int = 2,
) -> str:
    """Render tree as HTML with collapsible details elements."""
    lines = []
    spaces = "    " * indent
    
    subdirs = []
    for key, value in tree.items():
        if key.startswith('_'):
            continue
        if isinstance(value, dict) and '_counts' in value:
            subdirs.append((key, value))
    
    files = tree.get('_files', [])
    
    subdirs.sort(key=lambda x: x[0])
    files.sort(key=lambda x: x['name'])
    
    for name, data in subdirs:
        counts = data.get('_counts', {})
        count_parts = []
        if counts.get('added', 0):
            count_parts.append(f'+{counts["added"]}')
        if counts.get('modified', 0):
            count_parts.append(f'~{counts["modified"]}')
        if counts.get('deleted', 0):
            count_parts.append(f'-{counts["deleted"]}')
        if counts.get('renamed', 0):
            count_parts.append(f'→{counts["renamed"]}')
        count_str = f' <span class="counts">({", ".join(count_parts)})</span>' if count_parts else ''
        
        child_html = _render_tree_html(data, indent + 1)
        lines.append(f'{spaces}<details>')
        lines.append(f'{spaces}    <summary>{name}/{count_str}</summary>')
        lines.append(child_html)
        lines.append(f'{spaces}</details>')
    
    for f in files:
        status = f.get('status', 'modified')
        symbol = STATUS_SYMBOLS.get(status, '?')
        css_class = status
        lines.append(f'{spaces}<div class="file {css_class}">[{symbol}] {f["name"]}</div>')
    
    return '\n'.join(lines)


def tree_diff(
    old_rev: str = "HEAD",
    new_rev: Optional[str] = None,
    targets: Optional[List[str]] = None,
    level: Union[int, str] = "auto",
    output_format: str = "terminal",
    verbose: bool = False,
) -> str:
    """Show which files changed between revisions.
    
    Args:
        old_rev: Git revision for old version (default: HEAD)
        new_rev: Git revision for new version (default: None = workspace)
        targets: Optional list of paths to filter
        level: Max tree depth (int) or "auto" to fit GH comment
        output_format: One of terminal, json, table, md, csv, html
        verbose: Show additional details
        
    Returns:
        Formatted output string
        
    Raises:
        DiffError: If diff fails
    """
    # Handle table format separately (uses dvc diff --md)
    if output_format == "table":
        return _run_dvc_diff_md(old_rev, new_rev, targets)
    
    # Get changes from dvc diff
    diff_data = _run_dvc_diff(old_rev, new_rev, targets)
    
    # JSON format: return raw JSON
    if output_format == "json":
        return _format_json(diff_data)
    
    # CSV format: tabular output
    if output_format == "csv":
        return _format_csv(diff_data)
    
    # Count total changes
    total_changes = sum(len(diff_data.get(s, [])) for s in ['added', 'deleted', 'modified', 'renamed'])
    
    if total_changes == 0:
        return "No changes detected."
    
    # Build tree
    tree = _build_tree(diff_data)
    
    # HTML format: interactive tree (doesn't need level)
    if output_format == "html":
        return _format_html(tree, diff_data, old_rev, new_rev)
    
    # Determine level for tree-based formats
    if level == "auto":
        max_level = _find_auto_level(tree)
        if verbose:
            print(f"Auto-selected level: {max_level}")
    else:
        max_level = int(level)
    
    # Markdown format: diff code block
    if output_format == "md":
        return _format_md(tree, diff_data, old_rev, new_rev, max_level)
    
    # Default: terminal format
    return _format_terminal(tree, diff_data, old_rev, new_rev, max_level)


# =============================================================================
# Handler base class and registry
# =============================================================================

class DiffHandler(ABC):
    """Base class for format-specific diff handlers."""

    # File extensions this handler supports (e.g., ['.csv', '.tsv'])
    extensions: List[str] = []

    # Human-readable format name
    format_name: str = "Unknown"

    # Higher priority handlers are checked first.
    # Built-in handlers use 0; plugins should use a positive value (e.g. 10)
    # so they can override built-ins for the same extension if desired.
    priority: int = 0

    # Python packages required by this handler (informational — used by
    # `dt diff --list-handlers`).  Example: ['anndata']
    requires: List[str] = []
    
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
        detail_level: str = "normal",
    ) -> str:
        """Compute and format the diff between two file versions.

        Args:
            old_path:     Path to the old version of the file
            new_path:     Path to the new version of the file
            output_format: Output format ('terminal', 'json', 'html', 'md')
            detail_level: Verbosity of the diff:
                          'summary'  – brief statistics / key changes only
                          'normal'   – standard diff (default)
                          'granular' – exhaustive diff, all changed details

        Returns:
            Formatted diff string
        """
        pass


# Global registry of handlers (built-ins + plugins, kept in priority order)
_handlers: List[Type[DiffHandler]] = []

# Ensures plugin entry points are only loaded once per process
_plugin_handlers_loaded: bool = False


def register_handler(handler_class: Type[DiffHandler]) -> Type[DiffHandler]:
    """Decorator to register a diff handler.

    Built-in handlers call this at import time.  Plugin packages call it from
    their own module, which is imported by ``_load_plugin_handlers()``.
    """
    _handlers.append(handler_class)
    return handler_class


def _load_plugin_handlers() -> None:
    """Discover and load handlers registered via the 'dvc_tools.diff_handlers'
    entry-point group.

    Plugin packages advertise themselves in their ``pyproject.toml``::

        [project.entry-points."dvc_tools.diff_handlers"]
        my_format = "my_package:MyHandler"

    The handler class must be a subclass of ``DiffHandler`` and should be
    decorated with ``@register_handler`` in its own module so that importing
    the class is sufficient to register it.

    Loading is done lazily (once per process) the first time ``get_handler()``
    or ``list_handlers()`` is called.
    """
    global _plugin_handlers_loaded
    if _plugin_handlers_loaded:
        return
    _plugin_handlers_loaded = True

    try:
        from importlib.metadata import entry_points
    except ImportError:
        # Python < 3.9 — importlib.metadata is available but entry_points()
        # doesn't accept the 'group' keyword; fall back gracefully.
        try:
            from importlib.metadata import entry_points as _ep
            eps = _ep().get("dvc_tools.diff_handlers", [])
        except Exception:
            return
    else:
        try:
            eps = entry_points(group="dvc_tools.diff_handlers")
        except TypeError:
            # Python 3.8/3.9 shim
            eps = entry_points().get("dvc_tools.diff_handlers", [])

    for ep in eps:
        try:
            ep.load()  # the module is expected to call @register_handler
        except Exception as exc:
            warnings.warn(
                f"dvc_tools: failed to load diff handler plugin '{ep.name}': {exc}",
                stacklevel=2,
            )


def get_handler(path: str) -> Optional[DiffHandler]:
    """Return the highest-priority handler that can process *path*, or None."""
    _load_plugin_handlers()
    # Sort descending by priority so higher-priority handlers win
    for handler_class in sorted(_handlers, key=lambda h: h.priority, reverse=True):
        if handler_class.can_handle(path):
            return handler_class()
    return None


def list_handlers() -> List[Dict[str, Any]]:
    """Return metadata for all registered handlers, sorted by priority."""
    _load_plugin_handlers()
    return [
        {
            'name': h.format_name,
            'extensions': h.extensions,
            'priority': h.priority,
            'requires': h.requires,
        }
        for h in sorted(_handlers, key=lambda h: h.priority, reverse=True)
    ]


# =============================================================================
# Built-in handlers
# =============================================================================

def _summarise_daff_json(json_str: str) -> str:
    """Parse daff JSON output and return a one-line count summary.

    daff rows use the following first-column markers:
      '!'  – column-header row (skip)
      '@@' – context ellipsis    (skip)
      '+'  – added row
      '-'  – deleted row
      '->' – modified row
    """
    try:
        data = json.loads(json_str)
        added = deleted = modified = 0
        for row in data:
            if not row:
                continue
            marker = str(row[0])
            if marker == '+':
                added += 1
            elif marker in ('-', '---'):
                deleted += 1
            elif marker == '->':
                modified += 1
        return (
            f"CSV diff summary: {added} row(s) added, "
            f"{deleted} row(s) deleted, {modified} row(s) modified"
        )
    except Exception:
        return "CSV diff summary: (could not parse diff statistics)"


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
        detail_level: str = "normal",
    ) -> str:
        """Diff CSV files using daff.

        detail_level behaviour:
          'summary'  – counts of added/deleted/modified rows (no cell details)
          'normal'   – standard daff output (changed rows + context)
          'granular' – daff output with all rows shown (--all flag)
        """
        import shutil
        if not shutil.which('daff'):
            raise DiffError(
                "daff not found. Install with: pip install daff\n"
                "See: https://github.com/paulfitz/daff"
            )

        # summary: parse JSON counts, no cell-level output
        if detail_level == "summary":
            cmd = ["daff", "--output-format=json", str(old_path), str(new_path)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0 and result.stderr:
                raise DiffError(f"daff failed: {result.stderr}")
            return _summarise_daff_json(result.stdout)

        # normal / granular: full diff
        cmd = ["daff"]

        if detail_level == "granular":
            cmd.append("--all")  # show all rows, not just changed + context

        if output_format == "json":
            cmd.append("--output-format=json")
        # terminal/md/html use default (csv) output from daff

        cmd.extend([str(old_path), str(new_path)])

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0 and result.stderr:
            raise DiffError(f"daff failed: {result.stderr}")

        # daff doesn't support --output-format=html; render via daff render
        if output_format == "html":
            return self._render_html(result.stdout)

        return result.stdout

    @staticmethod
    def _render_html(diff_csv: str) -> str:
        """Render a daff CSV diff as HTML using ``daff render``."""
        import tempfile
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as tmp:
            tmp.write(diff_csv)
            tmp_path = tmp.name
        try:
            render = subprocess.run(
                ["daff", "render", tmp_path],
                capture_output=True, text=True,
            )
            if render.returncode != 0 and render.stderr:
                raise DiffError(f"daff render failed: {render.stderr}")
            return render.stdout
        finally:
            Path(tmp_path).unlink(missing_ok=True)


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
        detail_level: str = "normal",
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
    compare_path: Optional[str] = None,
    output_format: str = "terminal",
    detail_level: str = "normal",
    verbose: bool = False,
) -> str:
    """Compute the content diff between two file versions.

    Two modes:

    **Revision mode** (default): compares two git/DVC revisions of the same
    file.  ``old_rev`` and ``new_rev`` select the revisions; ``new_rev=None``
    means the current workspace copy.

        content_diff("data.h5ad", old_rev="HEAD~1")

    **Direct mode**: compares two different files in the workspace, bypassing
    DVC entirely.  Set ``compare_path`` to the second file; ``old_rev`` and
    ``new_rev`` are ignored.

        content_diff("atlas_a.h5ad", compare_path="atlas_b.h5ad")

    Args:
        path:          Path to the first (old) file
        old_rev:       Git revision for the old version (revision mode only;
                       default: HEAD)
        new_rev:       Git revision for the new version (revision mode only;
                       default: None = workspace)
        compare_path:  Path to the second (new) file for direct comparison.
                       When set, revision mode is skipped entirely.
        output_format: Output format ('terminal', 'json', 'html', 'md')
        detail_level:  'summary', 'normal', or 'granular'
        verbose:       Show additional progress details

    Returns:
        Formatted diff string

    Raises:
        DiffError: If the diff cannot be computed
    """
    path = str(Path(path))

    # Choose handler based on the first path's extension
    handler = get_handler(path)
    if handler is None:
        handler = FallbackHandler()
        if verbose:
            print(f"No specific handler for {Path(path).suffix}, using fallback")
    elif verbose:
        print(f"Using {handler.format_name} handler")

    # -------------------------------------------------------------------------
    # Direct mode: compare two workspace files, no DVC involved
    # -------------------------------------------------------------------------
    if compare_path is not None:
        old_file = Path(path)
        new_file = Path(compare_path)
        if not old_file.exists():
            raise DiffError(f"'{path}' not found in workspace")
        if not new_file.exists():
            raise DiffError(f"'{compare_path}' not found in workspace")
        return handler.diff(old_file, new_file, output_format, detail_level)

    # -------------------------------------------------------------------------
    # Revision mode: fetch versions from DVC/git
    # -------------------------------------------------------------------------
    import os

    repo_abs = os.path.abspath('.')

    if verbose:
        import dvc as _dvc
        print(f"DVC version : {_dvc.__version__}")
        print(f"Repo path   : {repo_abs}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        old_file = tmpdir / f"old_{Path(path).name}"
        new_file = tmpdir / f"new_{Path(path).name}"

        def _fetch_revision(rev: str, dest: Path) -> None:
            """Fetch a DVC-tracked file at a git revision via subprocess."""
            if verbose:
                print(f"Fetching    : {path!r} @ {rev!r}")
            result = subprocess.run(
                ['dvc', 'get', '--rev', rev, '.', path, '--out', str(dest)],
                capture_output=True, text=True, cwd=repo_abs,
            )
            if result.returncode != 0:
                raise DiffError(
                    f"Failed to get '{path}' at revision '{rev}': "
                    f"{result.stderr.strip() or result.stdout.strip()}"
                )

        # Fetch old version
        _fetch_revision(old_rev, old_file)

        # Fetch new version
        if new_rev is None:
            # Use workspace version
            workspace_path = Path(path)
            if not workspace_path.exists():
                raise DiffError(f"'{path}' not found in workspace")
            new_file = workspace_path
        else:
            _fetch_revision(new_rev, new_file)

        return handler.diff(old_file, new_file, output_format, detail_level)


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
