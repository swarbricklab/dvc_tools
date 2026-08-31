"""List and filter DVC-tracked files.

Wraps `dvc list` with filtering capabilities for path patterns, size, type, and hash.
"""

import fnmatch
import html
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .errors import LsError


def parse_size(size_str: str) -> int:
    """Parse a human-readable size string to bytes.
    
    Args:
        size_str: Size string like '100', '10K', '5M', '1G', '2T'
        
    Returns:
        Size in bytes
        
    Raises:
        LsError: If size string is invalid
    """
    size_str = size_str.strip().upper()
    
    multipliers = {
        'K': 1024,
        'M': 1024 ** 2,
        'G': 1024 ** 3,
        'T': 1024 ** 4,
    }
    
    if size_str[-1] in multipliers:
        try:
            return int(float(size_str[:-1]) * multipliers[size_str[-1]])
        except ValueError:
            raise LsError(f"Invalid size: {size_str}")
    
    try:
        return int(size_str)
    except ValueError:
        raise LsError(f"Invalid size: {size_str}")


def format_size(size: int) -> str:
    """Format bytes as human-readable size.
    
    Args:
        size: Size in bytes
        
    Returns:
        Human-readable string like '1.5M', '256K'
    """
    if size is None:
        return '-'
    
    for unit, threshold in [('T', 1024**4), ('G', 1024**3), ('M', 1024**2), ('K', 1024)]:
        if size >= threshold:
            value = size / threshold
            if value >= 100:
                return f"{int(value)}{unit}"
            elif value >= 10:
                return f"{value:.1f}{unit}"
            else:
                return f"{value:.2f}{unit}"
    
    return str(size)


def run_dvc_list(
    url: str = '.',
    path: Optional[str] = None,
    rev: Optional[str] = None,
    recursive: bool = False,
    dvc_only: bool = True,
    size: bool = False,
    show_hash: bool = False,
) -> List[Dict[str, Any]]:
    """Run dvc list and return parsed JSON output.

    Args:
        url: Repository URL or '.' for local
        path: Path within repository to list
        rev: Git revision
        recursive: List recursively
        dvc_only: Only show DVC outputs
        size: Ask dvc to resolve each entry's size (``--size``)
        show_hash: Ask dvc to resolve each entry's md5 (``--show-hash``)

    Returns:
        List of item dictionaries with keys: isout, isdir, isexec, path
        (plus size when ``size`` is set and md5 when ``show_hash`` is set)

    Raises:
        LsError: If dvc list fails

    Note:
        ``--size``/``--show-hash`` force dvc to resolve sizes/hashes for every
        entry, which reads .dir manifests and the object store. On large trees
        (100k+ files) that is the expensive, mmap-heavy path, so only request
        them when the caller actually needs the values.
    """
    cmd = ['dvc', 'list', url, '--json']

    if size:
        cmd.append('--size')
    if show_hash:
        cmd.append('--show-hash')
    if path:
        cmd.append(path)
    if rev:
        cmd.extend(['--rev', rev])
    if recursive:
        cmd.append('--recursive')
    if dvc_only:
        cmd.append('--dvc-only')
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip() if e.stderr else str(e)
        raise LsError(f"dvc list failed: {error_msg}")
    except json.JSONDecodeError as e:
        raise LsError(f"Failed to parse dvc list output: {e}")


def filter_items(
    items: List[Dict[str, Any]],
    pattern: Optional[str] = None,
    regex: Optional[str] = None,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
    files_only: bool = False,
    dirs_only: bool = False,
    exec_only: bool = False,
    hash_prefix: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Filter list items by various criteria.
    
    Args:
        items: List of items from run_dvc_list
        pattern: Glob pattern for path matching
        regex: Regex pattern for path matching
        min_size: Minimum size in bytes
        max_size: Maximum size in bytes
        files_only: Only include files
        dirs_only: Only include directories
        exec_only: Only include executable files
        hash_prefix: Match items with hash starting with this prefix
        
    Returns:
        Filtered list of items
    """
    result = []
    
    # Compile regex if provided
    regex_compiled = None
    if regex:
        try:
            regex_compiled = re.compile(regex)
        except re.error as e:
            raise LsError(f"Invalid regex: {e}")
    
    for item in items:
        # Type filter
        if files_only and item.get('isdir'):
            continue
        if dirs_only and not item.get('isdir'):
            continue
        if exec_only and not item.get('isexec'):
            continue
        
        # Path pattern filter
        path = item.get('path', '')
        if pattern and not fnmatch.fnmatch(path, pattern):
            continue
        if regex_compiled and not regex_compiled.search(path):
            continue
        
        # Size filter
        size = item.get('size')
        if min_size is not None:
            if size is None or size < min_size:
                continue
        if max_size is not None:
            if size is None or size > max_size:
                continue
        
        # Hash filter
        if hash_prefix:
            md5 = item.get('md5')
            if not md5:
                continue
            # Remove .dir suffix for matching
            md5_clean = md5.replace('.dir', '').lower()
            if not md5_clean.startswith(hash_prefix.lower()):
                continue
        
        result.append(item)
    
    return result


def format_output(
    items: List[Dict[str, Any]],
    long_format: bool = False,
    show_hash: bool = False,
    json_output: bool = False,
) -> str:
    """Format filtered items for display.
    
    Args:
        items: List of filtered items
        long_format: Show size and type in addition to path
        show_hash: Show MD5 hash
        json_output: Output as JSON
        
    Returns:
        Formatted string
    """
    if json_output:
        return json.dumps(items, indent=2)
    
    if not items:
        return ""
    
    lines = []
    for item in items:
        path = item.get('path', '')
        
        if long_format or show_hash:
            parts = []
            
            if long_format:
                # Type indicator
                type_char = 'd' if item.get('isdir') else '-'
                parts.append(type_char)
                
                # Size
                size = item.get('size')
                parts.append(f"{format_size(size):>8}")
            
            if show_hash:
                md5 = item.get('md5') or '-'
                parts.append(f"{md5:>36}")
            
            parts.append(path)
            lines.append('  '.join(parts))
        else:
            lines.append(path)
    
    return '\n'.join(lines)


# =============================================================================
# Tree view
# =============================================================================

def build_tree(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build a nested directory tree from ``dvc list`` items.

    Each node is a dict mapping subdirectory names to child nodes, plus a
    special ``_files`` key holding the list of file names at that level.
    Directories are derived from the path components of each item, so a
    recursive listing (full paths) is required to build a complete tree.

    Args:
        items: List of items from run_dvc_list / filter_items

    Returns:
        Nested dict representing the directory tree
    """
    tree: Dict[str, Any] = {'_files': []}

    for item in items:
        path = item.get('path', '')
        if not path:
            continue

        parts = Path(path).parts
        current = tree

        # Every component except the last names a directory.
        for part in parts[:-1]:
            child = current.get(part)
            if not isinstance(child, dict):
                child = {'_files': []}
                current[part] = child
            current = child

        leaf = parts[-1] if parts else path
        if item.get('isdir'):
            # Ensure the directory node exists (it may otherwise be empty).
            if not isinstance(current.get(leaf), dict):
                current[leaf] = {'_files': []}
        else:
            current['_files'].append(leaf)

    return tree


def _count_tree(tree: Dict[str, Any]) -> Tuple[int, int]:
    """Count (files, directories) contained in a tree, recursively."""
    files = len(tree.get('_files', []))
    dirs = 0
    for key, value in tree.items():
        if key.startswith('_'):
            continue
        dirs += 1
        sub_files, sub_dirs = _count_tree(value)
        files += sub_files
        dirs += sub_dirs
    return files, dirs


def _subtree_file_count(node: Dict[str, Any]) -> int:
    """Number of files anywhere beneath ``node``, memoized on the node.

    Rendering asks for this count at every directory (fold-up summaries and
    collapse placeholders). Computing it naively re-walks each subtree once
    per ancestor -- super-linear on wide trees. Caching the result under the
    reserved ``_nf`` key makes the whole render O(number of nodes).
    """
    cached = node.get('_nf')
    if cached is not None:
        return cached
    n = len(node.get('_files', []))
    for key, value in node.items():
        if key.startswith('_'):
            continue
        n += _subtree_file_count(value)
    node['_nf'] = n
    return n


def _n_files(n: int) -> str:
    """Pluralize a file count, e.g. '1 file' / '3 files'."""
    return f"{n} file" if n == 1 else f"{n} files"


def _tree_entries(tree: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """Return (sorted subdirectory names, sorted file names) for a node."""
    subdirs = sorted(k for k in tree if not k.startswith('_'))
    files = sorted(tree.get('_files', []))
    return subdirs, files


def _render_tree_text(
    tree: Dict[str, Any],
    prefix: str = "",
    max_level: Optional[int] = None,
    current_level: int = 0,
) -> List[str]:
    """Render a tree as ASCII lines (directories first, then files).

    When ``max_level`` is set, contents deeper than that many levels are
    collapsed into a single ``… (N files)`` placeholder.
    """
    if max_level is not None and current_level >= max_level:
        n_files = _subtree_file_count(tree)
        return [f"{prefix}└── … ({_n_files(n_files)})"] if n_files else []

    subdirs, files = _tree_entries(tree)
    entries = [(name, True) for name in subdirs] + [(name, False) for name in files]

    lines = []
    for i, (name, is_dir) in enumerate(entries):
        is_last = (i == len(entries) - 1)
        connector = "└── " if is_last else "├── "
        if is_dir:
            lines.append(f"{prefix}{connector}{name}/")
            child_prefix = prefix + ("    " if is_last else "│   ")
            lines.extend(_render_tree_text(
                tree[name], child_prefix, max_level, current_level + 1,
            ))
        else:
            lines.append(f"{prefix}{connector}{name}")
    return lines


def _split_repo_url(src: str) -> Optional[Tuple[str, str]]:
    """Split a git/DVC repo URL into ``(host, owner/repo)`` or None.

    Handles scp-style ``git@host:owner/repo.git`` and
    ``scheme://[user@]host/owner/repo.git`` forms. Returns None for local
    paths or anything we can't turn into a host + path.
    """
    src = src.strip()

    # scp-like: git@github.com:owner/repo(.git)
    m = re.match(r'^[\w.-]+@([^:/]+):(.+?)(?:\.git)?/?$', src)
    if m:
        return m.group(1), m.group(2)

    # scheme://[user@]host/owner/repo(.git)
    m = re.match(r'^(?:https?|ssh|git)://(?:[^@/]+@)?([^/]+)/(.+?)(?:\.git)?/?$', src)
    if m:
        return m.group(1), m.group(2)

    return None


def _parse_repo_url(src: str) -> Tuple[str, Optional[str]]:
    """Parse a git/DVC repo URL into (repo name, browsable https URL).

    Returns ``web_url=None`` for local paths or hosts we can't turn into a
    browsable URL.
    """
    parts = _split_repo_url(src)
    if parts:
        host, owner_repo = parts
        return owner_repo.split('/')[-1], f"https://{host}/{owner_repo}"

    # Local path or unrecognized: use the basename, no web URL.
    name = Path(src.strip().rstrip('/')).name
    if name.endswith('.git'):
        name = name[:-4]
    return (name or src.strip()), None


def _repo_identity(url: str) -> Dict[str, Optional[str]]:
    """Resolve the repository name, web URL, and https/ssh clone URLs.

    For ``url='.'`` this reads ``git remote get-url origin``; otherwise the
    supplied URL is parsed directly. ``https_url``/``ssh_url`` are what the
    command popup offers for ``dvc get``/``dvc import`` (both derived from the
    same host + path so the two protocol tabs are always consistent). When the
    source can't be split into a host + path, both fall back to the raw source,
    or a ``<repo-url>`` placeholder for the user to fill in.
    """
    src: Optional[str] = None
    if url in ('.', '', None):
        try:
            r = subprocess.run(
                ['git', 'remote', 'get-url', 'origin'],
                capture_output=True, text=True,
            )
            if r.returncode == 0 and r.stdout.strip():
                src = r.stdout.strip()
        except OSError:
            src = None
    else:
        src = url

    parts = _split_repo_url(src) if src else None
    if parts:
        host, owner_repo = parts
        name = owner_repo.split('/')[-1]
        web_url = f"https://{host}/{owner_repo}"
        https_url = f"https://{host}/{owner_repo}.git"
        ssh_url = f"git@{host}:{owner_repo}.git"
    else:
        name = None
        web_url = None
        https_url = ssh_url = src or '<repo-url>'

    if not name:
        name = Path.cwd().name if url in ('.', '', None) else url

    return {
        'name': name,
        'web_url': web_url,
        'https_url': https_url,
        'ssh_url': ssh_url,
    }


def _root_label(repo: Dict[str, Optional[str]], base: str, rev: Optional[str]) -> str:
    """Plain-text label for the root of the tree (e.g. ``repo/subdir@rev``)."""
    label = repo['name'] or '.'
    if base:
        label = f"{label}/{base}"
    if rev:
        label = f"{label}@{rev}"
    return label


def _revision_line(revinfo: Optional[Dict[str, Any]]) -> str:
    """One-line ``sha · tags · date`` summary of a revision (empty if unknown)."""
    if not revinfo:
        return ''
    parts = [revinfo['sha']]
    if revinfo.get('tags'):
        parts.append(', '.join(revinfo['tags']))
    if revinfo.get('date'):
        parts.append(revinfo['date'])
    return ' · '.join(parts)


def _format_tree_text(
    tree: Dict[str, Any], repo: Dict[str, Optional[str]], base: str,
    rev: Optional[str], revinfo: Optional[Dict[str, Any]] = None,
    max_level: Optional[int] = None,
) -> str:
    """Format a tree for terminal display."""
    files, dirs = _count_tree(tree)
    lines = [_root_label(repo, base, rev)]
    rline = _revision_line(revinfo)
    if rline:
        lines.append(rline)
    lines.extend(_render_tree_text(tree, max_level=max_level))
    lines.append("")
    lines.append(f"{dirs} directories, {files} files")
    return '\n'.join(lines)


def _format_tree_md(
    tree: Dict[str, Any], repo: Dict[str, Optional[str]], base: str,
    rev: Optional[str], revinfo: Optional[Dict[str, Any]] = None,
    max_level: Optional[int] = None,
) -> str:
    """Format a tree as markdown (a fenced code block plus a summary line)."""
    files, dirs = _count_tree(tree)
    label = _root_label(repo, base, rev)
    heading = f"[{label}]({repo['web_url']})" if repo['web_url'] else label
    lines = [f"**Repository tree:** {heading} — {dirs} directories, {files} files"]
    rline = _revision_line(revinfo)
    if rline:
        lines.append("")
        lines.append(f"_{rline}_")
    lines.extend(["", "```text", label,
                  *_render_tree_text(tree, max_level=max_level), "```"])
    return '\n'.join(lines)


def _cmd_button() -> str:
    """The per-node button that opens the dvc get/import command popup."""
    return ('<button class="cmd-btn" type="button" '
            'title="Show dvc get / import commands">dvc</button>')


def _render_tree_html(
    tree: Dict[str, Any],
    indent: int = 3,
    max_level: Optional[int] = None,
    current_level: int = 0,
    parent: str = "",
) -> str:
    """Render a tree as collapsible ``<details>``/``<summary>`` HTML.

    Each directory and file carries a ``data-path`` attribute with its full
    repo-relative path (``parent`` accumulates the prefix), which the command
    popup reads. When ``max_level`` is set, contents deeper than that many
    levels are collapsed into a single ``… (N files)`` placeholder.
    """
    spaces = "    " * indent

    if max_level is not None and current_level >= max_level:
        n_files = _subtree_file_count(tree)
        if n_files:
            return f'{spaces}<div class="file counts">… ({_n_files(n_files)})</div>'
        return ''

    subdirs, files = _tree_entries(tree)
    btn = _cmd_button()

    lines = []
    for name in subdirs:
        child = tree[name]
        full = f"{parent}/{name}" if parent else name
        attr = html.escape(full, quote=True)
        n_files = _subtree_file_count(child)
        count_str = (
            f' <span class="counts">({n_files})</span>' if n_files else ''
        )
        lines.append(f'{spaces}<details data-path="{attr}">')
        lines.append(
            f'{spaces}    <summary>{html.escape(name)}/{count_str} {btn}</summary>'
        )
        child_html = _render_tree_html(
            child, indent + 1, max_level, current_level + 1, full,
        )
        if child_html:
            lines.append(child_html)
        lines.append(f'{spaces}</details>')

    for name in files:
        full = f"{parent}/{name}" if parent else name
        attr = html.escape(full, quote=True)
        lines.append(
            f'{spaces}<div class="file" data-path="{attr}">'
            f'{html.escape(name)} {btn}</div>'
        )

    return '\n'.join(lines)


# Rendered once into every HTML tree. Kept as plain strings (not an f-string)
# so the CSS/JS braces need no escaping; values are substituted with .replace().
_TREE_HTML_STYLE = """
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            line-height: 1.5;
            padding: 20px;
            max-width: 900px;
            margin: 0 auto;
        }
        .repo-title { margin: 0 0 4px 0; font-size: 20px; }
        .repo-title a { color: #0366d6; text-decoration: none; }
        .repo-title a:hover { text-decoration: underline; }
        .summary {
            margin-bottom: 20px;
            padding: 10px 15px;
            background: #f6f8fa;
            border-radius: 6px;
        }
        .subtitle { color: #6a737d; font-size: 13px; }
        .subtitle code { font-size: 12px; }
        .rev-tags {
            background: #ddf4ff;
            color: #0969da;
            border-radius: 4px;
            padding: 1px 6px;
            font-size: 12px;
        }
        .tree {
            font-family: "SF Mono", Consolas, monospace;
            font-size: 13px;
        }
        details { margin-left: 20px; }
        details > summary { cursor: pointer; list-style: none; }
        details > summary::-webkit-details-marker { display: none; }
        details > summary::before { content: "\\25B6 "; font-size: 10px; }
        details[open] > summary::before { content: "\\25BC "; }
        .file { margin-left: 20px; padding: 2px 0; }
        .counts { color: #6a737d; font-size: 12px; }
        .controls { margin-bottom: 12px; }
        .controls button {
            font-size: 13px;
            padding: 4px 12px;
            margin-right: 6px;
            border: 1px solid #d1d5da;
            border-radius: 4px;
            background: #f6f8fa;
            cursor: pointer;
        }
        .controls button:hover { background: #e1e4e8; }
        .cmd-btn {
            font-family: inherit;
            font-size: 11px;
            line-height: 1;
            padding: 2px 6px;
            margin-left: 4px;
            border: 1px solid #d1d5da;
            border-radius: 4px;
            background: #fff;
            color: #6a737d;
            cursor: pointer;
            opacity: 0;
            transition: opacity 0.1s;
        }
        summary:hover .cmd-btn, .file:hover .cmd-btn, .cmd-btn:focus { opacity: 1; }
        #cmd-overlay {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.4);
            align-items: center;
            justify-content: center;
            z-index: 1000;
        }
        #cmd-box {
            background: #fff;
            border-radius: 8px;
            padding: 20px 22px;
            max-width: 640px;
            width: calc(100% - 48px);
            box-shadow: 0 8px 30px rgba(0, 0, 0, 0.2);
            position: relative;
        }
        #cmd-close {
            position: absolute;
            top: 8px;
            right: 10px;
            border: none;
            background: none;
            font-size: 22px;
            line-height: 1;
            color: #6a737d;
            cursor: pointer;
        }
        #cmd-path {
            font-family: "SF Mono", Consolas, monospace;
            font-weight: 600;
            margin-bottom: 12px;
            word-break: break-all;
        }
        .cmd-label { display: block; font-size: 12px; color: #6a737d; margin: 12px 0 4px; }
        .cmd-tabs { display: flex; gap: 4px; margin-bottom: 4px; }
        .cmd-tab {
            font-size: 11px;
            padding: 2px 10px;
            border: 1px solid #d1d5da;
            border-radius: 4px;
            background: #f6f8fa;
            color: #57606a;
            cursor: pointer;
        }
        .cmd-tab:hover { background: #eaeef2; }
        .cmd-tab.active {
            background: #0366d6;
            border-color: #0366d6;
            color: #fff;
        }
        .cmd-row { display: flex; align-items: stretch; gap: 8px; }
        .cmd-row code {
            flex: 1;
            font-family: "SF Mono", Consolas, monospace;
            font-size: 12px;
            background: #f6f8fa;
            border: 1px solid #e1e4e8;
            border-radius: 4px;
            padding: 8px 10px;
            white-space: pre-wrap;
            word-break: break-all;
        }
        .cmd-row .copy {
            border: 1px solid #d1d5da;
            border-radius: 4px;
            background: #f6f8fa;
            cursor: pointer;
            font-size: 12px;
            padding: 0 12px;
        }
        .cmd-row .copy:hover { background: #e1e4e8; }
"""

_TREE_HTML_SCRIPT = """
        const REPO_HTTPS = %%HTTPS_JS%%;
        const REPO_SSH = %%SSH_JS%%;
        const REV = %%REV_JS%%;

        document.querySelectorAll('details').forEach(d => d.open = false);

        // Protocol is shared across both commands (so the two tab strips stay
        // in sync) and remembered, but defaults to HTTPS on first use.
        let proto = 'https';
        try { if (localStorage.getItem('dtLsProto') === 'ssh') proto = 'ssh'; } catch (e) {}
        let currentPath = null;

        function tabStrip() {
            return '<div class="cmd-tabs">' +
                '<button class="cmd-tab" type="button" data-proto="https">HTTPS</button>' +
                '<button class="cmd-tab" type="button" data-proto="ssh">SSH</button>' +
                '</div>';
        }
        function cmdBlock(cmd, label) {
            return '<div class="cmd-block" data-cmd="' + cmd + '">' +
                '<label class="cmd-label">' + label + '</label>' +
                tabStrip() +
                '<div class="cmd-row"><code class="cmd-code"></code>' +
                    '<button class="copy" type="button">Copy</button></div>' +
                '</div>';
        }

        const overlay = document.createElement('div');
        overlay.id = 'cmd-overlay';
        overlay.innerHTML =
            '<div id="cmd-box" role="dialog" aria-modal="true">' +
                '<button id="cmd-close" type="button" title="Close" aria-label="Close">\\u00d7</button>' +
                '<div id="cmd-path"></div>' +
                cmdBlock('get', 'Download a copy') +
                cmdBlock('import', 'Import (track for updates)') +
            '</div>';
        document.body.appendChild(overlay);

        // Nothing to choose when the two clone URLs are identical (e.g. a local
        // path or an unresolved remote): hide the protocol tabs.
        if (REPO_HTTPS === REPO_SSH) {
            overlay.querySelectorAll('.cmd-tabs').forEach(t => t.style.display = 'none');
        }

        function revFlag() { return REV ? ' --rev ' + REV : ''; }
        function repoUrl() { return proto === 'ssh' ? REPO_SSH : REPO_HTTPS; }
        function shquote(p) {
            return /[^\\w@%+=:,.\\/-]/.test(p) ? "'" + p.replace(/'/g, "'\\\\''") + "'" : p;
        }
        function render() {
            if (currentPath === null) return;
            const q = shquote(currentPath);
            overlay.querySelectorAll('.cmd-block').forEach(function (block) {
                const cmd = block.getAttribute('data-cmd');
                block.querySelector('.cmd-code').textContent =
                    'dvc ' + cmd + ' ' + repoUrl() + ' ' + q + revFlag();
                block.querySelectorAll('.cmd-tab').forEach(function (t) {
                    t.classList.toggle('active', t.getAttribute('data-proto') === proto);
                });
            });
        }
        function showCommands(path) {
            currentPath = path;
            document.getElementById('cmd-path').textContent = path;
            render();
            overlay.style.display = 'flex';
        }
        function hide() { overlay.style.display = 'none'; }

        overlay.addEventListener('click', function (e) {
            if (e.target === overlay) { hide(); return; }
            const tab = e.target.closest('.cmd-tab');
            if (tab) {
                proto = tab.getAttribute('data-proto');
                try { localStorage.setItem('dtLsProto', proto); } catch (e) {}
                render();
                return;
            }
            const c = e.target.closest('.copy');
            if (c) {
                const code = c.closest('.cmd-row').querySelector('.cmd-code');
                navigator.clipboard.writeText(code.textContent).then(function () {
                    const orig = c.textContent;
                    c.textContent = 'Copied';
                    setTimeout(function () { c.textContent = orig; }, 1200);
                });
            }
        });
        document.getElementById('cmd-close').addEventListener('click', hide);
        document.addEventListener('keydown', function (e) { if (e.key === 'Escape') hide(); });
        document.querySelector('.tree').addEventListener('click', function (e) {
            const btn = e.target.closest('.cmd-btn');
            if (!btn) return;
            e.preventDefault();
            e.stopPropagation();
            const node = btn.closest('[data-path]');
            if (node) showCommands(node.getAttribute('data-path'));
        });
"""

_TREE_HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>%%TITLE%%</title>
    <style>%%STYLE%%    </style>
</head>
<body>
    <div class="summary">
        <h1 class="repo-title">%%HEADER%%</h1>
        <div class="subtitle">%%SUBTITLE%%%%COUNTS%%</div>
    </div>
    <div class="controls">
        <button onclick="document.querySelectorAll('details').forEach(d => d.open = true)">Expand all</button>
        <button onclick="document.querySelectorAll('details').forEach(d => d.open = false)">Collapse all</button>
    </div>
    <div class="tree">
%%TREE%%
    </div>
    <script>%%SCRIPT%%    </script>
</body>
</html>"""


def _revision_html(revinfo: Optional[Dict[str, Any]], rev: Optional[str]) -> str:
    """HTML fragment for the revision subtitle (sha · tags · date)."""
    if not revinfo:
        # Remote listing we couldn't resolve locally: show the raw ref if any.
        return f'at <code>{html.escape(rev)}</code>' if rev else ''
    bits = [f'<code>{html.escape(revinfo["sha"])}</code>']
    if revinfo.get('tags'):
        tags = ', '.join(html.escape(t) for t in revinfo['tags'])
        bits.append(f'<span class="rev-tags">{tags}</span>')
    if revinfo.get('date'):
        bits.append(html.escape(revinfo['date']))
    return ' · '.join(bits)


def _format_tree_html(
    tree: Dict[str, Any], repo: Dict[str, Optional[str]], base: str,
    rev: Optional[str], revinfo: Optional[Dict[str, Any]] = None,
    max_level: Optional[int] = None,
) -> str:
    """Format a tree as an interactive HTML document with fold-up sections.

    The title is the repository name linked to its web page, a subtitle shows
    the revision (short SHA, any tags, and date), and each node exposes a popup
    with copy-pastable ``dvc get`` / ``dvc import`` commands. Mirrors the
    collapsible display used by ``dt diff -o html``.
    """
    files, dirs = _count_tree(tree)
    name = html.escape(repo['name'] or '.')
    if repo['web_url']:
        header = f'<a href="{html.escape(repo["web_url"], quote=True)}">{name}</a>'
    else:
        header = name

    subtitle_bits = []
    revision = _revision_html(revinfo, rev)
    if revision:
        subtitle_bits.append(revision)
    if base:
        subtitle_bits.append(f'<code>{html.escape(base)}/</code>')
    subtitle = (' — '.join(subtitle_bits) + ' — ') if subtitle_bits else ''
    counts = f'{dirs} directories, {files} files'

    html_tree = _render_tree_html(tree, max_level=max_level, parent=base)
    script = (_TREE_HTML_SCRIPT
              .replace('%%HTTPS_JS%%', json.dumps(repo['https_url']))
              .replace('%%SSH_JS%%', json.dumps(repo['ssh_url']))
              .replace('%%REV_JS%%', json.dumps(rev or '')))

    return (_TREE_HTML_TEMPLATE
            .replace('%%TITLE%%', html.escape(_root_label(repo, base, rev)))
            .replace('%%STYLE%%', _TREE_HTML_STYLE)
            .replace('%%HEADER%%', header)
            .replace('%%SUBTITLE%%', subtitle)
            .replace('%%COUNTS%%', counts)
            .replace('%%TREE%%', html_tree)
            .replace('%%SCRIPT%%', script))


def _run_git(args: List[str]) -> Optional[str]:
    """Run a git command; return stripped stdout, or None on any failure."""
    try:
        r = subprocess.run(['git'] + args, capture_output=True, text=True)
    except OSError:
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def _git_path_set(args: List[str]) -> Optional[set]:
    """Run a path-listing git command and return the paths as a set (or None)."""
    out = _run_git(args)
    if out is None:
        return None
    return {line for line in out.split('\n') if line}


def _git_tracked_paths() -> Optional[set]:
    """Repo-root-relative paths git tracks in the index/workspace (or None)."""
    return _git_path_set(['ls-files'])


def _git_ignored_paths() -> Optional[set]:
    """Repo-root-relative paths git ignores in the working tree (or None)."""
    return _git_path_set(
        ['ls-files', '--others', '--ignored', '--exclude-standard']
    )


def _git_revision_info(rev: Optional[str]) -> Optional[Dict[str, Any]]:
    """Resolve (short SHA, tags pointing at it, commit date) for *rev*.

    Defaults to HEAD. Returns None outside a git repo or if the ref is unknown.
    """
    ref = rev or 'HEAD'
    sha = _run_git(['rev-parse', '--short', ref])
    if not sha:
        return None
    date = _run_git(['show', '-s', '--format=%cs', ref])  # YYYY-MM-DD
    tags_out = _run_git(['tag', '--points-at', ref])
    tags = [t for t in tags_out.split('\n') if t] if tags_out else []
    return {'sha': sha, 'date': date, 'tags': tags}


# Paths hidden from the tree: only the tracked *objects* are shown, not the
# files/dirs that point at, configure, or ignore them.
_HIDDEN_BASENAMES = {'dvc.lock', '.gitignore', '.dvcignore'}
_HIDDEN_DIRS = {'.dvc', '.dt'}


def _is_hidden_in_tree(rel: str) -> bool:
    """True for paths the tree suppresses.

    Covers DVC pointer/metadata (``*.dvc``, ``dvc.lock``), VCS/DVC ignore files
    (``.gitignore``, ``.dvcignore``), and the ``.dvc``/``.dt`` tooling
    directories together with everything beneath them.
    """
    parts = Path(rel).parts
    if any(p in _HIDDEN_DIRS for p in parts):
        return True
    name = parts[-1] if parts else rel
    return name in _HIDDEN_BASENAMES or name.endswith('.dvc')


def _filter_tree_items(
    items: List[Dict[str, Any]],
    base: str,
    apply_git: bool,
    include_untracked: bool,
) -> List[Dict[str, Any]]:
    """Reduce a dvc-list result to the tracked/ignored view for the tree.

    Always drops the paths in :func:`_is_hidden_in_tree` (DVC pointer/metadata,
    ``.gitignore``/``.dvcignore``, and the ``.dvc``/``.dt`` dirs) so only the
    tracked objects show, and always keeps DVC outputs (``isout``). ``dvc list``
    already honours ``.dvcignore``, so dvc-ignored paths are absent to begin
    with.

    When ``apply_git`` (a local workspace listing), non-DVC files are kept only
    when git-tracked (default) or -- with ``include_untracked`` -- when git
    does not ignore them. When not ``apply_git`` (a remote URL or a committed
    ``--rev``), every listed path is already part of a tracked tree, so only
    the bookkeeping files are removed.

    dvc-list paths are relative to the listed subdir while git reports
    repo-root-relative paths, so ``base`` reconciles the prefix.
    """
    if not apply_git:
        return [it for it in items
                if not _is_hidden_in_tree(it.get('path', ''))]

    tracked = None if include_untracked else _git_tracked_paths()
    ignored = _git_ignored_paths() if include_untracked else None

    kept = []
    for it in items:
        rel = it.get('path', '')
        if _is_hidden_in_tree(rel):
            continue
        if it.get('isout'):
            kept.append(it)          # DVC-tracked object: always shown
            continue
        full = f"{base}/{rel}" if base else rel
        if include_untracked:
            # --all: everything git does not ignore (None => git unavailable,
            # so we cannot exclude and fall back to showing the file).
            if ignored is not None and full in ignored:
                continue
        else:
            # default: only git-tracked files (None => keep, as above).
            if tracked is not None and full not in tracked:
                continue
        kept.append(it)
    return kept


def tree_view(
    url: str = '.',
    path: Optional[str] = None,
    rev: Optional[str] = None,
    output_format: str = 'text',
    dvc_only: bool = False,
    include_untracked: bool = False,
    level: Optional[int] = None,
    pattern: Optional[str] = None,
    regex: Optional[str] = None,
    min_size: Optional[str] = None,
    max_size: Optional[str] = None,
    files_only: bool = False,
    dirs_only: bool = False,
    exec_only: bool = False,
    hash_prefix: Optional[str] = None,
) -> str:
    """List a repository as a tree, rendered for terminal, markdown, or HTML.

    Wraps ``dvc list --recursive`` and renders a nested tree of the tracked
    objects. By default this is the git-tracked + DVC-tracked view: untracked
    and git-ignored files are excluded, as are DVC pointer/metadata files
    (``*.dvc``, ``dvc.lock``), ``.gitignore``/``.dvcignore``, and the
    ``.dvc``/``.dt`` tooling directories -- only the tracked objects are shown.
    (``dvc list`` already honours ``.dvcignore``.) ``include_untracked`` adds
    untracked files back while still excluding ignored ones; ``dvc_only``
    restricts to DVC outputs.

    The HTML format uses the same collapsible fold-up display as
    ``dt diff -o html``, with the repository name linked to its web page and a
    per-node popup of copy-pastable ``dvc get`` / ``dvc import`` commands.

    Args:
        url: Repository URL or '.' for this repo (any URL ``dvc list``
            accepts, including remote git/DVC repositories)
        path: Path within repository to list
        rev: Git revision
        output_format: One of 'text', 'md', 'html'
        dvc_only: Only include DVC outputs
        include_untracked: Also show untracked files (still excludes
            git-ignored and dvc-ignored); only affects a local ('.') listing
        level: Maximum tree depth to render; deeper contents are collapsed
            into a ``… (N files)`` placeholder (None = unlimited)
        pattern: Glob pattern for path matching
        regex: Regex pattern for path matching
        min_size: Minimum size (e.g., '100K', '1M')
        max_size: Maximum size (e.g., '1G')
        files_only: Only include files
        dirs_only: Only include directories
        exec_only: Only include executable files
        hash_prefix: Match items with hash starting with this prefix

    Returns:
        Rendered tree as a string

    Raises:
        LsError: If listing, filtering, the output format, or level is invalid
    """
    if output_format not in ('text', 'md', 'html'):
        raise LsError(
            f"Invalid tree output format: {output_format} "
            "(expected 'text', 'md', or 'html')"
        )

    if level is not None and level < 1:
        raise LsError(f"--level must be 1 or greater, got: {level}")

    min_bytes = parse_size(min_size) if min_size else None
    max_bytes = parse_size(max_size) if max_size else None

    # The tree shows names only, so sizes/hashes are needed solely to satisfy a
    # --min-size/--max-size/--hash filter. Skipping them keeps dvc off its
    # expensive, mmap-heavy resolution path on large (100k+ file) trees.
    need_meta_size = min_bytes is not None or max_bytes is not None
    need_meta_hash = hash_prefix is not None

    items = run_dvc_list(
        url=url,
        path=path,
        rev=rev,
        recursive=True,
        dvc_only=dvc_only,
        size=need_meta_size,
        show_hash=need_meta_hash,
    )

    # Reduce to the tracked view (drop untracked/ignored + DVC bookkeeping).
    # --dvc-only already restricted the listing to outputs, so there is nothing
    # more to strip there. Git-set filtering only makes sense for a local
    # workspace listing; a remote URL or a committed --rev is already a fully
    # tracked tree, so we just drop the bookkeeping files.
    base = '' if path in (None, '', '.') else path.strip('/')
    if not dvc_only:
        apply_git = url in ('.', '', None) and rev is None
        items = _filter_tree_items(
            items, base, apply_git=apply_git, include_untracked=include_untracked,
        )

    filtered = filter_items(
        items,
        pattern=pattern,
        regex=regex,
        min_size=min_bytes,
        max_size=max_bytes,
        files_only=files_only,
        dirs_only=dirs_only,
        exec_only=exec_only,
        hash_prefix=hash_prefix,
    )

    tree = build_tree(filtered)
    repo = _repo_identity(url)
    revinfo = _git_revision_info(rev) if url in ('.', '', None) else None

    if output_format == 'html':
        return _format_tree_html(tree, repo, base, rev, revinfo, max_level=level)
    if output_format == 'md':
        return _format_tree_md(tree, repo, base, rev, revinfo, max_level=level)
    return _format_tree_text(tree, repo, base, rev, revinfo, max_level=level)


def list_files(
    url: str = '.',
    path: Optional[str] = None,
    rev: Optional[str] = None,
    recursive: bool = False,
    dvc_only: bool = True,
    pattern: Optional[str] = None,
    regex: Optional[str] = None,
    min_size: Optional[str] = None,
    max_size: Optional[str] = None,
    files_only: bool = False,
    dirs_only: bool = False,
    exec_only: bool = False,
    hash_prefix: Optional[str] = None,
    long_format: bool = False,
    show_hash: bool = False,
    json_output: bool = False,
) -> Tuple[List[Dict[str, Any]], str]:
    """List and filter DVC-tracked files.
    
    Main entry point that combines listing, filtering, and formatting.
    
    Args:
        url: Repository URL or '.' for local
        path: Path within repository to list
        rev: Git revision
        recursive: List recursively
        dvc_only: Only show DVC outputs
        pattern: Glob pattern for path matching
        regex: Regex pattern for path matching
        min_size: Minimum size (e.g., '100K', '1M')
        max_size: Maximum size (e.g., '1G')
        files_only: Only include files
        dirs_only: Only include directories
        exec_only: Only include executable files
        hash_prefix: Match items with hash starting with this prefix
        long_format: Show size and type
        show_hash: Show MD5 hash
        json_output: Output as JSON
        
    Returns:
        Tuple of (filtered items, formatted output string)
        
    Raises:
        LsError: If listing or filtering fails
    """
    # Parse size strings
    min_bytes = parse_size(min_size) if min_size else None
    max_bytes = parse_size(max_size) if max_size else None

    # Only ask dvc to resolve sizes/hashes when we actually display or filter
    # on them -- these flags are the expensive path on large trees. JSON output
    # carries every field, so request both there to preserve its shape.
    need_size = bool(long_format or min_bytes is not None or max_bytes is not None
                     or json_output)
    need_hash = bool(show_hash or hash_prefix or json_output)

    # Get items from dvc list
    items = run_dvc_list(
        url=url,
        path=path,
        rev=rev,
        recursive=recursive,
        dvc_only=dvc_only,
        size=need_size,
        show_hash=need_hash,
    )
    
    # Apply filters
    filtered = filter_items(
        items,
        pattern=pattern,
        regex=regex,
        min_size=min_bytes,
        max_size=max_bytes,
        files_only=files_only,
        dirs_only=dirs_only,
        exec_only=exec_only,
        hash_prefix=hash_prefix,
    )
    
    # Format output
    output = format_output(
        filtered,
        long_format=long_format,
        show_hash=show_hash,
        json_output=json_output,
    )
    
    return filtered, output
