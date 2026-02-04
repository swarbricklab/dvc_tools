"""List and filter DVC-tracked files.

Wraps `dvc list` with filtering capabilities for path patterns, size, type, and hash.
"""

import fnmatch
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
) -> List[Dict[str, Any]]:
    """Run dvc list and return parsed JSON output.
    
    Args:
        url: Repository URL or '.' for local
        path: Path within repository to list
        rev: Git revision
        recursive: List recursively
        dvc_only: Only show DVC outputs
        
    Returns:
        List of item dictionaries with keys: isout, isdir, isexec, size, md5, path
        
    Raises:
        LsError: If dvc list fails
    """
    cmd = ['dvc', 'list', url, '--json', '--size', '--show-hash']
    
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
    
    # Get items from dvc list
    items = run_dvc_list(
        url=url,
        path=path,
        rev=rev,
        recursive=recursive,
        dvc_only=dvc_only,
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
