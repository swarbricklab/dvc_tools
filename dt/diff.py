"""Show content differences between versions of DVC-tracked files.

Provides a plugin architecture for format-specific diffing (CSV, AnnData, etc.)
with graceful fallback for unsupported formats.
"""

import json
import subprocess
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from .errors import DiffError


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
# Main diff function
# =============================================================================

def diff(
    path: str,
    old_rev: str = "HEAD",
    new_rev: Optional[str] = None,
    output_format: str = "terminal",
    verbose: bool = False,
) -> str:
    """Compute the diff between two versions of a DVC-tracked file.
    
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
