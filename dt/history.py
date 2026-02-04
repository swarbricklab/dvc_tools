"""Show version history of DVC-tracked files.

Lists the different versions (checksums) of a file across git history,
showing when each version was introduced.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from .errors import HistoryError
from .offline import status as offline_status, enable as offline_enable, disable as offline_disable
from .utils import get_candidate_commits, get_commit_info, get_hash_at_rev


def history(
    path: str,
    limit: Optional[int] = None,
    since: Optional[str] = None,
    json_output: bool = False,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """Get version history for a DVC-tracked file.
    
    Finds all commits where the file's DVC hash changed, showing when
    each version was introduced.
    
    Args:
        path: Path to the DVC-tracked file or directory
        limit: Maximum number of versions to return
        since: Only look at commits since this date (e.g., "2025-01-01")
        json_output: If True, return raw data (for JSON output)
        verbose: If True, include additional details
        
    Returns:
        List of version entries, each containing:
            - commit: Full commit hash
            - short_commit: Abbreviated commit hash
            - date: Commit date
            - message: Commit message
            - author: Commit author
            - hash: DVC file hash at this version
            
    Raises:
        HistoryError: If the file is not tracked by DVC or other errors
    """
    try:
        from dvc.repo import Repo
    except ImportError as e:
        raise HistoryError(f"DVC not available: {e}")
    
    # Normalize path
    path = str(Path(path))
    
    # Enable offline mode to avoid slow import clones (optional optimization)
    was_offline = True  # Assume offline so we don't try to disable
    try:
        was_offline = offline_status().get('enabled', False)
        if not was_offline:
            offline_enable()
    except Exception:
        # Offline mode not available (e.g., no .dt directory) - continue without it
        pass
    
    try:
        # Initialize DVC repo once for reuse
        repo = Repo()
        
        # Get candidate commits (those that touched DVC metadata)
        # Over-sample if we have a limit, since not all commits will have our file
        sample_limit = limit * 5 if limit else None
        candidates = get_candidate_commits(since=since, limit=sample_limit)
        
        if not candidates:
            raise HistoryError(f"No DVC metadata commits found")
        
        # Check each candidate for hash changes
        results = []
        prev_hash = None
        
        # Process oldest to newest so we can detect changes
        for commit in reversed(candidates):
            file_hash = get_hash_at_rev(path, commit, repo=repo)
            
            if file_hash is None:
                # File didn't exist at this revision
                continue
            
            if file_hash != prev_hash:
                # New version found
                info = get_commit_info(commit)
                results.append({
                    'commit': info['hash'],
                    'short_commit': info['short_hash'],
                    'date': info['date'],
                    'message': info['message'],
                    'author': info['author'],
                    'hash': file_hash,
                })
                prev_hash = file_hash
                
                if limit and len(results) >= limit:
                    break
        
        # Reverse to show newest first
        results.reverse()
        
        if not results:
            raise HistoryError(f"'{path}' has no DVC-tracked history")
        
        return results
        
    except HistoryError:
        raise
    except Exception as e:
        raise HistoryError(f"Failed to get history for '{path}': {e}")
    finally:
        # Restore offline mode if we enabled it
        if not was_offline:
            try:
                offline_disable()
            except Exception:
                pass


def format_history(
    entries: List[Dict[str, Any]],
    json_output: bool = False,
    verbose: bool = False,
) -> str:
    """Format history entries for display.
    
    Args:
        entries: List of history entries from history()
        json_output: If True, output as JSON
        verbose: If True, show full commit hashes and author
        
    Returns:
        Formatted string for display
    """
    if json_output:
        return json.dumps(entries, indent=2)
    
    if not entries:
        return "No history found"
    
    lines = []
    
    # Header
    if verbose:
        lines.append(f"{'COMMIT':<12}  {'DATE':<12}  {'AUTHOR':<20}  {'HASH':<34}  MESSAGE")
        lines.append("-" * 100)
    else:
        lines.append(f"{'COMMIT':<9}  {'DATE':<12}  {'HASH':<16}  MESSAGE")
        lines.append("-" * 60)
    
    for entry in entries:
        if verbose:
            lines.append(
                f"{entry['short_commit']:<12}  "
                f"{entry['date']:<12}  "
                f"{entry['author'][:20]:<20}  "
                f"{entry['hash']:<34}  "
                f"{entry['message'][:40]}"
            )
        else:
            # Truncate hash for compact display
            short_hash = entry['hash'][:16]
            lines.append(
                f"{entry['short_commit']:<9}  "
                f"{entry['date']:<12}  "
                f"{short_hash:<16}  "
                f"{entry['message'][:40]}"
            )
    
    return '\n'.join(lines)
