"""Push DVC-tracked files to all project-configured remotes."""

import subprocess
import sys
from typing import List, Tuple


class PushError(Exception):
    """Error during push operation."""
    pass


def get_project_remotes() -> List[Tuple[str, str]]:
    """Get remotes configured at project or local scope.
    
    Returns:
        List of (name, url) tuples for remotes in project/local config.
    """
    remotes = []
    
    # Check both local and project scopes
    for scope in ['local', 'project']:
        try:
            result = subprocess.run(
                ['dvc', 'remote', 'list', f'--{scope}'],
                capture_output=True,
                text=True,
                check=True,
            )
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    parts = line.split(None, 1)
                    if len(parts) >= 1:
                        name = parts[0]
                        url = parts[1] if len(parts) > 1 else ''
                        # Avoid duplicates (local overrides project)
                        if not any(r[0] == name for r in remotes):
                            remotes.append((name, url))
        except subprocess.CalledProcessError:
            # Scope might not exist, continue
            continue
    
    return remotes


def push_to_remote(remote: str, args: List[str]) -> Tuple[bool, str]:
    """Push to a single remote, passing through all arguments.
    
    Args:
        remote: Name of the remote to push to.
        args: Additional arguments to pass to dvc push.
        
    Returns:
        Tuple of (success, output).
    """
    cmd = ['dvc', 'push', '-r', remote] + list(args)
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output.strip()
    except Exception as e:
        return False, str(e)


def push_all(args: List[str]) -> List[Tuple[str, bool, str]]:
    """Push to all project-configured remotes.
    
    Args:
        args: Arguments to pass through to dvc push.
        
    Returns:
        List of (remote_name, success, output) tuples.
    """
    remotes = get_project_remotes()
    
    if not remotes:
        raise PushError("No remotes configured at project or local scope.")
    
    results = []
    for name, url in remotes:
        success, output = push_to_remote(name, args)
        results.append((name, success, output))
    
    return results
