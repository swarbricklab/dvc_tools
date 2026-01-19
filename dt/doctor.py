"""Diagnostic checks for DVC Tools.

Verifies environment setup and common configuration issues.
"""

import os
import subprocess
import shutil
from pathlib import Path
from typing import Optional, Tuple

from . import config as cfg


class DiagnosticResult:
    """Result of a diagnostic check."""
    
    def __init__(self, name: str, passed: bool, message: str, help_text: Optional[str] = None):
        self.name = name
        self.passed = passed
        self.message = message
        self.help_text = help_text
    
    def __str__(self) -> str:
        icon = "✓" if self.passed else "✗"
        result = f"{icon} {self.message}"
        if not self.passed and self.help_text:
            result += f"\n  {self.help_text}"
        return result


def get_dt_version() -> str:
    """Get the dt package version."""
    try:
        from importlib.metadata import version
        return version('dvc-tools')
    except Exception:
        return "unknown"


def check_command_version(command: str, version_arg: str = '--version') -> Tuple[bool, str]:
    """Check if a command is available and get its version.
    
    Returns:
        Tuple of (available, version_string)
    """
    if not shutil.which(command):
        return False, ""
    
    try:
        result = subprocess.run(
            [command, version_arg],
            capture_output=True,
            text=True,
            timeout=10,
        )
        # Extract version from output (first line, common patterns)
        output = result.stdout.strip() or result.stderr.strip()
        # Get first line and clean up
        version_line = output.split('\n')[0]
        return True, version_line
    except Exception:
        return True, "version unknown"


def check_git() -> DiagnosticResult:
    """Check if git is installed."""
    available, version_info = check_command_version('git')
    if available:
        return DiagnosticResult("git", True, f"Git installed ({version_info})")
    else:
        return DiagnosticResult(
            "git", False, "Git not found",
            "Install git: https://git-scm.com/downloads"
        )


def check_dvc() -> DiagnosticResult:
    """Check if DVC is installed."""
    available, version_info = check_command_version('dvc')
    if available:
        # Clean up DVC version output
        version = version_info.replace('dvc version ', '').split()[0] if version_info else "unknown"
        return DiagnosticResult("dvc", True, f"DVC installed ({version})")
    else:
        return DiagnosticResult(
            "dvc", False, "DVC not found",
            "Install DVC: https://dvc.org/doc/install"
        )


def check_gh() -> DiagnosticResult:
    """Check if GitHub CLI is installed."""
    available, version_info = check_command_version('gh')
    if available:
        # Clean up gh version output (e.g., "gh version 2.40.0 (2024-01-01)")
        version = version_info.replace('gh version ', '').split()[0] if version_info else "unknown"
        return DiagnosticResult("gh", True, f"GitHub CLI installed ({version})")
    else:
        return DiagnosticResult(
            "gh", False, "GitHub CLI not found",
            "Install: https://cli.github.com (optional, enables some features)"
        )


def check_ssh_key() -> DiagnosticResult:
    """Check if SSH keys exist."""
    ssh_dir = Path.home() / '.ssh'
    
    # Common key file patterns
    key_patterns = ['id_ed25519', 'id_rsa', 'id_ecdsa', 'id_dsa']
    
    for key_name in key_patterns:
        pub_key = ssh_dir / f"{key_name}.pub"
        if pub_key.exists():
            return DiagnosticResult(
                "ssh_key", True, 
                f"SSH key found ({pub_key})"
            )
    
    return DiagnosticResult(
        "ssh_key", False, "No SSH key found",
        'Run: ssh-keygen -t ed25519 -C "your.email@example.com"'
    )


def check_github_ssh() -> DiagnosticResult:
    """Check if SSH connection to GitHub works."""
    try:
        result = subprocess.run(
            ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', 'git@github.com'],
            capture_output=True,
            text=True,
            timeout=10,
        )
        # GitHub returns exit code 1 with success message, 255 for auth failure
        output = result.stdout + result.stderr
        if 'successfully authenticated' in output.lower() or 'Hi ' in output:
            # Extract username if present
            if 'Hi ' in output:
                username = output.split('Hi ')[1].split('!')[0]
                return DiagnosticResult("github_ssh", True, f"GitHub SSH works (authenticated as {username})")
            return DiagnosticResult("github_ssh", True, "GitHub SSH connection works")
        elif 'permission denied' in output.lower():
            return DiagnosticResult(
                "github_ssh", False, "GitHub SSH authentication failed",
                "Add your SSH key to GitHub: https://github.com/settings/keys"
            )
        else:
            return DiagnosticResult(
                "github_ssh", False, "GitHub SSH connection failed",
                "See: https://docs.github.com/en/authentication/connecting-to-github-with-ssh"
            )
    except subprocess.TimeoutExpired:
        return DiagnosticResult(
            "github_ssh", False, "GitHub SSH connection timed out",
            "Check your network connection and firewall settings"
        )
    except Exception as e:
        return DiagnosticResult(
            "github_ssh", False, f"GitHub SSH check failed: {e}",
            "See: https://docs.github.com/en/authentication/connecting-to-github-with-ssh"
        )


def check_cache_root() -> DiagnosticResult:
    """Check if cache root is configured and accessible."""
    cache_root = cfg.get_value('cache.root')
    
    if not cache_root:
        return DiagnosticResult(
            "cache_root", False, "Cache root not configured",
            "Run: dt config set cache.root /path/to/cache"
        )
    
    cache_path = Path(cache_root)
    if cache_path.exists():
        if os.access(cache_path, os.W_OK):
            return DiagnosticResult("cache_root", True, f"Cache root accessible ({cache_root})")
        else:
            return DiagnosticResult(
                "cache_root", False, f"Cache root not writable ({cache_root})",
                "Check directory permissions"
            )
    else:
        return DiagnosticResult(
            "cache_root", False, f"Cache root does not exist ({cache_root})",
            "Create the directory or update the configuration"
        )


def check_remote_root() -> DiagnosticResult:
    """Check if remote root is configured and accessible."""
    remote_root = cfg.get_value('remote.root')
    
    if not remote_root:
        return DiagnosticResult(
            "remote_root", False, "Remote root not configured",
            "Run: dt config set remote.root /path/to/remote"
        )
    
    remote_path = Path(remote_root)
    if remote_path.exists():
        if os.access(remote_path, os.W_OK):
            return DiagnosticResult("remote_root", True, f"Remote root accessible ({remote_root})")
        else:
            return DiagnosticResult(
                "remote_root", False, f"Remote root not writable ({remote_root})",
                "Check directory permissions"
            )
    else:
        return DiagnosticResult(
            "remote_root", False, f"Remote root does not exist ({remote_root})",
            "Create the directory or update the configuration"
        )


def run_diagnostics(verbose: bool = False) -> list[DiagnosticResult]:
    """Run all diagnostic checks.
    
    Args:
        verbose: If True, includes additional detailed checks
        
    Returns:
        List of DiagnosticResult objects
    """
    results = []
    
    # Core tool checks
    results.append(check_git())
    results.append(check_dvc())
    results.append(check_gh())
    
    # SSH checks
    results.append(check_ssh_key())
    results.append(check_github_ssh())
    
    # Configuration checks
    results.append(check_cache_root())
    results.append(check_remote_root())
    
    return results


def run_dvc_doctor() -> str:
    """Run dvc doctor and return output."""
    try:
        result = subprocess.run(
            ['dvc', 'doctor'],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.stdout + result.stderr
    except Exception as e:
        return f"Failed to run dvc doctor: {e}"


def get_config_with_sources() -> list[tuple[str, str, str]]:
    """Get configuration values with their sources."""
    return cfg.list_config_with_sources()
