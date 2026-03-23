"""Diagnostic checks for DVC Tools.

Verifies environment setup and common configuration issues.
Provides centralized environment checks used by fetch and other commands.
"""

import os
import socket
import subprocess
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

from . import config as cfg


@dataclass
class EnvironmentStatus:
    """Results of environment checks for fetch operations.
    
    This class consolidates all pre-flight checks that fetch needs:
    - Repository status (git, dvc, dt)
    - Network connectivity
    - Remote accessibility
    
    Use `check_environment()` to populate this.
    """
    # Repository checks
    in_git_repo: bool = False
    in_dvc_repo: bool = False
    in_dt_repo: bool = False
    git_root: Optional[Path] = None
    dvc_root: Optional[Path] = None
    
    # Network checks  
    has_network: bool = False
    network_checked: bool = False
    
    # Remote access checks
    local_remote_name: Optional[str] = None
    local_remote_path: Optional[str] = None
    remote_accessible: bool = False
    remote_error: Optional[str] = None
    
    # Error messages for failures
    errors: List[str] = field(default_factory=list)
    
    def require_git_repo(self) -> None:
        """Raise error if not in a git repository."""
        if not self.in_git_repo:
            from .errors import FetchError
            raise FetchError(
                "Not in a git repository. This command requires a git repository.\n"
                "Run 'git init' to initialize one, or cd to an existing repo."
            )
    
    def require_dvc_repo(self) -> None:
        """Raise error if not in a DVC repository."""
        if not self.in_dvc_repo:
            from .errors import FetchError
            raise FetchError(
                "Not in a DVC repository. This command requires DVC to be initialized.\n"
                "Run 'dvc init' to initialize DVC in this repository."
            )
    
    def require_network(self) -> None:
        """Raise error if network is not available."""
        if not self.has_network:
            from .errors import FetchError
            raise FetchError(
                "No network connectivity. This operation requires network access.\n"
                "Check your network connection or try again later."
            )


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


# =============================================================================
# Environment checks (used by fetch and other commands)
# =============================================================================

def check_in_git_repo() -> Tuple[bool, Optional[Path]]:
    """Check if we're inside a git repository.
    
    Returns:
        Tuple of (is_in_repo, git_root_path)
    """
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return True, Path(result.stdout.strip())
        return False, None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False, None


def check_in_dvc_repo() -> Tuple[bool, Optional[Path]]:
    """Check if we're inside a DVC repository.
    
    Returns:
        Tuple of (is_in_repo, dvc_root_path)
    """
    # Check for .dvc directory
    cwd = Path.cwd()
    for parent in [cwd] + list(cwd.parents):
        if (parent / '.dvc').is_dir():
            return True, parent
    return False, None


def check_in_dt_repo() -> bool:
    """Check if this appears to be a dt-initialized repository.
    
    A dt repo has .dvc and typically has dt-specific config.
    """
    in_dvc, _ = check_in_dvc_repo()
    if not in_dvc:
        return False
    
    # Check for dt-specific markers (e.g., dt config file or cache.root set)
    # For now, having DVC is sufficient
    return True


def check_network_connectivity(timeout: float = 1.0) -> bool:
    """Check if network/internet access is available.
    
    Attempts to connect to common reliable hosts to detect network connectivity.
    Uses aggressive timeouts to fail fast on isolated nodes.
    
    Args:
        timeout: Connection timeout in seconds per host (default 1s).
        
    Returns:
        True if network is accessible, False otherwise.
    """
    # Try a few reliable hosts - use IP addresses to avoid DNS delays
    test_hosts = [
        ("8.8.8.8", 53),      # Google DNS (fastest to check)
        ("1.1.1.1", 53),      # Cloudflare DNS
    ]
    
    for host, port in test_hosts:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return True
        except (socket.error, socket.timeout, OSError):
            continue
    
    return False


def check_local_remote_access() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Check if a local remote is accessible.
    
    Returns:
        Tuple of (remote_name, remote_path, error_message)
        - If accessible: (name, path, None)
        - If not accessible: (None, None, error_message)
    """
    from . import remote as remote_mod
    
    try:
        remotes = remote_mod.list_remotes()
        if not remotes:
            return None, None, "No DVC remotes configured"
        
        result, error = remote_mod.check_remote_access(remotes)
        if result:
            return result[0], result[1], None
        elif error:
            return None, None, error
        else:
            return None, None, "No locally-accessible remote found"
    except Exception as e:
        return None, None, f"Error checking remotes: {e}"


def check_environment(
    check_network: bool = False,
    check_remote: bool = False,
    network_timeout: float = 1.0,
) -> EnvironmentStatus:
    """Run environment checks and return status.
    
    This is the main entry point for pre-flight checks. It efficiently
    checks the environment and caches results in an EnvironmentStatus object.
    
    Args:
        check_network: If True, check network connectivity (may be slow).
        check_remote: If True, check local remote accessibility.
        network_timeout: Timeout for network check in seconds.
        
    Returns:
        EnvironmentStatus with all check results.
    """
    status = EnvironmentStatus()
    
    # Git repo check
    status.in_git_repo, status.git_root = check_in_git_repo()
    if not status.in_git_repo:
        status.errors.append("Not in a git repository")
    
    # DVC repo check
    status.in_dvc_repo, status.dvc_root = check_in_dvc_repo()
    if not status.in_dvc_repo:
        status.errors.append("Not in a DVC repository")
    
    # DT repo check
    status.in_dt_repo = check_in_dt_repo()
    
    # Network check (optional, can be slow)
    if check_network:
        status.has_network = check_network_connectivity(timeout=network_timeout)
        status.network_checked = True
    
    # Remote access check (optional)
    if check_remote and status.in_dvc_repo:
        name, path, error = check_local_remote_access()
        if name and path:
            status.local_remote_name = name
            status.local_remote_path = path
            status.remote_accessible = True
        else:
            status.remote_error = error
    
    return status


def check_git_repo() -> DiagnosticResult:
    """Check if we're in a git repository (for dt doctor output)."""
    in_repo, root = check_in_git_repo()
    if in_repo:
        return DiagnosticResult("git_repo", True, f"In git repository ({root})")
    else:
        return DiagnosticResult(
            "git_repo", False, "Not in a git repository",
            "Run 'git init' or cd to a git repository"
        )


def check_dvc_repo() -> DiagnosticResult:
    """Check if we're in a DVC repository (for dt doctor output)."""
    in_repo, root = check_in_dvc_repo()
    if in_repo:
        return DiagnosticResult("dvc_repo", True, f"In DVC repository ({root})")
    else:
        return DiagnosticResult(
            "dvc_repo", False, "Not in a DVC repository",
            "Run 'dvc init' to initialize DVC"
        )


def check_network() -> DiagnosticResult:
    """Check network connectivity (for dt doctor output)."""
    has_network = check_network_connectivity(timeout=2.0)
    if has_network:
        return DiagnosticResult("network", True, "Network connectivity available")
    else:
        return DiagnosticResult(
            "network", False, "No network connectivity",
            "Check your network connection"
        )


def check_local_remote() -> DiagnosticResult:
    """Check local remote accessibility (for dt doctor output)."""
    name, path, error = check_local_remote_access()
    if name and path:
        return DiagnosticResult(
            "local_remote", True, 
            f"Local remote accessible: '{name}' at {path}"
        )
    elif error:
        return DiagnosticResult("local_remote", False, error)
    else:
        return DiagnosticResult(
            "local_remote", False,
            "No locally-accessible remote configured",
            "Configure a DVC remote with 'dvc remote add'"
        )


def check_auth_access() -> DiagnosticResult:
    """Run auth endpoint checks and summarise as a single diagnostic."""
    try:
        from .auth.checks import _try_check, STATUS_FAIL, STATUS_WARN
        from .auth.endpoints import discover_endpoints
    except ImportError:
        return DiagnosticResult(
            "auth_access", False,
            "Auth module not available",
            "Reinstall dvc-tools with 'pip install -e .'"
        )

    try:
        print("  Discovering endpoints...", flush=True)
        endpoints = discover_endpoints(verbose=False)
        print(f"  Found {len(endpoints)} endpoint(s), running access checks...",
              flush=True)
        results = []
        for i, ep in enumerate(endpoints, 1):
            print(f"  [{i}/{len(endpoints)}] {ep.type}: {ep.url}", flush=True)
            result = _try_check(ep)
            results.append(result)
            for child in ep.children:
                print(f"    └ {child.type}: {child.url}", flush=True)
                results.append(_try_check(child))
    except Exception as e:
        return DiagnosticResult(
            "auth_access", False,
            f"Auth check error: {e}",
        )

    if not results:
        return DiagnosticResult(
            "auth_access", True,
            "Auth access: no endpoints discovered",
        )

    fails = sum(1 for r in results if r.status == STATUS_FAIL)
    warns = sum(1 for r in results if r.status == STATUS_WARN)
    total = len(results)

    if fails:
        return DiagnosticResult(
            "auth_access", False,
            f"Auth access: {fails}/{total} endpoint(s) failed",
            "Run 'dt auth check -v' for details",
        )
    if warns:
        return DiagnosticResult(
            "auth_access", True,
            f"Auth access: {total} endpoint(s) OK, {warns} warning(s)",
        )
    return DiagnosticResult(
        "auth_access", True,
        f"Auth access: all {total} endpoint(s) accessible",
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
    
    # Repository context checks
    results.append(check_git_repo())
    results.append(check_dvc_repo())
    
    # Environment checks (may be slow)
    if verbose:
        results.append(check_network())
        results.append(check_local_remote())
        results.append(check_auth_access())
    
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
