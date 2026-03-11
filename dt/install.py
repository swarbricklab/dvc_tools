"""Install and manage git hooks for DVC Tools.

Provides:
- install/uninstall: Write or remove git hook files and merge driver.
- hook_run: Config-driven check runner invoked by git hooks.
- hook_list: Display configured checks per hook.
- check_large_files: Built-in check rejecting oversized staged files.

Hooks are thin dispatchers that call ``dt hook run <hook-name>``.
All behaviour is controlled via ``dt config hooks.*`` keys, following
the standard local > project > user > system precedence.
"""

import json
import os
import re
import stat
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import config as cfg
from . import hpc
from . import utils
from .errors import HookError, InstallError


# =============================================================================
# Constants
# =============================================================================

HOOK_NAMES = ['pre-commit', 'post-checkout', 'pre-push']

HOOK_TEMPLATE = """\
#!/bin/sh
exec dt hook run {hook_name} "$@"
"""

MERGE_DRIVER_NAME = 'dvc'
MERGE_DRIVER_CMD = 'dvc git-hook merge-driver --ancestor %O --our %A --their %B'

# Default check configuration written by ``dt install``
DEFAULT_HOOKS_CONFIG = {
    'hooks': {
        'pre-commit': {
            'checks': {
                'dvc-status': {
                    'enabled': True,
                    'mode': 'sync',
                },
                'large-files': {
                    'enabled': True,
                    'mode': 'sync',
                    'max_size': '1MB',
                },
            },
        },
        'post-checkout': {
            'checks': {
                'dvc-checkout': {
                    'enabled': True,
                    'mode': 'sync',
                },
                'index-sync': {
                    'enabled': True,
                    'mode': 'sync',
                },
            },
        },
        'pre-push': {
            'checks': {
                'dvc-push': {
                    'enabled': True,
                    'mode': 'sync',
                },
            },
        },
    },
}

# Size multipliers for parse_size
_SIZE_UNITS = {
    'B': 1,
    'KB': 1024,
    'MB': 1024 ** 2,
    'GB': 1024 ** 3,
    'TB': 1024 ** 4,
}

# Directory name under .dt/ for async hook results
HOOK_RESULTS_DIR_NAME = 'hook-results'


# =============================================================================
# Size parsing
# =============================================================================

def parse_size(size_str: str) -> int:
    """Parse a human-readable size string into bytes.

    Accepts formats like ``50MB``, ``1.5GB``, ``1024``, ``500 KB``.

    Args:
        size_str: Human-readable size string.

    Returns:
        Size in bytes.

    Raises:
        ValueError: If the string cannot be parsed.
    """
    size_str = size_str.strip().upper()
    match = re.match(r'^(\d+(?:\.\d+)?)\s*([A-Z]{1,2})?$', size_str)
    if not match:
        raise ValueError(f"Cannot parse size: {size_str!r}")
    number = float(match.group(1))
    unit = match.group(2) or 'B'
    if unit not in _SIZE_UNITS:
        raise ValueError(f"Unknown size unit: {unit!r}")
    return int(number * _SIZE_UNITS[unit])


def format_size(size_bytes: int) -> str:
    """Format bytes into a human-readable string."""
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(size_bytes) < 1024:
            if unit == 'B':
                return f"{size_bytes}{unit}"
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}PB"


# =============================================================================
# Git helpers
# =============================================================================

def _git_hooks_dir() -> Path:
    """Return the hooks directory for the current git repo."""
    result = subprocess.run(
        ['git', 'rev-parse', '--git-dir'],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise InstallError("Not in a git repository")
    return Path(result.stdout.strip()) / 'hooks'


def _is_dt_hook(path: Path) -> bool:
    """Return True if the hook file was installed by dt."""
    if not path.is_file():
        return False
    content = path.read_text()
    return 'dt hook run' in content


# =============================================================================
# Install / Uninstall
# =============================================================================

def install(force: bool = False, verbose: bool = False) -> List[str]:
    """Install git hooks and DVC merge driver.

    Writes thin hook scripts that delegate to ``dt hook run <name>``
    and registers the DVC merge driver in git config.

    Also writes default hook check configuration to project scope if
    no ``hooks`` key is already configured.

    Args:
        force: Overwrite existing hooks even if they were not written by dt.
        verbose: Print progress.

    Returns:
        List of installed hook names.

    Raises:
        InstallError: If hooks already exist and *force* is False.
    """
    utils.check_git()

    hooks_dir = _git_hooks_dir()
    hooks_dir.mkdir(parents=True, exist_ok=True)

    installed = []
    for hook_name in HOOK_NAMES:
        hook_path = hooks_dir / hook_name
        if hook_path.exists() and not force:
            if not _is_dt_hook(hook_path):
                raise InstallError(
                    f"Hook {hook_name} already exists and was not installed by dt.\n"
                    f"Use --force to overwrite, or remove it manually:\n"
                    f"  rm {hook_path}"
                )
        content = HOOK_TEMPLATE.format(hook_name=hook_name)
        hook_path.write_text(content)
        hook_path.chmod(hook_path.stat().st_mode | stat.S_IEXEC)
        installed.append(hook_name)
        if verbose:
            print(f"  Installed {hook_name} → {hook_path}")

    # Install DVC merge driver
    subprocess.run(
        ['git', 'config', 'merge.dvc.name', 'DVC merge driver'],
        capture_output=True,
    )
    subprocess.run(
        ['git', 'config', 'merge.dvc.driver', MERGE_DRIVER_CMD],
        capture_output=True,
    )
    if verbose:
        print(f"  Installed merge driver: {MERGE_DRIVER_NAME}")

    # Write default config if no hooks config exists yet
    existing = cfg.get_value('hooks')
    if existing is None:
        _write_default_config(verbose=verbose)

    return installed


def _write_default_config(verbose: bool = False) -> None:
    """Write default hooks config to local scope."""
    import yaml

    paths = cfg.get_config_paths()
    local_path = paths['local']

    if local_path.exists():
        with open(local_path) as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}

    data.setdefault('hooks', DEFAULT_HOOKS_CONFIG['hooks'])

    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, 'w') as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)

    if verbose:
        print("  Wrote default hooks config to local scope")


def uninstall(verbose: bool = False) -> List[str]:
    """Remove git hooks installed by dt.

    Only removes hooks that contain the ``dt hook run`` marker.
    Also removes the DVC merge driver configuration.

    Args:
        verbose: Print progress.

    Returns:
        List of removed hook names.
    """
    utils.check_git()

    hooks_dir = _git_hooks_dir()
    removed = []
    for hook_name in HOOK_NAMES:
        hook_path = hooks_dir / hook_name
        if _is_dt_hook(hook_path):
            hook_path.unlink()
            removed.append(hook_name)
            if verbose:
                print(f"  Removed {hook_name}")
        elif hook_path.exists() and verbose:
            print(f"  Skipped {hook_name} (not installed by dt)")

    # Remove merge driver config
    subprocess.run(
        ['git', 'config', '--remove-section', 'merge.dvc'],
        capture_output=True,
    )
    if verbose:
        print(f"  Removed merge driver config")

    return removed


# =============================================================================
# Check resolution
# =============================================================================

def _get_checks(hook_name: str) -> List[Dict]:
    """Load enabled checks for a hook from config.

    Returns:
        List of dicts with keys: name, mode, command, max_size, enabled, source.
    """
    checks_cfg = cfg.get_value(f'hooks.{hook_name}.checks')
    if not checks_cfg or not isinstance(checks_cfg, dict):
        return []

    checks = []
    for name, settings in checks_cfg.items():
        if not isinstance(settings, dict):
            continue
        entry = {
            'name': name,
            'enabled': settings.get('enabled', True),
            'mode': settings.get('mode', 'sync'),
            'command': settings.get('command'),
            'max_size': settings.get('max_size', '1MB'),
        }
        checks.append(entry)
    return checks


def _get_checks_with_sources(hook_name: str) -> List[Dict]:
    """Load checks for a hook, annotated with the config scope they come from."""
    checks = []
    for scope in reversed(cfg.SCOPES):
        scope_data = cfg.load_scope_config(scope)
        scope_checks = (
            scope_data
            .get('hooks', {})
            .get(hook_name, {})
            .get('checks', {})
        )
        if not isinstance(scope_checks, dict):
            continue
        for name, settings in scope_checks.items():
            if not isinstance(settings, dict):
                continue
            # Higher-precedence scope wins (processed later in loop)
            existing = next((c for c in checks if c['name'] == name), None)
            entry = {
                'name': name,
                'enabled': settings.get('enabled', True),
                'mode': settings.get('mode', 'sync'),
                'command': settings.get('command'),
                'max_size': settings.get('max_size', '1MB'),
                'source': scope,
            }
            if existing:
                # Replace with higher-precedence scope
                idx = checks.index(existing)
                checks[idx] = entry
            else:
                checks.append(entry)
    return checks


# =============================================================================
# Built-in checks
# =============================================================================

def check_large_files(max_size_str: str = '1MB', verbose: bool = False) -> bool:
    """Reject staged files that exceed *max_size*.

    Only inspects files in the git staging area (``git diff --cached``).

    Args:
        max_size_str: Maximum allowed file size as a human-readable string.
        verbose: Print progress.

    Returns:
        True if all staged files are within the limit, False otherwise.

    Raises:
        HookError: If any staged file exceeds the limit.
    """
    max_bytes = parse_size(max_size_str)

    result = subprocess.run(
        ['git', 'diff', '--cached', '--diff-filter=d', '--name-only', '-z'],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise HookError(f"git diff failed: {result.stderr.strip()}")

    # -z produces NUL-separated output
    files = [f for f in result.stdout.split('\0') if f]
    oversized = []

    for filepath in files:
        path = Path(filepath)
        if not path.exists():
            continue
        # Skip .dvc files—they are expected to be committed
        if path.suffix == '.dvc' or path.name == '.gitignore':
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size > max_bytes:
            oversized.append((filepath, size))

    if oversized:
        lines = [f"Files exceed {max_size_str} limit:"]
        for filepath, size in oversized:
            lines.append(f"  {filepath} ({format_size(size)})")
        lines.append("")
        lines.append("Track large files with DVC instead:  dt add <file>")
        lines.append("Adjust the limit:  dt config set hooks.pre-commit.checks.large-files.max_size 10MB")
        lines.append("Skip this check once:  git commit --no-verify")
        raise HookError('\n'.join(lines))

    if verbose:
        print(f"  large-files: {len(files)} staged file(s) OK (limit {max_size_str})")
    return True


def _run_builtin_check(name: str, hook_name: str, check_cfg: Dict,
                       hook_args: List[str], verbose: bool = False) -> bool:
    """Dispatch a built-in check by name.

    Returns True on success. Raises HookError on failure.
    """
    if name == 'dvc-status':
        from . import status as status_mod
        status_mod.status(verbose=verbose)
        return True

    if name == 'large-files':
        max_size = check_cfg.get('max_size', '1MB')
        return check_large_files(max_size_str=str(max_size), verbose=verbose)

    if name == 'dvc-checkout':
        return _run_dvc_checkout(hook_args, verbose=verbose)

    if name == 'index-sync':
        return _run_index_sync(verbose=verbose)

    if name == 'dvc-push':
        from . import push as push_mod
        push_mod.push(verbose=verbose)
        return True

    # Unknown built-in — treat as external if it has a command
    return False


def _run_dvc_checkout(hook_args: List[str], verbose: bool = False) -> bool:
    """Run DVC checkout for post-checkout hook.

    Skips during rebase/merge.  Only runs on branch switch (flag == 1).
    """
    # post-checkout args: <prev-HEAD> <new-HEAD> <flag>
    # flag=1 means branch checkout, flag=0 means file checkout
    if len(hook_args) >= 3:
        flag = hook_args[2]
        if flag == '0':
            if verbose:
                print("  dvc-checkout: file checkout, skipping")
            return True

    # Skip during rebase
    git_dir_result = subprocess.run(
        ['git', 'rev-parse', '--git-dir'],
        capture_output=True, text=True,
    )
    if git_dir_result.returncode == 0:
        git_dir = Path(git_dir_result.stdout.strip())
        if (git_dir / 'rebase-merge').exists() or (git_dir / 'rebase-apply').exists():
            if verbose:
                print("  dvc-checkout: rebase in progress, skipping")
            return True

    from . import pull as pull_mod
    pull_mod.pull(verbose=verbose)
    return True


def _run_index_sync(verbose: bool = False) -> bool:
    """Pull then push the site cache index."""
    from . import index as index_mod

    if not index_mod.is_auto_sync_enabled():
        if verbose:
            print("  index-sync: not configured, skipping")
        return True

    try:
        index_mod.pull(quiet=not verbose, verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"  index-sync: pull warning: {e}")

    try:
        index_mod.push(quiet=not verbose, verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"  index-sync: push warning: {e}")

    return True


# =============================================================================
# Hook runner
# =============================================================================

def hook_run(hook_name: str, hook_args: Optional[List[str]] = None,
             verbose: bool = False) -> bool:
    """Run all enabled checks for *hook_name*.

    Sync checks run inline and can abort the git operation (non-zero exit).
    Async checks are deferred for Phase 2 (currently logged as skipped).

    Args:
        hook_name: One of the supported git hook names.
        hook_args: Additional arguments passed by git to the hook.
        verbose: Print progress.

    Returns:
        True if all sync checks passed.

    Raises:
        HookError: If any sync check fails.
    """
    if hook_args is None:
        hook_args = []

    checks = _get_checks(hook_name)
    enabled = [c for c in checks if c.get('enabled', True)]

    if not enabled:
        if verbose:
            print(f"{hook_name}: no checks configured")
        return True

    failures = []

    for check in enabled:
        name = check['name']
        mode = check.get('mode', 'sync')

        if mode == 'async':
            job_id = _dispatch_async_check(
                name, hook_name, check, hook_args, verbose=verbose,
            )
            if verbose:
                if job_id:
                    print(f"  {name}: async job submitted ({job_id})")
                else:
                    print(f"  {name}: async dispatch skipped")
            continue

        if verbose:
            print(f"  {name}: running...")

        try:
            command = check.get('command')
            if command:
                # External check — run as subprocess
                result = subprocess.run(
                    command, shell=True,
                    capture_output=True, text=True,
                )
                if result.returncode != 0:
                    output = (result.stdout + result.stderr).strip()
                    raise HookError(f"{name} failed:\n{output}")
            else:
                # Built-in check
                handled = _run_builtin_check(
                    name, hook_name, check, hook_args, verbose=verbose,
                )
                if not handled:
                    if verbose:
                        print(f"  {name}: unknown built-in, skipping")

            if verbose:
                print(f"  {name}: OK")

        except HookError as e:
            failures.append((name, str(e)))
            # Continue running remaining checks so user sees all failures

    if failures:
        lines = [f"{hook_name}: {len(failures)} check(s) failed"]
        for name, msg in failures:
            lines.append(f"\n--- {name} ---\n{msg}")
        raise HookError('\n'.join(lines))

    return True


# =============================================================================
# Hook list
# =============================================================================

def hook_list() -> Dict[str, List[Dict]]:
    """Return all configured checks per hook, annotated with source scope.

    Returns:
        Dict mapping hook name → list of check dicts.
    """
    result = {}
    for hook_name in HOOK_NAMES:
        result[hook_name] = _get_checks_with_sources(hook_name)
    return result


# =============================================================================
# Async dispatch
# =============================================================================

def _get_hook_results_dir() -> Path:
    """Return the .dt/hook-results directory, creating it if needed."""
    root = utils.find_git_root() or Path.cwd()
    results_dir = root / '.dt' / HOOK_RESULTS_DIR_NAME
    results_dir.mkdir(parents=True, exist_ok=True)
    return results_dir


def _save_hook_result(
    check_name: str,
    hook_name: str,
    passed: bool,
    output: str,
) -> Path:
    """Save a hook check result to .dt/hook-results/.

    Returns:
        Path to the result file.
    """
    results_dir = _get_hook_results_dir()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{timestamp}-{hook_name}-{check_name}.json"
    result_path = results_dir / filename

    result_data = {
        'check': check_name,
        'hook': hook_name,
        'passed': passed,
        'timestamp': datetime.now().isoformat(),
        'output': output,
    }

    with open(result_path, 'w') as f:
        json.dump(result_data, f, indent=2)

    return result_path


def _dispatch_async_check(
    name: str,
    hook_name: str,
    check_cfg: Dict,
    hook_args: List[str],
    verbose: bool = False,
) -> Optional[str]:
    """Submit an async check via qxub.

    Builds a ``dt hook run-check <hook> <check> --worker`` command and
    submits it to a compute node.

    Args:
        name: Check name.
        hook_name: Git hook name.
        check_cfg: Check configuration dict.
        hook_args: Arguments passed by git to the hook.
        verbose: Print progress.

    Returns:
        Job ID string, or None if submission failed or qxub unavailable.
    """
    if not hpc.check_qxub():
        if verbose:
            print(f"  {name}: qxub not available, skipping async check")
        return None

    # Build worker command
    worker_cmd = ['dt', 'hook', 'run-check', hook_name, name, '--worker']
    if verbose:
        worker_cmd.append('--verbose')
    if hook_args:
        worker_cmd.append('--')
        worker_cmd.extend(hook_args)

    job_name = f'dt-hook-{hook_name}-{name}'
    cmd = hpc.build_qxub_command(job_name, worker_cmd)

    if verbose:
        print(f"  {name}: submitting → {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
        )
        if result.returncode == 0:
            job_id = result.stdout.strip().split('\n')[0]
            return job_id
        else:
            if verbose:
                print(f"  {name}: submission failed: {result.stderr.strip()}")
            return None
    except Exception as e:
        if verbose:
            print(f"  {name}: submission error: {e}")
        return None


def run_check(
    hook_name: str,
    check_name: str,
    hook_args: Optional[List[str]] = None,
    verbose: bool = False,
) -> bool:
    """Run a single named check and save the result.

    This is the worker-side function called on the compute node by
    ``dt hook run-check --worker``.  Runs the check and writes the
    result to ``.dt/hook-results/``.

    Args:
        hook_name: Git hook name.
        check_name: Name of the check to run.
        hook_args: Additional arguments passed by git.
        verbose: Print progress.

    Returns:
        True if the check passed.

    Raises:
        HookError: If the check configuration cannot be found.
    """
    if hook_args is None:
        hook_args = []

    # Find the check configuration
    checks = _get_checks(hook_name)
    check_cfg = next((c for c in checks if c['name'] == check_name), None)
    if check_cfg is None:
        raise HookError(f"Check {check_name!r} not found for hook {hook_name!r}")

    passed = True
    output = ''

    try:
        command = check_cfg.get('command')
        if command:
            # External check — run as subprocess
            result = subprocess.run(
                command, shell=True,
                capture_output=True, text=True,
            )
            output = (result.stdout + result.stderr).strip()
            if result.returncode != 0:
                passed = False
        else:
            # Built-in check — capture stdout
            import io
            import contextlib

            f = io.StringIO()
            with contextlib.redirect_stdout(f):
                _run_builtin_check(
                    check_name, hook_name, check_cfg, hook_args,
                    verbose=verbose,
                )
            output = f.getvalue().strip()

    except HookError as e:
        passed = False
        output = str(e)
    except Exception as e:
        passed = False
        output = f"Unexpected error: {e}"

    result_path = _save_hook_result(check_name, hook_name, passed, output)
    if verbose:
        status_str = 'PASS' if passed else 'FAIL'
        print(f"{check_name}: {status_str}")
        print(f"Result saved to {result_path}")

    return passed


# =============================================================================
# Hook results
# =============================================================================

def list_hook_results(limit: int = 20) -> List[Dict]:
    """List recent hook check results.

    Reads JSON result files from ``.dt/hook-results/``, most recent first.

    Args:
        limit: Maximum number of results to return.

    Returns:
        List of result dicts with keys: check, hook, passed, timestamp,
        output, file.
    """
    try:
        results_dir = _get_hook_results_dir()
    except Exception:
        return []

    result_files = sorted(results_dir.glob('*.json'), reverse=True)

    results = []
    for path in result_files[:limit]:
        try:
            with open(path) as f:
                data = json.load(f)
            data['file'] = str(path)
            results.append(data)
        except (json.JSONDecodeError, OSError):
            continue

    return results


def clear_hook_results(older_than_days: Optional[int] = None) -> int:
    """Remove hook result files.

    Args:
        older_than_days: Only remove results older than this many days.
            If None, remove all results.

    Returns:
        Number of files removed.
    """
    try:
        results_dir = _get_hook_results_dir()
    except Exception:
        return 0

    removed = 0
    now = datetime.now()

    for path in results_dir.glob('*.json'):
        if older_than_days is not None:
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                age_days = (now - mtime).days
                if age_days < older_than_days:
                    continue
            except OSError:
                continue
        try:
            path.unlink()
            removed += 1
        except OSError:
            continue

    return removed
