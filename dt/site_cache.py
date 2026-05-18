"""DVC site_cache_dir management for DVC Tools.

DVC stores per-repo state (object index, link tracking, file-state cache)
under ``core.site_cache_dir``. By default this lives under ``/var/tmp/dvc``
which is local to each compute node. For shared HPC workspaces it is
preferable to place ``site_cache_dir`` on a shared filesystem so that
multiple nodes mounting the same workspace can re-use a single index.

This module resolves a target ``site_cache_dir`` path from CLI arguments
or dt config (``site_cache.root``, ``site_cache.enabled``) and writes it
to ``.dvc/config.local`` via ``dvc config --local core.site_cache_dir``.

DVC itself creates the per-repo subdirectory hierarchy
(``repo/<sha256>/...``) inside the configured directory, so the resolved
path is simply ``{root}/{name}`` where ``name`` defaults to the project
name.
"""

import subprocess
from pathlib import Path
from typing import Optional

from . import config as cfg
from . import utils
from .errors import SiteCacheError


# Config key for the site_cache root. Combined with the project name to
# produce the per-repo directory written to ``core.site_cache_dir``.
CONFIG_KEY_ROOT = 'site_cache.root'

# Config key for whether dt should manage site_cache_dir at all.
# When set to ``False`` (in any scope) dt init / dt clone skip the step,
# leaving DVC's built-in default behaviour in place.
CONFIG_KEY_ENABLED = 'site_cache.enabled'


def is_enabled() -> bool:
    """Return True if dt should manage core.site_cache_dir.

    Defaults to True. Set ``site_cache.enabled: false`` in any dt config
    scope to opt out.
    """
    value = cfg.get_value(CONFIG_KEY_ENABLED, default=True)
    if isinstance(value, str):
        return value.strip().lower() not in ('false', '0', 'no', 'off')
    return bool(value)


def resolve_path(
    name: Optional[str] = None,
    site_cache_root: Optional[str] = None,
    site_cache_path: Optional[str] = None,
) -> Path:
    """Resolve the target site_cache_dir path.

    Resolution order:
      1. ``site_cache_path`` — complete path override.
      2. ``{site_cache_root}/{name}`` — root from arg or
         ``site_cache.root`` config; name defaults to project name.

    Args:
        name: Project name (defaults to current directory name).
        site_cache_root: Override the configured site_cache root.
        site_cache_path: Complete path override.

    Returns:
        Absolute path to use for ``core.site_cache_dir``.

    Raises:
        SiteCacheError: If no root can be determined.
    """
    if site_cache_path:
        return Path(site_cache_path).expanduser().resolve()

    root = site_cache_root or cfg.get_value(CONFIG_KEY_ROOT)
    if not root:
        raise SiteCacheError(
            "site_cache root not configured.\n"
            "Either specify --site-cache-root, pass --site-cache-path, "
            "or set the root once:\n"
            f"  dt config set {CONFIG_KEY_ROOT} /scratch/<project>/dvc/site"
        )

    project_name = name or utils.get_project_name()
    return (Path(root).expanduser() / project_name).resolve()


def get_current(repo_path: Optional[Path] = None) -> Optional[Path]:
    """Return the site_cache_dir currently configured for this repo.

    Reads ``core.site_cache_dir`` from any DVC config scope (local,
    project, global, system). Returns ``None`` if unset, in which case
    DVC's built-in default applies.

    Args:
        repo_path: DVC repo root (defaults to cwd).
    """
    repo_path = repo_path or Path.cwd()
    try:
        result = subprocess.run(
            ['dvc', 'config', 'core.site_cache_dir'],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        raise SiteCacheError("dvc command not found")

    if result.returncode != 0:
        # Unset keys exit non-zero with an empty stdout; treat as "not set".
        return None
    value = result.stdout.strip()
    return Path(value) if value else None


def apply_to_repo(
    repo_path: Path,
    site_cache_dir: Path,
    verbose: bool = True,
) -> None:
    """Write ``core.site_cache_dir`` to ``.dvc/config.local``.

    Creates the directory if needed. The setting is written with
    ``--local`` so it is workspace-specific (and gitignored by DVC).

    Args:
        repo_path: DVC repo root.
        site_cache_dir: Target directory for DVC's site cache.
        verbose: Print progress messages.

    Raises:
        SiteCacheError: If ``dvc config`` fails.
    """
    site_cache_dir = Path(site_cache_dir).expanduser().resolve()
    site_cache_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"Configuring DVC site_cache_dir: {site_cache_dir}")

    result = subprocess.run(
        ['dvc', 'config', '--local', 'core.site_cache_dir', str(site_cache_dir)],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SiteCacheError(
            f"Failed to set core.site_cache_dir: {result.stderr.strip() or result.stdout.strip()}"
        )


def init_site_cache(
    name: Optional[str] = None,
    site_cache_root: Optional[str] = None,
    site_cache_path: Optional[str] = None,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
) -> Optional[Path]:
    """Initialise ``core.site_cache_dir`` for the repo.

    If ``site_cache.enabled`` is False, returns ``None`` without making
    any changes. Otherwise resolves the target path, creates it, and
    writes it to ``.dvc/config.local``.

    Args:
        name: Project name (defaults to current directory name).
        site_cache_root: Override the configured root.
        site_cache_path: Complete path override.
        repo_path: DVC repo root (defaults to cwd).
        verbose: Print progress messages.

    Returns:
        Path that was configured, or ``None`` if skipped.

    Raises:
        SiteCacheError: If configuration fails.
    """
    if not is_enabled():
        if verbose:
            print("Skipping site_cache_dir setup (site_cache.enabled=false)")
        return None

    # If neither arg nor config provides a root, allow init to proceed
    # silently using DVC's default rather than aborting the whole
    # init/clone. This is the most common case for users who haven't
    # opted in yet.
    if not site_cache_path and not (site_cache_root or cfg.get_value(CONFIG_KEY_ROOT)):
        if verbose:
            print(
                "Skipping site_cache_dir setup "
                f"({CONFIG_KEY_ROOT} not configured; using DVC default)"
            )
        return None

    repo_path = repo_path or Path.cwd()
    target = resolve_path(name, site_cache_root, site_cache_path)
    apply_to_repo(repo_path, target, verbose=verbose)
    return target


def migrate(
    target: Path,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
) -> Path:
    """Copy the current site_cache_dir contents to ``target`` and switch DVC.

    Performs a plain ``shutil.copytree`` of the current site_cache_dir
    into ``target`` (merging with anything already there), then writes
    ``core.site_cache_dir = target`` to ``.dvc/config.local``. The old
    location is left in place — clean it up manually if desired.

    Args:
        target: New site_cache_dir.
        repo_path: DVC repo root (defaults to cwd).
        verbose: Print progress messages.

    Returns:
        The new site_cache_dir path.

    Raises:
        SiteCacheError: If the copy or DVC config update fails.
    """
    import shutil

    repo_path = repo_path or Path.cwd()
    target = Path(target).expanduser().resolve()

    current = get_current(repo_path)
    if current is None:
        if verbose:
            print(
                "No existing core.site_cache_dir configured; "
                "skipping copy and just setting the new value."
            )
    elif not current.exists():
        if verbose:
            print(f"Current site_cache_dir {current} does not exist; nothing to copy.")
    elif current.resolve() == target:
        if verbose:
            print(f"site_cache_dir already set to {target}; nothing to do.")
        return target
    else:
        if verbose:
            print(f"Copying {current} -> {target}")
        try:
            shutil.copytree(current, target, dirs_exist_ok=True)
        except OSError as e:
            raise SiteCacheError(f"Failed to copy site_cache_dir: {e}")

    apply_to_repo(repo_path, target, verbose=verbose)
    return target
