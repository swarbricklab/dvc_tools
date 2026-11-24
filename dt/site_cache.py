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
path is ``{root}/{user}/{name}`` where ``name`` defaults to the project
name. The per-user level keeps each user's state out of reach of others
sharing the root.
"""

import os
import pwd
import stat
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


# Opt-in for a workspace deliberately shared by several users: the per-repo
# dir is then group-writable (never other-writable).
CONFIG_KEY_GROUP_SHARED = 'site_cache.group_shared'

# DVC trusts (and may deserialize) what it reads from site_cache_dir, so the
# per-repo dir is private to its owner unless group sharing is opted into.
PRIVATE_DIR_MODE = 0o700
GROUP_DIR_MODE = 0o2770
# The per-user level under a shared root: never writable by anyone else.
PRIVATE_USER_DIR_MODE = 0o700
GROUP_USER_DIR_MODE = 0o2750


def _current_user() -> str:
    try:
        return pwd.getpwuid(os.geteuid()).pw_name
    except KeyError:
        raise SiteCacheError(f"Cannot determine user name for uid {os.geteuid()}")


# Keys that decide who can write the state dir. They are honoured only from
# user/system config (or the CLI): project config is tracked in git and local
# config lives in the workspace, so a collaborator could set them there.
_TRUSTED_SCOPES = ('user', 'system')


def _trusted_config(key: str, default=None):
    for scope in _TRUSTED_SCOPES:
        node = cfg.load_scope_config(scope)
        for part in key.split('.'):
            node = node.get(part) if isinstance(node, dict) else None
        if node is not None:
            return node
    if cfg.get_value(key) is not None:
        print(f"note: ignoring {key} from project/local dt config; "
              f"set it with 'dt config set {key} ...' (user scope) instead")
    return default


def _group_shared() -> bool:
    value = _trusted_config(CONFIG_KEY_GROUP_SHARED, default=False)
    if isinstance(value, str):
        return value.strip().lower() in ('true', '1', 'yes', 'on')
    return bool(value)


def _abs(path) -> Path:
    """Absolute, normalised path WITHOUT following symlinks."""
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def _untrusted_write_bits(shared: bool) -> int:
    return stat.S_IWOTH | (0 if shared else stat.S_IWGRP)


def _strip_acls(path: Path) -> None:
    """Best-effort removal of (default) ACLs that would re-widen access."""
    try:
        subprocess.run(['setfacl', '-b', '--', str(path)],
                       capture_output=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass


def _configured_root(root: Optional[str]) -> Optional[Path]:
    value = root or _trusted_config(CONFIG_KEY_ROOT)
    return _abs(value) if value else None


def _check_symlinks(path: Path, trusted_uids: set) -> None:
    """Refuse a path containing a symlink planted by an untrusted user."""
    for comp in (path, *path.parents):
        try:
            st = comp.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(st.st_mode) and st.st_uid not in trusted_uids | {0}:
            raise SiteCacheError(
                f"Refusing site_cache_dir {path}: {comp} is a symlink owned by "
                f"another user (uid {st.st_uid})."
            )


def _check_ancestors(
    path: Path, shared: bool, trusted_uids: set, start: Path, verbose: bool = True,
) -> None:
    """Refuse ``path`` if an untrusted user could rename/replace it or a parent.

    Anyone who can write a directory without the sticky bit — or who owns
    it — can rename the entries inside it and put their own in place. Walk up
    the real (symlink-resolved) chain from ``start`` and require each
    directory to be owned by a trusted uid and not writable by untrusted users
    unless sticky. The walk stops at the first root-owned directory:
    system/site-managed directories are outside dt's control, so if one of
    those is itself open to renames we can only warn.
    """
    bad = _untrusted_write_bits(shared)
    real = Path(os.path.realpath(start))
    for parent in (real, *real.parents):
        try:
            pst = parent.stat()
        except OSError as e:
            raise SiteCacheError(f"Cannot inspect {parent}: {e}")
        if pst.st_uid == 0:
            if (verbose and (pst.st_mode & (stat.S_IWGRP | stat.S_IWOTH))
                    and not (pst.st_mode & stat.S_ISVTX)):
                print(f"note: {parent} (system-managed) lets other users rename "
                      f"entries in it, so dt cannot fully protect {path}. "
                      f"Ask the system administrators to set its sticky bit.")
            return
        if pst.st_uid not in trusted_uids:
            raise SiteCacheError(
                f"Refusing site_cache_dir {path}: parent {parent} is owned by another "
                f"user (uid {pst.st_uid}), who could replace its contents."
            )
        if (pst.st_mode & bad) and not (pst.st_mode & stat.S_ISVTX):
            raise SiteCacheError(
                f"Refusing site_cache_dir {path}: {parent} is writable by other users "
                f"without the sticky bit, so they could replace its contents.\n"
                f"  Fix (as its owner): chmod +t {parent}"
            )


def _ensure_owned_dir(
    path: Path, mode: int, shared: bool, verbose: bool, check_contents: bool = False,
) -> None:
    """Create ``path`` with ``mode``, or require an existing one to be a real
    directory owned by the caller and set it to ``mode``.

    With ``check_contents`` (the state dir itself), an existing dir is only
    adopted if everything already in it is private to the caller — another
    user may have planted files while it was writable by them.
    """
    try:
        path.mkdir(mode=mode)
        created = True
    except FileExistsError:
        created = False
    if not created:
        st = path.lstat()
        if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.geteuid():
            raise SiteCacheError(
                f"Refusing {path}: it is not a directory owned by you. "
                f"Remove it or choose another path with --site-cache-path."
            )
        if check_contents and not shared and not _tree_is_private(path, check_top=False):
            raise SiteCacheError(
                f"Refusing {path}: it contains files that other users could have "
                f"written, so DVC should not trust them. It is a rebuildable cache: "
                f"remove it (rm -rf {path}) and re-run."
            )
        if verbose and st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            print(f"Tightening permissions on {path} (was {stat.filemode(st.st_mode)})")
    os.chmod(path, mode)
    if not shared:
        _strip_acls(path)


def _prepare_dir(
    site_cache_dir: Path,
    verbose: bool = True,
    root: Optional[str] = None,
) -> Path:
    """Create or validate the per-repo site_cache dir; return its real path.

    The dir (and, under a shared root, the ``{root}/{user}`` level) is
    created owned by the caller with a private mode — group-only if
    ``site_cache.group_shared`` is set. An existing dir is adopted only if the
    caller owns it. Symlinks planted by other users, and parents that other
    users could rename, are refused.

    Raises:
        SiteCacheError: If the dir is unsafe to use.
    """
    shared = _group_shared()
    mode = GROUP_DIR_MODE if shared else PRIVATE_DIR_MODE
    user_mode = GROUP_USER_DIR_MODE if shared else PRIVATE_USER_DIR_MODE

    trusted_uids = {os.geteuid()}
    root_path = _configured_root(root)
    if root_path is not None:
        root_path = Path(os.path.realpath(root_path))
        try:
            trusted_uids.add(root_path.stat().st_uid)
        except OSError:
            pass

    try:
        # Only trusted symlinks may appear in the literal path; after that it
        # is safe to work on the real path.
        _check_symlinks(_abs(site_cache_dir), trusted_uids)
        site_cache_dir = Path(os.path.realpath(_abs(site_cache_dir)))
        under_root = root_path is not None and root_path in site_cache_dir.parents
        if under_root:
            _check_shared_perms(root_path, verbose=verbose)

        # Refuse an unsafe parent chain before creating anything under it.
        # Under a configured root, start at the root: the caller's own dirs
        # below it are created/fixed next and re-checked at the end.
        if under_root and root_path.exists():
            start = root_path
        else:
            start = next(p for p in site_cache_dir.parents if p.exists())
        _check_ancestors(site_cache_dir, shared, trusted_uids, start, verbose=False)

        user_dir = site_cache_dir.parent
        if under_root and user_dir.parent == root_path and user_dir.name == _current_user():
            if not root_path.exists():
                # A root dt creates itself is private; a shared root has to be
                # set up deliberately (see docs/index.md).
                root_path.parent.mkdir(parents=True, exist_ok=True)
                root_path.mkdir(mode=0o700, exist_ok=True)
            _ensure_owned_dir(user_dir, user_mode, shared, verbose)
        else:
            user_dir.mkdir(parents=True, exist_ok=True)

        _ensure_owned_dir(site_cache_dir, mode, shared, verbose, check_contents=True)
        _check_ancestors(site_cache_dir, shared, trusted_uids,
                         site_cache_dir.parent, verbose=verbose)
    except OSError as e:
        raise SiteCacheError(f"Cannot set up site_cache_dir {site_cache_dir}: {e}")
    return site_cache_dir


def _tree_is_private(path: Path, check_top: bool = True) -> bool:
    """True if ``path`` and everything under it is owned by the caller and
    not writable by anyone else (so its contents can be trusted).

    ``check_top=False`` skips the top dir's own mode (the caller is about to
    tighten it) but still requires it to be a real, caller-owned directory.
    """
    uid = os.geteuid()
    open_bits = stat.S_IWGRP | stat.S_IWOTH
    try:
        st = path.lstat()
        if not stat.S_ISDIR(st.st_mode) or st.st_uid != uid:
            return False
        if check_top and (st.st_mode & open_bits or _has_default_acl(path)):
            return False
        for dirpath, dirnames, filenames in os.walk(path):
            for name in dirnames + filenames:
                st = os.lstat(os.path.join(dirpath, name))
                if st.st_uid != uid or st.st_mode & open_bits:
                    return False
    except OSError:
        return False
    return True


def describe_risk(path: Path) -> Optional[str]:
    """Return a warning if a configured site_cache_dir is not private."""
    try:
        st = Path(path).stat()
    except OSError:
        return None
    if st.st_uid != os.geteuid():
        return f"{path} is owned by another user (uid {st.st_uid})"
    if st.st_mode & _untrusted_write_bits(_group_shared()):
        return f"{path} is writable by other users ({stat.filemode(st.st_mode)})"
    return None


def _has_default_acl(path: Path) -> Optional[bool]:
    """Return True if ``path`` has a default ACL granting group rwx.

    Returns ``None`` if ``getfacl`` is unavailable or unreadable
    (so callers can suppress the warning rather than nag spuriously).
    """
    try:
        result = subprocess.run(
            ['getfacl', '--absolute-names', '--omit-header', '--', str(path)],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith('default:group::') and 'w' in line.split(':', 2)[2]:
            return True
    return False


def _check_shared_perms(parent: Path, verbose: bool = True) -> None:
    """Warn if ``parent`` (the site_cache root) looks unsuitable for shared use.

    A shared root must let each user create their own per-repo subdir
    (group-writable) without letting them rename or replace anyone else's
    (sticky bit). A default ACL granting group write is flagged because it
    would make new per-repo state writable by the whole group. All findings
    are warnings; nothing is fatal.
    """
    if not verbose or not parent.exists():
        return
    try:
        st = parent.stat()
    except OSError:
        return

    warnings = []
    if (st.st_mode & (stat.S_IWGRP | stat.S_IWOTH)) and not (st.st_mode & stat.S_ISVTX):
        warnings.append("writable by others without the sticky bit (users can replace each other's dirs)")
    if _has_default_acl(parent):
        warnings.append("default ACL grants group write (new per-repo state would be group-writable)")

    if not warnings:
        return

    print(f"warning: {parent} may not be suitable as a shared site_cache root:")
    for w in warnings:
        print(f"  - {w}")
    print("  Recommended setup (as the directory owner):")
    print(f"    chgrp <project-group> {parent}")
    print(f"    chmod 3775 {parent}        # setgid + sticky")
    print(f"    setfacl -k {parent}        # drop any default ACL")



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
      2. ``{site_cache_root}/{user}/{name}`` — root from arg or
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
        return _abs(site_cache_path)

    root = site_cache_root or _trusted_config(CONFIG_KEY_ROOT)
    if not root:
        raise SiteCacheError(
            "site_cache root not configured.\n"
            "Either specify --site-cache-root, pass --site-cache-path, "
            "or set the root once:\n"
            f"  dt config set {CONFIG_KEY_ROOT} /scratch/<project>/dvc/site"
        )

    project_name = name or utils.get_project_name()
    if not project_name or project_name in ('.', '..') or '/' in project_name:
        raise SiteCacheError(f"Invalid project name for site_cache_dir: {project_name!r}")
    return _abs(root) / _current_user() / project_name


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
    site_cache_root: Optional[str] = None,
) -> None:
    """Write ``core.site_cache_dir`` to ``.dvc/config.local``.

    Creates the directory if needed (private to the caller, or to the group
    when ``site_cache.group_shared`` is set) and refuses an existing directory
    that the caller cannot trust. The setting is written with ``--local`` so it is
    workspace-specific (and gitignored by DVC).

    Args:
        repo_path: DVC repo root.
        site_cache_dir: Target directory for DVC's site cache.
        verbose: Print progress messages.

    Raises:
        SiteCacheError: If the directory is unsafe or ``dvc config`` fails.
    """
    site_cache_dir = _prepare_dir(site_cache_dir, verbose=verbose, root=site_cache_root)

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
    if not site_cache_path and not (site_cache_root or _trusted_config(CONFIG_KEY_ROOT)):
        if verbose:
            print(
                "Skipping site_cache_dir setup "
                f"({CONFIG_KEY_ROOT} not configured; using DVC default)"
            )
        return None

    repo_path = repo_path or Path.cwd()
    target = resolve_path(name, site_cache_root, site_cache_path)
    apply_to_repo(repo_path, target, verbose=verbose, site_cache_root=site_cache_root)
    return target


def migrate(
    target: Path,
    repo_path: Optional[Path] = None,
    verbose: bool = True,
    site_cache_root: Optional[str] = None,
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

    # Validate/create the target before copying anything into it.
    target = _prepare_dir(target, verbose=verbose, root=site_cache_root)

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
    elif not _tree_is_private(current):
        # Contents another user could have written can't be trusted; the
        # index is a rebuildable cache, so start empty instead.
        if verbose:
            print(f"Not copying {current}: it is not private to you, so its "
                  f"contents cannot be trusted. DVC will rebuild the index.")
    else:
        if verbose:
            print(f"Copying {current} -> {target}")
        try:
            shutil.copytree(current, target, symlinks=True, dirs_exist_ok=True)
        except OSError as e:
            raise SiteCacheError(f"Failed to copy site_cache_dir: {e}")
        finally:
            # copytree copies the source dir's mode onto the target.
            os.chmod(target, GROUP_DIR_MODE if _group_shared() else PRIVATE_DIR_MODE)

    # Re-validates (and re-tightens: copytree copies the source dir's mode).
    apply_to_repo(repo_path, target, verbose=verbose, site_cache_root=site_cache_root)
    return target
