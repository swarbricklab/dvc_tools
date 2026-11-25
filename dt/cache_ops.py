"""Cache population operations for DVC Tools.

Shared utilities for copying/linking files into the DVC cache.
Uses the DVC-preferred order: reflink → hardlink → symlink → copy.
"""

import errno
import fcntl
import hashlib
import os
import secrets
import shutil
import stat
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple

from . import utils

LinkType = Literal['reflink', 'hardlink', 'symlink', 'copy', 'skipped', 'failed']
CacheType = Literal['reflink', 'hardlink', 'symlink', 'copy']

# Valid cache types (matches DVC's cache.type config)
VALID_CACHE_TYPES = ('reflink', 'hardlink', 'symlink', 'copy')


# Linux FICLONE ioctl: _IOW(0x94, 9, int). This is what `cp --reflink` uses.
_FICLONE = 0x40049409

# Reflink support is a property of the filesystem, so probe each one once.
# Lustre and NFS -- the filesystems this runs on in anger -- have no CoW
# support, and without this every single file would pay for a doomed attempt.
_reflink_supported: Dict[int, bool] = {}


def _try_reflink(source: Path, dest: Path) -> bool:
    """Try to create a reflink (copy-on-write clone).

    Issues FICLONE directly rather than shelling out to ``cp``. The old
    implementation passed ``--reflink=only``, which GNU coreutils rejects
    (it accepts only auto/always/never), so reflinking never once succeeded --
    it just forked a process per file to earn a usage error.
    """
    # cp would happily clobber an existing destination; link_file promises not
    # to. Also cheap insurance, since we create dest with O_EXCL below.
    if dest.exists():
        return False

    try:
        device = dest.parent.stat().st_dev
    except OSError:
        return False
    if _reflink_supported.get(device) is False:
        return False

    try:
        src_fd = os.open(source, os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            dest_fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except OSError:
            return False
        try:
            fcntl.ioctl(dest_fd, _FICLONE, src_fd)
        except OSError as e:
            os.close(dest_fd)
            # Best effort: we created it, so we clean it up.
            try:
                os.unlink(dest)
            except OSError:
                pass
            # EOPNOTSUPP/EXDEV mean the filesystem cannot do this, ever.
            if e.errno in (errno.EOPNOTSUPP, errno.ENOTTY, errno.EXDEV):
                _reflink_supported[device] = False
            return False
        os.close(dest_fd)
        _reflink_supported[device] = True
        return True
    finally:
        os.close(src_fd)


def _try_hardlink(source: Path, dest: Path) -> bool:
    """Try to create a hardlink."""
    try:
        os.link(source, dest)
        return True
    except OSError:
        return False


def _try_symlink(source: Path, dest: Path) -> bool:
    """Try to create a symlink."""
    try:
        os.symlink(source, dest)
        return True
    except OSError:
        return False


def _try_copy(source: Path, dest: Path) -> bool:
    """Try to copy the file into a newly created ``dest``.

    ``dest`` is created exclusively and without following links, so anything
    already at that name -- a file, or a link, dangling or not -- makes this
    fail rather than be written through or truncated. Everything after the
    create works on the open descriptor.
    """
    try:
        src_fd = os.open(source, os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            dest_fd = os.open(
                dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
        except OSError:
            return False
        ok = False
        try:
            with os.fdopen(src_fd, 'rb', closefd=False) as fsrc, \
                    os.fdopen(dest_fd, 'wb', closefd=False) as fdst:
                shutil.copyfileobj(fsrc, fdst, 1 << 20)
            st = os.fstat(src_fd)
            os.utime(dest_fd, ns=(st.st_atime_ns, st.st_mtime_ns))
            ok = True
        except OSError:
            pass
        finally:
            os.close(dest_fd)
            if not ok:
                try:
                    os.unlink(dest)
                except OSError:
                    pass
        return ok
    finally:
        os.close(src_fd)


def write_object(dest: Path, data: bytes) -> bool:
    """Create the cache object ``dest`` holding ``data``, read-only.

    The bytes go to a private temporary file (random name, exclusive, no
    links followed) that is then published with ``link(2)``, which never
    follows or replaces whatever is at ``dest``.

    Returns:
        True if created; False if a regular object already existed there.

    Raises:
        OSError: If something other than a regular file (e.g. a link,
            dangling or not) occupies ``dest``, or the write fails.
    """
    def existing() -> bool:
        st = os.lstat(dest)
        if stat.S_ISREG(st.st_mode):
            return True
        raise OSError(errno.EEXIST, f"{dest} exists and is not a regular file")

    try:
        if existing():
            return False
    except FileNotFoundError:
        pass

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{secrets.token_hex(8)}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view):]
            os.fchmod(fd, 0o444)
        finally:
            os.close(fd)
        try:
            os.link(tmp, dest)
        except FileExistsError:
            # A concurrent writer got there first (fine), or something was
            # planted (refused).
            existing()
            return False
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass
    return True


def _make_readonly(path: Path) -> None:
    """Make a file read-only (matching DVC cache protection).
    
    Sets permissions to 0o444 (read-only for user, group, and others).
    This matches DVC's cache file protection behavior. Only a regular file
    at ``path`` itself is changed; a link there is left alone.
    
    Args:
        path: Path to the file to protect.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return
    try:
        if stat.S_ISREG(os.fstat(fd).st_mode):
            os.fchmod(fd, 0o444)
    except OSError:
        # Best effort - don't fail if chmod fails (e.g., on some network filesystems)
        pass
    finally:
        os.close(fd)


def link_file(
    source: Path,
    dest: Path,
    verbose: bool = False,
    label: str = '',
    cache_type: Optional[CacheType] = None,
) -> Tuple[bool, LinkType]:
    """Copy or link a file from source to destination.
    
    By default, uses DVC's preferred order: reflink → hardlink → symlink → copy.
    If cache_type is specified, only that method is attempted.
    
    Creates parent directories as needed. Skips if destination already exists.
    
    Args:
        source: Source file path.
        dest: Destination file path.
        verbose: Print progress messages.
        label: Optional label for verbose output (e.g., hash or filename).
        cache_type: If specified, only use this link type (reflink, hardlink,
            symlink, or copy). Fails if the specified type doesn't work.
        
    Returns:
        Tuple of (success, link_type).
        success is True if file was linked/copied, False if skipped or failed.
        link_type indicates how the file was handled.
    """
    # Skip explicit dest.exists() check - it's slow on network filesystems.
    # Instead, just try the operation and catch FileExistsError.
    # This is faster on Lustre/NFS because the filesystem checks existence
    # as part of the link syscall anyway.
    
    if not source.exists():
        if verbose:
            print(f"  ERROR: Source not found: {source}")
        return False, 'failed'
    
    # Ensure parent directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    display = label or source.name
    
    # If specific cache_type requested, only try that one
    if cache_type:
        methods = {
            'reflink': (_try_reflink, 'reflink'),
            'hardlink': (_try_hardlink, 'hardlink'),
            'symlink': (_try_symlink, 'symlink'),
            'copy': (_try_copy, 'copy'),
        }
        
        try_func, type_name = methods[cache_type]
        if try_func(source, dest):
            # Make copied files read-only (reflink and copy create new files)
            if type_name in ('reflink', 'copy'):
                _make_readonly(dest)
            if verbose:
                print(f"  Cached ({type_name}): {display}")
            return True, type_name
        else:
            # Check if failure was because dest already exists
            if dest.exists():
                return False, 'skipped'
            if verbose:
                print(f"  ERROR: {type_name} failed for {display}")
            return False, 'failed'
    
    # Default: try all methods in order
    # 1. Try reflink (copy-on-write) - best option: instant, zero space, safe to modify
    if _try_reflink(source, dest):
        _make_readonly(dest)
        if verbose:
            print(f"  Cached (reflink): {display}")
        return True, 'reflink'
    
    # 2. Try hardlink - same inode, no extra space, works within same filesystem
    # Note: hardlink shares permissions with source, which should already be read-only
    if _try_hardlink(source, dest):
        if verbose:
            print(f"  Cached (hardlink): {display}")
        return True, 'hardlink'
    
    # 3. Try symlink - pointer to source, no extra space, works across filesystems
    # Note: symlink permissions are irrelevant on Unix; target permissions matter
    if _try_symlink(source, dest):
        if verbose:
            print(f"  Cached (symlink): {display}")
        return True, 'symlink'
    
    # 4. Fall back to regular copy - slower but universally compatible
    if _try_copy(source, dest):
        _make_readonly(dest)
        if verbose:
            print(f"  Cached (copy): {display}")
        return True, 'copy'
    
    # All methods failed. Check if it's because dest already exists.
    # This check happens only once per file (after failure) rather than
    # once per file upfront, which is much faster when most files are new.
    if dest.exists():
        return False, 'skipped'
    
    if verbose:
        print(f"  ERROR: All link methods failed for {display}")
    
    return False, 'failed'


def get_cache_file_path(
    md5: str,
    cache_root: Path,
    use_v3_layout: bool = True,
) -> Path:
    """Get the cache file path for a given MD5 hash.
    
    Args:
        md5: The MD5 hash (with optional .dir suffix).
        cache_root: Path to the cache root directory.
        use_v3_layout: If True, use v3 layout (files/md5/XX/hash).
            If False, use v2 layout (XX/hash).
            
    Returns:
        Path to the cache file.
    """
    prefix, filename = utils.oid_path_parts(md5)

    if use_v3_layout:
        return cache_root / 'files' / 'md5' / prefix / filename
    else:
        return cache_root / prefix / filename


def find_source_file(
    md5: str,
    source_cache: Path,
) -> Optional[Path]:
    """Find a file in a source cache, checking both v3 and v2 layouts.
    
    Args:
        md5: The MD5 hash (with optional .dir suffix).
        source_cache: Path to the source cache root.
        
    Returns:
        Path to the source file if found, None otherwise.
    """
    try:
        prefix, filename = utils.oid_path_parts(md5)
    except ValueError:
        return None

    # Try v3 path first
    v3_path = source_cache / 'files' / 'md5' / prefix / filename
    if v3_path.exists():
        return v3_path
    
    # Fall back to v2 path
    v2_path = source_cache / prefix / filename
    if v2_path.exists():
        return v2_path
    
    return None


# Upper bound on a .dir manifest read into memory.
MAX_DIR_MANIFEST_BYTES = 2 << 30


def read_dir_manifest(path: Path, md5: str) -> Optional[bytes]:
    """Bytes of the ``.dir`` object at ``path``, if they hash to ``md5``.

    A ``.dir`` object's name is the md5 of its bytes, so content that does
    not match is not that object and is not returned.
    """
    if not md5.endswith('.dir'):
        return None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOCTTY)
    except OSError:
        return None
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_size > MAX_DIR_MANIFEST_BYTES:
            return None
        chunks = []
        remaining = MAX_DIR_MANIFEST_BYTES + 1
        while remaining > 0:
            chunk = os.read(fd, min(1 << 20, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    except OSError:
        return None
    finally:
        os.close(fd)
    raw = b''.join(chunks)
    if hashlib.md5(raw).hexdigest() != md5[:-4]:
        return None
    return raw


def place_dir_object(dest: Path, md5: str, data: bytes) -> Optional[bool]:
    """Make ``dest`` this user's own copy of the checked ``.dir`` bytes.

    An existing object is kept only if it is a regular, unshared file
    holding the right bytes; otherwise (a link, a file shared with another
    path, other content) it is replaced by a fresh copy.

    Returns:
        True if written, False if a good copy was already there, None if
        it could not be written.
    """
    try:
        if write_object(dest, data):
            return True
    except OSError:
        pass
    try:
        st = os.lstat(dest)
        if (stat.S_ISREG(st.st_mode) and st.st_nlink == 1
                and read_dir_manifest(dest, md5) is not None):
            return False
    except OSError:
        pass
    tmp = dest.with_name(f".{dest.name}.{secrets.token_hex(8)}.tmp")
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view):]
            os.fchmod(fd, 0o444)
        finally:
            os.close(fd)
        os.rename(tmp, dest)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None
    return True


def find_dir_manifest(md5: str, source_cache: Path) -> Optional[bytes]:
    """Read the ``.dir`` object ``md5`` from ``source_cache`` (either layout).

    Only content matching ``md5`` is returned; see :func:`read_dir_manifest`.
    """
    try:
        prefix, filename = utils.oid_path_parts(md5)
    except ValueError:
        return None
    for path in (source_cache / 'files' / 'md5' / prefix / filename,
                 source_cache / prefix / filename):
        raw = read_dir_manifest(path, md5)
        if raw is not None:
            return raw
    return None


def object_size(md5: str, *cache_roots: Optional[str]) -> Optional[int]:
    """Size of a cache object, searched across roots and both layouts.

    The object *is* the file content, so its own size is authoritative -- no
    need to ask the source repo. Uses :func:`find_source_file` so a remote in
    the legacy v2 layout (or a half-migrated one holding some objects at
    ``<xx>/<rest>`` and others under ``files/md5/``) answers as readily as a v3
    one. That fallback is the whole point: sizing only the v3 path against a
    mixed remote misses silently, and a miss that is then treated as zero
    understates a multi-GB import (issue #182).

    Args:
        md5: The MD5 hash (with optional .dir suffix).
        *cache_roots: Cache/remote roots to search, in priority order.
            None/empty entries are skipped so callers can pass optional paths.

    Returns:
        Size in bytes, or None if the object was not found in any root.
    """
    for root in cache_roots:
        if not root:
            continue
        path = find_source_file(md5, Path(root))
        if path is None:
            continue
        try:
            return path.stat().st_size
        except OSError:
            # Vanished or unreadable between the lookup and the stat -- try
            # the next root rather than reporting a size we did not observe.
            continue
    return None


def populate_cache_file(
    md5: str,
    source_cache: str,
    dest_cache: str,
    verbose: bool = False,
    use_v3_layout: bool = True,
    cache_type: Optional[CacheType] = None,
) -> Optional[bool]:
    """Copy or link a single file from source cache to destination cache.
    
    Used for .dir files and single file imports where we need to ensure
    the file exists in the primary cache.
    
    Supports both DVC v3 cache layout (files/md5/XX/hash) and legacy 
    DVC v2 layout (XX/hash directly in remote root).
    
    Args:
        md5: The MD5 hash (with optional .dir suffix).
        source_cache: Path to the source cache/remote root.
        dest_cache: Path to the destination cache base directory.
        verbose: Print progress messages.
        use_v3_layout: If True, use v3 layout for destination.
        cache_type: If specified, only use this link type (reflink, hardlink,
            symlink, or copy). Fails if the specified type doesn't work.
            
    Returns:
        True if file was added to cache.
        False if file already exists in cache.
        None if source file not found (error case).
    """
    source_path = find_source_file(md5, Path(source_cache))
    if source_path is None:
        return None
    
    dest_path = get_cache_file_path(md5, Path(dest_cache), use_v3_layout)

    if md5.endswith('.dir'):
        # Manifests are small and are read back later: store the checked
        # bytes themselves rather than a link to the source's file.
        raw = find_dir_manifest(md5, Path(source_cache))
        if raw is None:
            if verbose:
                print(f"  {md5[:12]}...: .dir content does not match its name")
            return None
        return place_dir_object(dest_path, md5, raw)
    
    # Note: We don't check dest_path.exists() here - it's slow on network
    # filesystems. link_file() handles existing files efficiently by catching
    # the failure and checking existence only when needed.
    
    label = f"{md5[:12]}..."
    success, link_type = link_file(source_path, dest_path, verbose=verbose, label=label, cache_type=cache_type)
    
    if link_type == 'skipped':
        return False  # Already existed
    if link_type == 'failed':
        return None
    return success


def is_v2_hash_name(hash_name: str) -> bool:
    """Check if a hash name indicates v2/legacy format.
    
    Args:
        hash_name: The hash algorithm name (e.g., 'md5', 'md5-dos2unix').
        
    Returns:
        True if v2/legacy format, False if v3.
    """
    return hash_name in ('md5-dos2unix', 'params')

