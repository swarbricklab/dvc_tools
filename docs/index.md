# dt index

Manage the DVC site cache index mirror for shared lookups.

## Overview

The site cache index allows DVC to quickly look up files across caches. This command syncs the local index (on `/tmp`) with a shared network mirror so all users benefit from the same index without rebuilding it.

### How it works

1. **Local index**: DVC maintains a site cache index in a temporary location (typically `/tmp/.dvc/...`)
2. **Mirror**: A shared network location stores the persistent copy
3. **Sync**: `dt index pull` and `push` use rsync to sync between them
4. **Auto-sync**: Commands like `dt pull`, `dt fetch`, and `dt add` automatically sync

## Configuration

Configure the mirror location (typically in system or user config):

```bash
# Set mirror root (required)
dt config set index.mirror_root /g/data/a56/dvc/mirror

# Optional settings
dt config set index.lock_timeout 120      # Lock timeout in seconds (default: 120)
dt config set index.retry_interval 5      # Initial retry interval (default: 5)
dt config set index.auto_sync true        # Enable auto-sync (default: true)
```

## Commands

### dt index pull

Sync the shared index mirror to your local site cache index.

```bash
dt index pull           # Pull latest index
dt index pull -v        # Verbose output
dt index pull --dry     # Preview what would sync
```

### dt index push

Sync your local site cache index to the shared mirror.

```bash
dt index push           # Push index to mirror
dt index push -v        # Verbose output
dt index push --dry     # Preview what would sync
```

### dt index status

Show index configuration and status.

```bash
dt index status
# Output:
# Index configuration:
#   Local:  /tmp/.dvc/site_cache_dir/abc123...
#   Mirror: /g/data/a56/dvc/mirror/repo/abc123...
#
# Status:
#   Local exists:  yes
#   Mirror exists: yes
```

## Automatic sync

When `index.auto_sync` is enabled (default), these commands automatically sync the index:

| Command | Before | After |
|---------|--------|-------|
| `dt pull` | pull | push |
| `dt fetch` | pull | push |
| `dt add` | - | push |

This ensures:
- Before pulling/fetching, you get the latest index entries from others
- After modifying the cache, your changes are shared with others

### Disabling auto-sync

For any command, use `--no-index-sync` to skip automatic sync:

```bash
dt pull --no-index-sync          # Skip index sync
dt fetch --no-index-sync         # Skip index sync
dt add --no-index-sync data/     # Skip index sync
```

To disable auto-sync globally:

```bash
dt config set index.auto_sync false
```

## Locking

The index uses file-based locks to prevent concurrent modifications:

- `local.lock` - Prevents multiple local updates
- `mirror.lock` - Prevents multiple mirror updates

If a lock is held, commands will wait and retry with exponential backoff up to the configured timeout.

### Stale locks

If a process was interrupted, locks may be left behind. The status command shows lock information:

```bash
dt index status
# ...
# Status:
#   Mirror locked: yes (by johree, 3600s ago)
```

To remove a stale lock, delete the lock file manually:

```bash
rm /g/data/a56/dvc/mirror/repo/<hash>/mirror.lock
```

## Failure handling

Index sync failures are treated as warnings, not errors. Commands will continue even if sync fails, with a warning message.

This ensures that:
- Commands work offline or when mirror is unreachable
- Temporary network issues don't block your work
- You can always use `--no-index-sync` if needed

## See also

- [dt pull](pull.md) - Pull DVC-tracked files
- [dt fetch](fetch.md) - Fetch imports into cache
- [dt add](add.md) - Add files to DVC tracking
- [Configuration Options](config_options.md) - Index settings
