# Debugging dt fetch on NCI

## Problem Summary

`dt fetch` works locally but fails on NCI. We need to understand:
1. What link type is being used (reflink/hardlink/symlink/copy)?
2. Is the cache being populated correctly?
3. Does `dvc checkout` work with the populated cache?

## Quick Debug Commands

### 1. Check filesystem types

```bash
# What filesystems are scratch and gdata?
df -T /scratch/$PROJECT /g/data/$PROJECT 2>/dev/null || df /scratch/$PROJECT /g/data/$PROJECT

# Are they the same filesystem? (hardlinks won't work across filesystems)
stat -f %d /scratch/$PROJECT /g/data/$PROJECT 2>/dev/null || stat -c %d /scratch/$PROJECT /g/data/$PROJECT
```

### 2. Test link types manually

```bash
# Create test files
TEST_DIR=$(mktemp -d)
REMOTE_FILE="$TEST_DIR/remote/test.txt"
CACHE_FILE="$TEST_DIR/cache/test.txt"
mkdir -p "$(dirname $REMOTE_FILE)" "$(dirname $CACHE_FILE)"
echo "test content" > "$REMOTE_FILE"

# Test reflink (usually not supported on Lustre)
cp --reflink=only "$REMOTE_FILE" "$CACHE_FILE.reflink" 2>&1
echo "Reflink: $?"

# Test hardlink
ln "$REMOTE_FILE" "$CACHE_FILE.hardlink" 2>&1
echo "Hardlink: $?"

# Test symlink
ln -s "$REMOTE_FILE" "$CACHE_FILE.symlink" 2>&1
echo "Symlink: $?"

# Show results
ls -la "$TEST_DIR/cache/"

# Cleanup
rm -rf "$TEST_DIR"
```

### 3. Test cross-project linking

If cache is on scratch and remote is on gdata:

```bash
# Test hardlink across projects (will likely fail with EPERM)
SCRATCH_FILE="/scratch/$PROJECT/$USER/test_link_$$"
GDATA_FILE="/g/data/$PROJECT/$USER/test_link_$$"
mkdir -p "$(dirname $SCRATCH_FILE)" "$(dirname $GDATA_FILE)"
echo "test" > "$GDATA_FILE"

ln "$GDATA_FILE" "$SCRATCH_FILE" 2>&1
echo "Cross-project hardlink exit code: $?"

# Test symlink (should work)
rm -f "$SCRATCH_FILE"
ln -s "$GDATA_FILE" "$SCRATCH_FILE" 2>&1
echo "Cross-project symlink exit code: $?"

# Verify symlink works
cat "$SCRATCH_FILE"

# Cleanup
rm -f "$SCRATCH_FILE" "$GDATA_FILE"
```

### 4. Run dt fetch with verbose output

```bash
cd /path/to/your/dvc/repo

# Run fetch with verbose to see which link type is used
dt fetch -v imported/file.csv.dvc 2>&1 | tee /tmp/dt_fetch_debug.log

# Check what was created in cache
CACHE_DIR=$(dvc cache dir)
echo "Cache dir: $CACHE_DIR"
find "$CACHE_DIR" -type l -ls 2>/dev/null | head -10  # symlinks
find "$CACHE_DIR" -type f -ls 2>/dev/null | head -10  # regular files
```

### 5. Check if symlink in cache works with dvc checkout

```bash
# After dt fetch, try checkout
dvc checkout imported/file.csv.dvc -v 2>&1

# If it fails, check what's in cache
CACHE_DIR=$(dvc cache dir)
ls -la "$CACHE_DIR/files/md5/"*/  # v3 layout
ls -la "$CACHE_DIR/"*/            # v2 layout

# Check if symlinks point to valid targets
find "$CACHE_DIR" -type l -exec sh -c 'echo "{}"; ls -la "{}"; test -e "{}" && echo "  -> EXISTS" || echo "  -> BROKEN"' \;
```

### 6. Trace exactly what populate_cache_file does

```python
# Run this Python snippet in the repo directory
import os
from pathlib import Path

# Simulate what populate_cache_file does
source = Path("/g/data/PROJECT/.remote/files/md5/ab/cdef123...")  # adjust path
dest = Path("/scratch/PROJECT/.cache/files/md5/ab/cdef123...")    # adjust path

print(f"Source exists: {source.exists()}")
print(f"Source is file: {source.is_file()}")
print(f"Source is symlink: {source.is_symlink()}")

# Try each link type
dest.parent.mkdir(parents=True, exist_ok=True)

# Reflink
import subprocess
result = subprocess.run(['cp', '--reflink=only', str(source), str(dest)], capture_output=True)
print(f"Reflink: {'OK' if result.returncode == 0 else result.stderr.decode()}")
if dest.exists(): dest.unlink()

# Hardlink
try:
    os.link(source, dest)
    print("Hardlink: OK")
    dest.unlink()
except OSError as e:
    print(f"Hardlink: {e}")

# Symlink
try:
    os.symlink(source, dest)
    print("Symlink: OK")
    print(f"  Symlink target exists: {dest.resolve().exists()}")
    dest.unlink()
except OSError as e:
    print(f"Symlink: {e}")
```

## Key Questions to Answer

1. **What link type succeeds?** Run the manual tests above.

2. **If symlink succeeds, why does dvc checkout fail?**
   - Is the symlink target path correct (absolute vs relative)?
   - Does the target file actually exist?
   - Is there a permissions issue reading through the symlink?

3. **What error does dvc checkout give?**
   ```bash
   dvc checkout -vvv target.dvc 2>&1 | grep -i "error\|fail\|missing"
   ```

4. **What's the cache layout?**
   - Is source remote using v2 (`XX/hash`) or v3 (`files/md5/XX/hash`)?
   - Is dest cache using v2 or v3?

## Report Back

After running these tests, report:

1. Filesystem types for scratch and gdata
2. Which link types work (reflink/hardlink/symlink)
3. The exact error message from `dt fetch -v` and `dvc checkout -vvv`
4. Contents of cache dir after fetch (are files/symlinks present?)
5. If symlinks are created, do they point to valid targets?
