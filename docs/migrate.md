# dt migrate

Migrate `.dvc` files from v2 to v3 format, one file at a time.

## Background

DVC v3 introduced two key changes to how files are tracked:

1. **Hash field**: v3 `.dvc` files include an explicit `hash: md5` in each output entry. v2 files omit this.
2. **Hash algorithm**: v2 used `md5-dos2unix` which normalised CRLF line endings to LF before hashing. v3 hashes files as-is with plain `md5`. For binary files and files created on Unix, the hashes are identical — only text files with Windows-style line endings will produce different hashes.

DVC provides `dvc cache migrate` to relocate cache data from v2 layout (`XX/hash`) to v3 layout (`files/md5/XX/hash`), and `dvc cache migrate --dvc-files` to update `.dvc` files. However, `--dvc-files` can trip over imports (which we use liberally), particularly when the source repo's remote still has v2-format data.

`dt migrate` addresses this by updating `.dvc` files one at a time, including imports, by re-hashing from the workspace or cache.

## Usage

```bash
dt migrate [options] [targets...]
```

### Options

- `--dry`: Show what would change without modifying files
- `-v`, `--verbose`: Print detailed progress (hash changes per file)
- `--cache-root <path>`: Override cache root directory (auto-detected if omitted)

### Targets

- Without targets: migrates all `.dvc` files in the project
- With targets: migrates only the specified `.dvc` files or directories
- You can pass data paths too — `dt migrate data.csv` will look for `data.csv.dvc`

## Recommended Workflow

```bash
# 1. First, migrate cache data to v3 layout
dvc cache migrate

# 2. Preview what dt migrate would change
dt migrate --dry -v

# 3. Migrate all .dvc files
dt migrate -v

# 4. Verify and commit
dvc status
git add -u
git commit -m "Migrate .dvc files to v3 format"
```

## Examples

```bash
# Migrate all .dvc files in the project
dt migrate

# Preview changes without modifying files
dt migrate --dry

# Migrate a single file with verbose output
dt migrate data.csv.dvc -v

# Migrate all .dvc files in a directory
dt migrate data/

# Migrate an import file
dt migrate imported/file.csv.dvc -v

# Specify cache root explicitly
dt migrate --cache-root /scratch/dvc/cache/my-project
```

## What it Does

For each `.dvc` file:

1. **Checks format**: If already v3 (has `hash: md5`), skips it
2. **Single files**: Re-hashes the cache copy of the file with plain md5. If the hash changes (CRLF file), links the file under the new hash in the v3 cache layout
3. **Directories**: Reads the `.dir` manifest from the cache, re-hashes each child file from cache, builds a new manifest, and writes it to the v3 cache
4. **Imports**: Handled exactly like regular files — the local cache copy is re-hashed. The import metadata (deps, repo URL, rev_lock) is preserved
5. **Top-level checksum**: Recomputes the top-level `md5` field present in import `.dvc` files

All hashing is done from cache files — the workspace is never read. This avoids any risk of a dirty workspace producing incorrect hashes.

## v2 vs v3 Format

### v2 `.dvc` file

```yaml
outs:
- md5: abcdef1234567890abcdef1234567890
  size: 1024
  path: data.csv
```

### v3 `.dvc` file

```yaml
outs:
- md5: abcdef1234567890abcdef1234567890
  size: 1024
  hash: md5
  path: data.csv
```

The key difference is the `hash: md5` field. Additionally, the `md5` value itself may change if the file contained CRLF line endings (since v2 normalised them before hashing).

## Notes

- **Run `dvc cache migrate` first** to ensure cache files are in the v3 layout (`files/md5/XX/...`). `dt migrate` updates `.dvc` file metadata; it does not relocate cache data.
- **Hashes from cache only** — the workspace is never read, so dirty or un-checked-out files don't matter. The cache is the authoritative source of truth.
- **No DVC internals** — uses standard YAML parsing and `hashlib.md5`, consistent with the rest of `dt`. The only DVC interaction is auto-detecting the cache path.
- **Binary files and Unix-created text files** will have the same hash in v2 and v3. Only CRLF text files will get a new hash.
- **Imports are safe** as long as the imported data is in the local cache. The source repo's format doesn't matter — we re-hash the local cache copy.
- **Dry run first**: Use `--dry -v` to preview changes before committing.
