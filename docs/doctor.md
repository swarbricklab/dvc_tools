# dt doctor

Diagnose common setup issues and verify your environment is correctly configured.

## Usage

```bash
dt doctor           # Quick health check
dt doctor -v        # Verbose output, includes dvc doctor
```

## Options

- `-v, --verbose`: Show detailed output including `dvc doctor` results

## Checks Performed

| Check | What it verifies |
|-------|------------------|
| Git | Git is installed and accessible |
| DVC | DVC is installed and accessible |
| GitHub CLI | `gh` is available (optional, enables some features) |
| SSH key | A public key exists in `~/.ssh/` (`id_ed25519`, `id_rsa`, `id_ecdsa` or `id_dsa`) |
| GitHub SSH | Can connect to `git@github.com` |
| Cache root | `cache.root` is configured, exists and is writable |
| Remote root | Every `remote.root` entry exists; the first one (where new remotes are created) is writable |
| Archived remotes | No configured DVC remote has been archived (carries an `ARCHIVED.yaml` signpost) |
| Git repo | The current directory is inside a git repository |
| DVC repo | The current directory is inside a DVC repository |
| dvcignore | `.dvcignore` excludes `.gitignore`, so this repo's generated ignore files cannot be hashed into the payload of anyone importing a directory from it ([why](init.md#why-dvcignore-starts-with-gitignore)) |

### Verbose-only checks

These additional checks run with `-v` because they may be slow (network
access, endpoint discovery):

| Check | What it verifies |
|-------|------------------|
| Network | Internet connectivity (TCP connect to `8.8.8.8:53`, then `1.1.1.1:53`) |
| Local remote | A DVC local remote is configured |
| Auth access | All discovered storage endpoints are accessible (runs `dt auth check` internally) |

## Example Output

```
$ dt doctor
DVC Tools version: 0.1.0

✓ Git installed (2.39.0)
✓ DVC installed (3.55.2)
✓ GitHub CLI installed (2.40.0)
✓ SSH key found (~/.ssh/id_ed25519.pub)
✓ GitHub SSH connection works
✓ Cache root accessible (/g/data/a56/dvc_cache)
✓ Remote root accessible (/g/data/a56/dvc_remote)
✓ No archived remotes detected
✓ In git repository (/scratch/a56/me/my-project)
✓ In DVC repository (/scratch/a56/me/my-project)
✓ .dvcignore excludes .gitignore files

All 11 checks passed.
```

With issues:

```
$ dt doctor
DVC Tools version: 0.1.0

✓ Git installed (2.39.0)
✓ DVC installed (3.55.2)
✗ GitHub CLI not found
  Install: https://cli.github.com (optional, enables some features)
✗ No SSH key found
  Run: ssh-keygen -t ed25519 -C "your.email@example.com"
✗ GitHub SSH connection failed
  See: https://docs.github.com/en/authentication/connecting-to-github-with-ssh
✗ Cache root not configured
  Run: dt config set cache.root /path/to/cache
✗ Remote root not configured
  Run: dt config set remote.root /path/to/remote
✓ No archived remotes detected
✓ In git repository (/scratch/a56/me/my-project)
✓ In DVC repository (/scratch/a56/me/my-project)

5 passed, 5 failed.
```

## Verbose Mode

With `-v`, also runs auth access checks, `dvc doctor` and shows configuration sources:

```bash
$ dt doctor -v
DVC Tools version: 0.4.2

✓ Git installed (2.39.0)
...
✓ Auth access: all 4 endpoint(s) accessible

All 13 checks passed.

--- Configuration (with sources) ---
user    owner=myorg
project cache.root=/g/data/a56/dvc_cache
project remote.root=/g/data/a56/dvc_remote

--- DVC Doctor ---
DVC version: 3.55.2
...
```

## See Also

- [dt auth check](auth.md) - Detailed per-endpoint access checks
- [dt clone](clone.md) - Clone repositories (uses SSH)
- [dt config](config.md) - Configure cache and remote paths
