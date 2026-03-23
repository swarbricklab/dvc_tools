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
| SSH key | SSH key exists in `~/.ssh/` |
| GitHub SSH | Can connect to `git@github.com` |
| Cache root | `cache.root` is configured and accessible |
| Remote root | `remote.root` is configured and accessible |

### Verbose-only checks

These additional checks run with `-v` because they may be slow (network
access, endpoint discovery):

| Check | What it verifies |
|-------|------------------|
| Network | Internet connectivity (e.g. `github.com` reachable) |
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

All 7 checks passed.
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

3 passed, 4 failed.
```

## Verbose Mode

With `-v`, also runs auth access checks, `dvc doctor` and shows configuration sources:

```bash
$ dt doctor -v
DVC Tools version: 0.4.2

✓ Git installed (2.39.0)
...
✓ Auth access: all 4 endpoint(s) accessible

All 12 checks passed.

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
