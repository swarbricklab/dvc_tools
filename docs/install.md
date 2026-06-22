# dt install

The `install` command sets up git hooks and a DVC merge driver so that
common DVC housekeeping—status checks, file-size guards, cache syncing,
and push—happens automatically at the right points in the git workflow.

All behaviour is driven by configuration keys under `hooks.*`, following
the standard local > project > user > system precedence.

## Quick Start

```bash
# Install hook scripts (no config file is written — defaults live in dt)
dt install

# See what was configured
dt hook list

# Remove everything
dt uninstall
```

After installation your `.git/hooks/` will contain thin scripts that
delegate to `dt hook run <name>`:

```
.git/hooks/
├── pre-commit       →  dt hook run pre-commit "$@"
├── post-checkout    →  dt hook run post-checkout "$@"
└── pre-push         →  dt hook run pre-push "$@"
```

The DVC merge driver (`dvc git-hook merge-driver`) is also registered in
git config so `.dvc` file conflicts are resolved automatically.

## Hooks and Default Checks

| Hook | Check | Mode | What it does |
|------|-------|------|--------------|
| `pre-commit` | `dvc-status` | sync | Runs `dvc status` to warn about uncommitted DVC changes |
| `pre-commit` | `large-files` | sync | Rejects staged files larger than `max_size` (default 1 MB) |
| `post-checkout` | `dvc-checkout` | sync | Relinks DVC-tracked files after a branch switch — **local-only by default** (skips file checkouts and rebases) |
| `pre-push` | `dvc-push` | remind | Warns about unpushed DVC data without blocking the git push (see [`dvc-push` modes](#dvc-push-modes)) |

These defaults are **baked into `dt`**, not written to a config file.
`dt install` therefore writes no `.dt/config.local.yaml` — hooks work out
of the box, and a user's `user`/`system` preferences are never silently
overridden by an auto-written high-precedence file.

Override any field from any scope (standard `local > project > user >
system` precedence): set `enabled: false` to turn a check off, `mode: off`
to skip it, or `mode: async` to offload it to a compute node. Overrides
deep-merge with the defaults per field, so configuring one field of one
check leaves the other defaults intact; to drop a default check, disable it
explicitly.

### `dvc-checkout` is local-only by default

After a branch switch, `dvc-checkout` runs `dt pull` to relink the
workspace, but with **`network: false`** by default: it uses only local
sources (the local cache plus any locally-mounted remote, e.g. the shared
cache on HPC). Data that isn't already available is **reported, not
downloaded**.

This makes branch switches safe on a collaborator's laptop where the remote
is a multi-TB store on NCI — a checkout never triggers an inline,
potentially huge network pull, and can never hang on an SSH prompt. On HPC,
where the remote is on a mounted filesystem, the relink is still fast
because local symlinking is unaffected.

To opt back into fetching missing data from the remote on checkout (the
pre-0.12.5 behaviour), set:

```bash
dt config set hooks.post-checkout.checks.dvc-checkout.network true
```

Run an explicit `dt pull` any time to fetch missing data over the network.

## Commands

### dt install

```bash
dt install [--force] [-v]
```

| Option | Description |
|--------|-------------|
| `--force` | Overwrite existing hooks even if they were not installed by dt |
| `-v, --verbose` | Print detailed progress |

Installs hook scripts and the DVC merge driver. It does **not** write any
check configuration — the defaults are built into `dt` and applied as a
fallback, so nothing is forced into a committed or high-precedence file. If
hooks already exist and were not installed by dt, the command refuses to
overwrite them unless `--force` is given. Re-running `dt install` on hooks
already installed by dt is a no-op.

### dt uninstall

```bash
dt uninstall [-v]
```

Removes only hooks installed by `dt install` (identified by the
`dt hook run` marker).  Foreign hooks are left untouched.  Also
removes the DVC merge driver configuration from git config.

---

## dt hook

The `hook` command group manages and runs individual checks.

### dt hook list

```bash
dt hook list
```

Displays every configured check for every hook, showing:

- Check name
- Mode (`sync` or `async`)
- Config scope it comes from (local, project, user, system)
- Extra settings (`max_size`, external `command`)
- Whether it is disabled

Example output:

```
pre-commit:
  dvc-status           sync   (local)
  large-files          sync   (local)  max_size=1MB

post-checkout:
  dvc-checkout         sync   (local)

pre-push:
  dvc-push             remind (local)
```

### dt hook run

```bash
dt hook run <hook-name> [ARGS...]
```

Runs all enabled checks for the named hook.  This is what the git hook
scripts call—you rarely invoke it directly, but it can be useful for
testing your configuration:

```bash
# Dry-run the pre-commit checks
dt hook run pre-commit

# Simulate a branch-switch checkout (prev, new, flag=1)
dt hook run post-checkout abc123 def456 1
```

**Sync checks** run inline.  If any fail, the git operation is aborted
(non-zero exit).  All sync checks run even if one fails, so you see the
full set of problems at once.

**Async checks** are submitted to a compute node via `qxub` (see below)
and do not block the git operation.

### dt hook check large-files

```bash
dt hook check large-files [--max-size SIZE] [-v]
```

Stand-alone invocation of the built-in large-file guard.  Scans
`git diff --cached` for files exceeding `SIZE` (default `1MB`).
Files with `.dvc` extension are excluded.

```bash
dt hook check large-files --max-size 100MB
```

---

## Async Dispatch (HPC)

Checks configured with `mode: async` are submitted to a compute node
via `qxub` instead of running inline.  This is useful for expensive
checks on HPC systems where login-node time is limited.

### How it works

1. `dt hook run` encounters a check with `mode: async`.
2. It builds a worker command: `dt hook run-check <hook> <check> --worker`.
3. The command is submitted via `hpc.build_qxub_command()`.
4. The git operation continues without waiting.
5. On the compute node, `dt hook run-check --worker` runs the check and
   saves the result as JSON in `.dt/hook-results/`.

### dt hook run-check

```bash
# Submit a check to a compute node
dt hook run-check <hook-name> <check-name>

# Run directly on this node and save the result (worker mode)
dt hook run-check <hook-name> <check-name> --worker [-v]
```

Without `--worker`, submits to qxub.  With `--worker`, runs the check
inline and writes the result to `.dt/hook-results/`.

### dt hook results

```bash
dt hook results [-n LIMIT]
dt hook results --clear [--days N]
```

| Option | Description |
|--------|-------------|
| `-n, --limit N` | Show at most N results (default 20) |
| `--clear` | Remove result files |
| `--days N` | With `--clear`, only remove results older than N days |

Displays recent async check results, most recent first:

```
✓ 2026-03-11 14:32:01  pre-commit/dvc-status
✗ 2026-03-11 14:31:58  pre-commit/large-files
    Files exceed 1MB limit:
      data/big_matrix.npy (128.5MB)

    Track large files with DVC instead:  dt add <file>
    Adjust the limit:  dt config set hooks.pre-commit.checks.large-files.max_size 10MB
    Skip this check once:  git commit --no-verify
```

---

## Configuration

Checks are configured under `hooks.<hook-name>.checks.<check-name>`:

```yaml
hooks:
  pre-commit:
    checks:
      dvc-status:
        enabled: true
        mode: sync
      large-files:
        enabled: true
        mode: sync
        max_size: 1MB
      my-linter:
        enabled: true
        mode: async
        command: "black --check ."
```

### Check settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `true` | Whether the check runs |
| `mode` | string | `sync` | `sync` (blocks git) or `async` (qxub) |
| `command` | string | — | Shell command for external checks |
| `max_size` | string | `1MB` | For `large-files` check only |

### Built-in checks

| Name | Hook | Description |
|------|------|-------------|
| `dvc-status` | pre-commit | Runs `dvc status` via `dt status` |
| `large-files` | pre-commit | Rejects staged files exceeding `max_size` |
| `dvc-checkout` | post-checkout | Runs `dvc checkout` (skips file checkouts and rebases) |
| `dvc-push` | pre-push | Reminds about / prompts for / performs `dt push` — see [`dvc-push` modes](#dvc-push-modes) |

#### `dvc-push` modes

The `pre-push` `dvc-push` check supports five values for `mode`:

| Mode | Behaviour |
|------|-----------|
| `remind` *(default)* | Run a fast `dvc status -c` and print a yellow warning if outstanding DVC data is detected. Never pushes, never blocks the git push. |
| `prompt` | As `remind`, then interactively ask whether to push. Skipped automatically when stdin is not a TTY (e.g. CI). |
| `sync` | Push inline (blocks the git push until done). |
| `async` | Submit `dt push` to a compute node via qxub and return immediately. |
| `off` | Do nothing. |

Change the mode with:

```bash
dt config set hooks.pre-push.checks.dvc-push.mode prompt
```

`remind` is the default because inline pushes from a pre-push hook can
be very slow on HPC login nodes and surprise users with long blocking
operations. The reminder is a single fast status check.

### External checks

Any check with a `command` key runs that shell command as a subprocess.
Non-zero exit means failure.  Use this for linters, formatters, or any
project-specific validation:

```yaml
hooks:
  pre-commit:
    checks:
      black:
        enabled: true
        mode: sync
        command: "black --check ."
      isort:
        enabled: true
        mode: async
        command: "isort --check-only ."
```

### Overriding the large-file limit

The default limit is 1 MB—intentionally strict for repos that use DVC for
data.  There are three ways to override when needed:

**Raise the limit permanently** (in project or local config):

```bash
dt config set hooks.pre-commit.checks.large-files.max_size 10MB
```

**Skip the check for a single commit** (e.g. committing a vendored PDF):

```bash
git commit --no-verify
```

`--no-verify` skips *all* git hooks for that commit, so use it sparingly.

**Disable the check entirely:**

```bash
dt config set hooks.pre-commit.checks.large-files.enabled false
```

### Disabling a check

Override from any scope:

```bash
# Disable large-files check in local config
dt config set hooks.pre-commit.checks.large-files.enabled false
```

Or edit `.dt/config.local.yaml` directly:

```yaml
hooks:
  pre-commit:
    checks:
      large-files:
        enabled: false
```

---

## Comparison with dvc install

| Feature | `dvc install` | `dt install` |
|---------|---------------|--------------|
| pre-commit hook | `dvc status` | Configurable checks (dvc-status, large-files, custom) |
| post-checkout hook | `dvc checkout` | Configurable (dvc-checkout, custom) |
| pre-push hook | `dvc push` | Configurable: `remind` / `prompt` / `sync` / `async` / `off` |
| Merge driver | ✓ `.dvc` conflict resolution | ✓ Same driver |
| Large file guard | — | ✓ Built-in `large-files` check |
| Async dispatch | — | ✓ Offload to compute node via qxub |
| External checks | — | ✓ Run arbitrary shell commands |
| Config-driven | — | ✓ Enable/disable/override per scope |
| Result tracking | — | ✓ `.dt/hook-results/` for async results |

---

## See Also

- [Configuration Options](config_options.md)
- [Configuration Scopes](config_scopes.md)
