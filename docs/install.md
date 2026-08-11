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
├── post-merge       →  dt hook run post-merge "$@"
├── post-rewrite     →  dt hook run post-rewrite "$@"
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
| `post-merge` | `dvc-checkout` | sync | Same reconcile after `git merge` |
| `post-rewrite` | `dvc-checkout` | sync | Same reconcile after `git rebase` / `git commit --amend` |
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

After a branch switch, merge, or rewrite, `dvc-checkout` runs `dt pull` to
relink the workspace, but with **`network: false`** by default: it uses only local
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

`dvc-checkout` is registered separately on `post-checkout`, `post-merge`
and `post-rewrite`, so set `network` on each hook you want it to apply to.

Run an explicit `dt pull` any time to fetch missing data over the network.

### `dvc-checkout` safety gates

Reconciling the workspace against an incoming `dvc.lock` can delete large
amounts of data, so `dvc-checkout` refuses to run (and prints why, without
failing the git operation) when either of these holds:

- **Subset drop** — the incoming `dvc.lock` records a strict subset of the
  outs the outgoing one had. The dropped paths are listed.
- **Behind upstream** — the current branch is behind its configured
  `@{upstream}`. No fetch is performed; this is a local snapshot only.

Set `DT_HOOK_FORCE=1` in the environment to bypass both gates for a single
git operation.

The hook also skips entirely when: the `post-checkout` flag says it was a
file checkout, `prev == new` (e.g. `git checkout -b` at the current tip),
or a rebase is in progress (`post-rewrite` picks it up once the rebase
finishes). When it does reconcile, it prints an added/modified/deleted
summary and lists deleted paths, so a large deletion is never silent.

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
- Where the setting comes from — `default` for the built-in defaults, or the
  config scope that overrode them (local, project, user, system)
- Extra settings (`max_size`, external `command`)
- Whether it is disabled

Example output on a repo with no `hooks` config of its own — everything
comes from the built-in defaults:

```
pre-commit:
  dvc-status           sync   (default)
  large-files          sync   (default)  max_size=1MB

post-checkout:
  dvc-checkout         sync   (default)

post-merge:
  dvc-checkout         sync   (default)

post-rewrite:
  dvc-checkout         sync   (default)

pre-push:
  dvc-push             remind (default)
```

### dt hook run

```bash
dt hook run <hook-name> [ARGS...] [-v]
```

`<hook-name>` is one of `pre-commit`, `post-checkout`, `post-merge`,
`post-rewrite`, `pre-push`.

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
Files with a `.dvc` extension and files named `.gitignore` are excluded,
as are staged deletions.

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
3. The command is submitted via `hpc.build_qxub_command()`. For `dvc-push`,
   `--internet` is added when no locally-mounted remote is available.
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
dt hook results [-n LIMIT] [--all]
dt hook results --clear [--days N]
```

| Option | Description |
|--------|-------------|
| `-n, --limit N` | Show at most N results (default 20) |
| `--all` | Show all results, not just unread ones |
| `--clear` | Remove result files |
| `--days N` | With `--clear`, only remove results older than N days |

By default only **unread** results are shown — those newer than the
`.last-read` sentinel in `.dt/hook-results/`, which is touched every time
you run `dt hook results`. Hooks print a reminder when unread results are
waiting. Results are listed most recent first:

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
| `mode` | string | `sync` | `sync` (blocks git), `async` (qxub), or `off` (skip). `dvc-push` accepts more — see [`dvc-push` modes](#dvc-push-modes) |
| `command` | string | — | Shell command for external checks |
| `max_size` | string | `1MB` | For `large-files` check only |
| `network` | bool | `false` | For `dvc-checkout` check only — allow fetching missing data from the remote |

There is also a top-level `hooks.verbosity` key (not per-check) controlling
how much hook output you see: `quiet`, `normal` (default), or `verbose`.
`dt hook run -v` forces `verbose` regardless of the config value.

```bash
dt config set hooks.verbosity quiet
```

### Built-in checks

| Name | Hook | Description |
|------|------|-------------|
| `dvc-status` | pre-commit | Runs `dvc status` via `dt status` |
| `large-files` | pre-commit | Rejects staged files exceeding `max_size` |
| `dvc-checkout` | post-checkout, post-merge, post-rewrite | Reconciles the workspace via `dt pull` (skips file checkouts and in-progress rebases; see [safety gates](#dvc-checkout-safety-gates)) |
| `dvc-push` | pre-push | Reminds about / prompts for / performs `dt push` — see [`dvc-push` modes](#dvc-push-modes) |
| `index-sync` | any | Pulls then pushes the site cache index. Not enabled by default; a no-op unless `index.auto_sync` is configured |

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

When the push does run (`sync`, `prompt`-confirmed, or on the compute node
for `async`) it prefers a locally-mounted remote, and retries if DVC's
workspace lock is held by another process — up to 12 retries over roughly
9 minutes. This matters most for `async`, where the job runs later while
you keep working in the same checkout. Any other `dvc push` failure fails
immediately.

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
| post-checkout hook | `dvc checkout` | Configurable (dvc-checkout, custom), with safety gates |
| post-merge / post-rewrite hooks | — | ✓ Reconcile after `git merge` / `git rebase` |
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
