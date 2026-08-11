# dt offline

Redirect Git and DVC lookups to local copies so DVC operations work on compute nodes without internet access.

## Overview

Compute nodes on most HPC systems have no outbound network. That breaks any DVC operation that needs to reach a source repository — imports, in particular, resolve their dependency by cloning the repo they were imported from.

`dt offline` works around this by rewriting *where* Git and DVC look, without changing anything tracked in the repository:

- **Git URLs** are redirected to the sparse clones under `.dt/tmp/clones/` using Git's `url.<path>.insteadOf` mechanism, written to the repository's local Git config.
- **SSH DVC remotes** are overridden to their underlying local filesystem path, written to `.dvc/config.local`.

Both are local-only, so nothing is committed and other clones of the repo are unaffected.

You prepare on the login node, which has internet, and then submit the job.

```bash
# On the login node
dt tmp clone source-repo     # fetch the .dvc config you'll need
dt offline enable

# Submit the job - Git and DVC operations now resolve locally

dt offline disable           # back to normal when you're done
```

## dt offline enable

Set up the Git redirects and DVC remote overrides.

### Usage

```bash
dt offline enable [-v]
```

### Options

- `-v`, `--verbose`: Show each redirect and override as it is set

### What it does

For every temporary clone in `.dt/tmp/clones/<host>/<owner>/<repo>`, it adds an `insteadOf` entry covering all four URL spellings of that repository, so a match does not depend on how the `.dvc` file happens to name it:

```
git@github.com:owner/repo.git
git@github.com:owner/repo
https://github.com/owner/repo.git
https://github.com/owner/repo
```

For every SSH remote in `.dvc/config` whose path exists on the local filesystem, it sets `remote.<name>.url` to that path in `.dvc/config.local`. Remotes whose local path does not exist are left alone.

The set of redirects and overrides it created — including each remote's original URL — is recorded in `.dt/config.local.yaml` under the `offline` key, so `disable` can reverse exactly what `enable` did.

Fails if there are no temporary clones *and* no usable SSH remotes; there would be nothing to redirect.

## dt offline disable

Remove the redirects and overrides, restoring the original URLs.

### Usage

```bash
dt offline disable [-v]
```

### Options

- `-v`, `--verbose`: Show each redirect and override as it is removed

Restoration is driven by the state saved in `.dt/config.local.yaml` rather than by guessing, so remotes are returned to the exact URLs they had before `enable` ran.

## dt offline status

Report what is currently redirected.

### Usage

```bash
dt offline status
```

### What it reports

- Temporary clones that are available
- Which of those have an active Git redirect
- Clones that are available but *not* redirected — usually means they were cloned after `enable` ran
- SSH remotes available for override, and which are currently overridden

If you clone a new repository while offline mode is on, re-run `dt offline enable` to pick it up.

## Refreshing clones

Temporary clones are snapshots. To update them you need internet, so this has to happen on the login node, and the redirects must be off while it happens — otherwise Git resolves the clone's own origin back to the local copy:

```bash
dt offline disable
dt tmp clone source-repo   # refreshes an existing clone by default
dt offline enable
```

## Files touched

| File | Written by | Contents |
|------|-----------|----------|
| `.git/config` | `git config --local` | `url.<clone-path>.insteadOf` entries |
| `.dvc/config.local` | `dvc config --local` | `remote.<name>.url` overrides |
| `.dt/config.local.yaml` | `dt offline` | Saved state under the `offline` key |

None of these are tracked by Git, so offline mode never leaks into a commit or affects a colleague.

## Requirements

Must be run inside a `dt`-initialized repository — `dt offline` needs a `.dt/` directory to store its state. Run `dt init` first if you do not have one.

## See also

- [dt tmp](tmp.md) - Manage the temporary clones offline mode redirects to
- [dt fetch](fetch.md) - Fetch data, including imports resolved via local clones
- [dt import](import.md) - Import data from another repository
