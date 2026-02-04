# dt worktree

Manage git worktrees with DVC cache properly configured.

## Overview

Git worktrees allow working on multiple branches simultaneously without stashing changes. However, DVC's local cache configuration (`dvc cache dir --local`) doesn't carry over to new worktrees, causing DVC operations to fail or use the wrong cache.

`dt worktree` wraps git worktree commands to ensure:
1. The worktree uses the same DVC cache as the main repository
2. Git submodules are initialized

## dt worktree add

Create a git worktree with DVC cache configured.

### Usage

```bash
dt worktree add <path> [options]
```

### Options

- `-b, --new-branch <name>`: Create a new branch with this name
- `--branch <name>`: Checkout an existing branch
- `-v, --verbose`: Show detailed progress

### What it does

1. Gets the current DVC cache path from the main repository
2. Creates the git worktree at the specified path
3. Configures DVC in the new worktree to use the same cache
4. Initializes git submodules recursively

### Examples

```bash
# Create worktree for an existing branch
dt worktree add ../feature-work --branch feature/new-thing

# Create worktree with a new branch
dt worktree add ../experiment -b experiment/test

# With verbose output
dt worktree add -v ../bugfix --branch hotfix/issue-123
```

## dt worktree list

List all git worktrees.

### Usage

```bash
dt worktree list
```

### Example output

```
/Users/me/projects/myrepo
  branch: main
  commit: abc12345

/Users/me/projects/myrepo-feature
  branch: feature/new-thing
  commit: def67890

/Users/me/projects/myrepo-experiment
  branch: experiment/test
  commit: 11223344
```

## dt worktree remove

Remove a git worktree.

### Usage

```bash
dt worktree remove <path> [options]
```

### Options

- `-f, --force`: Force removal even if the worktree has uncommitted changes
- `-v, --verbose`: Show detailed progress

### Examples

```bash
# Remove a clean worktree
dt worktree remove ../feature-work

# Force remove a dirty worktree
dt worktree remove ../experiment --force
```

## Typical workflow

```bash
# Working on main, need to fix a bug on another branch
dt worktree add ../bugfix --branch hotfix/issue-42
cd ../bugfix

# Work on the fix... DVC commands work normally
dvc pull
# ... make changes ...
dvc push
git commit -am "Fix issue #42"
git push

# Return to main work
cd ../myrepo

# Clean up when done
dt worktree remove ../bugfix
```

## Why not just use git worktree?

Regular `git worktree add` doesn't copy DVC's local configuration:

```bash
# Without dt worktree:
git worktree add ../feature feature-branch
cd ../feature
dvc cache dir  # Returns default .dvc/cache, not the shared cache!
dvc pull       # Pulls to wrong cache location
```

With `dt worktree add`, the shared cache is automatically configured:

```bash
# With dt worktree:
dt worktree add ../feature --branch feature-branch
cd ../feature
dvc cache dir  # Returns /scratch/project/dvc/cache (correct!)
dvc pull       # Works correctly with shared cache
```

## See also

- [dt clone](clone.md) - Clone a repository with DVC configured
- [dt cache](cache.md) - Manage external shared caches
- [dt tmp](tmp.md) - Manage temporary repository clones
