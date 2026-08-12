---
name: navigate-codebase
description: >
  Navigate the dvc_tools codebase to find the right module quickly.
  Use this when asked to implement, modify, or debug any `dt` command,
  or when you need to know where a feature lives before reading or editing files.
  Also use this when adding a new module or CLI command — to look up where it
  fits, and to regenerate modules.json so the map stays current.
---

The structured module map for this codebase lives in `modules.json` alongside
this file. Query it with `jq` before reading source files.

`modules.json` has two arrays: `commands` maps every `dt` command to the module
that implements it, and `modules` describes the shared infrastructure modules
that no single command owns. A command entry may carry an `also` field listing
the other modules it draws on.

## Useful queries

```sh
# Find the module(s) for a dt command
jq '.commands[] | select(.command | contains("summary"))' modules.json

# Which command(s) does a given module implement?
jq '.commands[] | select(.module == "dt/perms.py")' modules.json

# Find an infrastructure module by keyword in its description
jq '.modules[] | select(.description | ascii_downcase | contains("lock"))' modules.json

# Find the command that calls a given function
jq '.commands[] | select(.functions[]? == "sweep")' modules.json

# List every command
jq -r '.commands[].command' modules.json
```

Run these from the skill directory, or pass the full path to `modules.json`.

## Key structural facts (not in the JSON)

- **All CLI commands** are defined in `dt/cli.py` (~5800 lines). Each one
  delegates immediately to the corresponding module listed in `modules.json`.
  Some commands import their implementation *inside* the function body rather
  than at module level, so grep the body, not just the imports.
- **Error handling**: module-specific exceptions (e.g. `DiffError`,
  `AuthError`, `SummaryError`) bubble up and are caught at the Click boundary
  with `raise click.ClickException(str(e))`.
- **Driving `dvc`**: to run a DVC *command*, shell out with
  `subprocess.run(['dvc', …])` — `dvc.api.*` is used nowhere. But DVC is also
  used as a **library** for non-command work: `dvc.repo.Repo.find_root`,
  `dvc_data.hashfile.*` for hashing and tree manipulation, `dvc.dvcfile.load_file`.
  Roughly 22 modules import it this way, `dt/utils.py` most heavily. So "never
  import dvc" is the wrong lesson — the split is by *what you need*, not by rule.
- **Config**: `.dt/config.yaml` accessed via `cfg.get_value('section.key')` /
  `cfg.set_value(…)` from `dt/config.py`.
- **HPC parallel jobs**: go through `dt/hpc.py` + `qxub`.

## Keeping `modules.json` up to date

When you add a new module or CLI command, refresh `modules.json` as part of the
same change.

The `commands` array is generated, so don't hand-edit it — regenerate instead:

```sh
conda run -n dt python .github/skills/navigate-codebase/generate.py
conda run -n dt python .github/skills/navigate-codebase/generate.py --check
```

`--check` writes nothing and exits non-zero when the committed file is stale.

The `modules` array is different: those descriptions are written by hand and
the generator preserves them, adding only files that appear and dropping ones
that are gone. A new entry arrives with its module docstring as a placeholder —
replace it with something that says *when you'd want this module*.

Worth knowing why this is automated: the map was hand-kept until it sat six
minor versions behind, describing 40 of 97 commands. The gaps were invisible,
because every entry it did have was still correct, so it read as complete.
