---
name: navigate-codebase
description: >
  Navigate the dvc_tools codebase to find the right module quickly.
  Use this when asked to implement, modify, or debug any `dt` command,
  or when you need to know where a feature lives before reading or editing files.
  Also use this when adding a new module, CLI command, or exported function —
  to look up where it fits and to update modules.json to reflect the change.
---

The structured module map for this codebase lives in `modules.json` alongside
this file. Query it with `jq` before reading source files.

> Note: `jq` is available in the `dt3` conda env and the base conda env on
> this system. If unavailable, fall back to reading `modules.json` directly.

## Useful queries

```sh
# Find the module(s) for a dt command
jq '.commands[] | select(.command | contains("summary"))' modules.json

# Find a module by keyword in its description or exports
jq '.modules[] | select(.description | ascii_downcase | contains("secret"))' modules.json
jq '.modules[] | select(.exports[]? | contains("generate_dag"))' modules.json

# List all top-level dt commands
jq -r '.commands[].command' modules.json
```

Run these from the skill directory, or pass the full path to `modules.json`.

## Key structural facts (not in the JSON)

- **All CLI commands** are defined in `dt/cli.py` (~3600 lines). Each one
  delegates immediately to the corresponding module listed in `modules.json`.
- **Error handling**: module-specific exceptions (e.g. `DiffError`,
  `AuthError`, `SummaryError`) bubble up and are caught at the Click boundary
  with `raise click.ClickException(str(e))`.
- **DVC subprocess calls**: always `subprocess.run(['dvc', …])` — never
  `dvc.api.*`.
- **Config**: `.dt/config.yaml` accessed via `cfg.get_value('section.key')` /
  `cfg.set_value(…)` from `dt/config.py`.
- **HPC parallel jobs**: go through `dt/hpc.py` + `qxub`.

## Keeping `modules.json` up to date

When you add a new module, CLI command, or significant exported function,
add or update the relevant entry in `modules.json` as part of the same change.
