#!/usr/bin/env python
"""Regenerate ``modules.json`` for the navigate-codebase skill.

The ``commands`` array is derived from ``dt/cli.py`` rather than maintained by
hand -- it went six minor versions out of date last time it was hand-kept, and
the failure was silent: every entry still pointed at a real module, so the map
read as complete while missing well over half the CLI.

The ``modules`` array is different. It describes the shared infrastructure that
no single command owns, and those descriptions are written by humans, so they
are preserved across runs. This script only adds files that appear and drops
entries whose file is gone.

Usage::

    conda run -n dt python .github/skills/navigate-codebase/generate.py
    conda run -n dt python .github/skills/navigate-codebase/generate.py --check

``--check`` writes nothing and exits non-zero if the committed file is stale,
which is what you want if this is ever wired into CI.

Requires the ``dt`` package to be importable (it walks the live Click tree), so
run it inside the project's conda environment.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
TARGET = HERE / "modules.json"

# Every command touches these, so naming them as the implementing module tells
# the reader nothing. They stay eligible as a last resort (see _primary).
UBIQUITOUS = {"dt/config.py", "dt/utils.py", "dt/errors.py"}

# Packages whose __init__ re-exports names defined in submodules. Without this
# every `dt auth *` command resolves to dt/auth/__init__.py, which is true but
# useless -- the reader wants the file the function is actually written in.
REEXPORT_PACKAGES = ("auth", "secrets", "archive")


def _module_file(parts: List[str]) -> Optional[str]:
    """Resolve dotted parts to a repo-relative .py path, module or package."""
    stem = Path(*parts).as_posix()
    for candidate in (REPO / f"{stem}.py", REPO / stem / "__init__.py"):
        if candidate.exists():
            return candidate.relative_to(REPO).as_posix()
    return None


def _toplevel_aliases(tree: ast.Module) -> Dict[str, str]:
    """Map `from . import x as y` aliases in cli.py to their module file."""
    aliases: Dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level >= 1:
            prefix = node.module.split(".") if node.module else []
            for name in node.names:
                found = _module_file(["dt", *prefix, name.name])
                if found:
                    aliases[name.asname or name.name] = found
    return aliases


def _local_aliases(node: ast.AST) -> Dict[str, str]:
    """Map relative imports written *inside* a function body.

    Several commands import their implementation in the function rather than at
    module level -- `dt auth setup` is one. Scanning only module-level imports
    silently misattributes those to whatever else the body happens to call.
    """
    aliases: Dict[str, str] = {}
    for sub in ast.walk(node):
        if isinstance(sub, ast.ImportFrom) and sub.level >= 1:
            prefix = sub.module.split(".") if sub.module else []
            for name in sub.names:
                # `from .auth.setup import auth_setup` -- the name may be the
                # function, in which case the module is the prefix itself.
                found = _module_file(["dt", *prefix, name.name]) or _module_file(
                    ["dt", *prefix]
                )
                if found:
                    aliases[name.asname or name.name] = found
    return aliases


def _reexport_map() -> Dict[str, Dict[str, str]]:
    """For each package __init__, map re-exported name -> defining submodule."""
    out: Dict[str, Dict[str, str]] = {}
    for pkg in REEXPORT_PACKAGES:
        init = REPO / f"dt/{pkg}/__init__.py"
        if not init.exists():
            continue
        mapping: Dict[str, str] = {}
        for node in ast.walk(ast.parse(init.read_text())):
            if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module:
                target = REPO / f"dt/{pkg}/{node.module}.py"
                if target.exists():
                    rel = target.relative_to(REPO).as_posix()
                    for name in node.names:
                        mapping[name.asname or name.name] = rel
        out[f"dt/{pkg}/__init__.py"] = mapping
    return out


def _collect(
    node: ast.AST,
    functions: Dict[str, ast.AST],
    aliases: Dict[str, str],
    depth: int = 0,
) -> Dict[str, Set[str]]:
    """Module-qualified calls made by a function, following cli.py helpers.

    ``dt cache perms`` and friends dispatch through a shared ``_run_perms``
    helper in cli.py, so stopping at the callback body finds nothing.
    """
    hits: Dict[str, Set[str]] = {}
    local = _local_aliases(node)
    for file in local.values():
        hits.setdefault(file, set())

    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call):
            continue
        func = sub.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            file = aliases.get(func.value.id)
            if file:
                hits.setdefault(file, set()).add(func.attr)
        elif isinstance(func, ast.Name):
            if func.id in local:
                hits.setdefault(local[func.id], set()).add(func.id.lstrip("_"))
            elif depth < 2:
                helper = functions.get(func.id)
                if helper is not None and helper is not node:
                    for file, names in _collect(
                        helper, functions, aliases, depth + 1
                    ).items():
                        hits.setdefault(file, set()).update(names)
    return hits


def _primary(hits: Dict[str, Set[str]], words: Set[str]) -> str:
    """Pick the module a reader should open first.

    Prefer one whose filename echoes the command (``dt fetch`` -> fetch.py);
    a bare call count picks the wrong file when a command leans on a helper
    module more heavily than on its own.
    """
    matches = [
        f
        for f in hits
        if Path(f).stem in words or Path(f).stem.split("_")[0] in words
    ]
    if len(matches) == 1:
        return matches[0]
    return max(hits, key=lambda f: len(hits[f]))


def build_commands() -> List[dict]:
    """Walk the live Click tree and map every leaf command to its module."""
    import click

    from dt.cli import cli

    tree = ast.parse((REPO / "dt/cli.py").read_text())
    aliases = _toplevel_aliases(tree)
    functions = {
        n.name: n
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    reexports = _reexport_map()

    rows: List[dict] = []

    def walk(cmd, path: List[str]) -> None:
        if isinstance(cmd, click.Group):
            for name in sorted(cmd.commands):
                walk(cmd.commands[name], [*path, name])
            return

        callback = getattr(cmd, "callback", None)
        if callback is None or callback.__name__ not in functions:
            return

        hits = _collect(functions[callback.__name__], functions, aliases)

        resolved: Dict[str, Set[str]] = {}
        for file, names in hits.items():
            for name in names or {""}:
                target = reexports.get(file, {}).get(name, file)
                resolved.setdefault(target, set())
                if name:
                    resolved[target].add(name)

        meaningful = {f: n for f, n in resolved.items() if f not in UBIQUITOUS}
        meaningful = meaningful or resolved
        if not meaningful:
            rows.append(
                {"command": " ".join(path), "module": "dt/cli.py", "functions": []}
            )
            return

        words = set(path[1:]) | {"_".join(path[1:])}
        primary = _primary(meaningful, words)
        entry = {
            "command": " ".join(path),
            "module": primary,
            "functions": sorted(meaningful[primary]),
        }
        also = sorted(f for f in meaningful if f != primary)
        if also:
            entry["also"] = also
        rows.append(entry)

    walk(cli, ["dt"])
    # A private helper command, not part of the public surface.
    return [r for r in rows if not r["command"].endswith("_build-prefix")]


def build_modules(commands: List[dict], existing: List[dict]) -> List[dict]:
    """Curated infrastructure modules: those no single command implements."""
    described = {m["file"]: m["description"] for m in existing}
    implementing = {c["module"] for c in commands}

    on_disk: Set[str] = set()
    for root, dirs, files in os.walk(REPO / "dt"):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in files:
            if name.endswith(".py"):
                rel = Path(root, name).relative_to(REPO).as_posix()
                on_disk.add(rel)

    keep = ((on_disk - implementing) | set(described)) & on_disk

    out: List[dict] = []
    for file in sorted(keep, key=lambda f: (f.count("/"), f)):
        description = described.get(file)
        if description is None:
            try:
                doc = ast.get_docstring(ast.parse((REPO / file).read_text())) or ""
            except (OSError, SyntaxError):
                doc = ""
            description = doc.strip().split("\n")[0] or "TODO: describe this module."
        if file == "dt/cli.py":
            lines = len((REPO / "dt/cli.py").read_text().splitlines())
            description = (
                f"All Click CLI commands (~{lines // 100 * 100} lines). "
                "Each command delegates immediately to its module."
            )
        out.append({"file": file, "description": description})
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the committed modules.json is out of date.",
    )
    args = parser.parse_args()

    existing = json.loads(TARGET.read_text()) if TARGET.exists() else {}
    commands = build_commands()
    modules = build_modules(commands, existing.get("modules", []))
    payload = json.dumps({"commands": commands, "modules": modules}, indent=2) + "\n"

    if args.check:
        if TARGET.exists() and TARGET.read_text() == payload:
            print(f"up to date: {len(commands)} commands, {len(modules)} modules")
            return 0
        print(
            "modules.json is out of date -- rerun without --check",
            file=sys.stderr,
        )
        return 1

    TARGET.write_text(payload)
    print(f"wrote {TARGET.relative_to(REPO)}: "
          f"{len(commands)} commands, {len(modules)} modules")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
