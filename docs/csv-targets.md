# `--csv` for `dt fetch` and `dt pull`

**Status:** implemented in 0.22.0. Kept as the design record — why the option
surface is two flags, why a blank cell is fatal, and what was deliberately left
out.

## Goal

`dt get --csv` and `dt import --csv` let you drive a batch off a sample sheet.
`dt fetch` and `dt pull` can't — they take targets on argv only. So
the moment your selection lives in a spreadsheet, you're back to
`xargs`/`$(cut -d, -f2 sheet.csv | tail -n+2)`, which means hand-counting
columns and losing the header names that made the sheet readable.

The concrete case: you are working *inside* a registry repo (or a repo that
imports from one), you have a 400-row sample sheet, and you want the 38 WTS
rows materialised in your workspace. Today that is `dt pull` on everything
(tens of TB) or a hand-built argv.

## The semantic difference — this is the crux

`dt get`/`dt import` read **two** columns: a path column naming a source path in
a *remote* repository, and an optional `output` column naming where to put it
locally. Both halves are meaningful.

`dt fetch`/`dt pull` operate on the *current* repo. A target is a
`.dvc` file, a stage name, or an output path, and the destination is fixed by
the repo layout. There is nothing for `output` to mean.

So `--csv` here is narrower and simpler: **it is a way to build the target list,
nothing more.** Everything else about each command is unchanged.

Two consequences:

- The reusable unit is a *list of strings*, not `(path, output)` pairs. New
  helper, sharing the reader with `read_csv_targets`.
- A sheet written for `dt get` is still usable, via `--path-col`. If the sheet
  has registry paths in `path` and local paths in `output`, then
  `dt pull --csv sheet.csv --path-col output` is the right invocation.

### Option surface

Two flags per command, and no more:

```
--csv FILE        CSV file listing targets (requires a path column)
--path-col NAME   Column holding the target path  [default: path]
```

**No `--filter`.** A user who wants a subset can produce a subset sheet; that's
a `csvgrep`/`awk`/pandas one-liner they already know, and it leaves an artifact
recording exactly what was selected. Pushing row selection into `dt` buys
little and costs a filter grammar, its error cases, and a second way to end up
with a selection you didn't intend. Adding it later is purely additive, so
declining it now is the reversible choice.

This applies retroactively: **`dt get --csv --filter` is removed too**, so every
CSV-consuming command agrees. See [Removing `--filter` from `dt
get`](#removing---filter-from-dt-get) below.

**No special treatment of an `output` column.** `--path-col` names the column
that matters; every other column is ignored, uniformly and silently — `output`
no differently from `sample_id` or `kind`. Warning about one particular ignored
header would be special-casing that raises more questions than it answers, and
the mistake it would guard against isn't silent anyway: if `output` was the
column you meant, the values in `path` won't resolve and DVC says so.

## Hazards

These are the reasons this is not a five-line change.

### 1. An empty target list means *the whole repo*

Both commands use `targets=None` to mean *everything*:

- [cli.py:3794](dt/cli.py#L3794) — `targets=list(targets) if targets else None`
- [cli.py:4038](dt/cli.py#L4038) — `target_list = list(targets) if targets else None`

So an empty list from the CSV path, run through that same idiom, is a full-repo
fetch or pull rather than a no-op. (`dt push` uses the same idiom at
[cli.py:3463](dt/cli.py#L3463) — one more reason not to extend it there without
care.)

Without `--filter` this is much harder to reach than it would otherwise have
been — a header-only CSV is already a `ValueError`
([utils.py:1537](dt/utils.py#L1537)), and blank path cells are rejected too
(hazard 2). Between them the helper cannot return an empty list at all. But
`X if X else None` appears three times in the CLI already and is exactly the
shape a later edit would reach for by reflex, so the invariant should be
asserted at the call site rather than left to hold by construction in
`dt/utils.py`.

### 2. Empty path cells become empty targets

`read_csv_targets` deliberately keeps rows with a blank path cell so `dt get`
can report them as per-row failures rather than dropping them silently
([utils.py:1506-1508](dt/utils.py#L1506-L1508)). Here there is no per-row
reporting to hang that on, and an empty-string target is worse than useless:
`dvc checkout ''` and `collect_stages([''])` will most likely resolve to the
working directory, i.e. hazard 1 again by another route.

Reject blank cells up front, naming the offending line numbers.

Note this is a blank *cell* in a populated row (`,foo`), not a blank line —
`csv.DictReader` drops wholly empty lines before we ever see them, so a
trailing newline is harmless and needs no special handling.

### 3. One bad row kills the batch

`utils.collect_stages` raises `StageFileDoesNotExistError` on the first target
that doesn't resolve ([fetch.py:1259-1261](dt/fetch.py#L1259-L1261)). On a
400-row sheet with three typos that's three separate runs to discover three
problems, each reporting only one path, with no indication the path came from a
CSV at all.

### 4. argv length

`dt pull` puts every target on the `dvc checkout` command line
([pull.py:254-255](dt/pull.py#L254-L255)). At ~60 chars per path, `ARG_MAX`
(typically 2 MB) is reached somewhere around 30,000 rows. Not a concern for the
sheets we actually have (hundreds of rows), but it is a real ceiling and it
fails obscurely — worth a documented bound rather than a surprise.

## Design

### Shared helper

Add to `dt/utils.py`, next to `read_csv_targets`:

```python
def read_csv_target_list(
    csv_path: str,
    path_col: str = 'path',
) -> List[str]:
    """Read a DVC target list from a CSV file.

    The counterpart to read_csv_targets for commands that act on the current
    repo, where a row names a target and there is no destination to choose.
    Unlike read_csv_targets, a blank path cell is an error rather than a
    reportable per-row failure: an empty target would resolve to the whole
    repo, and "pull everything" is not a failure mode a typo should reach.
    """
```

Refactor the file-reading and header-checking half of `read_csv_targets` into a
private `_read_csv_rows(csv_path, path_col) -> (fieldnames, rows)` and build
both public functions on it — `read_csv_targets` keeps its filter handling,
since `dt get` and `dt import` still use it. Its signature and behaviour stay
exactly as they are; it has twelve tests pinning it
([test_get.py:456-536](tests/unit/test_get.py#L456-L536)).

Contract for the new one:

| Condition | Behaviour |
|---|---|
| CSV missing / header-only / lacks `path_col` | `ValueError` (inherited) |
| Any blank path cell | `ValueError` naming the row numbers |
| Duplicate paths | De-duplicate, preserving first-seen order |
| Any other column | Ignored, silently |

The blank-cell rule plus the inherited header-only rule make hazards 1 and 2
unrepresentable rather than merely handled: the function cannot return a list
that escalates to a whole-repo operation.

De-duplication is worth having because sheets are often one row per *file* with
a shared directory in the path column — 40 rows collapsing to 3 targets.

### Error context (hazard 3)

**Done.** `_csv_context` in `dt/cli.py` appends the CSV, the column, and the
target count to `FetchError`/`PullError`, which is what
`StageFileDoesNotExistError` surfaces as
([fetch.py:1259-1261](dt/fetch.py#L1259-L1261)). Cheap, no new failure
semantics — DVC still names one bad target and stops, but you now know where
the target came from.

**Not done, deliberately:** validating every target before dispatching and
reporting all misses together. The tempting cheap version —
`collect_stages(targets=None)` once, then set-membership — is *not* cheap: a
full collect on a large repo is exactly the work that passing targets avoids.
Revisit only if one-typo-per-run proves annoying in practice.

### `dt fetch`

The smaller of the two. Add `--csv` and `--path-col`; build the list;
pass it as `targets`. The existing `--imports`/`--urls`/`--regular` type
filters compose naturally — they filter the collected stages afterwards
([fetch.py:1262-1270](dt/fetch.py#L1262-L1270)) — so `dt fetch --csv sheet.csv
--regular` is meaningful and needs no special handling.

`--dry` already prints the stage categorization, which gives a free "show me
what this sheet selected" preview.

Guard: `--csv` together with positional `TARGETS` is a `UsageError`. Merging
the two is never what someone means and silently doing so hides a mistake.

### `dt pull`

Same two options, same guard, same routing. `dt pull --csv` inherits `--dry`
for preview.

One extra consideration: pull's checkout phase passes targets to `dvc checkout`
verbatim (hazard 4). Document the practical ceiling; do not chunk it
pre-emptively.

### `dt push` — not doing this

Cut from scope. See [non-goals](#deferred--non-goals) for why.

## Removing `--filter` from `dt get`

`--filter` reaches only `dt get --csv`; `dt import --csv` never took it
([import_data.py:1276](dt/import_data.py#L1276)). It landed on 2026-08-12 in
commit 9323eef (v0.18.0) — one day before this plan — so there is no meaningful
exposure and no reason to deprecate rather than delete. A straight removal:

| File | Change |
|---|---|
| `dt/cli.py` | drop the `--filter` option, the `filters` parameter, the docstring lines, and the example at [cli.py:4312](dt/cli.py#L4312) |
| `dt/get.py` | drop `filters` from `get_from_csv` and its pass-through |
| `dt/utils.py` | drop `filters` from `read_csv_targets`; delete `parse_row_filters` and `row_matches` — nothing else calls them |
| `tests/unit/test_get.py` | drop the five filter tests (483, 491, 500, 508, 513) and the one at 705 |
| `docs/get.md` | drop the `--filter` documentation |

Doing this *first* also makes the `_read_csv_rows` extraction nearly trivial:
with filters gone, `read_csv_targets` and `read_csv_target_list` become thin
siblings over a shared reader, differing only in what they return and how they
treat a blank path cell.

**Sequencing constraint:** PR #178 (`feat/get-s3`) is open and edits both
`get_cmd`'s option block and `get_from_csv`. Land this removal *after* #178
merges, or on top of it — not on a parallel branch.

## Code changes

| File | Change |
|---|---|
| `dt/cli.py`, `dt/get.py`, `dt/utils.py` | remove `--filter` (see table above) |
| `dt/utils.py` | `_read_csv_rows` refactor; new `read_csv_target_list` |
| `dt/cli.py` — `fetch` | `--csv`, `--path-col`; guard vs positional targets; build list |
| `dt/cli.py` — `pull` | same |
| `docs/fetch.md`, `docs/pull.md` | new `## From a CSV` section each |
| `docs/get.md` | drop `--filter`; cross-reference the shared `--csv`/`--path-col` shape |
| `tests/unit/test_csv_targets.py` | new — helper contract |
| `tests/unit/test_fetch.py`, `test_pull.py` | routing + guards |
| `pyproject.toml`, `dt/__init__.py` | 0.21.0 → 0.22.0 |

## Testing

The helper carries the safety properties, so most tests live there and are pure
— no repo, no DVC, no network:

- header-only CSV raises, and specifically **does not** return `[]`
- blank path cell raises, message names the line number
- whitespace-only cell counts as blank
- wholly blank lines are dropped by `csv.DictReader` and stay harmless
- `--path-col output` selects the other column
- unknown `--path-col` raises, listing the columns that do exist
- duplicates collapse, order preserved
- extra columns (`output`, `sample_id`, …) are ignored without comment
- `read_csv_targets` behaviour unchanged after the refactor (existing tests)

Per-command tests need only cover routing and the guards — that `--csv` +
positional targets is a `UsageError`, that the built list reaches the
underlying function, and above all that no input produces `targets=None`. Mock
`fetch_mod.fetch` / `pull_mod.pull` and assert on the call.

Integration tests aren't needed: the underlying commands are unchanged.

## What shipped

Two commits on `feat/csv-target-list`, both in 0.22.0:

1. `refactor(csv)` — removed `--filter`, extracted `_read_csv_rows`, added
   `read_csv_target_list` with 18 tests.
2. `feat(fetch,pull)` — `--csv`/`--path-col` on both commands, the
   `_resolve_targets` guard, `_csv_context` provenance, 20 more tests.

The sequencing constraint held: step 1 landed after PR #178 merged as `ac11d2c`,
so the `get_cmd` and `get_from_csv` edits applied without conflict.

## Open questions

None outstanding. `--filter` is out of all three CSV commands, `--csv` on
`dt push` is cut, and the `output` column gets no special handling.

## Deferred / non-goals

- **`dt push --csv`** — cut. It was the messiest candidate and the weakest use
  case. Push has four modes, and in simple mode it never parses targets at
  all — `ctx.args` is forwarded to `dvc push` verbatim
  ([cli.py:3491](dt/cli.py#L3491)), so `--csv` would make it the first place
  push constructs its own argv, with validation landing in DVC rather than in
  us. It also sets `ignore_unknown_options=True`, so a typo like `--csvv` would
  be silently forwarded rather than rejected. And the motivating case is thin:
  you have just `dt add`ed a batch and want to push only it, but those paths
  are already in your shell history. Revisit only if someone asks.
- **`--filter` on fetch/pull** — deliberately omitted, see the option surface
  above. Additive if it's ever wanted.
- **`--targets-from FILE`** — a plain newline-delimited list of paths. Simpler
  than a CSV and some people will want it. Trivially built on the same
  plumbing, but a second flag needs its own justification. Deferred until
  someone asks.
- **Path rewriting / prefix mapping** — a sheet with registry paths
  (`fastq/SAMPLE1`) used in a repo where they live under `data/fastq/SAMPLE1`.
  `--path-col` covers this when the sheet has both columns; a `--prefix` option
  would cover it when it doesn't. Speculative; don't build it yet.
- **Per-row reporting for fetch/pull** — `dt get --csv` reports per row because
  it resolves each row separately anyway. fetch/pull batch into one index build
  and one checkout, which is the right call for performance; per-row reporting
  would mean giving that up. Not worth it.
- **Chunking `dvc checkout` argv** — see hazard 4. Document the bound; revisit
  if a sheet ever gets near it.
