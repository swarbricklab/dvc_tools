# dt deps

Inspect dependencies **between repositories**.

## Synopsis

```bash
dt deps list [OPTIONS]        # Direct sources of this repo
dt deps graph [OPTIONS]       # Recursive graph across repos
dt deps index [OPTIONS]       # Build/refresh the org-wide import index
dt deps downstream [REPO]     # Repos that import FROM this one
dt deps gaps [OPTIONS]        # Repos the graph could not resolve
```

## Description

Where `dvc dag` shows the graph of stages and files *inside* one repo, `dt deps`
shows the graph of imports *between* repos.

`dt deps list` scans `.dvc` files for `deps` sections that name a source repo,
then collapses them into **one line per source repo** rather than one per
import. A repo with thousands of imports usually draws on only a handful of
sources, and it is those connections — not the individual files — that this
command is about.

This is the direct, one-hop view. Recursive traversal
([`dt deps graph`](#dt-deps-graph)) and org-wide downstream discovery
([`dt deps index`](#dt-deps-index) / [`dt deps downstream`](#dt-deps-downstream))
build on the same scanner.

## Where imports live

Imports are recorded **only in `.dvc` files**. Neither `dvc.lock` nor `dvc.yaml`
can express a dependency on another repo:

- `dvc.lock` — DVC's lockfile serialiser (`to_single_stage_lockfile`) opens with
  `assert stage.cmd`, and import stages have no command, so they never reach the
  lockfile at all. Even on that path the serialiser builds its dep entries by
  hand and never calls `RepoDependency.dumpd()`, the only thing that emits a
  `repo:` key.
- `dvc.yaml` — stage `deps` are plain path strings; the schema has no `repo` key.

So scanning `.dvc` files is complete, not a heuristic.

A repo import looks like this:

```yaml
md5: 0123456789abcdef0123456789abcdef
frozen: true
deps:
- path: data/final
  repo:
    url: git@github.com:Myorg/data-repo.git
    rev_lock: a1b2c3d4e5f60718293a4b5c6d7e8f9012345678
    rev: main
outs:
- md5: fedcba9876543210fedcba9876543210.dir
  size: 1000000
  nfiles: 6
  hash: md5
  path: final
```

## Options

| Option | Description |
|--------|-------------|
| `--ref REF` | Scan a specific git ref instead of the working tree |
| `--all-branches` | Scan all local and origin branches |
| `--include-paths` | Show example import paths for each source repo |
| `--max-paths N` | Example paths to show per source repo (default: 5) |
| `--show-refs` | List the branches each edge appears on |
| `--max-refs N` | Branches to list per source repo with `--show-refs` (default: 5) |
| `--external` | Also report `dvc import-url` sources (`s3://`, `https://`, ...) |
| `--owner OWNER` | Owner used to expand short repo names |
| `--json` | Output as JSON |

## Examples

Sources for the working tree:

```bash
dt deps list
```

```
Imports in github.com/myorg/example_portal

  github.com/myorg/example_chromium  2 imports  2 revs  1.50G
  github.com/myorg/example_snp       1 import  rev c3d4e5f6  20.0k
  github.com/myorg/example_wgs       5 imports  3 revs  25.0G
  github.com/myorg/data-repo         1 import  rev a1b2c3d4  1.00M

  8 source repos, 21 imports
```

With example paths:

```bash
dt deps list --include-paths
```

```
  github.com/myorg/example_wgs  5 imports  3 revs  25.0G
      data/calls/drivers.csv -> drivers.csv
      data/cohort_summary.csv -> wgs_summary.csv
      index.csv -> wgs.csv
      ... and 2 more
```

## Scanning across branches

`--all-branches` reads every local and `origin/` branch **without checking
anything out** — trees are read straight from git objects, so the working tree
is untouched. Branches pointing at the same commit are scanned once and the
result replayed, since an edge set is a pure function of the commit sha.

The useful signal here is which sources exist *only* on unmerged work:

```bash
dt deps list --all-branches
```

```
Imports in github.com/myorg/example_portal (94 branches, local + origin)

  github.com/myorg/example_wgs    5 imports  7 revs  2.00T  94/94 branches
  github.com/myorg/mega_atlas     1 import  rev d4e5f607  300k  16/94 branches  NOT on origin/main
  github.com/myorg/project-index  2 imports  7 revs  10.0M  85/94 branches

  9 source repos, 24 imports
  1 source repo not present on origin/main (unmerged imports)
```

Import counts are per distinct import, not per (import, branch) pair, so
scanning 94 branches does not multiply the totals. Add `--show-refs` to list the
branches themselves.

Note that `main` and `origin/main` are counted as two branches when both exist.

## Repo identity

All repo URLs are normalised to a `host/owner/repo` id before comparison, so
these all resolve to the same node:

```
git@github.com:Myorg/data-repo.git
https://github.com/myorg/data-repo
git@github.com:myorg/data-repo
```

Owner/repo case is folded for `github.com`, `gitlab.com`, and `bitbucket.org`.
For other hosts only the hostname is lowercased, since their paths may be
case-sensitive.

## External sources

`dvc import-url` dependencies name an object store rather than a repo, so they
are excluded by default. Pass `--external` to report them, grouped by scheme:

```
External sources (import-url):
  s3://  4 imports
```

## JSON output

```bash
dt deps list --json
```

```json
{
  "target": "github.com/myorg/example_portal",
  "refs": [],
  "default_ref": "origin/main",
  "edges": [
    {
      "source": "github.com/myorg/data-repo",
      "source_url": "git@github.com:Myorg/data-repo.git",
      "target": "github.com/myorg/example_portal",
      "n_imports": 1,
      "revs": ["a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"],
      "sample_paths": ["data/final -> final"],
      "total_size": 1000000,
      "refs": [],
      "is_self_loop": false
    }
  ]
}
```

---

# dt deps graph

Build the dependency graph between repositories, recursively.

Starts from this repo, follows its imports to their source repos, clones each
one into `.dt/tmp/clones/`, and repeats. Clones are shared with `dt tmp` and
reused, so re-runs are far faster than the first.

## Options

| Option | Description |
|--------|-------------|
| `--depth N` | Maximum depth to expand (default: unlimited) |
| `--mode head\|pinned` | Which revision of each source repo to scan (default: `head`) |
| `--format text\|mermaid\|dot\|json` | Output format (default: `text`) |
| `-o, --output FILE` | Write to a file instead of stdout |
| `--all-branches` | Scan all branches of the root repo (sources are always read at one rev) |
| `--downstream` | Also include repos that import FROM this one (needs `dt deps index` to have been run) |
| `--org ORG` | Org whose index to use for `--downstream` (default: configured `owner`) |
| `--include-paths` | Show example import paths (text only) |
| `-j, --jobs N` | Concurrent clone/scan workers (default: 4) |
| `--no-refresh` | Use cached clones without fetching |
| `--strict` | Exit non-zero if any repo could not be resolved |
| `--owner OWNER` | Owner used to expand short repo names |
| `-v, --verbose` | Print progress |

`--depth 1` gives the root's direct sources without cloning anything, which
makes it equivalent to `dt deps list` in graph form.

## head vs pinned

These produce genuinely different graphs, and the difference is not cosmetic:

- **`--mode head`** (default) scans each source repo's **default branch**. This
  describes the *current* shape of the ecosystem — what the dependency structure
  looks like today.
- **`--mode pinned`** scans the **`rev_lock` we import at**. This describes the
  *provenance* of the data actually in your workspace — what those repos
  depended on at the moment you pinned them.

A repo can have no `.dvc` files at the revision you pinned and hundreds on its
default branch, so the two graphs can differ in size and shape. Pinned mode
scans at most 3 distinct revisions per source repo; anything beyond that is
reported under "Truncated revisions" rather than dropped silently.

## Example

```bash
dt deps graph
```

```
Repo dependency graph for github.com/myorg/example_portal
  (source repos at their default branch)

myorg/example_portal
├── myorg/example_chromium  (2 imports)
│   ├── myorg/preprocessing  (1 import)
│   │   ├── example-registries/references  (1 import)
│   │   │   └── example-registries/references  (1 import)  (cycle)
│   │   └── myorg/fixtures  (5 imports)
│   │       ├── example-registries/raw-registry  (3 imports)
│   │       └── myorg/spatial-pipeline  (2 imports)
│   └── myorg/references  (1 import)  (seen above)
├── myorg/example_snp  (1 import)
│   ├── myorg/project-index  (1 import)
│   │   ├── myorg/example_snp  (1 import)  (cycle)
...

  16 repos (16 resolved), 32 edges

Cycles:
  example-registries/references (imports from itself)
  myorg/example_snp <-> myorg/example_visium <-> myorg/example_wts <-> myorg/project-index
```

### Reading the tree

A repo graph is a DAG with cycles, not a tree, so two markers keep the output
proportional to the number of repos rather than the number of paths through
them:

- `(seen above)` — this repo's sources were already printed elsewhere. Expanding
  it again would duplicate a whole subtree.
- `(cycle)` — following this edge would return to a repo already on the current
  branch.

For the true graph structure rather than a tree projection of it, use
`--format mermaid` or `--format dot`.

## Cycles

Cycles are expected, not an error. Repos importing from each other is unusual
but legitimate, so nothing here topologically sorts. All cycles are detected
(via strongly connected components) and reported, including self-imports where a
repo imports from an earlier revision of itself.

## Other formats

```bash
dt deps graph --format mermaid -o docs/repo-dag.md
dt deps graph --format dot | dot -Tsvg -o repo-dag.svg
dt deps graph --format json
```

Mermaid and Graphviz both render cycles correctly. Use `dot` for graphs large
enough that Mermaid's layout struggles.

---

# dt deps index

Build or refresh the org-wide import index.

`dt deps graph` answers "what does this repo depend on" by following imports
outward. The inverse question — "what depends on this repo" — cannot be answered
that way, because a repo's `.dvc` files say nothing about who consumes it. That
requires scanning the whole org, which is what this command caches.

## How it stays cheap

The index rests on one property: **an edge set is a pure function of a commit
sha**, so cached scan records are immutable and never need invalidating. All the
volatility is in which sha each ref points at, and two filters exploit that:

1. **`pushed_at`** — a single paginated API call reports every repo's last-push
   timestamp. Repos that have not moved since the last scan are skipped with
   *no git operation at all*.
2. **The trees API** — for repos that did move, one API call lists their file
   tree. Those containing no `.dvc` files are recorded as such without ever
   being cloned.

A steady-state refresh therefore costs one API call plus a handful of clones.

A truncated trees response (very large repos) is treated as *unknown*, not as
"no `.dvc` files", and the repo is cloned. Each scan record stores the `method`
(`clone` or `tree-api`) so a zero-edge result is never ambiguous about how it
was determined.

## Where things are stored

| What | Where | Size |
|------|-------|------|
| The index (JSON) | `<cache-root>/<host>/<org>/` | small |
| The clones | the current project's `.dt/tmp/clones/` | large |

`<cache-root>` is `deps.cache_dir` when set, otherwise the platform user cache
directory for `dvc-tools` plus `repo-deps` — `~/.cache/dvc-tools/repo-deps` on
Linux. Each org gets `index.json` (the repo manifest) plus one JSON file per
repo under `edges/`.

The split is deliberate: on HPC, home directories are quota-constrained, so bulk
data stays on the project filesystem where `dt tmp clean` can reclaim it, while
the small metadata lives in the user cache. Set `deps.cache_dir` to point a whole
lab at one shared index:

```bash
dt config set deps.cache_dir /g/data/a56/dvc-tools/repo-deps
```

Scan records are written one file per repo so concurrent refreshes cannot
clobber each other, and all writes are atomic (temp file + rename).

## Options

| Option | Description |
|--------|-------------|
| `--org ORG` | Organisation or user (default: configured `owner`) |
| `-j, --jobs N` | Concurrent workers (default: 8) |
| `--force` | Rescan every repo, ignoring `pushed_at` |
| `--limit N` | Scan at most N repos this run; re-run to continue |
| `--no-prefilter` | Clone every repo instead of using the trees API |
| `--include-archived` / `--include-forks` | Widen the repo list |
| `--show` | Summarise the cache without refreshing |
| `--clear` | Delete the cached index |
| `--json` | Output as JSON |
| `-v, --verbose` | Print progress |

`--limit` reports how many repos it left unscanned rather than silently
truncating, so a partial index never reads as a complete one.

## Example

```bash
dt deps index --org myorg -v
```

```
Import index for myorg (github.com)

  repos listed:     200
  repos scanned:    20
  repos unreadable: 0
  edges:            25
  repos importing:  7
  generated:        2026-08-08T00:18:51+00:00

  this run: 7 cloned+scanned, 12 unchanged, 1 no .dvc files (not cloned), 0 failed
```

---

# dt deps downstream

Show which repositories import **from** a given repo — the ones that break if
you move or delete data in it.

Reads the cached index, so `dt deps index` must have been run first. The query
itself does no cloning and no network access; pass `--refresh` to rebuild the
index first. The index is looked up per org — `--org` defaults to the
configured `owner`.

```bash
dt deps downstream                                  # consumers of this repo
dt deps downstream myorg/references          # of another repo
dt deps downstream --depth 1                        # direct consumers only
dt deps downstream --org myorg --refresh     # refresh, then query
dt deps downstream --format mermaid -o consumers.md
```

```
Repos importing from github.com/example-registries/references

example-registries/references
├── myorg/atlas-data  (1 import)
│   └── myorg/atlas  (12 imports)
├── myorg/study_data  (2 imports)
├── myorg/other-data  (1 import)
└── myorg/references  (1 import)
    └── myorg/other-data  (9 imports)

  6 repos (6 resolved), 6 edges
```

Consumers are followed transitively, so this shows the full blast radius, not
just direct importers.

`REPO` accepts any of `repo`, `owner/repo`, `host/owner/repo`, or a full URL.

**Completeness caveat:** downstream results are only as complete as the index.
A repo that could not be read, or that has not been scanned yet, cannot appear
as a consumer. `dt deps index --show` reports how many repos are covered and how
many were unreadable — check it before treating an empty result as "nothing
depends on this".

## Combining both directions

`dt deps graph --downstream` merges the live upstream traversal with the cached
downstream view, printing both trees:

```bash
dt deps graph --downstream --org myorg
```

---

# dt deps gaps

Report repositories the graph could not resolve.

This builds the same upstream graph as `dt deps graph` and prints only the gap
report, so it clones just as `graph` does and shares its `--depth`, `-j/--jobs`,
`--no-refresh` and `--owner` options. Add `--json` for machine-readable output.

Access across an org is usually uneven, so a dependency graph is routinely
incomplete. Unreachable repos are always kept as nodes — an incomplete graph
that looks complete is worse than no graph — and rendered distinctly: `[STATUS]`
in text, dashed in Mermaid and Graphviz.

| Status | Meaning |
|--------|---------|
| `ok` | Cloned and scanned |
| `no_dvc` | Scanned fine, contains no `.dvc` files — a genuine leaf, **not** a gap |
| `no_access` | Could not read the repo |
| `not_found` | Repository does not exist |
| `clone_failed` | Network, timeout, or other git error |

Depth-truncated repos are **not** counted as gaps — a `--depth` limit is
something you asked for, and reporting it as a gap would bury real access
problems in expected noise. They are reported separately and marked `...`.

> **Note:** over SSH, GitHub returns the same `Repository not found` error for a
> private repo you cannot read as for one that does not exist. Those cases are
> therefore reported together as `no_access`, with the raw git error preserved.

```bash
dt deps gaps
```

```
Access gaps (2 repos not fully resolved):

  no_access -- no read access (or the repo does not exist -- GitHub does not
  distinguish). Try: dt auth request --repo <url>
    myorg/private-thing
      ERROR: Repository not found.

  The graph below these repos is incomplete.
```

`--strict` exits non-zero when any gap exists, for use in CI.

## See also

- [`dt import`](import.md) — create the imports this command reports on
- [`dt update`](update.md) — update imports to a new source revision
- [`dt tmp`](tmp.md) — manage the clones `dt deps graph` creates
- [`dt auth`](auth.md) — request access to repos reported as gaps
- [`dt summary`](summary.md) — `dag.md` (stages within this repo) and
  `repo-dag.md` (imports between repos, via `--repo-dag`)
