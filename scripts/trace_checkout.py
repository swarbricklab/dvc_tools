#!/usr/bin/env python
"""
Instrument `dvc checkout` to measure time spent in each phase.

Usage:
    python trace_checkout.py [targets...]

Run this from inside a DVC repo (e.g. your project directory on NCI).
It monkey-patches key DVC functions with timing wrappers, then runs checkout.
"""

import sys
import time
from contextlib import contextmanager
from functools import wraps

# ── Timing infrastructure ───────────────────────────────────────────

_timings: list[tuple[str, float]] = []
_depth = 0


@contextmanager
def timed_section(label):
    """Context manager that records wall-clock time for a labelled section."""
    global _depth
    indent = "  " * _depth
    print(f"{indent}⏱  START  {label}", flush=True)
    _depth += 1
    t0 = time.monotonic()
    try:
        yield
    finally:
        elapsed = time.monotonic() - t0
        _depth -= 1
        _timings.append((label, elapsed))
        print(f"{indent}⏱  END    {label}  ({elapsed:.2f}s)", flush=True)


def timed(label):
    """Decorator that wraps a function with timing."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            with timed_section(label):
                return fn(*args, **kwargs)
        return wrapper
    return decorator


def print_summary():
    print("\n" + "=" * 60)
    print("TIMING SUMMARY")
    print("=" * 60)
    for label, elapsed in _timings:
        print(f"  {elapsed:8.2f}s  {label}")
    total = sum(t for _, t in _timings if not any(
        other_label != label and label.startswith("  ") 
        for other_label, _ in _timings
    ))
    print("=" * 60)


# ── Monkey-patches ──────────────────────────────────────────────────

def patch_all():
    """Apply all timing patches before running checkout."""

    # 1. Patch _remove_unused_links
    import dvc.repo.checkout as checkout_mod
    _orig_remove_unused_links = checkout_mod._remove_unused_links

    @timed("_remove_unused_links")
    def _patched_remove_unused_links(repo):
        return _orig_remove_unused_links(repo)

    checkout_mod._remove_unused_links = _patched_remove_unused_links

    # 2. Patch index_from_targets
    import dvc.repo.index as index_mod
    _orig_index_from_targets = index_mod.index_from_targets

    @timed("index_from_targets")
    def _patched_index_from_targets(*args, **kwargs):
        return _orig_index_from_targets(*args, **kwargs)

    index_mod.index_from_targets = _patched_index_from_targets
    # Also patch it where checkout imports it
    checkout_mod.index_from_targets = _patched_index_from_targets

    # 3. Patch build_data_index
    _orig_build_data_index = index_mod.build_data_index

    @timed("build_data_index (Building workspace index)")
    def _patched_build_data_index(*args, **kwargs):
        return _orig_build_data_index(*args, **kwargs)

    index_mod.build_data_index = _patched_build_data_index

    # 4. Patch view.data["repo"] access — intercept the `data` cached_property
    #    on Index (not IndexView). This is where _load_data_from_outs runs.
    _orig_load_data_from_outs = index_mod._load_data_from_outs

    @timed("_load_data_from_outs (loading .dir manifests)")
    def _patched_load_data_from_outs(*args, **kwargs):
        return _orig_load_data_from_outs(*args, **kwargs)

    index_mod._load_data_from_outs = _patched_load_data_from_outs

    # Also patch the data_index access (DataIndex loading from SQLite)
    from dvc_data.index import DataIndex
    _orig_has_node = DataIndex.has_node
    _data_index_stats = {"has_node_calls": 0, "hits": 0}

    @wraps(_orig_has_node)
    def _patched_has_node(self, prefix):
        _data_index_stats["has_node_calls"] += 1
        result = _orig_has_node(self, prefix)
        if result:
            _data_index_stats["hits"] += 1
        return result

    DataIndex.has_node = _patched_has_node
    patch_all._data_index_stats = _data_index_stats

    # 5. Patch compare
    import dvc_data.index.checkout as data_checkout_mod
    _orig_compare = data_checkout_mod.compare

    @timed("compare (Comparing indexes)")
    def _patched_compare(*args, **kwargs):
        return _orig_compare(*args, **kwargs)

    data_checkout_mod.compare = _patched_compare

    # 6. Patch apply
    _orig_apply = data_checkout_mod.apply

    @timed("apply (Applying changes)")
    def _patched_apply(*args, **kwargs):
        return _orig_apply(*args, **kwargs)

    data_checkout_mod.apply = _patched_apply

    # 7. Patch hash_file (the expensive per-file operation)
    import dvc_data.hashfile.hash as hash_mod
    import dvc_data.index.build as build_mod
    _orig_hash_file = hash_mod.hash_file
    _hash_file_stats = {"calls": 0, "cache_hits": 0, "total_time": 0.0}

    @wraps(_orig_hash_file)
    def _patched_hash_file(path, fs, name, state=None, **kwargs):
        t0 = time.monotonic()
        result = _orig_hash_file(path, fs, name, state=state, **kwargs)
        elapsed = time.monotonic() - t0
        _hash_file_stats["calls"] += 1
        _hash_file_stats["total_time"] += elapsed
        # If it was very fast (<1ms), likely a state cache hit
        if elapsed < 0.001:
            _hash_file_stats["cache_hits"] += 1
        return result

    hash_mod.hash_file = _patched_hash_file
    # Also patch where it's imported directly
    build_mod.hash_file = _patched_hash_file

    # Stash for summary
    patch_all._hash_file_stats = _hash_file_stats

    # 8. Patch State.get / State.save to count DB operations
    from dvc_data.hashfile.state import State
    _orig_state_get = State.get
    _state_stats = {"get_calls": 0, "get_hits": 0, "save_calls": 0}

    @wraps(_orig_state_get)
    def _patched_state_get(self, path, fs, **kwargs):
        _state_stats["get_calls"] += 1
        result = _orig_state_get(self, path, fs, **kwargs)
        if result and result[0] is not None:
            _state_stats["get_hits"] += 1
        return result

    State.get = _patched_state_get

    _orig_state_save = State.save
    @wraps(_orig_state_save)
    def _patched_state_save(self, path, fs, hash_info, **kwargs):
        _state_stats["save_calls"] += 1
        return _orig_state_save(self, path, fs, hash_info, **kwargs)

    State.save = _patched_state_save

    _orig_state_save_link = State.save_link
    _state_link_stats = {"save_link_calls": 0, "get_unused_links_time": 0.0}

    @wraps(_orig_state_save_link)
    def _patched_state_save_link(self, path, fs):
        _state_link_stats["save_link_calls"] += 1
        return _orig_state_save_link(self, path, fs)

    State.save_link = _patched_state_save_link

    _orig_get_unused = State.get_unused_links
    @wraps(_orig_get_unused)
    def _patched_get_unused(self, used, fs):
        t0 = time.monotonic()
        result = _orig_get_unused(self, used, fs)
        _state_link_stats["get_unused_links_time"] = time.monotonic() - t0
        return result

    State.get_unused_links = _patched_get_unused

    patch_all._state_stats = _state_stats
    patch_all._state_link_stats = _state_link_stats

    # 9. Patch Repo() construction
    from dvc.repo import Repo
    _orig_repo_init = Repo.__init__

    @timed("Repo.__init__ (repository construction)")
    def _patched_repo_init(self, *args, **kwargs):
        return _orig_repo_init(self, *args, **kwargs)

    Repo.__init__ = _patched_repo_init

    # 10. Patch odb.exists / fs.exists to count stat calls
    from dvc_data.hashfile.db.local import LocalHashFileDB
    _orig_oids_exist = LocalHashFileDB.oids_exist
    _odb_stats = {"oids_exist_calls": 0, "total_oids_checked": 0}

    @wraps(_orig_oids_exist)
    def _patched_oids_exist(self, oids, jobs=None, progress=None):
        _odb_stats["oids_exist_calls"] += 1
        oids_list = list(oids)
        _odb_stats["total_oids_checked"] += len(oids_list)
        return _orig_oids_exist(self, oids_list, jobs=jobs, progress=progress)

    LocalHashFileDB.oids_exist = _patched_oids_exist
    patch_all._odb_stats = _odb_stats


def print_detailed_summary():
    print_summary()

    hf = getattr(patch_all, '_hash_file_stats', {})
    if hf.get("calls"):
        print(f"\nhash_file():")
        print(f"  Total calls:      {hf['calls']}")
        print(f"  Fast (<1ms):      {hf['cache_hits']}  (likely state cache hits)")
        print(f"  Slow (>=1ms):     {hf['calls'] - hf['cache_hits']}  (full MD5 compute)")
        print(f"  Total time:       {hf['total_time']:.2f}s")

    ss = getattr(patch_all, '_state_stats', {})
    if ss.get("get_calls"):
        print(f"\nState DB (hash cache):")
        print(f"  state.get() calls:  {ss['get_calls']}")
        print(f"  state.get() hits:   {ss['get_hits']}")
        print(f"  state.save() calls: {ss['save_calls']}")

    sl = getattr(patch_all, '_state_link_stats', {})
    if sl.get("save_link_calls") or sl.get("get_unused_links_time"):
        print(f"\nState links:")
        print(f"  save_link() calls:        {sl['save_link_calls']}")
        print(f"  get_unused_links() time:  {sl['get_unused_links_time']:.2f}s")

    od = getattr(patch_all, '_odb_stats', {})
    if od.get("oids_exist_calls"):
        print(f"\nODB (cache) stat calls:")
        print(f"  oids_exist() calls:     {od['oids_exist_calls']}")
        print(f"  Total OIDs checked:     {od['total_oids_checked']}")

    di = getattr(patch_all, '_data_index_stats', {})
    if di.get("has_node_calls"):
        print(f"\nDataIndex:")
        print(f"  has_node() calls:       {di['has_node_calls']}")
        print(f"  has_node() hits:        {di['hits']}  (cached in SQLite)")

    print()


# ── Main ────────────────────────────────────────────────────────────

def main():
    targets = sys.argv[1:] or None

    print("=" * 60)
    print("DVC CHECKOUT TIMING TRACE")
    print("=" * 60)
    print(f"Targets: {targets or '(all)'}")
    print(f"CWD: {sys.path[0] if sys.path else 'unknown'}")
    print()

    # Apply patches BEFORE importing Repo
    patch_all()

    from dvc.repo import Repo

    overall_t0 = time.monotonic()

    with timed_section("Total dvc checkout"):
        with timed_section("Repo() open"):
            repo = Repo()

        with timed_section("repo.checkout()"):
            repo.checkout(
                targets=targets,
                force=True,
                allow_missing=True,
            )

        repo.close()

    overall_elapsed = time.monotonic() - overall_t0

    print_detailed_summary()
    print(f"TOTAL WALL TIME: {overall_elapsed:.2f}s")


if __name__ == "__main__":
    main()
