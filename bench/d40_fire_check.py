#!/usr/bin/env python3
"""D40 fire-check: break the fix on purpose, one window at a time, and prove the tests SEE it.

A green suite that has never been made to fail is an untested detector. Each mutation below is
aimed at the exact window one of the D40 tests claims to guard; the run is a PASS only if every
mutation is killed by the test named against it, and only if the tree is byte-identical to the
committed fix before and after each one.

Restore is `git checkout <SHA> --`, never `git checkout --`: the latter restores the INDEX.
"""
import subprocess, sys, os

REPO = "/Users/idide/wt/ferrodb-d40-reaper-quadratic"
SHA = "cf1f913"
REAPER = os.path.join(REPO, "src/branch/reaper.rs")

# (name, file, old, new, tests that MUST fail)
MUTATIONS = [
    ("M1 collector is a no-op", REAPER,
     "    pub fn collect_orphaned_extents(&self) -> Result<u32, FerroError> {\n        let mut freed = 0u32;",
     "    pub fn collect_orphaned_extents(&self) -> Result<u32, FerroError> {\n        if true { return Ok(0); }\n        let mut freed = 0u32;",
     ["the_crash_orphan_collector_fires_on_state_the_narrowed_sweep_cannot_see",
      "a_crash_orphaned_extent_is_collected_when_the_store_is_reopened",
      "the_orphan_collector_runs_on_the_first_tick_then_only_once_per_cadence"]),

    ("M2 collector ignores whether the owner is gone", REAPER,
     "        owner_gone && self.store.extent_is_empty(arena)",
     "        let _ = owner_gone;\n        self.store.extent_is_empty(arena)",
     ["the_orphan_collector_refuses_a_live_owner_and_refuses_a_non_empty_extent"]),

    ("M3 collector ignores whether the extent is empty", REAPER,
     "        owner_gone && self.store.extent_is_empty(arena)",
     "        owner_gone",
     ["the_orphan_collector_refuses_a_live_owner_and_refuses_a_non_empty_extent"]),

    ("M4 open/recovery no longer collects orphans", REAPER,
     "        self.collect_orphaned_extents()?;\n        Ok(done)",
     "        Ok(done)",
     ["a_crash_orphaned_extent_is_collected_when_the_store_is_reopened"]),

    ("M5 cadence gate removed (collector runs every tick)", REAPER,
     "        if last != ORPHAN_SWEEP_NEVER && now_millis.saturating_sub(last) < ORPHAN_SWEEP_INTERVAL_MS\n        {\n            return Ok(0);\n        }",
     "        let _ = last;",
     ["the_orphan_collector_runs_on_the_first_tick_then_only_once_per_cadence"]),

    ("M6 reap stops seeding the sweep with its own arenas", REAPER,
     "        freed += self.drain_pending_seeded(own_arenas)?;",
     "        let _ = &own_arenas;\n        freed += self.drain_pending_seeded(BTreeSet::new())?;",
     ["the_narrowed_sweep_leaves_the_global_scan_nothing_to_find",
      "a_slow_path_reap_that_parks_nothing_still_gives_its_extents_back"]),

    ("M8 the narrowed sweep never runs at all", REAPER,
     "        self.sweep_touched_extents(&touched)?;",
     "        let _ = &touched;",
     ["the_narrowed_sweep_leaves_the_global_scan_nothing_to_find",
      "a_slow_path_reap_that_parks_nothing_still_gives_its_extents_back",
      "slow_path_pins_pages_a_live_child_can_still_see_then_releases_them"]),

    ("M7 drain stops recording the arenas it released into", REAPER,
     "                    touched.insert(pf.arena_id);",
     "                    // mutated: not recorded",
     ["the_narrowed_sweep_leaves_the_global_scan_nothing_to_find"]),
]


def run(cmd, **kw):
    return subprocess.run(cmd, cwd=REPO, shell=True, capture_output=True, text=True, **kw)


def tree_is_clean():
    return run("git diff HEAD --stat").stdout.strip() == ""


def restore():
    run(f"git checkout {SHA} -- src/branch/reaper.rs src/branch/arena.rs")
    if not tree_is_clean():
        print("FATAL: tree did not restore to the committed fix", file=sys.stderr)
        sys.exit(2)


results = []
restore()
for name, path, old, new, must_fail in MUTATIONS:
    if not tree_is_clean():
        print(f"FATAL: tree dirty before {name}", file=sys.stderr)
        sys.exit(2)
    src = open(path).read()
    if src.count(old) != 1:
        print(f"FATAL: {name}: anchor matched {src.count(old)} times, expected 1", file=sys.stderr)
        sys.exit(2)
    open(path, "w").write(src.replace(old, new))

    r = run("timeout 1800 cargo test --lib branch::reaper 2>&1")
    out = r.stdout + r.stderr
    compiled = "error[E" not in out and "error: could not compile" not in out
    failed = set()
    for line in out.splitlines():
        if line.startswith("test ") and line.rstrip().endswith("FAILED"):
            failed.add(line.split("...")[0].strip().split("::")[-1])
    killed_by = {t: (t in failed) for t in must_fail}
    verdict = "KILLED" if compiled and all(killed_by.values()) else ("BUILD-FAILED" if not compiled else "SURVIVED")
    results.append((name, verdict, killed_by, sorted(failed)))
    print(f"{verdict:12} {name}")
    for t, k in killed_by.items():
        print(f"             {'kills' if k else 'MISSED'}  {t}")
    sys.stdout.flush()
    restore()

print("\n==== D40 FIRE-CHECK SUMMARY ====")
bad = [r for r in results if r[1] != "KILLED"]
for name, verdict, killed_by, failed in results:
    print(f"{verdict:12} {name}  (tests that failed: {len(failed)})")
print("RESULT:", "ALL MUTATIONS KILLED" if not bad else f"{len(bad)} MUTATION(S) NOT KILLED")
sys.exit(0 if not bad else 1)
