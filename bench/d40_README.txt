D40 — sweep_empty_extents was a global O(N) scan nested in a per-branch loop
============================================================================
Branch d40-reaper-quadratic. Design authority: SCALE-DESIGN.md section D40
(options 2 + 3). Measured 2026-09-18 on darwin/aarch64, debug profile.

PRIMARY EVIDENCE — THE CATALOG-DESCENT COUNT
============================================
The claim is a COMPLEXITY CLASS, O(branches x live_arenas) -> O(arenas
actually touched). An operation count proves a class directly and does not move
when the machine is loaded; a wall clock proves it only indirectly and does
move. This machine was loaded throughout, so the count is the evidence and the
clock below is the illustration.

Instrument: `TwoTierReaper::sweep_descents` counts the `catalog.get_raw` calls
the extent sweep makes — each one a `BPlusTreeManager::search` through the
shipped `TableBranchCatalog`, which is the work `sample`(1) found 74% of d19's
stacks inside. `sweep_visits` counts arenas examined, so the shape change
cannot be confused with the work moving somewhere unmeasured.

Both arms are the SAME counter at the SAME call site in the SAME build: the
BEFORE arm restores the pre-D40 call graph (drain_pending ends in the global
scan; reap_expired runs it unconditionally) and changes nothing else.
Harness: examples/d40_descent_curve.rs, driven by bench/d40_descent_curve.py.
Raw: d40_descent_curve.txt. Workload: d19's — fork off trunk, claim an extent,
write one novel page, abandon, reap_expired the lot. One arena per branch.

PRE-REGISTERED before the first measurement, as arithmetic from the call graph:
reap_expired reaps N branches; each reap frees its own extent wholesale then
drains, and the drain scans every arena still live, which after the i-th reap
is N-i-1. Sum over i in [0,N) of (N-i-1) = N(N-1)/2.

       N    descents BEFORE   predicted N(N-1)/2   ratio per 2x N   descents AFTER   visits AFTER
     125             7,750                7,750                -                0            125
     250            31,125               31,125            x4.02                0            250
     500           124,750              124,750            x4.01                0            500
   1,000           499,500              499,500            x4.00                0          1,000
   2,016         2,031,120            2,031,120            x4.07                0          2,016

BEFORE lands on the predicted N(N-1)/2 EXACTLY at every point, including
2,031,120 at d19's 2016-branch arm against a pre-registration of ~2.03M. The
x4 per doubling is the signature of the quadratic. AFTER is ZERO descents and
N visits: the fast path frees each extent wholesale, so the arena the drain is
seeded with is already gone and the sweep has nothing left to ask the catalog.

That zero is not an unfired detector. The same counter, same build, same call
site produced 2,031,120 on the other arm, and
`reaping_n_branches_costs_no_catalog_descent_per_branch` forces the counter to
move (via the global scan) inside the same test that reads a zero off it.

SECONDARY — THE WALL CLOCK, TAKEN ON A LOADED MACHINE
=====================================================
tests/d19_leak_is_a_race.rs, /usr/bin/time -p, back to back, debug profile.
The BEFORE binary was built from src at c05c1f0 and copied aside; `nm`
confirmed it carries `sweep_empty_extents` and not `sweep_touched_extents`.

                    BEFORE (c05c1f0)      AFTER (7bc866a)
  test wall            834.54 s              198.16 s      4.21x
  real                 834.78 s              198.38 s
  user                 541.70 s               17.23 s     31.4x
  sys                    8.59 s                3.87 s

  Later AFTER runs: 178.89 s, 171.85 s, 176.74 s (user 17.87 s) at 4ae32a8.
  Raw: d40_d19_before.txt, d40_d19_after.txt.

Both runs carry a large non-CPU component the fix does not touch (BEFORE 284 s,
AFTER 177 s), which holds the AFTER wall clock up, so 4.21x wall is a FLOOR.
UNVERIFIED as to cause: the volume was at 100% capacity (2.2 GiB free) during
both runs and other builds were running. Not isolated. This is exactly why the
descent count above, not this table, is the evidence.

BEHAVIOUR IS UNCHANGED, which is the point. Identical leak counts and identical
reaped counts on all five arms, every run, both before and after:
  [table-1t]  1000 branches, reaped 1000, LEAK=0
  [table-8t]  1000 branches, reaped 1000, LEAK=0
  [log-8t]    1000 branches, reaped 1000, LEAK=0
  [table-16t] 2000 branches, reaped 2000, LEAK=0
  [table-32t] 2016 branches, reaped 2016, LEAK=0
d19 was NOT edited. It can still fail; it reports the same numbers.

THE FIRE-CHECK — d40_fire_check.py / d40_fire_check.txt
=======================================================
Nine mutations, each aimed at the exact window one D40 test claims to guard,
each run against the whole `branch::reaper` suite on both catalogs, with src/
proved byte-identical to the commit before and after every one. Restore is
`git checkout <SHA> --`, never `git checkout --`.

  M1 collector is a no-op                              KILLED (4 tests)
  M2 collector ignores whether the owner is gone       KILLED (1)
  M3 collector ignores whether the extent is empty     KILLED (4)
  M4 open/recovery no longer collects orphans          KILLED (1)
  M5 cadence gate removed                              KILLED (1)
  M6 reap stops seeding the sweep with its own arenas  KILLED (2)
  M7 drain stops recording the arenas it released into KILLED (2)
  M8 the narrowed sweep never runs at all              KILLED (3)
  M9 narrowed sweep is the global scan again           KILLED (2)

M6 SURVIVED THE FIRST PASS, against the full D40 suite including the
reserved-page residue check written specifically to catch it. The uncovered
case: a slow-path reap whose pages were ALL born after its child forked. The
interval rule releases every one of them immediately, nothing is parked, the
pending-free log stays empty, `drain_pending` accumulates no arena id at all,
and `reap`'s `own_arenas` seed is then the only thing in the process that can
name the extent the reap just emptied. Every pre-existing slow-path test in the
tree writes its pages BEFORE forking the child, which parks them all and hides
the case. Closed by
`a_slow_path_reap_that_parks_nothing_still_gives_its_extents_back` (cf1f913),
after which M6 is killed.

RESERVED PAGES — the ledger D40 states its exit criterion in
============================================================
tests/d40_reserved_pages_return_to_baseline.rs runs d19's workload and asks the
number `sweep_empty_extents` existed to move, then runs the global scan
afterwards on the same state as a residue check. It reaps branch by branch
rather than through `reap_expired`, because `reap_expired` ends in the orphan
collector and letting it run first would mop up the very residue being
measured. Both arms (flat 1000-branch fanout; 40 six-deep forked chains) return
reserved pages to baseline with residue 0.

REGRESSION SCOPE ACTUALLY MEASURED
==================================
  cargo test --lib                      1346 passed, 0 failed, 3 ignored
  17 reaper-facing integration targets   140 passed, 0 failed (d40_reaper_targets.txt)
  the two per-method durability guards named in the brief, by full path:
    branch::arena::tests::parking_pages_by_the_interval_rule_reaches_the_durable_map  ok
    branch::arena::tests::putting_the_pending_log_back_reaches_the_durable_map        ok

NOT measured: the other 86 integration targets.

TWO ENVIRONMENT TRAPS THAT EACH COST A RUN
==========================================
1. `integration_server_reaps` fails 5 tests at tests/integration_server_reaps.rs:109
   whenever the lib has been rebuilt more recently than examples/ — that is its
   OWN staleness guard firing, not a regression. `cargo build --examples` first.
   It bit twice here, once after the fire-check and once after the curve script.
2. A concurrent `wt new d40-reaper-quadratic` deleted this worktree's target/
   mid-run and destroyed the first BEFORE baseline (the one above is a rebuild).
   Source was never touched. The directory appears before the hardlink farm of
   target/ finishes, so a tree can look ready while it is still being populated.
