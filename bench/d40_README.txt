D40 — sweep_empty_extents was a global O(N) scan nested in a per-branch loop
============================================================================
Branch d40-reaper-quadratic. Design authority: SCALE-DESIGN.md section D40
(options 2 + 3). Measured 2026-09-18 on darwin/aarch64, debug profile.

THE MATCHED PAIR — tests/d19_leak_is_a_race.rs
----------------------------------------------
Same test, same machine, same debug profile, run back to back. The BEFORE
binary was built from src at c05c1f0 and copied aside (`nm` confirmed it
contains `sweep_empty_extents` and not `sweep_touched_extents`); the AFTER run
is `cargo test --test d19_leak_is_a_race`. /usr/bin/time -p is the instrument.

                    BEFORE (c05c1f0)      AFTER (7bc866a)
  test wall            834.54 s              198.16 s      4.21x
  real                 834.78 s              198.38 s
  user                 541.70 s               17.23 s     31.4x
  sys                    8.59 s                3.87 s

  Repeat AFTER runs at cf1f913: 178.89 s and 171.85 s.
  Raw: d40_d19_before.txt, d40_d19_after.txt, d40_reaper_targets.txt.

USER TIME IS THE NUMBER THAT MEANS SOMETHING HERE. The defect burned CPU —
one B+tree descent per live arena, per reaped branch — and that is what went
from 541.70 s to 17.23 s. Both runs also carry a large non-CPU component
(BEFORE 834.78 - 550.29 = 284 s; AFTER 198.38 - 21.10 = 177 s) which the fix
does not touch and which therefore holds the AFTER wall clock up. UNVERIFIED
as to cause: the volume was at 100% capacity (2.2 GiB free) throughout both
runs, which is a plausible confound and was not isolated. The 4.21x wall
figure is consequently a FLOOR, not the shape change.

BEHAVIOUR IS UNCHANGED, which is the point. Both runs report identical leak
counts and identical reaped counts on all five arms:
  [table-1t]  1000 branches, reaped 1000, LEAK=0
  [table-8t]  1000 branches, reaped 1000, LEAK=0
  [log-8t]    1000 branches, reaped 1000, LEAK=0
  [table-16t] 2000 branches, reaped 2000, LEAK=0
  [table-32t] 2016 branches, reaped 2016, LEAK=0
d19 was NOT edited. It can still fail; it reports the same numbers.

THE FIRE-CHECK — d40_fire_check.py / d40_fire_check.txt
-------------------------------------------------------
Eight mutations, each aimed at the exact window one D40 test claims to guard,
each run against the whole `branch::reaper` suite (both catalogs), with the
tree proved byte-identical to the commit before and after every one. Restore
is `git checkout <SHA> --`, never `git checkout --`.

  M1 collector is a no-op                              KILLED (3 tests)
  M2 collector ignores whether the owner is gone       KILLED (1)
  M3 collector ignores whether the extent is empty     KILLED (4)
  M4 open/recovery no longer collects orphans          KILLED (1)
  M5 cadence gate removed                              KILLED (1)
  M6 reap stops seeding the sweep with its own arenas  KILLED (2)
  M7 drain stops recording the arenas it released into KILLED (2)
  M8 the narrowed sweep never runs at all              KILLED (3)

M6 SURVIVED THE FIRST PASS, against the full D40 suite including the
reserved-page residue check written specifically to catch it. The uncovered
case: a slow-path reap whose pages were ALL born after its child forked. The
interval rule releases every one of them immediately, nothing is parked, the
pending-free log stays empty, `drain_pending` accumulates no arena id at all,
and `reap`'s `own_arenas` seed is then the only thing in the process that can
name the extent the reap just emptied. Every pre-existing slow-path test in
the tree writes its pages BEFORE forking the child, which parks them all and
hides the case. Closed by
`a_slow_path_reap_that_parks_nothing_still_gives_its_extents_back` (commit
cf1f913), after which M6 is killed.

RESERVED PAGES — the ledger D40 states its exit criterion in
-------------------------------------------------------------
tests/d40_reserved_pages_return_to_baseline.rs runs d19's workload and asks
the number `sweep_empty_extents` existed to move, then runs the global scan
afterwards on the same state as a residue check. It reaps branch by branch
rather than through `reap_expired`, because `reap_expired` ends in the orphan
collector and letting it run first would mop up the very residue being
measured. Both arms (flat 1000-branch fanout; 40 six-deep forked chains)
return reserved pages to baseline with residue 0.

REGRESSION SCOPE ACTUALLY MEASURED
----------------------------------
  cargo test --lib                     1344 passed, 0 failed, 3 ignored
  17 reaper-facing integration targets  140 passed, 0 failed (d40_reaper_targets.txt)
  the two per-method durability guards named in the brief, by full path:
    branch::arena::tests::parking_pages_by_the_interval_rule_reaches_the_durable_map  ok
    branch::arena::tests::putting_the_pending_log_back_reaches_the_durable_map        ok

NOT measured: the other 86 integration targets.

A NOTE ON THE ENVIRONMENT, because it cost a measurement
--------------------------------------------------------
A concurrent `wt new d40-reaper-quadratic` from a second session deleted this
worktree's target/ mid-run and destroyed the first BEFORE baseline (the one
reported above is a rebuild). Source files were never touched. The disk was at
100% capacity throughout.
