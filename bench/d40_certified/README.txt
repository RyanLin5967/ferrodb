D40 certification on the merged tip
===================================
Runner: tools/verify-suite.sh, not a bare cargo test. It rebuilds examples
first (refusing if that fails), runs --lib plus every tests/*.rs by name,
requires a `test result:` verdict AFTER each target's own marker, refuses a
zero-collected run, and re-reads HEAD and the dirty count at the end so a tree
that moved during the run cannot be recorded as green.

  VERIFY_OUT=/tmp/d40_verify_out3 VERIFY_MODE=per-target VERIFY_TIMEOUT=1800 \
    timeout 10800 tools/verify-suite.sh D40-final

CERTIFIED GREEN — runner's own summary, verbatim (RUNNER.txt, suite.log):

  D40-final: mode=per-target rc=0 passed=2048 failed=0 build_errors=0 head=c49e605 log=/tmp/d40_verify_out3/suite-D40-final.log
  D40-final: go rc=0 passed=97 failed=0 log=/tmp/d40_verify_out3/suite-D40-final.go.log
  RUNNER_EXIT=0

105 targets, 105 verdicts, 0 failed. head=c49e605 equals the merge commit this
certification is OF.

WHAT WAS MERGED, AND WHY THERE ARE TWO MERGES
=============================================
240a8e5  merged agent-isolation @ 375e5da (S22-BUFPOOL-LAND).
c49e605  merged agent-isolation @ be26b52, which had advanced by three commits
         DURING the first certification run: D39's two release-side lock-order
         commits (src/storage/page_latch.rs, src/buffer/buffer_pool.rs,
         tests/lock_order_runtime.rs) and a D35 doc retraction.

Both merges were clean, no conflicts. `tools/staleness.sh` called the branch
STALE-GREEN again after the first certification, and the newly arrived code was
lock-order work in the buffer pool — the exact class where "no file is edited by
both sides" is not a defence, because an inversion can be created by two files
that never appear in the same diff. So the merge-and-certify loop was run again
rather than landing a green taken against a base that had moved.

prev-240a8e5-RUNNER.txt is the superseded green at the earlier tip, kept as the
record that it passed there too:

  D40-final: mode=per-target rc=0 passed=2048 failed=0 build_errors=0 head=240a8e5

RUN 1 WAS RED, AND IS KEPT HERE ON PURPOSE
==========================================
run1-RED-RUNNER.txt / run1-RED-load-flake.log, taken at 240a8e5:

  D40-final: mode=per-target rc=101 passed=2047 failed=1 build_errors=2 head=240a8e5

One failure: integration_consensus_failover::
a_killed_leader_is_replaced_and_no_acknowledged_write_is_lost — "timed out
after 45s waiting for a new leader after the kill." The `build_errors=2` are
cargo's own `error: test failed` / `error: 1 target failed:` lines matching the
runner's `^error(\[|:)` heuristic, not compile errors; both greens report 0.

Why it is read as a load flake and not a D40 regression, in the order the
evidence was taken:

1. The failure is a 45-second election timeout in a 3-node failover fixture.
   Run 1 ran alongside another worktree's `cargo test -j 3`; load average was
   8.26 / 9.90 / 10.15 when it finished.
2. Re-run alone at the same commit: 3 for 3 green, 3.16s / 3.22s / 3.07s —
   against a 45s timeout. Two orders of magnitude of headroom when uncontended.
3. `git diff 375e5da...240a8e5 --name-only`: the D40 side touches
   src/branch/arena.rs (+4 lines) and src/branch/reaper.rs, and nothing else in
   src/. Nothing matching consensus|cluster|replic|raft is touched at all.
4. Two subsequent full runs on a quiet machine, at two different tips: 105/105,
   0 failures, both.

Kept rather than deleted because a certification whose first attempt was red is
a fact about this suite the next person running it should have. Both greens were
started only after confirming no other cargo was running and load had dropped —
that wait is part of the procedure here, not an optimisation.

THE HEADLINE, UNCHANGED BY EITHER MERGE
=======================================
Measured: 2031120 -> 0 descents at N=2016.
See bench/d40_README.txt and bench/d40_descent_curve.txt. That number carries
no disk confound and no load confound: it is an operation count, identical on
an idle machine and a thrashing one, and the BEFORE arm lands on the
pre-registered N(N-1)/2 exactly at every point.
