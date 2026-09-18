D40 certification on the merged tip
===================================
Merged agent-isolation (375e5da, S22-BUFPOOL-LAND) into d40-reaper-quadratic.
Merge was clean, no conflicts, no file edited by both sides. Merge commit
240a8e5, which is the tip this certification is OF.

Runner: tools/verify-suite.sh, not a bare cargo test. It rebuilds examples
first (refusing if that fails), runs --lib plus every tests/*.rs by name,
requires a `test result:` verdict AFTER each target's own marker, refuses a
zero-collected run, and re-reads HEAD and the dirty count at the end so a tree
that moved during the run cannot be recorded as green.

  VERIFY_OUT=/tmp/d40_verify_out2 VERIFY_MODE=per-target VERIFY_TIMEOUT=1800 \
    timeout 10800 tools/verify-suite.sh D40-final

CERTIFIED GREEN — runner's own summary, verbatim (suite.log, suite.go.log):

  D40-final: mode=per-target rc=0 passed=2048 failed=0 build_errors=0 head=240a8e5 log=/tmp/d40_verify_out2/suite-D40-final.log
  D40-final: go rc=0 passed=97 failed=0 log=/tmp/d40_verify_out2/suite-D40-final.go.log
  RUNNER_EXIT=0

105 targets, 105 verdicts, 0 failed. head=240a8e5 equals the merge commit.

RUN 1 WAS RED, AND IS KEPT HERE ON PURPOSE
==========================================
run1-RED-RUNNER.txt / run1-RED-load-flake.log:

  D40-final: mode=per-target rc=101 passed=2047 failed=1 build_errors=2 head=240a8e5 log=/tmp/d40_verify_out/suite-D40-final.log

One failure: integration_consensus_failover::
a_killed_leader_is_replaced_and_no_acknowledged_write_is_lost — "timed out
after 45s waiting for a new leader after the kill." The `build_errors=2` are
cargo's own `error: test failed` / `error: 1 target failed:` lines matching the
runner's `^error(\[|:)` heuristic, not compile errors; run 2 reports 0.

Why it is read as a load flake and not as a D40 regression, in the order the
evidence was taken:

1. The failure is a 45-second election timeout in a 3-node failover fixture.
   Run 1 ran alongside another worktree's `cargo test -j 3`; load average was
   8.26 / 9.90 / 10.15 when run 1 finished.
2. Re-run alone at the same commit: 3 for 3 green, 3.16s / 3.22s / 3.07s —
   against a 45s timeout. Two orders of magnitude of headroom when the machine
   is not contended.
3. `git diff 375e5da...240a8e5 --name-only` shows the D40 side touches
   src/branch/arena.rs (+4 lines) and src/branch/reaper.rs and nothing else in
   src/. Nothing matching consensus|cluster|replic|raft is touched at all.
4. Run 2, on a quiet machine, same commit: 105/105, 0 failures.

Kept rather than deleted because a certification whose first attempt was red is
a fact about this suite that the next person running it should have. The claim
being made is run 2's green, at head=240a8e5.

THE HEADLINE, UNCHANGED BY THE MERGE
====================================
Measured: 2031120 -> 0 descents at N=2016.
See bench/d40_README.txt and bench/d40_descent_curve.txt. That number carries
no disk confound and no load confound: it is an operation count, identical on
an idle machine and a thrashing one, and the BEFORE arm lands on the
pre-registered N(N-1)/2 exactly at every point.
