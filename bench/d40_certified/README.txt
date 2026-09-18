# D40 CERTIFICATION — THREE RUNS. THIS DIRECTORY'S CANONICAL NAMES CARRY THE LANDED ONE.

  RUNNER.txt / suite.log / suite.go.log      head=c49e605  <- WHAT THE LANDING RESTS ON
  prev-240a8e5-*                             head=240a8e5  green, but at a tip EARLIER than the landed one
  run1-RED-*                                 head=240a8e5  RED on a load-sensitive Raft flake

⚠ THIS LAYOUT IS A CORRECTION. As first landed, the canonical names held the SUPERSEDED 240a8e5
run, and the old README asserted "head=240a8e5 equals the merge commit" -- false for the tip that
was landed. A reader opening RUNNER.txt, the name you reach for first, would have concluded the
landing rested on a tip that is not the landed tip. That is exactly the shape refused twice in S22
(ccb9eea recorded head=2450722), sitting inside D40's own evidence directory.

Cause, recorded because it will recur: the landing session added its artifact under a `run3-LANDED-`
prefix ALONGSIDE the existing canonical files rather than replacing them, and the merge brought the
older canonically-named files in from the branch. Two naming schemes in one directory, and the
better-known name won the reader's attention while carrying the worse record. Found by the D40
builder reading what actually landed rather than the commit subject.

⇒ RULE: in an evidence directory, the UNPREFIXED name must always be the run the claim rests on.
Supersede by RENAMING the old one out of the way, never by adding a longer name beside it.

# RUN 3 — THE RUN D40 ACTUALLY LANDED ON. head=c49e605.

The two runs already in this directory are NOT the landing evidence:
  run1  rc=101  head=240a8e5  RED on a known load-sensitive Raft flake
  RUNNER.txt (run 2)  rc=0  head=240a8e5  green, but against a tip EARLIER than the one landed

This one:
  D40-final: mode=per-target rc=0 passed=2048 failed=0 build_errors=0 head=c49e605
  D40-final: go rc=0 passed=97 failed=0
  RUNNER_EXIT=0

Re-derived from the raw log by the landing session rather than read off RUNNER.txt:
  verdicts            105   (104 + this branch's tests/d40_reserved_pages_return_to_baseline.rs)
  FAILED verdicts       0
  passed sum         2048
  ENOSPC                0
  rustc compile errs    0
  go PASS lines        99   go FAIL 0

head=c49e605 is the tip that was landed, and agent-isolation was an ancestor of it, so
`git merge-tree agent-isolation c49e605` produced tree 7b62b4549edd4f4c0a4543b485d3767981a8e7c8
-- byte-identical to c49e605's own tree. The certified tree and the landed tree are the same
git object. That is only true because agent-isolation was frozen while this branch certified;
one more commit on the base would have produced a third tree no suite had seen.

Copied out of /tmp/d40_verify_out3/ because verify-suite.sh defaults VERIFY_OUT to a mktemp
dir, and a finished run whose log lives only in /tmp is one reboot from being unciteable.
That happened earlier the same day with S22 and cost real time.

================================================================================
THE BRANCH'S OWN README, KEPT VERBATIM BELOW RATHER THAN DISCARDED
================================================================================
Resolving this file's merge conflict by taking a side would have thrown away real
content either way: agent-isolation's copy (above) carries the CORRECTION about which
name holds the landed run, and the lane branch's copy (below) carries the runner
invocation, the verbatim summary and the two-merge history that the correction's
rewrite did not reproduce. Neither is a superset. Both are records of things that
happened, so both are kept and labelled.

Where the two disagree, the text ABOVE wins: it is the later record and it exists
because the text below asserted something false about which tip the landing rests on.

--- begin lane branch d40-reaper-quadratic, bench/d40_certified/README.txt ---
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
--- end lane branch copy ---
