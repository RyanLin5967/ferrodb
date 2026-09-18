D41 — certification evidence for "remove `put` from the BranchCatalog trait".

THE NAMING RULE THIS DIRECTORY FOLLOWS (e54da38). The UNPREFIXED name is always
the run the claim rests on. Superseding a run means RENAMING the old one out of
the way with a `prev-<sha>-` prefix, never adding a longer name beside it — two
naming schemes in one directory means the shorter name wins the reader's
attention while carrying the worse record, which is the defect e54da38 had to
fix inside D40's own evidence directory.

WHAT THE CLAIM RESTS ON
  RUNNER.txt / suite.log / suite.go.log
  D41: mode=per-target rc=0 passed=2062 failed=0 build_errors=0 head=ae126ee
  D41: go rc=0 passed=97 failed=0
  107 verdicts, 0 FAILED, 2 ignored, 0 ENOSPC. Finished 2026-09-18 08:27Z.

  head=ae126ee IS the D41 code commit, byte for byte. Verify it the way it must
  be verified — from the raw log's own summary line, not from any commit body:

      head -1 bench/d41_certified/suite.log     # "=== per-target sweep: ..."
      cat bench/d41_certified/RUNNER.txt        # head=ae126ee
      git rev-parse --short ae126ee

  `tools/staleness.sh` compares BRANCH to BASE and structurally cannot check
  SUITE to BRANCH, so this field is the one thing only a reader of this file
  can confirm.

THIS COMMIT IS bench/ ONLY. It carries zero delta under src/, tests/,
examples/ or tools/, so the tree certified above is the tree of its parent and
of every source file this branch changes:

      git diff --stat ae126ee HEAD -- src tests examples tools benches   # empty

SUPERSEDED RUN — KEPT, NOT DELETED
  prev-9f27e94-RUNNER.txt / prev-9f27e94-suite.log / prev-9f27e94-suite.go.log
  D41: mode=per-target rc=0 passed=2062 failed=0 build_errors=0 head=9f27e94
  Same counts, also green, finished 2026-09-18 08:13Z.

  It was superseded ninety seconds after it finished, by an amend that corrected
  ONE comment: the compare-and-set note in `TwoTierReaper::reap` claimed that a
  branch somebody else had already started reaping is "refused", which is false
  for the `resume_interrupted_reaps` ordering — a branch already `Reaping` when
  it is READ lands as `expect == to`, a no-op write, which is what lets the
  resume proceed. The refusal only fires on a branch that MOVED between the
  `get_raw` and the write.

  The delta between the two runs' trees is comment-only, and that is mechanical
  rather than asserted:

      git diff 9f27e94 ae126ee | grep -E '^[-+]' | grep -vE '^[-+]{3}' \
        | sed 's/^[-+]//' | grep -vE '^[[:space:]]*$' | grep -vE '^[[:space:]]*//' | wc -l
      # 0

  The re-run was still done rather than argued from that, because a seven-minute
  run turns "is this delta inert?" from a judgement into an identity. The
  superseded log is kept because it is a real measurement; a green that stopped
  being canonical is not a green that stopped being true.

WHAT THE MACHINE WAS DOING, since it bears on one target
  The certified run queued 360s behind D35-C1's suite lock and started at
  12:20:34Z. `integration_consensus_failover::a_killed_leader_is_replaced_and_no
  _acknowledged_write_is_lost` — a Raft election over TCP with a 45s wall-clock
  wait, which this project has seen produce four false REDs under load — PASSED
  in 2.93s in the certified run. It was not a factor.

  `build_errors=0` here is genuine rather than a parser artefact. verify-suite's
  `^error(\[|:)` grep also catches cargo's own "error: test failed", so a single
  test failure prints build_errors=2 and reads as "the tree stopped compiling".
  Checked directly: `grep -c 'error\[E' suite.log` is 0 and
  `grep -c 'could not compile' suite.log` is 0.

THE TEST COUNT, ACCOUNTED FOR RATHER THAN QUOTED
  2056 (D28's landed count) -> 2062. Exactly +4 from
  tests/d41_narrow_ops_close_the_window.rs and +2 from un-`#[ignore]`ing
  `collapse_discards_a_lease_renewal_that_lands_on_its_re_read`, which the D19
  macro instantiates once per catalog. No test moved from passing to ignored:
  the 2 ignored are pre-existing and elsewhere.

FIRE-CHECK — WHY THESE TESTS ARE WORTH THE LINE THEY PRINT
  All three reproductions were run against the pre-D41 shape before being
  trusted (a temporary whole-record writer on the trait plus the old
  get/mutate/write-back at all three call sites, reverted afterwards). The two
  new ones failed correctly and both controls passed. The collapse reproduction
  did NOT: its first rebuild — fire on every `get`, always to `u64::MAX` —
  PASSED against the mutant, i.e. it would have certified the fix while being
  blind to the defect, because D13b's re-read picks up any keepalive landing
  before it. The working version publishes a DISTINCT deadline per read and
  asserts the last one published is the one in force; that observation names no
  read count, so it survives D41 removing a read, and it fails against the old
  code on both catalogs. The full record is in ae126ee's commit body.
