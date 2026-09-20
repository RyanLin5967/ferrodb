D42 — certification evidence for "a 45 s expiry now reports WHETHER it is a verdict".

NAMING RULE THIS DIRECTORY FOLLOWS (e54da38): the UNPREFIXED name is always the run
the claim rests on. Superseding a run means RENAMING the old one out of the way with
a `prev-<sha>-` prefix, never adding a longer name beside it. There is only one run
here; nothing has been superseded.

WHAT THE CLAIM RESTS ON
  RUNNER.txt / suite.log / suite.go.log
  D42: mode=per-target rc=0 passed=2065 failed=0 build_errors=0 head=2045e97
  D42: go rc=0 passed=97 failed=0
  108 targets (--lib plus 107 integration targets), 0 FAILED. Finished 2026-09-18 16:16Z.

  head=2045e97 IS the D42 code commit. Verify it the way it must be verified — from the
  raw log's own summary line, not from any commit body:

      head -1 bench/d42_certified/suite.log     # "=== per-target sweep: ..."
      cat bench/d42_certified/RUNNER.txt        # head=2045e97
      git rev-parse --short 2045e97

  `tools/certify-head.sh bench/d42_certified 2045e97` printed:
      certify-head: OK — suite head=2045e97 == landing 2045e97

  `tools/staleness.sh` compares BRANCH to BASE and structurally cannot check SUITE to
  BRANCH, so this field is the one thing only a reader of this file can confirm.

THE COMMITS AFTER 2045e97 ARE bench/ ONLY, so the tree certified above is the tree of
every source file this branch changes:

      git diff --stat 2045e97 HEAD -- src tests examples tools   # empty

  They add this directory, plus the orphan fix and fire-check described below. Nothing
  under src/, tests/, examples/ or tools/ moved after the suite ran.

BASE AT CERTIFICATION TIME
  This branch was merged with `agent-isolation` at 8aeae20 before the suite ran, so the
  green covers the merged tree rather than a stale base. `tools/prepush.sh` also passes
  on it, including the Windows cross-check that compiles the classifier's cfg(not(unix))
  arm — the arm where signal C is unavailable and the verdict falls back to B alone.

WHAT THIS GREEN DOES NOT SAY
  It says the suite passes. It does NOT say the D42 classifier works — a classifier that
  never fires would also produce a green suite, because it only speaks on a timeout that
  a green run never reaches. The classifier's evidence is the two-directional falsifier,
  which lives outside this directory:

      bench/d42_verify_all.txt              all five arms, one lock, one machine state
      bench/d42_fire_starved_children.txt   must fire      -> INCONCLUSIVE
      bench/d42_fire_broken.txt             must NOT fire  -> FAILED
      bench/d42_fire_channel.txt            verify-suite.sh refuses, end to end
      bench/d42_fire_orphans.txt            the load harness cannot orphan
      bench/d42_DECISION.md                 the whole record, including three corrections
                                            against SCALE-DESIGN.md D42

  Read d42_DECISION.md before quoting the design entry: three of its claims are wrong,
  and two of them invert things the entry treats as settled.
