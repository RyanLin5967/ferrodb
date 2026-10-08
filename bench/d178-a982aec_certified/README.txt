D178 CERTIFICATION — the branch tip, superseding bench/d178-27c9d51_certified/.

RANGE CERTIFIED: fe40276..a982aec  (14 commits)

  d178-planspace: mode=per-target rc=0 passed=2545 failed=0 build_errors=0 head=a982aec
  d178-planspace: go rc=0 passed=97 failed=0

  ⇒ +14 @ fe40276, mode=per-target (baseline 2531). Unchanged from the 27c9d51 run: the two
    commits since then add an EXAMPLE and a bench/ text file, and examples carry no #[test].

WHY THIS SUPERSEDES THE 27c9d51 RUN. Not because that run was wrong — it was correct for the
commit it named. Two commits landed after it:
  a982aec  the plan-space differential, which CORRECTS a claim I had made about the H1 fix
  a9fd980  the banked 27c9d51 certification itself
A green certifies the commit it NAMES, so the earlier one stopped covering the tip.

INDEPENDENT CROSS-CHECK, run rather than trusted:
  sum of every "^test result: ok. N passed" line = 2545   (matches the reported total)
  "^test result: FAILED" .... 0      "panicked at" .... 0
  "^failures:" blocks ....... 0      targets run ...... 141
  INCONCLUSIVE.txt .......... absent

GATES:
  certify-head   OK — "suite head=a982aec == landing a982aec", rc=0
  staleness      COVERED, rc=0
  ⚠ BUT staleness.sh MEASURES AGAINST `agent-isolation`, NOT `main`, and reported "0 behind"
    while main HAD in fact moved. Measured directly: `git rev-list --left-right --count
    main...HEAD` = 1 behind, 14 ahead. COVERED is true about the base it checked and says
    nothing about main. Do not read it as "up to date with main".

THE ONE COMMIT THIS BRANCH IS BEHIND, and why the green still stands:
  e280517 "d173: label every hold_leader call site" touches exactly ONE file,
  tests/integration_cluster_agents.rs, and it is a labelling change to an existing test.
  File sets are DISJOINT from this branch's (checked with comm, not eyeballed), it touches no
  cluster code this branch goes near, and its net #[test] delta is 0 (35 before, 35 after).
  ⇒ FALSIFIABLE PREDICTION FOR THE LANDING LANE: merging this branch onto current main should
    report per-target passed=2545, unchanged. Anything else means something other than these
    two changes moved.
  ⚠ merge-tree returns rc=0, i.e. no LINE collided. That is not the same as the result being
    coherent; the disjoint file sets and the zero test delta are the claims, not the rc.

NOT REBASED ON PURPOSE. A suite was running against this tree, and rebasing would have voided
the green just taken. The d178-on-main lane owns the merge; as of this writing it had merged
27c9d51, which predates a982aec and therefore predates the correction in it.

WHAT A GREEN HERE DOES NOT MEAN — see bench/d178-27c9d51_certified/README.txt, which still
applies in full, plus:
  * The plan-space differential says SELECT's plan space DID change: 6 corpus rows go
    error -> correct answer, and 1 goes worse-plan -> better-plan. No answer moved anywhere.
    "The guard only removes unexecutable plans" was my argument and the measurement refuted it.
    See bench/d178_run5_PLANSPACE_RAW.txt.
