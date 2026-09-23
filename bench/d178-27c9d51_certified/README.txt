D178 CERTIFICATION — UPDATE/DELETE routed through the optimizer.

RANGE CERTIFIED: fe40276..27c9d51  (12 commits: 2 cherry-picked D176 instrument commits,
2 src/ changes, 2 test files, 1 harness, 4 raw/pre-registration text files)

THE VERDICT, with its base and its MODE — quoting the RUST line, not the Go one:
  d178-final: mode=per-target rc=0 passed=2545 failed=0 build_errors=0 head=27c9d51

  ⇒ +14 @ fe40276, mode=per-target (baseline 2531 at b90bde1, from
    bench/d175-b90bde1_certified/verdict.txt, same mode). The 14 new tests, exactly:
    13 in tests/d178_dml_index_correctness.rs + 1 in tests/d178_dml_index_counters.rs.
    The two cherry-picked D176 commits add ZERO tests (checked: 0 lines matching
    `^+.*#\[test\]` across both).

  ⚠ DO NOT COUNT THE TESTS WITH GREP. `grep -c '#[test]'` reports 2 for the counters
    file; one of those is the line "⛔ Do not add a second #[test] here." in its own doc
    comment. The suite log's per-target blocks are the instrument, and they say 13 and 1.

  ⚠ The console's LAST rc= line is the GO suite (`go rc=0 passed=97`) and is a DIFFERENT
    instrument counting a DIFFERENT thing. Quoting 97 as the suite total would be wrong by
    construction; the mode is what catches it.

INDEPENDENT CROSS-CHECK OF THE SCRIPT'S OWN ARITHMETIC, run rather than trusted:
  sum of every "^test result: ok. N passed" line in suite.log = 2545   (matches)
  "^test result: FAILED" lines ................................. 0
  "panicked at" lines .......................................... 0
  "^failures:" blocks .......................................... 0
  targets run ("^=== target") .................................. 141
  INCONCLUSIVE.txt ............................................. absent

THE GATES, run not predicted (2026-09-23, after the final suite):
  certify-head  OK   "suite head=27c9d51 == landing 27c9d51", rc=0
  staleness     COVERED   rc=0
  base          0 behind / 12 ahead of main; main == origin/main == fe40276

⚠ THIS COMMIT IS NOT ITSELF COVERED BY THE SUITE IT BANKS, and that is the same shape
  bench/d175-b90bde1_certified/ has: a certification can never quote a summary naming its
  own sha. Everything added here is TEXT under bench/ — no src/, no tests/, no examples/ —
  so it cannot change a test outcome. But certify-head.sh REFUSES a comment-only delta on
  purpose ("close enough" is the judgement it exists to delete), so whoever lands this
  either lands at 27c9d51 or re-runs the suite at the new tip. Do not wave it through.

⚠ WHY THERE ARE TWO SUITE RUNS AND ONLY ONE IS BANKED. An earlier green named head=f18987d
  (also 2545/0). Commit 27c9d51 then added a raw file and a doc-comment band to
  examples/d176_merge_rowcount.rs, which moved the tip past the commit the green named — so
  that run was superseded rather than quoted, and the suite was re-run. Editing examples/
  is not cosmetic for this suite: eight integration tests fail when the binary they spawn is
  older than src/ or examples/, which is why verify-suite.sh rebuilds examples first.

WHAT A GREEN HERE DOES NOT MEAN — from the branch's own report:
  * Nothing about CONCURRENCY. The D178 measurements are single-threaded by construction;
    read-twice-and-subtract on the process-global counters is exact only there.
  * Nothing about WALL-CLOCK TIME. No duration is claimed anywhere in this row, deliberately:
    run 3 was taken on a box carrying another agent's suite and a 110%-CPU mediaanalysisd,
    and sat at ~3.7% CPU (36.85 s CPU in 622 s elapsed). Integers do not move under load;
    that is the whole reason this row is counted rather than timed.
  * `secondary_col > v` still does NOT use an index. D178 makes it return the right answer
    via a sequential scan instead of ERRORING. See the report's §7 for the row that remains.
  * The counter test asserts an INVARIANT (SELECT and UPDATE plan the same predicate the same
    way), not a constant. A future cost-model change may move the numbers without breaking it.

FIRE-CHECKED, not merely passing. The 14 new tests were run against three deliberate mutants
(throwaway branch d178-firecheck, commit bbcfdf6, labelled DELIBERATELY BROKEN):
  M1 original defect restored -> counters RED, correctness GREEN (correct: the old path was
     slow, not wrong)
  M2 H1 guard removed        -> counters RED + correctness RED x2
  M3 Bound::Excluded mapped to Included, a silently WRONG ANSWER with no error anywhere
                             -> correctness RED on row identity: left [798,799], right [799]
M2 also caught a defect in the H1 test itself, which passed against the bug at a 60-row
fixture. Recorded as Amendment 2 in bench/d178_prereg.txt rather than fixed quietly.

Full write-up: /Users/idide/wt/artie-research/frontier/d178_dml_index.md
Pre-registration + amendments: bench/d178_prereg.txt
Raws: bench/d178_run1_BEFORE_RAW.txt, _run2_MID_RAW.txt, _run3_AFTER_RAW.txt, _run4_MERGE_RAW.txt
