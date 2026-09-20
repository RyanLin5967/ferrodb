D59 CERTIFICATION — the per-statement snapshot without a lock

  RUNNER.txt / suite.log / suite.go.log     head=0ca6581  <- WHAT THE LANDING RESTS ON

CERTIFIED GREEN:

  D59-snapcache-d: mode=per-target rc=0 passed=2134 failed=0 build_errors=0 head=0ca6581
  D59-snapcache-d: go rc=0 passed=97 failed=0            119 of 119 targets

tools/certify-head.sh: OK -- suite head=0ca6581 == landing 0ca6581.

FOUR SUITES RAN, and the one that went RED after a green was the most useful:
  cbaffd4  2128 / 0   the cache as first built. Green -- and a fresh-context review then found
           recovery mutating the table outside the guard, high_water (half of a snapshot)
           uncovered by the version, and every WAL record invalidating every reader's cache.
  9d1ddcf  2133 / 0   the review's fixes.
  38bac6e  2132 / 1   RED: the strengthened race test (now comparing BOTH snapshot fields while
           a writer raises the watermark) caught MY OWN fix raising the watermark and then
           bumping the version, unlocked -- "the version did not move (3758) but the cached
           high_water is 626254 against the locked 627254". That race test catches the bad
           ordering in only 3 of 8 runs, so a deterministic companion now tests the mechanism:
           8 of 8 on the mutant.
  0ca6581  2134 / 0   the raise and the bump in one critical section.

CI COLOUR AT PUSH TIME: all three platforms were RED on d7cf823 (the D58 push) for one test --
d58_latch_free_descent's eviction floor sampled residency after the race, on the tree's hottest
pages. Fixed at 38bac6e with a pigeonhole read from the file (DiskManager::high_water vs
frames.len()), fire-checked by breaking its premise. Windows' other tests were green on that run.
Windows STATUS_ACCESS_VIOLATION (bench/windows_access_violation.txt) remains UNRESOLVED;
agent-isolation does not merge to main until it is understood.

The performance evidence is NOT here -- bench/d59_after_quick.txt (x5.68 -> x8.12 at 16 threads)
and bench/d59_profile_16T_after.sample.txt (read_snapshot gone; no ferrodb mutex above 17 samples).
