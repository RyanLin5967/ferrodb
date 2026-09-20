D58 CERTIFICATION — the latch-free read descent

  RUNNER.txt / suite.log / suite.go.log     head=dc9aa73  <- WHAT THE LANDING RESTS ON

CERTIFIED GREEN:

  D58-latchfree-e: mode=per-target rc=0 passed=2124 failed=0 build_errors=0 head=dc9aa73
  D58-latchfree-e: go rc=0 passed=97 failed=0

tools/certify-head.sh: OK -- suite head=dc9aa73 == landing dc9aa73.

FIVE SUITES RAN. The two GREEN ones in the middle were not the ones to trust.
  6ea3f8c  2120 / 0   the descent as first built. GREEN, four mutants dead -- and WRONG: a
           fresh-context review found two real regressions (a frame relabelled for an incoming
           page published the OUTGOING page's bytes under the new label for the length of a disk
           read; the right-walk stopped on an EMPTY leaf, which insert's key reuse produces
           between two writes). Neither was reachable by any fixture in the file.
  029be09  2123 / 0   the fixes, plus a second identity check (the page's own header id) and
           four rewritten fixtures -- including the eviction test, which had assumed 60k keys
           overflow a 1024-frame pool. They do not (~600 pages), so the test named for eviction
           had been running fully resident. It now grows until page 1 is OBSERVED evicted.
  999231d  2070 / 53  RED, and not a regression: editing a #[cfg(test)]-only module leaves every
           example binary legitimately un-rebuilt, and thirteen tests' mtime staleness guard
           counted it as stale.
  4218ac5  2113 / 11  the same defect in three more spellings of that walk, found by re-running.
  dc9aa73  2124 / 0   all six spellings skip src/**/tests_*.rs, and the convention that makes the
           skip sound is enforced by lock_order_allowlist::test_only_sources_are_cfg_test_gated.

CI COLOUR AT PUSH TIME: windows-latest was RED on eefe7f6 (the D57 push) for
consensus::transport an_undrained_inbox_is_bounded_in_bytes, whose refund assertion was racy by
construction -- it drained one message and read the counter while the connection thread was still
charging ("3294 then 3294" is the bound WORKING). Replaced at 999231d with a stronger, race-free
property (drain to quiescence, budget must be zero, delivered + refused = sent), fire-checked with
two counter mutants that the old assertion passed. ubuntu and macos were green.
Windows STATUS_ACCESS_VIOLATION (bench/windows_access_violation.txt) remains UNRESOLVED;
agent-isolation does not merge to main until it is understood.

The performance evidence is NOT here -- bench/d58_after_quick.txt (x2.62 -> x5.68 at 16 threads)
and bench/d58_profile_16T_after.sample.txt (the page latch absent; read_snapshot is what is left).
