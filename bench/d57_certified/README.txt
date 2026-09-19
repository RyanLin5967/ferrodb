D57 CERTIFICATION — the overlay probe, after the fresh-context review

  RUNNER.txt / suite.log / suite.go.log     head=7c1e6d8  <- WHAT THE LANDING RESTS ON

CERTIFIED GREEN:

  D57-overlay-c: mode=per-target rc=0 passed=2115 failed=0 build_errors=0 head=7c1e6d8
  D57-overlay-c: go rc=0 passed=97 failed=0

tools/certify-head.sh: OK -- suite head=7c1e6d8 == landing 7c1e6d8.

THREE SUITES RAN, AND THE FIRST GREEN ONE WAS NOT THE ONE TO TRUST.
  18e70b1  2111 passed / 0 failed  -- the probe as first shipped. GREEN, with four mutants dead,
           and WRONG: a fresh-context review then found three real regressions (a DECIMAL key,
           a primary-key move on a branch, an off-type value staged in column 0), none reachable
           by any fixture in the suite because every fixture stages well-typed, un-moved rows.
  31476ee  2114 passed / 1 failed  -- the fixes. The one failure was the envelope's Workspace
           field-allowlist guard, which demands a written determination for every new field.
  7c1e6d8  2115 passed / 0 failed  -- the determination recorded. This is the certified head.

CI COLOUR AT PUSH TIME: the run for the previous push (7f370cd, D56) was RED on windows-latest
only -- integration_cluster_agents::a_hundred_agent_writes_..., "not the leader ... an election is
in progress". The test's own comment documents this as a known Windows lease race ("the same
commit passed on the push run and failed on the pull_request run"); ubuntu and macos passed.
Not caused by D56 (which makes statements faster, and the lease is wall-clock). Windows remains
UNRESOLVED (see bench/windows_access_violation.txt); agent-isolation does not merge to main
until it is understood.

The performance evidence is NOT here -- bench/d57_staged_curve_before.txt and
bench/d57_staged_curve_after.txt (74a9ac4, 18e70b1).
