D53 CERTIFICATION — the shared B+tree root cell

  RUNNER.txt / suite.log / suite.go.log     head=06aa445  <- WHAT THE LANDING RESTS ON

CERTIFIED GREEN — the runner's own summary, verbatim:

  D53-sharedroot: mode=per-target rc=0 passed=2096 failed=0 build_errors=0 head=06aa445
  D53-sharedroot: go rc=0 passed=97 failed=0

tools/certify-head.sh: OK -- suite head=06aa445 == landing 06aa445.

⚠ This run was FAST (about 6 minutes against the ~40 of the D52 run on the same suite) because
the build was already warm. That is not a smaller suite: passed=2096 against D52's 2093, plus the
three new tests in tests/d53_shared_root_cell.rs. Recorded because a suddenly-quick green is
exactly the shape that should make a reader suspicious, and the count is what settles it.
