D56 CERTIFICATION — the stats-less point lookup uses the index (the unique-key fact)

  RUNNER.txt / suite.log / suite.go.log     head=444f88d  <- WHAT THE LANDING RESTS ON

CERTIFIED GREEN:

  D56-statsless-b: mode=per-target rc=0 passed=2105 failed=0 build_errors=0 head=444f88d
  D56-statsless-b: go rc=0 passed=97 failed=0

tools/certify-head.sh: OK -- suite head=444f88d == landing 444f88d.

THE RUN BEFORE THIS ONE WAS NOT GREEN, and that is the point of running it. The first full
suite over 06a88d2 (the fix + its tests + the curve) came back 2104 passed / 1 FAILED:
optimizer::cost_model::tests::test_cost_secondary_over_primary asserted that a primary-key
equality and a secondary-column equality estimate the SAME row count on an un-ANALYZEd table.
That assertion was a statement of the defect (10 == 10). 444f88d rewrites it to test the
property the test is named for, twice, and is fire-checked (fails on the first block with the
fact removed). SCALE-DESIGN D56 records the reasoning; do not take a test edit on trust.

The performance evidence is NOT here -- it is bench/d56_plan_curve.txt (06a88d2), a size
curve: control 1/N, fix flat at the ANALYZE ceiling, x12.8 -> x167 with table size.
