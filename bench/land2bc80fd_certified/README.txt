CERTIFICATION EVIDENCE — main@2bc80fd, the green that authorised the push ac2ab26..2bc80fd
===========================================================================================

Banked 2026-09-22, BEFORE anything else ran into that directory. The verdict existed only in
$VERIFY_OUT/SUMMARY.txt, which verify-suite.sh writes with a TRUNCATING redirect to a fixed path.
Main's bb443c4, ac2ab26 and now bench/d126_certified/ all exist because exactly that loss happened
to D124's, D101's and D126's verdicts. This is the FOURTH instance of the same pattern; banking at
land time rather than after the next run is the only thing that has ever prevented it.
⚠ Distinct LABELS do not protect SUMMARY.txt — only a distinct VERIFY_OUT directory does.

THE HARNESS'S OWN LINES, VERBATIM
---------------------------------
land2bc80fd: mode=per-target rc=0 passed=2476 failed=0 build_errors=0 head=2bc80fd
land2bc80fd: go rc=0 passed=97 failed=0

GATES, ALL THREE
----------------
  verify-suite.sh  per-target, rc=0, 2476 passed, 0 failed, 0 build errors, go 97/0
  certify-head.sh  OK — suite head=2bc80fd == landing 2bc80fd
  prepush.sh       OK — FFI gate, -D dead_code on --examples and --all-targets, Windows cfg arms
  tree             sha_before == sha_after == 2bc80fd, dirty=0 at BOTH ends

⭐ THE TOTAL WAS PRE-REGISTERED AND HIT EXACTLY, AND CONFIRMED FROM A THIRD DIRECTION
-------------------------------------------------------------------------------------
`artie-research/frontier/LANDING-PREREG-2bc80fd.md`, committed before the run, predicted 2476 as
2459 + D125's 6 + D128's 4 + D126's 7, each addend a counted `#[test]` delta against its own merge
base. Two independent confirmations arrived from runs that were not built to confirm it:

    d126-34549e4   whole 2468  -> per-target 2466, on a 2459 base   => D126 is +7
    d126-8d2b673   whole 2474  -> per-target 2472, after merging D125 => 2465 + 7
    2472 + D128's 4 = 2476

⚠ AND THE ATTRIBUTION TRAP IT AVOIDS: D125 measured 2455, which is BELOW main's 2459 and reads as
a regression. It is not. D125 branches from dc2a2cf, before D101 and D124 added tests, so its base
is 2449. **A total is comparable only against its own base, and only within its own MODE**
(`whole` = `per-target` + 2 doctests). Both halves of that have produced phantom regressions here.

WHAT IT CERTIFIES — the commits pushed in ac2ab26..2bc80fd
-----------------------------------------------------------
  2bc80fd  Merge D126-atomic-upsert  — atomic catalog upsert; no momentarily-absent key
  7aa52b9  D128 sites 2 and 3        — the queue never leaves shared state; fire-checked both ways
  1012fea  D114 re-derived           — on the designated runtime; the row is REAL
  a7baa9e  D110 re-derived           — the banked zero was zero by construction
  2f15622  D101 fire-check           — and a correction to 7a7e998's own severity claim
  7a7e998  D101 rewiring             — the last three stub-runtime harnesses
  2b21996  Merge D125-starved-insert — a write is all-or-nothing against a failed allocation
