D60 CERTIFICATION — the branch depth cap removed

  RUNNER.txt / suite.log / suite.go.log     head=91270f9  <- WHAT THE LANDING RESTS ON

CERTIFIED GREEN:

  D60-depth-c: mode=per-target rc=0 passed=2139 failed=0 build_errors=0 head=91270f9
  D60-depth-c: go rc=0 passed=97 failed=0            120 of 120 targets

tools/certify-head.sh: OK -- suite head=91270f9 == landing 91270f9.

THREE SUITES RAN, AND THE FIRST GREEN ONE WAS THE DANGEROUS ONE.
  4ab0096  2137 / 0   the cap removed. GREEN -- and the commit had broken the full branch-record
           format: `depth` widened to u32 in the MIDDLE of a variable-length record, so every
           record written by an older build decoded with its later fields shifted WHILE ITS CRC
           STILL PASSED (depth 3 -> 50,331,648; child_len read out of the checksum's own bytes;
           a branch owning 900 arenas drove an 8.86 GB Vec::with_capacity -- an allocation abort).
           Reachable through the legacy {db}.branches migration the CLI and pgserver wire.
           No test could see it: the ONE cross-version fixture in that module
           (serialize_pre_envelope) had been edited by the same commit to emit the new width.
           A fresh-context review found it; three of its four reviewers also found a SECOND
           recursion (TwoTierReaper::detach_from_parent) whose termination argument cited the
           deleted cap.
  324cc8c  2138 / 1   the fixes. The one failure was the pgwire test that pins every system
           view's column type OIDs: widening ferro_branches.depth to BIGINT is a client-visible
           change, and that assertion exists to make such a change cost a deliberate edit.
  91270f9  2139 / 0   the expectation updated, with the reason recorded where it stands.

THE PREMISE, measured before any of it (bench/d60_depth_premise.txt, three runs):
fork ~2-3.5 us and read ~7-9 us, FLAT from depth 1 to 250 -- 31x the old cap. A branch's root is
its parent's root at fork (CoW) and a read never walks ancestry. A depth limit is the right answer
only when reads walk the chain (qcow2 backing files); it was never the right answer here.

CI COLOUR AT PUSH TIME: GREEN on all three platforms for 0f9d34a (the D59 push) -- ubuntu, macos
and windows. Windows STATUS_ACCESS_VIOLATION (bench/windows_access_violation.txt) has not recurred
but remains UNEXPLAINED; agent-isolation does not merge to main until it is understood.
