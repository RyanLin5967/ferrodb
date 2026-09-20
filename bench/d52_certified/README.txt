D52 CERTIFICATION — the ExecCtx read/write split

  RUNNER.txt / suite.log / suite.go.log     head=a56676e  <- WHAT THE LANDING RESTS ON

The unprefixed name is the run the claim rests on. That rule exists because D40's own evidence
directory once had the canonical names carrying a SUPERSEDED head=, and a reader opening
RUNNER.txt first would have concluded the landing rested on a tip that was not the landed tip.

Runner: tools/verify-suite.sh, not a bare cargo test. It rebuilds examples first (refusing if
that fails), runs --lib plus every tests/*.rs by name, requires a `test result:` verdict AFTER
each target's own marker, refuses a zero-collected run, and re-reads HEAD and the dirty count at
the end so a tree that moved during the run cannot be recorded as green.

  VERIFY_OUT=~/wt/artie-research/verify-out-d52 VERIFY_MODE=per-target VERIFY_TIMEOUT=1800 \
    timeout 5400 tools/verify-suite.sh D52-readctx

CERTIFIED GREEN — the runner's own summary, verbatim:

  D52-readctx: mode=per-target rc=0 passed=2093 failed=0 build_errors=0 head=a56676e
  D52-readctx: go rc=0 passed=97 failed=0
  RUNNER_EXIT=0

tools/certify-head.sh printed:
  certify-head: OK — suite head=a56676e == landing a56676e

tools/prepush.sh rc=0 (CI's RUSTFLAGS="-D duplicate_macro_attributes -D dead_code" over
--all-targets, plus the x86_64-pc-windows-msvc cross-check, which does NOT link).

The tree was clean before and after, and `git log -1` still named a56676e when certify-head ran
— so the green names the commit that was pushed, not one behind it.
