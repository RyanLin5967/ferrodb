#!/usr/bin/env bash
# tools/verify-suite.sh [label] — run the full suite and report a number you can trust.
#
# WHY THIS LIVES IN THE REPO. It used to live in the session scratchpad under /private/tmp, and
# LEDGER.md's Phase E plus RESUME-CHECKPOINT.md both instructed the next pass to run it from there.
# The machine restarted on 2026-08-20 and /private/tmp was wiped, so both documents pointed at a
# file that no longer existed — a broken instruction in the two places a fresh pass is told to look
# first. Anything a ledger tells the future to execute has to be versioned with the code.
#
# WHAT IT GUARDS. Four ways this project has produced a wrong test count before:
#   1. a run whose tree moved underneath it (a mid-write sample once read 375 against a real 374),
#   2. `cargo test | head`, where the pipe SIGPIPEs cargo and the partial total reads as a regression,
#   3. a run killed by too short a timeout, whose partial count looks like a finished one.
#   4. a suite run against STALE EXAMPLE BINARIES. `cargo test` does not rebuild `examples/`, and
#      eight integration tests carry a guard that FAILS when the binary they spawn is older than
#      `src/` or `examples/`. So any run taken straight after a merge that touched `src/` reports a
#      wave of failures that are the harness working, not the code breaking. This bit the project
#      before: a suite count "is meaningless unless examples were rebuilt after the last src/ edit".
#      The build is therefore part of the measurement, not a thing the caller is trusted to remember.
#   5. A RUST-ONLY RUN REPORTED AS "the suite". `cargo test` does not build or run `cdc-consumer/`,
#      a separate Go module holding both CDC sinks. Row I20's own summary says so — "`cargo test`
#      does not run it ... this is the suite where four of the five fixes live" — and on 2026-08-24
#      I20's fifth commit (`0783479`, the fresh-context adversarial pass that found four real
#      defects in the finding-4 fix) touched ONLY Go files, so no Rust count could see it at all.
#      A merge verified on the Rust count alone leaves such a commit entirely unmeasured. The Go
#      suite is therefore part of the measurement too, and its absence is a REFUSAL, not a skip:
#      this repo already settled that argument in `require_python_module`
#      (`tests/integration_pgwire.rs`) — "a skipped check would report success for the wrong reason".
# So: no pipe, a generous bound, examples rebuilt first, both suites run, and HEAD plus the
# dirty-file count compared before and after. If either moved, it refuses to print a number at all
# rather than printing one that cannot be trusted.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

LABEL=${1:-verify}
OUT=${VERIFY_OUT:-$(mktemp -d)}
# `mktemp -d` creates its directory; a caller-supplied VERIFY_OUT may not exist. Without this, every
# redirect below fails, which makes the build step look like it failed and the guard refuse with the
# wrong reason — a fail-safe direction, but a false diagnosis. Found by fire-checking the guard.
mkdir -p "$OUT" || { echo "$LABEL: REFUSING — cannot create output dir $OUT"; exit 1; }
LOG="$OUT/suite-$LABEL.log"
BOUND=${VERIFY_TIMEOUT:-7200}

command -v timeout >/dev/null || { echo "$LABEL: REFUSING — no \`timeout\`; an unbounded suite can wedge a pass"; exit 1; }

h0=$(git log -1 --format=%h); d0=$(git status --short | wc -l | tr -d ' ')

# Examples first — see note 4 above. A failure here is a real build failure and must stop the run:
# continuing would measure the previous binaries and call the result a suite.
if ! timeout "$BOUND" cargo build --examples > "$LOG.examples" 2>&1; then
    echo "$LABEL: REFUSING — \`cargo build --examples\` failed, so the suite would spawn stale binaries"
    tail -20 "$LOG.examples"
    echo "  log: $LOG.examples"
    exit 1
fi

timeout "$BOUND" cargo test --no-fail-fast > "$LOG" 2>&1; rc=$?

# The Go module, AFTER the Rust suite and never beside it. Both suites bind TCP ports, and this
# project has a documented load-sensitive port race; row I19's own resume state carries the same
# instruction ("only AFTER the Rust suite; port contention"). Sequential is slower and honest.
GOLOG="$OUT/suite-$LABEL.go.log"
gorc=0; gp=0; gf=0; go_ran=no
if [ -f cdc-consumer/go.mod ]; then
    go_ran=yes
    if ! command -v go >/dev/null; then
        echo "$LABEL: REFUSING — cdc-consumer/go.mod exists but there is no \`go\` on PATH, so the"
        echo "  suite that holds both CDC sinks cannot run. A skipped check reports success for the"
        echo "  wrong reason; install Go or delete the module, but do not measure without it."
        exit 1
    fi
    # -count=1 defeats Go's per-package result cache: a cached `ok` is not a run.
    # -mod=readonly so a missing dependency cannot rewrite go.sum and trip the tree-moved check
    # below with a false "the tree moved" instead of the real "your module is incomplete".
    ( cd cdc-consumer && timeout "$BOUND" go test -v -count=1 -mod=readonly ./... ) > "$GOLOG" 2>&1
    gorc=$?
    gp=$(grep -c '^--- PASS' "$GOLOG"); gf=$(grep -c '^--- FAIL' "$GOLOG")
    # A run that collected nothing has not passed. `go test` prints "no test files" and exits 0,
    # which is a zero-collected run wearing a green exit code.
    if [ "$gp" -eq 0 ] && [ "$gf" -eq 0 ]; then
        echo "$LABEL: REFUSING — the Go suite collected zero tests (rc=$gorc). That is a broken run,"
        echo "  not a green one."
        tail -20 "$GOLOG"; echo "  log: $GOLOG"; exit 1
    fi
fi

h1=$(git log -1 --format=%h); d1=$(git status --short | wc -l | tr -d ' ')

if [ "$h0" != "$h1" ] || [ "$d0" != "$d1" ]; then
    echo "$LABEL: UNTRUSTWORTHY — the tree moved during the run ($h0/$d0 -> $h1/$d1). Re-run; do not record this."
    echo "  log: $LOG"
    exit 1
fi

p=$(awk '/^test result:/ {gsub(/;/,""); for(i=1;i<=NF;i++) if($i=="passed") s+=$(i-1)} END {print s+0}' "$LOG")
f=$(awk '/^test result:/ {gsub(/;/,""); for(i=1;i<=NF;i++) if($i=="failed") s+=$(i-1)} END {print s+0}' "$LOG")
be=$(grep -cE '^error(\[|:)' "$LOG")
# A run that collected nothing has not passed, whatever its exit code says.
if [ "${p:-0}" -eq 0 ]; then
    echo "$LABEL: REFUSING — zero tests collected (rc=$rc). That is a broken run, not a green one."
    echo "  log: $LOG"; exit 1
fi
echo "$LABEL: rc=$rc passed=$p failed=$f build_errors=$be head=$h1 log=$LOG"
if [ "$go_ran" = yes ]; then
    echo "$LABEL: go rc=$gorc passed=$gp failed=$gf log=$GOLOG"
else
    echo "$LABEL: go NOT RUN — no cdc-consumer/go.mod in this tree"
fi
[ "$f" = "0" ] && [ "$be" = "0" ] && [ "$rc" = "0" ] && [ "$gorc" = "0" ] && [ "$gf" = "0" ]
