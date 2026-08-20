#!/usr/bin/env bash
# tools/verify-suite.sh [label] — run the full suite and report a number you can trust.
#
# WHY THIS LIVES IN THE REPO. It used to live in the session scratchpad under /private/tmp, and
# LEDGER.md's Phase E plus RESUME-CHECKPOINT.md both instructed the next pass to run it from there.
# The machine restarted on 2026-08-20 and /private/tmp was wiped, so both documents pointed at a
# file that no longer existed — a broken instruction in the two places a fresh pass is told to look
# first. Anything a ledger tells the future to execute has to be versioned with the code.
#
# WHAT IT GUARDS. Three ways this project has produced a wrong test count before:
#   1. a run whose tree moved underneath it (a mid-write sample once read 375 against a real 374),
#   2. `cargo test | head`, where the pipe SIGPIPEs cargo and the partial total reads as a regression,
#   3. a run killed by too short a timeout, whose partial count looks like a finished one.
# So: no pipe, a generous bound, and HEAD plus the dirty-file count compared before and after. If
# either moved, it refuses to print a number at all rather than printing one that cannot be trusted.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

LABEL=${1:-verify}
OUT=${VERIFY_OUT:-$(mktemp -d)}
LOG="$OUT/suite-$LABEL.log"
BOUND=${VERIFY_TIMEOUT:-7200}

command -v timeout >/dev/null || { echo "$LABEL: REFUSING — no \`timeout\`; an unbounded suite can wedge a pass"; exit 1; }

h0=$(git log -1 --format=%h); d0=$(git status --short | wc -l | tr -d ' ')
timeout "$BOUND" cargo test --no-fail-fast > "$LOG" 2>&1; rc=$?
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
[ "$f" = "0" ] && [ "$be" = "0" ] && [ "$rc" = "0" ]
