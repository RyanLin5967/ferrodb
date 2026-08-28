#!/bin/bash
# Per-target suite run with a DURABLE, RESUMABLE result file.
#
# `tools/verify-suite.sh` in per-target mode is the sanctioned instrument and it refuses correctly on
# a silent target — but it has no resume, so one SIGTERM costs the whole ~2-hour run. That is not
# hypothetical here: on 2026-08-28 at 18:39 this worktree's `cargo test --test
# integration_base_backup` was killed with `Terminated: 15` by a sibling agent's cleanup thirteen
# targets in, and the script refused (correctly) rather than reporting a partial pass.
#
# So: one line per target in $RESULTS, appended only after that target printed a verdict. Re-running
# skips every target already recorded, so a kill costs one target. A target that is killed leaves NO
# line, which is the point — silence is never recorded as a pass.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
OUT=${OUT:-/Users/idide/wt/artie-research/build-G}
RESULTS="$OUT/suite-f10-results.tsv"
LOGDIR="$OUT/suite-f10-logs"
mkdir -p "$LOGDIR"
touch "$RESULTS"

run_one() {
    local target="$1"
    local flag="$2"
    local log="$LOGDIR/$target.log"
    if grep -q "^$target	" "$RESULTS" 2>/dev/null; then
        echo "skip   $target (already recorded)"
        return 0
    fi
    timeout 1800 cargo test $flag --no-fail-fast > "$log" 2>&1
    local rc=$?
    local verdict
    verdict=$(grep 'test result:' "$log" | tail -1)
    if [ -z "$verdict" ]; then
        echo "SILENT $target rc=$rc — NOT recorded; re-run this target"
        return 1
    fi
    local passed failed
    passed=$(grep -o '[0-9]* passed' "$log" | awk '{s+=$1} END {print s+0}')
    failed=$(grep -o '[0-9]* failed' "$log" | awk '{s+=$1} END {print s+0}')
    printf '%s\t%s\t%s\t%s\n' "$target" "$passed" "$failed" "$rc" >> "$RESULTS"
    echo "done   $target passed=$passed failed=$failed rc=$rc"
    return 0
}

# Examples first: a failure here is a real build failure, and continuing would measure stale binaries.
if ! timeout 1800 cargo build --examples > "$LOGDIR/examples.log" 2>&1; then
    echo "REFUSING — cargo build --examples failed"; tail -20 "$LOGDIR/examples.log"; exit 1
fi

silent=0
run_one "lib" "--lib" || silent=$((silent+1))
for f in tests/*.rs; do
    t=$(basename "$f" .rs)
    run_one "$t" "--test $t" || silent=$((silent+1))
done

echo "--- totals ---"
awk -F'\t' '{p+=$2; f+=$3; n++} END {printf "targets=%d passed=%d failed=%d\n", n, p, f}' "$RESULTS"
echo "silent this pass: $silent"
[ "$silent" -eq 0 ] || { echo "INCOMPLETE — re-run to pick up the silent targets"; exit 2; }
awk -F'\t' '$3>0 {print "FAILING TARGET: "$0}' "$RESULTS"
awk -F'\t' 'BEGIN{bad=0} $3>0 {bad=1} END {exit bad}' "$RESULTS" || { echo "SUITE RED"; exit 1; }
echo "SUITE GREEN"
