#!/bin/sh
# W4 check 3 — the main measurement, one arm per invocation.
#
# One arm per process, for a reason the first full run taught: the harness refused in the fourth
# arm (the merge arm hit the engine's 255-entry page provenance dictionary) and, because every
# number was printed at the end, took nine completed arms down with it. Arms are independent
# experiments and are now run as such, each into its own file.
#
# SERIAL on purpose. The instrument is an integer and load-immune, but the quantity it measures is
# a contention probability, so two arms running at once would be measuring each other.
#
# Usage: bench/w4_standdown/run.sh <out-dir>
set -eu
OUT="${1:?usage: run.sh <out-dir>}"
BIN=./target/release/examples/w4_standdown_count
[ -x "$BIN" ] || { echo "no $BIN -- build with --features w4-standdown-count" >&2; exit 1; }
mkdir -p "$OUT"
rc=0
# The binary is re-checked before EVERY arm, not once at the top. In the first full pass another
# process on this box reclaimed disk by deleting this worktree's entire `target/` directory
# midway through, and the three remaining arms then failed one by one with a message from
# `timeout` rather than from anything that knew what the run was for.
for arm in ${W4_RUN_ARMS:-readonly agent agent_dml merge ddl}; do
    [ -x "$BIN" ] || {
        echo "REFUSED: $BIN vanished before arm $arm -- another process removed target/" >&2
        exit 1
    }
    echo "== arm $arm =="
    W4_CLIENTS=8 W4_ROUNDS=400 W4_ARMS="$arm" \
        timeout 900 "$BIN" > "$OUT/arm_$arm.txt" 2> "$OUT/arm_$arm.err" || {
            echo "  REFUSED (rows completed before the refusal are kept):" >&2
            head -2 "$OUT/arm_$arm.err" >&2
            rc=1
        }
done
exit "$rc"
