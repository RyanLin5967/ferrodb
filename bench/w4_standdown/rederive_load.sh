#!/bin/sh
# W4 check 3 — E5: is the instrument actually load-immune, as its own module doc claims?
#
# The harness says: "The discriminating quantity is a ratio of two integers — `begin_read` either
# returned `Some(ReadPass)` or it did not — and an integer is load-immune. Nothing in this harness
# is timed, and nothing in it needs a quiet box."
#
# The integers are exact. Their RATIO is not a load-invariant property, and that is a different
# claim. The stand-down fraction is a sampled estimate of the DUTY CYCLE of `writer_active` — the
# fraction of wall time some connection is inside an exclusive catalog guard — which is a ratio of
# two durations. Both durations move under scheduler pressure, and they do not have to move
# together: a thread descheduled INSIDE the guard widens the announced window without adding a
# single statement to the workload.
#
# So this runs the SAME arm twice, once on the quiet box we hold the suite lock for and once
# beside a CPU burner, and reads whether the fraction moves. If it does, every number this
# experiment produces is a number about this box at that moment, and the artifact must say so.
#
# ⚠ The burner is `timeout`-bounded per worker, so a worker cannot outlive this script even if the
# script is killed: the deadline lives in the worker's own process, not in a supervisor.
#
# Usage: bench/w4_standdown/rederive_load.sh <out-dir> [burn-seconds]
set -eu
OUT="${1:?usage: rederive_load.sh <out-dir> [burn-seconds]}"
BURN="${2:-900}"
BIN=./target/release/examples/w4_standdown_count
[ -x "$BIN" ] || { echo "REFUSED: no $BIN" >&2; exit 1; }
mkdir -p "$OUT"

NCPU=$(sysctl -n hw.ncpu)
rc=0

quiet_then_loud() {
    label=$1; shift
    echo "== $label: QUIET =="
    env "$@" timeout 1800 "$BIN" > "$OUT/${label}_quiet.txt" 2> "$OUT/${label}_quiet.err" \
        || { echo "  REFUSED:" >&2; head -3 "$OUT/${label}_quiet.err" >&2; rc=1; }

    echo "== $label: LOADED ($NCPU burners, self-terminating after ${BURN}s) =="
    i=0
    burners=""
    while [ "$i" -lt "$NCPU" ]; do
        # Each burner carries its OWN deadline. Nothing supervises them, so nothing can fail to.
        # The pid is captured from `$!` rather than read back out of `jobs -p`, which would also
        # hand back any straggler from a previous call to this function.
        timeout "$BURN" sh -c 'while : ; do : ; done' &
        burners="$burners $!"
        i=$((i+1))
    done
    env "$@" timeout 1800 "$BIN" > "$OUT/${label}_loaded.txt" 2> "$OUT/${label}_loaded.err" \
        || { echo "  REFUSED:" >&2; head -3 "$OUT/${label}_loaded.err" >&2; rc=1; }
    # Stop them the moment the measured run is done, rather than waiting out the deadline. The
    # deadline is the guarantee; this is the courtesy.
    for p in $burners; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    # Prove they are gone rather than assuming a kill landed.
    left=0
    for p in $burners; do kill -0 "$p" 2>/dev/null && left=$((left+1)); done
    echo "   burners still alive after the kill: $left"
    [ "$left" -eq 0 ] || rc=1
}

# The deciding arm and the row a real driver produces.
quiet_then_loud e5_agent_extended W4_CLIENTS=8 W4_ROUNDS=400 W4_ARMS=agent W4_PROTOS=extended
# ...and the read-only arm under `simple`, whose whole stand-down population is reader-vs-reader
# contention on the Parse site. If load moves anything it should move this.
quiet_then_loud e5_readonly_simple W4_CLIENTS=8 W4_ROUNDS=400 W4_ARMS=readonly W4_PROTOS=simple

echo "load campaign complete rc=$rc"
exit "$rc"
