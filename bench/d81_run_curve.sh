#!/bin/bash
# D81 — the before/after curve, both halves on ONE box inside ONE lock hold.
#
# WHY ONE HOLD. The exit criterion is a RATIO (persistence ON / persistence OFF) and how that
# ratio moves with L. A ratio taken across two lock holds is a ratio across two box states, and
# this box is never quiet: `bench/d79_persistence_is_the_wall.txt` already records its ON arm
# reading LOWER at L=1000 than at L=500. Four arms back to back is the only arrangement in which
# the before/after comparison is of the change rather than of the afternoon.
#
# WHY TWO BINARIES AND NOT TWO CHECKOUTS. Both are built from the SAME examples/ source — the
# instrument commit (0111764) added the counter columns and changed no behaviour, so the only
# difference between d75_BEFORE and d75_AFTER is the arena change itself. Each binary prints its
# own `build_provenance()` header, so every table below names the commit that produced it rather
# than the commit this script was launched from.
#
# THE HEARTBEAT. measure-lock.sh reclaims a lock whose owner file is older than 45 minutes, on the
# assumption that such a holder is dead. Four arms take longer than that, so a sidecar touches the
# owner file while — and only while — the run's pid is actually alive. That is the liveness the
# breaker is trying to infer, supplied directly; it is not a way of holding the lock past death.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

BEFORE=${1:?usage: d81_run_curve.sh <before-binary> <after-binary> <out>}
AFTER=${2:?}
OUT=${3:?}
LOCK=/Users/idide/wt/logs/.measure.lock
LIVE=${D75_LIVE:-500,1000,2000,4000}

for b in "$BEFORE" "$AFTER"; do
  [ -x "$b" ] || { echo "REFUSED: $b is not an executable"; exit 1; }
done

/Users/idide/wt/logs/measure-lock.sh acquire d81-fsm | tee -a "$OUT" || exit 1
RUN_PID=$$
( while kill -0 "$RUN_PID" 2>/dev/null; do
    [ -d "$LOCK" ] && touch "$LOCK/owner" 2>/dev/null
    sleep 60
  done ) &
HEART=$!
cleanup() {
  kill "$HEART" 2>/dev/null
  /Users/idide/wt/logs/measure-lock.sh release d81-fsm | tee -a "$OUT"
}
trap cleanup EXIT INT TERM

arm() { # arm <label> <binary> <persist>
  {
    echo
    echo "======================================================================"
    echo "ARM: $1   binary=$2   D75_PERSIST=${3:-unset}   started $(date -u +%FT%TZ)"
    echo "load at arm start: $(uptime | sed 's/.*averages*: *//')"
    echo "======================================================================"
  } >> "$OUT"
  local t0 t1
  t0=$(date +%s)
  if [ "${3:-}" = "1" ]; then
    D75_PERSIST=1 D75_LIVE="$LIVE" timeout 3600 "$2" >> "$OUT" 2>&1
  else
    D75_LIVE="$LIVE" timeout 3600 "$2" >> "$OUT" 2>&1
  fi
  local rc=$?
  t1=$(date +%s)
  echo "ARM $1 rc=$rc elapsed=$((t1 - t0))s" >> "$OUT"
  [ "$rc" -eq 0 ] || echo "⛔ ARM $1 DID NOT COMPLETE — its rows are not a result" >> "$OUT"
}

# EIGHT arms: each cell twice, and the second round runs the four in REVERSE.
#
# The first version of this ran each cell once, in a fixed order, and the result was unusable: the
# two OFF controls — the arm the change cannot affect — disagreed by up to 2.4x
# (`bench/d81_curve_raw.txt`). A single pass cannot tell a real effect from that, and it cannot
# tell an effect of L from an effect of WHERE IN THE RUN a cell happened to land, because L and
# position advance together inside each arm.
#
# Reversing the arm order in round 2 puts every cell at a different point in the window, so a
# quantity that is a property of the code appears twice and one that is a property of the
# afternoon does not. It does NOT reverse the L axis inside an arm — that is the harness's own
# loop — so the L-versus-position confound within an arm remains, and is named in the report
# rather than claimed to be solved.
arm "R1 BEFORE/OFF" "$BEFORE" ""
arm "R1 BEFORE/ON"  "$BEFORE" 1
arm "R1 AFTER/OFF"  "$AFTER"  ""
arm "R1 AFTER/ON"   "$AFTER"  1
arm "R2 AFTER/ON"   "$AFTER"  1
arm "R2 AFTER/OFF"  "$AFTER"  ""
arm "R2 BEFORE/ON"  "$BEFORE" 1
arm "R2 BEFORE/OFF" "$BEFORE" ""

echo "ALL ARMS DONE $(date -u +%FT%TZ)" >> "$OUT"
