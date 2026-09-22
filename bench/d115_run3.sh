#!/bin/bash
# D115 — the before-curve on the CONFIGURED runtime (D101 fixed in the harness), both stores.
set -u -o pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-D115-frame-clone || exit 1
LOCK=$HOME/wt/logs/measure-lock.sh
NAME=D115
acquired=0
for attempt in 1 2 3; do
  echo "[d115] acquire attempt $attempt at $(date -u +%FT%TZ)"
  if "$LOCK" acquire "$NAME"; then acquired=1; break; fi
done
[ "$acquired" -eq 1 ] || { echo "⛔ REFUSED: no lock after 3 x 60 min. No numbers produced."; exit 1; }
trap '"$LOCK" release "$NAME"' EXIT INT TERM

echo "[d115] building at $(date -u +%FT%TZ)"
timeout 2400 cargo build --release --example d115_stage_all_phases 2>&1 | tail -25 || {
  echo "⛔ BUILD FAILED"; exit 1; }
BIN=target/release/examples/d115_stage_all_phases
[ -x "$BIN" ] || { echo "⛔ $BIN missing"; exit 1; }

# A short probe FIRST: the real rig fsyncs per statement in places the stub runtime did not, so
# the cost per point is unknown. Finding that out on a 3-point axis costs seconds; finding it out
# on the full axis costs the lock.
echo "[d115] probe (mem, ops=32,128, reps=1) at $(date -u +%FT%TZ)"
D115_LOG=mem D115_REPS=1 D115_OPS=32,128 timeout 900 "$BIN" > bench/d115_probe.txt 2>&1
echo "[d115] probe rc=$?"
grep -E "REFUSED|writes ms|^ +(32|128) " bench/d115_probe.txt | head -12
