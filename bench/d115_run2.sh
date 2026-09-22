#!/bin/bash
# D115 — the corrected before-curve: mem store (reproducing the row's own configuration) AND the
# durable store production actually ships, under one lock acquisition.
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
timeout 2400 cargo build --release --example d115_stage_all_phases 2>&1 | tail -20 || {
  echo "⛔ BUILD FAILED"; exit 1; }
BIN=target/release/examples/d115_stage_all_phases
[ -x "$BIN" ] || { echo "⛔ $BIN missing"; exit 1; }

echo "[d115] mem arm at $(date -u +%FT%TZ)"
D115_LOG=mem D115_REPS=3 timeout 1800 "$BIN" > bench/d115_before.txt 2>&1
echo "[d115] mem rc=$?"

echo "[d115] durable arm (the SHIPPED store) at $(date -u +%FT%TZ)"
D115_LOG=durable D115_REPS=1 timeout 3600 "$BIN" > bench/d115_before_durable.txt 2>&1
echo "[d115] durable rc=$?"
echo "[d115] done at $(date -u +%FT%TZ)"
