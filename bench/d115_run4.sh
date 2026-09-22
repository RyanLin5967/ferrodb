#!/bin/bash
# D115 — the DEFINITIVE before-curve, on the runtime the harness actually configures.
#
# Three arms under one lock hold:
#   1. mem store, full axis, reps=3          — comparable with the stub-runtime run
#   2. durable store (SHIPPED), full axis    — reps=1, because it fsyncs per statement
#   3. crossover, 8192 and 32768             — the mirror is a per-statement CONSTANT and the
#      clone is per-statement O(n); one of them wins below the crossover and the other above it,
#      and a curve that stops before the crossover names the wrong term.
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

timeout 2400 cargo build --release --example d115_stage_all_phases 2>&1 | tail -20 || {
  echo "⛔ BUILD FAILED"; exit 1; }
BIN=target/release/examples/d115_stage_all_phases
[ -x "$BIN" ] || { echo "⛔ $BIN missing"; exit 1; }

echo "[d115] arm 1 mem full axis reps=3 at $(date -u +%FT%TZ)"
D115_LOG=mem D115_REPS=3 timeout 1800 "$BIN" > bench/d115_real_mem.txt 2>&1
echo "[d115] arm1 rc=$?"

echo "[d115] arm 2 durable (SHIPPED) full axis reps=1 at $(date -u +%FT%TZ)"
D115_LOG=durable D115_REPS=1 timeout 3000 "$BIN" > bench/d115_real_durable.txt 2>&1
echo "[d115] arm2 rc=$?"

echo "[d115] arm 3 crossover 8192,32768 reps=1 at $(date -u +%FT%TZ)"
D115_LOG=mem D115_REPS=1 D115_OPS=8192,32768 timeout 3000 "$BIN" > bench/d115_crossover.txt 2>&1
echo "[d115] arm3 rc=$?"
echo "[d115] done at $(date -u +%FT%TZ)"
