#!/bin/bash
# D115 — the whole lib suite as ONE target, with a durable result file.
# Per-target and file-backed because the fleet SIGTERMs a long `cargo test` mid-suite, and a
# suite that was killed reports as a suite that passed.
set -u -o pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-D115-frame-clone || exit 1
LOCK=$HOME/wt/logs/measure-lock.sh
NAME=D115v
acquired=0
for attempt in 1 2 3; do
  echo "[d115v] acquire attempt $attempt at $(date -u +%FT%TZ)"
  if "$LOCK" acquire "$NAME"; then acquired=1; break; fi
done
[ "$acquired" -eq 1 ] || { echo "⛔ REFUSED: no lock after 3 x 60 min."; exit 1; }
trap '"$LOCK" release "$NAME"' EXIT INT TERM

HEAD=$(git rev-parse --short HEAD)
echo "[d115v] full lib suite at HEAD=$HEAD, $(date -u +%FT%TZ)"
timeout 2700 cargo test --release --lib > /tmp/d115_libsuite.txt 2>&1
rc=$?
echo "[d115v] rc=$rc head=$HEAD"
grep -E "^test result|^error" /tmp/d115_libsuite.txt | tail -5
echo "[d115v] done at $(date -u +%FT%TZ)"
