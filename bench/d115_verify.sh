#!/bin/bash
# D115 — does the instrument change behaviour? The two modules it edits, per target.
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

for t in tel agent_sql; do
  echo "=== lib $t at $(date -u +%FT%TZ)"
  timeout 1800 cargo test --release --lib "$t::" 2>&1 | tail -12
  echo "=== $t rc=$?"
done
echo "[d115v] done at $(date -u +%FT%TZ)"
