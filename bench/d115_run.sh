#!/bin/bash
# D115 — build and run the stage_all phase-attribution harness under the fleet's measure lock.
#
# ⛔ The lock is acquired ONCE around the build AND the run. Two acquisitions would let another
# measurer in between them, and a run whose binary was built while someone else was timing is a
# run whose build contended with their measurement.
#
# The acquire is re-queued up to 3 times. `measure-lock.sh acquire` gives up after 60 minutes and
# returns non-zero; a single refusal accepted silently leaves the work undone in a log that looks
# like nobody wanted it. A bounded wait may only ever expire into a REFUSAL — never into
# "proceed anyway" — so the last failure exits 1 and writes no numbers.
# `pipefail` is load-bearing: the build is piped through `tail`, and without it the pipeline's
# status is tail's. The first version of this script read a FAILED build as a successful one and
# only noticed because the binary was missing afterwards.
set -u -o pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-D115-frame-clone || exit 1

LOCK=$HOME/wt/logs/measure-lock.sh
NAME=D115
OUT=${1:-bench/d115_before.txt}

acquired=0
for attempt in 1 2 3; do
  echo "[d115] acquire attempt $attempt at $(date -u +%FT%TZ)"
  if "$LOCK" acquire "$NAME"; then acquired=1; break; fi
  echo "[d115] attempt $attempt refused; re-queueing"
done
if [ "$acquired" -ne 1 ]; then
  echo "⛔ REFUSED: could not acquire the measure lock in 3 x 60 min. No numbers produced."
  exit 1
fi
trap '"$LOCK" release "$NAME"' EXIT INT TERM

echo "[d115] building at $(date -u +%FT%TZ)"
if ! timeout 2400 cargo build --release --example d115_stage_all_phases 2>&1 | tail -40; then
  echo "⛔ BUILD FAILED or timed out. No numbers produced."
  exit 1
fi
BIN=target/release/examples/d115_stage_all_phases
[ -x "$BIN" ] || { echo "⛔ $BIN missing after a successful build"; exit 1; }

echo "[d115] running at $(date -u +%FT%TZ)"
timeout 3600 "$BIN" > "$OUT" 2>&1
rc=$?
echo "[d115] harness rc=$rc, output in $OUT"
tail -5 "$OUT"
exit "$rc"
