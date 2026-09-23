#!/bin/sh
# D173 natural-rate probe. Runs the real failing test with the WHOLE PROCESS periodically
# descheduled, which is what a loaded CI runner does to three driver loops living in one process.
# SIGSTOP is precise, bounded and reversible; the EXIT trap guarantees a SIGCONT.
BIN="$1"; OUT="$2"; FILTER="$3"; STOP="$4"; GO="$5"
"$BIN" --nocapture --test-threads=1 "$FILTER" > "$OUT" 2>&1 &
PID=$!
trap 'kill -CONT $PID 2>/dev/null' EXIT INT TERM
while kill -0 "$PID" 2>/dev/null; do
  kill -STOP "$PID" 2>/dev/null
  sleep "$STOP"
  kill -CONT "$PID" 2>/dev/null
  sleep "$GO"
done
wait "$PID"
echo "exit=$?"
