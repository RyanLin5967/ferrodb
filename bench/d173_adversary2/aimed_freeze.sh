#!/bin/sh
# D173 natural-rate probe: AIMED freeze, with SUSTAINED background freezes in the gaps.
#
# Three measured facts forced this shape:
#  * Periodic freezes alone put the lease death in `hold_leader` 48/48 and inside `merge` 0/48 --
#    `merge` is well under 1% of this test's wall clock on a fast box.
#  * An aimed freeze alone reaches the window (proposed=1) 3/3, then leaves the box FAST, so a peer
#    wins the election and `hold_leader(original)` never returns (the 354 168 ms behaviour).
#  * Aimed + periodic in ONE loop diluted the aim to a 1.4 s detection latency and reached the
#    window 0/4 -- the merge was over before the watcher noticed.
# So the watch stays TIGHT (no sleep on the detection path) and the periodic freeze is time-gated.
# Nothing is faked: a real SIGSTOP of a real process, election.rs's own lease, the unmodified test.
BIN="$1"; OUT="$2"; FILTER="$3"; AIM="$4"; STOP="$5"; EVERY="$6"
: > "$OUT"
"$BIN" --nocapture --test-threads=1 "$FILTER" > "$OUT" 2>&1 &
PID=$!
trap 'kill -CONT $PID 2>/dev/null' EXIT INT TERM
SEEN=0; LAST=$(date +%s)
while kill -0 "$PID" 2>/dev/null; do
  N=$(grep -c "pre-merge" "$OUT" 2>/dev/null || echo 0)
  if [ "$N" -gt "$SEEN" ]; then
    SEEN="$N"
    kill -STOP "$PID" 2>/dev/null; sleep "$AIM"; kill -CONT "$PID" 2>/dev/null
    LAST=$(date +%s); continue
  fi
  NOW=$(date +%s)
  if [ $((NOW - LAST)) -ge "$EVERY" ]; then
    kill -STOP "$PID" 2>/dev/null; sleep "$STOP"; kill -CONT "$PID" 2>/dev/null
    LAST=$NOW
  fi
done
wait "$PID"
echo "exit=$?"
