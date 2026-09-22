#!/bin/bash
# Amendment 7's intervention arm, re-run. The lead's attempt died ~40 s in: novelty.txt stopped at
# the banner (608 bytes, mtime 19:59:04) and the process was gone by 19:59:46, when my own F1 arms
# took the lock. No result was produced and none is being reinterpreted.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
BIN=$W/target/release/examples/d123_serial_attribution
OUT=$W/bench/d123_final; mkdir -p "$OUT"
# Wait for MY OWN f1 arms to finish before queuing for the lock, so I never race myself.
while pgrep -f d123_final_arms.sh >/dev/null 2>&1; do sleep 20; done
~/wt/logs/measure-lock.sh acquire d123-nov2 > "$OUT/40_lock.txt" 2>&1
grep -q ACQUIRED "$OUT/40_lock.txt" || { echo "NO LOCK - DO NOT REPORT A NUMBER"; exit 1; }
release_if_mine () {
  if ~/wt/logs/measure-lock.sh status | grep -q '^d123-nov2 '; then
    ~/wt/logs/measure-lock.sh release d123-nov2
  else echo "NOT RELEASING: the lock is not ours"; fi
}
trap release_if_mine EXIT
cat "$OUT/40_lock.txt"
( echo "# FERRODB_D123_WARM=20000  FERRODB_D123_REPS=5"
  echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
  FERRODB_D123_WARM=20000 FERRODB_D123_REPS=5 timeout 3000 "$BIN" novelty 8000 1,64 2>&1
  echo "# harness_exit=$?"
  echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/40_novelty.txt" 2>&1
echo "[$(date +%T)] done 40_novelty.txt"
echo "NOVELTY COMPLETE"
