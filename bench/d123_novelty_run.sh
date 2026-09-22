#!/bin/bash
# Amendment 7's intervention arm. No pgrep-based wait: the previous version blocked forever because
# `pgrep -f d123_final_arms.sh` matched MY OWN interactive shells, which had the string in their
# command lines. A name pattern cannot distinguish "the job is running" from "someone typed the
# job's name", so this waits on nothing and simply queues on the measure lock, which is the real
# mutual-exclusion primitive.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
BIN=$W/target/release/examples/d123_serial_attribution
OUT=$W/bench/d123_final; mkdir -p "$OUT"
~/wt/logs/measure-lock.sh acquire d123-nov2 > "$OUT/40_lock.txt" 2>&1
grep -q ACQUIRED "$OUT/40_lock.txt" || { echo "NO LOCK - DO NOT REPORT A NUMBER"; cat "$OUT/40_lock.txt"; exit 1; }
release_if_mine () {
  if ~/wt/logs/measure-lock.sh status | grep -q '^d123-nov2 '; then
    ~/wt/logs/measure-lock.sh release d123-nov2
  else echo "NOT RELEASING: the lock is not ours"; fi
}
trap release_if_mine EXIT
cat "$OUT/40_lock.txt"
{ echo "=== box at acquire ==="; date -u +%FT%TZ; uptime | sed 's/.*averages*: *//'
  pgrep -fl 'cargo|rustc|verify-suite' 2>/dev/null | grep -v d123_serial | head -10; } > "$OUT/40_box.txt" 2>&1
( echo "# FERRODB_D123_WARM=20000  FERRODB_D123_REPS=5"
  echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
  FERRODB_D123_WARM=20000 FERRODB_D123_REPS=5 timeout 3000 "$BIN" novelty 8000 1,64 2>&1
  echo "# harness_exit=$?"
  echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/40_novelty.txt" 2>&1
echo "[$(date +%T)] done 40_novelty.txt"
echo "NOVELTY COMPLETE"
