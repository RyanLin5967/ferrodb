#!/bin/bash
# F1 clean, attempt 2 (51). Queued LAST so that nothing of mine builds during it.
# ⛔ NO BUILD MAY BE ISSUED FROM THIS WORKTREE WHILE THIS IS QUEUED OR RUNNING. The previous
# attempt was contaminated by its own author's cargo invocations 2 m 50 s into the window.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
BIN=$W/target/release/examples/d123_serial_attribution
OUT=$W/bench/d123_final; mkdir -p "$OUT"
try=0
while [ $try -lt 4 ]; do
  ~/wt/logs/measure-lock.sh acquire d123-f1c2 > "$OUT/51_lock.txt" 2>&1
  grep -q ACQUIRED "$OUT/51_lock.txt" && break
  try=$((try+1)); echo "[$(date +%T)] acquire attempt $try timed out; re-queuing"
done
grep -q ACQUIRED "$OUT/51_lock.txt" || { echo "NO LOCK after 4 attempts - DO NOT REPORT A NUMBER"; exit 1; }
release_if_mine () {
  ~/wt/logs/measure-lock.sh status | grep -q '^d123-f1c2 ' && ~/wt/logs/measure-lock.sh release d123-f1c2 || echo "NOT RELEASING: not ours"
}
trap release_if_mine EXIT
# Wait for the box to actually settle. This is an OBSERVATION gate with a floor, not the
# unreachable threshold that once deadlocked the fleet: it waits at most 10 minutes and then
# proceeds anyway, stamping whatever the load was.
for _ in $(seq 1 40); do
  l=$(uptime | sed 's/.*averages*: *//' | awk '{print int($1)}')
  [ "${l:-99}" -le 6 ] && break
  sleep 15
done
( echo "# lock: $(cat "$OUT/51_lock.txt")"
  echo "# acquired_utc: $(date -u +%FT%TZ)"
  echo "# waited for load<=6 before starting; load now: $(uptime | sed 's/.*averages*: *//')"
  pgrep -fl 'cargo test|cargo build|verify-suite' 2>/dev/null | grep -v d123_serial | sed 's/^/#   /' | head -6
  echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
  FERRODB_D123_WARM=20000 FERRODB_D123_REPS=9 timeout 3000 "$BIN" f1 8000 64 2>&1
  echo "# harness_exit=$?"
  echo "# released_utc: $(date -u +%FT%TZ)"
  echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/51_f1_clean2.txt" 2>&1
echo "[$(date +%T)] done 51_f1_clean2.txt"
echo "F1CLEAN2 COMPLETE"
