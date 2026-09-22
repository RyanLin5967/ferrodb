#!/bin/bash
# D123 last two arms, in priority order, each queued on the measure lock (acquire, never poll).
#   40_novelty  — Amendment 7's intervention, the only thing standing between Round 3 and soundness
#   50_f1_clean — F1 again, because only 5 of the previous 18 paired draws fall outside a declared
#                 contamination window. Now carries per-rep UTC stamps so any later declaration can
#                 be intersected with the raw matrix exactly instead of reconstructed from mtimes.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
BIN=$W/target/release/examples/d123_serial_attribution
OUT=$W/bench/d123_final; mkdir -p "$OUT"
arm () {  # arm <tag> <file> <mode> <N> <threads> <warm> <reps> <timeout>
  # ⛔ RE-QUEUE RATHER THAN GIVE UP. measure-lock.sh refuses after 240 x 15 s = 60 min, and a
  # legitimate holder (a full per-target suite over 27 commits) can exceed that. Giving up there
  # would lose the arm silently while the box was merely busy, which is indistinguishable in the
  # log from the arm never having been wanted. Three attempts = 3 h of patience, then refuse loudly.
  local try=0
  while [ $try -lt 3 ]; do
    ~/wt/logs/measure-lock.sh acquire "$1" > "$OUT/${2%.txt}_lock.txt" 2>&1
    grep -q ACQUIRED "$OUT/${2%.txt}_lock.txt" && break
    try=$((try+1)); echo "[$(date +%T)] acquire attempt $try for $2 timed out; re-queuing"
  done
  grep -q ACQUIRED "$OUT/${2%.txt}_lock.txt" || { echo "NO LOCK for $2 after 3 attempts - DO NOT REPORT A NUMBER"; return 1; }
  ( echo "# lock: $(cat "$OUT/${2%.txt}_lock.txt")"
    echo "# acquired_utc: $(date -u +%FT%TZ)"
    echo "# other cargo/rustc/suite at acquire:"
    pgrep -fl 'cargo|rustc|verify-suite' 2>/dev/null | grep -v d123_serial | sed 's/^/#   /' | head -8
    echo "# FERRODB_D123_WARM=$6  FERRODB_D123_REPS=$7"
    echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
    FERRODB_D123_WARM=$6 FERRODB_D123_REPS=$7 timeout "$8" "$BIN" "$3" "$4" "$5" 2>&1
    echo "# harness_exit=$?"
    echo "# released_utc: $(date -u +%FT%TZ)"
    echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/$2" 2>&1
  if ~/wt/logs/measure-lock.sh status | grep -q "^$1 "; then ~/wt/logs/measure-lock.sh release "$1"; fi
  echo "[$(date +%T)] done $2"
}
arm d123-nov2 40_novelty.txt  novelty 8000 1,64 20000 5 3000
arm d123-f1c  50_f1_clean.txt f1      8000 64   20000 9 3000
echo "LAST ARMS COMPLETE"
