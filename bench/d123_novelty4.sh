#!/bin/bash
# Novelty arm, paired and rotated (Amendment 10). Runs BEFORE 50_f1_clean in the queue because the
# flat-combining verdict is held on it.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
BIN=$W/target/release/examples/d123_serial_attribution
OUT=$W/bench/d123_final; mkdir -p "$OUT"
try=0
while [ $try -lt 3 ]; do
  ~/wt/logs/measure-lock.sh acquire d123-nov4 > "$OUT/41_lock.txt" 2>&1
  grep -q ACQUIRED "$OUT/41_lock.txt" && break
  try=$((try+1)); echo "[$(date +%T)] acquire attempt $try timed out; re-queuing"
done
grep -q ACQUIRED "$OUT/41_lock.txt" || { echo "NO LOCK after 3 attempts - DO NOT REPORT A NUMBER"; exit 1; }
release_if_mine () {
  ~/wt/logs/measure-lock.sh status | grep -q '^d123-nov4 ' && ~/wt/logs/measure-lock.sh release d123-nov4 || echo "NOT RELEASING: not ours"
}
trap release_if_mine EXIT
( echo "# lock: $(cat "$OUT/41_lock.txt")"
  echo "# acquired_utc: $(date -u +%FT%TZ)"
  echo "# NOTE: the '# other ...' pgrep below OVER-REPORTS - it matches shells that merely mention"
  echo "#       cargo. load_at_* is the trustworthy field."
  pgrep -fl 'cargo test|cargo build|verify-suite' 2>/dev/null | grep -v d123_serial | sed 's/^/#   /' | head -6
  echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
  FERRODB_D123_WARM=20000 FERRODB_D123_REPS=5 timeout 3000 "$BIN" novelty 8000 1,64 2>&1
  echo "# harness_exit=$?"
  echo "# released_utc: $(date -u +%FT%TZ)"
  echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/41_novelty_paired.txt" 2>&1
echo "[$(date +%T)] done 41_novelty_paired.txt"
echo "NOVELTY4 COMPLETE"
