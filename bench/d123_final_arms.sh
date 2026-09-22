#!/bin/bash
# D123 final arms, on the binary that actually contains them (8665941).
#   novelty   — Amendment 7's INTERVENTION: same upsert call, fixed key vs new key per fork.
#   f1 paired — F1 with the raw rep x level matrix and within-rep pairing.
# Both regimes for F1 because the warm=0 arm is the one the verdict rests on and its first run fell
# inside another agent's declared cargo window (bench/d123_rerun/00_CONTAMINATION_diff_identity.txt).
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
BIN=$W/target/release/examples/d123_serial_attribution
OUT=$W/bench/d123_final; mkdir -p "$OUT"
~/wt/logs/measure-lock.sh acquire d123-final > "$OUT/00_lock.txt" 2>&1
grep -q ACQUIRED "$OUT/00_lock.txt" || { echo "NO LOCK - DO NOT REPORT A NUMBER"; cat "$OUT/00_lock.txt"; exit 1; }
release_if_mine () {
  if ~/wt/logs/measure-lock.sh status | grep -q '^d123-final '; then
    ~/wt/logs/measure-lock.sh release d123-final
  else echo "NOT RELEASING: the lock is not ours"; fi
}
trap release_if_mine EXIT
cat "$OUT/00_lock.txt"
{ echo "=== box at acquire ==="; date -u +%FT%TZ; uptime | sed 's/.*averages*: *//'
  echo "--- other cargo/rustc/suite processes (the lock excludes MEASURERS, not builds) ---"
  pgrep -fl 'cargo|rustc|verify-suite' 2>/dev/null | grep -v d123_serial_attribution | head -20
} > "$OUT/00_box_state.txt" 2>&1
run () {  # run <file> <mode> <N> <threads> <warm> <reps> <timeout>
  ( echo "# FERRODB_D123_WARM=$5  FERRODB_D123_REPS=$6"
    echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
    FERRODB_D123_WARM=$5 FERRODB_D123_REPS=$6 timeout "$7" "$BIN" "$2" "$3" "$4" 2>&1
    echo "# harness_exit=$?"
    echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/$1" 2>&1
  echo "[$(date +%T)] done $1"
}
# novelty is being run by the lead under lock d123-novelty on this same binary (8665941);
# not duplicated here. Result: bench/d123_novelty/novelty.txt
run 20_f1_paired_warm0.txt  f1    8000 64   0     9 3000
run 30_f1_paired_warm20k.txt f1   8000 64   20000 9 3000
{ echo "=== box at release ==="; date -u +%FT%TZ; uptime | sed 's/.*averages*: *//'; } >> "$OUT/00_box_state.txt"
echo "FINAL ARMS COMPLETE"
