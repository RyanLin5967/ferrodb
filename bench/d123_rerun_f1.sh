#!/bin/bash
# D123 clean F1 re-run (Amendment 6). Probe OFF, replicated, level order rotated.
# Ordered by value so that if this is cut short, the highest-value arms are already banked.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
BIN=$W/target/release/examples/d123_serial_attribution
OUT=$W/bench/d123_rerun; mkdir -p "$OUT"

~/wt/logs/measure-lock.sh acquire d123-f1 > "$OUT/00_lock.txt" 2>&1
grep -q ACQUIRED "$OUT/00_lock.txt" || { echo "NO LOCK - DO NOT REPORT A NUMBER"; cat "$OUT/00_lock.txt"; exit 1; }
release_if_mine () {
  if ~/wt/logs/measure-lock.sh status | grep -q '^d123-f1 '; then
    ~/wt/logs/measure-lock.sh release d123-f1
  else echo "NOT RELEASING: the lock is not ours"; fi
}
trap release_if_mine EXIT
cat "$OUT/00_lock.txt"

# What else is on this box while we measure. The lock excludes other MEASURERS, not builds or
# suites, so this is recorded rather than assumed quiet.
{ echo "=== at acquire ==="; date -u +%FT%TZ; uptime | sed 's/.*averages*: *//'
  echo "--- other cargo/test/bench processes ---"
  pgrep -fl 'cargo|verify-suite|examples/' 2>/dev/null | grep -v d123_serial_attribution | head -20
} > "$OUT/00_box_state.txt" 2>&1

run () {  # run <file> <mode> <N> <threads> <warm> <reps> <timeout>
  ( echo "# FERRODB_D123_WARM=$5  FERRODB_D123_REPS=$6"
    echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
    FERRODB_D123_WARM=$5 FERRODB_D123_REPS=$6 timeout "$7" "$BIN" "$2" "$3" "$4" 2>&1
    echo "# harness_exit=$?"
    echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/$1" 2>&1
  echo "[$(date +%T)] done $1"
}

run 10_f1_warm20k.txt   f1       8000 64  20000 5 2400
run 11_f1_warm0.txt     f1       8000 64  0     5 2400
run 20_perturb2_warm0.txt  perturb2 8000 64 0     5 1800
run 21_perturb2_warm20k.txt perturb2 8000 64 20000 5 1800
for r in 1 2 3; do run 3${r}_phases_warm20k.txt phases 8000 1,64 20000 1 1800; done
for r in 1 2;   do run 4${r}_extra_warm20k.txt  extra  8000 1,64 20000 1 2400; done
{ echo "=== at release ==="; date -u +%FT%TZ; uptime | sed 's/.*averages*: *//'; } >> "$OUT/00_box_state.txt"
echo "RERUN COMPLETE"
