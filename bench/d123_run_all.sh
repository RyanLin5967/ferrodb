#!/bin/bash
# D123 full battery: BOTH regimes under ONE lock acquisition.
#
#   pass A  warm=20,000  — a tree with real depth, matching `serial_section_profile`'s regime
#   pass B  warm=0       — matching `fork_concurrency.rs`'s regime exactly (Amendment 4)
#
# One acquisition rather than two because re-queuing on a contended fleet lock between passes can
# take longer than the measurement, and because two passes separated by an hour of other agents'
# load are not comparable to each other.
#
# `perturb` runs FIRST: if the instrument moves the number it reports, every duration after it is
# quarantined and the rest of the battery is worthless.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
BIN=$W/target/release/examples/d123_serial_attribution

~/wt/logs/measure-lock.sh acquire d123-attribution > /tmp/d123_lock.txt 2>&1
if ! grep -q ACQUIRED /tmp/d123_lock.txt; then
  echo "NO LOCK - DO NOT REPORT A NUMBER"; cat /tmp/d123_lock.txt; exit 1
fi
# ⛔ Release only what we own: `release` is an unconditional `rm -rf`, so a trap firing while
# ANOTHER agent holds the lock destroys their exclusivity mid-run.
release_if_mine () {
  if ~/wt/logs/measure-lock.sh status | grep -q '^d123-attribution '; then
    ~/wt/logs/measure-lock.sh release d123-attribution
  else echo "NOT RELEASING: the lock is not ours"; fi
}
trap release_if_mine EXIT

pass () {   # pass <outdir> <warm>
  local OUT=$W/bench/$1; mkdir -p "$OUT"
  cp /tmp/d123_lock.txt "$OUT/00_lock.txt"
  export FERRODB_D123_WARM=$2
  run () {  # run <file> <mode> <N> <threads> <timeout>
    ( echo "# FERRODB_D123_WARM=$FERRODB_D123_WARM"
      echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
      timeout "$5" "$BIN" "$2" "$3" "$4" 2>&1; echo "# harness_exit=$?"
      echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/$1" 2>&1
    echo "  done $1 ($(grep -c . "$OUT/$1") lines)"
  }
  echo "=== PASS $1 (warm=$2) ==="
  run 10_perturb.txt perturb 8000  1,64               900
  run 20_threads.txt threads 16000 1,8,64,128,256,512 1800
  run 30_phases.txt  phases  8000  1,8,64             900
  run 40_stub.txt    stub    8000  1,64               1800
  run 50_extra.txt   extra   8000  1,64               1800
}

pass d123_raw       20000
pass d123_raw_warm0 0
echo "BATTERY COMPLETE"
