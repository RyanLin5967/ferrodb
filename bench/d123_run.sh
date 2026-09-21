#!/bin/bash
# D123 measurement battery. Takes the fleet's measure lock for the WHOLE battery, so no other
# agent times anything in the middle of it, and stamps load_at_acquire on the output.
#
# Order is deliberate: `perturb` first, because if the instrument moves the number it reports then
# every duration after it is quarantined and the rest of the battery is worthless.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
OUT=$W/bench/d123_raw
mkdir -p "$OUT"

~/wt/logs/measure-lock.sh acquire d123-attribution | tee "$OUT/00_lock.txt"
if ! grep -q ACQUIRED "$OUT/00_lock.txt"; then
  echo "NO LOCK - DO NOT REPORT A NUMBER"; exit 1
fi
# ⛔ RELEASE ONLY WHAT WE OWN. `measure-lock.sh release` is an unconditional `rm -rf` of the lock
# directory, so a trap that fires while ANOTHER agent holds it destroys their exclusivity in the
# middle of their run. Ordering already prevents that here (the trap is installed after a
# successful acquire), but ordering is an argument and this is a check.
release_if_mine () {
  if ~/wt/logs/measure-lock.sh status | grep -q '^d123-attribution '; then
    ~/wt/logs/measure-lock.sh release d123-attribution
  else
    echo "NOT RELEASING: the lock is not ours"
  fi
}
trap release_if_mine EXIT

BIN=$W/target/release/examples/d123_serial_attribution
run () {  # run <file> <mode> <N> <threads> <timeout>
  echo "=== $2 N=$3 T=$4 ==="
  ( echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
    timeout "$5" "$BIN" "$2" "$3" "$4" 2>&1
    echo "# harness_exit=$?"
    echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')"
  ) > "$OUT/$1" 2>&1
  tail -3 "$OUT/$1"
}

run 10_perturb.txt perturb 8000  1,64             900
run 20_threads.txt threads 16000 1,8,64,128,256,512 1800
run 30_phases.txt  phases  8000  1,8,64           900
run 40_stub.txt    stub    8000  1,64             1800
run 50_extra.txt   extra   8000  1,64             1800

echo "BATTERY COMPLETE"
