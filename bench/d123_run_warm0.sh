#!/bin/bash
# D123 battery, SECOND PASS: warm=0, which is `fork_concurrency.rs`'s exact regime (empty catalog,
# tree grows during the timed window). Amendment 4. Same binary, no rebuild — the knob is env only.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
export FERRODB_D123_WARM=0
W=/Users/idide/wt/ferrodb-D123-serial-attribution
OUT=$W/bench/d123_raw_warm0
mkdir -p "$OUT"
~/wt/logs/measure-lock.sh acquire d123-warm0 | tee "$OUT/00_lock.txt"
grep -q ACQUIRED "$OUT/00_lock.txt" || { echo "NO LOCK - DO NOT REPORT A NUMBER"; exit 1; }
release_if_mine () {
  if ~/wt/logs/measure-lock.sh status | grep -q '^d123-warm0 '; then
    ~/wt/logs/measure-lock.sh release d123-warm0
  else echo "NOT RELEASING: the lock is not ours"; fi
}
trap release_if_mine EXIT
BIN=$W/target/release/examples/d123_serial_attribution
run () {
  ( echo "# load_at_run_start: $(uptime | sed 's/.*averages*: *//')"
    timeout "$5" "$BIN" "$2" "$3" "$4" 2>&1; echo "# harness_exit=$?"
    echo "# load_at_run_end: $(uptime | sed 's/.*averages*: *//')" ) > "$OUT/$1" 2>&1
  echo "done $1"; tail -2 "$OUT/$1"
}
run 20_threads.txt threads 16000 1,8,64,128,256,512 1800
run 30_phases.txt  phases  8000  1,8,64             900
run 40_stub.txt    stub    8000  1,64               1800
run 50_extra.txt   extra   8000  1,64               1800
echo "WARM0 BATTERY COMPLETE"
