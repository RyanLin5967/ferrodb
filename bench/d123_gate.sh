#!/bin/bash
# D123 precondition gate, then the battery.
#
# ⛔ WHY THIS EXISTS. The measure lock cannot answer "is the holder alive?": measure-lock.sh writes
# its OWN pid (`$$`) into $LOCK/owner and then exits, so that pid is dead for every holder within
# milliseconds of acquisition. Waiting on the lock alone therefore does two wrong things — it reads
# a live holder as stale, and (via `age > 2700 -> rm -rf $LOCK` in EVERY waiter) it eventually
# breaks that live holder's exclusivity. So this gate waits on the things that actually advance:
# a real pid, and a named suite process.
#
# It does NOT gate on load average. The measure lock itself had a load gate and it deadlocked the
# fleet — this box runs 13 build agents and load sits at 22-60, so a load threshold is a guard that
# never allows. Load is STAMPED instead, and every duration is reported as an upper bound.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
W=/Users/idide/wt/ferrodb-D123-serial-attribution
DEADLINE=$(( $(date +%s) + 10800 ))   # 3h budget for the whole gate

note () { echo "[$(date +%T)] $*"; }

wait_pid () {   # wait_pid <pid> <label>
  if kill -0 "$1" 2>/dev/null; then
    note "WAITING on $2 (pid $1, alive)"
    while kill -0 "$1" 2>/dev/null; do
      [ "$(date +%s)" -gt "$DEADLINE" ] && { note "TIMED OUT waiting on $2 - DO NOT REPORT A NUMBER"; exit 1; }
      sleep 30
    done
    note "$2 exited"
  else
    note "$2 (pid $1) already gone"
  fi
}

wait_pat () {   # wait_pat <regex> <label>   -- bracket the regex so pgrep cannot match itself
  while pgrep -f "$1" >/dev/null 2>&1; do
    note "WAITING on $2 ($(pgrep -f "$1" | tr '\n' ' '))"
    [ "$(date +%s)" -gt "$DEADLINE" ] && { note "TIMED OUT waiting on $2 - DO NOT REPORT A NUMBER"; exit 1; }
    sleep 30
  done
  note "$2 clear"
}

note "gate start; load $(uptime | sed 's/.*averages*: *//')"
wait_pid 51494 "d101-rerun (the REAL lock holder; the pid in the lock file is always dead)"
wait_pat 'd101_rerun[.]sh'  'any d101_rerun'
wait_pat 'verify-suite[.]sh' 'gate15 per-target suite'
note "preconditions clear; load $(uptime | sed 's/.*averages*: *//')"
exec bash "$W/bench/d123_run_all.sh"
