#!/usr/bin/env bash
# bench/d42_load_sweep.sh — what load shape actually starves this test?
#
# WHY THIS EXISTS, AND IT IS A CORRECTION TO THE FALSIFIER'S OWN PREMISE. D42 pre-registered
# "run the target under deliberate oversubscription (14x) and require INCONCLUSIVE". Run
# (bench/d42_fire_starved.txt, head ddbc601): 252 pure-CPU spinners on 18 cores took the machine to
# loadavg 137 and the test PASSED in 4.76 s — barely slower than its 3.9-5.6 s unloaded. No wait
# expired, so the classifier never ran at all.
#
# That is not a failed detector. It is a failed REPRODUCTION: the precondition the falsifier needs —
# a 45 s budget actually expiring — never happened. macOS's timeshare scheduler demotes pure CPU
# spinners and keeps mostly-sleeping interactive processes responsive, which is exactly what it is
# built to do. A 10 ms sleeper asks for so little that it gets it.
#
# So this sweep measures BOTH of D42's signals directly under a range of shapes, instead of guessing
# which one reproduces. It runs the same probe as bench/d42_childcpu_probe.py, so every number is
# comparable to the quiet-machine floor the thresholds were calibrated from.
#
# THE `nice` SHAPES ARE THE POINT, and they are not a cheat. The classifier's claim is precisely
# "this process, or its children, did not get the CPU". `nice -n 20` against a loaded machine
# produces exactly that state, and it aims the break at the window the guard covers instead of
# hoping ambient load wanders into it. The `cpu`/`io` shapes stay in the sweep because what they
# show — that both signals are FLAT from loadavg 269 to 829 — is the finding that sent this row
# looking for a different lever.
#
# Reading it: a shape that drives `self-schedule` below 0.085 or `child CPU` below 0.00043 is a
# shape under which the classifier WOULD say INCONCLUSIVE. That is the shape falsifier 1 needs.
#
# It holds the machine-wide suite lock throughout, because these shapes would wreck a concurrent
# suite's timings — the very confusion this row exists to end. Memory pressure is deliberately NOT
# one of the shapes: this box is shared with an agent fleet, and inducing swap to win an argument
# would risk OOM-killing someone else's multi-hour run.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

NCPU=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
WINDOW=${WINDOW:-45}
LOCK=/tmp/ferrodb-suite.lock
IOTMP=$(mktemp -d)

load_pids=()
stop_load() {
    for p in "${load_pids[@]:-}"; do kill -9 "$p" 2>/dev/null; done
    load_pids=()
    return 0
}
cleanup() {
    stop_load
    rm -rf "$IOTMP"
    [ "${held:-0}" = 1 ] && rm -rf "$LOCK"
    return 0
}
trap cleanup EXIT INT TERM

# Each worker carries its own `timeout` as well as the trap: a trap dies with the shell, and
# hundreds of orphaned spinners would be indistinguishable from a wedged machine. `disown` keeps
# bash from printing the whole worker command line when it reaps one, which buried the first run of
# this sweep in several hundred lines of noise.
start_cpu() {
    local n=$1
    for _ in $(seq "$n"); do
        timeout 300 sh -c 'while :; do :; done' >/dev/null 2>&1 &
        load_pids+=($!); disown $! 2>/dev/null
    done
}
# Write-and-fsync in a tight loop. This is the half pure-CPU load does not have, and the half the
# nodes are exposed to: every Persist in the consensus driver fsyncs before it acknowledges.
start_io() {
    local n=$1 i
    for i in $(seq "$n"); do
        timeout 300 python3 -c "
import os,sys
f=os.open(sys.argv[1], os.O_CREAT|os.O_WRONLY, 0o600)
b=b'x'*65536
while True:
    os.pwrite(f,b,0); os.fsync(f)
" "$IOTMP/io$i" >/dev/null 2>&1 &
        load_pids+=($!); disown $! 2>/dev/null
    done
}

held=0; w=0
while ! mkdir "$LOCK" 2>/dev/null; do
    o=$(cat "$LOCK/owner" 2>/dev/null || echo unknown); p=${o%% *}
    if [ -n "$p" ] && ! kill -0 "$p" 2>/dev/null; then rm -rf "$LOCK"; continue; fi
    [ "$w" -ge 3600 ] && { echo "REFUSING — waited ${w}s for the suite lock held by: $o"; exit 3; }
    [ "$w" -eq 0 ] && echo "queued behind a running suite ($o)" >&2
    sleep 15; w=$((w+15))
done
printf '%s %s %s\n' "$$" "d42-load-sweep" "$(date -u +%FT%TZ)" > "$LOCK/owner"
held=1

cargo build --examples >/dev/null 2>&1 || { echo "REFUSING — cargo build --examples failed"; exit 1; }

echo "D42 load sweep — which shape moves the signals below their thresholds?"
echo "  when   : $(date -u +%FT%TZ)"
echo "  head   : $(git log -1 --format=%h)"
echo "  cpus   : $NCPU"
echo "  window : ${WINDOW}s per shape"
echo "  thresholds: self-schedule < 0.085 STARVED, child CPU < 0.00043 STARVED"
echo ""

shape() {
    local name=$1 cpu=$2 io=$3 nice=$4
    echo "================ shape: $name (cpu=$cpu io=$io nice=$nice) ================"
    start_cpu "$cpu"
    start_io "$io"
    [ $((cpu + io)) -gt 0 ] && sleep 20
    echo "  loadavg during: $(uptime | sed 's/.*load averages*: //')"
    if [ "$nice" = 0 ]; then
        timeout 300 python3 bench/d42_childcpu_probe.py "$WINDOW" 2>&1 \
            | grep -E "iterations|max stall|FLOOR|TOTAL|leader elected|REFUSING"
    else
        timeout 300 nice -n "$nice" python3 bench/d42_childcpu_probe.py "$WINDOW" 2>&1 \
            | grep -E "iterations|max stall|FLOOR|TOTAL|leader elected|REFUSING"
    fi
    stop_load
    echo ""
    sleep 5
}

#      name                    cpu              io   nice
shape "baseline"               0                0    0
shape "cpu-14x"                $((NCPU * 14))   0    0
shape "cpu-14x + io-64"        $((NCPU * 14))   64   0
shape "nice20 + cpu-14x"       $((NCPU * 14))   0    20
shape "nice20 + cpu-28x"       $((NCPU * 28))   0    20
shape "nice20 + cpu-28x+io-64" $((NCPU * 28))   64   20
