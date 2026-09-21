#!/bin/bash
# D98 — produce bench/d98_outer_runtime_lock.txt.
#
# WHY THIS IS A SCRIPT AND NOT A SHELL HISTORY. The result is a BEFORE/AFTER pair taken on a box
# that never goes quiet, so the ORDER the two binaries run in is part of the instrument. Written
# down, the order can be checked; typed twice, it cannot.
#
# TWO BINARIES, ONE WINDOW. `examples/outer_runtime_lock.rs` drives the real `LeaseThread` through
# the real production wiring, so "before" and "after" cannot be two arms of one process — they are
# different code. They are therefore two builds of the SAME harness source, run alternately inside
# one acquisition of the fleet's measure lock:
#
#   A B B A     A = the binary built at the parent of the fix, B = the binary built at the fix.
#
# ABBA, not ABAB: a linear drift in machine load cancels in an ABBA pair and does not in ABAB. The
# arm order INSIDE each invocation rotates with the rep index on top of that (a Latin square over
# the five arms), because the arms are what the control comparison rests on.
#
# THE LOCK IS NOT OPTIONAL. `~/wt/logs/measure-lock.sh` refuses rather than warns: it waits for the
# fleet's timing lock AND for load to fall below 8, and exits non-zero if either fails. This script
# records what it said in the artifact instead of deciding for the reader — a run taken at load 40
# is a curve of the fleet, and the header has to say so.
#
#   bench/d98_run.sh <before-binary> <after-binary> <out-dir>
set -u

BEFORE="${1:?before binary}"
AFTER="${2:?after binary}"
OUT="${3:?output directory}"
mkdir -p "$OUT"

# Small N is cheap enough for three reps; N=10^5 costs ~6 minutes of fixture per rep, so it gets
# one per invocation and earns its statistics from the ABBA repetition instead.
#   args: N[,N] K_denom K_fixed probe_us reps warmup_ms fixture_threads clients
SMALL_ARGS="1000,10000 100 64 50 3 200 8 32"
# 500 us at 10^5, not 50: the free arm probes for the whole ~50 s sweep and 32 clients at 50 us
# would hold ~600 MB of samples. 500 us still resolves a hold of tens of milliseconds with ~100
# samples per client, and the harness REFUSES a cell no probe overlapped rather than calling it
# fast — so a spacing that turned out to be too coarse reports itself.
LARGE_ARGS="100000 100 64 500 1 200 8 32"

run() {  # run <tag> <binary> <args...>
  local tag="$1" bin="$2"; shift 2
  echo "=== $tag $(date -u +%FT%TZ) :: $bin $* ===" >&2
  timeout 5400 "$bin" "$@" > "$OUT/$tag.txt" 2> "$OUT/$tag.err"
  echo "$tag exit=$? $(grep -c '^ *[0-9]' "$OUT/$tag.txt" || true) data rows" >&2
}

echo "measure-lock: $(~/wt/logs/measure-lock.sh acquire D98 2>&1)" | tee "$OUT/lock.txt"
echo "load at start: $(uptime)" >> "$OUT/lock.txt"

for round in 1 2; do
  if [ "$round" = 1 ]; then order="before after"; else order="after before"; fi
  for who in $order; do
    case "$who" in
      before) bin="$BEFORE" ;;
      after)  bin="$AFTER" ;;
    esac
    run "small-$who-r$round" "$bin" $SMALL_ARGS
    run "large-$who-r$round" "$bin" $LARGE_ARGS
  done
done

echo "load at end: $(uptime)" >> "$OUT/lock.txt"
~/wt/logs/measure-lock.sh release D98 >> "$OUT/lock.txt" 2>&1
echo "done" >&2
