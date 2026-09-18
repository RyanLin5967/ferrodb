#!/usr/bin/env bash
# D35 GATE: is the `arc_cache` mutex on the HIT path the binding constraint, or one of several?
#
# The RESIDENT hit LOOP is fetch_page + unpin_page, and between them they take FOUR RwLock read
# acquisitions per iteration -- `page_table.read()` twice and a frame latch twice -- plus two
# `pin_counter` RMWs, plus `arc_cache.lock()` for `touch`. An RwLock's reader count is a single
# process-wide cache line that every reader atomically RMWs, so it contends exactly like a mutex.
#
# ARMS (interleaved, same binary for STUB/C1 selected by FERRO_D35_ARM, so the harness is identical)
#   BASE  unmodified S22 lane.
#   STUB  `touch` deleted from the hit path. The UPPER BOUND on BP-Wrapper or any batching
#         scheme, since a real batcher costs more than zero.
#   C1    `touch` deleted AND page-table resolution replaced by a lock-free direct-mapped mirror,
#         in BOTH fetch_page and unpin_page. Frame latch and pin KEPT, so this arm is CORRECT.
#
# READ: if STUB does not turn the slope positive but C1 does, the `arc_cache` mutex was never the
# binding constraint and D35-as-scoped (batch the policy update) is the wrong fix.
#
# A C2 arm (mirror only, no frame latch, no pin) was attempted as an instrument ceiling and is
# omitted: it panics in setup with `allocate: NotEnoughSpace`, because a pool that never pins
# cannot keep its own warmup pages. It failed loudly rather than reporting a comfortable number,
# and C1 is a better control anyway -- it is correct, so its positive slope is a real one.
set -uo pipefail
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

OUT=bench/d35_gate_stubtouch.txt
BASE_BIN=/tmp/d35_base_bin
CTL_BIN=/tmp/d35_ctl_bin

# Matches the RESIDENT arm header of bench/s22_bufpool_before_after.txt exactly.
ARGS=(--resident 200000 1,2,4,8,16 0 3)

{
  echo "# D35 gate: does removing the arc_cache mutex from the HIT path change the SHAPE?"
  echo "# generated $(date -Iseconds)"
  echo "# host: $(sysctl -n hw.ncpu) cores, load average at start: $(uptime | sed 's/.*averages*: //')"
  echo "# base commit: $(git rev-parse --short HEAD) on $(git rev-parse --abbrev-ref HEAD)"
  echo "#"
  echo "# BASE = unmodified S22 lane (arc_cache.lock().touch() on every hit)"
  echo "# STUB = that one call deleted. Upper bound on BP-Wrapper / any batching scheme."
  echo "# C1   = STUB plus lock-free page resolution in fetch_page AND unpin_page."
  echo "#        Frame latch and pin kept, so C1 is CORRECT, not a ceiling."
  echo "#"
  echo "# The criterion is the SIGN OF THE SLOPE, not the multiplier: the two reps of"
  echo "# s22_bufpool_before_after.txt disagree 40% on the multiplier under fleet load."
  echo
} > "$OUT"

run_arm() {
  local rep=$1 name=$2 bin=$3 env=$4
  {
    echo "===== rep $rep | $name | RESIDENT (every fetch is a cache HIT) ====="
    echo "# loadavg at launch: $(uptime | sed 's/.*averages*: //')"
  } >> "$OUT"
  FERRO_D35_ARM="$env" timeout 600 "$bin" "${ARGS[@]}" >> "$OUT" 2>&1
  echo "HARNESS_EXIT=$?" >> "$OUT"
  echo >> "$OUT"
}

for rep in 1 2 3; do
  run_arm "$rep" BASE "$BASE_BIN" stub
  run_arm "$rep" STUB "$CTL_BIN"  stub
  run_arm "$rep" C1   "$CTL_BIN"  c1
done

echo "WROTE $OUT"
