#!/usr/bin/env bash
# D35 C1: does taking the page table off the HIT path change the SHAPE of the scaling curve?
#
# The resident hit LOOP is fetch_page + unpin_page, and between them they took `page_table.read()`
# twice per iteration plus two frame latches and two pin_counter RMWs. A Rust RwLock's reader count
# is a single process-wide cache line that every reader atomically RMWs, so it contends exactly like
# a mutex. C1 resolves page_id -> frame through a lock-free, direct-mapped, TAGGED mirror instead.
#
# ARMS (interleaved, so both sides see the same machine load)
#   BASE  the agent-isolation tip this branch was cut from, unmodified.
#   C1    the page table off the hit path, with the frame latch, the pin and `touch` all KEPT.
#
# NOT the D35 gate's arms. That gate had a third arm with `touch` DELETED, which is not shippable --
# it degrades ARC's recency -- and its C1 arm was measured on top of that deletion. This measures a
# C1 that retains `touch`, so the absolute numbers are this file's and must not be compared against
# the gate's multiplier.
#
# READ THE SLOPE, NOT THE MULTIPLIER. D35's medians are over 3 reps with a BASE spread of
# 18.1M-23.6M at one thread under loadavg 16-33, and the two reps of s22_bufpool_before_after.txt
# disagree 40% on multipliers under fleet load. What reproduces is the SIGN of the slope.
#
# The OVERSUBSCRIBED arm is here because the mirror's maintenance cost falls on the MISS path --
# every eviction clears a slot and every fault publishes one -- and because it is the only arm that
# reports a non-zero reads_per_fetch, which is the harness's window onto hit rate.
set -uo pipefail
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

OUT=${OUT:-bench/d35_c1_pagetable.txt}
BASE_BIN=${BASE_BIN:?set BASE_BIN to a bufpool_fault_concurrency built at the base commit}
C1_BIN=${C1_BIN:?set C1_BIN to a bufpool_fault_concurrency built at the C1 commit}
BASE_REF=${BASE_REF:-unknown}
C1_REF=${C1_REF:-$(git rev-parse --short HEAD)}

for b in "$BASE_BIN" "$C1_BIN"; do
  [ -x "$b" ] || { echo "REFUSING: $b is not an executable" >&2; exit 2; }
done

# Byte-identical to the RESIDENT arm header of bench/s22_bufpool_before_after.txt and of
# bench/d35_gate_stubtouch.txt, so the three files are comparable.
RESIDENT_ARGS=(--resident 200000 1,2,4,8,16 0 3)
# Byte-identical to the "primary instrument" arm of bench/s22_bufpool_before_after.txt.
OVERSUB_ARGS=(500 1,2,4,8,16 500 1)

{
  echo "# D35 C1: page table off the hit path -- BASE vs C1, interleaved"
  echo "# generated $(date -Iseconds)"
  echo "# host: $(sysctl -n hw.ncpu) cores, load average at start: $(uptime | sed 's/.*averages*: //')"
  echo "# BASE = $BASE_REF (unmodified)"
  echo "# C1   = $C1_REF (lock-free tagged page-table mirror in fetch_page AND unpin_page)"
  echo "# Everything else, harness included, is byte-identical between the two binaries."
  echo "#"
  echo "# The criterion is the SIGN OF THE SLOPE, not the multiplier."
  echo "#"
  echo "# NOTE ON LOAD: this machine is shared with other agents. Arms are interleaved so both"
  echo "# sides see whatever load there is; the ABSOLUTE numbers are depressed by it."
  echo
} > "$OUT"

run_arm() {
  local rep=$1 name=$2 bin=$3 label=$4
  shift 4
  {
    echo "===== rep $rep | $name | $label ====="
    echo "# loadavg at launch: $(uptime | sed 's/.*averages*: //')"
  } >> "$OUT"
  timeout 900 "$bin" "$@" >> "$OUT" 2>&1
  echo "HARNESS_EXIT=$?" >> "$OUT"
  echo >> "$OUT"
}

for rep in 1 2 3; do
  run_arm "$rep" BASE "$BASE_BIN" "RESIDENT (every fetch is a cache HIT)" "${RESIDENT_ARGS[@]}"
  run_arm "$rep" C1   "$C1_BIN"   "RESIDENT (every fetch is a cache HIT)" "${RESIDENT_ARGS[@]}"
done

for rep in 1 2 3; do
  run_arm "$rep" BASE "$BASE_BIN" "OVERSUBSCRIBED, MODELLED IO 500us per read" "${OVERSUB_ARGS[@]}"
  run_arm "$rep" C1   "$C1_BIN"   "OVERSUBSCRIBED, MODELLED IO 500us per read" "${OVERSUB_ARGS[@]}"
done

echo "WROTE $OUT"
