#!/usr/bin/env bash
# D35 C1 FACTORIAL: is the page table the wall, or one of TWO walls in series?
#
# WHY THIS RUN EXISTS. The first C1 measurement (bench/d35_c1_pagetable.txt) disagrees with the
# D35 gate (bench/d35_gate_stubtouch.txt on D35-gate-stubtouch, ad46887). The gate's C1 arm reports
# 16T/1T = x0.936 with the curve rising monotonically; a C1 that RETAINS `touch` reports x0.137,
# still collapsing. Two explanations fit and only a measurement separates them:
#
#   (a) the implementations differ -- this mirror is tagged and masked, the gate's was indexed by
#       page id directly -- and the difference costs the shape change; or
#   (b) `touch` and the page table are TWO serialising points IN SERIES, so removing either one
#       alone leaves the other binding, and the gate's C1 arm saw a shape change because it had
#       BOTH removed: its C1 was built on top of its STUB.
#
# Explanation (b) is consistent with the gate's own numbers -- STUB alone was x0.129, marginally
# WORSE than BASE -- but it is not what the gate's entry concluded, and a mechanism that has not
# itself been checked is not a finding. So this measures all four cells rather than arguing:
#
#              touch KEPT        touch DELETED
#   no mirror    BASE               STUB
#   mirror       C1                 C1STUB
#
# Under (b), C1STUB reproduces the gate's rising curve and the other three all collapse. Under (a),
# C1STUB collapses too and the difference is in this implementation, not in `touch`.
#
# ⛔ STUB and C1STUB are MEASUREMENT SCAFFOLDS AND MUST NEVER BE MERGED. Deleting `touch` degrades
# ARC's recency -- bench/d35_c1_evictiontrace.txt shows it changes the eviction sequence outright.
#
# READ THE SLOPE, NOT THE MULTIPLIER: the machine is shared and multipliers do not reproduce.
set -uo pipefail
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

OUT=${OUT:-bench/d35_c1_factorial.txt}
BASE_BIN=${BASE_BIN:?}; STUB_BIN=${STUB_BIN:?}; C1_BIN=${C1_BIN:?}; C1STUB_BIN=${C1STUB_BIN:?}
for b in "$BASE_BIN" "$STUB_BIN" "$C1_BIN" "$C1STUB_BIN"; do
  [ -x "$b" ] || { echo "REFUSING: $b is not an executable" >&2; exit 2; }
done

# Byte-identical to the RESIDENT arm of bench/s22_bufpool_before_after.txt and of
# bench/d35_gate_stubtouch.txt, so all three files are comparable.
ARGS=(--resident 200000 1,2,4,8,16 0 3)

{
  echo "# D35 C1 factorial: {page-table mirror} x {arc_cache touch}, RESIDENT arm, interleaved"
  echo "# generated $(date -Iseconds)"
  echo "# host: $(sysctl -n hw.ncpu) cores, load average at start: $(uptime | sed 's/.*averages*: //')"
  echo "#"
  echo "# BASE   = c56127f unmodified                     (no mirror, touch kept)"
  echo "# STUB   = c56127f with touch deleted             (no mirror, touch deleted)  SCAFFOLD"
  echo "# C1     = $(git rev-parse --short HEAD) as it will merge          (mirror,    touch kept)"
  echo "# C1STUB = that, plus touch deleted               (mirror,    touch deleted)  SCAFFOLD"
  echo "#"
  echo "# Everything else, harness included, is byte-identical across all four binaries."
  echo "# The criterion is the SIGN OF THE SLOPE (16T/1T), not the multiplier."
  echo
} > "$OUT"

run_arm() {
  local rep=$1 name=$2 bin=$3
  {
    echo "===== rep $rep | $name | RESIDENT (every fetch is a cache HIT) ====="
    echo "# loadavg at launch: $(uptime | sed 's/.*averages*: //')"
  } >> "$OUT"
  timeout 900 "$bin" "${ARGS[@]}" >> "$OUT" 2>&1
  echo "HARNESS_EXIT=$?" >> "$OUT"
  echo >> "$OUT"
}

for rep in 1 2 3; do
  run_arm "$rep" BASE   "$BASE_BIN"
  run_arm "$rep" STUB   "$STUB_BIN"
  run_arm "$rep" C1     "$C1_BIN"
  run_arm "$rep" C1STUB "$C1STUB_BIN"
done

echo "WROTE $OUT"
