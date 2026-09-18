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
#
# ARM ORDER ALTERNATES over 4 reps, and that is a fix rather than a flourish. The first version of
# this script ran BASE then C1 in a FIXED order, and the machine's load drifted monotonically
# during the run -- so C1 sat at the higher load in every rep. Interleaving only cancels a bias
# that is constant in time; against a DRIFT it leaves a systematic POSITION bias. Alternating the
# order means each arm runs first twice and second twice, so a monotonic drift cancels to first
# order. See bench/d35_c1_factorial_SUPERSEDED_rising_load.txt for the run that got this wrong.
#
# It also REFUSES to start while a live pid holds /tmp/ferrodb-suite.lock. An empty process table
# is NOT proof of quiet: between targets a per-target suite is a bash script with no cargo child.
set -uo pipefail
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

OUT=${OUT:-bench/d35_c1_pagetable.txt}
BASE_BIN=${BASE_BIN:?set BASE_BIN to a bufpool_fault_concurrency built at the base commit}
C1_BIN=${C1_BIN:?set C1_BIN to a bufpool_fault_concurrency built at the C1 commit}
BASE_REF=${BASE_REF:-unknown}
C1_REF=${C1_REF:-$(git rev-parse --short HEAD)}

SUITE_LOCK=${SUITE_LOCK:-/tmp/ferrodb-suite.lock}
for b in "$BASE_BIN" "$C1_BIN"; do
  [ -x "$b" ] || { echo "REFUSING: $b is not an executable" >&2; exit 2; }
done

# TWO conditions, because each one alone has been measured to miss the other. The suite lock
# catches a per-target run BETWEEN targets, when it is a bash script with no cargo child at all.
# The process table catches a build or a bench that never takes the lock -- which is how
# bench/d44_pair_CONTAMINATED.txt got taken: lock free, three rustc processes running, load rising
# 12 -> 24 during the run. Matching on the EXECUTABLE (ps -eo comm) and never on the command line,
# because `pgrep -f cargo` matches anything that merely spells it, this script included.
quiet_or_refuse() {
  if [ -d "$SUITE_LOCK" ]; then
    local owner pid
    owner=$(cat "$SUITE_LOCK/owner" 2>/dev/null || echo "unknown")
    pid=${owner%% *}
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "REFUSING: a suite is running and holds $SUITE_LOCK ($owner)." >&2
      echo "  Numbers taken beside a compiling, linking, TCP-binding test suite are not quotable." >&2
      echo "  Wait for it, then re-run. Do not bypass this by deleting the lock." >&2
      exit 3
    fi
    echo "# note: $SUITE_LOCK is present but its pid is dead (stale): $owner" >&2
  fi
  local builders
  builders=$(ps -eo comm | grep -cE '^(.*/)?(cargo|rustc)$')
  if [ "$builders" -gt 0 ]; then
    echo "REFUSING: $builders cargo/rustc process(es) are running." >&2
    echo "  A compile alongside the sweep depresses the 1-thread point hardest, and 16T/1T is" >&2
    echo "  most sensitive exactly there. Wait for them, then re-run." >&2
    exit 3
  fi
}
quiet_or_refuse

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
  local rep=$1 pos=$2 name=$3 label=$4
  shift 4
  local bin
  case "$name" in BASE) bin=$BASE_BIN ;; C1) bin=$C1_BIN ;; *) echo "unknown arm $name" >&2; exit 2 ;; esac
  {
    echo "===== rep $rep | $name | $label ====="
    echo "# position in rep: $pos of 2"
    echo "# loadavg at launch: $(uptime | sed 's/.*averages*: //')"
  } >> "$OUT"
  timeout 900 "$bin" "$@" >> "$OUT" 2>&1
  local rc=$?
  {
    echo "# loadavg at finish: $(uptime | sed 's/.*averages*: //')"
    echo "HARNESS_EXIT=$rc"
    echo
  } >> "$OUT"
}

# Alternating order: each arm runs first twice and second twice.
ORDERS=("BASE C1" "C1 BASE" "BASE C1" "C1 BASE")

for rep in 1 2 3 4; do
  quiet_or_refuse
  pos=0
  for arm in ${ORDERS[$((rep-1))]}; do
    pos=$((pos+1))
    run_arm "$rep" "$pos" "$arm" "RESIDENT (every fetch is a cache HIT)" "${RESIDENT_ARGS[@]}"
  done
done

for rep in 1 2 3 4; do
  quiet_or_refuse
  pos=0
  for arm in ${ORDERS[$((rep-1))]}; do
    pos=$((pos+1))
    run_arm "$rep" "$pos" "$arm" "OVERSUBSCRIBED, MODELLED IO 500us per read" "${OVERSUB_ARGS[@]}"
  done
done

echo "WROTE $OUT"
