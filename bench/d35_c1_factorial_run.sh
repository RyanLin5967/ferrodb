#!/usr/bin/env bash
# D35 C1 FACTORIAL: is the page table the wall, or one of TWO walls in series?
#
# WHY THIS RUN EXISTS. The first C1 measurement (bench/d35_c1_pagetable.txt) disagrees with the
# D35 gate (bench/d35_gate_stubtouch.txt on D35-gate-stubtouch, ad46887). The gate's C1 arm reports
# 16T/1T = x0.936 with the curve rising monotonically; a C1 that RETAINS `touch` reports a curve
# that still collapses. Two explanations fit and only a measurement separates them:
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
# ── WHY THE ARM ORDER ROTATES, which is the fix for how the first run of this was wrong ────────
#
# The first factorial (bench/d35_c1_factorial_SUPERSEDED_rising_load.txt) interleaved the arms --
# `for rep; do BASE; STUB; C1; C1STUB; done` -- and that is NOT sufficient. Its recorded loadavg
# rose monotonically from 6.10 to 13.37 across the run, and with a FIXED arm order a monotonic
# drift becomes a systematic POSITION bias: C1STUB ran last every time, so it was measured at the
# highest load in every rep. Interleaving only removes a bias that is constant in time.
#
# So the order rotates as a LATIN SQUARE over 4 reps: every arm occupies every position exactly
# once, and a monotonic drift cancels to first order in the per-arm mean. loadavg is recorded at
# block FINISH as well as launch, so drift inside one block is visible rather than assumed away.
#
# ── AND IT REFUSES TO RUN WHILE A SUITE IS UP ──────────────────────────────────────────────────
#
# An empty process table is NOT proof of a quiet machine: between targets, a per-target suite is a
# bash script with no cargo or rustc child at all. The authority is /tmp/ferrodb-suite.lock plus
# `kill -0` on the pid it names. That is a guard here rather than a line in a brief, because a
# brief is advice the next runner may not read.
#
# ⛔ STUB and C1STUB are MEASUREMENT SCAFFOLDS AND MUST NEVER BE MERGED. Deleting `touch` degrades
# ARC's recency -- bench/d35_c1_evictiontrace.txt shows it changes the eviction sequence outright.
#
# READ THE SLOPE, NOT THE MULTIPLIER: the machine is shared and multipliers do not reproduce.
set -uo pipefail
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

OUT=${OUT:-bench/d35_c1_factorial.txt}
BASE_BIN=${BASE_BIN:?}; STUB_BIN=${STUB_BIN:?}; C1_BIN=${C1_BIN:?}; C1STUB_BIN=${C1STUB_BIN:?}
SUITE_LOCK=${SUITE_LOCK:-/tmp/ferrodb-suite.lock}

for b in "$BASE_BIN" "$STUB_BIN" "$C1_BIN" "$C1STUB_BIN"; do
  [ -x "$b" ] || { echo "REFUSING: $b is not an executable" >&2; exit 2; }
done

# The quiet guard. A held lock whose pid is ALIVE means a certification suite is running, and any
# absolute number taken beside one is not quotable. A lock whose pid is dead is stale and ignored.
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

bin_for() {
  case "$1" in
    BASE)   echo "$BASE_BIN" ;;
    STUB)   echo "$STUB_BIN" ;;
    C1)     echo "$C1_BIN" ;;
    C1STUB) echo "$C1STUB_BIN" ;;
    *) echo "unknown arm $1" >&2; exit 2 ;;
  esac
}

# Byte-identical to the RESIDENT arm of bench/s22_bufpool_before_after.txt and of
# bench/d35_gate_stubtouch.txt, so all three files are comparable.
ARGS=(--resident 200000 1,2,4,8,16 0 3)

# Latin square: each arm in each position exactly once.
ORDERS=(
  "BASE STUB C1 C1STUB"
  "STUB C1 C1STUB BASE"
  "C1 C1STUB BASE STUB"
  "C1STUB BASE STUB C1"
)

{
  echo "# D35 C1 factorial: {page-table mirror} x {arc_cache touch}, RESIDENT arm"
  echo "# generated $(date -Iseconds)"
  echo "# host: $(sysctl -n hw.ncpu) cores, load average at start: $(uptime | sed 's/.*averages*: //')"
  echo "# suite lock at start: $(cat "$SUITE_LOCK/owner" 2>/dev/null || echo 'none held')"
  echo "# cargo/rustc by executable at start: $(ps -eo comm | grep -cE '^(.*/)?(cargo|rustc)$') process(es)"
  echo "#"
  echo "# BASE   = c56127f unmodified                     (no mirror, touch kept)"
  echo "# STUB   = c56127f with touch deleted             (no mirror, touch deleted)  SCAFFOLD"
  echo "# C1     = $(git rev-parse --short HEAD) as it will merge          (mirror,    touch kept)"
  echo "# C1STUB = that, plus touch deleted               (mirror,    touch deleted)  SCAFFOLD"
  echo "#"
  echo "# Everything else, harness included, is byte-identical across all four binaries."
  echo "# ARM ORDER ROTATES as a Latin square over 4 reps, so no arm sits at the load peak twice."
  echo "# The criterion is the SIGN OF THE SLOPE (16T/1T), not the multiplier."
  echo
} > "$OUT"

run_arm() {
  local rep=$1 pos=$2 name=$3 bin
  bin=$(bin_for "$name")
  {
    echo "===== rep $rep | $name | RESIDENT (every fetch is a cache HIT) ====="
    echo "# position in rep: $pos of 4"
    echo "# loadavg at launch: $(uptime | sed 's/.*averages*: //')"
  } >> "$OUT"
  timeout 900 "$bin" "${ARGS[@]}" >> "$OUT" 2>&1
  local rc=$?
  {
    echo "# loadavg at finish: $(uptime | sed 's/.*averages*: //')"
    echo "HARNESS_EXIT=$rc"
    echo
  } >> "$OUT"
}

for rep in 1 2 3 4; do
  # Re-check before every rep: a suite can start mid-run, and a run that straddles one is not a
  # measurement. Refusing partway leaves a truncated artifact, which is honest -- the summariser
  # counts blocks and says so.
  quiet_or_refuse
  pos=0
  for arm in ${ORDERS[$((rep-1))]}; do
    pos=$((pos+1))
    run_arm "$rep" "$pos" "$arm"
  done
done

echo "WROTE $OUT"
