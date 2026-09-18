#!/usr/bin/env bash
# D44 INSTRUMENT CHECK: was the ceiling the DESIGN's or the HARNESS's?
#
# THE DEFECT. Until ec0c53c this harness counted completed fetches into one shared
# `Arc<AtomicUsize>` with a fetch_add per iteration, INSIDE the timed window -- ~9.6M contended
# RMWs on one cache line per 16-thread point, which is the very construct the buffer pool is being
# measured for. Every arm carried it, including the control, so it cancelled in every comparison
# and could only ever appear as a CEILING that no arm beats.
#
# ⛔ PRE-REGISTERED READING, written before this ran:
#   BASE's slope UNCHANGED, ceiling arm RISES  -> the counter was a ceiling and nothing else. The
#       database walls D35/D44 measured are real, the ORDERING of every published cell stands, and
#       what must be restated is D44's x0.5 bar and its x0.579 ceiling, both of which were set
#       against a number the instrument was capping.
#   BASE's slope MOVES MATERIALLY                -> a share of what this lane has been measuring
#       all along was the harness. That is a larger retraction than the bar, and it reaches back
#       through D35's factorial. Report it as that, do not soften it.
#   BOTH arms unchanged                          -> the counter never mattered at these rates and
#       the adversary's synthetic model does not transfer. Say so; it is a clean negative.
#
# The adversary's model measured a shared-counter arm at 45.7 M/s at 16T against 203.5 M/s with
# per-thread counters, and 45.7 lands within 2% of this lane's measured C1STUB. That is a
# HYPOTHESIS -- synthetic, 2 reps, loaded box, one arm reporting 8.2 G ops/s which smells like
# partial optimisation-out. This file is what settles it on the calibrated instrument.
#
# ⛔ STUB and C1STUB are MEASUREMENT SCAFFOLDS AND MUST NEVER BE MERGED. Deleting `touch` degrades
# ARC's recency -- bench/d35_c1_evictiontrace.txt shows it changes the eviction sequence outright.
#
# READ THE SLOPE, NOT THE MULTIPLIER: the machine is shared and multipliers do not reproduce.
set -uo pipefail
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

OUT=${OUT:-bench/d44_harness_counter.txt}
BASEOLD_BIN=${BASEOLD_BIN:?}; BASENEW_BIN=${BASENEW_BIN:?}; STUBOLD_BIN=${STUBOLD_BIN:?}; STUBNEW_BIN=${STUBNEW_BIN:?}
SUITE_LOCK=${SUITE_LOCK:-/tmp/ferrodb-suite.lock}

for b in "$BASEOLD_BIN" "$BASENEW_BIN" "$STUBOLD_BIN" "$STUBNEW_BIN"; do
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
  # When the CALLER already holds the suite lock on this run's behalf, the lock is evidence of
  # quiet rather than evidence against it -- checking it would refuse because of ourselves.
  if [ "${SUITE_LOCK_HELD_BY_CALLER:-0}" != "1" ] && [ -d "$SUITE_LOCK" ]; then
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

# A LOAD CEILING, checked before every rep. The start-time checks cannot see load that arrives
# after the run begins, which is exactly what happened to bench/d44_batchsize_VOID_loadavg1021.txt:
# lock free and zero builders at start, then another agent's load sweep took the 1-minute average
# to 1021 on an 18-core box and every ratio came out above 1.0. A machine with more than a few
# runnable threads per core is not measuring this program, and a number from it is not a
# conservative estimate -- it is noise with a plausible shape.
LOADAVG_MAX=${LOADAVG_MAX:-$(( $(sysctl -n hw.ncpu) * 3 ))}
load_or_refuse() {
  local la
  la=$(uptime | sed 's/.*averages*: //' | awk '{print $1}' | tr -d ',')
  if awk -v a="$la" -v m="$LOADAVG_MAX" 'BEGIN{exit !(a > m)}'; then
    echo "REFUSING: 1-minute load average $la exceeds $LOADAVG_MAX ($(sysctl -n hw.ncpu) cores x 3)." >&2
    echo "  Ratios taken here come out ABOVE 1.0 for arms that collapse everywhere else, because" >&2
    echo "  the 1-thread point starves harder than the 16-thread one. That is not a measurement." >&2
    exit 3
  fi
}

# The mid-run bar: only a certification suite aborts. See the rep loop for why.
suite_or_refuse() {
  if [ "${SUITE_LOCK_HELD_BY_CALLER:-0}" != "1" ] && [ -d "$SUITE_LOCK" ]; then
    local owner pid
    owner=$(cat "$SUITE_LOCK/owner" 2>/dev/null || echo "unknown")
    pid=${owner%% *}
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "REFUSING mid-run: a suite started and holds $SUITE_LOCK ($owner)." >&2
      exit 3
    fi
  fi
}

builders_now() { ps -eo comm | grep -cE '^(.*/)?(cargo|rustc)$'; }
quiet_or_refuse
load_or_refuse

bin_for() {
  case "$1" in
    BASEOLD) echo "$BASEOLD_BIN" ;;
    BASENEW) echo "$BASENEW_BIN" ;;
    STUBOLD) echo "$STUBOLD_BIN" ;;
    STUBNEW) echo "$STUBNEW_BIN" ;;
    *) echo "unknown arm $1" >&2; exit 2 ;;
  esac
}

# Byte-identical to the RESIDENT arm of bench/s22_bufpool_before_after.txt and of
# bench/d35_gate_stubtouch.txt, so all three files are comparable.
ARGS=(--resident 200000 1,2,4,8,16 0 3)

# Latin square: each arm in each position exactly once.
ORDERS=(
  "BASEOLD BASENEW STUBOLD STUBNEW"
  "BASENEW STUBOLD STUBNEW BASEOLD"
  "STUBOLD STUBNEW BASEOLD BASENEW"
  "STUBNEW BASEOLD BASENEW STUBOLD"
)

{
  echo "# D35 C1 factorial: {page-table mirror} x {arc_cache touch}, RESIDENT arm"
  echo "# generated $(date -Iseconds)"
  echo "# host: $(sysctl -n hw.ncpu) cores, load average at start: $(uptime | sed 's/.*averages*: //')"
  echo "# suite lock at start: $(cat "$SUITE_LOCK/owner" 2>/dev/null || echo 'none held')"
  echo "# cargo/rustc by executable at start: $(ps -eo comm | grep -cE '^(.*/)?(cargo|rustc)$') process(es)"
  echo "#"
  echo "# BASEOLD = c56127f pool, OLD harness (shared AtomicUsize per fetch, in the timed window)"
  echo "# BASENEW = c56127f pool, NEW harness (per-thread counts, nothing shared in the loop)"
  echo "# STUBOLD = mirror + no policy update, OLD harness            SCAFFOLD"
  echo "# STUBNEW = mirror + no policy update, NEW harness            SCAFFOLD"
  echo "#"
  echo "# Old and new harness IN ONE SWEEP, rotated, so the comparison is not across two runs on"
  echo "# a machine whose load moves. That is the whole design of this file."
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
    echo "# builders at launch: $(builders_now)"
  } >> "$OUT"
  timeout 900 "$bin" "${ARGS[@]}" >> "$OUT" 2>&1
  local rc=$?
  {
    echo "# loadavg at finish: $(uptime | sed 's/.*averages*: //')"
    echo "# builders at finish: $(builders_now)"
    echo "HARNESS_EXIT=$rc"
    echo
  } >> "$OUT"
}

# 8 reps = two complete Latin squares. More reps than the minimum on purpose: this machine runs an
# agent fleet that compiles continuously, so a window with zero builders for four minutes may never
# come. The answer is not to wait for one -- it is to make the measurement survive load: rotate so
# drift cannot land on one arm, take medians over many reps, and RECORD the contamination per block
# so a reader can see it instead of trusting that it was absent.
for rep in 1 2 3 4 5 6 7 8; do
  # Mid-run the bar is lower than at start: only a certification SUITE aborts, because that is the
  # perturbation big enough to invalidate the run outright. An ordinary build is recorded per block
  # and left to the rotation and the medians, which is what they are for. Refusing on every passing
  # rustc produced a one-rep artifact and no answer at all.
  suite_or_refuse
  load_or_refuse
  pos=0
  for arm in ${ORDERS[$(( (rep-1) % 4 ))]}; do
    pos=$((pos+1))
    run_arm "$rep" "$pos" "$arm"
  done
done

echo "WROTE $OUT"
