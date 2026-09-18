#!/usr/bin/env bash
# D28 at 10^6: does the system-view generator hint hold at the scale the wall was measured at?
#
# The 10^5 matched-base pair landed at 1246188 (br_1row 27,084x, runs 38,867x, br_all flat at 1.0x
# as the control). That file says in its own words that the 10^6 headline is NOT in it: three
# attempts were abandoned -- one against a base that no longer existed, one contaminated by a second
# run its liveness check could not see, one dead with the volume at 786 MiB. This is the fourth.
#
# ── WHY ARM B IS A ONE-LINE PATCH AND NOT `git archive agent-isolation` ────────────────────────
#
# The 10^5 file's own REPRODUCING section says `git archive agent-isolation | tar -x` for arm B,
# because at that time D28 was an unmerged branch and the base WAS the tip-without-pushdown. D28
# merged at 8d9c04b, so that recipe is now dead: `agent-isolation` HAS the pushdown. Reverting the
# four D28 files from the current tip is not available either -- D41 (81a64b6) and D45
# (7541502, 0783fce) have since rewritten `branch/mod.rs`, `table_catalog.rs` and `runtime.rs`, so
# a four-file revert would drag three unrelated rows out with it and stop being a matched A/B.
#
# So arm B flips the ONE switch the design already built for exactly this comparison:
#
#     src/catalog/system_views.rs, fn run_select:
#         select_with(view, stmt, catalog, runtime, Pushdown::On)     <- arm A, as it ships
#         select_with(view, stmt, catalog, runtime, Pushdown::Off)    <- arm B, this scaffold
#
# `Pushdown::Off` yields `ViewHint::All` for every statement, which is what `materialise` passes
# and what every unrecognised predicate produces: the generator builds every row and `Filter` does
# the rest. That IS the pre-D28 behaviour, and it is reached through the path `run_select_unhinted`
# already exercises in the drift test, so it is a supported mode rather than a hand-cut mutant.
#
# ⛔ ARM B IS A MEASUREMENT SCAFFOLD AND MUST NEVER BE MERGED, exactly like D35's STUB arms. It
#    turns a shipped optimisation off. It exists for the duration of this run and nothing else.
#
# ── WHY THE ARM ORDER ROTATES ──────────────────────────────────────────────────────────────────
#
# Interleaving A/B/A/B -- which the 10^5 driver did -- cancels only a bias that is CONSTANT IN
# TIME. This box drifts: D35 measured load rising 3.72 -> 13.14 DURING a single run with the suite
# lock free and no cargo on the process table, and D35's own first factorial was wrong because a
# fixed arm order under a monotonic drift becomes a systematic POSITION bias. So the order rotates
# A B / B A: each arm occupies each position exactly once per pair of reps, and a monotonic drift
# cancels to first order in the per-arm median. The summariser prints br_all BY POSITION as the
# check that the rotation actually worked -- br_all is an unchanged code path in both arms, so a
# position effect there is drift and not the arms.
#
# ── AND IT REFUSES RATHER THAN MEASURING BESIDE A SUITE OR INTO A FULL DISK ────────────────────
#
# An empty process table is NOT proof of a quiet machine: between targets a per-target suite is a
# bash script with no cargo or rustc child at all. The authority is the suite lock plus `kill -0`.
#
# ⛔ AND A FREE LOCK IS NOT A QUIET BOX EITHER, which is a separate guard because it is a separate
# failure. Measured here 2026-09-18T13:33Z: the suite lock was FREE, `ps` showed ZERO cargo and
# ZERO rustc, and the 1-minute load average was 514 on 18 cores -- another lane was running 500+
# `sh -c 'while :; do :; done'` spinners as a deliberate load experiment. Every signal this script
# had at that moment said "go". So load is read directly, and it is read again before every rep,
# because the state that matters is the one during the run and not the one at launch.
# Disk is guarded at the START as well as per round, because a 10^6 round costs tens of minutes and
# discovering the volume is full at the end of one wastes all of it -- that is how the third
# attempt died. Both are guards here rather than lines in a brief, because a brief is advice the
# next runner may not read.
set -uo pipefail
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

OUT=${OUT:?set OUT to the raw artifact path}
ARM_A=${ARM_A:?set ARM_A to the hinted binary}
ARM_B=${ARM_B:?set ARM_B to the unhinted binary}
N=${N:-1000000}
REPS=${REPS:-2}
THREADS=${THREADS:-64}
ROUND_TIMEOUT=${ROUND_TIMEOUT:-7200}
SUITE_LOCK=${SUITE_LOCK:-/tmp/ferrodb-suite.lock}
# A 10^6 round writes a durable catalog. 12 GiB is not a guess: the third abandoned attempt died
# with the volume at 786 MiB, and a round that dies at the end has cost its whole wall time.
MIN_FREE_KB=${MIN_FREE_KB:-12582912}
SCRATCH=${SCRATCH:-/tmp/d28_1e6}
# 2x cores. Deliberately generous rather than strict: the landed 10^5 pair was taken at load 14-20
# on this 18-core box and is a good measurement, because the design does not depend on a quiet
# machine -- br_all is the control that sets the noise floor and the rotation cancels drift. What
# this ceiling exists to refuse is the PATHOLOGICAL case, where blocks time out or swap and the
# run fails mechanically rather than noisily. A strict ceiling here would refuse the conditions
# this project actually measures under, and a guard that cries wolf gets walked around.
NCPU=$(sysctl -n hw.ncpu 2>/dev/null || echo 8)
LOAD_CEILING=${LOAD_CEILING:-$((NCPU * 2))}

for b in "$ARM_A" "$ARM_B"; do
  [ -x "$b" ] || { echo "REFUSING: $b is not an executable" >&2; exit 2; }
done

# The two arms MUST be different artifacts. If cargo handed back the same one twice the A/B would
# report 1.00x, which reads exactly like an honest null -- the 10^5 run named this trap and checked
# for it, so this one does too, and refuses instead of reporting it.
SHA_A=$(shasum -a 256 "$ARM_A" | awk '{print $1}')
SHA_B=$(shasum -a 256 "$ARM_B" | awk '{print $1}')
if [ "$SHA_A" = "$SHA_B" ]; then
  echo "REFUSING: both arms are the same binary ($SHA_A)." >&2
  echo "  cargo returned one artifact twice; the A/B would report 1.00x, a fake null." >&2
  exit 2
fi

free_kb() { df -k "$SCRATCH" 2>/dev/null | awk 'NR==2 {print $4}'; }
loadavg()  { uptime | sed 's/.*averages*: *//' | awk '{print $1}'; }

quiet_or_refuse() {
  if [ -d "$SUITE_LOCK" ]; then
    local owner pid
    owner=$(cat "$SUITE_LOCK/owner" 2>/dev/null || echo "unknown")
    pid=${owner%% *}
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "REFUSING: a suite is running and holds $SUITE_LOCK ($owner)." >&2
      echo "  A 10^6 arm taken beside a compiling, linking, TCP-binding suite is not quotable." >&2
      echo "  Wait for it, then re-run. Do not bypass this by deleting the lock." >&2
      exit 3
    fi
    echo "# note: $SUITE_LOCK present but its pid is dead (stale): $owner" >&2
  fi
}

load_or_refuse() {
  local l1
  l1=$(loadavg)
  # Integer compare: bash has no floats, and truncating toward zero is the permissive direction.
  if [ "${l1%%.*}" -gt "$LOAD_CEILING" ]; then
    echo "REFUSING: 1-minute load average is $l1 on $NCPU cores, ceiling is $LOAD_CEILING." >&2
    echo "  A free suite lock is not a quiet box. Blocks time out and swap at this load, so the" >&2
    echo "  run would fail mechanically rather than noisily. Wait for the box, then re-run." >&2
    echo "  Override with LOAD_CEILING= only if you can say why the number is still quotable." >&2
    exit 5
  fi
}

disk_or_refuse() {
  local f; f=$(free_kb)
  if [ "${f:-0}" -lt "$MIN_FREE_KB" ]; then
    echo "REFUSING: only ${f}KiB free on $SCRATCH, floor is ${MIN_FREE_KB}KiB." >&2
    echo "  A 10^6 round costs tens of minutes and dies at the END when the volume fills," >&2
    echo "  so the precondition is guarded rather than discovered. Free space and re-run." >&2
    exit 4
  fi
}

mkdir -p "$SCRATCH"
quiet_or_refuse
load_or_refuse
disk_or_refuse

# The data row is keyed on "first field is a bare integer", NOT on N: the harness reports
# per*threads (999936 for N=1000000, 99968 for 100000), so matching N literally drops every row.
row() { awk '$1 ~ /^[0-9]+$/ && NF >= 12 { last = $0 } END { if (last != "") print last }'; }

{
  echo "# D28 at N=$N: system-view generator hint ON (arm A) vs OFF (arm B), matched binary base"
  echo "# generated $(date -Iseconds)"
  echo "# tip: $(git rev-parse --short HEAD)   $(git log -1 --format=%s | cut -c1-70)"
  echo "# host: $(sysctl -n hw.ncpu) cores; loadavg at start: $(uptime | sed 's/.*averages*: //')"
  echo "# suite lock at start: $(cat "$SUITE_LOCK/owner" 2>/dev/null || echo 'none held')"
  echo "# free at start: $(free_kb) KiB on $SCRATCH"
  echo "# load ceiling: $LOAD_CEILING on $NCPU cores"
  echo "#"
  echo "# arm A = hinted   (Pushdown::On,  as it ships)   $SHA_A"
  echo "# arm B = unhinted (Pushdown::Off, SCAFFOLD)      $SHA_B"
  echo "# harness: examples/runtime_curve.rs phase 'query', identical source in both arms;"
  echo "#          the arms differ by exactly one line in src/catalog/system_views.rs."
  echo "#"
  echo "# ARM ORDER ROTATES A B / B A. Interleaving alone cancels only a bias constant in time,"
  echo "# and this box drifts (D35: 3.72 -> 13.14 within one run). The summariser prints br_all"
  echo "# by POSITION as the check the rotation worked; br_all is unchanged in both arms."
  echo
} > "$OUT"

bin_for() { case "$1" in A) echo "$ARM_A" ;; B) echo "$ARM_B" ;; *) echo "unknown arm $1" >&2; exit 2 ;; esac; }

run_arm() {
  local rep=$1 pos=$2 arm=$3 bin err line f0 f1
  bin=$(bin_for "$arm")
  err="$SCRATCH/rep${rep}_pos${pos}_${arm}.err"
  f0=$(free_kb)
  {
    echo "===== rep $rep | arm $arm | N=$N ====="
    echo "# position in rep: $pos of 2"
    echo "# loadavg at launch: $(uptime | sed 's/.*averages*: //')"
    echo "# free at launch: ${f0} KiB"
    echo "# started: $(date -Iseconds)"
  } >> "$OUT"

  local t0 t1
  t0=$(date +%s)
  # stderr is KEPT. Sent to /dev/null, a round killed by a full disk is indistinguishable from a
  # round that merely produced no row.
  line=$(timeout "$ROUND_TIMEOUT" "$bin" query "$N" "$THREADS" 2>"$err" | row)
  local rc=$?
  t1=$(date +%s)
  f1=$(free_kb)

  {
    echo "# loadavg at finish: $(uptime | sed 's/.*averages*: //')"
    echo "# free at finish: ${f1} KiB"
    echo "# wall: $((t1 - t0)) s"
    echo "HARNESS_EXIT=$rc"
  } >> "$OUT"

  # OUTCOME, not wording, and this ordering is load-bearing. A full disk is precisely the condition
  # under which "No space left" cannot be WRITTEN: round 5 of the 10^5 run left one EMPTY .err and
  # one missing, the text grep matched nothing, and a disk death was labelled NO-ROW-EMITTED. Free
  # space at round end is the one signal a full disk cannot suppress, so it is checked FIRST.
  if [ "${f1:-0}" -lt "$MIN_FREE_KB" ]; then
    echo "VOID-DISK rep=$rep pos=$pos arm=$arm -- ${f1}KiB free at round end (floor ${MIN_FREE_KB});" >> "$OUT"
    echo "  NOT a datapoint and NOT a red result." >> "$OUT"; echo >> "$OUT"
    return 0
  fi
  if grep -qE "No space left|os error 28" "$err" 2>/dev/null; then
    echo "VOID-ENOSPC rep=$rep pos=$pos arm=$arm -- disk filled mid-run; NOT a datapoint." >> "$OUT"
    echo >> "$OUT"; return 0
  fi
  if [ -z "$line" ]; then
    echo "NO-ROW-EMITTED rep=$rep pos=$pos arm=$arm -- rc=$rc; run failed or was killed." >> "$OUT"
    echo "  NOT a datapoint. stderr kept at $err" >> "$OUT"; echo >> "$OUT"
    return 0
  fi

  # N BEGIN INSERT SELECT ASOF br_1row br_all runs activity quaran live_cnt RSS
  echo "$line" | awk -v r="$rep" -v p="$pos" -v a="$arm" -v l="$(loadavg)" \
    '{printf "DATA rep=%d pos=%d arm=%s n=%s br_1row=%s br_all=%s runs=%s live_cnt=%s rss=%s load=%s\n",
              r, p, a, $1, $6, $7, $8, $11, $12, l}' >> "$OUT"
  echo >> "$OUT"
  sync
  rm -f "$err"
}

for rep in $(seq 1 "$REPS"); do
  # Re-checked before EVERY rep: a suite can start mid-run, and a run that straddles one is not a
  # measurement. Refusing partway leaves a truncated artifact, which is honest -- the summariser
  # counts blocks and says so.
  quiet_or_refuse
  load_or_refuse
  disk_or_refuse
  if [ $((rep % 2)) -eq 1 ]; then order="A B"; else order="B A"; fi
  pos=0
  for arm in $order; do
    pos=$((pos + 1))
    run_arm "$rep" "$pos" "$arm"
  done
done

echo "# done $(date -Iseconds)" >> "$OUT"
echo "WROTE $OUT"
