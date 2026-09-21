#!/bin/bash
# D101 — re-run the five harnesses that timed an in-memory stub, on the runtime they build.
#
# Serialised behind the fleet's measure lock, and every block stamps the load at acquire and the
# load either side of the run, because this box runs a 13-agent build fleet and a duration here is
# an upper bound, not a measurement. COUNTERS (fsyncs, bytes/fsync, merges applied) are the parts
# of this output that mean something in absolute terms.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
WT=/Users/idide/wt/ferrodb-D101-stub-runtime-guard
EX=$WT/target/release/examples
OUT=$WT/bench/d101_rerun.txt
SHA=$(git -C "$WT" rev-parse --short HEAD)
FAILED=0

ACQ=$(~/wt/logs/measure-lock.sh acquire d101-rerun) || { echo "LOCK FAILED: $ACQ" >>"$OUT"; exit 1; }
trap '~/wt/logs/measure-lock.sh release d101-rerun' EXIT

{
  echo "================================================================================"
  echo "D101 RE-RUN — the five harnesses, on the runtime they build."
  echo "[HERE] $(date -u +%FT%TZ)  worktree=$WT  sha=$SHA"
  echo "$ACQ"
  echo "⚠ Durations are UPPER BOUNDS. Counters (fsyncs, bytes/fsync) are not."
  echo "================================================================================"
} >>"$OUT"

run() {           # run <label> <timeout-seconds> <env...> -- <binary> [args]
  local label=$1 tmo=$2; shift 2
  {
    echo
    echo "#### $label"
    echo "# load before: $(uptime | sed 's/.*averages*: *//')"
    echo "# cmd: $*"
  } >>"$OUT"
  # HEARTBEAT. A waiting measurer steals this lock when the owner file is >45 min old
  # (`measure-lock.sh`, the stall-breaker). The owner file is written once at acquire, so a
  # multi-hour sweep would be robbed mid-run and two measurers would time simultaneously —
  # the one thing the lock exists to prevent. Touched per BLOCK, not on a timer: if one block
  # genuinely stalls past 45 minutes the lock SHOULD become stealable, and it still does.
  touch /Users/idide/wt/logs/.measure.lock/owner 2>/dev/null
  local t0=$(date +%s)
  timeout "$tmo" env "$@" >>"$OUT" 2>&1
  local rc=$?
  {
    echo "# rc=$rc  elapsed=$(( $(date +%s) - t0 ))s"
    echo "# load after: $(uptime | sed 's/.*averages*: *//')"
  } >>"$OUT"
  # ⛔ A BLOCK THAT DID NOT EXIT 0 IS NOT A RESULT. The first version of this script printed
  # "ALL FIVE COMPLETE" unconditionally; a disk emergency deleted target/ mid-sweep, eleven
  # blocks exited 127 ("No such file or directory"), and the banner went out over them. A run
  # that collected nothing has not passed, so every failure is counted and the banner is gated.
  if [ "$rc" -ne 0 ]; then
    FAILED=$((FAILED + 1))
    echo "# ⛔ FAILED BLOCK: $label (rc=$rc) — NOT A RESULT" >>"$OUT"
  fi
  # Re-assert the binary still exists: a vanished target/ is the exact failure that produced the
  # false banner, and it is silent until the next exec.
  if [ ! -x "$EX/d68_merge_is_o_table" ]; then
    echo "# ⛔ ABORTING: $EX is gone (target/ deleted mid-sweep). Nothing below would be a result." >>"$OUT"
    exit 1
  fi
}

run "D68 — merge latency vs TABLE SIZE at fixed delta (banked: bench/d68_merge_is_o_table.txt)" \
    3600 "$EX/d68_merge_is_o_table"

run "D71 arm=PLAIN — ordinary UPDATE (banked: bench/d71_point_update_curve.txt)" \
    3600 "$EX/d71_point_update_curve"
run "D71 arm=STAGED — the same UPDATE inside an agent session" \
    3600 D71_ARM=staged "$EX/d71_point_update_curve"

run "D67 — merges/sec vs thread count (banked: bench/d67_merge_contention.txt)" \
    3600 "$EX/d67_merge_contention"

run "D55 QUICK shared arm staged=10 (banked: bench/d55_after_quick.txt, AFTER column)" \
    3600 D55_QUICK=1 "$EX/d55_agent_read_scaling"

# D56 is one point per invocation; the banked file's sweep is driven from outside the binary.
# Size order is rotated per round so a drift in the box cannot be read as a size effect.
for r in 1 2 3; do
  case $r in
    1) sizes="1000 4000 16000" ;;
    2) sizes="4000 16000 1000" ;;
    3) sizes="16000 1000 4000" ;;
  esac
  for n in $sizes; do
    run "D56 r$r rows=$n (banked: bench/d56_plan_curve.txt, FIX rows)" \
        1800 D56_ROWS=$n D56_STAGED=10 D56_LABEL="r$r D101" "$EX/d56_plan_curve"
  done
done

{
  echo
  if [ "$FAILED" -eq 0 ]; then
    echo "#### ALL FIVE COMPLETE, every block rc=0 — $(date -u +%FT%TZ)"
  else
    echo "#### ⛔ SWEEP INCOMPLETE: $FAILED block(s) did not exit 0 — $(date -u +%FT%TZ)"
    echo "#### Do NOT quote a number from this file until the failed blocks are re-run."
  fi
} >>"$OUT"
exit $(( FAILED > 0 ))
