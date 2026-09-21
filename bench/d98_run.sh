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
# THE LOCK IS NOT OPTIONAL, AND IT DOES LESS THAN THIS HEADER ONCE CLAIMED.
#
# ⛔ CORRECTED 2026-09-21. This said the lock "waits for the fleet's timing lock AND for load to
# fall below 8, and exits non-zero if either fails". That was true of the first version of
# `~/wt/logs/measure-lock.sh` and it DEADLOCKED THE FLEET — the box runs a dozen build agents at
# load 22-60, so the quiet gate could never open. The script was rewritten: it blocks only on other
# MEASURERS, there is no exit 3, and the load is STAMPED as `load_at_acquire=<n>` rather than
# gated. The script's own header is the authority; this comment is a pointer to it.
#
# WHAT THAT MEANS FOR THE NUMBERS. Mutual exclusion against other measurers is real and is what
# this takes. Quiet is not available on this machine, so every duration below is an UPPER BOUND
# carrying `load_at_acquire` beside it, and the claim rests first on the two INTEGER counters the
# harness reports — reaps per lock acquisition, and statements completed during the sweep. Those
# are counts of events, not durations, so fleet load does not move them.
#
#   bench/d98_run.sh <before-binary> <after-binary> <out-dir>
#
# PROVENANCE OF THIS FILE, because its first commit says something about it that is not true.
#
# This file entered the repository in f2057c4, whose message reads "QUARANTINE: agent killed by
# the weekly rate limit, UNREVIEWED. Re-derive before trusting." That label was applied by the
# team lead to preserve uncommitted work when the rate limit killed this lane mid-task, and it was
# the right call at the time -- but it is now false about this file and it is merged into main,
# where a commit message cannot be amended without rewriting shared history.
#
# So the correction lives here, where anyone reading the file will see it. The contents were
# written by this lane, and have since been reviewed and changed twice in tracked commits: once to
# correct a header describing the OLD measure-lock (the load gate and exit 3, both removed when
# that script was rewritten), and once to fix the exit-status defect described at run() below,
# found by review. Nothing in f2057c4 survives unreviewed. Do not treat the QUARANTINE label as a
# live warning about this file; treat it as a record of how the file reached the repository.
set -u

BEFORE="${1:?before binary}"
AFTER="${2:?after binary}"
OUT="${3:?output directory}"
mkdir -p "$OUT"

# Small N is cheap enough for three reps; N=10^5 costs ~6 minutes of fixture per rep, so it gets
# one per invocation and earns its statistics from the ABBA repetition instead.
#   args: N[,N] K_denom K_fixed probe_us reps warmup_ms fixture_threads clients
SMALL_ARGS="1000,10000 100 64 50 2 200 8 32"
# 500 us at 10^5, not 50: the free arm probes for the whole ~50 s sweep and 32 clients at 50 us
# would hold ~600 MB of samples. 500 us still resolves a hold of tens of milliseconds with ~100
# samples per client, and the harness REFUSES a cell no probe overlapped rather than calling it
# fast — so a spacing that turned out to be too coarse reports itself.
LARGE_ARGS="100000 100 64 500 1 200 8 32"

# Blocks that did not finish cleanly. The trailing banner is a function of this and nothing else.
FAILED=""
STATUS="$OUT/exit_codes.txt"
: > "$STATUS"

# ⛔ THIS FUNCTION USED TO THROW THE EXIT STATUS AWAY, AND THE LARGE ARM IS THE ONE THAT CAN HIT
# THE WALL. It read `echo "$tag exit=$? ... rows"` — the status reached the SCRIPT'S STDERR and
# nothing else: nothing tested it, nothing wrote it into $OUT, and `set -e` would not have helped
# because `echo` consumes it. So a `timeout 5400` kill on the N=10^5 block (≈6 min of fixture plus
# a ~50 s sweep per rep, on a box whose load this file's own header puts at 22-60) exited 124, the
# loop carried on, the lock was released, the script printed `done`, and a TRUNCATED
# large-after-r1.txt sat in the evidence directory reading exactly like a complete one.
#
# That is the same defect class this row's own finding turned on: a summary that is not a function
# of what it summarises. It is why the status is captured on its own line below, before anything
# else can overwrite `$?`, and why the file is checked for the harness's SUMMARY section as well —
# a process can exit 0 and still have been cut short by a signal its parent swallowed, but it
# cannot print a SUMMARY it never reached.
run() {  # run <tag> <binary> <args...>
  local tag="$1" bin="$2"; shift 2
  local rc rows summary
  echo "=== $tag $(date -u +%FT%TZ) :: $bin $* ===" >&2
  timeout 5400 "$bin" "$@" > "$OUT/$tag.txt" 2> "$OUT/$tag.err"
  rc=$?                        # captured IMMEDIATELY: the next command overwrites $?
  rows=$(grep -c '^ *[0-9]' "$OUT/$tag.txt" 2>/dev/null || true)
  if grep -q '^# SUMMARY' "$OUT/$tag.txt" 2>/dev/null; then summary=yes; else summary=no; fi
  printf '%s rc=%s rows=%s summary=%s\n' "$tag" "$rc" "${rows:-0}" "$summary" >> "$STATUS"
  echo "$tag rc=$rc rows=${rows:-0} summary=$summary" >&2
  if [ "$rc" -ne 0 ] || [ "$summary" != yes ]; then
    FAILED="$FAILED $tag(rc=$rc,summary=$summary)"
  fi
}

# THE LOCK IS TAKEN PER ROUND, NOT ONCE FOR THE WHOLE RUN. The script's own stale-owner rule lets
# another agent break a lock held longer than 45 minutes, and the full four invocations take about
# 48 — so a single acquisition would be stolen mid-measurement and the run would silently become
# one taken alongside another measurer. Each ROUND is already a complete before/after pair with
# every arm inside it, so per-round locking costs the comparison nothing and keeps each hold to
# roughly 21 minutes.
#
# Refuse rather than warn. Exit 1 is "timed out waiting for another MEASURER", and a timing run
# taken while a sibling agent is also timing is not a run to caveat — it is a run not to take.
: > "$OUT/lock.txt"

# Release the measure lock however this script leaves — a refusal below must not strand it and
# make every other agent wait out the 45-minute stale-owner timer.
LOCK_HELD=""
trap '[ -n "$LOCK_HELD" ] && ~/wt/logs/measure-lock.sh release D98 >> "$OUT/lock.txt" 2>&1' EXIT

for round in 1 2; do
  LOCKOUT=$(~/wt/logs/measure-lock.sh acquire D98 2>&1); LOCKRC=$?
  { echo "=== round $round ==="; echo "measure-lock rc=$LOCKRC"; echo "$LOCKOUT"
    echo "load at round start: $(uptime)"; } | tee -a "$OUT/lock.txt"
  if [ "$LOCKRC" -ne 0 ]; then
    echo "REFUSING TO MEASURE round $round: measure-lock exited $LOCKRC." >&2
    exit "$LOCKRC"
  fi
  LOCK_HELD=1

  if [ "$round" = 1 ]; then order="before after"; else order="after before"; fi
  for who in $order; do
    case "$who" in
      before) bin="$BEFORE" ;;
      after)  bin="$AFTER" ;;
    esac
    run "small-$who-r$round" "$bin" $SMALL_ARGS
    run "large-$who-r$round" "$bin" $LARGE_ARGS
  done

  echo "load at round end: $(uptime)" >> "$OUT/lock.txt"
  ~/wt/logs/measure-lock.sh release D98 >> "$OUT/lock.txt" 2>&1
  LOCK_HELD=""
done

# ---- the banner is a function of every block, not of reaching the end ----------------------
{ echo "=== completeness ==="; cat "$STATUS"; } >> "$OUT/lock.txt"
if [ -n "$FAILED" ]; then
  msg="INCOMPLETE MEASUREMENT — these blocks did not finish:$FAILED. The evidence directory holds
truncated files that read like complete ones; see $STATUS. DO NOT REPORT A NUMBER FROM THIS RUN."
  echo "$msg" | tee -a "$OUT/lock.txt" >&2
  exit 3
fi
echo "COMPLETE: $(wc -l < "$STATUS" | tr -d ' ') blocks, all rc=0 with a SUMMARY section." \
  | tee -a "$OUT/lock.txt" >&2
