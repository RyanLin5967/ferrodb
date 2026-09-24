#!/usr/bin/env bash
# bench/d219/run.sh — the D219 FAN-QUEUE row, steps (a)–(d), in order. The BEFORE curve is first.
# Revised for the whole exit (one provenance sync per MERGE, physical stamps included).
#
# ⛔ FAN WORK. It builds and runs a benchmark. Run it only when the lead releases the D219 row from
# frontier/FAN-QUEUE.md, and only under the suite lock:
#   bash ~/wt/logs/lead-lanes-0924/lockrun.sh d219-run bash /Users/idide/wt/ferrodb-d219-provenance-sync.noindex/bench/d219/run.sh
#
# Pre-registered in artie-research frontier/lane_d219_provenance_sync.md, committed before this
# script first ran. This script writes RAW output only, one file per step under bench/d219/, each
# ending in its own `rc=` line. It prints no verdict: commit every file by explicit path BEFORE
# reading any of them, then read them against the pre-registration.
#
# Trees: every build happens in a throwaway detached worktree ($FIRE), so the lane's own tree only
# ever receives output files. $FIRE is removed at the end with `git worktree remove` (never
# `wt reap`, which is fleet-wide).
set -u
WT=/Users/idide/wt/ferrodb-d219-provenance-sync.noindex
FIRE=/Users/idide/wt/ferrodb-d219-fire.noindex
OUT=$WT/bench/d219
BASE=9aa6968     # main when the lane was cut
HARNESS=3b9a39e  # instrument + first red test + harness, NO fix: the BEFORE tree
ROWONLY=bf10eec  # row authorship batched (871846a) + the whole-exit red test, stamps NOT batched
FIX=39fd5a5      # the whole fix (86e1762) + the harness's model labels
EX=d219_merge_sync_curve
T=d219_one_provenance_sync_per_merge
export CARGO_TARGET_DIR=$WT/target

cd "$WT" || exit 3
[ -z "$(git status --porcelain)" ] || { echo "REFUSING: $WT is not clean"; exit 3; }
[ -e "$FIRE" ] && { echo "REFUSING: $FIRE exists; remove it with git worktree remove first"; exit 3; }
[ "$(python3 -c 'print(6*7)')" = 42 ] || { echo "REFUSING: python3 does not run here"; exit 3; }
python3 bench/d219/mutants.py --check "$FIX" || exit 3
mkdir -p "$OUT"
git worktree add --detach "$FIRE" "$HARNESS" || exit 3

# step <name> <cmd...>: run in $FIRE, stdout+stderr and the rc on its own line into $OUT/<name>.txt
step() {
  local name=$1 rc
  shift
  echo "$(date -u +%FT%TZ) $name: $*"
  {
    echo "# $name at $(git -C "$FIRE" rev-parse HEAD) $(date -u +%FT%TZ)"
    echo "# cmd: $*"
  } > "$OUT/$name.txt"
  (cd "$FIRE" && "$@") >> "$OUT/$name.txt" 2>&1
  rc=$?
  echo "rc=$rc" >> "$OUT/$name.txt"
}

# ---- (a) BEFORE: the curve at the tree without the fix ------------------------------------------
step a_build_before timeout 3600 cargo build --release --example "$EX"
cp "$CARGO_TARGET_DIR/release/examples/$EX" "$WT/target/d219_before_bin" || exit 3
step a_curve_before_$HARNESS timeout 7200 "$WT/target/d219_before_bin"

# ---- (b) RED at the same tree, and the instrument's own unit test --------------------------------
step b_red_$HARNESS timeout 3600 cargo test --test "$T"
step b_instrument_$HARNESS timeout 3600 cargo test --lib provenance::durable::tests::the_sync_counter_fires_once_per_append_by_kind_and_not_for_a_failed_one
step b_lib_list_$HARNESS timeout 3600 cargo test --lib provenance:: -- --list

# ---- (b2) the whole-exit red test, on the row-only tree ------------------------------------------
git -C "$FIRE" checkout --detach -q "$ROWONLY" || exit 3
step b2_red_$ROWONLY timeout 3600 cargo test --test "$T"
step b2_lib_list_$ROWONLY timeout 3600 cargo test --lib provenance:: -- --list

# ---- (c) AFTER: the fix --------------------------------------------------------------------------
git -C "$FIRE" checkout --detach -q "$FIX" || exit 3
step c_build_after timeout 3600 cargo build --release --example "$EX"
cp "$CARGO_TARGET_DIR/release/examples/$EX" "$WT/target/d219_after_bin" || exit 3
step c_curve_after_$FIX timeout 7200 "$WT/target/d219_after_bin"
step c_green_$FIX timeout 3600 cargo test --test "$T"
step c_lib_provenance_$FIX timeout 3600 cargo test --lib provenance::
step c_lib_list_$FIX timeout 3600 cargo test --lib provenance:: -- --list
git -C "$FIRE" checkout --detach -q "$BASE" || exit 3
step c_lib_list_$BASE timeout 3600 cargo test --lib provenance:: -- --list

# ---- (d) mutants, each on a fresh checkout of the fix --------------------------------------------
mutant() {
  local m=$1
  shift
  git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
  (cd "$FIRE" && python3 "$WT/bench/d219/mutants.py" "$m") > "$OUT/d_${m}_applied.txt" 2>&1 || exit 3
  git -C "$FIRE" diff >> "$OUT/d_${m}_applied.txt"
  local i=0
  for filter in "$@"; do
    i=$((i + 1))
    if [ "$filter" = INTEGRATION ]; then
      step "d_${m}_$i" timeout 3600 cargo test --test "$T"
    else
      step "d_${m}_$i" timeout 3600 cargo test --lib "$filter"
    fi
  done
}
mutant M1_sync_per_record INTEGRATION provenance::durable::tests::a_batch_of_row_authors_is_one_sync_and_the_same_bytes_as_one_call_per_row
mutant M2_dedupe_the_batch INTEGRATION
mutant M3_skip_the_batch INTEGRATION
mutant M4_sync_before_write INTEGRATION provenance::
mutant M5_ignore_the_memory_refusal provenance::durable::tests::a_batch_naming_an_uninterned_run_attributes_none_of_its_rows
mutant M6_empty_batch_is_a_write provenance::durable::tests::an_empty_batch_is_not_a_write
mutant M7_no_interned_guard provenance::store::tests::a_batch_of_rows_is_refused_whole_or_applied_whole
mutant M8_counter_books_the_wrong_kind provenance::durable::tests::the_sync_counter_fires_once_per_append_by_kind_and_not_for_a_failed_one INTEGRATION
mutant M9_publish_stamps_eager INTEGRATION
mutant M10_rewrite_stamps_eager INTEGRATION
mutant M11_no_explicit_flush INTEGRATION provenance::
mutant M12_pending_written_last provenance::deferred::tests::stamps_through_the_stamper_ride_the_next_sync_and_write_the_same_file
mutant M13_pending_ignores_the_refusal provenance::deferred::tests::the_stamper_refuses_at_the_stamp_and_queues_nothing_it_refused
mutant M14_guard_drop_does_not_flush provenance::deferred::tests::a_guard_dropped_without_a_flush_still_writes_its_stamps
mutant M15_store_drop_does_not_flush provenance::durable::tests::a_store_dropped_with_pending_stamps_writes_them
mutant M16_pending_never_cleared provenance::deferred::tests::stamps_through_the_stamper_ride_the_next_sync_and_write_the_same_file

git -C "$FIRE" checkout --detach -f -q "$FIX"
git -C "$WT" worktree remove --force "$FIRE"
echo "$(date -u +%FT%TZ) done; raw output in $OUT — commit it by explicit path before reading"
