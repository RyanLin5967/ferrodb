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
DEFERALL=882e475 # every stamp deferred to the final sync (86e1762) + the schema-phase red test
ONEFLUSH=b875ab3 # ONE flush after all the rewrites (eff03e8) + the two-table red test
ALTERRED=789d1da # catalog::alter tests 1-2 on 6f22427's code (flush before finish, unconditional)
POISONRED=33e3d67 # catalog::alter test 3 on 77bddcb's code (stamps still written inside the rewrite loop)
A1RED=e60c5be    # PREREG A1's red: test 3 expects the poisoned-store ALTER refused (on f513a37's code)
FIX=49c852f      # the whole fix (src as at 50e175b, PREREG A1's probe) + PREREG A2's tests and texts
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

# ---- (b3) the schema-phase red test, on the tree that deferred every stamp to the final sync --------
git -C "$FIRE" checkout --detach -q "$DEFERALL" || exit 3
step b3_red_$DEFERALL timeout 3600 cargo test --test "$T"

# ---- (b4) the two-table red test, on the tree with one flush after every rewrite ------------------
git -C "$FIRE" checkout --detach -q "$ONEFLUSH" || exit 3
step b4_red_$ONEFLUSH timeout 3600 cargo test --test "$T"

# ---- (b5) the catalog::alter red tests, on the tree that flushed before finish, unconditionally ---
git -C "$FIRE" checkout --detach -q "$ALTERRED" || exit 3
step b5_red_$ALTERRED timeout 3600 cargo test --lib catalog::alter::tests
git -C "$FIRE" checkout --detach -q "$POISONRED" || exit 3
step b6_red_$POISONRED timeout 3600 cargo test --lib catalog::alter::tests

# ---- (b7) PREREG A1's red: test 3 on the code that refuses only after the install -----------------
git -C "$FIRE" checkout --detach -q "$A1RED" || exit 3
step b7_red_$A1RED timeout 3600 cargo test --lib catalog::alter::tests

# ---- (c) AFTER: the fix --------------------------------------------------------------------------
git -C "$FIRE" checkout --detach -q "$FIX" || exit 3
step c_build_after timeout 3600 cargo build --release --example "$EX"
cp "$CARGO_TARGET_DIR/release/examples/$EX" "$WT/target/d219_after_bin" || exit 3
step c_curve_after_$FIX timeout 7200 "$WT/target/d219_after_bin"
step c_green_$FIX timeout 3600 cargo test --test "$T"
step c_lib_provenance_$FIX timeout 3600 cargo test --lib provenance::
step c_lib_alter_$FIX timeout 3600 cargo test --lib catalog::alter::tests
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
mutant M11_no_explicit_flush INTEGRATION provenance::
mutant M12_pending_written_last provenance::deferred::tests::stamps_through_the_stamper_ride_the_next_sync_and_write_the_same_file
mutant M13_pending_ignores_the_refusal provenance::deferred::tests::the_stamper_refuses_at_the_stamp_and_queues_nothing_it_refused
mutant M14_guard_drop_does_not_flush provenance::deferred::tests::a_guard_dropped_without_a_flush_still_writes_its_stamps
mutant M15_store_drop_does_not_flush provenance::durable::tests::a_store_dropped_with_pending_stamps_writes_them
mutant M16_pending_never_cleared provenance::deferred::tests::stamps_through_the_stamper_ride_the_next_sync_and_write_the_same_file
mutant M17_pending_ignores_the_poison provenance::durable::tests::a_poisoned_store_refuses_a_pending_stamp
mutant M18_flush_checks_poison_first provenance::durable::tests::an_empty_flush_is_not_a_write_even_on_a_poisoned_store
mutant M19_rewrite_does_not_stamp INTEGRATION catalog::alter::tests::a_failed_flush_after_a_rewrite_leaves_the_table_consistently_altered
mutant M20_flush_even_if_nothing_moved catalog::alter::tests::an_alter_that_stamps_nothing_is_not_refused_by_a_poisoned_provenance_store
# M21: test 2 kills it (Err before finish: 2 columns); test 3 no longer can, the probe refuses first (PREREG A2 F6).
mutant M21_stamps_before_finish catalog::alter::tests::a_failed_flush_after_a_rewrite_leaves_the_table_consistently_altered catalog::alter::tests::a_store_poisoned_between_plan_and_apply_leaves_the_table_consistently_altered
mutant M22_flush_error_swallowed catalog::alter::tests::a_failed_flush_after_a_rewrite_leaves_the_table_consistently_altered
# M23: killed only by the store poisoned between plan and apply (PREREG A2 F1); test 3's is refused by the probe.
mutant M23_stamp_refusal_swallowed catalog::alter::tests::a_store_poisoned_between_plan_and_apply_leaves_the_table_consistently_altered
mutant M24_rewrite_stamps_eagerly catalog::alter::tests::a_plain_alter_stamps_every_moved_row_at_its_new_rid_with_one_sync
mutant M25_stamp_the_old_rid catalog::alter::tests::a_plain_alter_stamps_every_moved_row_at_its_new_rid_with_one_sync
mutant M26_more_than_one_moved_row catalog::alter::tests::a_rewrite_that_moves_one_attributed_row_still_stamps_it
mutant M27_epoch_bump_after_provenance catalog::alter::tests::a_failed_flush_after_a_rewrite_leaves_the_table_consistently_altered
mutant M28_no_writable_probe catalog::alter::tests::a_poisoned_store_refusing_the_rewrites_stamps_leaves_the_table_consistently_altered
mutant M29_probe_without_attribution catalog::alter::tests::an_alter_that_stamps_nothing_is_not_refused_by_a_poisoned_provenance_store

git -C "$FIRE" checkout --detach -f -q "$FIX"
git -C "$WT" worktree remove --force "$FIRE"
echo "$(date -u +%FT%TZ) done; raw output in $OUT — commit it by explicit path before reading"
