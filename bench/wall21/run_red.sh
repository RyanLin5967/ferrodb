#!/usr/bin/env bash
# Wall #21 lane: every pre-registered RED run, each at the commit it was predicted red at (lane FAN-QUEUE ROW
# steps a, f, h, i, k, l, m, n, o, p, and §8.22's q). Run under lockrun.sh with CARGO_TARGET_DIR set by the caller. Checks out
# each sha DETACHED in this worktree and returns to the branch at the end, also on a signal or an error.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
WT=/Users/idide/wt/ferrodb-wall21-reaped-subtree.noindex
BRANCH=wall21-reaped-subtree
cd "$WT" || exit 2
OUT=bench/wall21/red
mkdir -p "$OUT"
back(){ git checkout -q "$BRANCH" && echo "$(date -u +%FT%TZ) back on $(git rev-parse --short HEAD)" >> "$OUT/steps.log"; }
trap back EXIT
[ -z "$(git status --porcelain --untracked-files=no)" ] || { echo "tracked changes present; refusing" >> "$OUT/steps.log"; exit 3; }

# step <label> <sha> <cargo test args...>
step(){
  local label=$1 sha=$2; shift 2
  git checkout -q --detach "$sha" || { echo "$label: checkout $sha FAILED" >> "$OUT/steps.log"; return; }
  { echo "step=$label sha=$(git rev-parse HEAD)"; echo "cmd=cargo test $*"; date -u +%FT%TZ; } > "$OUT/$label.txt"
  timeout 1800 cargo test "$@" >> "$OUT/$label.txt" 2>&1
  local rc=$?
  echo "rc=$rc" >> "$OUT/$label.txt"
  echo "$(date -u +%FT%TZ) $label sha=$sha rc=$rc" >> "$OUT/steps.log"
}

step a_wall21 3d4d4d2 --test wall21_reaped_chain_liveness -- --nocapture
step f_d200_d201 372515f --test d200_reap_releases_id_slots --test d201_reaperless_seal_keeps_the_pin -- --nocapture
step h_audit1 204c077 --lib --test d200_reap_releases_id_slots --test d201_reaperless_seal_keeps_the_pin -- \
  a_healthy_open_reads_no_released_and_no_pinned_slot \
  a_reaperless_runtime_unlinks_a_subtree_once_nothing_in_it_is_alive \
  branch::table_catalog::tests::a_catalog_from_before_the_unreleased_span_builds_it_once_at_open
step i_audit2 7f23cee --test d200_reap_releases_id_slots -- a_cascade_that --nocapture
step k_audit2c1 1ed9042 --lib --test d200_reap_releases_id_slots -- \
  branch::table_catalog::tests::a_release_whose_liveness_read_fails_keeps_the_slot_keyed \
  a_resumed_reap_that_errs_does_not_fail_the_open
step l_buildcost a7563cb --lib -- branch::table_catalog::tests::the_one_time_build_scans_each_child_span_once --nocapture
step m_audit3 fa21775 --lib --test d200_reap_releases_id_slots -- \
  branch::table_catalog::tests::a_failed_free_list_write_keys_a_keyless_slot \
  branch::table_catalog::tests::a_release_that_cannot_read_its_record_keys_the_slot \
  branch::table_catalog::tests::an_unreadable_record_at_open_is_a_refusal_not_a_failed_open \
  branch::table_catalog::tests::an_unreadable_reaped_record_does_not_stop_the_one_time_build \
  branch::table_catalog::tests::a_release_whose_liveness_read_fails_keeps_the_slot_keyed \
  branch::table_catalog::tests::a_failed_free_list_write_leaves_the_slot_keyed \
  a_resumed_reap_that_errs_does_not_fail_the_open
step n_audit4 1ab35e9 --lib --test d200_reap_releases_id_slots -- \
  a_non_branch_error_at_open_is_that_slots_refusal \
  branch::table_catalog::tests::a_stale_reaping_key_on_a_live_record_is_refused_not_reaped \
  branch::table_catalog::tests::the_build_keys_a_cycle_member_with_nothing_below
step o_audit5 6024928 --lib --test d200_reap_releases_id_slots -- \
  a_write_error_inside_a_unit_at_open_fails_the_open \
  a_non_branch_read_inside_a_cascade_is_the_slots_refusal \
  branch::table_catalog::tests::a_failed_record_write_leaves_the_slot_in_a_state_span \
  branch::table_catalog::tests::a_question_from_above_a_cycle_ends
step p_audit6 661afe3 --lib --test d200_reap_releases_id_slots -- \
  a_failed_read_on_the_slow_path_at_open_is_the_reaps_refusal \
  branch::table_catalog::tests::a_live_record_re_deadlined_twice_appears_once \
  branch::table_catalog::tests::a_move_into_live_that_fails_never_leaves_a_live_record_unindexed \
  branch::table_catalog::tests::a_stale_quarantined_key_does_not_list_a_live_branch
step q_drain 89e4e82 --test d200_reap_releases_id_slots -- a_persistent_fault_in_the_drain_read_fails_no_open --nocapture
date -u +%FT%TZ > "$OUT/DONE"
