#!/usr/bin/env bash
# Wall #21 lane: GREEN at the branch tip. Run under lockrun.sh with CARGO_TARGET_DIR set by the caller.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd /Users/idide/wt/ferrodb-wall21-reaped-subtree.noindex || exit 2
OUT=bench/wall21/green
mkdir -p "$OUT"
{ echo "head=$(git rev-parse HEAD)"; echo "target_dir=${CARGO_TARGET_DIR:-unset}"; git status --short; date -u +%FT%TZ; } > "$OUT/head.txt"
timeout 1800 cargo test --lib branch:: > "$OUT/lib_branch.txt" 2>&1
echo "rc=$?" >> "$OUT/lib_branch.txt"
timeout 1800 cargo test --test wall21_reaped_chain_liveness --test d200_reap_releases_id_slots \
  --test d201_reaperless_seal_keeps_the_pin -- --nocapture > "$OUT/mine.txt" 2>&1
echo "rc=$?" >> "$OUT/mine.txt"
timeout 1800 cargo test --test d124_owner_record_refusal --test d126_atomic_upsert --test d15_concurrent_fork_and_reap \
  --test d16_transitive_visibility --test d18_reaper_asks_the_catalog --test d19_leak_is_a_race \
  --test d33_add_arena_must_be_durable --test d40_reserved_pages_return_to_baseline \
  --test d41_narrow_ops_close_the_window --test d45_log_catalog_rmw --test d60_depth_is_not_capped \
  --test integration_branch_attestation --test w4_forget_branches --test w4_stale_branch_crosses_agents \
  --test w4_sweep_slot_recycle --test adv_f5_probe --test adv_f6_dropped_capture --test integration_zero_copy_fork \
  --test integration_simulate --test integration_cluster_grants --test integration_server_reaps \
  > "$OUT/neighbours.txt" 2>&1
echo "rc=$?" >> "$OUT/neighbours.txt"
date -u +%FT%TZ > "$OUT/DONE"
