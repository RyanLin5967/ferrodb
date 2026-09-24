#!/usr/bin/env bash
# bench/d246/run.sh — the D246 FAN-QUEUE row, steps (a)–(d).
#
# ⛔ FAN WORK: it builds pgserver and runs it. Run it only when the lead releases the D246 row, and
# only under the suite lock:
#   bash ~/wt/logs/lead-lanes-0924/lockrun.sh d246-run bash /Users/idide/wt/ferrodb-d246.noindex/bench/d246/run.sh
#
# Pre-registered in artie-research frontier/lane_d246_pgserver_provenance.md and, for A1–A4c,
# bench/d246/PREREG.md (ba5914b onward, append-only), all committed before this script first ran. Raw output only,
# one file per step under bench/d246/, each ending in its own `rc=` line; commit every file by
# explicit path BEFORE reading any of them.
#
# Every build happens in a throwaway detached worktree ($FIRE), removed at the end with
# `git worktree remove` (never `wt reap`, which is fleet-wide).
set -u
WT=/Users/idide/wt/ferrodb-d246.noindex
FIRE=/Users/idide/wt/ferrodb-d246-fire.noindex
OUT=$WT/bench/d246
BASE=fbfe038   # D219 tip when D246 was cut: pgserver on the in-memory provenance store (pgserver.rs identical through 1c226d8)
RED=b6960d5    # the red test, before the fix
RED2=8764a2d   # A2/A3's red tests (the fork's run synced under the guard) on eca45fb's code
RED4=a7127cd   # A4's red tests (the fresh review's defects) on the A2/A3 fix's code
FIX=2e554e8    # D246 + D219 tip cd0302c (910add7), A1 (69f8e85), the A2/A3 fix, A4's (312301c) and A4c's
T=d246_pgserver_provenance_survives_restart
TF=d246_fork_run_is_durable_after_the_guard
# Pre-registered test names, each required in the output of the steps that name it (A4c).
T_FORK="a_new_runs_record_is_synced_after_the_guard_not_under_it a_known_runs_fork_issues_no_provenance_sync new_runs_staged_before_either_completes_share_one_provenance_sync a_staged_fork_dropped_without_completing_still_makes_its_run_durable"
U_A2="a_runs_group_sync_does_not_hold_the_lock_every_staged_write_needs a_second_await_of_a_run_already_written_waits_for_its_sync a_run_a_synchronous_append_already_carried_costs_its_await_nothing interning_a_run_whose_record_is_pending_makes_it_durable_first"
U_A4="a_run_sync_overlapping_a_failed_append_is_not_acknowledged a_stamp_queued_behind_a_run_is_never_left_written_but_unsynced a_poisoned_store_refuses_a_pending_intern_and_leaves_the_index_alone a_poisoned_store_refuses_await_run_and_writes_nothing"
R12="a_publish_never_declares_to_the_log_a_run_its_provenance_file_lacks a_publish_of_an_uncompleted_fork_declares_its_run_and_the_file_holds_it"
R3="a_publish_refused_for_its_runs_record_leaves_the_schema_unchanged"
R4="a_failed_completion_closes_the_session_it_opened"
N5="a_rewrite_refused_mid_restamp_still_writes_the_stamps_before_it"
D219F1="a_store_poisoned_between_plan_and_apply_leaves_the_table_consistently_altered"
export CARGO_TARGET_DIR=$WT/target

cd "$WT" || exit 3
[ -z "$(git status --porcelain)" ] || { echo "REFUSING: $WT is not clean"; exit 3; }
[ -e "$FIRE" ] && { echo "REFUSING: $FIRE exists; remove it with git worktree remove first"; exit 3; }
(cd tests/pg && python3 -B -c 'import pg_client') || { echo "REFUSING: python3 cannot import tests/pg/pg_client.py"; exit 3; }
python3 bench/d246/mutants.py --check "$FIX" || exit 3
mkdir -p "$OUT"
git worktree add --detach "$FIRE" "$RED" || exit 3

# step <name> <cmd...>: run in $FIRE, stdout+stderr and the rc on its own line into $OUT/<name>.txt
step() {
  local name=$1 rc
  shift
  echo "$(date -u +%FT%TZ) $name: $*"
  {
    echo "# $name at $(git -C "$FIRE" rev-parse HEAD) $(date -u +%FT%TZ)"
    git -C "$FIRE" diff --stat | sed 's/^/# dirty: /'
    echo "# cmd: $*"
  } > "$OUT/$name.txt"
  (cd "$FIRE" && "$@") >> "$OUT/$name.txt" 2>&1
  rc=$?
  # A test step that collected nothing has not passed (PREREG A4, N-8): a filter that matches no
  # test prints "running 0 tests" and exits 0.
  if [ "$3" = cargo ] && [ "$4" = test ] && grep -q '^running 0 tests' "$OUT/$name.txt"; then
    echo "# REFUSED: this step collected zero tests" >> "$OUT/$name.txt"
    rc=97
  fi
  # And a count is not a name (A4c): every test REQUIRE names must appear in this step's output.
  local t
  for t in ${REQUIRE:-}; do
    grep -q "$t" "$OUT/$name.txt" || {
      echo "# REFUSED: pre-registered test $t is not in this step's output" >> "$OUT/$name.txt"
      rc=98
    }
  done
  echo "rc=$rc" >> "$OUT/$name.txt"
}

# Build pgserver AFTER the checkout, so the test's stale-binary guard sees a binary newer than src/.
build_and_test() {
  local name=$1
  step "${name}_build" timeout 3600 cargo build --example pgserver
  step "$name" timeout 3600 cargo test --test "$T"
}

# ---- (a) RED: the committed red tests, before the fix --------------------------------------------
build_and_test a_red_$RED
# A2/A3 (PREREG): T1 and T3 FAIL, T2 and T4 pass; R1 and R2 pass (the base synced the run at BEGIN).
git -C "$FIRE" checkout --detach -f -q "$RED2" || exit 3
REQUIRE="$T_FORK" step a2_red_fork_$RED2 timeout 3600 cargo test --test "$TF"
REQUIRE="$R12" step a2_red_publish_$RED2 timeout 3600 cargo test --lib agent_sql::runtime::tests::a_publish
# A4 as corrected by A4c: U5 FAILS, U6-U8 pass; R3 FAILS, R1 and R2 pass; the N-5 test FAILS.
git -C "$FIRE" checkout --detach -f -q "$RED4" || exit 3
REQUIRE="$U_A2 $U_A4" step a4_red_durable_$RED4 timeout 3600 cargo test --lib provenance::durable::tests::
REQUIRE="$R12 $R3" step a4_red_publish_$RED4 timeout 3600 cargo test --lib agent_sql::runtime::tests::a_publish
REQUIRE="$N5" step a4_red_alter_$RED4 timeout 3600 cargo test --lib catalog::alter::tests

# ---- (b) GREEN: the fix -------------------------------------------------------------------------
git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
build_and_test b_green_$FIX
REQUIRE="$T_FORK" step b_green_fork_$FIX timeout 3600 cargo test --test "$TF"
step b_green_d159_$FIX timeout 3600 cargo test --test d159_fork_sync_is_deferred
step b_green_d219_$FIX timeout 3600 cargo test --test d219_one_provenance_sync_per_merge
REQUIRE="$U_A2 $U_A4" step b_green_lib_provenance_$FIX timeout 3600 cargo test --lib provenance::
step b_green_lib_group_commit_$FIX timeout 3600 cargo test --lib branch::group_commit
REQUIRE="$N5 $D219F1" step b_green_lib_alter_$FIX timeout 3600 cargo test --lib catalog::alter::tests
REQUIRE="$R12 $R3 $R4" step b_green_lib_runtime_$FIX timeout 3600 cargo test --lib agent_sql::runtime::tests::
for t in d154_length_prefix_refusal agent_sql_surface integration_prompt_clause integration_run_identity_feed; do
  step b_green_${t}_$FIX timeout 3600 cargo test --test "$t"
done

# ---- (c) fire-checks on the fix -----------------------------------------------------------------
# M1: the fix removed (pgserver.rs as it was at BASE). C1, C2 (decode refused at this base), C3, C4.
git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
git -C "$FIRE" checkout "$BASE" -- examples/pgserver.rs || exit 3
build_and_test c_M1_no_durable_store

# M2: the durable store opened on ANOTHER file. Only the path claim fails.
git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
(cd "$FIRE" && python3 - <<'PYEOF') || exit 3
p = "examples/pgserver.rs"
s = open(p).read()
old = '.with_durable_provenance(format!("{db}.provenance"))'
if s.count(old) != 1:
    raise SystemExit(f"REFUSING: {s.count(old)} matches for the M2 pattern")
open(p, "w").write(s.replace(old, '.with_durable_provenance(format!("{db}.provenance-elsewhere"))'))
PYEOF
build_and_test c_M2_another_file

# ---- (d) A2–A4 mutants, each on a fresh checkout of the fix (PREREG A2's and A4's tables) --------
# mutant <name> <filter>...: FORK = the fork integration target, D219 = D219's integration target,
# else a `cargo test --lib` filter.
mutant() {
  local m=$1 i=0 filter
  shift
  git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
  (cd "$FIRE" && python3 "$WT/bench/d246/mutants.py" "$m") > "$OUT/d_${m}_applied.txt" 2>&1 || exit 3
  git -C "$FIRE" diff >> "$OUT/d_${m}_applied.txt"
  for filter in "$@"; do
    i=$((i + 1))
    if [ "$filter" = FORK ]; then
      step "d_${m}_$i" timeout 3600 cargo test --test "$TF"
    elif [ "$filter" = D219 ]; then
      step "d_${m}_$i" timeout 3600 cargo test --test d219_one_provenance_sync_per_merge
    else
      step "d_${m}_$i" timeout 3600 cargo test --lib "$filter"
    fi
  done
}
mutant M3_intern_under_the_guard FORK                       # T1, T3 fail
mutant M4_complete_skips_the_run FORK                       # T1, T2, T3 fail
mutant M5_drop_skips_the_run FORK                           # T4 fails
mutant M6_publish_before_the_run_is_durable agent_sql::runtime::tests::a_publish FORK   # R1 and R3 fail (A4c); FORK all pass
mutant M7_run_sync_under_the_lock provenance::durable::tests::   # U1 and U5 fail; U6 passes after 30 s (A4c)
mutant M8_written_run_is_not_awaited provenance::durable::tests::   # U2 fails
mutant M9_synchronous_sync_covers_nothing provenance::durable::tests::   # U3 fails
mutant M10_intern_repeat_does_not_await provenance::durable::tests::   # U4 fails
# PREREG A4
mutant M11_repeat_run_syncs FORK                            # T2 fails, only T2
mutant M15_pending_written_last provenance::deferred::tests::stamps_through_the_stamper_ride_the_next_sync_and_write_the_same_file
mutant M16_sync_before_write provenance::                   # SURVIVOR: all pass
mutant M17_ticket_before_write provenance:: FORK agent_sql::runtime::tests::a_   # SURVIVOR: all pass
mutant M18_covered_before_sync provenance::                 # SURVIVOR: all pass
mutant M19_no_poison_check_after_the_run_sync provenance::durable::tests::   # U5 fails
mutant M20_await_writes_every_pending_record provenance::durable::tests::   # U6 fails
mutant M21_intern_pending_ignores_the_poison provenance::durable::tests::   # U7 fails
mutant M22_await_run_ignores_the_poison provenance::durable::tests::   # U8 fails
mutant M23_await_after_the_schema_apply agent_sql::runtime::tests::a_   # R3 fails
mutant M24_failed_complete_keeps_the_session agent_sql::runtime::tests::a_failed_completion   # R4 fails
mutant M25_refused_restamp_leaves_stamps_pending catalog::alter::tests   # the N-5 test fails
# PREREG A4c
mutant M26_d219m19_rewrite_does_not_stamp D219 catalog::alter::tests::a_failed_flush_after_a_rewrite_leaves_the_table_consistently_altered
mutant M27_d219m22_flush_error_swallowed catalog::alter::tests::a_failed_flush_after_a_rewrite_leaves_the_table_consistently_altered
mutant M28_d219m23_stamp_refusal_swallowed catalog::alter::tests::$D219F1 catalog::alter::tests::$N5
mutant M29_d219m24_rewrite_stamps_eagerly catalog::alter::tests::a_plain_alter_stamps_every_moved_row_at_its_new_rid_with_one_sync
mutant M30_await_refuses_durable_runs provenance::durable::tests::   # U8 fails
mutant M31_shared_description provenance:: FORK               # SURVIVOR: all pass
mutant M32_post_check_without_the_lock provenance::durable::tests::   # SURVIVOR: all pass
mutant M33_executor_site_uses_complete agent_sql::runtime::tests::a_ FORK   # SURVIVOR: all pass
mutant M34_dispatch_site_uses_complete agent_sql::runtime::tests::a_ FORK   # SURVIVOR: all pass
mutant M35_pgwire_site_uses_complete agent_sql::runtime::tests::a_ FORK     # SURVIVOR: all pass

git -C "$FIRE" checkout --detach -f -q "$FIX"
git -C "$WT" worktree remove --force "$FIRE"
echo "$(date -u +%FT%TZ) done; raw output in $OUT — commit it by explicit path BEFORE reading"
