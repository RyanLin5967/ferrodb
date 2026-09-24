#!/usr/bin/env bash
# bench/d246/run.sh — the D246 FAN-QUEUE row, steps (a)–(d).
#
# ⛔ FAN WORK: it builds pgserver and runs it. Run it only when the lead releases the D246 row, and
# only under the suite lock:
#   bash ~/wt/logs/lead-lanes-0924/lockrun.sh d246-run bash /Users/idide/wt/ferrodb-d246.noindex/bench/d246/run.sh
#
# Pre-registered in artie-research frontier/lane_d246_pgserver_provenance.md and, for A1–A3,
# bench/d246/PREREG.md (ba5914b), both committed before this script first ran. Raw output only,
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
FIX=89b6624    # D246 on D219 tip 1c226d8 (f5154d1), plus A1 (69f8e85) and the A2/A3 fix
T=d246_pgserver_provenance_survives_restart
TF=d246_fork_run_is_durable_after_the_guard
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
step a2_red_fork_$RED2 timeout 3600 cargo test --test "$TF"
step a2_red_publish_$RED2 timeout 3600 cargo test --lib agent_sql::runtime::tests::a_publish

# ---- (b) GREEN: the fix -------------------------------------------------------------------------
git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
build_and_test b_green_$FIX
step b_green_fork_$FIX timeout 3600 cargo test --test "$TF"
step b_green_d159_$FIX timeout 3600 cargo test --test d159_fork_sync_is_deferred
step b_green_d219_$FIX timeout 3600 cargo test --test d219_one_provenance_sync_per_merge
step b_green_lib_provenance_$FIX timeout 3600 cargo test --lib provenance::
step b_green_lib_group_commit_$FIX timeout 3600 cargo test --lib branch::group_commit
step b_green_lib_alter_$FIX timeout 3600 cargo test --lib catalog::alter::tests
step b_green_lib_publish_$FIX timeout 3600 cargo test --lib agent_sql::runtime::tests::a_publish

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

# ---- (d) A2/A3 mutants, each on a fresh checkout of the fix (PREREG A2's table) ------------------
# mutant <name> <filter>...: FORK = the fork integration target, else a `cargo test --lib` filter.
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
    else
      step "d_${m}_$i" timeout 3600 cargo test --lib "$filter"
    fi
  done
}
mutant M3_intern_under_the_guard FORK                       # T1, T3 fail
mutant M4_complete_skips_the_run FORK                       # T1, T2, T3 fail
mutant M5_drop_skips_the_run FORK                           # T4 fails
mutant M6_publish_before_the_run_is_durable agent_sql::runtime::tests::a_publish FORK   # R1 fails; FORK all pass
mutant M7_run_sync_under_the_lock provenance::durable::tests::   # U1 fails
mutant M8_written_run_is_not_awaited provenance::durable::tests::   # U2 fails
mutant M9_synchronous_sync_covers_nothing provenance::durable::tests::   # U3 fails
mutant M10_intern_repeat_does_not_await provenance::durable::tests::   # U4 fails

git -C "$FIRE" checkout --detach -f -q "$FIX"
git -C "$WT" worktree remove --force "$FIRE"
echo "$(date -u +%FT%TZ) done; raw output in $OUT — commit it by explicit path BEFORE reading"
