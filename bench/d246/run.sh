#!/usr/bin/env bash
# bench/d246/run.sh — the D246 FAN-QUEUE row, steps (a)–(c).
#
# ⛔ FAN WORK: it builds pgserver and runs it. Run it only when the lead releases the D246 row, and
# only under the suite lock:
#   bash ~/wt/logs/lead-lanes-0924/lockrun.sh d246-run bash /Users/idide/wt/ferrodb-d246.noindex/bench/d246/run.sh
#
# Pre-registered in artie-research frontier/lane_d246_pgserver_provenance.md, committed before this
# script first ran. Raw output only, one file per step under bench/d246/, each ending in its own
# `rc=` line; commit every file by explicit path BEFORE reading any of them.
#
# Every build happens in a throwaway detached worktree ($FIRE), removed at the end with
# `git worktree remove` (never `wt reap`, which is fleet-wide).
set -u
WT=/Users/idide/wt/ferrodb-d246.noindex
FIRE=/Users/idide/wt/ferrodb-d246-fire.noindex
OUT=$WT/bench/d246
BASE=fbfe038   # D219 tip when D246 was cut: pgserver on the in-memory provenance store (pgserver.rs identical through 1c226d8)
RED=b6960d5    # the red test, before the fix
FIX=f5154d1    # D246 (fix 7e39cb1, claims b2af37e, 57c4dc2) merged onto D219 tip 1c226d8
T=d246_pgserver_provenance_survives_restart
export CARGO_TARGET_DIR=$WT/target

cd "$WT" || exit 3
[ -z "$(git status --porcelain)" ] || { echo "REFUSING: $WT is not clean"; exit 3; }
[ -e "$FIRE" ] && { echo "REFUSING: $FIRE exists; remove it with git worktree remove first"; exit 3; }
(cd tests/pg && python3 -B -c 'import pg_client') || { echo "REFUSING: python3 cannot import tests/pg/pg_client.py"; exit 3; }
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

# ---- (a) RED: the committed red test, before the fix --------------------------------------------
build_and_test a_red_$RED

# ---- (b) GREEN: the fix -------------------------------------------------------------------------
git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
build_and_test b_green_$FIX

# ---- (c) fire-checks on the fix -----------------------------------------------------------------
# M1: the fix removed (pgserver.rs as it was at BASE). Every claim fails.
git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
git -C "$FIRE" checkout "$BASE" -- examples/pgserver.rs || exit 3
build_and_test c_M1_no_durable_store

# M2: the durable store opened on ANOTHER file. Only the path claim fails.
git -C "$FIRE" checkout --detach -f -q "$FIX" || exit 3
(cd "$FIRE" && python3 - <<'EOF') || exit 3
p = "examples/pgserver.rs"
s = open(p).read()
old = '.with_durable_provenance(format!("{db}.provenance"))'
if s.count(old) != 1:
    raise SystemExit(f"REFUSING: {s.count(old)} matches for the M2 pattern")
open(p, "w").write(s.replace(old, '.with_durable_provenance(format!("{db}.provenance-elsewhere"))'))
EOF
build_and_test c_M2_another_file

git -C "$FIRE" checkout --detach -f -q "$FIX"
git -C "$WT" worktree remove --force "$FIRE"
echo "$(date -u +%FT%TZ) done; raw output in $OUT — commit it by explicit path before reading"
