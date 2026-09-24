#!/usr/bin/env bash
# build_c9d.sh — compile and run D212 (a') lane targets at c9d1e6e. Run under lockrun. Raw output only.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR=/Users/idide/wt/ferrodb-lane-target.noindex
export RUSTFLAGS="-D duplicate_macro_attributes -D dead_code"
unset FERRODB_CHECKPOINT_INTERVAL FERRODB_REVERT_RETENTION_MERGES
W=/Users/idide/wt/ferrodb-d212-history-store.noindex
OUT=/Users/idide/wt/logs/d212/c9d
TIP=c9d1e6e
cd "$W" || exit 2
[ -z "$(git status --porcelain --untracked-files=no)" ] || { echo "REFUSING: tree dirty"; exit 2; }
[ "$(git rev-parse HEAD)" = "$(git rev-parse $TIP)" ] || { echo "REFUSING: HEAD is not $TIP"; exit 2; }
step() {
  local name=$1 f=$OUT/$1; shift
  { echo "# $(date -u +%FT%TZ) HEAD=$(git rev-parse --short HEAD) dirty=$(git status --porcelain --untracked-files=no | wc -l | tr -d ' ')"
    echo "# cmd: $*"; } > "$f"
  "$@" >> "$f" 2>&1
  local rc=$?
  echo "# rc=$rc end=$(date -u +%FT%TZ)" >> "$f"
  echo "$name rc=$rc"
  return $rc
}
step CHECK.txt timeout 1800 cargo check --lib --tests --examples || { echo "check failed; stopping"; exit 1; }
step TEST_integration.txt timeout 1800 cargo test --no-fail-fast \
  --test d212_nonce_ids --test d212_history_durable --test d212_history_crash --test d212_history_window \
  --test d212_history_pin --test d212_history_reach --test d212_history_snapshot \
  --test d212_step0_revert_identity --test d226_revert_restores_row_author \
  --test wal_format_upgrade --test open_path_allowlist --test integration_capability_envelope --test agent_sql_surface
step TEST_lib.txt timeout 1800 cargo test --no-fail-fast --lib -- wal:: agent_sql:: consensus::snapshot replication::
step TEST_snapshot_ignored.txt timeout 900 cargo test --no-fail-fast --test d212_history_snapshot -- --ignored
[ "$(git rev-parse HEAD)" = "$(git rev-parse $TIP)" ] && [ -z "$(git status --porcelain --untracked-files=no)" ] && echo "tree still clean at $TIP" || echo "WARNING: tree moved during the run; discard"
echo done
