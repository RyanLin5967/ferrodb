#!/usr/bin/env bash
# Red/green verification of lane d216-clean-restart against its PREREG (lane report §9 (A)-(H) and the
# FAN-QUEUE row): each red tree, then each tip, one tree at a time in a throwaway worktree, every cargo
# run through the fleet lock (lockrun.sh). Usage: verify_red_green.sh <outdir>
#
# Writes <outdir>/<label>__<target>.txt (cargo's whole output, then "rc=N") and <outdir>/progress.txt;
# the last line of progress.txt is "DONE" only when every run has finished. Commit the outputs by
# explicit path BEFORE reading them.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
# The D216 gate controls need their pages dirty at write-back (lane PREREG (G), (H)).
unset FERRODB_CHECKPOINT_INTERVAL
OUT=${1:?usage: verify_red_green.sh <outdir>}
TREE=/Users/idide/wt/ferrodb-d216-verify.noindex
REPO=/Users/idide/projects/ferrodb
LOCKRUN=$HOME/wt/logs/lead-lanes-0924/lockrun.sh
TGT=/Users/idide/wt/ferrodb-lane-target.noindex
mkdir -p "$OUT"
[ -e "$TREE" ] && { echo "refusing: $TREE already exists" | tee -a "$OUT/progress.txt"; exit 2; }

log() { echo "$(date -u +%FT%TZ) $*" >> "$OUT/progress.txt"; }

tree_at() {
  git -C "$TREE" checkout -q --detach "$1" || { log "CHECKOUT FAILED $1"; exit 2; }
  log "tree $1 = $(git -C "$TREE" rev-parse HEAD)"
}

run() { # label name cargo-test-args...
  local label=$1 name=$2; shift 2
  local f="$OUT/${label}__${name}.txt"
  (cd "$TREE" && bash "$LOCKRUN" "d216-$label-$name" env CARGO_TARGET_DIR="$TGT" timeout 1800 cargo test "$@") > "$f" 2>&1
  local rc=$?
  echo "rc=$rc" >> "$f"
  log "$label $name rc=$rc"
}

t216() { # label
  run "$1" lib_wal --lib wal::
  run "$1" d227 --test d227_restart_keeps_declarations
  run "$1" d247 --test d247_arena_page_write_back_flushes_no_log
  run "$1" site --test integration_cluster_snapshot an_install_under_a_wal_pin_is_refused_and_names_the_pin
}
t252() { # label
  t216 "$1"
  run "$1" lib_stream --lib replication::stream::
  run "$1" d252 --test d252_caught_up_subscription_lets_the_log_truncate
}

git -C "$REPO" worktree add --detach "$TREE" d96200d >/dev/null 2>&1 || { log "WORKTREE ADD FAILED"; exit 2; }
trap 'git -C "$REPO" worktree remove --force "$TREE" >/dev/null 2>&1' EXIT

# (a) red d216, (b) green d216.
tree_at d96200d; t216 a_red216
tree_at 717667c; t216 b_green216; run b_green216 d234 --test d234_decoder_history_still_collapses
# (d) red d252, (e) green d252.
tree_at 831646e; t252 d_red252
tree_at a152d77; t252 e_green252; run e_green252 d234 --test d234_decoder_history_still_collapses
# (g) red d276, (h) green d276.
tree_at 2110968; t252 g_red276
tree_at 341b542; t252 h_green276; run h_green276 d234 --test d234_decoder_history_still_collapses
log DONE
