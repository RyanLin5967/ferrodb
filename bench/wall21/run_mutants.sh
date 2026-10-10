#!/usr/bin/env bash
# Wall #21 lane §8.22: E1's BASE (both arena.rs mappings removed), M64, M65, M69, M70, each running its killer,
# in a THROWAWAY worktree at the given sha. Run under lockrun.sh with CARGO_TARGET_DIR set by the caller.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
WT=/Users/idide/wt/ferrodb-wall21-reaped-subtree.noindex
MT=/Users/idide/wt/ferrodb-wall21-mutants.noindex
OUT=$WT/bench/wall21/mutants
TIP=$1
mkdir -p "$OUT"
[ -e "$MT" ] && { echo "$MT exists; refusing" >> "$OUT/steps.log"; exit 3; }
cleanup(){ git -C "$WT" worktree remove --force "$MT" >/dev/null 2>&1; echo "$(date -u +%FT%TZ) removed $MT" >> "$OUT/steps.log"; }
trap cleanup EXIT
git -C "$WT" worktree add -q --detach "$MT" "$TIP" || { echo "worktree add failed" >> "$OUT/steps.log"; exit 4; }
cd "$MT" || exit 5

# run <label> <mutant>... -- <cargo test args...>
run(){
  local label=$1; shift
  local muts=""
  while [ "$1" != "--" ]; do muts="$muts $1"; shift; done; shift
  git checkout -q -- src/
  for m in $muts; do
    python3 "$WT/bench/wall21/mutate.py" "$m" >> "$OUT/steps.log" 2>&1 || { echo "$label: mutant $m did not apply" >> "$OUT/steps.log"; git checkout -q -- src/; return; }
  done
  { echo "mutant=$label applied=$muts base=$(git rev-parse HEAD)"; echo "cmd=cargo test $*"; date -u +%FT%TZ; git diff; } > "$OUT/$label.txt"
  timeout 1800 cargo test "$@" >> "$OUT/$label.txt" 2>&1
  local rc=$?
  echo "rc=$rc" >> "$OUT/$label.txt"
  echo "$(date -u +%FT%TZ) $label applied=$muts rc=$rc" >> "$OUT/steps.log"
  git checkout -q -- src/
}

E1=a_failed_read_on_the_slow_path_at_open_is_the_reaps_refusal
DRAIN=a_persistent_fault_in_the_drain_read_fails_no_open
run base_e1 page_birth window_read -- --test d200_reap_releases_id_slots -- $E1 --nocapture
run m65_window_read window_read -- --test d200_reap_releases_id_slots -- $E1 --nocapture
run m64_page_birth page_birth -- --test d200_reap_releases_id_slots -- $E1 --nocapture
run m69_drain_window drain_window -- --test d200_reap_releases_id_slots -- $DRAIN --nocapture
run m70_drain_record drain_record -- --test d200_reap_releases_id_slots -- $DRAIN --nocapture
date -u +%FT%TZ > "$OUT/DONE"
