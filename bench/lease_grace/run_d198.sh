#!/usr/bin/env bash
# run_d198.sh — the registered steps of bench/lease_grace/PREREG.md, as the FAN-QUEUE row in
# artie-research/frontier/lane_lease_grace.md lists them: (0), (g), (g2), then the reds (a)–(f4).
#
#   bash bench/lease_grace/run_d198.sh <outdir> [phase...]      default: s0 g g2 a b c d e f f2 f3 f4
#
# The lead's build rule (2026-09-24, quiet mode off): every cargo call goes through
# ~/wt/logs/lead-lanes-0924/lockrun.sh with CARGO_TARGET_DIR=~/wt/ferrodb-lane-target.noindex and
# `timeout 1800`. One lock acquisition per phase, not per cargo call, for one reason: the target dir
# is SHARED, so its `debug/examples/` is too. A `cargo build --examples` and an example-spawning test
# in two acquisitions could spawn another lane's binary built in between, and the staleness guard in
# `tests/integration_server_reaps.rs` would pass it (it compares mtimes, not trees).
#
# Reds run in `git archive` trees under ~/wt/lease-grace-red.noindex/<sha>, never by checking out the
# worktree. A shared target dir reuses a unit whose sources look older than its last build (measured
# on a toy crate, cargo 1.97.1); these trees are safe only because this base's build.rs watches
# `.git/HEAD`, which a git-archive tree and a linked worktree do not have, so the build script and
# the crate rerun on every cargo call. Each phase's output must therefore show a `Compiling ferrodb`
# line naming its own tree; `check_compiled` below records whether it does.
#
# (0) failing voids every later step (PREREG amendment 14 S1), so the driver stops there.
# Outputs are written OUTSIDE the worktree, and nothing here reads them beyond an exit code.
set -u
if [ "${1:-}" = "--phase" ]; then
    # Inner mode: runs INSIDE one lockrun acquisition, in the tree to test.
    shift; phase=$1
    worst=0
    c() { local r; echo "### \$ $*"; timeout 1800 "$@"; r=$?; echo "### rc=$r :: $*"; [ "$r" -eq 0 ] || worst=$r; return "$r"; }
    case $phase in
        s0) bash tools/prepush.sh; r=$?; echo "### rc=$r :: bash tools/prepush.sh"; worst=$r ;;
        g)  c cargo test --test lease_expired_is_refused
            c cargo build --examples && c cargo test --test integration_server_reaps
            c cargo test --lib -- f1_lease_grace lease_thread::tests::f1_ cluster::tests::f2_ \
                the_alive_key_is_its_own_group an_over_long_lease_from_now the_first_start_key_is_its_own_group
            c cargo test --lib reaper
            c cargo test --test integration_simulate --test integration_capability_envelope \
                --test integration_cluster_grants --test integration_system_views --test d124_owner_record_refusal ;;
        g2) c cargo run --release --example d198_soft_mark_cost -- 2000 200 6 ;;
        a)  c cargo build --examples && c cargo test --test lease_expired_is_refused
            c cargo test --test integration_server_reaps a_lease_that
            c cargo test --lib f1_a_lease_that_lapsed ;;
        b)  c cargo test --lib a_resume_writes_the_same_number_of_keys ;;
        c)  c cargo test --test integration_cluster_grants an_over_long_lease
            c cargo test --lib -- an_over_long_lease_from_now a_shifted_deadline_never_forges ;;
        d)  c cargo test --lib -- a_catalog_with_a_last_alive_mark a_heartbeat_carries_the_offset f1_a_clean_stop ;;
        e)  c cargo test --lib f1_an_unresumed_marked_catalog ;;
        f)  c cargo test --lib -- a_pre_d198_catalog a_migrated_legacy_log set_root_and_set_state_after_a_resume ;;
        f2) c cargo test --lib f1_lease_grace ;;
        f3) c cargo test --lib -- an_unmarked_writer_whose_lease_clock_lagged the_soft_mark_is_the_last_commit ;;
        f4) c cargo test --lib -- an_unresumed_writer_whose_lease_clock_lagged_at_open \
                a_migration_by_a_process_whose_lease_clock_lagged the_pre_d198_fixture_helper_refuses ;;
        *)  echo "unknown phase $phase"; exit 2 ;;
    esac
    exit "$worst"
fi

OUT=${1:?usage: run_d198.sh <outdir> [phase...]}; shift
[ $# -gt 0 ] || set -- s0 g g2 a b c d e f f2 f3 f4
WT=/Users/idide/wt/ferrodb-lease-grace.noindex
TIP=434bedf
RED=/Users/idide/wt/lease-grace-red.noindex
TD=/Users/idide/wt/ferrodb-lane-target.noindex
LOCKRUN=/Users/idide/wt/logs/lead-lanes-0924/lockrun.sh
SELF=$WT/bench/lease_grace/run_d198.sh
export PATH="$HOME/.cargo/bin:$PATH"
mkdir -p "$OUT" "$RED"

child=""
trap '[ -n "$child" ] && kill -TERM "$child" 2>/dev/null; wait; echo "ABORTED by signal" >> "$OUT/RCS.txt"; exit 143' TERM HUP INT QUIT

sha_of() {
    case $1 in
        s0|g|g2) echo $TIP ;; a) echo ea60cc4 ;; b) echo 957e113 ;; c) echo c3f62ab ;; d) echo 7cd26ab ;;
        e) echo fb9bc43 ;; f) echo b5bf3a3 ;; f2) echo ba77cbd ;; f3) echo 960cc02 ;; f4) echo 1d64b36 ;;
    esac
}

# The tree a phase runs in: the worktree for the tip's phases, refusing unless HEAD is the tip and
# the tracked tree is clean; otherwise a git-archive extract of the phase's sha.
DIR=""
tree_for() {
    local phase=$1 sha full
    sha=$(sha_of "$phase")
    full=$(git -C "$WT" rev-parse --verify "$sha^{commit}") || { echo "cannot resolve $sha"; return 1; }
    if [ "$sha" = "$TIP" ]; then
        [ "$(git -C "$WT" rev-parse HEAD)" = "$full" ] || { echo "REFUSING: $WT HEAD is not $TIP"; return 1; }
        [ -z "$(git -C "$WT" status --porcelain --untracked-files=no)" ] || { echo "REFUSING: $WT has tracked changes"; return 1; }
        DIR=$WT
    else
        DIR=$RED/$sha
        if [ "$(cat "$DIR/.archived-sha" 2>/dev/null)" != "$full" ]; then
            rm -rf "$DIR" && mkdir -p "$DIR" || return 1
            git -C "$WT" archive "$full" | tar -x -C "$DIR" || return 1
            echo "$full" > "$DIR/.archived-sha"
        fi
    fi
}

for phase in "$@"; do
    f=$OUT/$phase.txt
    if ! tree_for "$phase" > "$f" 2>&1; then
        echo "$phase rc=PREP-FAILED" >> "$OUT/RCS.txt"; exit 1
    fi
    {
        echo "## phase $phase at $(sha_of "$phase") in $DIR"
        echo "## tree sha $(git -C "$DIR" rev-parse HEAD 2>/dev/null || cat "$DIR/.archived-sha")"
        echo "## start $(date -u +%FT%TZ) loadavg $(sysctl -n vm.loadavg)"
    } >> "$f"
    (cd "$DIR" && exec bash "$LOCKRUN" lease env CARGO_TARGET_DIR="$TD" bash "$SELF" --phase "$phase") >> "$f" 2>&1 &
    child=$!
    wait "$child"
    rc=$?
    child=""
    echo "## end $(date -u +%FT%TZ) loadavg $(sysctl -n vm.loadavg) rc=$rc" >> "$f"
    # Provenance: did cargo compile the crate from THIS tree in this phase? (s0's prepush pipes
    # cargo through `tail -3`, so its Compiling lines are not kept; it is marked n/a.)
    if [ "$phase" = s0 ]; then comp=n/a
    elif grep -qF "Compiling ferrodb v" "$f" && ! grep "Compiling ferrodb v" "$f" | grep -vqF "($DIR)"; then comp=own-tree
    else comp=NOT-SHOWN
    fi
    echo "$phase sha=$(sha_of "$phase") rc=$rc compiled=$comp" >> "$OUT/RCS.txt"
    if [ "$phase" = s0 ] && [ "$rc" -ne 0 ]; then
        echo "STOP: (0) failed; every later step is void (PREREG amendment 14 S1)" >> "$OUT/RCS.txt"; exit 1
    fi
done
echo "DONE $(date -u +%FT%TZ)" >> "$OUT/RCS.txt"
