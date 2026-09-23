#!/usr/bin/env bash
# bench/d42_verify_all.sh — every D42 arm, under ONE acquisition of the machine-wide suite lock.
#
# WHY ONE ACQUISITION. Each arm used to take and release the lock itself. On a box running an agent
# fleet that means queueing behind somebody else's suite once per arm — and, worse, another suite
# can start BETWEEN two arms, so the arms end up measured against different machine states while
# being reported as one result. Holding the lock across the batch makes the batch one measurement.
#
# WHAT IT RUNS, and why each is here rather than assumed:
#   1. happy path x3        — falsifier direction 3: the unloaded pass must be unchanged and silent.
#   2. falsifier 1          — starve the NODES; must earn INCONCLUSIVE with self-schedule HEALTHY,
#                             which is what shows signal C firing alone.
#   3. falsifier 1 BREAK_PS — a deliberately failing `ps`; must earn CLASSIFIER-BROKEN, i.e. refuse
#                             rather than silently degrade to a one-signal verdict.
#   4. falsifier 2          — a genuinely broken cluster in a THROWAWAY worktree; must earn FAILED.
#                             Needs its own tree because it leaves broken code on disk.
#   5. verify-suite selftest— marker drift plus the landing guard in both directions.
#
# Arms 2 and 4 are the two-directional falsifier: neither means anything without the other, because
# a detector that fires on everything is worse than none.
#
# Usage: bench/d42_verify_all.sh [throwaway-worktree-for-falsifier-2]
#        Without the throwaway path, arm 4 is REPORTED AS SKIPPED and the batch exits non-zero —
#        a batch that quietly dropped the must-not-fire half would be exactly the half-check this
#        row exists to prevent.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

BROKEN_TREE=${1:-}
LOCK=/tmp/ferrodb-suite.lock
held=0
lock_pid=
# LOCK-RELEASE-OWNERSHIP-CHECKED (D188). `[ "$held" = 1 ] && rm -rf "$LOCK"` tests a flag meaning
# "I acquired A lock once" and never asks whether the directory on disk is still THE lock it
# acquired. That shape fired live on 2026-09-23 and deleted a RUNNING suite's lock. Remove only on
# a pid match; anything else refuses and warns. Reasoning: note 8 in tools/verify-suite.sh.
release_lock_if_ours() {
    [ "$held" = 1 ] || return 0
    held=0
    [ -d "$LOCK" ] || { echo "WARNING: held $LOCK as pid $lock_pid; ALREADY GONE at release" >&2; return 0; }
    local o p
    o=$(cat "$LOCK/owner" 2>/dev/null) || o=
    p=${o%% *}
    case "$p" in
        '' | *[!0-9]*)
            echo "REFUSING to release $LOCK — owner missing or unparseable (read: '$o'); this run" >&2
            echo "  held it as pid $lock_pid. Leaving the directory alone." >&2
            return 0 ;;
    esac
    [ "$p" = "$lock_pid" ] || {
        echo "REFUSING to release $LOCK — now owned by pid $p, not this run (pid $lock_pid)." >&2
        echo "  Owner line: '$o'. This run's lock was removed and re-created by another run, which" >&2
        echo "  has been running UNPROTECTED alongside it. Leaving the new owner's lock intact." >&2
        return 0; }
    rm -rf "$LOCK"
}
cleanup() { release_lock_if_ours; return 0; }
trap cleanup EXIT INT TERM

w=0
while ! mkdir "$LOCK" 2>/dev/null; do
    o=$(cat "$LOCK/owner" 2>/dev/null || echo unknown); p=${o%% *}
    if [ -n "$p" ] && ! kill -0 "$p" 2>/dev/null; then rm -rf "$LOCK"; continue; fi
    [ "$w" -ge 5400 ] && { echo "REFUSING — waited ${w}s for the suite lock held by: $o"; exit 3; }
    [ "$w" -eq 0 ] && echo "queued behind a running suite ($o)" >&2
    sleep 15; w=$((w+15))
done
printf '%s %s %s\n' "$$" "d42-verify-all" "$(date -u +%FT%TZ)" > "$LOCK/owner"
lock_pid=$$        # what the release compares against; a flag cannot identify a directory
held=1
export D42_NOLOCK=1

echo "########## D42 — every arm, one lock, one machine state ##########"
echo "  when    : $(date -u +%FT%TZ)"
echo "  head    : $(git log -1 --format=%h)  $(git log -1 --format=%s)"
echo "  dirty   : $(git status --short | wc -l | tr -d ' ') file(s)"
echo "  loadavg : $(uptime | sed 's/.*load averages*: //')"
echo "  lock    : held for the whole batch"
echo ""

fails=0
arm() {
    local name=$1; shift
    echo ""
    echo "################ ARM: $name ################"
    if "$@"; then echo "ARM OK: $name"; else echo "ARM FAILED: $name"; fails=$((fails+1)); fi
}

happy() {
    local bad=0 i
    cargo build --examples >/dev/null 2>&1 || return 1
    for i in 1 2 3; do
        out=$(timeout 300 cargo test --test integration_consensus_failover 2>&1)
        echo "$out" | grep -E '^test result'
        grep -q '^test result: ok' <<<"$out" || bad=1
        # The happy path must stay SILENT: no classifier output on a run that never expired.
        grep -q 'classifier (D42)' <<<"$out" && { echo "⛔ classifier printed on a PASSING run"; bad=1; }
    done
    return $bad
}

arm "1. happy path x3 (must pass, must print nothing new)" happy
arm "2. falsifier 1 — starve the nodes (must be INCONCLUSIVE)" bash bench/d42_fire_starved_children.sh
arm "3. falsifier 1 BREAK_PS (must be CLASSIFIER-BROKEN)" env BREAK_PS=1 bash bench/d42_fire_starved_children.sh
if [ -n "$BROKEN_TREE" ]; then
    arm "4. falsifier 2 — broken cluster (must be FAILED)" bash bench/d42_fire_broken.sh "$BROKEN_TREE"
else
    echo ""
    echo "################ ARM: 4. falsifier 2 ################"
    echo "⛔ SKIPPED — no throwaway worktree given, so the must-NOT-fire half did not run."
    echo "   A batch reporting only the must-fire half would be a detector nobody tried to"
    echo "   make misfire. Re-run with: bench/d42_verify_all.sh <throwaway-worktree>"
    fails=$((fails+1))
fi
arm "5. verify-suite selftest (marker drift + landing guard)" bash tools/verify-suite-selftest.sh

echo ""
echo "########## RESULT ##########"
if [ "$fails" -eq 0 ]; then echo "ALL D42 ARMS PASSED at $(git log -1 --format=%h)"; exit 0; fi
echo "$fails ARM(S) FAILED at $(git log -1 --format=%h)"; exit 1
