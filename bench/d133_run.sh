#!/usr/bin/env bash
# D133 — run the pending-free queue sweep and write the raw artifact.
#
# Refuses rather than producing a contaminated number: it will not run without the fleet measure
# lock held, and will not run while the suite lock is taken.
#
# ⛔ **The first draw of this artifact was contaminated and the script could not tell.** It
# checked the suite lock ONCE, at start. `verify-suite.sh land21ae398` took that lock at
# 11:57:51Z, 106 seconds into a 252-second run, and every ms printed after that point was
# measured against a full suite build. The measure lock does not stop a suite from starting.
# So the lock is now sampled BEFORE AND AFTER EVERY ARM and the run exits non-zero if it ever
# became taken: a contended box returns "no effect", and a number that cannot say whether the
# box was quiet is not a number.
#
# Usage: bench/d133_run.sh > bench/d133_pending_len.txt 2>&1
set -u

WT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$WT/target/release/examples/d133_pending_len"

LABEL=d133-pending-len
SUITE_LOCK=${SUITE_LOCK:-/tmp/ferrodb-suite.lock}
LOCK_WAIT=${LOCK_WAIT:-3600}

# ---- HOLD the suite lock; do not wait for a gap that never comes ---------------------------
#
# Waiting for the lock to be free was tried and does not converge: THREE suites took it inside
# 35 minutes (land98a44ef 11:40, land21ae398 11:57, d127invert 12:08), and the gaps between them
# are shorter than this sweep. D130 — the other result in this bench directory — ran its whole
# measurement under this lock for the same reason. So this ACQUIRES it, with the same semantics
# `tools/verify-suite.sh:135-162` uses, and a suite that starts meanwhile queues behind us
# instead of racing us.
#
# `$$` is this script's pid and lives for the whole run, which is what makes the dead-holder
# break in verify-suite.sh meaningful against us. A subshell pid would read dead immediately and
# invite a healthy holder's lock to be broken.
_HELD=0
_release() { [ "$_HELD" = "1" ] || return 0; _HELD=0; rm -rf "$SUITE_LOCK"; }
_kill_descendants() {
    local p=$1 c
    for c in $(pgrep -P "$p" 2>/dev/null); do _kill_descendants "$c"; done
    kill -9 "$p" 2>/dev/null
}
_abort() {
    trap '' TERM INT                      # a second signal must not re-enter this handler
    local c
    for c in $(pgrep -P $$ 2>/dev/null); do _kill_descendants "$c"; done
    _release
    echo "REFUSED: aborted by SIG$1 after ${SECONDS}s. A sweep cut short is not a measurement." >&2
    exit 143
}

# ⛔ **Every arm goes through this, never a bare foreground call.** Fire-checked 2026-09-22: with
# the harness in the foreground a SIGTERM did NOT release the lock — bash does not run a trap
# until the foreground command returns, so the lock stayed held for the remaining 55 s of a
# `sleep 60` and the abort message read "after 60s". An arm here runs for minutes, and a stale
# `/tmp/ferrodb-suite.lock` blocks EVERY suite on the box. `wait` is interruptible where a
# foreground command is not; this is `tools/verify-suite.sh`'s note 7, arrived at the same way.
run_arm() {
    "$@" &
    _child=$!
    wait "$_child"
}
trap '_release' EXIT
trap '_abort TERM' TERM
trap '_abort INT' INT

# Am I still the holder? Anything else means someone broke the lock and is on the box with me.
CONTENDED=0
suite_state() {
    local owner pid
    owner=$(cat "$SUITE_LOCK/owner" 2>/dev/null || echo "GONE")
    pid=${owner%% *}
    if [ "$pid" = "$$" ]; then
        echo "# suite lock @ $1: HELD BY THIS RUN ($(date -u +%H:%M:%SZ), load $(uptime | sed 's/.*load averages*: *//'))"
    else
        CONTENDED=1
        echo "# suite lock @ $1: LOST — now '$owner' <- CONTAMINATED"
    fi
}

# ---- refusals ------------------------------------------------------------------------------
if [ ! -d /tmp/ferrodb-measure.lock ]; then
    echo "REFUSED: /tmp/ferrodb-measure.lock is not held. A contended box returns 'no effect'." >&2
    exit 2
fi
if [ ! -x "$BIN" ]; then
    echo "REFUSED: $BIN does not exist. Nothing would be measured." >&2
    exit 2
fi

# Poll at 5 s, not verify-suite.sh's 15 s: at 15 s this loses the handover to a suite that is
# also queueing, which has already cost this project four missed acquisitions in 50 minutes.
_waited=0
while ! mkdir "$SUITE_LOCK" 2>/dev/null; do
    _owner=$(cat "$SUITE_LOCK/owner" 2>/dev/null || echo "unknown")
    _pid=${_owner%% *}
    if [ -n "$_pid" ] && [ "$_pid" != "unknown" ] && ! kill -0 "$_pid" 2>/dev/null; then
        echo "$LABEL: suite lock held by dead pid $_pid — breaking it" >&2
        rm -rf "$SUITE_LOCK"; continue
    fi
    if [ "$_waited" -ge "$LOCK_WAIT" ]; then
        echo "REFUSED: waited ${LOCK_WAIT}s for the suite lock held by: $_owner" >&2
        exit 3
    fi
    [ "$_waited" -eq 0 ] && echo "$LABEL: queued behind $_owner" >&2
    sleep 5; _waited=$((_waited+5))
done
printf '%s %s %s\n' "$$" "$LABEL" "$(date -u +%FT%TZ)" > "$SUITE_LOCK/owner"
_HELD=1
echo "$LABEL: acquired the suite lock after ${_waited}s" >&2

# ---- provenance, read from the tree and the binary, not from intent ------------------------
echo "# D133 — pending-free queue length under a branching workload. RAW ARTIFACT."
echo "#"
echo "# run at            $(date -u +%Y-%m-%dT%H:%M:%SZ) UTC"
echo "# worktree          $WT"
echo "# branch            $(git -C "$WT" rev-parse --abbrev-ref HEAD)"
echo "# head              $(git -C "$WT" rev-parse HEAD)"
echo "# tree state        $(git -C "$WT" status --porcelain | wc -l | tr -d ' ') modified/untracked path(s)"
git -C "$WT" status --porcelain | sed 's/^/#                   /'
echo "# binary mtime      $(date -u -r "$BIN" +%Y-%m-%dT%H:%M:%SZ)"
echo "# host              $(uname -srm)"
echo "# measure lock      $(cat /tmp/ferrodb-measure.lock/owner 2>&1)"
echo "# suite lock        HELD BY THIS RUN: $(cat "$SUITE_LOCK/owner" 2>&1) (queued ${_waited}s)"
echo "# load before       $(uptime | sed 's/.*load averages*: *//')"
echo "#"
echo "# THE CONFIGURATION GATE, quoted from the code this harness invokes:"
echo "#   arena.rs persist_if_configured —"
sed -n '/fn persist_if_configured/,/^    }/p' "$WT/src/branch/arena.rs" | sed 's/^/#     /'
echo "#   cli/cli.rs, the shipped server — this is why persist=on is the production row:"
grep -n 'checkpoint_to' "$WT/src/cli/cli.rs" | sed 's/^/#     /'
echo "#   lease_thread.rs, the lock the reap loop runs inside:"
grep -n 'const REAP_CHUNK' "$WT/src/branch/lease_thread.rs" | sed 's/^/#     /'
echo "#"

# ---- the sweep -----------------------------------------------------------------------------
echo "=============================================================================="
echo "ARM 1 — the branch-count curve, in-memory catalog, both persist settings."
echo "  fan/chain pin their children forever (lag = infinity); fanlag holds the lag at ONE"
echo "  reap; fanreap reaps every child first (lag = zero); leaf is the forced negative."
echo "=============================================================================="
suite_state "before arm 1 persist=off"
run_arm env D133_SHAPES=fan,chain,fanreap,fanlag,leaf D133_BRANCHES=100,300,1000,3000 \
    D133_PERSIST=off D133_CATALOG=mem "$BIN"
rc1=$?
suite_state "after arm 1 persist=off"
echo "(arm 1 persist=off rc=$rc1)"
echo
suite_state "before arm 1 persist=on"
run_arm env D133_SHAPES=fan,fanlag,leaf D133_BRANCHES=100,300,1000,3000 \
    D133_PERSIST=on D133_CATALOG=mem "$BIN"
rc2=$?
suite_state "after arm 1 persist=on"
echo "(arm 1 persist=on rc=$rc2 — every pass fsyncs the whole map, so this arm is the slow one."
echo " Shapes trimmed to fan/fanlag/leaf: chain matched fan EXACTLY on every integer and"
echo " fanreap matches leaf, so running them here would hold the fleet's lock to re-derive"
echo " a duplicate. fan is the growing queue, fanlag the bounded one, leaf the fast path.)"
echo

echo "=============================================================================="
echo "ARM 2 — the SHIPPED catalog, at counts it can reach. D99's rule: two catalogs"
echo "must agree on the integers at a count both can reach, or one of them is the"
echo "thing being measured."
echo "=============================================================================="
suite_state "before arm 2"
run_arm env D133_SHAPES=fan,chain,fanreap,fanlag,leaf D133_BRANCHES=100,300 \
    D133_PERSIST=off D133_CATALOG=table "$BIN"
rc3=$?
suite_state "after arm 2"
echo "(arm 2 rc=$rc3)"
echo

echo "=============================================================================="
echo "ARM 3 — the PAGES axis, held against ARM 1's N=300 row. pending is pages"
echo "parked, not branches reaped, so P must scale it linearly or the model is wrong."
echo "=============================================================================="
suite_state "before arm 3"
for p in 1 4 16; do
    { env D133_SHAPES=fan D133_BRANCHES=300 D133_PERSIST=off D133_CATALOG=mem D133_PAGES=$p \
        "$BIN" | grep -E '^  (shape|fan)' | sed "s/^/  P=$p /"; } &
    wait $!
done
echo

suite_state "after arm 3"
echo
echo "=============================================================================="
echo "ARM 4 — a THIRD point on the shipped catalog's chain slope. Two points cannot"
echo "separate a slope from a drifting box, and this project has lost four results"
echo "to exactly that."
echo "=============================================================================="
suite_state "before arm 4"
run_arm env D133_SHAPES=chain D133_BRANCHES=600 D133_PERSIST=off D133_CATALOG=table "$BIN"
rc4=$?
suite_state "after arm 4"
echo "(arm 4 rc=$rc4)"
echo

echo "# load after        $(uptime | sed 's/.*load averages*: *//')"
echo "# arm rcs           $rc1 $rc2 $rc3 $rc4"
if [ "$rc1" -ne 0 ] || [ "$rc2" -ne 0 ] || [ "$rc3" -ne 0 ] || [ "$rc4" -ne 0 ]; then
    echo "# VERDICT: AT LEAST ONE ARM REFUSED. Do not quote this file."
    exit 2
fi
if [ "$CONTENDED" -ne 0 ]; then
    echo "# VERDICT: THIS RUN LOST THE SUITE LOCK PART-WAY. The integers stand — they are"
    echo "# counts and immune to load — but DO NOT QUOTE ANY ms COLUMN from this file."
    exit 3
fi
echo "# every arm returned 0, i.e. every fan/chain row parked pages and every leaf row did not."
