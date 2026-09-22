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

CONTENDED=0
# Print the suite lock's state and remember, for the whole run, whether it was ever taken.
suite_state() {
    if [ -d /tmp/ferrodb-suite.lock ]; then
        CONTENDED=1
        echo "# suite lock @ $1: TAKEN by $(cat /tmp/ferrodb-suite.lock/owner 2>&1) <- CONTAMINATED"
    else
        echo "# suite lock @ $1: free   ($(date -u +%H:%M:%SZ), load $(uptime | sed 's/.*load averages*: *//'))"
    fi
}

# ---- refusals ------------------------------------------------------------------------------
if [ ! -d /tmp/ferrodb-measure.lock ]; then
    echo "REFUSED: /tmp/ferrodb-measure.lock is not held. A contended box returns 'no effect'." >&2
    exit 2
fi
if [ -d /tmp/ferrodb-suite.lock ]; then
    echo "REFUSED: /tmp/ferrodb-suite.lock is taken by: $(cat /tmp/ferrodb-suite.lock/owner 2>&1)" >&2
    exit 2
fi
if [ ! -x "$BIN" ]; then
    echo "REFUSED: $BIN does not exist. Nothing would be measured." >&2
    exit 2
fi

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
D133_SHAPES=fan,chain,fanreap,fanlag,leaf D133_BRANCHES=100,300,1000,3000 D133_PERSIST=off \
    D133_CATALOG=mem "$BIN"
rc1=$?
suite_state "after arm 1 persist=off"
echo "(arm 1 persist=off rc=$rc1)"
echo
suite_state "before arm 1 persist=on"
D133_SHAPES=fan,chain,fanreap,fanlag,leaf D133_BRANCHES=100,300,1000 D133_PERSIST=on \
    D133_CATALOG=mem "$BIN"
rc2=$?
suite_state "after arm 1 persist=on"
echo "(arm 1 persist=on rc=$rc2 — every pass fsyncs the whole map, so this arm is the slow one)"
echo

echo "=============================================================================="
echo "ARM 2 — the SHIPPED catalog, at counts it can reach. D99's rule: two catalogs"
echo "must agree on the integers at a count both can reach, or one of them is the"
echo "thing being measured."
echo "=============================================================================="
suite_state "before arm 2"
D133_SHAPES=fan,chain,fanreap,fanlag,leaf D133_BRANCHES=100,300 D133_PERSIST=off \
    D133_CATALOG=table "$BIN"
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
    D133_SHAPES=fan D133_BRANCHES=300 D133_PERSIST=off D133_CATALOG=mem D133_PAGES=$p "$BIN" \
        | grep -E '^  (shape|fan)' | sed "s/^/  P=$p /"
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
D133_SHAPES=chain D133_BRANCHES=600 D133_PERSIST=off D133_CATALOG=table "$BIN"
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
    echo "# VERDICT: THE SUITE LOCK WAS TAKEN DURING THIS RUN. The integers stand — they are"
    echo "# counts and immune to load — but DO NOT QUOTE ANY ms COLUMN from this file."
    exit 3
fi
echo "# every arm returned 0, i.e. every fan/chain row parked pages and every leaf row did not."
