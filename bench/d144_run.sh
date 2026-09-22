#!/usr/bin/env bash
# D144 — run the lease-produced lag sweep and write the raw artifact.
#
# Same lock discipline as bench/d133_run.sh, and for the same reasons: it HOLDS the suite lock
# rather than waiting for a gap that does not come, every arm runs backgrounded under `wait` so a
# signal can actually reach the trap, and the lock is released on EXIT.
#
# ⚠ This arm is WALL-CLOCK DRIVEN in a way D133's was not. D133's integers were immune to load;
# here the lease thread's sweep cadence and the fork schedule are both real time, so a box under
# load can shift the phase the whole experiment turns on. Holding the suite lock is therefore not
# hygiene here, it is a precondition.
set -u

WT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$WT/target/release/examples/d144_lease_lag"
LABEL=d144-lease-lag
SUITE_LOCK=${SUITE_LOCK:-/tmp/ferrodb-suite.lock}
LOCK_WAIT=${LOCK_WAIT:-3600}

SCAN_MS=${SCAN_MS:-3000}
# Where this run's own stdout lands, so the feature check below can read back what was printed.
OUT_SELF=${OUT_SELF:-$WT/bench/d144_lease_lag.txt}

_HELD=0
_release() { [ "$_HELD" = "1" ] || return 0; _HELD=0; rm -rf "$SUITE_LOCK"; }
_kill_descendants() {
    local p=$1 c
    for c in $(pgrep -P "$p" 2>/dev/null); do _kill_descendants "$c"; done
    kill -9 "$p" 2>/dev/null
}
_abort() {
    trap '' TERM INT
    local c
    for c in $(pgrep -P $$ 2>/dev/null); do _kill_descendants "$c"; done
    _release
    echo "REFUSED: aborted by SIG$1 after ${SECONDS}s. A sweep cut short is not a measurement." >&2
    exit 143
}
trap '_release' EXIT
trap '_abort TERM' TERM
trap '_abort INT' INT

run_arm() { "$@" & _child=$!; wait "$_child"; }

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

if [ ! -d /tmp/ferrodb-measure.lock ]; then
    echo "REFUSED: /tmp/ferrodb-measure.lock is not held." >&2
    exit 2
fi
if [ ! -x "$BIN" ]; then
    echo "REFUSED: $BIN does not exist. Nothing would be measured." >&2
    exit 2
fi

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

echo "# D144 — the lag the LEASE produces. RAW ARTIFACT."
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
echo "#"
echo "# ⛔ THE SCAN INTERVAL THIS RUN USED IS **$SCAN_MS ms**, NOT THE SHIPPED 30 s."
echo "#    Δ is swept against \$SCAN_MS, never against 30 s. The knob is the shipped one:"
grep -n 'pub const SCAN_INTERVAL_ENV\|pub const DEFAULT_SCAN_MILLIS' "$WT/src/branch/lease_thread.rs" | sed 's/^/#     /'
echo "#    and the harness reads it through the server's own scan_interval_from_env():"
sed -n '/pub fn scan_interval_from_env/,/^}/p' "$WT/src/branch/lease_thread.rs" | sed 's/^/#     /'
echo "#"
echo "# ⛔ THE LEASE IS COMPRESSED. The shipped constant has no environment override:"
grep -n 'pub const DEFAULT_LEASE_MILLIS' "$WT/src/agent_sql/runtime.rs" | sed 's/^/#     /'
grep -n 'LeaseDeadline::from_now(DEFAULT_LEASE_MILLIS)' "$WT/src/agent_sql/runtime.rs" | sed 's/^/#     /'
echo "#    The window under test is exactly Δ and does not contain L, so a compressed L rescales"
echo "#    WHEN the window opens and not HOW WIDE it is. The CONTROL arm varies L at fixed Δ and"
echo "#    must not move; the run REFUSES if it does."
echo "#"

echo "=============================================================================="
echo "ARM 1 — Δ swept ACROSS the scan interval S=$SCAN_MS ms, below and above."
echo "=============================================================================="
suite_state "before arm 1"
run_arm env FERRODB_LEASE_SCAN_MILLIS="$SCAN_MS" D144_LEASE_MS=20000 D144_PAIRS=60 \
    D144_DELTAS=0,500,1000,1500,2000,2500,3000,4500,6000,9000 \
    D144_CONTROL_DELTA=6000 D144_CONTROL_LEASES=20000,40000 "$BIN"
rc1=$?
suite_state "after arm 1"
echo "(arm 1 rc=$rc1)"
echo

# ⛔ ASK THE ARTIFACT WHICH BINARY RAN, not the filesystem. Draw 1 was voided because a rebuild
# landed while the run was already executing: a running process keeps its mapped image, so an
# mtime check on disk would have said "current" while the old code was producing the rows. The
# only witness that cannot lie is the output itself, and the calibration columns are only printed
# by a build that has the calibration.
if ! grep -q "S run/cal" "$OUT_SELF" 2>/dev/null; then
    echo "# VERDICT: THE ROWS ABOVE CAME FROM A BINARY WITHOUT THE REALISED-INTERVAL CALIBRATION."
    echo "# Its Δ/S divides a measured Δ by an ASSUMED S. Void this file."
    exit 4
fi
echo "# binary self-identified from its own output: calibration columns present."
echo "# load after        $(uptime | sed 's/.*load averages*: *//')"
echo "# arm rcs           $rc1"
if [ "$rc1" -ne 0 ]; then
    echo "# VERDICT: THE ARM REFUSED. Do not quote this file."
    exit 2
fi
if [ "$CONTENDED" -ne 0 ]; then
    echo "# VERDICT: THIS RUN LOST THE SUITE LOCK PART-WAY. Every number here is wall-clock"
    echo "# driven, so unlike D133 NOTHING in this file survives that. Do not quote it."
    exit 3
fi
echo "# the arm returned 0: every lease thread scanned, none refused, none failed, and the"
echo "# lease control did not move."
