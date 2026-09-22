#!/usr/bin/env bash
# D130 — the batch-vs-threads sweep, at BOTH layers, under the machine-wide suite lock.
#
#   bench/d130_run.sh <output-file> [direct_reps] [pgwire_reps]
#
# WHY THE LOCK. This harness issues real fsyncs, and so does every other suite on this box. The
# quantity being measured is `f/sync`, a ratio of two counters taken in one process over one run,
# which is load-immune by construction — but a concurrent suite competing for the same device
# changes the fsync LATENCY `D`, and the model under test is `min(1/S, T/D)`. Holding the lock is
# therefore not about noise in a timing; it is about not moving the term the model is about.
#
# The lock is acquired, never polled-then-taken, and it records THIS script's `$$`, which lives for
# the whole run — the property `tools/verify-suite.sh` relies on when it breaks a dead holder's
# lock, and the one a subshell that exits immediately does not have.
set -u

export PATH="$HOME/.cargo/bin:$PATH"

OUT=${1:?usage: d130_run.sh <output-file> [direct_reps] [pgwire_reps]}
DIRECT_REPS=${2:-3}
PGWIRE_REPS=${3:-1}
# The pre-registered sweep. Overridable only so the harness itself can be smoke-tested cheaply
# before the run that is quoted; the defaults ARE the pre-registration and D130 names T=128 as
# mandatory, because every number this project has on this ladder is at T=64.
TLIST=${TLIST:-1,2,4,8,16,32,64,128}
# Which phases to run, and whether to truncate. An addendum run (a control added after the fact)
# must APPEND and must carry its own lock hold and its own timestamps, so a reader can see it was
# a separate acquisition rather than assume one continuous run.
PHASES=${PHASES:-direct pgwire}
APPEND=${APPEND:-0}
DIRECT_N=${DIRECT_N:-8000}
PGWIRE_F=${PGWIRE_F:-40}
LABEL="d130-$(git rev-parse --short HEAD 2>/dev/null || echo nohead)"

SUITE_LOCK=${SUITE_LOCK:-/tmp/ferrodb-suite.lock}
LOCK_WAIT=${LOCK_WAIT:-5400}
_HELD_LOCK=0
_release_lock() {
    [ "$_HELD_LOCK" = "1" ] || return 0
    _HELD_LOCK=0
    rm -rf "$SUITE_LOCK"
}
_abort() {
    trap '' TERM INT
    _release_lock
    echo "$LABEL: REFUSING — aborted by SIG$1 after ${SECONDS}s. A sweep cut short is not a count." >&2
    exit 143
}
trap '_release_lock' EXIT
trap '_abort TERM' TERM
trap '_abort INT' INT

_waited=0
while ! mkdir "$SUITE_LOCK" 2>/dev/null; do
    _owner=$(cat "$SUITE_LOCK/owner" 2>/dev/null || echo "unknown")
    _pid=${_owner%% *}
    # Break a lock ONLY when its holder is genuinely gone — the same rule, and the same reasoning,
    # as tools/verify-suite.sh:139-152.
    if [ -n "$_pid" ] && ! kill -0 "$_pid" 2>/dev/null; then
        echo "$LABEL: suite lock held by dead pid $_pid — breaking it" >&2
        rm -rf "$SUITE_LOCK"; continue
    fi
    if [ "$_waited" -ge "$LOCK_WAIT" ]; then
        echo "$LABEL: REFUSING — waited ${LOCK_WAIT}s for the suite lock held by: $_owner" >&2
        exit 3
    fi
    [ "$_waited" -eq 0 ] && echo "$LABEL: queued behind a running suite ($_owner)" >&2
    # 5s, not the 15s tools/verify-suite.sh uses. Not impatience: a longer poll loses the race
    # for a freed lock to a shorter one at roughly the ratio of the intervals, and with a steady
    # stream of suites on this box a 30s poll was starved out of four consecutive handovers.
    # The poll is a mkdir on tmpfs, so the shorter interval costs nothing.
    sleep 5; _waited=$((_waited+5))
done
printf '%s %s %s\n' "$$" "$LABEL" "$(date -u +%FT%TZ)" > "$SUITE_LOCK/owner"
_HELD_LOCK=1
echo "$LABEL: holding the suite lock as pid $$ (waited ${_waited}s)" >&2

run_bounded() {
    local bound=$1; shift
    timeout "$bound" "$@" >> "$OUT" 2>&1 &
    local child=$!
    wait "$child"
}

[ "$APPEND" = "1" ] || : > "$OUT"
{
    echo "D130 — IS THE GROUP-COMMIT BATCH SET BY THE THREAD COUNT?"
    echo "Run by bench/d130_run.sh under $SUITE_LOCK, so no other suite's fsyncs move D."
    echo "tree: $(git rev-parse HEAD 2>/dev/null)  worktree: $(pwd)"
    echo "host: $(uname -srm)  started: $(date -u +%FT%TZ)"
    echo
} >> "$OUT"

echo "$LABEL: building" >&2
if ! run_bounded 1800 cargo build --release --example fork_concurrency --example d130_pgwire_batch; then
    echo "$LABEL: REFUSING — the build did not succeed; see $OUT" >&2
    exit 1
fi

# ------------------------------------------------------------------------------------------------
# A PROOF PASS, under the same hold, before anything that gets quoted.
#
# The suite lock is a shared resource and this sweep holds it for a quarter of an hour. A run that
# refuses at minute fifteen for a reason discoverable at minute two — a harness that will not
# build, an `ABANDON` the server rejects, an instrument self-check that fails — has spent that
# hold for nothing and has to queue again. This pass is deliberately too small to be evidence and
# is labelled as such; its only job is to reach every refusal in both binaries.
{
    echo
    echo "================================================================================"
    echo "PROOF PASS — NOT EVIDENCE. Two threads, a handful of forks, both binaries."
    echo "Its only purpose is to reach every refusal in both harnesses under this same lock"
    echo "hold, so a sweep that cannot run fails in minutes rather than at the end. Read no"
    echo "ratio out of it: at this size f/sync is dominated by whichever thread arrived first."
    echo "================================================================================"
} >> "$OUT"
if ! run_bounded 600 ./target/release/examples/fork_concurrency 200 2; then
    echo "$LABEL: REFUSING — the direct harness failed its proof pass; see $OUT" >&2
    exit 1
fi
if ! run_bounded 600 ./target/release/examples/d130_pgwire_batch 4 2; then
    echo "$LABEL: REFUSING — the pgwire harness failed its proof pass; see $OUT" >&2
    exit 1
fi

# ------------------------------------------------------------------------------------------------
case " $PHASES " in *" direct "*)
{
    echo
    echo "================================================================================"
    echo "MODE=direct — examples/fork_concurrency.rs, UNMODIFIED."
    echo
    echo "  LAYER: TableBranchCatalog::fork driven directly (open_sidecar + cat.fork), holding"
    echo "  \`logical\` and nothing else. This is the layer the recorded 21x/1.9x/12.6x pair and"
    echo "  the L0->L3 ladder were measured at. It is NOT what any shipped front-end does."
    echo
    echo "  N=$DIRECT_N forks per arm, matching the rung of bench/ceiling_raw/22_direct_crosscheck.txt."
    echo "  The rung is the REAL fork: no stub level. The ceiling ladder's L0 row (the only"
    echo "  unstubbed one) reported f/sync 23.8 at T=64; its 32.0 is L3, which is not a database."
    echo
    echo "⛔ forks/sec in this harness's own output is an UPPER BOUND on a shared box and is NOT"
    echo "   evidence for anything in D130. The f/sync column is the finding."
    echo "================================================================================"
} >> "$OUT"

for rep in $(seq 1 "$DIRECT_REPS"); do
    { echo; echo "--- direct rep $rep/$DIRECT_REPS  load: $(uptime | sed 's/.*load averages: //')"; } >> "$OUT"
    if ! run_bounded 1800 ./target/release/examples/fork_concurrency "$DIRECT_N" "$TLIST"; then
        echo "$LABEL: REFUSING — direct rep $rep failed or timed out; see $OUT" >&2
        exit 1
    fi
done
;; esac

# ------------------------------------------------------------------------------------------------
case " $PHASES " in *" pgwire "*)
{
    echo
    echo "================================================================================"
    echo "MODE=pgwire — examples/d130_pgwire_batch.rs."
    echo "================================================================================"
} >> "$OUT"

for rep in $(seq 1 "$PGWIRE_REPS"); do
    { echo; echo "--- pgwire rep $rep/$PGWIRE_REPS"; } >> "$OUT"
    if ! run_bounded 3600 ./target/release/examples/d130_pgwire_batch "$PGWIRE_F" "$TLIST"; then
        echo "$LABEL: REFUSING — pgwire rep $rep failed or timed out; see $OUT" >&2
        exit 1
    fi
done
;; esac

{ echo; echo "finished: $(date -u +%FT%TZ)"; } >> "$OUT"
echo "$LABEL: done -> $OUT" >&2
