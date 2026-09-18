#!/usr/bin/env bash
# bench/d42_fire_starved_children.sh — D42 falsifier 1, the decisive arm: signal C must fire.
#
# WHY THIS ARM EXISTS. Two attempts at the pre-registered shape came first and both are kept, because
# a negative result here is a fact about the machine, not a gap in the record:
#
#   * 14x oversubscription (bench/d42_fire_starved.txt run 1): loadavg 137, test PASSED in 4.76 s.
#   * `nice -n 20` + 28x oversubscription (run 2): loadavg 254, test PASSED in 8.12 s.
#   * Six shapes swept (bench/d42_load_sweep.txt): both signals nearly FLAT from loadavg 269 to 829,
#     self-scheduling falling only 0.78 -> 0.55.
#
# Ambient CPU load is not a lever on this machine. macOS's timeshare scheduler demotes spinners, and
# a process asking for 0.4 % of a core keeps getting it however many spinners are queued. Hoping load
# wanders into the window is not a fire-check.
#
# ⚠ A SECOND FINDING, MEASURED, THAT CHANGES WHICH SIGNAL MATTERS. Starving the TEST THREAD does not
# produce a timeout in this file at all. The loop is `pump; pred; deadline; sleep`, `pump` drains an
# mpsc queue fed by reader threads, and the predicate is evaluated over the ACCUMULATED transcript.
# A descheduled test thread therefore resumes, drains everything the nodes said while it was away,
# and finds the predicate already satisfied — it PASSES, it does not expire. The budget can only
# expire if the NODES failed to progress. So signal B is a cheap backstop for this target and signal
# C — added only to cover B's blind spot — is carrying the row. That is the opposite weighting to
# the one D42's decision implies, and it is recorded rather than left as an impression.
#
# WHAT THIS SCRIPT DOES, AND WHY IT IS AIMED WHERE IT IS. It lets the cluster start normally, then
# SIGSTOPs the consensus_node CHILD PROCESSES for longer than one ELECTION_BUDGET while leaving the
# test thread fully scheduled. From every observable the classifier has, a stopped child and a child
# that never got the machine are the same thing — which is the condition under test. The stop begins
# only AFTER every node has said READY, because stopping one earlier would trip the unrelated
# `never said READY` panic and measure nothing.
#
# The expected reading is pre-registered arithmetic, not a fitted number: a running node consumes
# 0.0043 cpu-s per wall-s (bench/d42_childcpu_probe.txt), so the 0.00043 threshold is exactly "a
# node that got under 10 % of its normal running time". A stopped node reads ~0.
#
# REQUIRED OUTCOME: INCONCLUSIVE, with self-schedule HEALTHY and child CPU STARVED. Self-schedule
# reading HEALTHY is not incidental — it is the evidence that C fired ALONE, covering the blind spot
# B is known to have. If C cannot fire here it cannot fire anywhere, and the row is dead.
#
# ⛔ SAFETY. Every pid signalled is matched against the ABSOLUTE path of THIS worktree's own example
# binary, so it cannot touch another agent's processes on a shared machine. It refuses to start if
# any such process is already alive. The trap SIGCONTs and then SIGKILLs anything still matching: a
# process left stopped is indistinguishable from a wedged one, and this box runs an agent fleet.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1
TREE=$(pwd -P)
BIN="$TREE/target/debug/examples/consensus_node"
PAT="^$BIN "

HOLD=${HOLD:-60}          # seconds to keep the nodes stopped; must exceed ELECTION_BUDGET (45s)
TEST_BOUND=${TEST_BOUND:-900}
LOCK=/tmp/ferrodb-suite.lock
STOPLOG=$(mktemp)

held=0; keeper=""
kids() { pgrep -f "$PAT" 2>/dev/null; }
cleanup() {
    [ -n "$keeper" ] && kill -9 "$keeper" 2>/dev/null
    for p in $(kids); do kill -CONT "$p" 2>/dev/null; kill -9 "$p" 2>/dev/null; done
    [ "$held" = 1 ] && rm -rf "$LOCK"
    rm -f "$STOPLOG"
    return 0
}
trap cleanup EXIT INT TERM

cargo build --examples >/dev/null 2>&1 || { echo "REFUSING — cargo build --examples failed"; exit 1; }
cargo test --no-run --test integration_consensus_failover >/dev/null 2>&1 \
    || { echo "REFUSING — the test target does not build"; exit 1; }
[ -x "$BIN" ] || { echo "REFUSING — $BIN is missing, so nothing could be matched or stopped"; exit 1; }
if [ -n "$(kids)" ]; then
    echo "REFUSING — consensus_node processes from this worktree are already running: $(kids | tr '\n' ' ')"
    echo "  Stopping them would corrupt whatever is using them."
    exit 1
fi

w=0
while ! mkdir "$LOCK" 2>/dev/null; do
    o=$(cat "$LOCK/owner" 2>/dev/null || echo unknown); p=${o%% *}
    if [ -n "$p" ] && ! kill -0 "$p" 2>/dev/null; then rm -rf "$LOCK"; continue; fi
    [ "$w" -ge 3600 ] && { echo "REFUSING — waited ${w}s for the suite lock held by: $o"; exit 3; }
    [ "$w" -eq 0 ] && echo "queued behind a running suite ($o)" >&2
    sleep 15; w=$((w+15))
done
printf '%s %s %s\n' "$$" "d42-fire-children" "$(date -u +%FT%TZ)" > "$LOCK/owner"
held=1

echo "D42 falsifier 1 (decisive arm) — starve the NODES, leave the test thread scheduled"
echo "  when          : $(date -u +%FT%TZ)"
echo "  head          : $(git log -1 --format=%h), $(git status --short | wc -l | tr -d ' ') dirty file(s)"
echo "  tree          : $TREE"
echo "  method        : SIGSTOP every node for ${HOLD}s once all three have said READY"
echo "  budget        : ELECTION_BUDGET is 45s, so the hold outlasts one full budget"
echo "  predicted     : child CPU ~0 per node-wall-s — a stopped process consumes none. The"
echo "                  threshold 0.00043 is itself 10% of the measured running rate 0.0043."
echo "  loadavg       : $(uptime | sed 's/.*load averages*: //')"

# The keeper. Two conditions have to be met before it may stop anything, and the second one is a
# CONDITION rather than a timer for a reason that was measured: the first version waited a fixed
# 2.5 s for READY, and on a quiet machine the entire test finished in 3.15 s inside that wait. The
# keeper stopped nothing, and the run would have read as "the classifier did not fire" when in fact
# nothing had been starved. (The anti-vacuity check below caught it and refused — which is the only
# reason it is a corrected script and not a false negative in the record.)
#
# So: wait for all three nodes, then wait until all three are LISTENING on TCP. The example binds
# its listener and then prints the address it bound as READY, so a listening socket means READY has
# been published. Stopping a node before that trips the unrelated `never said READY` panic and
# measures nothing at all.
(
    n=0
    while [ "$n" -lt 3 ]; do n=$(pgrep -f "$PAT" 2>/dev/null | wc -l | tr -d ' '); sleep 0.05; done
    pids=$(pgrep -f "$PAT" 2>/dev/null | tr '\n' ',' | sed 's/,$//')
    echo "$(date -u +%T) saw 3 node(s): $pids; waiting for all three to LISTEN" >> "$STOPLOG"
    i=0; lis=0
    while [ "$lis" -lt 3 ] && [ "$i" -lt 600 ]; do
        lis=$(lsof -nP -iTCP -sTCP:LISTEN -a -p "$pids" 2>/dev/null | grep -c LISTEN)
        [ "$lis" -lt 3 ] && sleep 0.05
        i=$((i+1))
    done
    echo "$(date -u +%T) $lis of 3 listening after $((i*50))ms — READY has been published" >> "$STOPLOG"
    got=$(pgrep -f "$PAT" 2>/dev/null | tr '\n' ' ')
    c=0; for p in $got; do kill -STOP "$p" 2>/dev/null && c=$((c+1)); done
    echo "$(date -u +%T) STOPPED $c pid(s): $got" >> "$STOPLOG"
    # Re-stop on a short timer, so nothing drifts back to running during the hold.
    e=0
    while [ "$e" -lt $((HOLD * 2)) ]; do
        for p in $(pgrep -f "$PAT" 2>/dev/null); do kill -STOP "$p" 2>/dev/null; done
        sleep 0.5; e=$((e+1))
    done
    c=0; for p in $(pgrep -f "$PAT" 2>/dev/null); do kill -CONT "$p" 2>/dev/null && c=$((c+1)); done
    echo "$(date -u +%T) CONTINUED $c pid(s) after ${HOLD}s" >> "$STOPLOG"
) &
keeper=$!
disown $keeper 2>/dev/null   # or bash prints the keeper's whole body when it reaps it

echo ""
echo "---------------- cargo test --test integration_consensus_failover ----------------"
OUT=$(timeout "$TEST_BOUND" cargo test --test integration_consensus_failover 2>&1)
rc=$?
kill -9 "$keeper" 2>/dev/null; keeper=""
for p in $(kids); do kill -CONT "$p" 2>/dev/null; done
echo "$OUT"
echo "---------------- exit $rc ----------------"
echo ""
echo "keeper log (anti-vacuity: this run means nothing if nothing was actually stopped):"
sed 's/^/   /' "$STOPLOG"
echo ""

fails=0
if ! grep -q 'STOPPED [1-9]' "$STOPLOG"; then
    echo "⛔ VACUOUS — the keeper never stopped a single process, so nothing was starved and this"
    echo "   run is not evidence in either direction."
    fails=$((fails+1))
fi
if grep -qF 'FERRODB-VERDICT: INCONCLUSIVE' <<<"$OUT"; then
    echo "ok: the starved-children run earned INCONCLUSIVE — the classifier fired"
elif grep -qF 'FERRODB-VERDICT: CLASSIFIER-BROKEN' <<<"$OUT"; then
    echo "⛔ the classifier refused rather than guessing, but this run does not show that it FIRES."
    fails=$((fails+1))
elif grep -q 'timed out after 45s' <<<"$OUT"; then
    echo "⛔ FALSIFIER 1 FAILED — the wait expired and the classifier still called it FAILED."
    echo "   Children that consumed no CPU at all did not read as starved. Report it; do not retune."
    fails=$((fails+1))
else
    echo "⛔ PRECONDITION NOT REACHED — no wait expired, so there was no verdict to classify."
    fails=$((fails+1))
fi
if grep -q 'self-schedule.*HEALTHY' <<<"$OUT"; then
    echo "ok: self-scheduling read HEALTHY — signal C fired ALONE, covering B's known blind spot"
elif grep -q 'self-schedule' <<<"$OUT"; then
    echo "note: self-scheduling also read STARVED, so this run does not isolate C from B"
fi
grep -E 'verdict       :|self-schedule|child CPU|max stall|window        :' <<<"$OUT" | sed 's/^/   /'

echo ""
if [ "$fails" -eq 0 ]; then echo "FALSIFIER 1 PASSED"; exit 0; fi
echo "FALSIFIER 1 FAILED — $fails check(s) did not hold."
exit 1
