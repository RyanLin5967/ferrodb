#!/usr/bin/env bash
# bench/d42_fire_starved.sh — D42 falsifier 1 of 2: the classifier MUST fire.
#
# A detector that has never been made to fire is not a clean result. This one drives
# `integration_consensus_failover.rs` under deliberate starvation and requires the verdict
# INCONCLUSIVE, with both signals' measured values recorded in the artifact.
#
# ⚠ THE PRE-REGISTERED SHAPE DID NOT WORK, AND THE AMENDMENT IS RECORDED RATHER THAN QUIETLY MADE.
# D42 pre-registered "14x oversubscription". Measured at head ddbc601 (bench/d42_fire_starved.txt
# run 1): 252 pure-CPU spinners on 18 cores reached loadavg 137 and the test PASSED in 4.76 s. The
# load sweep (bench/d42_load_sweep.txt) then measured both signals across six shapes and found them
# nearly FLAT from loadavg 269 to 829 — self-scheduling only fell 0.78 -> 0.68. macOS's timeshare
# scheduler demotes pure CPU spinners and keeps a 10 ms sleeper responsive, which is what it is for.
# A process asking for 0.4 % of a core gets it no matter how many spinners are queued.
#
# So the precondition the falsifier needs — a 45 s budget actually EXPIRING — was never reached, and
# 14x is not a lever on this machine. The sweep found the shape that is: `nice -n 20` against 28x
# oversubscription, under which the cluster could not elect a leader within 45 s at all.
#
# ⛔ THAT IS AN AMENDMENT TO THE FALSIFIER'S LOAD SHAPE, NOT TO ITS EXIT CRITERION. The requirement
# is unchanged and unweakened: a starved run must earn INCONCLUSIVE, and bench/d42_fire_broken.sh
# must independently show that a genuinely broken cluster still earns FAILED. `nice` is not a cheat:
# the classifier's claim is exactly "this process, or its children, did not get the CPU", and `nice`
# aims the break at that window instead of hoping ambient load wanders into it. The test binary and
# the consensus_node children both inherit the niceness, so BOTH signals are exposed.
#
# It holds the machine-wide suite lock: oversubscribing this box while another agent's suite runs
# would wreck their numbers, which is the very confusion this row exists to end.
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

NCPU=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
FACTOR=${FACTOR:-28}
NICE=${NICE:-20}
N=$((NCPU * FACTOR))
SPIN_BOUND=${SPIN_BOUND:-900}
TEST_BOUND=${TEST_BOUND:-900}
LOCK=/tmp/ferrodb-suite.lock

spinners=()
cleanup() {
    for p in "${spinners[@]:-}"; do kill -9 "$p" 2>/dev/null; done
    [ "${held:-0}" = 1 ] && rm -rf "$LOCK"
    return 0
}
trap cleanup EXIT INT TERM

# Build BEFORE the load goes on, or the measurement is of rustc, not of the test.
cargo build --examples >/dev/null 2>&1 || { echo "REFUSING — cargo build --examples failed"; exit 1; }
cargo test --no-run --test integration_consensus_failover >/dev/null 2>&1 \
    || { echo "REFUSING — the test target does not build"; exit 1; }

held=0; w=0
while ! mkdir "$LOCK" 2>/dev/null; do
    o=$(cat "$LOCK/owner" 2>/dev/null || echo unknown); p=${o%% *}
    if [ -n "$p" ] && ! kill -0 "$p" 2>/dev/null; then rm -rf "$LOCK"; continue; fi
    [ "$w" -ge 3600 ] && { echo "REFUSING — waited ${w}s for the suite lock held by: $o"; exit 3; }
    [ "$w" -eq 0 ] && echo "queued behind a running suite ($o)" >&2
    sleep 15; w=$((w+15))
done
printf '%s %s %s\n' "$$" "d42-fire-starved" "$(date -u +%FT%TZ)" > "$LOCK/owner"
held=1

echo "D42 falsifier 1 — the classifier must fire when the run is starved"
echo "  when            : $(date -u +%FT%TZ)"
echo "  head            : $(git log -1 --format=%h), $(git status --short | wc -l | tr -d ' ') dirty file(s)"
echo "  cpus            : $NCPU"
echo "  shape           : nice -n $NICE, ${FACTOR}x oversubscription = $N pure-CPU spinners"
echo "  loadavg before  : $(uptime | sed 's/.*load averages*: //')"

for _ in $(seq "$N"); do
    timeout "$SPIN_BOUND" sh -c 'while :; do :; done' >/dev/null 2>&1 &
    spinners+=($!); disown $! 2>/dev/null
done
sleep 20
echo "  loadavg loaded  : $(uptime | sed 's/.*load averages*: //')"
echo ""
echo "---------------- nice -n $NICE cargo test --test integration_consensus_failover ----------------"
OUT=$(timeout "$TEST_BOUND" nice -n "$NICE" cargo test --test integration_consensus_failover 2>&1)
rc=$?
echo "$OUT"
echo "---------------- exit $rc ----------------"
echo "  loadavg after   : $(uptime | sed 's/.*load averages*: //')"
echo ""

# ── the verdict on the verdict ──────────────────────────────────────────────────────────────────
fails=0
if grep -qF 'FERRODB-VERDICT: INCONCLUSIVE' <<<"$OUT"; then
    echo "ok: the starved run earned INCONCLUSIVE — the classifier fired"
elif grep -qF 'FERRODB-VERDICT: CLASSIFIER-BROKEN' <<<"$OUT"; then
    echo "⛔ the classifier could not measure; it refused rather than guessing, but this run does"
    echo "   not demonstrate that it FIRES. Fix the instrument and re-run."
    fails=$((fails+1))
elif grep -q 'timed out after 45s' <<<"$OUT"; then
    echo "⛔ FALSIFIER 1 FAILED — the wait expired and the classifier still called it FAILED."
    echo "   The signals did not separate starvation from failure at these thresholds."
    fails=$((fails+1))
else
    echo "⛔ FALSIFIER 1 DID NOT REACH ITS PRECONDITION — no wait expired, so there was no verdict"
    echo "   to classify. Raise FACTOR or NICE; this run proves nothing in either direction."
    fails=$((fails+1))
fi
grep -E 'verdict       :|self-schedule|child CPU|max stall|window        :' <<<"$OUT" | sed 's/^/   /'

echo ""
if [ "$fails" -eq 0 ]; then echo "FALSIFIER 1 PASSED"; exit 0; fi
echo "FALSIFIER 1 FAILED — $fails check(s) did not hold."
exit 1
