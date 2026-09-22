#!/bin/bash
# E7 — is the reaper a material announcer? The axis is EXPIRED BRANCHES PER TICK, not the period.
#
# readonly/extended is the one configuration that reads EXACTLY 0.000000 with ZERO announcements of
# any kind: a cached prepared statement never touches the exclusive catalog and there are no
# writers. Adding expiries makes the REAPER THE ONLY ANNOUNCER IN THE SYSTEM, which is the cleanest
# attribution available anywhere in this experiment.
export PATH="$HOME/.cargo/bin:$PATH"
SCR=/private/tmp/claude-501/-Users-idide-projects-ferrodb/b2b44149-483d-42d1-b512-89bf5de5a135/scratchpad/w4c3
WT=/Users/idide/wt/ferrodb-agent-a3242b062ca90442e
OUT=$WT/bench/w4_standdown/rederive
HOLDER=$(cat "$SCR/HOLDER_PID2")
BIN=./target/release/examples/w4_standdown_count

rm -f "$SCR/E7_DONE"
while true; do
    owner=$(cat /tmp/ferrodb-suite.lock/owner 2>/dev/null); pid=${owner%% *}
    [ "$pid" = "$HOLDER" ] && break
    if ! kill -0 "$HOLDER" 2>/dev/null; then
        echo "REFUSING: holder $HOLDER died before acquiring"; echo holder-died > "$SCR/E7_DONE"; exit 3
    fi
    sleep 15
done
echo "lock is ours ($HOLDER) at $(date -u +%FT%TZ)"
cd "$WT" || exit 1

echo "== BUILD =="
if ! timeout 3600 cargo build --release --features w4-standdown-count \
        --example w4_standdown_count > "$SCR/build2.log" 2>&1; then
    echo "BUILD FAILED"; tail -40 "$SCR/build2.log"; echo build-failed > "$SCR/E7_DONE"; exit 1
fi
echo "build ok"

# CONTROL FIRST, and it must still read exactly zero: if the minter changed the arm even with the
# knob off, every later row is uninterpretable.
for ms in 0 200 50 10 2; do
    echo "== e7_expiry_$ms =="
    W4_CLIENTS=8 W4_ROUNDS=200 W4_ARMS=readonly W4_PROTOS=extended \
        W4_LEASE_MS=10 W4_EXPIRY_MS="$ms" \
        timeout 900 "$BIN" > "$OUT/e7_expiry_$ms.txt" 2> "$OUT/e7_expiry_$ms.err" \
        || { echo "  REFUSED:"; head -3 "$OUT/e7_expiry_$ms.err"; }
done
echo complete > "$SCR/E7_DONE"
echo "E7 done $(date -u +%FT%TZ)"
