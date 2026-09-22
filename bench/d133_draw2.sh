#!/usr/bin/env bash
# D133 draw 2 — wait for the suite lock, run the hardened sweep, retry if a suite lands on us.
#
# Two suites started within 30 minutes on this box, and draw 1 was lost to the second of them.
# So this does not poll-then-decide-later: it waits for the lock to clear and starts IMMEDIATELY,
# which is the shortest window another `verify-suite.sh` can slip into, and if one does the
# hardened runner exits 3 and this tries again. The exit criterion is a run that returned 0, not
# a number of attempts.
set -u

WT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$WT/bench/d133_pending_len_draw2.txt"
ATTEMPTS=${ATTEMPTS:-4}
WAIT_S=${WAIT_S:-3000}

for attempt in $(seq 1 "$ATTEMPTS"); do
    # Wait for the suite lock to clear. Polled at 5 s: verify-suite.sh hands over on a 15 s
    # cadence, and a 30 s poll loses every handover.
    deadline=$((SECONDS + WAIT_S))
    while [ -d /tmp/ferrodb-suite.lock ] && [ "$SECONDS" -lt "$deadline" ]; do sleep 5; done
    if [ -d /tmp/ferrodb-suite.lock ]; then
        echo "ATTEMPT $attempt: gave up waiting; suite lock still held by \
$(cat /tmp/ferrodb-suite.lock/owner 2>&1)" >&2
        continue
    fi

    echo "ATTEMPT $attempt: suite lock free at $(date -u +%H:%M:%SZ), starting." >&2
    bash "$WT/bench/d133_run.sh" > "$OUT.attempt$attempt" 2>&1
    rc=$?
    echo "ATTEMPT $attempt: runner rc=$rc" >&2
    if [ "$rc" -eq 0 ]; then
        mv "$OUT.attempt$attempt" "$OUT"
        echo "CLEAN DRAW at attempt $attempt -> $OUT" >&2
        # The completion sentinel: its presence is what says the file is finished and whole.
        echo "clean rc=0 attempt=$attempt $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$OUT.done"
        exit 0
    fi
    echo "ATTEMPT $attempt: NOT clean (rc=$rc), kept as $OUT.attempt$attempt" >&2
done

echo "REFUSED: no clean draw in $ATTEMPTS attempts. Do not quote a ms column." >&2
exit 2
