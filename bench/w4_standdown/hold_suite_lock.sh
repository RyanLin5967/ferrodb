#!/bin/bash
# Acquire the machine-wide ferrodb suite lock the same way tools/verify-suite.sh does, then HOLD
# it until a release sentinel appears (or a hard cap elapses, so a dead agent cannot wedge the
# fleet). `$$` lives for the whole hold, so another agent's `kill -0` against the owner file is
# meaningful. ACQUIRE, never poll-then-act: a `[ ! -e lock ]` test followed by a build is a race.
LOCK=/tmp/ferrodb-suite.lock
LABEL=w4-check3-rederive
SCR=/private/tmp/claude-501/-Users-idide-projects-ferrodb/b2b44149-483d-42d1-b512-89bf5de5a135/scratchpad/w4c3
REL="$SCR/RELEASE_LOCK"
MAXHOLD=${MAXHOLD:-5400}
LOCK_WAIT=${LOCK_WAIT:-10800}

_HELD=0
_rel() { [ "$_HELD" = "1" ] && rm -rf "$LOCK"; _HELD=0; }
trap '_rel' EXIT TERM INT

rm -f "$REL" "$SCR/LOCK_ACQUIRED"
waited=0
while ! mkdir "$LOCK" 2>/dev/null; do
    owner=$(cat "$LOCK/owner" 2>/dev/null || echo "unknown")
    pid=${owner%% *}
    if [ -n "$pid" ] && [ "$pid" != "unknown" ] && ! kill -0 "$pid" 2>/dev/null; then
        echo "$LABEL: lock held by dead pid $pid -- breaking it"
        rm -rf "$LOCK"; continue
    fi
    if [ "$waited" -ge "$LOCK_WAIT" ]; then
        echo "$LABEL: REFUSING -- waited ${LOCK_WAIT}s for the suite lock held by: $owner"
        exit 3
    fi
    [ "$waited" -eq 0 ] && echo "$LABEL: queued behind $owner"
    sleep 15; waited=$((waited+15))
done
printf '%s %s %s\n' "$$" "$LABEL" "$(date -u +%FT%TZ)" > "$LOCK/owner"
_HELD=1
echo "$LABEL: ACQUIRED after ${waited}s at $(date -u +%FT%TZ), holder pid $$"
printf '%s\n' "$$" > "$SCR/LOCK_ACQUIRED"

held=0
while [ ! -e "$REL" ]; do
    sleep 5; held=$((held+5))
    if [ "$held" -ge "$MAXHOLD" ]; then
        echo "$LABEL: hard cap ${MAXHOLD}s reached -- releasing"
        exit 0
    fi
done
echo "$LABEL: released on sentinel after ${held}s"
