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
_LOCK_PID=
# LOCK-RELEASE-OWNERSHIP-CHECKED (D188). This used to be
#     _rel() { [ "$_HELD" = "1" ] && rm -rf "$LOCK"; _HELD=0; }
# which tests a flag meaning "I acquired A lock once" and never asks whether the directory on disk
# is still THE lock it acquired. That shape fired live on 2026-09-23 and deleted a RUNNING suite's
# lock, which then ran unprotected for 110 targets. Remove only on a pid match; every other reading
# is a refusal that warns. Failing closed strands nothing -- the acquire loop below breaks a lock
# whose recorded pid is dead. Full reasoning: note 8 in tools/verify-suite.sh.
_rel() {
    [ "$_HELD" = "1" ] || return 0
    _HELD=0
    if [ ! -d "$LOCK" ]; then
        echo "$LABEL: WARNING -- held $LOCK as pid $_LOCK_PID and it is ALREADY GONE at release;" >&2
        echo "  another run may have shared the machine with this hold." >&2
        return 0
    fi
    _o=$(cat "$LOCK/owner" 2>/dev/null) || _o=
    _p=${_o%% *}
    case "$_p" in
        '' | *[!0-9]*)
            echo "$LABEL: REFUSING to release $LOCK -- owner missing or unparseable (read: '$_o');" >&2
            echo "  this run held it as pid $_LOCK_PID. Leaving the directory alone." >&2
            return 0 ;;
    esac
    if [ "$_p" != "$_LOCK_PID" ]; then
        echo "$LABEL: REFUSING to release $LOCK -- now owned by pid $_p, not this run (pid" >&2
        echo "  $_LOCK_PID). Owner line: '$_o'. This run's lock was removed and re-created by" >&2
        echo "  another run, which has been running UNPROTECTED alongside it. Leaving it intact." >&2
        return 0
    fi
    rm -rf "$LOCK"
}
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
_LOCK_PID=$$        # what _rel compares against; a flag cannot identify a directory
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
