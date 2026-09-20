#!/usr/bin/env bash
# bench/d42_fire_orphans.sh — prove the load burners cannot outlive their parent.
#
# WHY THIS EXISTS. An earlier version of bench/d42_fire_starved.sh left 317 orphaned processes at
# ppid=1, still running 1.5 hours after their parent died, at loadavg 282. It froze this shared box
# TWICE and starved every other project on it while presenting as "the machine is slow".
#
# A cleanup path nobody has made fire is not a cleanup path, so this reproduces the EXACT incident
# state and requires the burners to die anyway.
#
# THE INCIDENT STATE, and why it must be built this way. Three defects compounded:
#   1. the burner body was `while :` — unbounded, so an abandoned one never stops;
#   2. cleanup killed `$!`, which is the **`timeout` WRAPPER's** pid. Killing `timeout` does NOT
#      kill the `sh` it spawned; the grandchild survives, reparents to init, and now has nothing
#      enforcing its bound;
#   3. `disown` detached them, defeating SIGHUP.
#
# So this script SIGKILLs the parent (defeating any trap — the trap is not what is under test) AND
# SIGKILLs the `timeout` wrappers (defeating the bound), leaving bare burners at ppid=1. The only
# thing that can save the machine in that state is the burner being SELF-TERMINATING, which is the
# property under test.
#
# TWO DIRECTIONS, because a check that only confirms "they are gone" would pass if they had never
# started:
#   A. ANTI-VACUITY — immediately after the kills, orphaned burners at ppid=1 MUST exist.
#   B. THE FIX      — after the burner's own deadline passes, they MUST all be gone.
#
# It uses a short bound and a handful of burners, so it needs no suite lock and perturbs nothing.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

N=${N:-8}
BOUND=${BOUND:-20}
TAG="d42-orphan-firecheck-$$"
PARENT=""

cleanup() {
    for p in $(pgrep -f "$TAG" 2>/dev/null); do kill -9 "$p" 2>/dev/null; done
    [ -n "$PARENT" ] && kill -9 "$PARENT" 2>/dev/null
    return 0
}
trap cleanup EXIT INT TERM

orphans() {   # burners whose parent is init
    ps -axo pid=,ppid=,command= 2>/dev/null | awk -v t="$TAG" '$2==1 && index($0,t)>0 {print $1}'
}
alive() {     # all burners, whatever their parent
    pgrep -f "$TAG" 2>/dev/null
}

echo "D42 orphan fire-check — a burner must not be able to outlive its parent"
echo "  when   : $(date -u +%FT%TZ)"
echo "  head   : $(git log -1 --format=%h)"
echo "  shape  : $N burners, self-terminating after ${BOUND}s, tagged $TAG"
echo ""

# The burner: a wall-clock deadline computed inside the burner itself, checked between bursts of
# pure arithmetic.
#
# ⚠ THE INNER BURST IS 2000, NOT 200000, AND THAT IS THE WHOLE CORRECTNESS ARGUMENT. A
# "self-terminating" burner is only bounded to within ONE INNER PASS, because the inner loop is not
# interruptible and the deadline is only tested between passes. That pass gets slower as the box
# gets more contended — which is the thing a load harness is itself creating — so the overshoot is
# worst exactly when the bound matters. Measured by another session on a 200000-iteration burst:
# burners at **05:15 elapsed against a 240 s deadline**. At 2000 the check fires ~100x more often,
# so the overshoot stays a rounding error even under heavy load.
#
# ⚠ KNOWN HOLE, stated rather than discovered later: a SIGSTOPped burner never reaches its deadline
# check at all, so suspending these is NOT a safe way to park them. They must be resumed or killed.
cat > "/tmp/$TAG.sh" <<EOS
#!/bin/sh
# $TAG
end=\$(( \$(date +%s) + $BOUND ))
while [ "\$(date +%s)" -lt "\$end" ]; do
    i=0; while [ "\$i" -lt 2000 ]; do i=\$((i+1)); done
done
EOS
chmod +x "/tmp/$TAG.sh"

# A parent that spawns them under `timeout`, exactly as the harnesses do, then waits.
cat > "/tmp/$TAG-parent.sh" <<EOS
#!/bin/sh
# $TAG parent
for _ in \$(seq $N); do
    timeout $((BOUND * 10)) "/tmp/$TAG.sh" >/dev/null 2>&1 &
done
sleep 600
EOS
chmod +x "/tmp/$TAG-parent.sh"

"/tmp/$TAG-parent.sh" >/dev/null 2>&1 &
PARENT=$!
sleep 3
echo "  spawned: $(alive | wc -l | tr -d ' ') process(es) matching the tag"

# Reproduce the incident: SIGKILL the parent (no trap can run) and SIGKILL every `timeout` wrapper
# (no bound is enforced any more). What remains must save itself.
kill -9 "$PARENT" 2>/dev/null; PARENT=""
for p in $(pgrep -f "timeout $((BOUND * 10)) /tmp/$TAG.sh" 2>/dev/null); do kill -9 "$p" 2>/dev/null; done
sleep 2

fails=0
orph=$(orphans | wc -l | tr -d ' ')
echo ""
echo "  A. anti-vacuity — orphans at ppid=1 right after the kills: $orph"
if [ "$orph" -gt 0 ]; then
    echo "     ok: the incident state was really reproduced, so direction B means something"
else
    echo "     ⛔ NO orphans were created, so this run proves nothing: a later 'all gone' would be"
    echo "        indistinguishable from burners that never started."
    fails=$((fails+1))
fi

echo ""
echo "  waiting ${BOUND}s + 8s for the burners' own deadline to pass..."
sleep $((BOUND + 8))

left=$(alive | wc -l | tr -d ' ')
echo ""
echo "  B. the fix — burners still alive after their own deadline: $left"
if [ "$left" -eq 0 ]; then
    echo "     ok: every orphan terminated itself with no parent, no timeout and no trap"
else
    echo "     ⛔ $left burner(s) OUTLIVED their bound with no parent. This is the incident."
    ps -axo pid=,ppid=,etime=,command= | grep "$TAG" | grep -v grep | sed 's/^/        /'
    fails=$((fails+1))
fi

rm -f "/tmp/$TAG.sh" "/tmp/$TAG-parent.sh"
echo ""
if [ "$fails" -eq 0 ]; then echo "ORPHAN FIRE-CHECK PASSED"; exit 0; fi
echo "ORPHAN FIRE-CHECK FAILED — $fails check(s) did not hold."; exit 1
