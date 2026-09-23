#!/usr/bin/env bash
# tools/verify-suite-selftest.sh — fire-check verify-suite.sh's refusals.
#
# Part 1 and part 2 cover the SIGTERM abort (note 7 in verify-suite.sh); part 3 covers the D42
# INCONCLUSIVE channel (note 6) and the landing guard that reads it; part 4 covers the release-time
# ownership check (note 8), and part 5 checks that no script has quietly grown a release without
# it. Parts 3, 4 and 5 are described where they begin; the SIGTERM halves are described here
# because they came first.
#
# Note 7 in verify-suite.sh claims a signal aborts the run, takes its children with it, and refuses
# to print a number. That claim is worth exactly what a run of this file says: a guard nobody has
# made fire is not evidence, and this one replaced a guard that had been failing open for weeks
# without anything noticing. Two halves, and BOTH must hold:
#
#   part 1  (anti-vacuity)  a minimal fixture carrying the OLD `trap 'rm -rf $L' EXIT INT TERM`
#                           shape must EXHIBIT the bug: the handler runs, deletes the lock, and the
#                           script then RESUMES and reaches its tail. If this half does not
#                           reproduce, the test cannot tell the two shapes apart and part 2's pass
#                           means nothing.
#   part 2  (the guard)     the real tools/verify-suite.sh, TERMed while a child is in flight, must
#                           exit 143, say REFUSING, release its lock, and leave no descendant alive.
#
# It never touches the real /tmp/ferrodb-suite.lock: SUITE_LOCK is redirected into a temp dir, so
# this is safe to run while nothing else is, and it will not steal a live suite's lock.
#
# Usage: tools/verify-suite-selftest.sh     exit 0 = every part behaved as documented.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export PATH="$HOME/.cargo/bin:$PATH"

TD=$(mktemp -d); trap 'rm -rf "$TD"' EXIT
fails=0
note() { printf '%s\n' "$*"; }
bad()  { printf 'FAIL: %s\n' "$*"; fails=$((fails+1)); }
ok()   { printf 'ok:   %s\n' "$*"; }

descendants() {                       # pid -> every descendant pid, deepest first
    local p=$1 c
    for c in $(pgrep -P "$p" 2>/dev/null); do descendants "$c"; echo "$c"; done
}
alive() { kill -0 "$1" 2>/dev/null; }

# ── part 1: the shape that was there before must fail, or this test proves nothing ──────────────
note "== part 1: the old trap shape (anti-vacuity) =="
L1=$TD/old.lock; mkdir -p "$L1"
cat > "$TD/old.sh" <<'EOS'
#!/usr/bin/env bash
trap 'rm -rf "$1"' EXIT INT TERM
sleep 3
echo "REACHED THE TAIL AFTER THE SIGNAL"
sleep 60
EOS
bash "$TD/old.sh" "$L1" > "$TD/old.out" 2>&1 &
oldpid=$!
sleep 1; kill -TERM "$oldpid" 2>/dev/null; sleep 5

grep -q 'REACHED THE TAIL AFTER THE SIGNAL' "$TD/old.out" \
    && ok "old shape resumed past the signal (bug reproduced)" \
    || bad "old shape did NOT resume past the signal — part 2 cannot distinguish the fix"
[ -d "$L1" ] \
    && bad "old shape did NOT release the lock — the fail-open half did not reproduce" \
    || ok "old shape deleted the lock and carried on (bug reproduced)"
alive "$oldpid" && ok "old shape still running after SIGTERM (bug reproduced)" \
                || note "note: old fixture had already exited; the two checks above are the evidence"
for p in $(descendants "$oldpid") "$oldpid"; do kill -9 "$p" 2>/dev/null; done
wait "$oldpid" 2>/dev/null

# ── part 2: the real verifier ───────────────────────────────────────────────────────────────────
note ""
note "== part 2: tools/verify-suite.sh under SIGTERM =="
L2=$TD/new.lock
SUITE_LOCK=$L2 VERIFY_OUT=$TD/out VERIFY_MODE=whole VERIFY_TIMEOUT=1800 LOCK_WAIT=60 \
    bash tools/verify-suite.sh selftest-term > "$TD/new.out" 2> "$TD/new.err" &
newpid=$!

i=0; while [ ! -f "$L2/owner" ] && [ $i -lt 90 ]; do sleep 1; i=$((i+1)); done
if [ ! -f "$L2/owner" ]; then
    bad "the verifier never took its lock within 90s — cannot run part 2"
    for p in $(descendants "$newpid") "$newpid"; do kill -9 "$p" 2>/dev/null; done
    exit 1
fi
ok "verifier holds its lock ($(cat "$L2/owner"))"

kids=""; i=0
while [ -z "$kids" ] && [ $i -lt 120 ]; do kids=$(descendants "$newpid" | tr '\n' ' '); [ -z "$kids" ] && sleep 1; i=$((i+1)); done
[ -n "$kids" ] && ok "children in flight before the signal: $kids" \
               || bad "no child ever appeared; the signal would not land mid-child"

# A watchdog, because if the fix is broken `wait` below blocks for the whole suite.
( sleep 90
  if kill -0 "$newpid" 2>/dev/null; then
      echo WEDGED > "$TD/watchdog"
      for p in $(pgrep -P "$newpid" 2>/dev/null); do kill -9 "$p" 2>/dev/null; done
      kill -9 "$newpid" 2>/dev/null
  fi ) & wd=$!

kill -TERM "$newpid"
t0=$(date +%s); wait "$newpid"; st=$?; t1=$(date +%s)
kill -9 "$wd" 2>/dev/null; wait "$wd" 2>/dev/null
note "exit status $st, $((t1-t0))s after the signal"

[ -f "$TD/watchdog" ] && bad "still alive 90s after SIGTERM — the abort did not abort"
[ "$st" = "143" ] && ok "exited 143 (aborted-by-signal refusal)" || bad "exit status was $st, expected 143"
grep -q 'REFUSING' "$TD/new.err" && ok "printed a refusal: $(grep -m1 REFUSING "$TD/new.err")" \
                                 || bad "no REFUSING line on stderr"
grep -qE 'passed=[0-9]+' "$TD/new.out" && bad "printed a total anyway — a suite cut short is not a count" \
                                       || ok "printed no total"
[ -d "$L2" ] && bad "the suite lock survived the abort" || ok "suite lock released"
surv=""
for p in $kids; do alive "$p" && surv="$surv $p"; done
[ -n "$surv" ] && bad "orphaned descendants survived:$surv ($(ps -o command= -p ${surv%% *} 2>/dev/null | cut -c1-60))" \
               || ok "no descendant survived the abort"
for p in $surv; do kill -9 "$p" 2>/dev/null; done

# ── part 3: the D42 INCONCLUSIVE channel ────────────────────────────────────────────────────────
#
# Note 6 in verify-suite.sh routes a starved test's own verdict into the refusal channel, and
# certify-head.sh refuses to land such a run. Two ways that rots, and both are checked here:
#
#   3a  DRIFT. The marker strings live in a Rust source file AND, by necessity, as literals in a
#       shell script that cannot read a Rust constant. If either side is renamed alone, the grep
#       silently matches nothing for ever and every starved run goes back to being reported as a
#       red — a guard that has quietly stopped existing while still appearing in the file.
#   3b  THE LANDING GUARD, in BOTH directions. A refusal that fires on everything is worse than
#       none, so certify-head.sh must refuse an INCONCLUSIVE directory AND still certify an honest
#       green one. Checking only the first half would pass for a guard that refuses unconditionally.
note ""
note "== part 3: the D42 INCONCLUSIVE channel =="
RS=tests/integration_consensus_failover.rs
for lit in 'FERRODB-VERDICT: INCONCLUSIVE' 'FERRODB-VERDICT: CLASSIFIER-BROKEN'; do
    in_sh=$(grep -cF "$lit" tools/verify-suite.sh)
    in_rs=$(grep -cF "$lit" "$RS")
    if [ "$in_sh" -gt 0 ] && [ "$in_rs" -gt 0 ]; then
        ok "marker '$lit' present in both verify-suite.sh and $RS"
    else
        bad "marker '$lit' is in verify-suite.sh $in_sh time(s) and $RS $in_rs time(s) — the two \
copies have drifted, so the grep matches a string nothing emits"
    fi
done

CH=$TD/certify
mkdir -p "$CH"
HEAD_SHORT=$(git log -1 --format=%h 2>/dev/null)
printf 'selftest: mode=whole rc=0 passed=1 failed=0 build_errors=0 head=%s log=/dev/null\nselftest: go rc=0 passed=1 failed=0 log=/dev/null\n' \
    "$HEAD_SHORT" > "$CH/SUMMARY.txt"
if bash tools/certify-head.sh "$CH" HEAD >/dev/null 2>&1; then
    ok "certify-head accepts an honest green directory (anti-vacuity for the check below)"
else
    bad "certify-head REFUSED a green directory naming HEAD — it refuses unconditionally, so the \
next check proves nothing"
fi
printf 'selftest: INCONCLUSIVE — FERRODB-VERDICT: INCONCLUSIVE (fixture)\n' > "$CH/INCONCLUSIVE.txt"
if bash tools/certify-head.sh "$CH" HEAD >/dev/null 2>&1; then
    bad "certify-head CERTIFIED a directory holding INCONCLUSIVE.txt — a starved run can be landed"
else
    ok "certify-head refuses a directory holding INCONCLUSIVE.txt"
fi

# ── part 4: the release must prove the lock is still THIS run's (note 8) ────────────────────────
#
# Note 8 in verify-suite.sh claims the release removes the lock only when the directory on disk is
# still the one this run created, and otherwise refuses and warns. That defect fired LIVE on
# 2026-09-23 — a run whose lock had been removed and re-created by another run deleted the OTHER
# run's lock on exit, and that run then went 110 targets unprotected with two lanes queued. A guard
# nobody has made fire is not evidence, and this guard must be shown to do BOTH things:
#
#   4a  (anti-vacuity)  a fixture carrying the OLD flag-only release must EXHIBIT the bug — hold,
#                       have its lock replaced underneath it, exit, and delete the REPLACEMENT.
#                       Without this half, 4b-4g cannot tell the two shapes apart.
#   4b  swapped owner       -> REFUSE, warn, leave the other run's lock intact
#   4c  nothing touched     -> remove its own lock. A guard that never allows is not a guard, and
#                              this is the half that would pass for an unconditional refusal.
#   4d  owner file deleted  -> REFUSE, leave the directory
#   4e  owner file garbage  -> REFUSE, leave the directory
#   4f  lock directory gone -> WARN that it was already gone (the first half of the live sequence)
#   4g  SIGTERM + swapped owner -> still exits 143 and still refuses to delete the other lock: the
#                              abort of note 7 survives and inherits the ownership check.
#
# HOW THE WINDOW IS MADE, without running a suite. The real tools/verify-suite.sh is launched with
# HOME pointed at a temp tree whose .cargo/bin/cargo is a `sleep`, because the script prepends
# $HOME/.cargo/bin to PATH — so `cargo build --examples` becomes a deterministic hold with the lock
# taken and nothing built. VERIFY_MODE is then an invalid value, so the run refuses and exits 1
# through the ORDINARY EXIT path: the exact path that deleted the other run's lock that day.
# SUITE_LOCK is redirected per arm into $TD, so this never touches /tmp/ferrodb-suite.lock.
note ""
note "== part 4: release-time ownership check =="

FAKEHOME=$TD/fakehome
mkdir -p "$FAKEHOME/.cargo/bin"
cat > "$FAKEHOME/.cargo/bin/cargo" <<'EOS'
#!/bin/sh
# A cargo that succeeds slowly. Not a flag telling the script to pretend — the binary on PATH
# really is this, the same honesty bench/d42_fire_starved_children.sh applies to its broken `ps`.
sleep 10
exit 0
EOS
chmod +x "$FAKEHOME/.cargo/bin/cargo"

HOLDER_PID=
start_holder() {                   # $1 = lock dir, $2 = label — leaves the pid in $HOLDER_PID
    SUITE_LOCK="$1" HOME="$FAKEHOME" VERIFY_OUT="$TD/out.$2" VERIFY_MODE=bogus LOCK_WAIT=30 \
        bash tools/verify-suite.sh "$2" > "$TD/$2.out" 2> "$TD/$2.err" &
    HOLDER_PID=$!
}
await_owner() {                    # $1 = lock dir — wait for the holder to record its ownership
    local i=0
    while [ ! -f "$1/owner" ] && [ $i -lt 40 ]; do sleep 1; i=$((i+1)); done
    [ -f "$1/owner" ]
}
swap_owner() {                     # reproduce the live sequence: removed by a third party, then
    rm -rf "$1"                    # legitimately re-created by another run that records ITS pid
    mkdir "$1"
    printf '%s other-run %s\n' "$OTHER" "$(date -u +%FT%TZ)" > "$1/owner"
}

sleep 600 & OTHER=$!               # a live pid that is definitely not any holder's

# 4a — the old shape must reproduce the bug, or nothing below distinguishes the fix.
cat > "$TD/oldrel.sh" <<'EOS'
#!/usr/bin/env bash
# the pre-D188 release, in shape: a flag meaning "I acquired A lock once", and an unconditional rm
LOCK=$1
_HELD_LOCK=0
_release_lock() {
    [ "${_HELD_LOCK:-0}" = "1" ] || return 0
    _HELD_LOCK=0
    rm -rf "$LOCK"
}
trap '_release_lock' EXIT
mkdir "$LOCK" || exit 9
printf '%s old-shape %s\n' "$$" "$(date -u +%FT%TZ)" > "$LOCK/owner"
_HELD_LOCK=1
touch "$LOCK.acquired"
sleep 5
EOS
LA=$TD/a.lock
bash "$TD/oldrel.sh" "$LA" > "$TD/a.out" 2>&1 &
apid=$!
i=0; while [ ! -f "$LA.acquired" ] && [ $i -lt 20 ]; do sleep 1; i=$((i+1)); done
swap_owner "$LA"
wait "$apid" 2>/dev/null
if [ -d "$LA" ]; then
    bad "4a: the OLD flag-only release left the other run's lock alone — the bug did not reproduce, \
so every check below is vacuous"
else
    ok "4a: the old flag-only release DELETED the other run's lock (bug reproduced)"
fi

# 4b — the real script, lock swapped underneath it, ordinary exit.
LB=$TD/b.lock
start_holder "$LB" arm-b
if await_owner "$LB"; then
    note "4b: holder owns $LB as $(cat "$LB/owner")"
    swap_owner "$LB"
    wait "$HOLDER_PID"; rcb=$?
    [ -d "$LB" ] && ok "4b: the other run's lock SURVIVED the holder's exit" \
                 || bad "4b: the other run's lock was deleted — the ownership check did not fire"
    [ -d "$LB" ] && [ "$(cut -d' ' -f1 "$LB/owner" 2>/dev/null)" = "$OTHER" ] \
        && ok "4b: and it still names the new owner (pid $OTHER), i.e. it was not replaced either" \
        || bad "4b: the surviving directory does not name pid $OTHER"
    grep -q 'REFUSING to release' "$TD/arm-b.err" \
        && ok "4b: warned: $(grep -m1 'REFUSING to release' "$TD/arm-b.err")" \
        || bad "4b: no 'REFUSING to release' on stderr — the theft is invisible, which is the half \
of this that cost an hour on 2026-09-23"
    grep -q "now owned by pid $OTHER" "$TD/arm-b.err" \
        && ok "4b: the warning names the pid it found ($OTHER)" \
        || bad "4b: the warning does not name the pid it found"
    [ "$rcb" = "1" ] && ok "4b: exited 1 via the ordinary refusal path (not a signal)" \
                     || bad "4b: exit status was $rcb, expected 1"
else
    bad "4b: the holder never took its lock within 40s"
    kill -9 "$HOLDER_PID" 2>/dev/null
fi

# 4c — nothing interferes. It MUST remove its own lock: a refusal that fires on everything is
#      worse than none, and 4b/4d/4e/4f would all pass for one.
LC=$TD/c.lock
start_holder "$LC" arm-c
if await_owner "$LC"; then
    wait "$HOLDER_PID"; rcc=$?
    [ -d "$LC" ] && bad "4c: the holder did NOT release its own lock — the guard refuses \
unconditionally, so 4b, 4d, 4e and 4f prove nothing" \
                 || ok "4c: released its own lock on a clean exit"
    grep -q 'REFUSING to release\|ALREADY GONE' "$TD/arm-c.err" \
        && bad "4c: warned about ownership on a lock that WAS its own: $(grep -m1 'REFUSING to release\|ALREADY GONE' "$TD/arm-c.err")" \
        || ok "4c: no spurious ownership warning"
    [ "$rcc" = "1" ] && ok "4c: exited 1 via the ordinary refusal path" \
                     || bad "4c: exit status was $rcc, expected 1"
else
    bad "4c: the holder never took its lock within 40s"; kill -9 "$HOLDER_PID" 2>/dev/null
fi

# 4d — owner file deleted under a lock directory that is still there.
LD=$TD/d.lock
start_holder "$LD" arm-d
if await_owner "$LD"; then
    rm -f "$LD/owner"
    wait "$HOLDER_PID"
    [ -d "$LD" ] && ok "4d: refused to remove a lock whose owner file is missing" \
                 || bad "4d: removed a lock it could not identify — fail-open on an unreadable owner"
    grep -q 'missing or unparseable' "$TD/arm-d.err" \
        && ok "4d: warned: $(grep -m1 'REFUSING to release' "$TD/arm-d.err")" \
        || bad "4d: no 'missing or unparseable' warning on stderr"
else
    bad "4d: the holder never took its lock within 40s"; kill -9 "$HOLDER_PID" 2>/dev/null
fi

# 4e — owner file present but not a pid.
LE=$TD/e.lock
start_holder "$LE" arm-e
if await_owner "$LE"; then
    printf 'not-a-pid whatever\n' > "$LE/owner"
    wait "$HOLDER_PID"
    [ -d "$LE" ] && ok "4e: refused to remove a lock whose owner does not parse as a pid" \
                 || bad "4e: removed a lock whose owner is garbage"
    grep -q 'missing or unparseable' "$TD/arm-e.err" \
        && ok "4e: warned: $(grep -m1 'REFUSING to release' "$TD/arm-e.err")" \
        || bad "4e: no 'missing or unparseable' warning on stderr"
else
    bad "4e: the holder never took its lock within 40s"; kill -9 "$HOLDER_PID" 2>/dev/null
fi

# 4f — the whole lock directory removed, and nothing re-created it. This is the FIRST half of the
#      live sequence, and it is the only signal a run ever gets that its lock was taken.
LF=$TD/f.lock
start_holder "$LF" arm-f
if await_owner "$LF"; then
    rm -rf "$LF"
    wait "$HOLDER_PID"
    grep -q 'ALREADY GONE' "$TD/arm-f.err" \
        && ok "4f: warned: $(grep -m1 'ALREADY GONE' "$TD/arm-f.err")" \
        || bad "4f: a run whose lock vanished said nothing — the removal stays invisible"
else
    bad "4f: the holder never took its lock within 40s"; kill -9 "$HOLDER_PID" 2>/dev/null
fi

# 4g — the note-7 abort must survive intact AND inherit the ownership check. If this arm ever
#      reports exit 0 or a missing REFUSING line, note 7 has been regressed by note 8.
LG=$TD/g.lock
start_holder "$LG" arm-g
if await_owner "$LG"; then
    # The signal must land while a child is in flight, or it tests a different code path than the
    # one note 7 is about — the same reason part 2 waits for children before signalling.
    kidsg=""; i=0
    while [ -z "$kidsg" ] && [ $i -lt 30 ]; do kidsg=$(descendants "$HOLDER_PID" | tr '\n' ' '); [ -z "$kidsg" ] && sleep 1; i=$((i+1)); done
    [ -n "$kidsg" ] && ok "4g: children in flight before the signal: $kidsg" \
                    || bad "4g: no child ever appeared; the signal would not land mid-child"
    swap_owner "$LG"
    kill -TERM "$HOLDER_PID"
    wait "$HOLDER_PID"; rcg=$?
    [ "$rcg" = "143" ] && ok "4g: still exited 143 — the signal abort of note 7 survives" \
                       || bad "4g: exit status was $rcg, expected 143 — note 7 regressed"
    grep -q 'aborted by SIGTERM' "$TD/arm-g.err" \
        && ok "4g: still printed the note-7 abort refusal" \
        || bad "4g: no 'aborted by SIGTERM' refusal — note 7 regressed"
    [ -d "$LG" ] && ok "4g: and the other run's lock survived the abort path too" \
                 || bad "4g: the abort path deleted the other run's lock — the ownership check is \
absent from _abort's release"
    survg=""
    for p in $kidsg; do alive "$p" && survg="$survg $p"; done
    [ -n "$survg" ] && bad "4g: descendants survived the abort:$survg" \
                    || ok "4g: no descendant survived the abort (note 7's child-kill intact)"
    for p in $survg; do kill -9 "$p" 2>/dev/null; done
else
    bad "4g: the holder never took its lock within 40s"; kill -9 "$HOLDER_PID" 2>/dev/null
fi

kill -9 "$OTHER" 2>/dev/null; wait "$OTHER" 2>/dev/null

# ── part 5: every script that RELEASES the suite lock carries the check ─────────────────────────
#
# The ownership check lives in six scripts rather than one sourced helper, because each must stay
# runnable standalone: a bench artifact that sources a file which changes afterwards stops being a
# record of the run it documents. Six copies is exactly how a guard quietly stops existing — part 3
# above exists for that hazard over two copies of a string. So this sweeps for the shape instead of
# trusting the list: every script under tools/ and bench/ that removes a lock directory must be
# registered here, and every registered RELEASER must carry the marker. A new script that takes the
# machine-wide lock and forgets the check fails this check rather than the fleet.
note ""
note "== part 5: no unregistered lock-remover =="
RELEASERS="tools/verify-suite.sh
bench/d130_run.sh
bench/w4_standdown/hold_suite_lock.sh
bench/d42_verify_all.sh
bench/d42_fire_broken.sh
bench/d42_fire_starved_children.sh"
# This file itself matches the sweep and is NOT a releaser: part 4a's anti-vacuity fixture contains
# the old flag-only release verbatim, which is the point of it. Registered rather than excluded by
# name, so the sweep still has to account for every file it finds.
NOT_RELEASERS="tools/verify-suite-selftest.sh"
MARKER='LOCK-RELEASE-OWNERSHIP-CHECKED'
# `^[^#]*` so a COMMENT quoting the old shape does not trip the sweep — three of the fixed files
# quote it deliberately, and bench/d123_gate.sh describes a lock-breaking rule it does not perform.
#
# ⚠ WHAT THIS PATTERN CANNOT SEE, stated here rather than left to be discovered. It wants the word
# `rm` and the lock path on ONE uncommented line. Measured while fire-checking this part: the first
# version demanded the literal `rm -rf`, and `rm -fr` — the same command, flags swapped — made a
# releaser invisible to it. Widening to any `rm` fixes that, but a removal that never writes `rm`
# beside the path still evades it: `find "$LOCK" -delete`, or a helper that takes the path as an
# argument. That residual is what the second loop below is for — it does not ask "did the sweep
# find anything", it asks whether the sweep still finds each releaser we already know about, so the
# pattern rotting is a FAILURE here rather than a silent narrowing.
found=$(grep -rlE '^[^#]*rm[^#]*(\$(SUITE_)?LOCK|ferrodb-suite\.lock)' --include='*.sh' tools bench 2>/dev/null | sort)
for f in $found; do
    if printf '%s\n' "$RELEASERS" "$NOT_RELEASERS" | grep -qFx "$f"; then continue; fi
    bad "$f removes a lock directory but is not registered in part 5 of this file. Give it the \
ownership check (note 8 in tools/verify-suite.sh) and add it to RELEASERS."
done
# Anti-vacuity, and the direction that matters: if the sweep stopped matching a registered releaser
# — pattern rotted, file renamed, release rewritten — the loop above would report nothing wrong
# while covering nothing. So every releaser must be FOUND as well as marked.
for f in $RELEASERS; do
    if [ ! -f "$f" ]; then
        bad "registered releaser $f no longer exists — update RELEASERS in this file"
        continue
    fi
    if ! printf '%s\n' "$found" | grep -qFx "$f"; then
        bad "the sweep did not match $f, a known releaser — the pattern has rotted, so an \
unregistered lock-remover would now pass unnoticed"
    elif grep -qF "$MARKER" "$f"; then
        ok "$f: found by the sweep and carries the ownership-check marker"
    else
        bad "$f releases the suite lock but has lost the '$MARKER' marker — the ownership check \
has drifted out of one of the six copies"
    fi
done

note ""
if [ "$fails" -eq 0 ]; then note "SELFTEST PASSED — all five parts behaved as documented"; exit 0; fi
note "SELFTEST FAILED — $fails check(s) did not hold"; exit 1
