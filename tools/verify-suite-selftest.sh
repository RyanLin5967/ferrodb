#!/usr/bin/env bash
# tools/verify-suite-selftest.sh — fire-check the SIGTERM abort in verify-suite.sh.
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
# Usage: tools/verify-suite-selftest.sh     exit 0 = both halves behaved as documented.
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

note ""
if [ "$fails" -eq 0 ]; then note "SELFTEST PASSED — both halves behaved as note 7 documents"; exit 0; fi
note "SELFTEST FAILED — $fails check(s) did not hold"; exit 1
