#!/usr/bin/env bash
# Trap fire-check for a fire-check judge (bench/d232/firecheck.sh, bench/d263/firecheck.sh): the judge's REAL mode,
# run in a throwaway clone against a fake `cargo` on PATH that parks (sleeps) at a chosen point, then signalled.
# Nothing is built: the fake prints cargo-shaped output. Memory an-exit-trap-waits-for-the-foreground-child.
#
# Usage: trapcheck.sh REPO TIP JUDGE_IN_REPO SUBJECT_SHA MUTANT_PREFIX OLD_JUDGE_SHA WORKDIR
#   TIP            the commit the clone checks out (its judge is the one under test)
#   JUDGE_IN_REPO  e.g. bench/d232/firecheck.sh
#   OLD_JUDGE_SHA  a commit whose copy of the judge is the negative control ("-" for none)
# Per case it prints: judge, park point, signal, judge exit code, seconds from signal to exit, whether the parked
# fake survived the judge (ORPHAN), and whether src/ equals SUBJECT_SHA with a clean porcelain afterwards.
set -u
[ $# -eq 7 ] || { echo "usage: $0 REPO TIP JUDGE_IN_REPO SUBJECT_SHA MUTANT_PREFIX OLD_JUDGE_SHA WORKDIR" >&2; exit 2; }
REPO=$1 TIP=$2 JUDGE=$3 SUBJECT=$4 MUTPFX=$5 OLD=$6 WORK=$7
case "$WORK" in *.noindex) ;; *) echo "WORKDIR must end in .noindex" >&2; exit 2 ;; esac
rm -rf "$WORK"; mkdir -p "$WORK/bin" || exit 2
CLONE=$WORK/clone
git clone -q --shared --no-checkout "$REPO" "$CLONE" || exit 2
git -C "$CLONE" checkout -q --detach "$TIP" || exit 2
for r in $(git -C "$CLONE" for-each-ref --format='%(refname:short)' "refs/remotes/origin/$MUTPFX*"); do
  git -C "$CLONE" branch -q "${r#origin/}" "$r" || exit 2
done
if [ "$OLD" != - ]; then git -C "$CLONE" show "$OLD:$JUDGE" > "$WORK/old_judge.sh" || exit 2; fi

cat > "$WORK/bin/cargo" <<'FAKE'
#!/bin/bash
# Fake cargo: cargo-shaped output, and a park (exec sleep) at the point FAKE_PARK names.
args="$*"
case "$args" in *"-- --list"*) mode=list ;; *) mode=run ;; esac
if { [ "$mode" = list ] && [ "$FAKE_PARK" = list ]; } ||
   { [ "$mode" = run ] && [ "$FAKE_PARK" = arm ] && ! git diff --quiet "$FAKE_SUBJECT" -- src/; }; then
  echo "PARK $$" >> "$FAKE_LOG"
  exec sleep 300
fi
n=31
case "$args" in *"--lib d263_"*) n=7 ;; *"--lib d232_"*) n=31 ;; *"--lib branch::"*) n=400 ;; *"--test "*) n=3 ;; esac
bins=1; case "$args" in *"--test d40"*) bins=3 ;; esac
b=0
while [ $b -lt $bins ]; do
  b=$((b + 1)); i=0
  if [ "$mode" = list ]; then
    while [ $i -lt $n ]; do i=$((i + 1)); echo "b$b::t$i: test"; done
  else
    echo "running $n tests"
    echo "test result: ok. $n passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s"
  fi
done
exit 0
FAKE
chmod +x "$WORK/bin/cargo"

clean_src() { # prints ok or DIRTY
  if git -C "$CLONE" diff --quiet "$SUBJECT" -- src/ tests/ && [ -z "$(git -C "$CLONE" status --porcelain -- src/ tests/)" ]
  then echo ok; else echo DIRTY; fi
}

run_case() { # $1 judge file, $2 park (list|arm), $3 signal
  local judge=$1 park=$2 sig=$3 jpid ppid t0 t1 waited_s rc orphan src i
  : > "$WORK/fake.log"
  set -m # job control, so the judge's INT and QUIT are not ignored as a background job's would be
  ( cd "$CLONE" && PATH="$WORK/bin:$PATH" FAKE_SUBJECT="$SUBJECT" FAKE_PARK="$park" FAKE_LOG="$WORK/fake.log" \
      exec bash "$judge" ) > "$WORK/judge.$park.$sig.out" 2>&1 &
  jpid=$!
  set +m
  ppid=""
  for i in $(seq 1 600); do # up to 60 s for the fake to park
    ppid=$(sed -n 's/^PARK //p' "$WORK/fake.log" | head -n 1)
    [ -n "$ppid" ] && break
    kill -0 "$jpid" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$ppid" ]; then
    wait "$jpid"; rc=$?
    echo "$(basename "$judge") park=$park sig=$sig: NEVER PARKED (judge rc=$rc; see judge.$park.$sig.out)"
    return
  fi
  sleep 0.5
  t0=$(date +%s)
  kill "-$sig" "$jpid"
  for i in $(seq 1 100); do kill -0 "$jpid" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$jpid" 2>/dev/null; then
    # Still alive 10 s after the signal: the trap is deferred. Release the park so the judge can finish.
    kill "$ppid" 2>/dev/null
    wait "$jpid"; rc=$?
    t1=$(date +%s)
    echo "$(basename "$judge") park=$park sig=$sig: rc=$rc DEFERRED (alive 10 s after the signal; exited $((t1 - t0)) s after it once the park was released) src=$(clean_src)"
    return
  fi
  wait "$jpid"; rc=$?
  t1=$(date +%s)
  waited_s=$((t1 - t0))
  orphan=no
  if kill -0 "$ppid" 2>/dev/null; then orphan=ORPHAN; kill "$ppid" 2>/dev/null; fi
  src=$(clean_src)
  [ "$src" = ok ] || git -C "$CLONE" restore --source="$SUBJECT" --staged --worktree -- src/
  echo "$(basename "$judge") park=$park sig=$sig: rc=$rc exit_after=${waited_s}s parked_fake=$orphan src=$src"
}

echo "trapcheck: tip=$(git -C "$CLONE" rev-parse HEAD) judge=$JUDGE subject=$SUBJECT old=$OLD"
echo "branches: $(git -C "$CLONE" for-each-ref --format='%(refname:short)' "refs/heads/$MUTPFX*" | wc -l | tr -d ' ') $MUTPFX*"
for park in list arm; do
  for sig in TERM HUP INT QUIT; do run_case "$CLONE/$JUDGE" "$park" "$sig"; done
done
if [ "$OLD" != - ]; then
  run_case "$WORK/old_judge.sh" list TERM
  run_case "$WORK/old_judge.sh" arm HUP
fi
