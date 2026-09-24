#!/usr/bin/env bash
# D257 fire-check. Pre-registration: artie-research frontier/lane_d257_insert_bound.md §3, amended by
# §3.1 and §3.2 (review 1). Fan work: it runs only when the lead releases the FAN-QUEUE row. Run from the
# worktree root at DEFAULT QoS (never taskpolicy -b).
#
#   bash bench/d257/firecheck.sh             the run (it runs the self-test first and refuses on a miss)
#   bash bench/d257/firecheck.sh --selftest  the judge on planted outputs only: no cargo, no git writes
#   anything else                            exits 2
#
# Arms, in run order:
# - control: SUBJECT_SHA, both targets: every file OK, nothing FAILED, 7 and 16 passed. Anything else VOIDS
#            the run: exit 2, before any other arm is built.
# - base:    src/ at 17e4c26, tests at SUBJECT_SHA. d257_insert_bound must FAIL exactly T1 T3 T4 T5 T6, EACH
#            WITH ITS REGISTERED REASON (§3.2 R6a: a substring of its failing assertion, found in that
#            test's own `---- <name> stdout ----` block); integration_alter_refusal_safety must pass.
# - mutants: `bench/d257/mutants.py`, one at a time. KILLED-AS-REGISTERED when every file is OK, every
#            required killer FAILED, and nothing outside required + optional did (`alter::*` = any test of
#            that target). MC is pre-registered as equivalent: its as-registered outcome is that nothing
#            fails (SURVIVED-AS-REGISTERED).
#
# The judge reads exactly one file per target, `$OUT/<label>.<target>.out`, named from TARGETS and never
# globbed (D237 review 2, N2: a glob read a diffstat as a target output). Each file gets a state before
# any name is read: TIMEOUT (rc 124), COMPILE-FAIL (no `test result:` line), INCOMPLETE (no file, no rc
# line, or more than one result line), RC-MISMATCH (the rc and the FAILED lines disagree).
#
# src/ is restored from GIT on every exit (an EXIT trap; never from a copy), and every cargo command runs
# in the background and is waited on, so a TERM or INT reaches the traps at once: bash defers a trap until
# a foreground child exits, and `timeout` puts cargo in its own process group (D237 e8054dc).
#
# Blind spots, stated: two targets, not the whole suite (the per-target suite is the FAN-QUEUE row's own
# step); the judge reads cargo's own lines, so a test printing a line of exactly that shape would be
# misread (none is run with --nocapture); reasons are checked on the base arm only.
set -u
set -f # no globbing: killer lists carry a literal `*`
unset RUSTFLAGS # review 1 R4: an exported `-D warnings` would make MF's unreachable code a compile error

SUBJECT_SHA=edc34a9
BASE_SHA=17e4c26
OUT=bench/d257/firecheck
TARGETS="d257 alter"
SCRIPT=$0

T1=an_oversize_insert_is_refused_before_any_page_is_allocated
T2=a_tuple_of_exactly_the_limit_is_still_accepted
T3=two_thousand_refused_inserts_leave_the_pool_and_the_file_as_they_were
T4=the_space_arithmetic_refuses_rather_than_wrapping_at_the_u16_boundary
T5=an_oversize_sql_insert_changes_no_page_of_the_table
T6=an_oversize_sql_update_writes_nothing_to_the_time_travel_heap
T7=an_insert_refused_inside_its_page_releases_the_pin
UNLOGGED=an_unlogged_update_too_large_for_any_page_refuses_instead_of_deleting_the_row

# name|required killers|optional killers
KILLERS=(
  "MA_no_refusal|d257::$T1 d257::$T3 d257::$T4 d257::$T5 d257::$T6 alter::$UNLOGGED|alter::*"
  "MB_off_by_one|d257::$T2 d257::$T4|alter::*"
  "MC_wrapping_cast||"
  "MD_no_update_precheck|d257::$T6|"
  "ME_insert_into_hand_unpin|d257::$T7|"
  "MF_old_error|d257::$T1 d257::$T3 d257::$T4 d257::$T5 d257::$T6|"
  "MG_narrowed_refusal|d257::$T4|"
)
BASE_REQUIRED="d257::$T1 d257::$T3 d257::$T4 d257::$T5 d257::$T6"
# test|registered reason (§3.2 R6a)
BASE_REASONS=(
  "$T1|changed the heap's pages"
  "$T3|refused inserts changed the file"
  "$T4|must be refused with a Constraint naming the limit"
  "$T5|changed t's pages"
  "$T6|changed the time-travel heap's pages"
)

# ---- Running. A command runs in the background and is waited on; see the header. ----
child=""
waited() { # the command's rc
  local rc
  "$@" &
  child=$!
  wait "$child"
  rc=$?
  child=""
  return "$rc"
}

target_args() { # $1 target
  case "$1" in
    d257)  echo "--test d257_insert_bound" ;;
    alter) echo "--test integration_alter_refusal_safety" ;;
    *) echo "unknown target $1" >&2; exit 2 ;;
  esac
}

run_target() { # $1 label, $2 target
  local rc
  # shellcheck disable=SC2046
  waited timeout 1800 cargo test $(target_args "$2") > "$OUT/$1.$2.out" 2>&1
  rc=$?
  echo "rc=$rc" >> "$OUT/$1.$2.out"
}

# ---- The judge. It reads ONLY "$OUT/<label>.<target>.out" for the targets it is given. ----
tfile() { printf '%s/%s.%s.out' "$OUT" "$1" "$2"; }

tstate() { # $1 label, $2 target: OK, or why its names cannot be judged
  local f last rc n nf
  f=$(tfile "$1" "$2")
  [ -f "$f" ] || { echo "INCOMPLETE ($2: no output file)"; return; }
  last=$(tail -n 1 "$f")
  case "$last" in rc=[0-9]*) rc=${last#rc=} ;; *) echo "INCOMPLETE ($2: no rc line)"; return ;; esac
  if [ "$rc" = 124 ]; then echo "TIMEOUT ($2)"; return; fi
  n=$(grep -cE '^test result:' "$f")
  if [ "$n" -eq 0 ]; then echo "COMPILE-FAIL ($2)"; return; fi
  if [ "$n" -ne 1 ]; then echo "INCOMPLETE ($2: $n result lines)"; return; fi
  nf=$(grep -cE '^test .+ \.\.\. FAILED$' "$f")
  case "$rc/$nf" in
    0/0) ;;
    101/0) echo "RC-MISMATCH ($2: rc=101 and nothing FAILED)"; return ;;
    101/*) ;;
    0/*) echo "RC-MISMATCH ($2: rc=0 with FAILED lines)"; return ;;
    *) echo "RC-$rc ($2)"; return ;;
  esac
  echo OK
}

arm_state() { # $1 label, then targets: the first state that is not OK, or OK
  local label=$1 t s
  shift
  for t in "$@"; do
    s=$(tstate "$label" "$t")
    [ "$s" = OK ] || { echo "$s"; return; }
  done
  echo OK
}

failed_names() { # $1 label, then targets: `<target>::<test>` for every FAILED test, sorted
  local label=$1 t
  shift
  for t in "$@"; do
    sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' "$(tfile "$label" "$t")" | sed -E "s/.*:://; s/^/$t::/"
  done | sort -u
}

passed_in() { # $1 label, $2 target
  grep -hE '^test result:' "$(tfile "$1" "$2")" 2>/dev/null | tail -1 | sed -nE 's/.* ([0-9]+) passed.*/\1/p'
}

words() { printf '%s\n' $1 | sed '/^$/d' | sort -u; }

covered() { # $1 target::test, $2 list: is it in the list? `target::*` covers a whole target
  local w
  for w in $2; do
    case "$w" in
      *::\*) [ "${1%%::*}" = "${w%%::*}" ] && return 0 ;;
      *) [ "$1" = "$w" ] && return 0 ;;
    esac
  done
  return 1
}

verdict() { # $1 label, $2 required, $3 optional, then the targets the arm ran
  local label=$1 req=$2 opt=$3 st actual missing extra name
  shift 3
  st=$(arm_state "$label" "$@")
  [ "$st" = OK ] || { echo "$st"; return; }
  actual=$(failed_names "$label" "$@")
  if [ -z "$actual" ]; then
    if [ -z "$(words "$req")" ]; then echo "SURVIVED-AS-REGISTERED"; else echo "SURVIVED"; fi
    return
  fi
  missing=$(comm -23 <(words "$req") <(printf '%s\n' "$actual"))
  extra=""
  while read -r name; do
    covered "$name" "$req $opt" || extra="$extra $name"
  done <<< "$actual"
  if [ -z "$missing" ] && [ -z "$extra" ]; then
    echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else
    echo "MISMATCH (missing: $(echo $missing); unexpected:$extra)"
  fi
}

# The lines of one test's `---- <name> stdout ----` block in a target's output.
failure_block() { # $1 label, $2 target, $3 test name
  awk -v h="---- $3 stdout ----" '
    $0 == h { on = 1; next }
    on && (/^---- / || /^failures:$/) { exit }
    on { print }
  ' "$(tfile "$1" "$2")"
}

# The base arm: the name verdict, and then each registered reason in its own test's block.
base_verdict() { # $1 label
  local v r test reason block miss=""
  v=$(verdict "$1" "$BASE_REQUIRED" "" $TARGETS)
  case "$v" in KILLED-AS-REGISTERED*) ;; *) echo "$v"; return ;; esac
  for r in "${BASE_REASONS[@]}"; do
    test=${r%%|*}; reason=${r#*|}
    block=$(failure_block "$1" d257 "$test")
    if [ -z "$block" ]; then
      miss="$miss $test:no-block"
    elif ! printf '%s\n' "$block" | grep -qF -- "$reason"; then
      miss="$miss $test:other-reason"
    fi
  done
  if [ -n "$miss" ]; then echo "MISMATCH-REASON ($(echo $miss))"; else echo "$v, every reason as registered"; fi
}

as_registered() { case "$1" in KILLED-AS-REGISTERED*|SURVIVED-AS-REGISTERED) return 0 ;; *) return 1 ;; esac; }

killers_of() { # $1 mutant: sets req and opt; returns 1 if it has no row
  local k kname kreq kopt
  req=""; opt=""
  for k in "${KILLERS[@]}"; do
    IFS='|' read -r kname kreq kopt <<< "$k"
    if [ "$kname" = "$1" ]; then req=$kreq; opt=$kopt; return 0; fi
  done
  return 1
}

# ---- --selftest: the judge on planted outputs, in a temporary directory. No cargo, no git writes. ----
plant() { # $1 label, $2 target, $3 rc ("none" = no rc line), then the file's lines
  local f rc line
  f=$(tfile "$1" "$2"); rc=$3
  shift 3
  { for line in "$@"; do printf '%s\n' "$line"; done; [ "$rc" = none ] || echo "rc=$rc"; } > "$f"
}
ok_file() { # $1 label, $2 target, $3 passed
  plant "$1" "$2" 0 "running $3 tests" "test result: ok. $3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
}
fail_file() { # $1 label, $2 target, $3 passed, then "name|reason" for each FAILED test
  local label=$1 t=$2 p=$3 e lines=()
  shift 3
  for e in "$@"; do lines+=("test ${e%%|*} ... FAILED"); done
  lines+=("" "failures:" "")
  for e in "$@"; do
    lines+=("---- ${e%%|*} stdout ----" "" "thread '${e%%|*}' panicked at tests/x.rs:1:5:" "${e#*|}" "")
  done
  lines+=("" "failures:")
  for e in "$@"; do lines+=("    ${e%%|*}"); done
  lines+=("" "test result: FAILED. $p passed; $# failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s")
  plant "$label" "$t" 101 "${lines[@]}"
}
expect() { # $1 case, $2 expected verdict prefix, $3 actual verdict
  case "$3" in
    "$2"*) echo "self-test PASS  $1: $3" ;;
    *) echo "self-test FAIL  $1: expected '$2', got '$3'"; st_bad=$((st_bad + 1)) ;;
  esac
}

self_test() {
  local keep=$OUT st_bad=0 tmp r base_ok base_wrong rc name
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/d257-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp

  ok_file clean d257 7; ok_file clean alter 16
  expect "nothing fails, nothing required" SURVIVED-AS-REGISTERED "$(verdict clean "" "" $TARGETS)"
  expect "nothing fails, a killer required" "SURVIVED" "$(verdict clean "d257::a" "" $TARGETS)"
  [ "$(passed_in clean d257)" = 7 ] && [ "$(passed_in clean alter)" = 16 ] && echo "self-test PASS  passed_in reads 7/16" \
    || { echo "self-test FAIL  passed_in read $(passed_in clean d257)/$(passed_in clean alter)"; st_bad=$((st_bad + 1)); }

  fail_file exact d257 5 "a|x" "b|y"; ok_file exact alter 16
  expect "exactly the required" KILLED-AS-REGISTERED "$(verdict exact "d257::a d257::b" "" $TARGETS)"
  expect "one required missing" MISMATCH "$(verdict exact "d257::a d257::b d257::c" "" $TARGETS)"
  expect "one failure unregistered" MISMATCH "$(verdict exact "d257::a" "" $TARGETS)"
  expect "the unregistered one optional" KILLED-AS-REGISTERED "$(verdict exact "d257::a" "d257::b" $TARGETS)"

  fail_file wild d257 6 "a|x"; fail_file wild alter 14 "z|x" "y|x"
  expect "alter::* covers any alter failure" KILLED-AS-REGISTERED "$(verdict wild "d257::a" "alter::*" $TARGETS)"
  expect "without alter::*, alter failures are unexpected" MISMATCH "$(verdict wild "d257::a" "" $TARGETS)"
  expect "alter::* does not cover d257" MISMATCH "$(verdict wild "alter::z alter::y" "alter::*" $TARGETS)"

  fail_file path d257 6 "tests::deep::a|x"; ok_file path alter 16
  expect "module path stripped" KILLED-AS-REGISTERED "$(verdict path "d257::a" "" $TARGETS)"

  # N2's plant: stray files that share the label, each with an unregistered FAILED line and no result.
  fail_file stray d257 6 "a|x"; ok_file stray alter 16
  for name in stray.diffstat stray.junk.txt stray.d257.out.txt stray.diff; do
    printf '%s\n' " src/storage/heap_file_manager.rs | 2 +-" "test stray_not_registered ... FAILED" > "$OUT/$name"
  done
  expect "stray files beside the outputs" KILLED-AS-REGISTERED "$(verdict stray "d257::a" "" $TARGETS)"

  for t in $TARGETS; do plant cf "$t" 101 "error[E0308]: mismatched types" "error: could not compile \`ferrodb\`"; done
  expect "compile failure" "COMPILE-FAIL" "$(verdict cf "d257::a" "" $TARGETS)"
  ok_file to alter 16; plant to d257 124 "running 7 tests" "test $T3 has been running for over 60 seconds"
  expect "timeout" "TIMEOUT (d257)" "$(verdict to "d257::a" "" $TARGETS)"
  ok_file gone d257 7
  expect "a target with no output file" "INCOMPLETE (alter: no output file)" "$(verdict gone "d257::a" "" $TARGETS)"
  ok_file norc alter 16; plant norc d257 none "running 7 tests" "test a ... FAILED"
  expect "a target with no rc line" "INCOMPLETE (d257: no rc line)" "$(verdict norc "d257::a" "" $TARGETS)"
  ok_file rcm alter 16; plant rcm d257 0 "test a ... FAILED" "test result: FAILED. 6 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "rc=0 with a FAILED line" "RC-MISMATCH" "$(verdict rcm "d257::a" "" $TARGETS)"

  # R6a: the base arm, with the registered reasons, a wrong reason, and a missing block.
  base_ok=()
  for r in "${BASE_REASONS[@]}"; do base_ok+=("${r%%|*}|assertion failed: ${r#*|}: planted"); done
  fail_file base d257 2 "${base_ok[@]}"; ok_file base alter 16
  expect "base arm, every reason as registered" "KILLED-AS-REGISTERED" "$(base_verdict base)"
  base_wrong=("${base_ok[@]}")
  base_wrong[1]="$T3|called \`Result::unwrap()\` on an \`Err\` value: NotEnoughSpace"
  fail_file basewrong d257 2 "${base_wrong[@]}"; ok_file basewrong alter 16
  expect "base arm, T3 failed for another reason" "MISMATCH-REASON ($T3:other-reason)" "$(base_verdict basewrong)"
  fail_file basenoblock d257 2 "${base_ok[@]}"; ok_file basenoblock alter 16
  sed -i.bak "/^---- $T5 stdout ----\$/,/^\$/d" "$(tfile basenoblock d257)"; rm -f "$(tfile basenoblock d257).bak"
  expect "base arm, T5's block missing" "MISMATCH-REASON ($T5:no-block)" "$(base_verdict basenoblock)"
  expect "base arm, a reason in ANOTHER test's block does not count" "MISMATCH-REASON" "$(
    fail_file basecross d257 2 "${base_ok[@]:0:4}" "$T6|assertion failed: changed the heap's pages"
    ok_file basecross alter 16
    base_verdict basecross)"

  # Registration: MC alone has no required killer, by design; every other mutant has one.
  for name in $(python3 bench/d257/mutants.py list 2>/dev/null || echo UNLISTED); do
    if ! killers_of "$name"; then echo "self-test FAIL  $name has no KILLERS row"; st_bad=$((st_bad + 1)); continue; fi
    if [ "$name" = MC_wrapping_cast ]; then
      [ -z "$req" ] || { echo "self-test FAIL  MC is registered equivalent but has killers"; st_bad=$((st_bad + 1)); }
    elif [ -z "$req" ]; then
      echo "self-test FAIL  $name has no required killer"; st_bad=$((st_bad + 1))
    fi
  done
  as_registered "SURVIVED" && { echo "self-test FAIL  SURVIVED counted as registered"; st_bad=$((st_bad + 1)); } \
    || echo "self-test PASS  SURVIVED is not as-registered"

  # Usage: an unknown argument exits 2, before touching anything.
  bash "$SCRIPT" --no-such-flag > /dev/null 2>&1
  rc=$?
  [ "$rc" = 2 ] && echo "self-test PASS  an unknown argument exits 2" \
    || { echo "self-test FAIL  an unknown argument exited $rc"; st_bad=$((st_bad + 1)); }

  rm -rf "$tmp"
  OUT=$keep
  echo "self-test: $st_bad case(s) missed"
  [ "$st_bad" -eq 0 ]
}

# ---- Entry ----
cd "$(git rev-parse --show-toplevel)" || exit 2
case "$#/${1:-}" in
  0/) ;;
  1/--selftest) self_test; exit $? ;;
  *) echo "usage: $SCRIPT [--selftest]" >&2; exit 2 ;;
esac

if ! git diff --quiet "$SUBJECT_SHA" -- src/ tests/; then
  echo "REFUSED: src/ or tests/ differ from $SUBJECT_SHA; the mutants' text may not apply" >&2
  exit 2
fi
if [ -n "$(git status --porcelain -- src/ tests/)" ]; then
  echo "REFUSED: uncommitted changes under src/ or tests/" >&2
  exit 2
fi
if ! timeout 60 python3 bench/d257/mutants.py check > /dev/null; then
  echo "REFUSED: a mutant's anchor no longer matches $SUBJECT_SHA" >&2
  exit 2
fi

rm -rf "$OUT"
mkdir -p "$OUT"
if ! self_test > "$OUT/selftest.log" 2>&1; then
  cat "$OUT/selftest.log" >&2
  echo "REFUSED: the judge's self-test failed" >&2
  exit 2
fi

on_exit() {
  git checkout "$SUBJECT_SHA" -- src/ 2>/dev/null
  git diff --quiet "$SUBJECT_SHA" -- src/ tests/ ||
    echo "ON EXIT: src/ or tests/ still differ from $SUBJECT_SHA; restore with: git checkout $SUBJECT_SHA -- src/" >&2
}
on_signal() { # $1 the exit status
  if [ -n "$child" ]; then
    kill -TERM "$child" 2>/dev/null
    wait "$child" 2>/dev/null
  fi
  exit "$1"
}
trap on_exit EXIT
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

restore() { # $1 what was just run
  git checkout "$SUBJECT_SHA" -- src/
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after $1" >&2; exit 3; }
}

bad=0

echo "== control: $SUBJECT_SHA"
for t in $TARGETS; do run_target control "$t"; done
st=$(arm_state control $TARGETS)
if [ "$st" != OK ] || [ -n "$(failed_names control $TARGETS)" ] \
   || [ "$(passed_in control d257)" != 7 ] || [ "$(passed_in control alter)" != 16 ]; then
  echo "control: VOID (state: $st; failed: $(failed_names control $TARGETS | tr '\n' ' '); passed d257=$(passed_in control d257) alter=$(passed_in control alter); expected 7/16)" | tee "$OUT/summary.txt"
  exit 2
fi
echo "control: clean (7/16 passed)" | tee "$OUT/summary.txt"

echo "== base: src/ at $BASE_SHA"
git checkout "$BASE_SHA" -- src/
for t in $TARGETS; do run_target base "$t"; done
restore base
v=$(base_verdict base)
echo "base: $v" | tee -a "$OUT/summary.txt"
as_registered "$v" || bad=$((bad + 1))

for k in "${KILLERS[@]}"; do
  IFS='|' read -r name req opt <<< "$k"
  echo "== $name"
  if ! timeout 60 python3 bench/d257/mutants.py apply "$name"; then
    echo "$name: NOT APPLIED" | tee -a "$OUT/summary.txt"
    bad=$((bad + 1))
    continue
  fi
  git diff -- src/ > "$OUT/$name.diff"
  for t in $TARGETS; do run_target "$name" "$t"; done
  restore "$name"
  v=$(verdict "$name" "$req" "$opt" $TARGETS)
  echo "$name: $v" | tee -a "$OUT/summary.txt"
  as_registered "$v" || bad=$((bad + 1))
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
