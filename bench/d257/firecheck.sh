#!/usr/bin/env bash
# D257 fire-check. Pre-registration: artie-research frontier/lane_d257_insert_bound.md §3, amended by
# §3.1, §3.2 (review 1), §3.4-§3.6 and §3.8 (review 2). Fan work: it runs only when the lead releases the
# FAN-QUEUE row. Run from the worktree root at DEFAULT QoS (never taskpolicy -b).
#
#   bash bench/d257/firecheck.sh             the run (it runs the self-test first and refuses on a miss)
#   bash bench/d257/firecheck.sh --selftest  the judge and the run's own counting on planted outputs only:
#                                            no cargo, no git writes (it reads the subject's test file)
#   anything else                            exits 2
#
# Arms, in run order (`run_arms`):
# - control: SUBJECT_SHA, both targets: every file OK, nothing FAILED, 7 and 16 passed. Anything else VOIDS
#            the run: exit 2, before any other arm is built.
# - base:    src/ at 17e4c26, tests at SUBJECT_SHA. d257_insert_bound must FAIL exactly T1 T3 T4 T5 T6, EACH
#            WITH ITS REGISTERED REASON (§3.2 R6a: a substring of its failing assertion, found in that
#            test's own `---- <name> stdout ----` block, which ends at the next test's header);
#            integration_alter_refusal_safety must pass. It counts in not-as-registered like every arm.
# - mutants: `bench/d257/mutants.py`, one at a time. KILLED-AS-REGISTERED when every file is OK, every
#            required killer FAILED, and nothing outside required + optional did (`alter::*` = any test of
#            that target). MC is pre-registered as equivalent: its as-registered outcome is that nothing
#            fails (SURVIVED-AS-REGISTERED).
#
# The judge reads exactly one file per target, `$OUT/<label>.<target>.out`, named from TARGETS and never
# globbed (D237 review 2, N2). Each file gets a state before any name is read: TIMEOUT (rc 124),
# COMPILE-FAIL (no `test result:` line; a crash reads the same, D237 judge review J1, carried as a label),
# INCOMPLETE (no file, no rc line as the LAST line, or more than one result line), RC-MISMATCH (the rc and
# the FAILED lines disagree), RC-<n>.
#
# The self-test's expectations are LITERAL (review 2 H3): it carries its own copy of the registration and
# requires KILLERS and BASE_REASONS to equal it, plants from it, and requires each registered reason to be
# in its own test's body in the subject's test file. So an edit to either table is caught.
#
# src/ is restored from GIT on every exit (an EXIT trap; never from a copy; `--no-overlay`). Every cargo
# command runs in the background and is waited on, so a TERM, INT, HUP or QUIT reaches the traps at once and
# stops the child first. `waited` refuses to run inside `$(...)`. A run started in the background from a
# non-interactive shell has INT and QUIT ignored (POSIX): stop it with TERM or HUP.
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
waited() { # the command's rc. REFUSES in a subshell (D237 judge review J4): there `child` is set in
  # the subshell, the script's traps see it empty, and a TERM waits for the command or orphans it.
  # `$(exec sh -c 'echo "$PPID"')` is the pid of the shell running this line; `BASHPID` would say
  # the same, but /bin/bash 3.2 has none.
  local rc me
  me=$(exec sh -c 'echo "$PPID"')
  if [ "$me" != "$$" ]; then
    echo "REFUSED: waited ran in a subshell (pid $me, script $$), where no trap can stop its child" >&2
    exit 2
  fi
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
  local rc args
  # Review 2 G3: `target_args` runs in `$(...)`, so its `exit 2` leaves only that subshell. Its status is
  # checked here, or an unknown target would run `cargo test` with no target: the whole suite.
  args=$(target_args "$2") || exit 2
  # shellcheck disable=SC2086
  waited timeout 1800 cargo test $args > "$OUT/$1.$2.out" 2>&1
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

# The lines of one test's `---- <name> stdout ----` block in a target's output: it ends at the next
# test's header or at cargo's second `failures:` list.
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

# The control: `clean`, or `VOID (why)`. Three separate gates, each with its own self-test case: the
# file states, no FAILED line, and the pre-registered passed counts (one case per target's count).
control_verdict() { # $1 label
  local st f
  st=$(arm_state "$1" $TARGETS)
  [ "$st" = OK ] || { echo "VOID ($st)"; return; }
  f=$(failed_names "$1" $TARGETS)
  [ -z "$f" ] || { echo "VOID (a test FAILED: $(echo $f))"; return; }
  if [ "$(passed_in "$1" d257)" != 7 ] || [ "$(passed_in "$1" alter)" != 16 ]; then
    echo "VOID (passed d257=$(passed_in "$1" d257) alter=$(passed_in "$1" alter); expected 7/16)"
    return
  fi
  echo clean
}

killers_of() { # $1 mutant: sets req and opt; returns 1 if it has no row
  local k kname kreq kopt
  req=""; opt=""
  for k in "${KILLERS[@]}"; do
    IFS='|' read -r kname kreq kopt <<< "$k"
    if [ "$kname" = "$1" ]; then req=$kreq; opt=$kopt; return 0; fi
  done
  return 1
}

# ---- The run: every arm, in order. Its side effects go through the four functions just below, which the
# self-test replaces with stubs to check the run's own verdicts, counting and exit status (review 2 Jab).
checkout_src() { git checkout --no-overlay "$1" -- src/; } # $1 sha
apply_mutant() { timeout 60 python3 bench/d257/mutants.py apply "$1"; } # $1 mutant
record_diff() { git diff -- src/ > "$OUT/$1.diff"; } # $1 mutant
restore() { # $1 what was just run
  git checkout --no-overlay "$SUBJECT_SHA" -- src/
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after $1" >&2; exit 3; }
}

run_arms() { # prints the summary; status 0 = every arm as registered, 1 = not, 2 = the control VOID
  local bad=0 t v k name req opt
  echo "== control: $SUBJECT_SHA"
  for t in $TARGETS; do run_target control "$t"; done
  v=$(control_verdict control)
  echo "control: $v" | tee "$OUT/summary.txt"
  [ "$v" = clean ] || return 2

  echo "== base: src/ at $BASE_SHA"
  checkout_src "$BASE_SHA"
  for t in $TARGETS; do run_target base "$t"; done
  restore base
  v=$(base_verdict base)
  echo "base: $v" | tee -a "$OUT/summary.txt"
  as_registered "$v" || bad=$((bad + 1)) # the base arm counts like every other

  for k in "${KILLERS[@]}"; do
    IFS='|' read -r name req opt <<< "$k"
    echo "== $name"
    if ! apply_mutant "$name"; then
      echo "$name: NOT APPLIED" | tee -a "$OUT/summary.txt"
      bad=$((bad + 1))
      continue
    fi
    record_diff "$name"
    for t in $TARGETS; do run_target "$name" "$t"; done
    restore "$name"
    v=$(verdict "$name" "$req" "$opt" $TARGETS)
    echo "$name: $v" | tee -a "$OUT/summary.txt"
    as_registered "$v" || bad=$((bad + 1)) # this mutant counts
  done

  echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
  [ "$bad" -eq 0 ]
}

# ---- --selftest: planted outputs, in a temporary directory. No cargo, no git writes. ----
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
# The verdict's FIRST WORD must be exactly the expected one, and the rest of the expectation a prefix of
# the rest: `SURVIVED` does not accept `SURVIVED-AS-REGISTERED`, nor `MISMATCH` `MISMATCH-REASON` (K2).
expect() { # $1 case, $2 expected, $3 actual
  if [ "${3%% *}" = "${2%% *}" ] && case "$3" in "$2"*) true ;; *) false ;; esac; then
    echo "self-test PASS  $1: $3"
  else
    echo "self-test FAIL  $1: expected '$2', got '$3'"; st_bad=$((st_bad + 1))
  fi
}
expect_not_registered() { # $1 case, $2 actual verdict: anything but an as-registered verdict
  if as_registered "$2"; then echo "self-test FAIL  $1: got '$2', which counts as registered"; st_bad=$((st_bad + 1))
  else echo "self-test PASS  $1: $2"; fi
}
# Plant the given targets' outputs from `target::test` names that FAILED (`target::*` plants one test of
# that target); a target with none gets a clean file.
plant_set() { # $1 label, then names
  local label=$1 t n list
  shift
  for t in $TARGETS; do
    list=()
    for n in "$@"; do
      case "$n" in "$t::*") list+=("a_planted_${t}_test|planted") ;; "$t::"*) list+=("${n#*::}|planted") ;; esac
    done
    if [ ${#list[@]} -eq 0 ]; then ok_file "$label" "$t" 5; else fail_file "$label" "$t" 5 "${list[@]}"; fi
  done
}
# One test's `fn` body in the SUBJECT's test file (a read of a blob, never a write).
test_body() { # $1 test name
  git show "$SUBJECT_SHA:tests/d257_insert_bound.rs" 2>/dev/null | awk -v f="fn $1(" '
    index($0, f) == 1 { on = 1; print; next }
    on && (/^#\[test\]/ || /^fn /) { exit }
    on { print }'
}

# THE LITERAL REGISTRATION (lane §3, §3.2, §3.4): written out, NOT read from KILLERS or BASE_REASONS, so
# an edit to either table is caught (review 2 H3).
LIT_KILLERS=(
  "MA_no_refusal|d257::an_oversize_insert_is_refused_before_any_page_is_allocated d257::two_thousand_refused_inserts_leave_the_pool_and_the_file_as_they_were d257::the_space_arithmetic_refuses_rather_than_wrapping_at_the_u16_boundary d257::an_oversize_sql_insert_changes_no_page_of_the_table d257::an_oversize_sql_update_writes_nothing_to_the_time_travel_heap alter::an_unlogged_update_too_large_for_any_page_refuses_instead_of_deleting_the_row|alter::*"
  "MB_off_by_one|d257::a_tuple_of_exactly_the_limit_is_still_accepted d257::the_space_arithmetic_refuses_rather_than_wrapping_at_the_u16_boundary|alter::*"
  "MC_wrapping_cast||"
  "MD_no_update_precheck|d257::an_oversize_sql_update_writes_nothing_to_the_time_travel_heap|"
  "ME_insert_into_hand_unpin|d257::an_insert_refused_inside_its_page_releases_the_pin|"
  "MF_old_error|d257::an_oversize_insert_is_refused_before_any_page_is_allocated d257::two_thousand_refused_inserts_leave_the_pool_and_the_file_as_they_were d257::the_space_arithmetic_refuses_rather_than_wrapping_at_the_u16_boundary d257::an_oversize_sql_insert_changes_no_page_of_the_table d257::an_oversize_sql_update_writes_nothing_to_the_time_travel_heap|"
  "MG_narrowed_refusal|d257::the_space_arithmetic_refuses_rather_than_wrapping_at_the_u16_boundary|"
)
LIT_REASONS=(
  "an_oversize_insert_is_refused_before_any_page_is_allocated|changed the heap's pages"
  "two_thousand_refused_inserts_leave_the_pool_and_the_file_as_they_were|refused inserts changed the file"
  "the_space_arithmetic_refuses_rather_than_wrapping_at_the_u16_boundary|must be refused with a Constraint naming the limit"
  "an_oversize_sql_insert_changes_no_page_of_the_table|changed t's pages"
  "an_oversize_sql_update_writes_nothing_to_the_time_travel_heap|changed the time-travel heap's pages"
)
norm_row() { # "name|req|opt" with each list sorted
  local n r o
  IFS='|' read -r n r o <<< "$1"
  printf '%s|%s|%s' "$n" "$(words "$r" | tr '\n' ' ')" "$(words "$o" | tr '\n' ' ')"
}
lit_row_of() { # $1 mutant: sets lreq and lopt from LIT_KILLERS
  local k n
  lreq=""; lopt=""
  for k in "${LIT_KILLERS[@]}"; do
    IFS='|' read -r n lreq lopt <<< "$k"
    [ "$n" = "$1" ] && return 0
  done
  lreq=""; lopt=""
  return 1
}
# The planted base output: each literal reason in its own test's block; $1 swaps T3's for another.
plant_base() { # $1 label, $2 "reason" to give T3 another reason
  local r entries=()
  for r in "${LIT_REASONS[@]}"; do
    if [ "${2:-}" = reason ] && [ "${r%%|*}" = two_thousand_refused_inserts_leave_the_pool_and_the_file_as_they_were ]; then
      entries+=("${r%%|*}|called \`Result::unwrap()\` on an \`Err\` value: NotEnoughSpace")
    else
      entries+=("${r%%|*}|assertion failed: ${r#*|}: planted")
    fi
  done
  fail_file "$1" d257 2 "${entries[@]}"
  ok_file "$1" alter 16
}
# The stubbed arms of a run: every output planted from the LITERAL registration, per scenario.
plant_arm() { # $1 label, $2 target; $SCENARIO = all | basereason | mgsurvives | void
  local n list=()
  case "$1" in
    control)
      if [ "$2" = d257 ] && [ "$SCENARIO" = void ]; then ok_file control d257 6
      elif [ "$2" = d257 ]; then ok_file control d257 7
      else ok_file control alter 16; fi ;;
    base)
      if [ "$2" = d257 ]; then
        if [ "$SCENARIO" = basereason ]; then plant_base base reason; else plant_base base; fi
      fi ;;
    *)
      lit_row_of "$1" || { plant "$1" "$2" none "no literal row for $1"; return; }
      if ! { [ "$SCENARIO" = mgsurvives ] && [ "$1" = MG_narrowed_refusal ]; }; then
        for n in $lreq; do case "$n" in "$2::"*) list+=("${n#*::}|planted") ;; esac; done
      fi
      if [ ${#list[@]} -eq 0 ]; then ok_file "$1" "$2" 5; else fail_file "$1" "$2" 5 "${list[@]}"; fi ;;
  esac
}
sc_run() { # $1 scenario: run_arms with its side effects stubbed; prints its output, returns its status
  ( SCENARIO=$1
    checkout_src() { :; }
    apply_mutant() { :; }
    record_diff() { :; }
    restore() { :; }
    run_target() { plant_arm "$1" "$2"; }
    run_arms )
}

self_test() {
  local keep=$OUT st_bad=0 tmp r rc name t k n lreq lopt out wanted body
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/d257-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp

  # `expect` itself: a wrong first word must fail it (K2).
  ( st_bad=0; expect meta SURVIVED SURVIVED-AS-REGISTERED > /dev/null; [ "$st_bad" = 1 ] ) \
    && ( st_bad=0; expect meta MISMATCH "MISMATCH-REASON (x:other-reason)" > /dev/null; [ "$st_bad" = 1 ] ) \
    && echo "self-test PASS  expect compares the verdict's first word exactly" \
    || { echo "self-test FAIL  expect accepted a verdict by prefix"; st_bad=$((st_bad + 1)); }

  ok_file clean d257 7; ok_file clean alter 16
  expect "nothing fails, nothing required" SURVIVED-AS-REGISTERED "$(verdict clean "" "" $TARGETS)"
  expect "nothing fails, a killer required" SURVIVED "$(verdict clean "d257::a" "" $TARGETS)"
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

  # Every file state.
  for t in $TARGETS; do plant cf "$t" 101 "error[E0308]: mismatched types" "error: could not compile \`ferrodb\`"; done
  expect "compile failure" "COMPILE-FAIL (d257)" "$(verdict cf "d257::a" "" $TARGETS)"
  ok_file to alter 16; plant to d257 124 "running 7 tests" "test $T3 has been running for over 60 seconds"
  expect "timeout" "TIMEOUT (d257)" "$(verdict to "d257::a" "" $TARGETS)"
  ok_file gone d257 7
  expect "a target with no output file" "INCOMPLETE (alter: no output file)" "$(verdict gone "d257::a" "" $TARGETS)"
  ok_file norc alter 16; plant norc d257 none "running 7 tests" "test a ... FAILED"
  expect "a target with no rc line" "INCOMPLETE (d257: no rc line)" "$(verdict norc "d257::a" "" $TARGETS)"
  ok_file late alter 16
  plant late d257 none "test a ... FAILED" "test result: FAILED. 6 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out" \
    "rc=101" "a line written after the rc line"
  expect "a line after the rc line (Jj)" "INCOMPLETE (d257: no rc line)" "$(verdict late "d257::a" "" $TARGETS)"
  ok_file rcm alter 16; plant rcm d257 0 "test a ... FAILED" "test result: FAILED. 6 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "rc=0 with a FAILED line" "RC-MISMATCH (d257: rc=0 with FAILED lines)" "$(verdict rcm "d257::a" "" $TARGETS)"
  ok_file two alter 16
  plant two d257 0 "test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out" \
    "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "two result lines" "INCOMPLETE (d257: 2 result lines)" "$(verdict two "d257::a" "" $TARGETS)"
  ok_file rc2 alter 16; plant rc2 d257 2 "test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "an rc cargo does not use" "RC-2 (d257)" "$(verdict rc2 "d257::a" "" $TARGETS)"
  ok_file r101 alter 16; plant r101 d257 101 "test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "rc=101 with nothing FAILED" "RC-MISMATCH (d257: rc=101 and nothing FAILED)" "$(verdict r101 "d257::a" "" $TARGETS)"
  ok_file rclast alter 16; plant rclast d257 124 "running 7 tests" "rc=0" "test $T3 has been running for over 60 seconds"
  expect "the rc is read from the last line" "TIMEOUT (d257)" "$(verdict rclast "d257::a" "" $TARGETS)"

  # The control, one gate at a time. Each VOID case passes the other gates.
  ok_file ctl d257 7; ok_file ctl alter 16
  expect "control, clean" clean "$(control_verdict ctl)"
  ok_file ctlfail alter 16; fail_file ctlfail d257 7 "$T2|planted"
  expect "control with a FAILED test and the counts still 7/16" "VOID (a test FAILED" "$(control_verdict ctlfail)"
  ok_file ctlcount d257 6; ok_file ctlcount alter 16
  expect "control with a short d257 count and nothing FAILED" "VOID (passed d257=6" "$(control_verdict ctlcount)"
  ok_file ctlcountalter d257 7; ok_file ctlcountalter alter 15
  expect "control with a short alter count and nothing FAILED" "VOID (passed d257=7 alter=15" "$(control_verdict ctlcountalter)"
  ok_file ctlrc alter 16; plant ctlrc d257 101 "test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "control with rc=101, nothing FAILED, counts 7/16" "VOID (RC-MISMATCH" "$(control_verdict ctlrc)"
  for t in $TARGETS; do plant ctlcf "$t" 101 "error: could not compile \`ferrodb\`"; done
  expect "control that did not compile" "VOID (COMPILE-FAIL" "$(control_verdict ctlcf)"

  # The literal registration against the tables (H3).
  if [ "${#KILLERS[@]}" = "${#LIT_KILLERS[@]}" ]; then echo "self-test PASS  KILLERS has the literal number of rows"
  else echo "self-test FAIL  KILLERS has ${#KILLERS[@]} rows, the registration ${#LIT_KILLERS[@]}"; st_bad=$((st_bad + 1)); fi
  for k in "${LIT_KILLERS[@]}"; do
    name=${k%%|*}
    if killers_of "$name" && [ "$(norm_row "$name|$req|$opt")" = "$(norm_row "$k")" ]; then
      echo "self-test PASS  KILLERS row $name equals the registration"
    else
      echo "self-test FAIL  KILLERS row $name differs from the registration: '$name|$req|$opt'"; st_bad=$((st_bad + 1))
    fi
  done
  wanted=""; for r in "${LIT_REASONS[@]}"; do wanted="$wanted d257::${r%%|*}"; done
  if [ "$(words "$BASE_REQUIRED" | tr '\n' ' ')" = "$(words "$wanted" | tr '\n' ' ')" ]; then
    echo "self-test PASS  BASE_REQUIRED equals the registration"
  else echo "self-test FAIL  BASE_REQUIRED differs from the registration"; st_bad=$((st_bad + 1)); fi
  if [ "${#BASE_REASONS[@]}" = "${#LIT_REASONS[@]}" ]; then
    n=0
    for r in "${LIT_REASONS[@]}"; do
      if [ "${BASE_REASONS[$n]}" = "$r" ]; then echo "self-test PASS  BASE_REASONS[$n] equals the registration"
      else echo "self-test FAIL  BASE_REASONS[$n] is '${BASE_REASONS[$n]}', registered '$r'"; st_bad=$((st_bad + 1)); fi
      n=$((n + 1))
    done
  else echo "self-test FAIL  BASE_REASONS has ${#BASE_REASONS[@]} rows, the registration ${#LIT_REASONS[@]}"; st_bad=$((st_bad + 1)); fi
  # Each registered reason is printed by an assertion in its own test's body at the subject.
  for r in "${LIT_REASONS[@]}"; do
    body=$(test_body "${r%%|*}")
    if [ -n "$body" ] && printf '%s\n' "$body" | grep -qF -- "${r#*|}"; then
      echo "self-test PASS  '${r#*|}' is in ${r%%|*}'s body at $SUBJECT_SHA"
    else
      echo "self-test FAIL  '${r#*|}' is not in ${r%%|*}'s body at $SUBJECT_SHA"; st_bad=$((st_bad + 1))
    fi
  done

  # Every mutant, planted from the LITERAL registration and judged through KILLERS: its full set, the full
  # set minus each required killer, optional-only, nothing, an optional added, and an unregistered one.
  for k in "${LIT_KILLERS[@]}"; do
    name=${k%%|*}
    lit_row_of "$name"
    if ! killers_of "$name"; then echo "self-test FAIL  $name has no KILLERS row"; st_bad=$((st_bad + 1)); continue; fi
    if [ -z "$lreq" ]; then
      plant_set "$name.none"
      expect "$name (registered equivalent), nothing FAILED" SURVIVED-AS-REGISTERED "$(verdict "$name.none" "$req" "$opt" $TARGETS)"
      plant_set "$name.any" "d257::$T1"
      expect_not_registered "$name (registered equivalent), a test FAILED" "$(verdict "$name.any" "$req" "$opt" $TARGETS)"
      continue
    fi
    plant_set "$name.full" $lreq
    expect "$name, its full registered set" KILLED-AS-REGISTERED "$(verdict "$name.full" "$req" "$opt" $TARGETS)"
    for n in $lreq; do
      plant_set "$name.less" $(printf '%s\n' $lreq | grep -vxF -- "$n")
      expect_not_registered "$name, all but $n" "$(verdict "$name.less" "$req" "$opt" $TARGETS)"
    done
    if [ -n "$lopt" ]; then
      plant_set "$name.opt" $lopt
      expect_not_registered "$name, only its optional killers" "$(verdict "$name.opt" "$req" "$opt" $TARGETS)"
      plant_set "$name.plusopt" $lreq $lopt
      expect "$name, its full set plus an optional one" KILLED-AS-REGISTERED "$(verdict "$name.plusopt" "$req" "$opt" $TARGETS)"
    fi
    plant_set "$name.nothing"
    expect "$name, nothing FAILED" SURVIVED "$(verdict "$name.nothing" "$req" "$opt" $TARGETS)"
    for t in $TARGETS; do
      covered "$t::not_a_registered_killer" "$lreq $lopt" && continue
      plant_set "$name.extra" $lreq "$t::not_a_registered_killer"
      expect "$name, its full set plus an unregistered $t test" MISMATCH "$(verdict "$name.extra" "$req" "$opt" $TARGETS)"
    done
  done

  # The base arm (R6a), from the LITERAL reasons: as registered; T3 for another reason; T5's block missing;
  # T6's reason only in ANOTHER test's block (H1); T1's reason only in the NEXT block (H1: a block ends at
  # the next test's header); and the five plus T2 (Jo).
  plant_base base
  expect "base arm, every reason as registered" KILLED-AS-REGISTERED "$(base_verdict base)"
  plant_base basewrong reason
  expect "base arm, T3 failed for another reason" "MISMATCH-REASON ($T3:other-reason)" "$(base_verdict basewrong)"
  plant_base basenoblock
  sed -i.bak "/^---- $T5 stdout ----\$/,/^\$/d" "$(tfile basenoblock d257)"; rm -f "$(tfile basenoblock d257).bak"
  expect "base arm, T5's block missing" "MISMATCH-REASON ($T5:no-block)" "$(base_verdict basenoblock)"
  fail_file basecross d257 2 \
    "$T1|assertion failed: changed the heap's pages"$'\n'"and changed the time-travel heap's pages" \
    "$T3|assertion failed: refused inserts changed the file" \
    "$T4|assertion failed: must be refused with a Constraint naming the limit" \
    "$T5|assertion failed: changed t's pages" \
    "$T6|assertion failed: some other reason entirely"
  ok_file basecross alter 16
  expect "base arm, T6's reason only in T1's block (H1)" "MISMATCH-REASON ($T6:other-reason)" "$(base_verdict basecross)"
  fail_file basenext d257 2 \
    "$T1|assertion failed: some other reason entirely" \
    "$T3|assertion failed: refused inserts changed the file"$'\n'"and changed the heap's pages" \
    "$T4|assertion failed: must be refused with a Constraint naming the limit" \
    "$T5|assertion failed: changed t's pages" \
    "$T6|assertion failed: changed the time-travel heap's pages"
  ok_file basenext alter 16
  expect "base arm, T1's reason only in the NEXT block (H1)" "MISMATCH-REASON ($T1:other-reason)" "$(base_verdict basenext)"
  plant_base baseextra
  fail_file baseextra d257 1 \
    "$T1|assertion failed: changed the heap's pages" \
    "$T2|planted" \
    "$T3|assertion failed: refused inserts changed the file" \
    "$T4|assertion failed: must be refused with a Constraint naming the limit" \
    "$T5|assertion failed: changed t's pages" \
    "$T6|assertion failed: changed the time-travel heap's pages"
  expect "base arm, the five plus T2 (Jo)" MISMATCH "$(base_verdict baseextra)"

  # Only KILLED-AS-REGISTERED and SURVIVED-AS-REGISTERED count as registered (Jp).
  for r in "SURVIVED" "MISMATCH (missing: x; unexpected:)" "MISMATCH-REASON (x:other-reason)" "COMPILE-FAIL (d257)" "VOID (x)"; do
    expect_not_registered "as_registered refuses '$r'" "$r"
  done

  # The run's own verdicts, counting and status, with its side effects stubbed (Jab).
  out=$(sc_run all 2>&1); rc=$?
  if [ "$rc" = 0 ] && printf '%s\n' "$out" | grep -qxF "not-as-registered=0" \
     && [ "$(printf '%s\n' "$out" | grep -c ': KILLED-AS-REGISTERED')" = 7 ] \
     && printf '%s\n' "$out" | grep -qxF "MC_wrapping_cast: SURVIVED-AS-REGISTERED"; then
    echo "self-test PASS  a run with every arm as registered exits 0"
  else echo "self-test FAIL  a run with every arm as registered: rc=$rc"; st_bad=$((st_bad + 1)); fi
  out=$(sc_run basereason 2>&1); rc=$?
  if [ "$rc" = 1 ] && printf '%s\n' "$out" | grep -qxF "not-as-registered=1" \
     && printf '%s\n' "$out" | grep -qxF "base: MISMATCH-REASON ($T3:other-reason)"; then
    echo "self-test PASS  a base arm failing for another reason makes the run exit 1"
  else echo "self-test FAIL  a base arm failing for another reason: rc=$rc"; st_bad=$((st_bad + 1)); fi
  out=$(sc_run mgsurvives 2>&1); rc=$?
  if [ "$rc" = 1 ] && printf '%s\n' "$out" | grep -qxF "not-as-registered=1" \
     && printf '%s\n' "$out" | grep -qxF "MG_narrowed_refusal: SURVIVED"; then
    echo "self-test PASS  a surviving mutant makes the run exit 1"
  else echo "self-test FAIL  a surviving mutant: rc=$rc"; st_bad=$((st_bad + 1)); fi
  out=$(sc_run void 2>&1); rc=$?
  if [ "$rc" = 2 ] && printf '%s\n' "$out" | grep -qxF "control: VOID (passed d257=6 alter=16; expected 7/16)" \
     && ! printf '%s\n' "$out" | grep -q "^== base"; then
    echo "self-test PASS  a VOID control exits 2 before any other arm"
  else echo "self-test FAIL  a VOID control: rc=$rc"; st_bad=$((st_bad + 1)); fi

  # `waited` refuses inside `$(...)`, where no trap could stop its child.
  r=$(waited true 2>&1)
  rc=$?
  case "$rc/$r" in
    2/REFUSED:*subshell*) echo "self-test PASS  waited refuses in a subshell" ;;
    *) echo "self-test FAIL  waited in a subshell: rc=$rc, '$r'"; st_bad=$((st_bad + 1)) ;;
  esac
  waited true
  rc=$?
  [ "$rc" = 0 ] && echo "self-test PASS  waited runs in the script's own shell" \
    || { echo "self-test FAIL  waited in the script's own shell: rc=$rc"; st_bad=$((st_bad + 1)); }

  # G3: an unknown target exits 2 and runs nothing.
  r=$(waited() { echo "$*" >> "$OUT/g3.calls"; return 0; }; run_target g3 bogus 2>&1)
  rc=$?
  if [ "$rc" = 2 ] && [ ! -e "$OUT/g3.calls" ] && [ ! -e "$(tfile g3 bogus)" ]; then
    echo "self-test PASS  an unknown target exits 2 and runs nothing"
  else echo "self-test FAIL  an unknown target: rc=$rc, calls: $(cat "$OUT/g3.calls" 2>/dev/null)"; st_bad=$((st_bad + 1)); fi

  # Registration: every mutant mutants.py lists has a KILLERS row, and every row names a mutant.
  for name in $(python3 bench/d257/mutants.py list 2>/dev/null || echo UNLISTED); do
    killers_of "$name" || { echo "self-test FAIL  $name has no KILLERS row"; st_bad=$((st_bad + 1)); }
  done
  for k in "${KILLERS[@]}"; do
    python3 bench/d257/mutants.py list 2>/dev/null | grep -qx "${k%%|*}" \
      || { echo "self-test FAIL  KILLERS row ${k%%|*} names no mutant"; st_bad=$((st_bad + 1)); }
  done

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
  git checkout --no-overlay "$SUBJECT_SHA" -- src/ 2>/dev/null
  git diff --quiet "$SUBJECT_SHA" -- src/ tests/ ||
    echo "ON EXIT: src/ or tests/ still differ from $SUBJECT_SHA; restore with: git checkout --no-overlay $SUBJECT_SHA -- src/" >&2
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
trap 'on_signal 129' HUP  # D237 judge review J5
trap 'on_signal 131' QUIT

run_arms
rc=$?
git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
exit "$rc"
