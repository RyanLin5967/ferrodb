#!/usr/bin/env bash
# D260 + D262 fire-check. Pre-registration: artie-research frontier/lane_d260_d262_new_page.md (§D260.2,
# §D262.2, and PREREG amendment 1 for D257 review 2's decisions). Fan work: it runs only when the lead
# releases the FAN-QUEUE row. Run from the worktree root at DEFAULT QoS (never taskpolicy -b). The shape is
# D257's (`d257-insert-bound` @ `d7b2335`); the differences are the targets, two base arms, and the mutants.
#
#   bash bench/d260_d262/firecheck.sh             the run (it runs the self-test first and refuses on a miss)
#   bash bench/d260_d262/firecheck.sh --selftest  the judge and the run's own counting on planted outputs
#                                                 only: no cargo, no git writes (it reads the subject's tests)
#   anything else                                 exits 2
#
# Arms, in run order (`run_arms`):
# - control:   SUBJECT_SHA, both targets: every file OK, nothing FAILED, 4 and 3 passed. Anything else VOIDS
#              the run: exit 2, before any other arm is built.
# - base-D260: src/ at bb6cde7 (before D260), tests at SUBJECT_SHA. d260 must FAIL exactly T1 T2 T3, each
#              with its registered reason in its own `---- <name> stdout ----` block (which ends at the next
#              test's header).
# - base-D262: src/ at 8ae086d (D260 fixed, D262 not). d262 must FAIL exactly R1 R2, each with its reason.
# - mutants:   `bench/d260_d262/mutants.py`, one at a time, both targets. KILLED-AS-REGISTERED when every
#              file is OK, every required killer FAILED, and nothing outside required + optional did
#              (`d262::*` = any test of that target).
# Every arm counts in not-as-registered; the run exits 0 only when every arm is as registered.
#
# The judge reads exactly one file per target, `$OUT/<label>.<target>.out`, named from the targets an arm
# ran and never globbed (D237 review 2, N2). Each file gets a state before any name is read: TIMEOUT (rc
# 124), COMPILE-FAIL (no `test result:` line; a crash reads the same, D237 judge review J1, a label),
# INCOMPLETE (no file, no rc line as the LAST line, or more than one result line), RC-MISMATCH, RC-<n>.
#
# The self-test's expectations are LITERAL (D257 review 2 H3): it carries its own copy of the registration,
# requires KILLERS and both base-reason tables to equal it, plants from it, and requires each registered
# reason to be in its own test's body in the subject's test file.
#
# src/ is restored from GIT on every exit (an EXIT trap; never from a copy; `--no-overlay`). Every cargo
# command runs in the background and is waited on, so a TERM, INT, HUP or QUIT reaches the traps at once and
# stops the child first. `waited` refuses to run inside `$(...)`. A run started in the background from a
# non-interactive shell has INT and QUIT ignored (POSIX): stop it with TERM or HUP.
#
# Blind spots, stated: two targets, not the whole suite (the per-target suite is the FAN-QUEUE row's own
# step); the judge reads cargo's own lines, so a test printing a line of exactly that shape would be
# misread (none is run with --nocapture); reasons are checked on the base arms only.
set -u
set -f # no globbing: killer lists carry a literal `*`
unset RUSTFLAGS

SUBJECT_SHA=d2a2921
BASE_D260_SHA=bb6cde7
BASE_D262_SHA=8ae086d
OUT=bench/d260_d262/firecheck
TARGETS="d260 d262"
SCRIPT=$0

T1=heap_new_leaves_a_second_holders_pin_on_its_directory_page
T2=add_empty_page_leaves_a_second_holders_pin_on_the_page_it_adds
T3=create_with_header_leaves_a_second_holders_pin_on_its_header_page
T4=new_page_takes_back_its_own_pin_and_only_its_own
R1=a_data_page_the_directory_cannot_list_is_given_back
R2=a_page_new_page_cannot_load_is_given_back
R3=a_page_that_is_linked_stays_allocated_and_listed

# name|required killers|optional killers
KILLERS=(
  "NA_heap_new_extra_unpin|d260::$T1|"
  "NB_add_empty_page_extra_unpin|d260::$T2|"
  "NC_create_with_header_extra_unpin|d260::$T3|"
  "ND_new_page_returns_pinned|d260::$T1 d260::$T2 d260::$T3 d260::$T4|d262::*"
  "OA_add_empty_page_keeps_the_orphan|d262::$R1|"
  "OB_new_page_keeps_the_orphan|d262::$R2|"
  "OC_add_empty_page_frees_a_listed_page|d262::$R3|d260::* d262::*"
)
# test|registered reason: each base arm's required set is exactly the tests listed here.
BASE_D260_REASONS=(
  "$T1|HeapFileManager::new took the second holder's pin"
  "$T2|add_empty_page took the second holder's pin"
  "$T3|create_with_header took the second holder's pin"
)
BASE_D262_REASONS=(
  "$R1|is still allocated after its directory entry could not be written"
  "$R2|is still allocated after new_page failed to load it"
)

# ---- Running. A command runs in the background and is waited on; see the header. ----
child=""
waited() { # the command's rc. REFUSES in a subshell: there `child` is set in the subshell, the
  # script's traps see it empty, and a TERM waits for the command or orphans it.
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
    d260) echo "--test d260_new_page_is_returned_unpinned" ;;
    d262) echo "--test d262_an_unlinked_page_is_given_back" ;;
    *) echo "unknown target $1" >&2; exit 2 ;;
  esac
}

run_target() { # $1 label, $2 target
  local rc args
  # D257 review 2 G3: `target_args` runs in `$(...)`, so its `exit 2` leaves only that subshell. Its
  # status is checked here, or an unknown target would run `cargo test` with no target: the whole suite.
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

# A base arm: the name verdict over its one target, then each registered reason in its own test's block.
# The required set is exactly the tests given, so a registration cannot list a reason for a test it
# does not require, or require a test it gives no reason for.
base_verdict() { # $1 label, $2 target, then "test|reason" for every test that must FAIL there
  local label=$1 t=$2 r test reason block req="" miss="" v
  shift 2
  for r in "$@"; do req="$req $t::${r%%|*}"; done
  v=$(verdict "$label" "$req" "" "$t")
  case "$v" in KILLED-AS-REGISTERED*) ;; *) echo "$v"; return ;; esac
  for r in "$@"; do
    test=${r%%|*}; reason=${r#*|}
    block=$(failure_block "$label" "$t" "$test")
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
  if [ "$(passed_in "$1" d260)" != 4 ] || [ "$(passed_in "$1" d262)" != 3 ]; then
    echo "VOID (passed d260=$(passed_in "$1" d260) d262=$(passed_in "$1" d262); expected 4/3)"
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
# self-test replaces with stubs to check the run's own verdicts, counting and exit status.
checkout_src() { git checkout --no-overlay "$1" -- src/; } # $1 sha
apply_mutant() { timeout 60 python3 bench/d260_d262/mutants.py apply "$1"; } # $1 mutant
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

  echo "== base-D260: src/ at $BASE_D260_SHA"
  checkout_src "$BASE_D260_SHA"
  run_target base_d260 d260
  restore base-D260
  v=$(base_verdict base_d260 d260 "${BASE_D260_REASONS[@]}")
  echo "base-D260: $v" | tee -a "$OUT/summary.txt"
  as_registered "$v" || bad=$((bad + 1)) # base-D260 counts

  echo "== base-D262: src/ at $BASE_D262_SHA"
  checkout_src "$BASE_D262_SHA"
  run_target base_d262 d262
  restore base-D262
  v=$(base_verdict base_d262 d262 "${BASE_D262_REASONS[@]}")
  echo "base-D262: $v" | tee -a "$OUT/summary.txt"
  as_registered "$v" || bad=$((bad + 1)) # base-D262 counts

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
# the rest: `SURVIVED` does not accept `SURVIVED-AS-REGISTERED`, nor `MISMATCH` `MISMATCH-REASON`.
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
# Plant every target's output from `target::test` names that FAILED (`target::*` plants one test of that
# target); a target with none gets a clean file.
plant_set() { # $1 label, then names
  local label=$1 t n list
  shift
  for t in $TARGETS; do
    list=()
    for n in "$@"; do
      case "$n" in "$t::*") list+=("a_planted_${t}_test|planted") ;; "$t::"*) list+=("${n#*::}|planted") ;; esac
    done
    if [ ${#list[@]} -eq 0 ]; then ok_file "$label" "$t" 2; else fail_file "$label" "$t" 2 "${list[@]}"; fi
  done
}
# One test's `fn` body in the SUBJECT's test file for a target (a read of a blob, never a write).
test_body() { # $1 target, $2 test name
  local file
  case "$1" in
    d260) file=tests/d260_new_page_is_returned_unpinned.rs ;;
    d262) file=tests/d262_an_unlinked_page_is_given_back.rs ;;
    *) return 1 ;;
  esac
  git show "$SUBJECT_SHA:$file" 2>/dev/null | awk -v f="fn $2(" '
    index($0, f) == 1 { on = 1; print; next }
    on && (/^#\[test\]/ || /^fn /) { exit }
    on { print }'
}

# THE LITERAL REGISTRATION (lane §D260.2, §D262.2): written out, NOT read from KILLERS or the reason tables,
# so an edit to any of them is caught.
LIT_KILLERS=(
  "NA_heap_new_extra_unpin|d260::heap_new_leaves_a_second_holders_pin_on_its_directory_page|"
  "NB_add_empty_page_extra_unpin|d260::add_empty_page_leaves_a_second_holders_pin_on_the_page_it_adds|"
  "NC_create_with_header_extra_unpin|d260::create_with_header_leaves_a_second_holders_pin_on_its_header_page|"
  "ND_new_page_returns_pinned|d260::heap_new_leaves_a_second_holders_pin_on_its_directory_page d260::add_empty_page_leaves_a_second_holders_pin_on_the_page_it_adds d260::create_with_header_leaves_a_second_holders_pin_on_its_header_page d260::new_page_takes_back_its_own_pin_and_only_its_own|d262::*"
  "OA_add_empty_page_keeps_the_orphan|d262::a_data_page_the_directory_cannot_list_is_given_back|"
  "OB_new_page_keeps_the_orphan|d262::a_page_new_page_cannot_load_is_given_back|"
  "OC_add_empty_page_frees_a_listed_page|d262::a_page_that_is_linked_stays_allocated_and_listed|d260::* d262::*"
)
LIT_D260_REASONS=(
  "heap_new_leaves_a_second_holders_pin_on_its_directory_page|HeapFileManager::new took the second holder's pin"
  "add_empty_page_leaves_a_second_holders_pin_on_the_page_it_adds|add_empty_page took the second holder's pin"
  "create_with_header_leaves_a_second_holders_pin_on_its_header_page|create_with_header took the second holder's pin"
)
LIT_D262_REASONS=(
  "a_data_page_the_directory_cannot_list_is_given_back|is still allocated after its directory entry could not be written"
  "a_page_new_page_cannot_load_is_given_back|is still allocated after new_page failed to load it"
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
# A planted base output: each literal "test|reason" in its own test's block, with "other=<test>" giving
# that one test another reason.
plant_reasons() { # $1 label, $2 target, $3 test to give another reason (or ""), then "test|reason" ...
  local label=$1 t=$2 other=$3 r entries=()
  shift 3
  for r in "$@"; do
    if [ "${r%%|*}" = "$other" ]; then entries+=("${r%%|*}|premise: something else failed first")
    else entries+=("${r%%|*}|assertion failed: ${r#*|}: planted"); fi
  done
  fail_file "$label" "$t" 1 "${entries[@]}"
}
# The stubbed arms of a run: every output planted from the LITERAL registration, per scenario.
plant_arm() { # $1 label, $2 target; $SCENARIO = all | b60reason | b62reason | obsurvives | void
  local n list=() other=""
  case "$1" in
    control)
      if [ "$2" = d260 ] && [ "$SCENARIO" = void ]; then ok_file control d260 3
      elif [ "$2" = d260 ]; then ok_file control d260 4
      else ok_file control d262 3; fi ;;
    base_d260)
      [ "$SCENARIO" = b60reason ] && other=$T2
      plant_reasons base_d260 d260 "$other" "${LIT_D260_REASONS[@]}" ;;
    base_d262)
      [ "$SCENARIO" = b62reason ] && other=$R1
      plant_reasons base_d262 d262 "$other" "${LIT_D262_REASONS[@]}" ;;
    *)
      lit_row_of "$1" || { plant "$1" "$2" none "no literal row for $1"; return; }
      if ! { [ "$SCENARIO" = obsurvives ] && [ "$1" = OB_new_page_keeps_the_orphan ]; }; then
        for n in $lreq; do case "$n" in "$2::"*) list+=("${n#*::}|planted") ;; esac; done
      fi
      if [ ${#list[@]} -eq 0 ]; then ok_file "$1" "$2" 2; else fail_file "$1" "$2" 2 "${list[@]}"; fi ;;
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
# The base-arm cases for one arm: as registered, one wrong reason, a missing block, a reason only in
# ANOTHER test's block, a reason only in the NEXT block, and an extra failure.
base_cases() { # $1 arm name, $2 target, $3 extra test name, then the literal "test|reason" entries
  local arm=$1 t=$2 extra=$3 first second r e=()
  shift 3
  first=${1%%|*}; second=${2%%|*}
  plant_reasons "$arm.ok" "$t" "" "$@"
  expect "$arm, every reason as registered" KILLED-AS-REGISTERED "$(base_verdict "$arm.ok" "$t" "$@")"
  plant_reasons "$arm.wrong" "$t" "$second" "$@"
  expect "$arm, $second failed for another reason" "MISMATCH-REASON ($second:other-reason)" "$(base_verdict "$arm.wrong" "$t" "$@")"
  plant_reasons "$arm.noblock" "$t" "" "$@"
  sed -i.bak "/^---- $first stdout ----\$/,/^\$/d" "$(tfile "$arm.noblock" "$t")"; rm -f "$(tfile "$arm.noblock" "$t").bak"
  expect "$arm, $first's block missing" "MISMATCH-REASON ($first:no-block)" "$(base_verdict "$arm.noblock" "$t" "$@")"
  # The second test's reason only inside the FIRST test's block; its own block carries another.
  e=("$first|assertion failed: ${1#*|}"$'\n'"and ${2#*|}" "$second|assertion failed: some other reason entirely")
  shift 2; for r in "$@"; do e+=("${r%%|*}|assertion failed: ${r#*|}"); done; set -- "${e[@]}"
  fail_file "$arm.cross" "$t" 1 "$@"
  expect "$arm, $second's reason only in $first's block" "MISMATCH-REASON ($second:other-reason)" "$(base_verdict "$arm.cross" "$t" "${ARM_REASONS[@]}")"
  # The first test's reason only inside the NEXT test's block: a block ends at the next header.
  e=("$first|assertion failed: some other reason entirely" "$second|assertion failed: ${ARM_REASONS[1]#*|}"$'\n'"and ${ARM_REASONS[0]#*|}")
  for r in "${ARM_REASONS[@]:2}"; do e+=("${r%%|*}|assertion failed: ${r#*|}"); done
  fail_file "$arm.next" "$t" 1 "${e[@]}"
  expect "$arm, $first's reason only in the NEXT block" "MISMATCH-REASON ($first:other-reason)" "$(base_verdict "$arm.next" "$t" "${ARM_REASONS[@]}")"
  e=(); for r in "${ARM_REASONS[@]}"; do e+=("${r%%|*}|assertion failed: ${r#*|}"); done; e+=("$extra|planted")
  fail_file "$arm.extra" "$t" 0 "${e[@]}"
  expect "$arm, its tests plus $extra" MISMATCH "$(base_verdict "$arm.extra" "$t" "${ARM_REASONS[@]}")"
}

self_test() {
  local keep=$OUT st_bad=0 tmp r rc name t k n lreq lopt out wanted body i
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/d260-d262-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp

  # `expect` itself: a wrong first word must fail it.
  ( st_bad=0; expect meta SURVIVED SURVIVED-AS-REGISTERED > /dev/null; [ "$st_bad" = 1 ] ) \
    && ( st_bad=0; expect meta MISMATCH "MISMATCH-REASON (x:other-reason)" > /dev/null; [ "$st_bad" = 1 ] ) \
    && echo "self-test PASS  expect compares the verdict's first word exactly" \
    || { echo "self-test FAIL  expect accepted a verdict by prefix"; st_bad=$((st_bad + 1)); }

  ok_file clean d260 4; ok_file clean d262 3
  expect "nothing fails, nothing required" SURVIVED-AS-REGISTERED "$(verdict clean "" "" $TARGETS)"
  expect "nothing fails, a killer required" SURVIVED "$(verdict clean "d260::a" "" $TARGETS)"
  [ "$(passed_in clean d260)" = 4 ] && [ "$(passed_in clean d262)" = 3 ] && echo "self-test PASS  passed_in reads 4/3" \
    || { echo "self-test FAIL  passed_in read $(passed_in clean d260)/$(passed_in clean d262)"; st_bad=$((st_bad + 1)); }

  fail_file exact d260 2 "a|x" "b|y"; ok_file exact d262 3
  expect "exactly the required" KILLED-AS-REGISTERED "$(verdict exact "d260::a d260::b" "" $TARGETS)"
  expect "one required missing" MISMATCH "$(verdict exact "d260::a d260::b d260::c" "" $TARGETS)"
  expect "one failure unregistered" MISMATCH "$(verdict exact "d260::a" "" $TARGETS)"
  expect "the unregistered one optional" KILLED-AS-REGISTERED "$(verdict exact "d260::a" "d260::b" $TARGETS)"

  fail_file wild d260 3 "a|x"; fail_file wild d262 1 "z|x" "y|x"
  expect "d262::* covers any d262 failure" KILLED-AS-REGISTERED "$(verdict wild "d260::a" "d262::*" $TARGETS)"
  expect "without d262::*, d262 failures are unexpected" MISMATCH "$(verdict wild "d260::a" "" $TARGETS)"
  expect "d262::* does not cover d260" MISMATCH "$(verdict wild "d262::z d262::y" "d262::*" $TARGETS)"

  fail_file path d260 3 "tests::deep::a|x"; ok_file path d262 3
  expect "module path stripped" KILLED-AS-REGISTERED "$(verdict path "d260::a" "" $TARGETS)"

  # N2's plant: stray files that share the label, each with an unregistered FAILED line and no result.
  fail_file stray d260 3 "a|x"; ok_file stray d262 3
  for name in stray.diffstat stray.junk.txt stray.d260.out.txt stray.diff; do
    printf '%s\n' " src/buffer/buffer_pool.rs | 2 +-" "test stray_not_registered ... FAILED" > "$OUT/$name"
  done
  expect "stray files beside the outputs" KILLED-AS-REGISTERED "$(verdict stray "d260::a" "" $TARGETS)"

  # Every file state.
  for t in $TARGETS; do plant cf "$t" 101 "error[E0308]: mismatched types" "error: could not compile \`ferrodb\`"; done
  expect "compile failure" "COMPILE-FAIL (d260)" "$(verdict cf "d260::a" "" $TARGETS)"
  ok_file to d262 3; plant to d260 124 "running 4 tests" "test $T1 has been running for over 60 seconds"
  expect "timeout" "TIMEOUT (d260)" "$(verdict to "d260::a" "" $TARGETS)"
  ok_file gone d260 4
  expect "a target with no output file" "INCOMPLETE (d262: no output file)" "$(verdict gone "d260::a" "" $TARGETS)"
  ok_file norc d262 3; plant norc d260 none "running 4 tests" "test a ... FAILED"
  expect "a target with no rc line" "INCOMPLETE (d260: no rc line)" "$(verdict norc "d260::a" "" $TARGETS)"
  ok_file late d262 3
  plant late d260 none "test a ... FAILED" "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out" \
    "rc=101" "a line written after the rc line"
  expect "a line after the rc line" "INCOMPLETE (d260: no rc line)" "$(verdict late "d260::a" "" $TARGETS)"
  ok_file rcm d262 3; plant rcm d260 0 "test a ... FAILED" "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "rc=0 with a FAILED line" "RC-MISMATCH (d260: rc=0 with FAILED lines)" "$(verdict rcm "d260::a" "" $TARGETS)"
  ok_file two d262 3
  plant two d260 0 "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out" \
    "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "two result lines" "INCOMPLETE (d260: 2 result lines)" "$(verdict two "d260::a" "" $TARGETS)"
  ok_file rc2 d262 3; plant rc2 d260 2 "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "an rc cargo does not use" "RC-2 (d260)" "$(verdict rc2 "d260::a" "" $TARGETS)"
  ok_file r101 d262 3; plant r101 d260 101 "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "rc=101 with nothing FAILED" "RC-MISMATCH (d260: rc=101 and nothing FAILED)" "$(verdict r101 "d260::a" "" $TARGETS)"
  ok_file rclast d262 3; plant rclast d260 124 "running 4 tests" "rc=0" "test $T1 has been running for over 60 seconds"
  expect "the rc is read from the last line" "TIMEOUT (d260)" "$(verdict rclast "d260::a" "" $TARGETS)"

  # The control, one gate at a time. Each VOID case passes the other gates.
  ok_file ctl d260 4; ok_file ctl d262 3
  expect "control, clean" clean "$(control_verdict ctl)"
  ok_file ctlfail d262 3; fail_file ctlfail d260 4 "$T4|planted"
  expect "control with a FAILED test and the counts still 4/3" "VOID (a test FAILED" "$(control_verdict ctlfail)"
  ok_file ctlcount60 d260 3; ok_file ctlcount60 d262 3
  expect "control with a short d260 count and nothing FAILED" "VOID (passed d260=3 d262=3" "$(control_verdict ctlcount60)"
  ok_file ctlcount62 d260 4; ok_file ctlcount62 d262 2
  expect "control with a short d262 count and nothing FAILED" "VOID (passed d260=4 d262=2" "$(control_verdict ctlcount62)"
  ok_file ctlrc d262 3; plant ctlrc d260 101 "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  expect "control with rc=101, nothing FAILED, counts 4/3" "VOID (RC-MISMATCH" "$(control_verdict ctlrc)"
  for t in $TARGETS; do plant ctlcf "$t" 101 "error: could not compile \`ferrodb\`"; done
  expect "control that did not compile" "VOID (COMPILE-FAIL" "$(control_verdict ctlcf)"

  # The literal registration against the tables.
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
  if [ "${#BASE_D260_REASONS[@]}" = "${#LIT_D260_REASONS[@]}" ]; then
    i=0; for r in "${LIT_D260_REASONS[@]}"; do
      if [ "${BASE_D260_REASONS[$i]}" = "$r" ]; then echo "self-test PASS  BASE_D260_REASONS[$i] equals the registration"
      else echo "self-test FAIL  BASE_D260_REASONS[$i] is '${BASE_D260_REASONS[$i]}', registered '$r'"; st_bad=$((st_bad + 1)); fi
      i=$((i + 1)); done
  else echo "self-test FAIL  BASE_D260_REASONS has ${#BASE_D260_REASONS[@]} rows"; st_bad=$((st_bad + 1)); fi
  if [ "${#BASE_D262_REASONS[@]}" = "${#LIT_D262_REASONS[@]}" ]; then
    i=0; for r in "${LIT_D262_REASONS[@]}"; do
      if [ "${BASE_D262_REASONS[$i]}" = "$r" ]; then echo "self-test PASS  BASE_D262_REASONS[$i] equals the registration"
      else echo "self-test FAIL  BASE_D262_REASONS[$i] is '${BASE_D262_REASONS[$i]}', registered '$r'"; st_bad=$((st_bad + 1)); fi
      i=$((i + 1)); done
  else echo "self-test FAIL  BASE_D262_REASONS has ${#BASE_D262_REASONS[@]} rows"; st_bad=$((st_bad + 1)); fi
  # Each registered reason is printed by an assertion in its own test's body at the subject.
  for r in "${LIT_D260_REASONS[@]/#/d260|}" "${LIT_D262_REASONS[@]/#/d262|}"; do
    t=${r%%|*}; r=${r#*|}
    body=$(test_body "$t" "${r%%|*}")
    if [ -n "$body" ] && printf '%s\n' "$body" | grep -qF -- "${r#*|}"; then
      echo "self-test PASS  '${r#*|}' is in ${r%%|*}'s body at $SUBJECT_SHA"
    else
      echo "self-test FAIL  '${r#*|}' is not in ${r%%|*}'s body at $SUBJECT_SHA"; st_bad=$((st_bad + 1))
    fi
  done

  # Every mutant, planted from the LITERAL registration and judged through KILLERS: its full set, the full
  # set minus each required killer, optional-only, an optional added, nothing, and an unregistered one.
  for k in "${LIT_KILLERS[@]}"; do
    name=${k%%|*}
    lit_row_of "$name"
    if ! killers_of "$name"; then echo "self-test FAIL  $name has no KILLERS row"; st_bad=$((st_bad + 1)); continue; fi
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

  # The base arms, from the LITERAL reasons (and judged through the scripts' own tables for the as-run
  # verdict: the ok case passes the tables, which must equal the literals above).
  ARM_REASONS=("${LIT_D260_REASONS[@]}")
  base_cases base-D260 d260 "$T4" "${LIT_D260_REASONS[@]}"
  ARM_REASONS=("${LIT_D262_REASONS[@]}")
  base_cases base-D262 d262 "$R3" "${LIT_D262_REASONS[@]}"

  # Only KILLED-AS-REGISTERED and SURVIVED-AS-REGISTERED count as registered.
  for r in "SURVIVED" "MISMATCH (missing: x; unexpected:)" "MISMATCH-REASON (x:other-reason)" "COMPILE-FAIL (d260)" "VOID (x)"; do
    expect_not_registered "as_registered refuses '$r'" "$r"
  done

  # The run's own verdicts, counting and status, with its side effects stubbed.
  out=$(sc_run all 2>&1); rc=$?
  if [ "$rc" = 0 ] && printf '%s\n' "$out" | grep -qxF "not-as-registered=0" \
     && [ "$(printf '%s\n' "$out" | grep -c ': KILLED-AS-REGISTERED')" = 9 ]; then
    echo "self-test PASS  a run with every arm as registered exits 0"
  else echo "self-test FAIL  a run with every arm as registered: rc=$rc"; st_bad=$((st_bad + 1)); fi
  out=$(sc_run b60reason 2>&1); rc=$?
  if [ "$rc" = 1 ] && printf '%s\n' "$out" | grep -qxF "not-as-registered=1" \
     && printf '%s\n' "$out" | grep -qxF "base-D260: MISMATCH-REASON ($T2:other-reason)"; then
    echo "self-test PASS  base-D260 failing for another reason makes the run exit 1"
  else echo "self-test FAIL  base-D260 failing for another reason: rc=$rc"; st_bad=$((st_bad + 1)); fi
  out=$(sc_run b62reason 2>&1); rc=$?
  if [ "$rc" = 1 ] && printf '%s\n' "$out" | grep -qxF "not-as-registered=1" \
     && printf '%s\n' "$out" | grep -qxF "base-D262: MISMATCH-REASON ($R1:other-reason)"; then
    echo "self-test PASS  base-D262 failing for another reason makes the run exit 1"
  else echo "self-test FAIL  base-D262 failing for another reason: rc=$rc"; st_bad=$((st_bad + 1)); fi
  out=$(sc_run obsurvives 2>&1); rc=$?
  if [ "$rc" = 1 ] && printf '%s\n' "$out" | grep -qxF "not-as-registered=1" \
     && printf '%s\n' "$out" | grep -qxF "OB_new_page_keeps_the_orphan: SURVIVED"; then
    echo "self-test PASS  a surviving mutant makes the run exit 1"
  else echo "self-test FAIL  a surviving mutant: rc=$rc"; st_bad=$((st_bad + 1)); fi
  out=$(sc_run void 2>&1); rc=$?
  if [ "$rc" = 2 ] && printf '%s\n' "$out" | grep -qxF "control: VOID (passed d260=3 d262=3; expected 4/3)" \
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
  for name in $(python3 bench/d260_d262/mutants.py list 2>/dev/null || echo UNLISTED); do
    killers_of "$name" || { echo "self-test FAIL  $name has no KILLERS row"; st_bad=$((st_bad + 1)); }
  done
  for k in "${KILLERS[@]}"; do
    python3 bench/d260_d262/mutants.py list 2>/dev/null | grep -qx "${k%%|*}" \
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
if ! timeout 60 python3 bench/d260_d262/mutants.py check > /dev/null; then
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
trap 'on_signal 129' HUP
trap 'on_signal 131' QUIT

run_arms
rc=$?
git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
exit "$rc"
