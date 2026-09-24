#!/usr/bin/env bash
# D257 fire-check. Pre-registration: artie-research frontier/lane_d257_insert_bound.md §3 (and any
# amendment after it). Fan work: it runs only when the lead releases the FAN-QUEUE row. Run from the
# worktree root at DEFAULT QoS (never taskpolicy -b).
#
#   bash bench/d257/firecheck.sh             the run
#   bash bench/d257/firecheck.sh --selftest  plants synthetic outputs and checks every verdict the
#                                            run can reach; no cargo. Run it first.
#
# Arms:
# - base:    src/ at 17e4c26, tests at SUBJECT_SHA. d257_insert_bound must FAIL exactly T1 T3 T4
#            T5 T6; integration_alter_refusal_safety must pass (it is the base's own).
# - control: SUBJECT_SHA. Both targets pass, 7 and 16. A control that fails, or has no result
#            line, VOIDS the run: exit 2.
# - mutants: `bench/d257/mutants.py`, one at a time. Each must FAIL exactly its required killers,
#            plus any of its optional ones (`alter::*` = any test of that target), and nothing else.
#            MC is pre-registered as equivalent: its as-registered outcome is that nothing fails.
#
# The verdict reads exactly one file per target, `$OUT/<label>.<target>.out`, named from TARGETS and
# never globbed: D237 review 2 (N2) found its fire-check's glob reading a diffstat as a target output,
# which made every mutant COMPILE-FAIL. Nothing else in $OUT ends in `.out`.
#
# Blind spots, stated: two targets, not the whole suite (the per-target suite is the FAN-QUEUE row's
# own step). "NO-RESULT" means a target printed no `test result:` line: a compile error or a harness
# abort, which this script does not tell apart; the raw file does.
set -u
set -f # no globbing: killer lists carry a literal `*`
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=e11a3b1
BASE_SHA=17e4c26
OUT=bench/d257/firecheck
TARGETS="d257 alter"

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
)
BASE_REQUIRED="d257::$T1 d257::$T3 d257::$T4 d257::$T5 d257::$T6"

run_target() { # $1 label, $2 target
  case "$2" in
    d257)  timeout 1800 cargo test --test d257_insert_bound > "$OUT/$1.d257.out" 2>&1 ;;
    alter) timeout 1800 cargo test --test integration_alter_refusal_safety > "$OUT/$1.alter.out" 2>&1 ;;
    *) echo "unknown target $2" >&2; exit 2 ;;
  esac
  echo "rc=$?" >> "$OUT/$1.$2.out"
}

# Every test that FAILED under a label, as `<target>::<name>`, sorted, one per line.
failed_names() {
  local t
  for t in $TARGETS; do
    sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' "$OUT/$1.$t.out" 2>/dev/null | sed -E "s/.*:://; s/^/$t::/"
  done | sort -u
}

# The first target of a label with no `test result:` line, or nothing.
missing_result() {
  local t
  for t in $TARGETS; do
    grep -qE '^test result:' "$OUT/$1.$t.out" 2>/dev/null || { echo "$t"; return; }
  done
}

# "N" passed in one target's output.
passed_in() {
  grep -hE '^test result:' "$OUT/$1.$2.out" 2>/dev/null | tail -1 | sed -nE 's/.* ([0-9]+) passed.*/\1/p'
}

words() { printf '%s\n' $1 | sed '/^$/d' | sort -u; }

# Is `name` (target::test) covered by the space-separated list `$2`? `target::*` covers a target.
covered() {
  local w
  for w in $2; do
    case "$w" in
      *::\*) [ "${1%%::*}" = "${w%%::*}" ] && return 0 ;;
      *) [ "$1" = "$w" ] && return 0 ;;
    esac
  done
  return 1
}

verdict() { # $1 label, $2 required, $3 optional
  local nr actual missing extra name
  nr=$(missing_result "$1")
  if [ -n "$nr" ]; then echo "NO-RESULT ($nr)"; return; fi
  actual=$(failed_names "$1")
  if [ -z "$actual" ]; then
    if [ -z "$(words "$2")" ]; then echo "SURVIVED-AS-REGISTERED"; else echo "SURVIVED"; fi
    return
  fi
  missing=$(comm -23 <(words "$2") <(printf '%s\n' "$actual"))
  extra=""
  while read -r name; do
    covered "$name" "$2 $3" || extra="$extra $name"
  done <<< "$actual"
  if [ -z "$missing" ] && [ -z "$extra" ]; then
    echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else
    echo "MISMATCH (missing: $(echo $missing); unexpected:$extra)"
  fi
}

as_registered() { case "$1" in KILLED-AS-REGISTERED*|SURVIVED-AS-REGISTERED) return 0 ;; *) return 1 ;; esac; }

# --------------------------------------------------------------------------------------------
if [ "${1:-}" = "--selftest" ]; then
  OUT=$(mktemp -d "${TMPDIR:-/tmp}/d257-selftest.XXXXXX") || exit 2
  fake() { # $1 label, $2 target, $3 failed names (space-separated), $4 "noresult" to omit the line
    local f="$OUT/$1.$2.out" n
    { echo "     Running tests/x.rs (target/debug/deps/x-0)"; echo "running 3 tests"
      echo "test fine_one ... ok"
      for n in $3; do echo "test $n ... FAILED"; done
      echo; echo "failures:"; for n in $3; do echo "    $n"; done; echo
      [ "${4:-}" = noresult ] || echo "test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out"
      echo "rc=101"; } > "$f"
  }
  fails=0
  expect() { # $1 description, $2 got, $3 expected prefix
    case "$2" in "$3"*) echo "selftest PASS: $1 -> $2" ;; *) echo "selftest FAIL: $1 -> $2 (expected $3...)"; fails=$((fails + 1)) ;; esac
  }
  fake clean d257 ""; fake clean alter ""
  expect "nothing fails, nothing required" "$(verdict clean "" "")" "SURVIVED-AS-REGISTERED"
  expect "nothing fails, a killer required" "$(verdict clean "d257::a" "")" "SURVIVED"
  fake exact d257 "a b"; fake exact alter ""
  expect "exactly the required" "$(verdict exact "d257::a d257::b" "")" "KILLED-AS-REGISTERED"
  expect "one required missing" "$(verdict exact "d257::a d257::b d257::c" "")" "MISMATCH"
  expect "one failure unregistered" "$(verdict exact "d257::a" "")" "MISMATCH"
  expect "the unregistered one optional" "$(verdict exact "d257::a" "d257::b")" "KILLED-AS-REGISTERED"
  fake wild d257 "a"; fake wild alter "z y"
  expect "alter::* covers any alter failure" "$(verdict wild "d257::a" "alter::*")" "KILLED-AS-REGISTERED"
  expect "without alter::*, alter failures are unexpected" "$(verdict wild "d257::a" "")" "MISMATCH"
  expect "alter::* does not cover d257" "$(verdict wild "alter::z" "alter::*")" "MISMATCH"
  fake nores d257 "a" noresult; fake nores alter ""
  expect "a target with no result line" "$(verdict nores "d257::a" "")" "NO-RESULT (d257)"
  # N2's plant: stray files beside the outputs must change nothing.
  fake stray d257 "a"; fake stray alter ""
  echo "1 file changed" > "$OUT/stray.diffstat"; echo "test q ... FAILED" > "$OUT/stray.junk.txt"
  echo "no result line here" > "$OUT/stray.extra.out.txt"
  expect "stray files beside the outputs" "$(verdict stray "d257::a" "")" "KILLED-AS-REGISTERED"
  # A module path is stripped and the target prefixed.
  fake path d257 "tests::deep::a"; fake path alter ""
  expect "module path stripped" "$(verdict path "d257::a" "")" "KILLED-AS-REGISTERED"
  [ "$(passed_in exact d257)" = 1 ] && echo "selftest PASS: passed_in reads 1" \
    || { echo "selftest FAIL: passed_in read '$(passed_in exact d257)'"; fails=$((fails + 1)); }
  as_registered "SURVIVED" && { echo "selftest FAIL: SURVIVED counted as registered"; fails=$((fails + 1)); } \
    || echo "selftest PASS: SURVIVED is not as-registered"
  rm -rf "$OUT"
  echo "selftest failures=$fails"
  [ "$fails" -eq 0 ]
  exit $?
fi

# --------------------------------------------------------------------------------------------
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
mkdir -p "$OUT"
: > "$OUT/summary.txt"
bad=0

echo "== base: src/ at $BASE_SHA"
git checkout "$BASE_SHA" -- src/
for t in $TARGETS; do run_target base "$t"; done
git checkout "$SUBJECT_SHA" -- src/
git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after base" >&2; exit 3; }
v=$(verdict base "$BASE_REQUIRED" "")
echo "base: $v" | tee -a "$OUT/summary.txt"
as_registered "$v" || bad=$((bad + 1))

echo "== control: $SUBJECT_SHA"
for t in $TARGETS; do run_target control "$t"; done
if [ -n "$(missing_result control)" ] || [ -n "$(failed_names control)" ] \
   || [ "$(passed_in control d257)" != 7 ] || [ "$(passed_in control alter)" != 16 ]; then
  echo "control: VOID (no result: $(missing_result control); failed: $(failed_names control | tr '\n' ' '); passed d257=$(passed_in control d257) alter=$(passed_in control alter); expected 7/16)" | tee -a "$OUT/summary.txt"
  exit 2
fi
echo "control: clean (7/16 passed)" | tee -a "$OUT/summary.txt"

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
  git checkout "$SUBJECT_SHA" -- src/
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after $name" >&2; exit 3; }
  v=$(verdict "$name" "$req" "$opt")
  echo "$name: $v" | tee -a "$OUT/summary.txt"
  as_registered "$v" || bad=$((bad + 1))
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
