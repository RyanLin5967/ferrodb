#!/usr/bin/env bash
# D263 fire-check: the red, the three mutants and the tip, judged from the files this script names and nothing
# else. The judge functions are bench/d232/firecheck.sh's @ 40f7570 (D232 review 4 Q5, in the shape of
# bench/d237/firecheck.sh @ e8054dc), with one addition: a row may register that NOTHING fails. Each arm restores
# src/ from its commit, runs its targets in the background and waits, and writes $OUT/<label>.<target>.out with
# an `arm=<sha>` first line and an `rc=<n>` last line. $OUT is cleared first, the control runs first, src/ is
# restored from git on any exit, and --self-test runs the judge on planted outputs.
# Pre-registration: artie-research frontier/lane_d263_free_after_durable.md §0 and §2.
# Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work: it runs only when the lead
# releases the FAN-QUEUE row.
#
# Usage: bash bench/d263/firecheck.sh               the run
#        bash bench/d263/firecheck.sh --self-test   the judge on planted outputs only: no cargo, no git writes
#
# Targets: d263 (`--lib d263_`), d232 (`--lib d232_`), branch (`--lib branch::`). Killers are full test paths.
# Arms, in run order:
# - control: SUBJECT_SHA, targets d263, d232 and branch. Every file OK, nothing FAILED, and per target
#   passed + ignored == the harness's own `-- --list` count, both > 0; d263 lists exactly 4 and d232 exactly
#   31. Anything else VOIDS the run (exit 2).
# - red: c38d920 (`d263_` only), with its registered FAILED set.
# - mutants: the three `d263-r4-mut-*` branches, a literal list below, each on `d263_` (its killers) and on
#   `d232_` (nothing may fail). The run refuses (exit 2) if the repo's `d263-r4-mut-*` branches differ from it
#   by name or by sha.
# A registered row is AS-REGISTERED when its file is OK, it selected exactly the registered count, every
# registered killer FAILED, nothing else did, no FAILED test's panic text is a premise (`fixture:` or
# `control:`), and each registered panic message appears in its test's block.
#
# Blind spots, stated: as bench/d232/firecheck.sh's. The per-target suite (FAN row step (c)) is judged by
# tools/verify-suite.sh, not here.
set -u
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=28c0525114c0005d81b1f261076042d1975fb259
OUT=bench/d263/firecheck

A=branch::arena::tests
FREE_RETURNS=$A::d263_a_free_that_failed_to_persist_returns_its_range_at_the_next_rewrite
CLAIM_RETURNS=$A::d263_a_claim_that_failed_to_persist_returns_its_range_at_the_next_rewrite
GUARD=$A::d263_a_rewrite_that_fails_keeps_the_range_out
COUNTER=$A::d263_the_quarantine_is_counted_and_empties_at_a_rewrite

# label|sha|target|selected (a count, or "any" for > 0)|required FAILED (full paths; every one must FAIL, and
# nothing else may; empty means nothing may fail). Rows of one label are adjacent and share a sha.
ARMS=(
  "red_c38d920|c38d9201682cfa7dfc5c3a0143f6f521d0285b5f|d263|3|$FREE_RETURNS $CLAIM_RETURNS"
  "d263-r4-mut-never-folds|921752646c2455ad56e5c4264cce2545b1a5f119|d263|4|$FREE_RETURNS $CLAIM_RETURNS"
  "d263-r4-mut-never-folds|921752646c2455ad56e5c4264cce2545b1a5f119|d232|31|"
  "d263-r4-mut-fold-survives-failure|f9478fd5b351144bcc01f8e725f04834257b1ec6|d263|4|$GUARD"
  "d263-r4-mut-fold-survives-failure|f9478fd5b351144bcc01f8e725f04834257b1ec6|d232|31|"
  "d263-r4-mut-no-quarantine|1f2f942f6ba146bbdfd077ceb4e71b292fbd90a2|d263|4|$FREE_RETURNS $COUNTER"
  "d263-r4-mut-no-quarantine|1f2f942f6ba146bbdfd077ceb4e71b292fbd90a2|d232|31|"
)
BASES=(
  "d263-r4-mut-never-folds|$SUBJECT_SHA" "d263-r4-mut-fold-survives-failure|$SUBJECT_SHA"
  "d263-r4-mut-no-quarantine|$SUBJECT_SHA"
)
# label|test (full path)|text its panic block must contain (each a one-line prefix of the assertion's message).
MSGS=(
  "red_c38d920|$FREE_RETURNS|D263: the rewrite after the failed free did not record its range as free"
  "red_c38d920|$CLAIM_RETURNS|D263: once a rewrite recorded the failed claim's range as free"
  "d263-r4-mut-never-folds|$FREE_RETURNS|D263: the rewrite after the failed free did not record its range as free"
  "d263-r4-mut-never-folds|$CLAIM_RETURNS|D263: once a rewrite recorded the failed claim's range as free"
  "d263-r4-mut-fold-survives-failure|$GUARD|D263: a range a failed rewrite had put on the free list"
  "d263-r4-mut-no-quarantine|$FREE_RETURNS|D263: the rewrite after the failed free did not record its range as free"
  "d263-r4-mut-no-quarantine|$COUNTER|D263: the free whose record failed was not counted as quarantined"
)
CONTROL_TARGETS="d263 d232 branch"
CONTROL_LISTED="d263=4 d232=31"

target_args() {
  case "$1" in
    d263)   echo "--lib d263_" ;;
    d232)   echo "--lib d232_" ;;
    branch) echo "--lib branch::" ;;
  esac
}
binaries() { echo 1; } # test binaries per target: one each here

# A command runs in the background and the script waits on it, so a TERM or INT reaches the traps at once
# instead of after the command (bash defers a trap until a foreground child exits, and `timeout` puts cargo in
# its own process group, out of reach of a signal to this script's group). The trap stops the child before src/
# is restored.
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

run_target() { # $1 label, $2 target, $3 the arm's sha (src/ is already checked out of it)
  local f rc
  f=$(tfile "$1" "$2")
  echo "arm=$3" > "$f"
  # shellcheck disable=SC2046
  waited timeout 3600 cargo test $(target_args "$2") >> "$f" 2>&1
  rc=$?
  echo "rc=$rc" >> "$f"
}

listed_in() { # $1 target: what the harness says it will run, at SUBJECT_SHA (the list is kept)
  # shellcheck disable=SC2046
  waited timeout 3600 cargo test $(target_args "$1") -- --list > "$OUT/control.$1.list" 2>&1
  grep -cE ': test$' "$OUT/control.$1.list"
}

# ---- The judge. It reads ONLY "$OUT/<label>.<target>.out" for the labels and targets it is given. Any other
# file in $OUT (a fire_*/red_*/tip_* output from this lane's earlier by-hand row, a list, the self-test log, a
# stale copy) cannot change a verdict (the D233/D237 review defect; D232 review 4 Q5). ----
tfile() { printf '%s/%s.%s.out' "$OUT" "$1" "$2"; }

# One target file's state: OK, or the reason its names cannot be judged. $3 is the registered sha.
tstate() { # $1 label, $2 target, $3 sha
  local f first last rc n nf
  f=$(tfile "$1" "$2")
  [ -f "$f" ] || { echo "INCOMPLETE ($2: no output file)"; return; }
  first=$(head -n 1 "$f")
  case "$first" in arm=*) ;; *) echo "INCOMPLETE ($2: no arm line)"; return ;; esac
  [ "${first#arm=}" = "$3" ] || { echo "STALE ($2: ran ${first#arm=}, registered $3)"; return; }
  last=$(tail -n 1 "$f")
  case "$last" in rc=[0-9]*) rc=${last#rc=} ;; *) echo "INCOMPLETE ($2: no rc line)"; return ;; esac
  if [ "$rc" = 124 ]; then echo "TIMEOUT ($2)"; return; fi
  n=$(grep -cE '^test result:' "$f")
  if [ "$n" -eq 0 ]; then echo "COMPILE-FAIL ($2)"; return; fi
  if [ "$n" -ne "$(binaries "$2")" ]; then echo "INCOMPLETE ($2: $n result lines)"; return; fi
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

count_in() { # $1 label, $2 target, $3 passed|failed|ignored: summed over the file's result lines
  grep -E '^test result:' "$(tfile "$1" "$2")" | sed -nE "s/.* ([0-9]+) $3.*/\\1/p" | awk '{ s += $1 } END { print s + 0 }'
}

selected_in() { # $1 label, $2 target
  echo $(($(count_in "$1" "$2" passed) + $(count_in "$1" "$2" failed) + $(count_in "$1" "$2" ignored)))
}

failed_paths() { # $1 label, $2 target: full paths of FAILED tests, sorted, duplicates kept
  sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' "$(tfile "$1" "$2")" | sort
}

block_of() { # $1 label, $2 target, $3 test path: its "---- <path> stdout ----" block
  awk -v want="---- $3 stdout ----" '
    $0 == want { on = 1; next }
    on && (/^---- / || $0 == "failures:") { on = 0 }
    on { print }' "$(tfile "$1" "$2")"
}

words() { printf '%s\n' $1 | sed '/^$/d' | sort; }

msgs_of() { # $1 label: "test|text" lines registered for it
  local m ml mt mx
  for m in "${MSGS[@]}"; do
    IFS='|' read -r ml mt mx <<< "$m"
    [ "$ml" = "$1" ] && printf '%s|%s\n' "$mt" "$mx"
  done
}

verdict() { # $1 label, $2 target, $3 sha, $4 selected (count or any), $5 required FAILED
  local label=$1 t=$2 sha=$3 want=$4 req=$5 st sel actual p missing extra line mt mx
  st=$(tstate "$label" "$t" "$sha")
  [ "$st" = OK ] || { echo "$st"; return; }
  sel=$(selected_in "$label" "$t")
  if [ "$want" = any ]; then
    [ "$sel" -gt 0 ] || { echo "SELECTED-MISMATCH ($t: nothing selected)"; return; }
  elif [ "$sel" -ne "$want" ]; then
    echo "SELECTED-MISMATCH ($t: selected $sel, registered $want)"; return
  fi
  actual=$(failed_paths "$label" "$t")
  for p in $actual; do
    if block_of "$label" "$t" "$p" | grep -qE 'fixture:|control:'; then
      echo "FIXTURE-FAIL ($t: $p failed on a premise: $(block_of "$label" "$t" "$p" | grep -m1 -E 'fixture:|control:'))"
      return
    fi
  done
  if [ -z "$actual" ]; then
    if [ -z "$(echo $req)" ]; then echo "AS-REGISTERED ($t: 0 FAILED of $sel)"; else echo "SURVIVED ($t)"; fi
    return
  fi
  missing=$(comm -23 <(words "$req") <(printf '%s\n' "$actual"))
  extra=$(comm -13 <(words "$req") <(printf '%s\n' "$actual"))
  if [ -n "$extra" ]; then
    echo "MISMATCH ($t; missing: $(echo $missing); unexpected: $(echo $extra))"; return
  fi
  if [ -n "$missing" ]; then echo "PARTIAL ($t; missing: $(echo $missing))"; return; fi
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    mt=${line%%|*}; mx=${line#*|}
    if ! block_of "$label" "$t" "$mt" | grep -qF "$mx"; then
      echo "MISMATCH ($t; $mt did not fail with the registered message: $mx)"; return
    fi
  done <<< "$(msgs_of "$label")"
  echo "AS-REGISTERED ($t: $(echo "$actual" | wc -l | tr -d ' ') FAILED of $sel)"
}

control_verdict() { # $1 label, $2 "target=listed ..." (the harness's own -- --list counts)
  local label=$1 lists=$2 st t l p i r
  for t in $CONTROL_TARGETS; do
    st=$(tstate "$label" "$t" "$SUBJECT_SHA")
    [ "$st" = OK ] || { echo "VOID ($st)"; return; }
    [ -z "$(failed_paths "$label" "$t")" ] || { echo "VOID (a test FAILED in $t)"; return; }
    # shellcheck disable=SC2086
    l=$(printf '%s\n' $lists | sed -n "s/^$t=//p")
    p=$(count_in "$label" "$t" passed)
    i=$(count_in "$label" "$t" ignored)
    case "$l" in ''|*[!0-9]*) echo "VOID ($t: no list count)"; return ;; esac
    if [ "$l" -eq 0 ] || [ "$p" -eq 0 ] || [ $((p + i)) -ne "$l" ]; then
      echo "VOID ($t: listed $l, passed $p, ignored $i)"; return
    fi
    r=$(printf '%s\n' $CONTROL_LISTED | sed -n "s/^$t=//p")
    if [ -n "$r" ] && [ "$l" -ne "$r" ]; then
      echo "VOID ($t: listed $l, registered $r)"; return
    fi
  done
  echo clean
}

ok_verdict() { case "$1" in AS-REGISTERED*) return 0 ;; *) return 1 ;; esac; }

# ---- --self-test: the judge on planted outputs, in a temporary directory. No cargo, no git writes. ----
plant() { # $1 label, $2 target, $3 sha, $4 rc, then the file's lines
  local f sha=$3 rc=$4 line
  f=$(tfile "$1" "$2")
  shift 4
  { echo "arm=$sha"; for line in "$@"; do printf '%s\n' "$line"; done; echo "rc=$rc"; } > "$f"
}
result_lines() { # $1 target, $2 passed, $3 failed: one result line per binary, the counts on the first
  local b=1 n st
  n=$(binaries "$1")
  st=ok; [ "$3" -eq 0 ] || st=FAILED
  echo "test result: $st. $2 passed; $3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  while [ "$b" -lt "$n" ]; do
    echo "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
    b=$((b + 1))
  done
}
ok_file() { # $1 label, $2 target, $3 sha, $4 passed
  local lines
  lines=$(result_lines "$2" "$4" 0)
  plant "$1" "$2" "$3" 0 "running $4 tests" "$lines"
}
fail_file() { # $1 label, $2 target, $3 sha, $4 passed, $5 message style (claim|fixture|other), then FAILED paths
  local label=$1 t=$2 sha=$3 p=$4 style=$5 n body="" blocks="" msg reg
  shift 5
  for n in "$@"; do
    body="${body}test $n ... FAILED
"
    reg=$(msgs_of "$label" | sed -n "s/^$(printf '%s' "$n" | sed 's/[.[\*^$/]/\\&/g')|//p" | head -n 1)
    case "$style" in
      claim) msg=${reg:-planted claim for $n} ;;
      fixture) msg="fixture: planted premise for $n" ;;
      other) msg="some other claim for $n" ;;
    esac
    blocks="${blocks}---- $n stdout ----

thread '$n' panicked at src/planted.rs:1:1:
assertion failed: $msg
note: run with \`RUST_BACKTRACE=1\` environment variable to display a backtrace

"
  done
  plant "$label" "$t" "$sha" 101 "running tests" "${body%?}" "" "failures:" "" "${blocks%?}" "failures:" \
    "$(printf '    %s\n' "$@")" "" "$(result_lines "$t" "$p" $#)"
}
expect() { # $1 case, $2 expected verdict prefix, $3 actual verdict
  case "$3" in
    "$2"*) echo "self-test PASS  $1: $3" ;;
    *) echo "self-test FAIL  $1: expected '$2', got '$3'"; st_bad=$((st_bad + 1)) ;;
  esac
}
row() { # $1 index into ARMS: sets r_label r_sha r_t r_sel r_req
  IFS='|' read -r r_label r_sha r_t r_sel r_req <<< "${ARMS[$1]}"
}
plant_registered() { # $1 index into ARMS: plant that row exactly as registered
  local nk p
  row "$1"
  nk=$(words "$r_req" | wc -l | tr -d ' ')
  if [ "$r_sel" = any ]; then p=5; else p=$((r_sel - nk)); fi
  # shellcheck disable=SC2046
  fail_file "$r_label" "$r_t" "$r_sha" "$p" claim $(words "$r_req")
}
plant_strays() { # stray files that share a prefix or a label; each holds FAILED lines and a result line
  local name
  for name in fire_d263-mut-never-folds.txt fire_d263-mut-never-folds_d232.txt red_fa80aba.txt tip_d263.txt \
    fire_d263-r4-mut-bogus.txt red_0000000.txt "d263-r4-mut-never-folds.d263.out.bak" \
    "d263-r4-mut-never-folds.d263.out~" "d263-r4-mut-never-folds.d263.outx" "d263-r4-mut-never-folds.stale.out" \
    "red_c38d920.d263.out.old" selftest.log summary.txt; do
    printf '%s\n' "arm=0000000000000000000000000000000000000000" "running 3 tests" \
      "test stray::not_a_registered_killer ... FAILED" \
      "test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s" \
      "rc=101" > "$OUT/$name"
  done
}
all_registered() { # every ARMS row judged; prints the number not AS-REGISTERED
  local i=0 bad=0 v
  while [ "$i" -lt "${#ARMS[@]}" ]; do
    row "$i"
    v=$(verdict "$r_label" "$r_t" "$r_sha" "$r_sel" "$r_req")
    ok_verdict "$v" || { echo "  $r_label/$r_t: $v" >&2; bad=$((bad + 1)); }
    i=$((i + 1))
  done
  echo "$bad"
}

self_test() {
  local keep=$OUT st_bad=0 tmp i m name sha rest lists k1 v
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/d263-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp
  lists="d263=4 d232=31 branch=400"

  ok_file control d263 "$SUBJECT_SHA" 4; ok_file control d232 "$SUBJECT_SHA" 31
  ok_file control branch "$SUBJECT_SHA" 400
  expect "clean control" "clean" "$(control_verdict control "$lists")"

  ok_file dirty d263 "$SUBJECT_SHA" 4; ok_file dirty d232 "$SUBJECT_SHA" 31
  fail_file dirty branch "$SUBJECT_SHA" 399 claim "$GUARD"
  # The branch list count is planted equal to the passed count, so the count gate alone would pass this
  # control: the case proves the FAILED check fires on its own.
  expect "dirty control" "VOID (a test FAILED in branch)" "$(control_verdict dirty "d263=4 d232=31 branch=399")"

  ok_file empty d263 "$SUBJECT_SHA" 4; ok_file empty d232 "$SUBJECT_SHA" 31
  plant empty branch "$SUBJECT_SHA" 0 "running 0 tests" \
    "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2600 filtered out; finished in 0.00s"
  expect "control that collected nothing" "VOID" "$(control_verdict empty "d263=4 d232=31 branch=0")"

  ok_file short d263 "$SUBJECT_SHA" 3; ok_file short d232 "$SUBJECT_SHA" 31; ok_file short branch "$SUBJECT_SHA" 400
  expect "control whose d263 list is not the registered 4" "VOID (d263: listed 3" \
    "$(control_verdict short "d263=3 d232=31 branch=400")"

  # Every registered row, planted exactly as registered, from the tables the run uses. A row that registers
  # nothing failing is planted as a clean file of its selected count.
  i=0
  while [ "$i" -lt "${#ARMS[@]}" ]; do
    row "$i"
    if [ -z "$r_req" ]; then ok_file "$r_label" "$r_t" "$r_sha" "$r_sel"; else plant_registered "$i"; fi
    i=$((i + 1))
  done
  expect "every registered row as registered" "0" "$(all_registered 2>&1 | tail -n 1)"
  plant_strays
  expect "every registered row, with this lane's old fire_*/red_*/tip_* names and label-sharing files in \$OUT" \
    "0" "$(all_registered 2>&1 | tail -n 1)"
  # The aggregate's positive control: one row turned survivor must count, or "0" above proves nothing.
  row 0
  ok_file "$r_label" "$r_t" "$r_sha" "$r_sel"
  expect "the aggregate counts one row that survived" "1" "$(all_registered 2>/dev/null | tail -n 1)"
  plant_registered 0

  name=d263-r4-mut-no-quarantine
  sha=1f2f942f6ba146bbdfd077ceb4e71b292fbd90a2

  fail_file "$name" d263 "$sha" 3 claim "$FREE_RETURNS"
  expect "PARTIAL kill: the counter passed" "PARTIAL" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  fail_file "$name" d263 "$sha" 1 claim "$FREE_RETURNS" "$COUNTER" "$GUARD"
  expect "unexpected extra failure" "MISMATCH" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  fail_file "$name" d263 "$sha" 2 fixture "$FREE_RETURNS" "$COUNTER"
  expect "killers failed on their premise" "FIXTURE-FAIL" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  fail_file "$name" d263 "$sha" 2 other "$FREE_RETURNS" "$COUNTER"
  expect "killers failed with unregistered messages" "MISMATCH" \
    "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  ok_file "$name" d263 "$sha" 4
  expect "survivor" "SURVIVED" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  fail_file "$name" d232 "$sha" 30 claim "$A::d232_a_free_whose_record_fails_to_persist_does_not_hand_its_range_out_again"
  expect "a d232_ row that registers nothing, with a failure" "MISMATCH" "$(verdict "$name" d232 "$sha" 31 "")"

  ok_file "$name" d232 "$sha" 31
  expect "a d232_ row that registers nothing, clean" "AS-REGISTERED" "$(verdict "$name" d232 "$sha" 31 "")"

  ok_file "$name" d232 "$sha" 29
  expect "a d232_ row that registers nothing, two tests short" "SELECTED-MISMATCH" \
    "$(verdict "$name" d232 "$sha" 31 "")"

  fail_file "$name" d263 "$SUBJECT_SHA" 2 claim "$FREE_RETURNS" "$COUNTER"
  expect "output from another commit (the mutant's base)" "STALE" \
    "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  rm -f "$(tfile "$name" d263)"
  expect "no output file" "INCOMPLETE (d263: no output file)" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  printf '%s\n' "arm=$sha" "running 4 tests" > "$(tfile "$name" d263)"
  expect "no rc line (killed mid-run)" "INCOMPLETE (d263: no rc line)" \
    "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  printf '%s\n' "running 4 tests" "rc=101" > "$(tfile "$name" d263)"
  expect "no arm line" "INCOMPLETE (d263: no arm line)" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  plant "$name" d263 "$sha" 124 "running 4 tests" "test $FREE_RETURNS has been running for over 60 seconds"
  expect "timeout" "TIMEOUT" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  plant "$name" d263 "$sha" 101 "error[E0308]: mismatched types" \
    "error: could not compile \`ferrodb\` (lib test) due to 1 previous error"
  expect "compile failure" "COMPILE-FAIL" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  plant "$name" d263 "$sha" 0 "test $FREE_RETURNS ... FAILED" \
    "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  expect "rc=0 with a FAILED line" "RC-MISMATCH" "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  plant "$name" d263 "$sha" 0 "running 4 tests" \
    "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s" \
    "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  expect "two result lines for a one-binary target" "INCOMPLETE (d263: 2 result lines)" \
    "$(verdict "$name" d263 "$sha" 4 "$FREE_RETURNS $COUNTER")"

  # Registration: every mutant has a killer row, a base and a d232_ row; every message names a killer.
  for m in "${BASES[@]}"; do
    IFS='|' read -r name rest <<< "$m"
    v=$(printf '%s\n' "${ARMS[@]}" | awk -F'|' -v n="$name" '$1 == n && $5 != "" { c++ } END { print c + 0 }')
    [ "$v" -gt 0 ] || { echo "self-test FAIL  registration: $name has no ARMS row with a killer"; st_bad=$((st_bad + 1)); }
    v=$(printf '%s\n' "${ARMS[@]}" | awk -F'|' -v n="$name" '$1 == n && $3 == "d232" && $5 == "" { c++ } END { print c + 0 }')
    [ "$v" -eq 1 ] || { echo "self-test FAIL  registration: $name has no clean d232_ row"; st_bad=$((st_bad + 1)); }
  done
  v=$(printf '%s\n' "${ARMS[@]}" | awk -F'|' '$1 ~ /^d263-r4-mut-/ { print $1 }' | sort -u | wc -l | tr -d ' ')
  [ "$v" -eq "${#BASES[@]}" ] || { echo "self-test FAIL  registration: $v mutants in ARMS, ${#BASES[@]} in BASES"; st_bad=$((st_bad + 1)); }
  for m in "${MSGS[@]}"; do
    IFS='|' read -r name k1 rest <<< "$m"
    printf '%s\n' "${ARMS[@]}" | awk -F'|' -v n="$name" -v k="$k1" '$1 == n { split($5, a, " "); for (x in a) if (a[x] == k) f = 1 } END { exit !f }' ||
      { echo "self-test FAIL  registration: message for $name names $k1, not one of its killers"; st_bad=$((st_bad + 1)); }
  done
  [ "$st_bad" -eq 0 ] && echo "self-test PASS  registration: every mutant has a killer row, a clean d232_ row and a base; every message names a killer"

  # Arguments: anything but nothing or --self-test is refused with 2, before any work.
  bash "$SELF" --bogus > /dev/null 2>&1
  v=$?
  expect "an unknown argument exits 2" "2" "$v"

  rm -rf "$OUT"
  OUT=$keep
  echo "self-test: $st_bad case(s) missed"
  [ "$st_bad" -eq 0 ]
}

if [ "${1:-}" = --self-test ] && [ $# -eq 1 ]; then self_test; exit $?; fi
[ $# -eq 0 ] || { echo "usage: $0 [--self-test]" >&2; exit 2; }

if ! git diff --quiet "$SUBJECT_SHA" -- src/ tests/; then
  echo "REFUSED: src/ or tests/ differ from $SUBJECT_SHA" >&2
  exit 2
fi
if [ -n "$(git status --porcelain -- src/ tests/)" ]; then
  echo "REFUSED: uncommitted changes under src/ or tests/" >&2
  exit 2
fi
# The literal mutant list must be the repo's: a mutant added or moved without a registration is refused here.
want=$(printf '%s\n' "${ARMS[@]}" | awk -F'|' '$1 ~ /^d263-r4-mut-/ { print $1 " " $2 }' | sort -u)
have=$(git for-each-ref --format='%(refname:short) %(objectname)' 'refs/heads/d263-r4-mut-*' | sort)
if [ "$want" != "$have" ]; then
  echo "REFUSED: the d263-r4-mut-* branches differ from the registered list" >&2
  diff <(echo "$want") <(echo "$have") >&2
  exit 2
fi
for m in "${BASES[@]}"; do
  IFS='|' read -r name base <<< "$m"
  if [ "$(git rev-parse "refs/heads/$name^")" != "$base" ]; then
    echo "REFUSED: $name does not sit on its registered base $base" >&2
    exit 2
  fi
done

rm -rf "$OUT"
mkdir -p "$OUT"
if ! self_test > "$OUT/selftest.log" 2>&1; then
  cat "$OUT/selftest.log" >&2
  echo "REFUSED: the judge's self-test failed" >&2
  exit 2
fi

# `git restore --source`, not `git checkout <sha> --`: a path the source lacks is REMOVED, so an arm whose commit
# has fewer files under src/ than the tip runs on exactly its own tree, and the way back restores them.
to_src() { git restore --source="$1" --staged --worktree -- src/; }
on_exit() {
  to_src "$SUBJECT_SHA" 2>/dev/null
  git diff --quiet "$SUBJECT_SHA" -- src/ tests/ ||
    echo "ON EXIT: src/ or tests/ still differ from $SUBJECT_SHA; restore with: git restore --source=$SUBJECT_SHA --staged --worktree -- src/" >&2
}
on_signal() { # $1 = the exit status
  if [ -n "$child" ]; then
    kill -TERM "$child" 2>/dev/null
    wait "$child" 2>/dev/null
  fi
  exit "$1"
}
trap on_exit EXIT
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

echo "== control: $SUBJECT_SHA"
for t in $CONTROL_TARGETS; do run_target control "$t" "$SUBJECT_SHA"; done
lists=""
for t in $CONTROL_TARGETS; do lists="$lists $t=$(listed_in "$t")"; done
v=$(control_verdict control "$lists")
echo "control: $v (lists:$lists)" | tee "$OUT/summary.txt"
[ "$v" = clean ] || exit 2

bad=0
current=""
i=0
while [ "$i" -lt "${#ARMS[@]}" ]; do
  row "$i"
  i=$((i + 1))
  if [ "$r_sha" != "$current" ]; then
    echo "== $r_label: src/ at $r_sha"
    to_src "$r_sha"
    if ! git diff --quiet "$r_sha" -- src/ || ! git diff --cached --quiet "$r_sha" -- src/; then
      echo "$r_label: NOT APPLIED (src/ is not $r_sha)" | tee -a "$OUT/summary.txt"
      bad=$((bad + 1)); current=""; continue
    fi
    current=$r_sha
  fi
  run_target "$r_label" "$r_t" "$r_sha"
  v=$(verdict "$r_label" "$r_t" "$r_sha" "$r_sel" "$r_req")
  echo "$r_label/$r_t: $v" | tee -a "$OUT/summary.txt"
  ok_verdict "$v" || bad=$((bad + 1))
  for p in $(failed_paths "$r_label" "$r_t"); do # the reader sees each FAILED test's panic line
    echo "    $p: $(block_of "$r_label" "$r_t" "$p" | grep -m1 -vE '^(thread |$)')" >> "$OUT/summary.txt"
  done
done
to_src "$SUBJECT_SHA"
git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored" >&2; exit 3; }

echo "arms not as registered: $bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ] || exit 1
exit 0
