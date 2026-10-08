#!/usr/bin/env bash
# D232 fire-check (review 4 Q5): the reds, the twenty mutants and the tip, judged from the files this script
# names and nothing else. Shape of bench/d237/firecheck.sh @ e8054dc: each arm restores src/ from its commit, runs
# its targets in the background and waits, and writes $OUT/<label>.<target>.out with an `arm=<sha>` first line
# and an `rc=<n>` last line. The judge reads only those files, $OUT is cleared first, the control runs first,
# src/ is restored from git on any exit, and --self-test runs the judge on planted outputs. The traps cover HUP,
# INT, QUIT and TERM, and no cargo runs inside $(...), where the traps could not reach it (D237 judge review J4/J5).
# Pre-registration: artie-research frontier/lane_d232_arena_claim.md §3, §6 (amendment 2), §8 and §10.
# Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work: it runs only when the lead
# releases the FAN-QUEUE row.
#
# Usage: bash bench/d232/firecheck.sh               the run
#        bash bench/d232/firecheck.sh --self-test   the judge on planted outputs only: no cargo, no git writes
#
# Targets: d232 (`--lib d232_`), branch (`--lib branch::`), arena (`--lib branch::arena::`), reap (three
# integration binaries). A reaper case runs twice, once per catalog, and each run is its own FAILED path
# (`...::log_catalog::` and `...::table_catalog::`), so killers are full test paths, never short names.
# Arms, in run order:
# - control: SUBJECT_SHA, targets d232, branch and reap. Every file OK, nothing FAILED, and per target
#   passed + ignored == the harness's own `-- --list` count, both > 0; d232 lists exactly 31. Anything else
#   VOIDS the run (exit 2).
# - reds: 097e5c0, c70d1c6, 3d825f2 (plus `branch::`) and 118d839, each with its registered FAILED set.
# - mutants: the twenty `d232-mut-*` branches, a literal list below. The run refuses (exit 2) if the repo's
#   `d232-mut-*` branches differ from it by name or by sha.
# A registered arm is AS-REGISTERED when its file is OK, it selected exactly the registered count, every
# registered killer FAILED, nothing else did, no FAILED test's panic text is a premise (`fixture:` or
# `control:`), and each registered panic message appears in its test's block.
#
# Blind spots, stated: a registered killer that panics on an unwrap or a claim other than the one intended still
# counts as killed (the panic line is printed for the reader); messages are registered only for the review 3 and
# review 4 arms, whose claims were traced. The per-target suite (FAN row step (c)) is judged by
# tools/verify-suite.sh, not here. The judge reads cargo's own lines ("test result:", "test <path> ... FAILED",
# "---- <path> stdout ----") and the arm and rc lines this script writes; a test that printed a line of that exact
# shape under --nocapture would be misread (none is run with --nocapture).
set -u
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
cd "$(git rev-parse --show-toplevel)" || exit 2

# The source the control runs and every arm returns to: `bb7cd7c` (review 4 B3.1, the load_state hold) on
# `74de46b`. Its src/ equals the tip's.
SUBJECT_SHA=bb7cd7c158f1c1fce30a03c7833ee75f8eb800ae
R4=74de46bc41e2f7929a064fe5c0365cab0210347d
OUT=bench/d232/firecheck

A=branch::arena::tests
RL=branch::reaper::tests::log_catalog
RT=branch::reaper::tests::table_catalog
L=branch::lease_thread::tests
both() { echo "$RL::$1 $RT::$1"; }

CRASH=$A::d232_a_crash_between_a_claims_two_writes_never_gives_one_arena_two_owners
H2B=$A::d232_a_claim_whose_record_fails_to_persist_publishes_no_extent
FREE_PERSIST=$A::d232_a_free_whose_record_fails_to_persist_does_not_hand_its_range_out_again
H3=$A::d232_a_fill_probe_that_hits_an_unreadable_page_leaves_the_fill_unknown
REFUSED=$A::d232_a_refused_claim_is_undone_through_the_durable_free_record
TENANT=$A::d232_a_fill_probe_does_not_count_a_previous_tenants_pages
REWRITE_FREE=$A::d232_a_free_persisted_as_a_full_rewrite_keeps_its_range_in_the_image
FAILED_APPEND=$A::d232_after_a_failed_append_the_next_persist_is_a_full_rewrite
F1=$A::d232_a_claim_written_as_a_full_rewrite_counts_its_own_pages
F1_UNDO=$A::d232_a_claim_whose_record_fails_to_persist_reserves_nothing
F5=$A::d232_after_a_failed_rewrite_the_next_persist_is_still_a_rewrite
LADDER=$A::d232_a_failed_append_rewrites_until_one_succeeds_then_appends_again
B2_IMAGE=$A::d232_the_image_charges_exactly_its_extents_pages_whatever_the_counter_says
B2_LOAD=$A::d232_a_loaded_image_charges_exactly_its_extents_pages
TAIL_CLAIMS=$A::a_restart_recovers_the_claims_that_live_only_in_the_tail
RECYCLED=$A::reusing_a_recycled_page_reaches_the_durable_map
H1=$(both d232_a_steady_state_sweep_does_not_free_a_live_extent_whose_owner_cannot_be_read)
FAST=$(both d232_a_reap_over_an_aliased_arena_frees_only_its_own_extents)
SLOW=$(both d232_a_slow_path_reap_over_an_aliased_arena_releases_none_of_its_pages)
ABSENT_OPEN=$(both d232_an_absent_owner_is_gone_at_open_and_not_evidence_after_it)
UNREADABLE=$(both d232_an_unreadable_owner_is_not_gone_even_at_open)
F3_FOREIGN=$(both d232_a_drain_releases_no_parked_page_into_another_branchs_extent)
F3_ABSENT=$(both d232_a_drain_leaves_alone_a_page_whose_extent_is_gone)
REPORT=$L::d232_unreadable_owners_and_foreign_arenas_are_printed_not_only_counted

D95=d95132e3a9d5973633ae01dd9c58035691eb84db
AE3=ae36eae6eba6eb76f66e03ac98e63f05ac25c814
T08=08318879b386337275c0ee880aec5dd9850fe4fc

# label|sha|target|selected (a count, or "any" for > 0)|required FAILED (full paths; every one must FAIL, and
# nothing else may). Rows of one label are adjacent and share a sha. The mutant shas are the branch tips.
ARMS=(
  "red_097e5c0|097e5c0becb1a56e1aab010a70694c233ffe4918|d232|10|$CRASH $H2B $FREE_PERSIST $H3 $H1 $FAST $SLOW"
  "red_c70d1c6|c70d1c6e1151554d58d292acc1cc7235023249e3|d232|26|$F1 $F5 $F3_FOREIGN"
  "red_3d825f2|3d825f22448d000877d4ada755cb6d5499585130|d232|28|$F1 $F5 $F3_FOREIGN $F3_ABSENT"
  "red_3d825f2|3d825f22448d000877d4ada755cb6d5499585130|branch|any|$F1 $F5 $F3_FOREIGN $F3_ABSENT $TAIL_CLAIMS $RECYCLED"
  "red_118d839|118d839280323ff160e2c4f8eefd5354db6b51af|d232|31|$B2_IMAGE $B2_LOAD"
  "d232-mut-catalog-first|aa0bf6aac67302d040a4cf489759d46fea274d77|d232|21|$CRASH $H2B"
  "d232-mut-publish-unpersisted|8485153559d126de6a045f28cb06b5d0161be8c3|d232|21|$H2B"
  "d232-mut-refusal-memory-only|49ce456d5647f50da8b9f3998a4b6a780675a76c|d232|21|$REFUSED"
  "d232-mut-no-takeback|e6b0d22e1af1a813cbd1c469d1d83dbb6f22e41b|d232|21|$FREE_PERSIST"
  "d232-mut-giveback-after|8735f7f5040e5074f4ebac8d831ebd2979eabe5a|d232|21|$REWRITE_FREE"
  "d232-mut-no-owner-check|b597aa05ea5549edde3e96ad4e519522e4e0517d|d232|21|$FAST $SLOW $REPORT"
  "d232-mut-unreadable-is-gone|c19d2a65977242d963c0ac9b87ddf7442b54bce0|d232|21|$H1 $UNREADABLE $ABSENT_OPEN $REPORT"
  "d232-mut-absent-refused-at-open|27893d8e8cdb05d761809073695c8ee376de85ad|d232|21|$ABSENT_OPEN"
  "d232-mut-probe-stops-anywhere|ce13a8d43dee75df55839f27b5fe4b55eafbb1f9|d232|21|$H3"
  "d232-mut-probe-counts-any-page|5448d9b8ed5839c9cc7b23ba6e81a14c2f1c9b71|d232|21|$TENANT"
  "d232-mut-append-keeps-image|45b4e737e1f2312cd6d6bf9aa3e9ee06208a9854|d232|21|$FAILED_APPEND"
  "d232-mut-no-report|0e9ac1723af794b43e6950ee9bdd1f6027794da1|d232|21|$REPORT"
  "d232-mut-reserve-after-persist|835639334b77fffe077142fa16084278953afce6|d232|28|$F1 $F5"
  "d232-mut-reserve-after-persist|835639334b77fffe077142fa16084278953afce6|arena|any|$F1 $F5 $TAIL_CLAIMS $RECYCLED"
  "d232-mut-reserve-no-undo|e94e908b737498ce6d26424628ca0537b3d08b24|d232|28|$F1_UNDO"
  "d232-mut-rewrite-drops-flag|795270af882494fc15e836ea7cf9f23dd9b4cb34|d232|28|$F5"
  "d232-mut-drain-no-owner-check|b3a23ffd73312dd2cb2f8afbff5be81569b6de77|d232|28|$F3_FOREIGN $F3_ABSENT"
  "d232-mut-drain-releases-into-gone|f0f4e8bd02c4a929a8466459b738b04653985329|d232|28|$F3_ABSENT"
  "d232-mut-rewrite-keeps-image-zero|24739b1bccfe64273487a6bef6982916f8e34101|d232|29|$LADDER"
  "d232-mut-image-reserved-from-counter|005a7910e003cb1f1cfec99c5ce448e99570bcc5|d232|31|$B2_IMAGE"
  "d232-mut-load-reserved-from-field|45c34569ceb23fda6ec10779d4cb2dbfb9cb7af7|d232|31|$B2_LOAD"
)
# The base each mutant sits on, for the reader: the first twelve on $D95, the next five on $AE3, the ladder
# mutant on $T08, the last two on $R4 (review 4's fix, one commit below SUBJECT_SHA). Checked in the real mode
# (a mutant's parent must be its base).
BASES=(
  "d232-mut-catalog-first|$D95" "d232-mut-publish-unpersisted|$D95" "d232-mut-refusal-memory-only|$D95"
  "d232-mut-no-takeback|$D95" "d232-mut-giveback-after|$D95" "d232-mut-no-owner-check|$D95"
  "d232-mut-unreadable-is-gone|$D95" "d232-mut-absent-refused-at-open|$D95" "d232-mut-probe-stops-anywhere|$D95"
  "d232-mut-probe-counts-any-page|$D95" "d232-mut-append-keeps-image|$D95" "d232-mut-no-report|$D95"
  "d232-mut-reserve-after-persist|$AE3" "d232-mut-reserve-no-undo|$AE3" "d232-mut-rewrite-drops-flag|$AE3"
  "d232-mut-drain-no-owner-check|$AE3" "d232-mut-drain-releases-into-gone|$AE3"
  "d232-mut-rewrite-keeps-image-zero|$T08"
  "d232-mut-image-reserved-from-counter|$R4" "d232-mut-load-reserved-from-field|$R4"
)
# label|test (full path)|text its panic block must contain. Registered where the failing claim was traced.
MSGS=(
  "red_118d839|$B2_IMAGE|D232 review 4 B2: the image charged"
  "red_118d839|$B2_LOAD|D232 review 4 B2: the loaded store charged"
  "d232-mut-rewrite-keeps-image-zero|$LADDER|audit r2: the retry never ended"
  "d232-mut-image-reserved-from-counter|$B2_IMAGE|D232 review 4 B2: the image charged"
  "d232-mut-load-reserved-from-field|$B2_LOAD|D232 review 4 B2: the loaded store charged"
)
CONTROL_TARGETS="d232 branch reap"
CONTROL_D232_LISTED=31

target_args() {
  case "$1" in
    d232)   echo "--lib d232_" ;;
    branch) echo "--lib branch::" ;;
    arena)  echo "--lib branch::arena::" ;;
    reap)   echo "--test d40_reserved_pages_return_to_baseline --test d124_owner_record_refusal --test integration_server_reaps" ;;
  esac
}
binaries() { case "$1" in reap) echo 3 ;; *) echo 1 ;; esac; } # test binaries per target

# A command runs in the background and the script waits on it, so a HUP, INT, QUIT or TERM reaches the traps at once
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

list_target() { # $1 target: what the harness says it will run, at SUBJECT_SHA, kept as control.<t>.list
  # Called in the main shell, never inside $(...): a subshell sets `child` where the traps cannot see it, so a
  # signal waited out the listing or orphaned cargo (D237 judge review J4, memory an-exit-trap-waits-for-...).
  # shellcheck disable=SC2046
  waited timeout 3600 cargo test $(target_args "$1") -- --list > "$OUT/control.$1.list" 2>&1
}
listed_count() { grep -cE ': test$' "$OUT/control.$1.list"; } # $1 target: counts the kept list, runs nothing

# ---- The judge. It reads ONLY "$OUT/<label>.<target>.out" for the labels and targets it is given. Any other
# file in $OUT (a stray fire_*/red_* output from an older layout, a list, the self-test log, a stale copy)
# cannot change a verdict (the D233/D237 review defect, and D232 review 4 Q5's two strays). ----
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
  if [ -z "$actual" ]; then echo "SURVIVED ($t)"; return; fi
  missing=$(comm -23 <(words "$req") <(printf '%s\n' "$actual"))
  extra=$(comm -13 <(words "$req") <(printf '%s\n' "$actual"))
  if [ -n "$extra" ]; then
    echo "MISMATCH ($t; missing: $(echo $missing); unexpected: $(echo $extra))"; return
  fi
  if [ -n "$missing" ]; then echo "PARTIAL ($t; missing: $(echo $missing))"; return; fi
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    mt=${line%%|*}; mx=${line#*|}
    # A label's messages cover all its rows; one for a test this row does not register belongs to the
    # label's row on another target, and is asked there, not here (found by D263's self-test).
    case " $(echo $req) " in *" $mt "*) ;; *) continue ;; esac
    if ! block_of "$label" "$t" "$mt" | grep -qF "$mx"; then
      echo "MISMATCH ($t; $mt did not fail with the registered message: $mx)"; return
    fi
  done <<< "$(msgs_of "$label")"
  echo "AS-REGISTERED ($t: $(echo "$actual" | wc -l | tr -d ' ') FAILED of $sel)"
}

control_verdict() { # $1 label, $2 "target=listed ..." (the harness's own -- --list counts)
  local label=$1 lists=$2 st t l p i
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
    if [ "$t" = d232 ] && [ "$l" -ne "$CONTROL_D232_LISTED" ]; then
      echo "VOID (d232: listed $l, registered $CONTROL_D232_LISTED)"; return
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
  for name in fire_reserve_after_persist_arena.txt red_3d825f2_branch.txt fire_d232-mut-bogus.txt red_0000000.txt \
    fire_d232-mut-catalog-first.txt "d232-mut-catalog-first.d232.out.bak" "d232-mut-catalog-first.d232.out~" \
    "d232-mut-catalog-first.d232.outx" "d232-mut-catalog-first.stale.out" "red_c70d1c6.d232.out.old" \
    selftest.log summary.txt; do
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
  local keep=$OUT st_bad=0 tmp i m name sha rest lists k1 k2 k3 v
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/d232-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp
  lists="d232=31 branch=400 reap=40"

  ok_file control d232 "$SUBJECT_SHA" 31; ok_file control branch "$SUBJECT_SHA" 400
  ok_file control reap "$SUBJECT_SHA" 38
  expect "clean control" "clean" "$(control_verdict control "$lists")"

  ok_file dirty d232 "$SUBJECT_SHA" 31; ok_file dirty reap "$SUBJECT_SHA" 38
  fail_file dirty branch "$SUBJECT_SHA" 399 claim "$F1"
  # The branch list count is planted equal to the passed count, so the count gate alone would pass this
  # control: the case proves the FAILED check fires on its own.
  expect "dirty control" "VOID (a test FAILED in branch)" "$(control_verdict dirty "d232=31 branch=399 reap=40")"

  ok_file empty d232 "$SUBJECT_SHA" 31; ok_file empty reap "$SUBJECT_SHA" 38
  plant empty branch "$SUBJECT_SHA" 0 "running 0 tests" \
    "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2600 filtered out; finished in 0.00s"
  expect "control that collected nothing" "VOID" "$(control_verdict empty "d232=31 branch=0 reap=40")"

  ok_file short d232 "$SUBJECT_SHA" 29; ok_file short branch "$SUBJECT_SHA" 400; ok_file short reap "$SUBJECT_SHA" 38
  expect "control whose d232 list is not the registered 31" "VOID (d232: listed 29" \
    "$(control_verdict short "d232=29 branch=400 reap=40")"

  # Every registered arm, planted exactly as registered, from the tables the run uses.
  i=0
  while [ "$i" -lt "${#ARMS[@]}" ]; do plant_registered "$i"; i=$((i + 1)); done
  expect "every registered arm as registered" "0" "$(all_registered 2>&1 | tail -n 1)"
  plant_strays
  expect "every registered arm, with stray fire_*/red_* and label-sharing files in \$OUT" "0" \
    "$(all_registered 2>&1 | tail -n 1)"
  # The aggregate's positive control: one arm turned into a survivor must count, or "0" above proves nothing.
  row 0
  ok_file "$r_label" "$r_t" "$r_sha" "$r_sel"
  expect "the aggregate counts one arm that survived" "1" "$(all_registered 2>/dev/null | tail -n 1)"
  plant_registered 0

  # The cases below use drain-no-owner-check: four killers, two per catalog.
  name=d232-mut-drain-no-owner-check
  sha=b3a23ffd73312dd2cb2f8afbff5be81569b6de77
  # shellcheck disable=SC2086
  set -- $F3_FOREIGN $F3_ABSENT
  k1=$1; k2=$2; k3=$3

  fail_file "$name" d232 "$sha" 25 claim "$k1" "$k2" "$k3"
  expect "PARTIAL kill: one catalog's run of a killer passed" "PARTIAL" \
    "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  fail_file "$name" d232 "$sha" 23 claim $F3_FOREIGN $F3_ABSENT "$F1"
  expect "unexpected extra failure" "MISMATCH" "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  fail_file "$name" d232 "$sha" 24 fixture $F3_FOREIGN $F3_ABSENT
  expect "killers failed on their premise" "FIXTURE-FAIL" "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  ok_file "$name" d232 "$sha" 28
  expect "survivor" "SURVIVED" "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  fail_file "$name" d232 "$sha" 23 claim $F3_FOREIGN $F3_ABSENT
  expect "selected one fewer than registered" "SELECTED-MISMATCH" \
    "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  fail_file "$name" d232 "ae36eae6eba6eb76f66e03ac98e63f05ac25c814" 24 claim $F3_FOREIGN $F3_ABSENT
  expect "output from another commit (the mutant's base)" "STALE" \
    "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  rm -f "$(tfile "$name" d232)"
  expect "no output file" "INCOMPLETE (d232: no output file)" \
    "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  printf '%s\n' "arm=$sha" "running 28 tests" > "$(tfile "$name" d232)"
  expect "no rc line (killed mid-run)" "INCOMPLETE (d232: no rc line)" \
    "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  printf '%s\n' "running 28 tests" "rc=101" > "$(tfile "$name" d232)"
  expect "no arm line" "INCOMPLETE (d232: no arm line)" "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  plant "$name" d232 "$sha" 124 "running 28 tests" "test $k1 has been running for over 60 seconds"
  expect "timeout" "TIMEOUT" "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  plant "$name" d232 "$sha" 101 "error[E0308]: mismatched types" \
    "error: could not compile \`ferrodb\` (lib test) due to 1 previous error"
  expect "compile failure" "COMPILE-FAIL" "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  plant "$name" d232 "$sha" 0 "test $k1 ... FAILED" \
    "test result: FAILED. 27 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  expect "rc=0 with a FAILED line" "RC-MISMATCH" "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  plant "$name" d232 "$sha" 0 "running 28 tests" \
    "test result: ok. 28 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s" \
    "test result: ok. 28 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  expect "two result lines for a one-binary target" "INCOMPLETE (d232: 2 result lines)" \
    "$(verdict "$name" d232 "$sha" 28 "$F3_FOREIGN $F3_ABSENT")"

  name=d232-mut-image-reserved-from-counter
  sha=005a7910e003cb1f1cfec99c5ce448e99570bcc5
  fail_file "$name" d232 "$sha" 30 other "$B2_IMAGE"
  expect "the killer failed with an unregistered message" "MISMATCH" "$(verdict "$name" d232 "$sha" 31 "$B2_IMAGE")"

  # A message is asked only of the row that registers its test: this label's B2_IMAGE message is not asked of
  # a row on another target whose killer is another test.
  fail_file "$name" arena "$sha" 30 other "$B2_LOAD"
  expect "a message registered for another row's test is not asked of this row" "AS-REGISTERED" \
    "$(verdict "$name" arena "$sha" 31 "$B2_LOAD")"

  # Registration: every mutant has a required killer, a row, a base, and every message names a killer.
  for m in "${BASES[@]}"; do
    IFS='|' read -r name rest <<< "$m"
    v=$(printf '%s\n' "${ARMS[@]}" | awk -F'|' -v n="$name" '$1 == n && $5 != "" { c++ } END { print c + 0 }')
    [ "$v" -gt 0 ] || { echo "self-test FAIL  registration: $name has no ARMS row with a killer"; st_bad=$((st_bad + 1)); }
  done
  v=$(printf '%s\n' "${ARMS[@]}" | awk -F'|' '$1 ~ /^d232-mut-/ { print $1 }' | sort -u | wc -l | tr -d ' ')
  [ "$v" -eq "${#BASES[@]}" ] || { echo "self-test FAIL  registration: $v mutants in ARMS, ${#BASES[@]} in BASES"; st_bad=$((st_bad + 1)); }
  for m in "${MSGS[@]}"; do
    IFS='|' read -r name k1 rest <<< "$m"
    printf '%s\n' "${ARMS[@]}" | awk -F'|' -v n="$name" -v k="$k1" '$1 == n { split($5, a, " "); for (x in a) if (a[x] == k) f = 1 } END { exit !f }' ||
      { echo "self-test FAIL  registration: message for $name names $k1, not one of its killers"; st_bad=$((st_bad + 1)); }
  done
  [ "$st_bad" -eq 0 ] && echo "self-test PASS  registration: every mutant has a row, a base and a killer; every message names a killer"

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
want=$(printf '%s\n' "${ARMS[@]}" | awk -F'|' '$1 ~ /^d232-mut-/ { print $1 " " $2 }' | sort -u)
have=$(git for-each-ref --format='%(refname:short) %(objectname)' 'refs/heads/d232-mut-*' | sort)
if [ "$want" != "$have" ]; then
  echo "REFUSED: the d232-mut-* branches differ from the registered list" >&2
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
# All four: an untrapped HUP ran the EXIT trap, which restored src/ while cargo went on building a mutant
# against it (D237 judge review J5).
trap on_exit EXIT
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 131' QUIT
trap 'on_signal 143' TERM

echo "== control: $SUBJECT_SHA"
for t in $CONTROL_TARGETS; do run_target control "$t" "$SUBJECT_SHA"; done
lists=""
for t in $CONTROL_TARGETS; do list_target "$t"; done
for t in $CONTROL_TARGETS; do lists="$lists $t=$(listed_count "$t")"; done
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
