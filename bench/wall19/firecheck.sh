#!/usr/bin/env bash
# Wall #19 + D199 fire-check: each mutant reverts ONE piece of the refusal, the reap pruning, or the
# lease-reap attestation, and must turn its NAMED killers red. Pre-registration: artie-research
# frontier/lane_wall19_attested.md §12 (it supersedes the counts of 10.6, 10.7 and 11). Run from the
# worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work: it runs only when FAN-QUEUE row #14
# is released.
#
# Usage: bash bench/wall19/firecheck.sh               the run
#        bash bench/wall19/firecheck.sh --self-test   the judge on planted outputs only: no cargo, no git writes
#
# Targets: lib (--lib branch::attest), lease (--lib lease_thread::tests::, the whole lease-thread test
# module), integ (--test integration_branch_attestation). Each runs one test binary.
# Arms, in run order:
# - control: SUBJECT_SHA: every file OK, nothing FAILED, 0 ignored, and exactly the pre-registered passed
#   counts (34 / 25 / 10). Anything else VOIDS the run (exit 2) (D199 seal review F5).
# - mutants: KILLED-AS-REGISTERED when every file is OK, every required killer FAILED, and nothing outside
#   required + optional did. Anything else (MISMATCH, SURVIVED, COMPILE-FAIL, TIMEOUT, INCOMPLETE, RC-*)
#   counts against the run.
#
# Blind spots, stated: three selections, not the whole suite, so collateral damage elsewhere is not seen.
# The judge reads cargo's own lines ("test result:", "test <name> ... FAILED") and the rc this script
# appends.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=2673674           # the tree these mutants were written against
OUT=bench/wall19/firecheck
CONTROL_COUNTS="lib=34 lease=25 integ=10" # pre-registered, lane §11 and §12

# name|file|old text (python literal)|new text (python literal)
# '|' is the field separator, so a literal pipe inside mutant text is written \u007c.
A=src/branch/attest.rs
R=src/agent_sql/runtime.rs
MUTANTS=(
  "M1_append_genesis_fallback|$A|"'"            None => return self.refuse(AppendRefused::NoLiveHead { branch }),\n"|"            None => Attestation::genesis(),\n"'
  "M2_fork_genesis_fallback|$A|"'"            None => return self.refuse(AppendRefused::NoLiveParent { parent }),\n"|"            None => Attestation::genesis(),\n"'
  "M3_trunk_reap_allowed|$A|"'"        if op == BranchOp::Reap && branch.is_trunk() {\n            return self.refuse(AppendRefused::TrunkReap);\n        }\n"|""'
  "M4_refusal_not_counted|$A|"'"        self.refused += 1;\n"|""'
  "M5_trunk_exemption_removed|$A|"'"            None if branch.is_trunk() => Attestation::genesis(),\n"|""'
  "M6_reap_keeps_head|$A|"'"        if e.op == BranchOp::Reap {\n            self.heads.remove(&e.branch);\n"|"        if false {\n            self.heads.remove(&e.branch);\n"'
  "M7_scan_forget_does_not_attest|$R|"'"\n            self.attest_landed_reaps(&gone);\n"|"\n"'
  "M8_sweep_forget_does_not_attest|$R|"'"\n                self.attest_landed_reaps(&gone);\n"|"\n"'
  "M9_no_head_check|$R|"'"            let Some(fork_epoch) = h.opened_at(branch) else {\n                continue;\n            };\n"|"            let fork_epoch = h.opened_at(branch).unwrap_or_default();\n"'
  "M10_lease_reap_published|$R|"'"            let _ = Self::append_reap(&mut h, branch, fork_epoch, false);\n"|"            let _ = Self::append_reap(&mut h, branch, fork_epoch, true);\n"'
  "M11_attest_before_landed|$R|"'"                Ok(r) if r.generation > b.generation => landed.push(b),\n"|"                Ok(_) => landed.push(b),\n"'
  "M12_lease_reap_current_epoch|$R|"'"            let _ = Self::append_reap(&mut h, branch, fork_epoch, false);\n"|"            let _ = Self::append_reap(&mut h, branch, self.branches.current_epoch(), false);\n"'
  "M13_opened_follows_latest|$A|"'"            let opened = self.heads.get(&e.branch).map_or(e.epoch, \u007ct\u007c t.opened);\n"|"            let opened = e.epoch;\n"'
  "M14_unreadable_not_counted|$R|"'"        for branch in unreadable {\n            if h.head_of(branch).is_some() {\n                h.count_refusal();\n            }\n"|"        for branch in unreadable {\n            if h.head_of(branch).is_some() {\n            }\n"'
  "M15_unreadable_as_landed|$R|"'"                Err(_) => unreadable.push(b),\n"|"                Err(_) => landed.push(b),\n"'
  "M16_first_entry_not_last|$A|"'"            if e.branch == branch {\n                last = Some(i);\n"|"            if e.branch == branch && last.is_none() {\n                last = Some(i);\n"'
  "M17_seal_attests_behind_the_fallible_step|$R|"'"        let retired = self.retire(branch);\n"|"        let retired = Ok::<(), FerroError>(self.retire(branch)?);\n"'
  "M18_seal_attests_a_flip_it_did_not_make|$R|"'"            .filter(\u007cr\u007c r.generation == branch.generation && r.state != BranchState::Reaped);\n"|"            ;\n"'
  "M19_seal_unreadable_not_counted|$R|"'"            Err(_) => {\n                let mut h = self.attested.lock().unwrap();\n                if h.head_of(branch).is_some() {\n                    h.count_refusal();\n                }\n            }\n"|"            Err(_) => {}\n"'
  "M20_seal_unreadable_guessed_landed|$R|"'"            Err(_) => {\n                let mut h = self.attested.lock().unwrap();\n                if h.head_of(branch).is_some() {\n                    h.count_refusal();\n                }\n            }\n"|"            Err(_) => {\n                let _ = self.attest_reap(branch, fork_epoch, published);\n            }\n"'
)
# name|required killers (all must FAIL)|optional killers (lane §12)
A_REG=a_branch_with_no_live_head_is_refused_and_a_reaped_walk_keeps_its_reap
A_RECYCLED=a_recycled_id_slot_does_not_inherit_the_reaped_branchs_chain
A_REAPED=reaped_branches_leave_the_per_branch_index_and_every_proof_survives
A_OPENED=opened_at_is_the_fork_epoch_and_leaves_with_the_head
A_VBF=verify_branch_findings_do_not_name_a_digest_nothing_produced
A_M5="the_incremental_tree_matches_the_rfc_recursion an_honest_history_verifies verification_does_not_fire_on_legitimate_histories replaying_a_fork_entry_cannot_truncate_a_branchs_ancestry"
L_LEASE=a_lease_expiry_reap_is_attested_exactly_once
L_LANDS=a_reap_is_attested_once_it_lands_even_if_its_workspace_was_forgotten_first
L_UNREAD=an_unreadable_record_is_counted_not_guessed
L_RECON=a_reap_the_reconciliation_finds_is_attested_exactly_once
L_SEALFAIL=a_seal_whose_reap_fails_after_the_flip_is_still_attested
L_SEALUNREAD=a_seal_that_cannot_read_its_record_after_the_flip_counts_it_not_guesses
I_OUT=a_branch_outside_the_log_cannot_parent_a_session_and_its_reap_writes_nothing
I_REAPED=a_reaped_branch_keeps_its_proofs_and_loses_its_head
I_ALTER=altering_one_entry_breaks_the_chain
I_8="a_fork_a_merge_and_a_reap_each_leave_an_attested_entry a_childs_chain_walks_into_the_parent_it_forked_from the_merge_entry_commits_to_the_rows_it_published $I_ALTER a_published_head_catches_a_rewrite_that_the_chain_walk_accepts an_auditor_can_check_every_entry_against_a_published_head a_reap_through_an_attached_reaper_is_attested_too $I_REAPED"
KILLERS=(
  "M1_append_genesis_fallback|$A_REG $A_RECYCLED $I_OUT|"
  "M2_fork_genesis_fallback|$A_REG $I_OUT|"
  "M3_trunk_reap_allowed|$A_REG|"
  "M4_refusal_not_counted|$A_REG $L_UNREAD $L_SEALUNREAD $I_OUT|"
  "M5_trunk_exemption_removed|$A_M5 $A_RECYCLED $A_REAPED $A_REG $I_8|"
  "M6_reap_keeps_head|$A_REAPED $A_REG $A_OPENED $L_LEASE $L_SEALFAIL $I_REAPED|"
  "M7_scan_forget_does_not_attest|$L_LEASE $L_LANDS $L_UNREAD|"
  "M8_sweep_forget_does_not_attest|$L_RECON|"
  "M9_no_head_check|$L_LEASE $L_SEALFAIL|"
  "M10_lease_reap_published|$L_LEASE|"
  "M11_attest_before_landed|$L_LANDS|"
  "M12_lease_reap_current_epoch|$L_LEASE|"
  "M13_opened_follows_latest|$A_OPENED|"
  "M14_unreadable_not_counted|$L_UNREAD|"
  "M15_unreadable_as_landed|$L_UNREAD|"
  "M16_first_entry_not_last|$A_REAPED $A_REG $A_VBF $L_LEASE $I_REAPED $I_ALTER|"
  "M17_seal_attests_behind_the_fallible_step|$L_SEALFAIL|$L_SEALUNREAD"
  "M18_seal_attests_a_flip_it_did_not_make|$L_LEASE $L_SEALFAIL|"
  "M19_seal_unreadable_not_counted|$L_SEALUNREAD|"
  "M20_seal_unreadable_guessed_landed|$L_SEALUNREAD|"
)
TARGETS="lib lease integ"

target_args() { # $1 = target
  case "$1" in
    lib)   echo "--lib branch::attest" ;;
    lease) echo "--lib lease_thread::tests::" ;;
    integ) echo "--test integration_branch_attestation" ;;
  esac
}
binaries() { echo 1; } # test binaries per target: one each here

# A command runs in the background and the script waits on it, so a TERM or INT reaches the traps at once
# instead of after the command (bash defers a trap until a foreground child exits, and `timeout` puts
# cargo in its own process group, out of reach of a signal to this script's group). The trap stops the
# child before src/ is restored.
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

run_target() { # $1 = label, $2 = target
  local rc
  # shellcheck disable=SC2046
  waited timeout 1800 cargo test $(target_args "$2") > "$OUT/$1.$2.txt" 2>&1
  rc=$?
  echo "rc=$rc" >> "$OUT/$1.$2.txt"
}

# ---- The judge. It reads ONLY "$OUT/<label>.<target>.txt" for the targets it is given. Any other file in
# $OUT (a diffstat, the self-test log, a stale output) cannot change a verdict (D233 review 3 H1). ----
tfile() { printf '%s/%s.%s.txt' "$OUT" "$1" "$2"; }

# One target file's state: OK, or the reason its names cannot be judged.
tstate() { # $1 label, $2 target
  local f last rc n nf
  f=$(tfile "$1" "$2")
  [ -f "$f" ] || { echo "INCOMPLETE ($2: no output file)"; return; }
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

arm_state() { # $1 label, then the targets the arm ran: the first state that is not OK, or OK
  local label=$1 t s
  shift
  for t in "$@"; do
    s=$(tstate "$label" "$t")
    [ "$s" = OK ] || { echo "$s"; return; }
  done
  echo OK
}

failed_names() { # $1 label, then targets: short names of FAILED tests
  local label=$1 t
  shift
  for t in "$@"; do
    sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' "$(tfile "$label" "$t")"
  done | sed -E 's/.*:://' | sort -u
}

count_in() { # $1 label, $2 target, $3 passed|ignored: summed over the file's result lines
  grep -E '^test result:' "$(tfile "$1" "$2")" | sed -nE "s/.* ([0-9]+) $3.*/\\1/p" | awk '{ s += $1 } END { print s + 0 }'
}

words() { printf '%s\n' $1 | sed '/^$/d' | sort -u; }

verdict() { # $1 label, $2 required, $3 optional, then the targets the arm ran
  local label=$1 req=$2 opt=$3 st actual extra missing
  shift 3
  st=$(arm_state "$label" "$@")
  [ "$st" = OK ] || { echo "$st"; return; }
  actual=$(failed_names "$label" "$@")
  if [ -z "$actual" ]; then echo "SURVIVED"; return; fi
  extra=$(comm -13 <(words "$req $opt") <(printf '%s\n' "$actual" | sed '/^$/d'))
  missing=$(comm -23 <(words "$req") <(printf '%s\n' "$actual"))
  if [ -z "$missing" ] && [ -z "$extra" ]; then echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else echo "MISMATCH (missing: $(echo $missing); unexpected: $(echo $extra))"; fi
}

control_verdict() { # $1 label, $2 "target=count ..." (the pre-registered passed counts)
  local label=$1 counts=$2 st t want p i
  # shellcheck disable=SC2086
  st=$(arm_state "$label" $TARGETS)
  [ "$st" = OK ] || { echo "VOID ($st)"; return; }
  # shellcheck disable=SC2086
  if [ -n "$(failed_names "$label" $TARGETS)" ]; then echo "VOID (a test FAILED)"; return; fi
  for t in $TARGETS; do
    # shellcheck disable=SC2086
    want=$(printf '%s\n' $counts | sed -n "s/^$t=//p")
    p=$(count_in "$label" "$t" passed)
    i=$(count_in "$label" "$t" ignored)
    case "$want" in ''|*[!0-9]*) echo "VOID ($t: no registered count)"; return ;; esac
    if [ "$p" -ne "$want" ] || [ "$i" -ne 0 ]; then
      echo "VOID ($t: passed $p, ignored $i; registered $want)"; return
    fi
  done
  echo clean
}

ok_verdict() { case "$1" in KILLED-AS-REGISTERED*) return 0 ;; *) return 1 ;; esac; }

killers_of() { # $1 mutant name: sets req and opt from KILLERS; returns 1 if it has no row
  local k kname kreq kopt
  req=""; opt=""
  for k in "${KILLERS[@]}"; do
    IFS='|' read -r kname kreq kopt <<< "$k"
    if [ "$kname" = "$1" ]; then req=$kreq; opt=$kopt; return 0; fi
  done
  return 1
}

# ---- --self-test: the judge on planted outputs, in a temporary directory. No cargo, no git writes. ----
plant() { # $1 label, $2 target, $3 rc, then the file's lines
  local f rc line
  f=$(tfile "$1" "$2"); rc=$3
  shift 3
  { for line in "$@"; do printf '%s\n' "$line"; done; echo "rc=$rc"; } > "$f"
}
ok_file() { # $1 label, $2 target, $3 passed
  plant "$1" "$2" 0 "running $3 tests" "test result: ok. $3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
}
fail_file() { # $1 label, $2 target, $3 passed, then the full names of the FAILED tests
  local label=$1 t=$2 p=$3 n body=""
  shift 3
  for n in "$@"; do body="${body}test $n ... FAILED
"; done
  plant "$label" "$t" 101 "running tests" "${body%?}" "" "failures:" \
    "test result: FAILED. $p passed; $# failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
}
expect() { # $1 case, $2 expected verdict prefix, $3 actual verdict
  case "$3" in
    "$2"*) echo "self-test PASS  $1: $3" ;;
    *) echo "self-test FAIL  $1: expected '$2', got '$3'"; st_bad=$((st_bad + 1)) ;;
  esac
}
clean_set() { ok_file "$1" lib 34; ok_file "$1" lease 25; ok_file "$1" integ 10; }

self_test() {
  local keep=$OUT st_bad=0 tmp m name rest t
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/wall19-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp

  clean_set control
  expect "clean control" "clean" "$(control_verdict control "$CONTROL_COUNTS")"

  ok_file short lib 33; ok_file short lease 25; ok_file short integ 10
  expect "control short of its registered count" "VOID (lib: passed 33" "$(control_verdict short "$CONTROL_COUNTS")"

  ok_file ign lib 34; ok_file ign integ 10
  plant ign lease 0 "running 26 tests" "test result: ok. 25 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  expect "control with an ignored test" "VOID (lease: passed 25, ignored 1" "$(control_verdict ign "$CONTROL_COUNTS")"

  ok_file dirty lib 34; ok_file dirty integ 10
  fail_file dirty lease 25 "branch::lease_thread::tests::$L_LEASE"
  # 25 passed plus a FAILED one: the count gate alone would pass this control, so the case proves the
  # FAILED check fires on its own.
  expect "dirty control" "VOID (a test FAILED)" "$(control_verdict dirty "$CONTROL_COUNTS")"

  # M4 with its registered sets (three targets), read from the table the run uses.
  killers_of M4_refusal_not_counted || { echo "self-test FAIL  M4 has no KILLERS row"; st_bad=$((st_bad + 1)); }
  fail_file M4 lib 33 "branch::attest::tests::$A_REG"
  fail_file M4 lease 23 "branch::lease_thread::tests::$L_UNREAD" "branch::lease_thread::tests::$L_SEALUNREAD"
  fail_file M4 integ 9 "$I_OUT"
  # shellcheck disable=SC2086
  expect "killed mutant" "KILLED-AS-REGISTERED" "$(verdict M4 "$req" "$opt" $TARGETS)"

  # The same outputs, with stray files that share the label. Each holds no result line and an unregistered
  # FAILED test, so a judge that read them would say COMPILE-FAIL or MISMATCH.
  for name in "M4.diffstat" "M4.diffstat.txt" "M4.stale.txt"; do
    printf '%s\n' " src/branch/attest.rs | 1 -" "test stray::not_a_registered_killer ... FAILED" > "$OUT/$name"
  done
  # shellcheck disable=SC2086
  expect "killed mutant, stray files in \$OUT" "KILLED-AS-REGISTERED" "$(verdict M4 "$req" "$opt" $TARGETS)"

  # Every named killer, plus one test that is not named: not a kill as registered.
  fail_file plus lib 33 "branch::attest::tests::$A_REG"
  fail_file plus lease 22 "branch::lease_thread::tests::$L_UNREAD" "branch::lease_thread::tests::$L_SEALUNREAD" \
    "branch::lease_thread::tests::stop_does_not_wait_out_the_scan_interval"
  fail_file plus integ 9 "$I_OUT"
  # shellcheck disable=SC2086
  expect "named killers plus an unregistered failure" "MISMATCH (missing: ; unexpected: stop_does_not_wait_out_the_scan_interval)" "$(verdict plus "$req" "$opt" $TARGETS)"

  # A file with more result lines than the target has binaries cannot be judged by name.
  ok_file two lib 34; ok_file two integ 10
  plant two lease 0 "test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s" \
    "test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  # shellcheck disable=SC2086
  expect "two result lines in a one-binary target" "INCOMPLETE (lease: 2 result lines)" "$(verdict two "$req" "$opt" $TARGETS)"

  # A kill on a test that is not the mutant's named killer is not a kill (F5).
  fail_file other lib 33 "branch::attest::tests::genesis_is_not_zero"; ok_file other lease 25; ok_file other integ 10
  # shellcheck disable=SC2086
  expect "a failure that is not the named killer" "MISMATCH" "$(verdict other "$req" "$opt" $TARGETS)"

  fail_file part lib 33 "branch::attest::tests::$A_REG"; ok_file part lease 25; fail_file part integ 9 "$I_OUT"
  # shellcheck disable=SC2086
  expect "a required killer missing" "MISMATCH (missing: " "$(verdict part "$req" "$opt" $TARGETS)"

  killers_of M17_seal_attests_behind_the_fallible_step || { echo "self-test FAIL  M17 has no KILLERS row"; st_bad=$((st_bad + 1)); }
  ok_file m17 lib 34; ok_file m17 integ 10
  fail_file m17 lease 23 "branch::lease_thread::tests::$L_SEALFAIL" "branch::lease_thread::tests::$L_SEALUNREAD"
  # shellcheck disable=SC2086
  expect "required plus optional killer" "KILLED-AS-REGISTERED" "$(verdict m17 "$req" "$opt" $TARGETS)"

  for t in $TARGETS; do plant cf "$t" 101 "error[E0308]: mismatched types" "error: could not compile \`ferrodb\` (lib test) due to 1 previous error"; done
  # shellcheck disable=SC2086
  expect "compile failure" "COMPILE-FAIL" "$(verdict cf "$req" "$opt" $TARGETS)"

  ok_file to lib 34; ok_file to integ 10
  plant to lease 124 "running 25 tests" "test branch::lease_thread::tests::$L_SEALFAIL has been running for over 60 seconds"
  # shellcheck disable=SC2086
  expect "timeout" "TIMEOUT (lease)" "$(verdict to "$req" "$opt" $TARGETS)"

  ok_file gone lib 34; ok_file gone lease 25
  # shellcheck disable=SC2086
  expect "a target with no output file" "INCOMPLETE (integ: no output file)" "$(verdict gone "$req" "$opt" $TARGETS)"

  ok_file rcm lib 34; ok_file rcm integ 10
  plant rcm lease 0 "test branch::lease_thread::tests::$L_SEALFAIL ... FAILED" "test result: FAILED. 24 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  # shellcheck disable=SC2086
  expect "rc=0 with a FAILED line" "RC-MISMATCH" "$(verdict rcm "$req" "$opt" $TARGETS)"

  clean_set sv
  # shellcheck disable=SC2086
  expect "survivor" "SURVIVED" "$(verdict sv "$req" "$opt" $TARGETS)"

  for m in "${MUTANTS[@]}"; do
    IFS='|' read -r name rest <<< "$m"
    if ! killers_of "$name" || [ -z "$req" ]; then
      echo "self-test FAIL  registration: $name has no required killer"; st_bad=$((st_bad + 1))
    fi
  done
  [ "$st_bad" -eq 0 ] && echo "self-test PASS  registration: every mutant has a required killer"

  rm -rf "$OUT"
  OUT=$keep
  echo "self-test: $st_bad case(s) missed"
  [ "$st_bad" -eq 0 ]
}

if [ "${1:-}" = --self-test ]; then self_test; exit $?; fi
[ $# -eq 0 ] || { echo "usage: $0 [--self-test]" >&2; exit 2; }

if ! git diff --quiet "$SUBJECT_SHA" -- src/ tests/ examples/; then
  echo "REFUSED: src/, tests/ or examples/ differ from $SUBJECT_SHA; the mutants' text may not apply" >&2
  exit 2
fi
if [ -n "$(git status --porcelain -- src/ tests/ examples/)" ]; then
  echo "REFUSED: uncommitted changes under src/, tests/ or examples/" >&2
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
  git diff --quiet "$SUBJECT_SHA" -- src/ tests/ examples/ ||
    echo "ON EXIT: src/, tests/ or examples/ still differ from $SUBJECT_SHA; restore with: git checkout $SUBJECT_SHA -- src/" >&2
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

echo "== control (unmutated $SUBJECT_SHA)"
for t in $TARGETS; do run_target control "$t"; done
v=$(control_verdict control "$CONTROL_COUNTS")
echo "control: $v (registered: $CONTROL_COUNTS)" | tee "$OUT/summary.txt"
[ "$v" = clean ] || exit 2

bad=0
for m in "${MUTANTS[@]}"; do
  IFS='|' read -r name FILE old new <<< "$m"
  killers_of "$name" || { echo "$name: NO KILLERS ROW" | tee -a "$OUT/summary.txt"; bad=$((bad + 1)); continue; }
  echo "== $name"
  if ! timeout 60 python3 - "$FILE" "$old" "$new" <<'EOF'
import ast, sys
path, old, new = sys.argv[1], ast.literal_eval(sys.argv[2]), ast.literal_eval(sys.argv[3])
src = open(path).read()
n = src.count(old)
if n != 1:
    sys.exit(f"mutant text occurs {n} times, not once; refusing")
open(path, "w").write(src.replace(old, new))
EOF
  then
    echo "$name: NOT APPLIED" | tee -a "$OUT/summary.txt"
    bad=$((bad + 1))
    continue
  fi
  git diff --stat -- "$FILE" > "$OUT/$name.diffstat"
  for t in $TARGETS; do run_target "$name" "$t"; done
  git checkout "$SUBJECT_SHA" -- "$FILE"
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: restore of $FILE after $name left a difference" >&2; exit 3; }
  # shellcheck disable=SC2086
  v=$(verdict "$name" "$req" "$opt" $TARGETS)
  echo "$name: $v" | tee -a "$OUT/summary.txt"
  ok_verdict "$v" || bad=$((bad + 1))
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ examples/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
