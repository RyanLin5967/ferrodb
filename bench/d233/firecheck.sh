#!/usr/bin/env bash
# D233 fire-check, amendment 6 (review 3: H1 the judge reads only the target files it names; L4
# --no-fail-fast and a result line per binary; L6 src/ restored on any exit; L9 the control first; H2's
# TIMEOUT label; M1 Rc registered for U4/U5; --self-test on planted outputs). Pre-registration: artie-research
# frontier/lane_d233_free_at_empty.md §12, amended by §13 (subject 79eec23; V8 and W5; the L2 and N1 tests in
# U1's killers and in the base arms). The arms and killers of §9-§11 are otherwise unchanged.
# Mutants cover BPlusTreeManager's free-at-empty (index.rs) and fork's FREE_ID reuse (table_catalog.rs).
# Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work: it runs only when the
# lead releases the FAN-QUEUE row for D233.
#
# Usage: bash bench/d233/firecheck.sh               the run
#        bash bench/d233/firecheck.sh --self-test   the judge on planted outputs only: no cargo, no git writes
#
# Targets: index (--lib storage::index), catalog (--lib branch::table_catalog), collateral (three test
# binaries, --no-fail-fast), lockorder (--test lock_order_allowlist).
# Arms, in run order:
# - control: SUBJECT_SHA, all four targets: every file OK, nothing FAILED, and per target
#            passed + ignored == the harness's own `-- --list` count, both > 0. Anything else VOIDS the run (exit 2).
# - base-F3: 0eda6ca's src/ with this tip's index.rs test module spliced in; the index selection
#            must FAIL exactly Ra, Rb, Rc, G6T and the L2 test (the F3 red run, split per case, review 2 G4).
# - base-G:  f03e25d's src/ with this tip's index.rs and table_catalog.rs test modules spliced in;
#            index + catalog must FAIL exactly G6T, G2T, G9a, G9b, the L2 test and the N1 test.
# - base-L:  2b9d2c0's src/, --test lock_order_allowlist: exactly
#            every_pool_method_that_locks_opens_a_pool_section FAILS (amendment 5).
# - mutants: KILLED-AS-REGISTERED when every file is OK, every required killer FAILED and nothing outside
#            required + optional failed; the pre-registered survivors (U8, V4, V5) are
#            SURVIVED-AS-REGISTERED when nothing outside their optional set failed. Anything else
#            (MISMATCH, SURVIVED, COMPILE-FAIL, TIMEOUT, INCOMPLETE, RC-*) counts against the run.
#
# Blind spots, stated: four selections, not the whole suite. CC (the concurrent arm) is optional
# wherever it is not required, because it sees a mutant only when the scheduler hits the window. The judge
# reads cargo's own lines ("test result:", "test <name> ... FAILED") and the rc this script appends.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=79eec23
OUT=bench/d233/firecheck

I=src/storage/index.rs
C=src/branch/table_catalog.rs
# name|file|old text (python literal)|new text (python literal); '|' and a single quote inside the
# text are written \u007c and \u0027.
MUTANTS=(
  "U1_never_unlink|$I|"'"        leaf.key_arr.is_empty() && (leaf.prev.is_some() \u007c\u007c leaf.next.is_some())\n"|"        false && leaf.key_arr.is_empty()\n"'
  "U2_no_prev_splice|$I|"'"                left.next = leaf.next;\n"|""'
  "U3_no_next_splice|$I|"'"                right.prev = leaf.prev;\n"|""'
  "U4_parent_keeps_pointer|$I|"'"        self.write_page(parent_id, parent)\n"|"        let _ = (parent_id, parent);\n        Ok(())\n"'
  "U5_wrong_child_removed|$I|"'"            parent.child_ptrs.remove(slot);\n"|"            parent.child_ptrs.remove(if slot == 0 { 1 } else { slot - 1 });\n"'
  "U6_no_cascade|$I|"'"            if parent.child_ptrs.len() == 1 {\n                doomed = parent_id;\n                continue;\n            }\n"|""'
  "U7_no_refill_recheck|$I|"'"        let unlink = Self::would_unlink(&leaf);\n"|"        let unlink = true;\n"'
  "U8_no_root_recheck|$I|"'"    fn delete_unlinking(&self, key: &K) -> Result<(), FerroError> {\n        loop {\n            let root = self.root_page_id.load(Ordering::Acquire);\n            let mut guards: Vec<PageWriteGuard<\u0027_>> = vec![self.latches().write(root)];\n            if self.root_page_id.load(Ordering::Acquire) != root {\n                continue; // `guards` is dropped here\n            }\n"|"    fn delete_unlinking(&self, key: &K) -> Result<(), FerroError> {\n        loop {\n            let root = self.root_page_id.load(Ordering::Acquire);\n            let mut guards: Vec<PageWriteGuard<\u0027_>> = vec![self.latches().write(root)];\n"'
  "U9_no_neighbour_clause|$I|"'"        leaf.key_arr.is_empty() && (leaf.prev.is_some() \u007c\u007c leaf.next.is_some())\n"|"        leaf.key_arr.is_empty()\n"'
  "U10_walk_stops_on_empty|$I|"'"                    while leaf.key_arr.last().is_none_or(\u007cmax\u007c max < key) {\n"|"                    while leaf.key_arr.last().is_some_and(\u007cmax\u007c max < key) {\n"'
  "V1_no_prev_link_check|$I|"'"                if left.next != Some(leaf_id) {\n"|"                if false && left.next != Some(leaf_id) {\n"'
  "V2_no_next_link_check|$I|"'"                if right.prev != Some(leaf_id) {\n"|"                if false && right.prev != Some(leaf_id) {\n"'
  "V3_write_before_check|$I|"'"        let _left_latch = leaf.prev.map(\u007cid\u007c self.latches().write(id));\n"|"        self.write_page(leaf_id, leaf.serialize()?)?;\n        let _left_latch = leaf.prev.map(\u007cid\u007c self.latches().write(id));\n"'
  "V4_prev_guard_dropped|$I|"'"        let _left_latch = leaf.prev.map(\u007cid\u007c self.latches().write(id));\n"|"        let _ = leaf.prev.map(\u007cid\u007c self.latches().write(id));\n"'
  "V5_next_guard_dropped|$I|"'"        let _right_latch = leaf.next.map(\u007cid\u007c self.latches().write(id));\n"|"        let _ = leaf.next.map(\u007cid\u007c self.latches().write(id));\n"'
  "V6_parent_planned_after_writes|$I|"'"        let (parent_id, parent) = self.unlink_from_parent(stack, leaf_id)?;\n        let parent = parent.serialize()?;\n        let leaf = leaf.serialize()?;\n\n        // The writes, in the order a latch-free reader may see them (above).\n        self.write_page(leaf_id, leaf)?;\n        if let Some((prev_id, image)) = left {\n            self.write_page(prev_id, image)?;\n        }\n        if let Some((next_id, image)) = right {\n            self.write_page(next_id, image)?;\n        }\n        self.write_page(parent_id, parent)\n"|"        let leaf = leaf.serialize()?;\n\n        // The writes, in the order a latch-free reader may see them (above).\n        self.write_page(leaf_id, leaf)?;\n        if let Some((prev_id, image)) = left {\n            self.write_page(prev_id, image)?;\n        }\n        if let Some((next_id, image)) = right {\n            self.write_page(next_id, image)?;\n        }\n        let (parent_id, parent) = self.unlink_from_parent(stack, leaf_id)?;\n        let parent = parent.serialize()?;\n        self.write_page(parent_id, parent)\n"'
  "V7_no_self_naming_check|$I|"'"        if (leaf.prev.is_some() && leaf.prev == leaf.next) \u007c\u007c leaf.prev == Some(leaf_id) \u007c\u007c leaf.next == Some(leaf_id) {\n"|"        if false && ((leaf.prev.is_some() && leaf.prev == leaf.next) \u007c\u007c leaf.prev == Some(leaf_id) \u007c\u007c leaf.next == Some(leaf_id)) {\n"'
  "W1_reuse_the_head_whatever|$C|"'"            match self.reusable_slot(id) {\n"|"            match self.core(id) {\n"'
  "W2_fork_checks_reaped_only|$C|"'"            match self.reusable_slot(id) {\n"|"            match self.core(id).map(\u007cr\u007c r.filter(\u007cr\u007c r.state() == BranchState::Reaped)) {\n"'
  "W3_unreadable_fails_the_fork|$C|"'"                Ok(None) \u007c Err(_) => stale.push(k),\n"|"                Ok(None) => stale.push(k),\n                Err(e) => return Err(e),\n"'
  "W4_malformed_skipped_silently|$C|"'"                stale.push(k);\n                continue;\n"|"                continue;\n"'
  "V8_no_path_check|$I|"'"        if let Some(on_path) = [leaf.prev, leaf.next].into_iter().flatten().find(\u007cp\u007c stack.contains(p)) {\n"|"        if let Some(on_path) = [leaf.prev, leaf.next].into_iter().flatten().find(\u007cp\u007c false && stack.contains(p)) {\n"'
  "W5_removal_before_the_build|$C|"'"            let child = BranchRecord::fork_child_from_core(\n                &parent_core,\n                parent_envelope.as_ref(),\n                child_id,\n                fork_epoch,\n                lease,\n            )?;\n\n        for k in &stale {\n            self.stale_free_ids.fetch_add(1, Ordering::Relaxed);\n            self.remove_if_present(k)?;\n        }\n        if reused {\n            self.remove_if_present(&keys::free_id(child_num))?;\n        }\n"|"        for k in &stale {\n            self.stale_free_ids.fetch_add(1, Ordering::Relaxed);\n            self.remove_if_present(k)?;\n        }\n        if reused {\n            self.remove_if_present(&keys::free_id(child_num))?;\n        }\n\n            let child = BranchRecord::fork_child_from_core(\n                &parent_core,\n                parent_envelope.as_ref(),\n                child_id,\n                fork_epoch,\n                lease,\n            )?;\n\n"'
)
# name|kill or survivor|required killers|optional killers
# U4 and U5 require Rc (review 3 M1): each breaks the unlink Rc's premise asserts (lane §12).
# U1 requires the L2 test too; V8 and W5 are §13's (lane §13 addendum).
KILLERS=(
  "U1_never_unlink|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement a_reader_from_before_an_unlink_walks_off_the_unlinked_leaf a_refused_unlink_with_a_broken_prev_link_writes_nothing a_refused_unlink_with_a_broken_next_link_writes_nothing a_refused_cascade_that_would_empty_the_root_writes_nothing a_chain_whose_prev_is_its_next_is_refused_not_waited_on a_neighbour_on_the_descent_path_is_refused_not_waited_on unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied|"
  "U2_no_prev_splice|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied|"
  "U3_no_next_splice|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement|unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied"
  "U4_parent_keeps_pointer|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement a_reader_from_before_an_unlink_walks_off_the_unlinked_leaf unlinks_racing_refills_and_readers_lose_no_key a_refused_cascade_that_would_empty_the_root_writes_nothing|a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied"
  "U5_wrong_child_removed|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement a_refused_cascade_that_would_empty_the_root_writes_nothing|a_reader_from_before_an_unlink_walks_off_the_unlinked_leaf unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied"
  "U6_no_cascade|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement a_refused_cascade_that_would_empty_the_root_writes_nothing|unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied"
  "U7_no_refill_recheck|kill|the_unlink_path_keeps_a_leaf_a_writer_refilled|unlinks_racing_refills_and_readers_lose_no_key"
  "U8_no_root_recheck|survivor||unlinks_racing_refills_and_readers_lose_no_key"
  "U9_no_neighbour_clause|kill|deleting_the_last_key_of_the_only_leaf_keeps_a_usable_tree|"
  "U10_walk_stops_on_empty|kill|a_reader_from_before_an_unlink_walks_off_the_unlinked_leaf|unlinks_racing_refills_and_readers_lose_no_key"
  "V1_no_prev_link_check|kill|a_refused_unlink_with_a_broken_prev_link_writes_nothing|"
  "V2_no_next_link_check|kill|a_refused_unlink_with_a_broken_next_link_writes_nothing|"
  "V3_write_before_check|kill|a_refused_unlink_with_a_broken_prev_link_writes_nothing a_refused_unlink_with_a_broken_next_link_writes_nothing a_refused_cascade_that_would_empty_the_root_writes_nothing|"
  "V4_prev_guard_dropped|survivor||unlinks_racing_refills_and_readers_lose_no_key"
  "V5_next_guard_dropped|survivor||unlinks_racing_refills_and_readers_lose_no_key"
  "V6_parent_planned_after_writes|kill|a_refused_cascade_that_would_empty_the_root_writes_nothing|"
  "V7_no_self_naming_check|kill|a_chain_whose_prev_is_its_next_is_refused_not_waited_on|"
  "W1_reuse_the_head_whatever|kill|fork_never_reuses_a_free_id_whose_record_is_live a_stale_free_id_is_skipped_counted_and_removed_and_the_next_is_reused fork_never_reuses_a_reaped_slot_that_still_has_a_live_child|"
  "W2_fork_checks_reaped_only|kill|fork_never_reuses_a_reaped_slot_that_still_has_a_live_child|"
  "W3_unreadable_fails_the_fork|kill|a_free_id_whose_record_cannot_be_read_is_skipped_not_fatal|"
  "W4_malformed_skipped_silently|kill|a_malformed_free_id_key_is_counted_and_removed|"
  "V8_no_path_check|kill|a_neighbour_on_the_descent_path_is_refused_not_waited_on|"
  "W5_removal_before_the_build|kill|a_refused_fork_of_a_quarantined_parent_keeps_the_free_slot|"
)
TARGETS="index catalog collateral lockorder"

target_args() { # $1 = target
  case "$1" in
    index)      echo "--lib storage::index" ;;
    catalog)    echo "--lib branch::table_catalog" ;;
    collateral) echo "--no-fail-fast --test d58_latch_free_descent --test d126_atomic_upsert --test integration_btree_concurrency" ;;
    lockorder)  echo "--test lock_order_allowlist" ;;
  esac
}
binaries() { # $1 = target: test binaries it runs, so result lines it must show
  case "$1" in collateral) echo 3 ;; *) echo 1 ;; esac
}

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

listed_in() { # $1 = target: what the harness says it will run, at SUBJECT_SHA (the list is kept)
  # shellcheck disable=SC2046
  waited timeout 1800 cargo test $(target_args "$1") -- --list > "$OUT/control.$1.list" 2>&1
  grep -cE ': test$' "$OUT/control.$1.list"
}

# ---- The judge. It reads ONLY "$OUT/<label>.<target>.txt" for the targets it is given. Any other file in
# $OUT (a diffstat, a list, the self-test log, a stale output) cannot change a verdict (review 3 H1). ----
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

verdict() { # $1 label, $2 kill|survivor, $3 required, $4 optional, then the targets the arm ran
  local label=$1 kind=$2 req=$3 opt=$4 st actual extra missing
  shift 4
  st=$(arm_state "$label" "$@")
  [ "$st" = OK ] || { echo "$st"; return; }
  actual=$(failed_names "$label" "$@")
  extra=$(comm -13 <(words "$req $opt") <(printf '%s\n' "$actual" | sed '/^$/d'))
  if [ "$kind" = survivor ]; then
    if [ -z "$extra" ]; then echo "SURVIVED-AS-REGISTERED ($(echo $actual))"; else echo "MISMATCH (unexpected: $(echo $extra))"; fi
    return
  fi
  if [ -z "$actual" ]; then echo "SURVIVED"; return; fi
  missing=$(comm -23 <(words "$req") <(printf '%s\n' "$actual"))
  if [ -z "$missing" ] && [ -z "$extra" ]; then echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else echo "MISMATCH (missing: $(echo $missing); unexpected: $(echo $extra))"; fi
}

control_verdict() { # $1 label, $2 "target=listed ..." (the harness's own -- --list counts)
  local label=$1 lists=$2 st t l p i
  # shellcheck disable=SC2086
  st=$(arm_state "$label" $TARGETS)
  [ "$st" = OK ] || { echo "VOID ($st)"; return; }
  # shellcheck disable=SC2086
  if [ -n "$(failed_names "$label" $TARGETS)" ]; then echo "VOID (a test FAILED)"; return; fi
  for t in $TARGETS; do
    # shellcheck disable=SC2086
    l=$(printf '%s\n' $lists | sed -n "s/^$t=//p")
    p=$(count_in "$label" "$t" passed)
    i=$(count_in "$label" "$t" ignored)
    case "$l" in ''|*[!0-9]*) echo "VOID ($t: no list count)"; return ;; esac
    if [ "$l" -eq 0 ] || [ "$p" -eq 0 ] || [ $((p + i)) -ne "$l" ]; then
      echo "VOID ($t: listed $l, passed $p, ignored $i)"; return
    fi
  done
  echo clean
}

ok_verdict() { case "$1" in KILLED-AS-REGISTERED*|SURVIVED-AS-REGISTERED*) return 0 ;; *) return 1 ;; esac; }

killers_of() { # $1 mutant name: sets kind, req and opt from KILLERS; returns 1 if it has no row
  local k kname kkind kreq kopt
  kind=""; req=""; opt=""
  for k in "${KILLERS[@]}"; do
    IFS='|' read -r kname kkind kreq kopt <<< "$k"
    if [ "$kname" = "$1" ]; then kind=$kkind; req=$kreq; opt=$kopt; return 0; fi
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
upto() { # 1..$1, one per line (BSD seq counts down when $1 < 1)
  local i=1
  while [ "$i" -le "$1" ]; do echo "$i"; i=$((i + 1)); done
}
result_ok() { echo "test result: ok. $1 passed; 0 failed; $2 ignored; 0 measured; 0 filtered out; finished in 0.01s"; }
ok_file() { # $1 label, $2 target, $3 passed per binary (the collateral target gets three result lines)
  local b lines=""
  for b in $(upto "$(binaries "$2")"); do lines="${lines}running $3 tests
$(result_ok "$3" 0)
"; done
  plant "$1" "$2" 0 "${lines%?}"
}
fail_file() { # $1 label, $2 target, $3 passed, then the full names of the FAILED tests (one binary's worth)
  local label=$1 t=$2 p=$3 n body="" b more=""
  shift 3
  for n in "$@"; do body="${body}test $n ... FAILED
"; done
  for b in $(upto $(($(binaries "$t") - 1))); do more="${more}$(result_ok 5 0)
"; done
  plant "$label" "$t" 101 "running tests" "${body%?}" "" "failures:" \
    "test result: FAILED. $p passed; $# failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s" "${more%?}"
}
expect() { # $1 case, $2 expected verdict prefix, $3 actual verdict
  case "$3" in
    "$2"*) echo "self-test PASS  $1: $3" ;;
    *) echo "self-test FAIL  $1: expected '$2', got '$3'"; st_bad=$((st_bad + 1)) ;;
  esac
}
all_ok() { # $1 label, then targets to plant clean
  local label=$1 t
  shift
  for t in "$@"; do ok_file "$label" "$t" 4; done
}

self_test() {
  local keep=$OUT st_bad=0 tmp lists m name rest t w req1 base_req cc
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/d233-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp
  lists="index=4 catalog=4 collateral=12 lockorder=4"

  all_ok control index catalog collateral lockorder
  expect "clean control" "clean" "$(control_verdict control "$lists")"

  plant ign catalog 0 "running 6 tests" "$(result_ok 4 2)"
  all_ok ign index collateral lockorder
  expect "clean control with 2 ignored" "clean" "$(control_verdict ign "index=4 catalog=6 collateral=12 lockorder=4")"

  all_ok dirty catalog collateral lockorder
  fail_file dirty index 3 free_at_empty_keeps_the_chain_and_the_parents_in_agreement
  # The list count is planted equal to the passed count, so the count gate alone would pass this control:
  # the case proves the FAILED check fires on its own.
  expect "dirty control" "VOID (a test FAILED)" "$(control_verdict dirty "index=3 catalog=4 collateral=12 lockorder=4")"

  all_ok empty index catalog collateral
  plant empty lockorder 0 "running 0 tests" "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s"
  expect "control that collected nothing" "VOID" "$(control_verdict empty "index=4 catalog=4 collateral=12 lockorder=0")"

  # U4 with its registered sets, read from the table the run uses.
  killers_of U4_parent_keeps_pointer || { echo "self-test FAIL  U4 has no KILLERS row"; st_bad=$((st_bad + 1)); }
  # shellcheck disable=SC2046
  fail_file U4 index 1 $(printf 'storage::index::tests::%s ' $req)
  all_ok U4 catalog collateral lockorder
  # shellcheck disable=SC2086
  expect "killed mutant" "KILLED-AS-REGISTERED" "$(verdict U4 "$kind" "$req" "$opt" $TARGETS)"

  # The same outputs, with stray files that share the label. Each holds no result line and an unregistered
  # FAILED test, so a judge that read them would say COMPILE-FAIL or MISMATCH.
  for name in "U4.diffstat" "U4.diffstat.txt" "U4.stale.txt"; do
    printf '%s\n' " src/storage/index.rs | 3 +--" "test stray::not_a_registered_killer ... FAILED" > "$OUT/$name"
  done
  # shellcheck disable=SC2086
  expect "killed mutant, stray files in \$OUT" "KILLED-AS-REGISTERED" "$(verdict U4 "$kind" "$req" "$opt" $TARGETS)"

  req1=${req%% *}
  for t in $TARGETS; do plant cf "$t" 101 "error[E0308]: mismatched types" "error: could not compile \`ferrodb\` (lib test) due to 1 previous error"; done
  # shellcheck disable=SC2086
  expect "compile failure" "COMPILE-FAIL" "$(verdict cf "$kind" "$req" "$opt" $TARGETS)"

  all_ok to catalog collateral lockorder
  plant to index 124 "running 40 tests" "test storage::index::tests::unlinks_racing_refills_and_readers_lose_no_key has been running for over 60 seconds"
  # shellcheck disable=SC2086
  expect "timeout" "TIMEOUT (index)" "$(verdict to "$kind" "$req" "$opt" $TARGETS)"

  all_ok gone index catalog lockorder
  # shellcheck disable=SC2086
  expect "a target with no output file" "INCOMPLETE (collateral: no output file)" "$(verdict gone "$kind" "$req" "$opt" $TARGETS)"

  all_ok nfs index catalog lockorder
  plant nfs collateral 101 "test d58::x ... ok" "$(result_ok 4 0)" "test d126::y ... FAILED" \
    "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s" \
    "error: test failed, to rerun pass \`--test d126_atomic_upsert\`"
  # shellcheck disable=SC2086
  expect "collateral with 2 result lines" "INCOMPLETE (collateral: 2 result lines)" "$(verdict nfs "$kind" "$req" "$opt" $TARGETS)"

  all_ok rcm catalog collateral lockorder
  plant rcm index 0 "test storage::index::tests::$req1 ... FAILED" "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  # shellcheck disable=SC2086
  expect "rc=0 with a FAILED line" "RC-MISMATCH" "$(verdict rcm "$kind" "$req" "$opt" $TARGETS)"

  all_ok sv index catalog collateral lockorder
  # shellcheck disable=SC2086
  expect "survivor" "SURVIVED" "$(verdict sv "$kind" "$req" "$opt" $TARGETS)"

  # shellcheck disable=SC2046
  fail_file mm index 1 $(printf 'storage::index::tests::%s ' $req) storage::index::tests::not_a_registered_killer
  all_ok mm catalog collateral lockorder
  # shellcheck disable=SC2086
  expect "unexpected failure" "MISMATCH" "$(verdict mm "$kind" "$req" "$opt" $TARGETS)"

  killers_of U8_no_root_recheck || { echo "self-test FAIL  U8 has no KILLERS row"; st_bad=$((st_bad + 1)); }
  all_ok u8 index catalog collateral lockorder
  # shellcheck disable=SC2086
  expect "registered survivor, nothing FAILED" "SURVIVED-AS-REGISTERED" "$(verdict u8 "$kind" "$req" "$opt" $TARGETS)"
  cc=${opt%% *}
  fail_file u8cc index 3 "storage::index::tests::$cc"
  all_ok u8cc catalog collateral lockorder
  # shellcheck disable=SC2086
  expect "registered survivor, its optional killer FAILED" "SURVIVED-AS-REGISTERED" "$(verdict u8cc "$kind" "$req" "$opt" $TARGETS)"
  fail_file u8x index 3 "storage::index::tests::$cc" storage::index::tests::not_a_registered_killer
  all_ok u8x catalog collateral lockorder
  # shellcheck disable=SC2086
  expect "registered survivor, an unregistered test FAILED" "MISMATCH" "$(verdict u8x "$kind" "$req" "$opt" $TARGETS)"

  base_req="every_pool_method_that_locks_opens_a_pool_section"
  fail_file base-L lockorder 3 "$base_req"
  expect "base arm" "KILLED-AS-REGISTERED" "$(verdict base-L kill "$base_req" "" lockorder)"

  for m in "${MUTANTS[@]}"; do
    IFS='|' read -r name rest <<< "$m"
    if ! killers_of "$name"; then
      echo "self-test FAIL  registration: $name has no KILLERS row"; st_bad=$((st_bad + 1)); continue
    fi
    case "$kind" in
      kill) [ -n "$req" ] || { echo "self-test FAIL  registration: $name has no required killer"; st_bad=$((st_bad + 1)); } ;;
      survivor) [ -z "$req" ] || { echo "self-test FAIL  registration: survivor $name lists a required killer"; st_bad=$((st_bad + 1)); } ;;
      *) echo "self-test FAIL  registration: $name has kind '$kind'"; st_bad=$((st_bad + 1)) ;;
    esac
  done
  [ "$st_bad" -eq 0 ] && echo "self-test PASS  registration: every mutant has a row, and every kill a required killer"

  rm -rf "$OUT"
  OUT=$keep
  echo "self-test: $st_bad case(s) missed"
  [ "$st_bad" -eq 0 ]
}

# Splice SUBJECT_SHA's test module of $1 into the working copy of $1 (which holds an older src/).
splice_tests() { # $1 = file, $2 = "old-counter" to read the pre-amendment-5 counter
  timeout 60 python3 - "$1" "$SUBJECT_SHA" "${2:-}" <<'EOF'
import subprocess, sys
path, sha, mode = sys.argv[1], sys.argv[2], sys.argv[3]
tip = subprocess.run(["git", "show", f"{sha}:{path}"], capture_output=True, text=True, check=True).stdout
old = open(path).read()
if path.endswith("index.rs"):
    anchor = "#[cfg(test)]\nmod tests {\n"
    def cut(t):
        assert t.count(anchor) == 1, "index.rs test-module anchor not unique"
        i = t.index(anchor)
        return t[:i], t[i:], ""
else:
    start = "#[cfg(test)]\nmod tests {\n    use super::*;\n    use crate::storage::disk_manager::DiskManager;\n"
    end = "\n}\n\n/// See [`TableBranchCatalog::child_liveness`]."
    def cut(t):
        assert t.count(start) == 1 and t.count(end) == 1, "table_catalog.rs test-module anchors not unique"
        i = t.index(start)
        j = t.index(end) + 3
        return t[:i], t[i:j], t[j:]
head, _, tail = cut(old)
_, module, _ = cut(tip)
if mode == "old-counter":
    new_name, old_name = "crate::buffer::page_reads::on_this_thread()", "crate::buffer::buffer_pool::page_reads_on_this_thread()"
    assert module.count(new_name) >= 1, "the counter call to rename is missing"
    module = module.replace(new_name, old_name)
open(path, "w").write(head + module + tail)
EOF
}

if [ "${1:-}" = --self-test ]; then self_test; exit $?; fi
[ $# -eq 0 ] || { echo "usage: $0 [--self-test]" >&2; exit 2; }

if ! git diff --quiet "$SUBJECT_SHA" -- src/ tests/; then
  echo "REFUSED: src/ or tests/ differ from $SUBJECT_SHA; the mutants' text may not apply" >&2
  exit 2
fi
if [ -n "$(git status --porcelain -- src/ tests/)" ]; then
  echo "REFUSED: uncommitted changes under src/ or tests/" >&2
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

bad=0
restore() {
  git checkout "$SUBJECT_SHA" -- src/
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after $1" >&2; exit 3; }
}

echo "== control: $SUBJECT_SHA"
for t in $TARGETS; do run_target control "$t"; done
lists=""
for t in $TARGETS; do lists="$lists $t=$(listed_in "$t")"; done
v=$(control_verdict control "$lists")
echo "control: $v (lists:$lists)" | tee "$OUT/summary.txt"
[ "$v" = clean ] || exit 2

echo "== base-F3: 0eda6ca src/ + this tip's index.rs tests"
git checkout 0eda6ca -- src/
splice_tests "$I" || { echo "ABORT: splice failed (base-F3)" >&2; restore base-F3; exit 3; }
run_target base-F3 index
restore base-F3
v=$(verdict base-F3 kill "a_refused_unlink_with_a_broken_prev_link_writes_nothing a_refused_unlink_with_a_broken_next_link_writes_nothing a_refused_cascade_that_would_empty_the_root_writes_nothing a_chain_whose_prev_is_its_next_is_refused_not_waited_on a_neighbour_on_the_descent_path_is_refused_not_waited_on" "" index)
echo "base-F3: $v" | tee -a "$OUT/summary.txt"
ok_verdict "$v" || bad=$((bad + 1))

echo "== base-G: f03e25d src/ + this tip's index.rs and table_catalog.rs tests"
git checkout f03e25d -- src/
# This tip's table_catalog tests read the page-read counter as `buffer::page_reads::on_this_thread()`
# (amendment 5); f03e25d has the same counter as `buffer::buffer_pool::page_reads_on_this_thread()`.
# The splice renames the call so the spliced tests read the counter f03e25d actually increments.
{ splice_tests "$I" && splice_tests "$C" old-counter; } || { echo "ABORT: splice failed (base-G)" >&2; restore base-G; exit 3; }
run_target base-G index
run_target base-G catalog
restore base-G
v=$(verdict base-G kill "a_chain_whose_prev_is_its_next_is_refused_not_waited_on fork_never_reuses_a_reaped_slot_that_still_has_a_live_child a_free_id_whose_record_cannot_be_read_is_skipped_not_fatal a_malformed_free_id_key_is_counted_and_removed a_neighbour_on_the_descent_path_is_refused_not_waited_on a_refused_fork_of_a_quarantined_parent_keeps_the_free_slot" "" index catalog)
echo "base-G: $v" | tee -a "$OUT/summary.txt"
ok_verdict "$v" || bad=$((bad + 1))

echo "== base-L: src/ at 2b9d2c0 (amendment 5: the page-read counter broke the lock-order scanner)"
git checkout 2b9d2c0 -- src/
run_target base-L lockorder
restore base-L
v=$(verdict base-L kill "every_pool_method_that_locks_opens_a_pool_section" "" lockorder)
echo "base-L: $v" | tee -a "$OUT/summary.txt"
ok_verdict "$v" || bad=$((bad + 1))

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
  v=$(verdict "$name" "$kind" "$req" "$opt" $TARGETS)
  echo "$name: $v" | tee -a "$OUT/summary.txt"
  ok_verdict "$v" || bad=$((bad + 1))
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
