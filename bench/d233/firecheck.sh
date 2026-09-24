#!/usr/bin/env bash
# D233 fire-check, amendment 3 (review 2: G1 a control that must be clean and kills by NAME; G3/G4 new
# mutants; G6/G2/G9 red tests). Pre-registration: artie-research frontier/lane_d233_free_at_empty.md §9.
# Mutants cover BPlusTreeManager's free-at-empty (index.rs) and fork's FREE_ID reuse
# (table_catalog.rs). Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan
# work: it runs only when the lead releases the FAN-QUEUE row for D233.
#
# Arms:
# - base-F3: 0eda6ca's src/ with this tip's index.rs test module spliced in; the index selection
#            must FAIL exactly Ra, Rb, Rc and G6T (the F3 red run, split per case, review 2 G4).
# - base-G:  f03e25d's src/ with this tip's index.rs and table_catalog.rs test modules spliced in;
#            index + catalog must FAIL exactly G6T, G2T, G9a, G9b.
# - base-L:  2b9d2c0's src/, --test lock_order_allowlist: exactly
#            every_pool_method_that_locks_opens_a_pool_section FAILS (amendment 5).
# - control: SUBJECT_SHA, all four selections (index, catalog, collateral, lockorder): zero FAILED lines, a result line and rc=0 in every
#            file, or the run is VOID (exit 2).
# - mutants: KILLED-AS-REGISTERED when every required killer FAILED and nothing outside
#            required + optional failed; the pre-registered survivors (U8, V4, V5) are
#            SURVIVED-AS-REGISTERED when nothing outside their optional set failed. Anything else
#            (MISMATCH, SURVIVED, COMPILE-FAIL) counts against the run.
#
# Blind spots, stated: four selections, not the whole suite. CC (the concurrent arm) is optional
# wherever it appears, because it sees a mutant only when the scheduler hits the window.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=f6909db
OUT=bench/d233/firecheck
mkdir -p "$OUT"

if ! git diff --quiet "$SUBJECT_SHA" -- src/ tests/; then
  echo "REFUSED: src/ or tests/ differ from $SUBJECT_SHA; the mutants' text may not apply" >&2
  exit 2
fi
if [ -n "$(git status --porcelain -- src/ tests/)" ]; then
  echo "REFUSED: uncommitted changes under src/ or tests/" >&2
  exit 2
fi

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
)
# name|kill or survivor|required killers|optional killers
KILLERS=(
  "U1_never_unlink|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement a_reader_from_before_an_unlink_walks_off_the_unlinked_leaf a_refused_unlink_with_a_broken_prev_link_writes_nothing a_refused_unlink_with_a_broken_next_link_writes_nothing a_refused_cascade_that_would_empty_the_root_writes_nothing a_chain_whose_prev_is_its_next_is_refused_not_waited_on unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied|"
  "U2_no_prev_splice|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied|"
  "U3_no_next_splice|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement|unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied"
  "U4_parent_keeps_pointer|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement a_reader_from_before_an_unlink_walks_off_the_unlinked_leaf unlinks_racing_refills_and_readers_lose_no_key|a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied"
  "U5_wrong_child_removed|kill|free_at_empty_keeps_the_chain_and_the_parents_in_agreement|a_reader_from_before_an_unlink_walks_off_the_unlinked_leaf unlinks_racing_refills_and_readers_lose_no_key a_lease_pass_does_not_walk_the_leaves_fifo_reaps_emptied"
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
)

run_target() { # $1 = label, $2 = index | catalog | collateral
  case "$2" in
    index)      timeout 1800 cargo test --lib storage::index > "$OUT/$1.index.txt" 2>&1 ;;
    catalog)    timeout 1800 cargo test --lib branch::table_catalog > "$OUT/$1.catalog.txt" 2>&1 ;;
    collateral) timeout 1800 cargo test --test d58_latch_free_descent --test d126_atomic_upsert --test integration_btree_concurrency > "$OUT/$1.collateral.txt" 2>&1 ;;
    lockorder)  timeout 1800 cargo test --test lock_order_allowlist > "$OUT/$1.lockorder.txt" 2>&1 ;;
  esac
  echo "rc=$?" >> "$OUT/$1.$2.txt"
}

failed_names() {
  cat "$OUT/$1".*.txt 2>/dev/null | sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' | sed -E 's/.*:://' | sort -u
}

all_ran() {
  for f in "$OUT/$1".*.txt; do
    grep -qE '^test result:' "$f" || return 1
  done
  return 0
}

all_rc0() {
  for f in "$OUT/$1".*.txt; do
    grep -qx 'rc=0' "$f" || return 1
  done
  return 0
}

words() { printf '%s\n' $1 | sed '/^$/d' | sort -u; }

verdict() { # $1 label, $2 kill|survivor, $3 required, $4 optional
  if ! all_ran "$1"; then echo "COMPILE-FAIL"; return; fi
  actual=$(failed_names "$1")
  extra=$(comm -13 <(words "$3 $4") <(printf '%s\n' "$actual" | sed '/^$/d'))
  if [ "$2" = survivor ]; then
    if [ -z "$extra" ]; then echo "SURVIVED-AS-REGISTERED ($(echo $actual))"; else echo "MISMATCH (unexpected: $(echo $extra))"; fi
    return
  fi
  if [ -z "$actual" ]; then echo "SURVIVED"; return; fi
  missing=$(comm -23 <(words "$3") <(printf '%s\n' "$actual"))
  if [ -z "$missing" ] && [ -z "$extra" ]; then
    echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else
    echo "MISMATCH (missing: $(echo $missing); unexpected: $(echo $extra))"
  fi
}

ok_verdict() { case "$1" in KILLED-AS-REGISTERED*|SURVIVED-AS-REGISTERED*) return 0 ;; *) return 1 ;; esac; }

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

bad=0
restore() {
  git checkout "$SUBJECT_SHA" -- src/
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after $1" >&2; exit 3; }
}

echo "== base-F3: 0eda6ca src/ + this tip's index.rs tests"
git checkout 0eda6ca -- src/
splice_tests "$I" || { echo "ABORT: splice failed (base-F3)" >&2; restore base-F3; exit 3; }
run_target base-F3 index
restore base-F3
v=$(verdict base-F3 kill "a_refused_unlink_with_a_broken_prev_link_writes_nothing a_refused_unlink_with_a_broken_next_link_writes_nothing a_refused_cascade_that_would_empty_the_root_writes_nothing a_chain_whose_prev_is_its_next_is_refused_not_waited_on" "")
echo "base-F3: $v" | tee "$OUT/summary.txt"
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
v=$(verdict base-G kill "a_chain_whose_prev_is_its_next_is_refused_not_waited_on fork_never_reuses_a_reaped_slot_that_still_has_a_live_child a_free_id_whose_record_cannot_be_read_is_skipped_not_fatal a_malformed_free_id_key_is_counted_and_removed" "")
echo "base-G: $v" | tee -a "$OUT/summary.txt"
ok_verdict "$v" || bad=$((bad + 1))

echo "== base-L: src/ at 2b9d2c0 (amendment 5: the page-read counter broke the lock-order scanner)"
git checkout 2b9d2c0 -- src/
run_target base-L lockorder
restore base-L
v=$(verdict base-L kill "every_pool_method_that_locks_opens_a_pool_section" "")
echo "base-L: $v" | tee -a "$OUT/summary.txt"
ok_verdict "$v" || bad=$((bad + 1))

echo "== control: $SUBJECT_SHA"
for t in index catalog collateral lockorder; do run_target control "$t"; done
if ! all_ran control || ! all_rc0 control || [ -n "$(failed_names control)" ]; then
  echo "control: VOID (failed: $(failed_names control | tr '\n' ' '); every file needs a result line and rc=0)" | tee -a "$OUT/summary.txt"
  exit 2
fi
echo "control: clean" | tee -a "$OUT/summary.txt"

for m in "${MUTANTS[@]}"; do
  IFS='|' read -r name FILE old new <<< "$m"
  kind=""; req=""; opt=""
  for k in "${KILLERS[@]}"; do
    IFS='|' read -r kname kkind kreq kopt <<< "$k"
    if [ "$kname" = "$name" ]; then kind=$kkind; req=$kreq; opt=$kopt; fi
  done
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
  # Not *.txt: the judge globs "$OUT/<label>.*.txt" for target outputs (D237 review 2, N2).
  git diff --stat -- "$FILE" > "$OUT/$name.diffstat"
  for t in index catalog collateral lockorder; do run_target "$name" "$t"; done
  git checkout "$SUBJECT_SHA" -- "$FILE"
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: restore of $FILE after $name left a difference" >&2; exit 3; }
  v=$(verdict "$name" "$kind" "$req" "$opt")
  echo "$name: $v" | tee -a "$OUT/summary.txt"
  ok_verdict "$v" || bad=$((bad + 1))
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
