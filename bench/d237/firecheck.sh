#!/usr/bin/env bash
# D237 fire-check, amendment 3 (review 2: N2 the diffstat no longer matches the target glob; the N1
# arm; M7-M10; the doctests' red arm). Pre-registration: artie-research frontier/lane_d237.md §10.
# Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work: it runs only when
# the lead releases the FAN-QUEUE row.
#
# Targets: d237 (tests/d237_pin_leak.rs), batch (tests/d237_batch_free.rs), heap (the heap's unit tests),
# pool (buffer_pool's unit tests, which hold the N1 tests), doc (`cargo test --doc PagePin`, the two
# compile_fail doctests).
# Arms:
# - base:  src/ at 9aa6968, d237: exactly its three tests FAIL.
# - base2: src/ at f651096, batch: the unfreeable-page test FAILS; the concurrent one may.
# - base3: src/ at 50688fe (the N1 red commit), pool: exactly the two N1 tests FAIL.
# - control: SUBJECT_SHA, all five targets: zero FAILED, a result line and rc=0 each, and each passed
#   count equal to that target's own `-- --list` count. Anything else VOIDS the run (exit 2).
# - mutants: KILLED-AS-REGISTERED when every required killer FAILED and nothing outside required +
#   optional did. M8 is judged on the doc target by count (2 FAILED), since doctest names carry line
#   numbers. Anything else (MISMATCH, SURVIVED, COMPILE-FAIL) counts against the run.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=8987a52
OUT=bench/d237/firecheck
mkdir -p "$OUT"

if ! git diff --quiet "$SUBJECT_SHA" -- src/ tests/; then
  echo "REFUSED: src/ or tests/ differ from $SUBJECT_SHA; the mutants' text may not apply" >&2
  exit 2
fi
if [ -n "$(git status --porcelain -- src/ tests/)" ]; then
  echo "REFUSED: uncommitted changes under src/ or tests/" >&2
  exit 2
fi

H=src/storage/heap_file_manager.rs
B=src/buffer/buffer_pool.rs
C=src/catalog/catalog.rs
D=src/storage/disk_manager.rs
# name|file|old text (python literal)|new text (python literal); '|' and a single quote inside the
# text are written \u007c and \u0027.
MUTANTS=(
  "M1_read_hand_unpin|$H|"'"        let pin = self.buffer_pool_manager.pin(record_id.page_id)?;\n        let frame = pin.read();\n        let page = Page::deserialize(frame.data)?;\n        let tuple = page.read(record_id.slot_num as usize)?;\n        drop(frame);\n        pin.unpin(false);\n"|"        let frame_i = self.buffer_pool_manager.fetch_page(record_id.page_id)?;\n        let frame = self.buffer_pool_manager.frames[frame_i].read().unwrap();\n        let page = Page::deserialize(frame.data)?;\n        let tuple = page.read(record_id.slot_num as usize)?;\n        drop(frame);\n        self.buffer_pool_manager.unpin_page(record_id.page_id, false);\n"'
  "M2_batch_free_unchecked|$B|"'"            if frame.pin_counter.load(Ordering::Relaxed) > 0 {\n                drop(frame);\n                unmark(&taken[..]);\n"|"            if false && frame.pin_counter.load(Ordering::Relaxed) > 0 {\n                drop(frame);\n                unmark(&taken[..]);\n"'
  "M3_drop_per_structure|$C|"'"        let mut pages = HeapFileManager::open(heap_dir, self.buffer_pool.clone()).page_ids()?;\n        pages.extend(HeapFileManager::open(tt_root, self.buffer_pool.clone()).page_ids()?);\n        pages.extend(BPlusTreeManager::<Value, RecordId>::open(primary_root, self.buffer_pool.clone()).page_ids()?);\n        for root in sec_roots {\n            pages.extend(BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).page_ids()?);\n        }\n        // A page reached twice (two trees sharing one, which a sound catalog never has) is freed\n        // once. Order kept, so the frees still run leaves and data pages first.\n        let mut seen = std::collections::HashSet::with_capacity(pages.len());\n        pages.retain(\u007cp\u007c seen.insert(*p));\n        self.buffer_pool.free_pages(&pages)?;\n"|"        HeapFileManager::open(heap_dir, self.buffer_pool.clone()).free_all()?;\n        HeapFileManager::open(tt_root, self.buffer_pool.clone()).free_all()?;\n        BPlusTreeManager::<Value, RecordId>::open(primary_root, self.buffer_pool.clone()).free_all()?;\n        for root in sec_roots {\n            BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).free_all()?;\n        }\n"'
  "M4_guard_skips_early_return|$B|"'"        let dirty = self.stated.unwrap_or(self.wrote.get());\n        self.pool.unpin_page(self.page_id, dirty);\n"|"        if let Some(dirty) = self.stated {\n            self.pool.unpin_page(self.page_id, dirty);\n        }\n"'
  "M5_batch_free_lets_go|$B|"'"        // Lock-order: `in_transit -> arc_cache -> page_table -> frame`, the module\u0027s order, as in\n        // `invalidate_all`, and all three pool locks are held to the end. See\n        // src/storage/page_latch.rs.\n        let _pool = enter_pool();\n        let transit = self.in_transit.lock().unwrap();\n        if page_ids.iter().any(\u007cp\u007c transit.contains(p)) {\n            return Err(FerroError::PagePinned);\n        }\n        let mut cache = self.arc_locked();\n        let mut pt = self.page_table.write().unwrap();\n\n        // Takes pass 1\u0027s marks off again, when the call refuses after marking some. It clears the\n        // mark and touches nothing else: a label is never written back, so it cannot land on a\n        // frame anything else has claimed since (D237 review 2, N1).\n        let unmark = \u007ctaken: &[(u32, usize)]\u007c {\n            for &(page_id, frame_i) in taken {\n                let mut frame = self.frame_write(frame_i);\n                if frame.freeing && frame.page_id == Some(page_id) {\n                    frame.freeing = false;\n                }\n            }\n        };\n\n        // Pass 1: refuse on any pin; mark every other resident frame as being freed.\n        let mut taken: Vec<(u32, usize)> = Vec::new();\n        for &page_id in page_ids {\n            let Some(&frame_i) = pt.get(&page_id) else { continue };\n            let mut frame = self.frame_write(frame_i);\n            if frame.page_id != Some(page_id) \u007c\u007c frame.freeing {\n                continue; // listed twice, and an earlier turn already marked its frame\n            }\n            if frame.pin_counter.load(Ordering::Relaxed) > 0 {\n                drop(frame);\n                unmark(&taken[..]);\n                return Err(FerroError::PagePinned);\n            }\n            frame.freeing = true;\n            drop(frame);\n            taken.push((page_id, frame_i));\n        }\n\n        // Test seam (D237 review 2, N1): a unit test can park the call here, between pass 1 and\n        // pass 2. An empty stub outside tests; see `buffer::fault_hooks`.\n        crate::buffer::fault_hooks::between_pass_1_and_2(self);\n\n        // Pass 2: the disk, validated whole before any bit is cleared.\n        if let Err(e) = self.disk_manager.deallocate_many(page_ids) {\n            unmark(&taken[..]);\n            return Err(e);\n        }\n\n        // Pass 3: forget the frames. The label goes to `None` and the mark comes off in the SAME\n        // frame-lock hold as the reset, as `free_page` does, so no one ever sees a free-looking frame\n        // that still holds a freed page\u0027s state.\n        for &(page_id, frame_i) in &taken {\n            pt.remove(&page_id);\n            let mut frame = self.frame_write(frame_i);\n            frame.page_id = None;\n            frame.data = [0u8; PAGE_SIZE];\n            frame.pin_counter = AtomicU16::new(0);\n            frame.dirty_flag = AtomicBool::new(false);\n            frame.freeing = false;\n        }\n        // Past the commit point: pass 2 has freed the pages on disk, so nothing here may fail the\n        // call. `ArcCache::remove` of an absent page is a no-op, as in `invalidate_all`.\n        for &(page_id, _) in &taken {\n            let _ = cache.remove(page_id);\n        }\n        Ok(())\n    }\n\n"|"        {\n            // Lock-order: `in_transit -> page_table -> frame`, the module\u0027s order, as in\n            // `invalidate_all`. See src/storage/page_latch.rs.\n            let _pool = enter_pool();\n            let transit = self.in_transit.lock().unwrap();\n            let pt = self.page_table.read().unwrap();\n            for page_id in page_ids {\n                if transit.contains(page_id) {\n                    return Err(FerroError::PagePinned);\n                }\n                if let Some(&frame_i) = pt.get(page_id) {\n                    if self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {\n                        return Err(FerroError::PagePinned);\n                    }\n                }\n            }\n        }\n        for &page_id in page_ids {\n            self.free_page(page_id)?;\n        }\n        Ok(())\n    }\n\n"'
  "M6_bitmap_written_as_built|$D|"'"            images.push((current_bitmap_id, image));\n"|"            self.write(current_bitmap_id, &image)?;\n            images.push((current_bitmap_id, image));\n"'
  "M7_no_unmark_on_pass_2_error|$B|"'"        if let Err(e) = self.disk_manager.deallocate_many(page_ids) {\n            unmark(&taken[..]);\n"|"        if let Err(e) = self.disk_manager.deallocate_many(page_ids) {\n"'
  "M8_guards_outlive_the_pin|$B|"'"impl PagePin<\u0027_> {\n    /// Read-lock the frame, through the tracked accessor. The guard borrows the pin.\n    pub fn read(&self) -> FrameGuard<RwLockReadGuard<\u0027_, Frame>> {\n        self.pool.frame_read(self.frame_i)\n    }\n\n    /// Write-lock the frame. The only way to write through a pin, because it is how the pin knows\n    /// the page may be dirty. The guard borrows the pin.\n    pub fn write(&self) -> FrameWriteGuard<\u0027_> {\n        self.wrote.set(true);\n        self.pool.frame_write(self.frame_i)\n    }\n\n"|"impl<\u0027a> PagePin<\u0027a> {\n    /// Read-lock the frame, through the tracked accessor. The guard borrows the pin.\n    pub fn read(&self) -> FrameGuard<RwLockReadGuard<\u0027a, Frame>> {\n        let pool: &\u0027a BufferPoolManager = self.pool;\n        pool.frame_read(self.frame_i)\n    }\n\n    /// Write-lock the frame. The only way to write through a pin, because it is how the pin knows\n    /// the page may be dirty. The guard borrows the pin.\n    pub fn write(&self) -> FrameWriteGuard<\u0027a> {\n        self.wrote.set(true);\n        let pool: &\u0027a BufferPoolManager = self.pool;\n        pool.frame_write(self.frame_i)\n    }\n\n"'
  "M9_pass_1_unlabels|$B|"'"            frame.freeing = true;\n            drop(frame);\n            taken.push((page_id, frame_i));\n"|"            frame.page_id = None;\n            drop(frame);\n            taken.push((page_id, frame_i));\n"'
  "M10_pin_ignores_freeing|$B|"'"        // would let this pin it after the call\u0027s pin check (D237 review 2, N1).\n        if frame.page_id != Some(page_id) \u007c\u007c frame.freeing {\n"|"        // would let this pin it after the call\u0027s pin check (D237 review 2, N1).\n        if frame.page_id != Some(page_id) {\n"'
)
# name|required killers (all must FAIL), or DOC2 for "the doc target has 2 FAILED"|optional killers
KILLERS=(
  "M1_read_hand_unpin|a_read_that_fails_unpins_its_page a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none|"
  "M2_batch_free_unchecked|a_drop_refused_for_a_pinned_page_has_freed_nothing|a_concurrent_pinner_never_sees_a_half_freed_set"
  "M3_drop_per_structure|a_drop_refused_for_a_pinned_page_has_freed_nothing|"
  "M4_guard_skips_early_return|a_read_that_fails_unpins_its_page a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none a_drop_refused_for_a_pinned_page_has_freed_nothing|"
  "M5_batch_free_lets_go|a_batch_free_with_an_unfreeable_page_frees_none_of_it|a_concurrent_pinner_never_sees_a_half_freed_set a_fault_that_claims_a_frame_while_a_set_is_freed_keeps_it a_refused_free_does_not_relabel_a_frame_a_fault_claimed"
  "M6_bitmap_written_as_built|a_batch_free_with_an_unfreeable_page_frees_none_of_it|"
  "M7_no_unmark_on_pass_2_error|a_batch_free_with_an_unfreeable_page_frees_none_of_it|a_refused_free_does_not_relabel_a_frame_a_fault_claimed"
  "M8_guards_outlive_the_pin|DOC2|"
  "M9_pass_1_unlabels|a_fault_that_claims_a_frame_while_a_set_is_freed_keeps_it a_refused_free_does_not_relabel_a_frame_a_fault_claimed|a_batch_free_with_an_unfreeable_page_frees_none_of_it a_drop_refused_for_a_pinned_page_has_freed_nothing a_concurrent_pinner_never_sees_a_half_freed_set"
  "M10_pin_ignores_freeing|a_fault_that_claims_a_frame_while_a_set_is_freed_keeps_it|a_concurrent_pinner_never_sees_a_half_freed_set"
)
TARGETS="d237 batch heap pool doc"

target_args() { # $1 = target
  case "$1" in
    d237)  echo "--test d237_pin_leak" ;;
    batch) echo "--test d237_batch_free" ;;
    heap)  echo "--lib storage::heap_file_manager" ;;
    pool)  echo "--lib buffer::buffer_pool::tests::" ;;
    doc)   echo "--doc PagePin" ;;
  esac
}

run_target() { # $1 = label, $2 = target
  # shellcheck disable=SC2046
  timeout 1800 cargo test $(target_args "$2") > "$OUT/$1.$2.out" 2>&1
  echo "rc=$?" >> "$OUT/$1.$2.out"
}

# Only the target outputs (*.out); the diffstat is *.diffstat and never matches (review 2 N2).
failed_names() { # $1 = label; doctests excluded (their names carry spaces and line numbers)
  for f in "$OUT/$1".*.out; do
    case "$f" in *.doc.out) continue ;; esac
    sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' "$f"
  done | sed -E 's/.*:://' | sort -u
}
doc_failed() { grep -cE '^test .* \.\.\. FAILED$' "$OUT/$1.doc.out" 2>/dev/null || true; }
all_ran() { for f in "$OUT/$1".*.out; do grep -qE '^test result:' "$f" || return 1; done; return 0; }
all_rc0() { for f in "$OUT/$1".*.out; do grep -qx 'rc=0' "$f" || return 1; done; return 0; }
passed_in() { grep -hE '^test result:' "$OUT/$1.$2.out" | tail -1 | sed -nE 's/.* ([0-9]+) passed.*/\1/p'; }
listed_in() { # $1 = target: what the harness says it will run, at SUBJECT_SHA
  # shellcheck disable=SC2046
  timeout 1800 cargo test $(target_args "$1") -- --list 2>/dev/null | grep -cE ': test$'
}
words() { printf '%s\n' $1 | sed '/^$/d' | sort -u; }

verdict() { # $1 label, $2 required (or DOC2), $3 optional
  if ! all_ran "$1"; then echo "COMPILE-FAIL"; return; fi
  actual=$(failed_names "$1")
  if [ "$2" = DOC2 ]; then
    d=$(doc_failed "$1")
    if [ "$d" = 2 ] && [ -z "$actual" ]; then echo "KILLED-AS-REGISTERED (doc: 2 FAILED)"
    else echo "MISMATCH (doc FAILED: $d; unexpected: $(echo $actual))"; fi
    return
  fi
  extra=$(comm -13 <(words "$2 $3") <(printf '%s\n' "$actual" | sed '/^$/d'))
  if [ -z "$actual" ]; then echo "SURVIVED"; return; fi
  missing=$(comm -23 <(words "$2") <(printf '%s\n' "$actual"))
  if [ -z "$missing" ] && [ -z "$extra" ]; then echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else echo "MISMATCH (missing: $(echo $missing); unexpected: $(echo $extra))"; fi
}
ok_verdict() { case "$1" in KILLED-AS-REGISTERED*) return 0 ;; *) return 1 ;; esac; }

bad=0
restore() {
  git checkout "$SUBJECT_SHA" -- src/
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after $1" >&2; exit 3; }
}

echo "== base: src/ at 9aa6968"
git checkout 9aa6968 -- src/; run_target base d237; restore base
v=$(verdict base "a_read_that_fails_unpins_its_page a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none a_drop_refused_for_a_pinned_page_has_freed_nothing" ""); echo "base: $v" | tee "$OUT/summary.txt"; ok_verdict "$v" || bad=$((bad + 1))

echo "== base2: src/ at f651096"
git checkout f651096 -- src/; run_target base2 batch; restore base2
v=$(verdict base2 "a_batch_free_with_an_unfreeable_page_frees_none_of_it" "a_concurrent_pinner_never_sees_a_half_freed_set"); echo "base2: $v" | tee -a "$OUT/summary.txt"; ok_verdict "$v" || bad=$((bad + 1))

echo "== base3: src/ at 50688fe (the N1 red commit)"
git checkout 50688fe -- src/; run_target base3 pool; restore base3
v=$(verdict base3 "a_fault_that_claims_a_frame_while_a_set_is_freed_keeps_it a_refused_free_does_not_relabel_a_frame_a_fault_claimed" ""); echo "base3: $v" | tee -a "$OUT/summary.txt"; ok_verdict "$v" || bad=$((bad + 1))

echo "== control: $SUBJECT_SHA"
for t in $TARGETS; do run_target control "$t"; done
counts_ok=1
for t in $TARGETS; do
  l=$(listed_in "$t"); p=$(passed_in control "$t")
  echo "control $t: listed $l, passed ${p:-none}" | tee -a "$OUT/summary.txt"
  [ -n "$l" ] && [ "$l" != 0 ] && [ "$p" = "$l" ] || counts_ok=0
done
if ! all_ran control || ! all_rc0 control || [ -n "$(failed_names control)" ] || [ "$(doc_failed control)" != 0 ] || [ "$counts_ok" != 1 ]; then
  echo "control: VOID" | tee -a "$OUT/summary.txt"
  exit 2
fi
echo "control: clean" | tee -a "$OUT/summary.txt"

for m in "${MUTANTS[@]}"; do
  IFS='|' read -r name FILE old new <<< "$m"
  req=""; opt=""
  for k in "${KILLERS[@]}"; do
    IFS='|' read -r kname kreq kopt <<< "$k"
    if [ "$kname" = "$name" ]; then req=$kreq; opt=$kopt; fi
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
    echo "$name: NOT APPLIED" | tee -a "$OUT/summary.txt"; bad=$((bad + 1)); continue
  fi
  git diff --stat -- "$FILE" > "$OUT/$name.diffstat"
  for t in $TARGETS; do run_target "$name" "$t"; done
  git checkout "$SUBJECT_SHA" -- "$FILE"
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: restore of $FILE after $name left a difference" >&2; exit 3; }
  v=$(verdict "$name" "$req" "$opt")
  echo "$name: $v" | tee -a "$OUT/summary.txt"
  ok_verdict "$v" || bad=$((bad + 1))
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
