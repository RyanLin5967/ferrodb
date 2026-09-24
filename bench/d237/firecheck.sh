#!/usr/bin/env bash
# D237 fire-check, amendment 2 (the D237 review's F9: named killers, a control that must be clean).
# Pre-registration: artie-research frontier/lane_d237.md §9. Run from the worktree root at DEFAULT QoS
# (never taskpolicy -b). This is fan work: it runs only when the lead releases the FAN-QUEUE row.
#
# Arms:
# - base:    tests at SUBJECT_SHA, src/ at 9aa6968; d237_pin_leak must FAIL exactly its three tests.
# - base2:   tests at SUBJECT_SHA, src/ at f651096; d237_batch_free must FAIL
#            a_batch_free_with_an_unfreeable_page_frees_none_of_it, and may FAIL the concurrent test
#            (probabilistic, pre-registered as almost surely).
# - control: SUBJECT_SHA; every selected test must pass, with the pre-registered counts. A control
#            that fails, or does not compile, VOIDS the run: exit 2.
# - mutants: each must FAIL exactly its pre-registered killers (plus, optionally, its optional ones)
#            and nothing else. Any other outcome is reported as MISMATCH, SURVIVED or COMPILE-FAIL.
#
# Blind spots, stated: three selections (the two d237 targets and the heap's unit tests), not the
# whole suite. The concurrent test's red is probabilistic; a run where it does not fire under M5 is
# still KILLED-AS-REGISTERED through the deterministic killer.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=639ab19
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
  "M2_batch_free_unchecked|$B|"'"            if frame.pin_counter.load(Ordering::Relaxed) > 0 {\n                drop(frame);\n                relabel(&taken[..]);\n"|"            if false && frame.pin_counter.load(Ordering::Relaxed) > 0 {\n                drop(frame);\n                relabel(&taken[..]);\n"'
  "M3_drop_per_structure|$C|"'"        let mut pages = HeapFileManager::open(heap_dir, self.buffer_pool.clone()).page_ids()?;\n        pages.extend(HeapFileManager::open(tt_root, self.buffer_pool.clone()).page_ids()?);\n        pages.extend(BPlusTreeManager::<Value, RecordId>::open(primary_root, self.buffer_pool.clone()).page_ids()?);\n        for root in sec_roots {\n            pages.extend(BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).page_ids()?);\n        }\n        // A page reached twice (two trees sharing one, which a sound catalog never has) is freed\n        // once. Order kept, so the frees still run leaves and data pages first.\n        let mut seen = std::collections::HashSet::with_capacity(pages.len());\n        pages.retain(\u007cp\u007c seen.insert(*p));\n        self.buffer_pool.free_pages(&pages)?;\n"|"        HeapFileManager::open(heap_dir, self.buffer_pool.clone()).free_all()?;\n        HeapFileManager::open(tt_root, self.buffer_pool.clone()).free_all()?;\n        BPlusTreeManager::<Value, RecordId>::open(primary_root, self.buffer_pool.clone()).free_all()?;\n        for root in sec_roots {\n            BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).free_all()?;\n        }\n"'
  "M4_guard_skips_early_return|$B|"'"        let dirty = self.stated.unwrap_or(self.wrote.get());\n        self.pool.unpin_page(self.page_id, dirty);\n"|"        if let Some(dirty) = self.stated {\n            self.pool.unpin_page(self.page_id, dirty);\n        }\n"'
  "M5_batch_free_lets_go|$B|"'"        // Lock-order: `in_transit -> arc_cache -> page_table -> frame`, the module\u0027s order, as in\n        // `invalidate_all`, and all three pool locks are held to the end. See\n        // src/storage/page_latch.rs.\n        let _pool = enter_pool();\n        let transit = self.in_transit.lock().unwrap();\n        if page_ids.iter().any(\u007cp\u007c transit.contains(p)) {\n            return Err(FerroError::PagePinned);\n        }\n        let mut cache = self.arc_locked();\n        let mut pt = self.page_table.write().unwrap();\n\n        // Gives pass 1\u0027s frames their labels back, when the call refuses after taking some.\n        let relabel = \u007ctaken: &[(u32, usize)]\u007c {\n            for &(page_id, frame_i) in taken {\n                self.frame_write(frame_i).page_id = Some(page_id);\n            }\n        };\n\n        // Pass 1: refuse on any pin; unlabel every other resident frame.\n        let mut taken: Vec<(u32, usize)> = Vec::new();\n        for &page_id in page_ids {\n            let Some(&frame_i) = pt.get(&page_id) else { continue };\n            let mut frame = self.frame_write(frame_i);\n            if frame.page_id != Some(page_id) {\n                continue; // listed twice, and an earlier turn already took its frame\n            }\n            if frame.pin_counter.load(Ordering::Relaxed) > 0 {\n                drop(frame);\n                relabel(&taken[..]);\n                return Err(FerroError::PagePinned);\n            }\n            frame.page_id = None;\n            drop(frame);\n            taken.push((page_id, frame_i));\n        }\n\n        // Pass 2: the disk, validated whole before any bit is cleared.\n        if let Err(e) = self.disk_manager.deallocate_many(page_ids) {\n            relabel(&taken[..]);\n            return Err(e);\n        }\n\n        // Pass 3: forget the frames.\n        for &(page_id, frame_i) in &taken {\n            pt.remove(&page_id);\n            let mut frame = self.frame_write(frame_i);\n            frame.data = [0u8; PAGE_SIZE];\n            frame.pin_counter = AtomicU16::new(0);\n            frame.dirty_flag = AtomicBool::new(false);\n        }\n        for &(page_id, _) in &taken {\n            cache.remove(page_id)?;\n        }\n        Ok(())\n    }\n\n"|"        {\n            // Lock-order: `in_transit -> page_table -> frame`, the module\u0027s order, as in\n            // `invalidate_all`. See src/storage/page_latch.rs.\n            let _pool = enter_pool();\n            let transit = self.in_transit.lock().unwrap();\n            let pt = self.page_table.read().unwrap();\n            for page_id in page_ids {\n                if transit.contains(page_id) {\n                    return Err(FerroError::PagePinned);\n                }\n                if let Some(&frame_i) = pt.get(page_id) {\n                    if self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {\n                        return Err(FerroError::PagePinned);\n                    }\n                }\n            }\n        }\n        for &page_id in page_ids {\n            self.free_page(page_id)?;\n        }\n        Ok(())\n    }\n\n"'
  "M6_bitmap_written_as_built|$D|"'"            images.push((current_bitmap_id, image));\n"|"            self.write(current_bitmap_id, &image)?;\n            images.push((current_bitmap_id, image));\n"'
)
# name|required killers (all must FAIL)|optional killers (may FAIL)
KILLERS=(
  "M1_read_hand_unpin|a_read_that_fails_unpins_its_page a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none|"
  "M2_batch_free_unchecked|a_drop_refused_for_a_pinned_page_has_freed_nothing|a_concurrent_pinner_never_sees_a_half_freed_set"
  "M3_drop_per_structure|a_drop_refused_for_a_pinned_page_has_freed_nothing|"
  "M4_guard_skips_early_return|a_read_that_fails_unpins_its_page a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none a_drop_refused_for_a_pinned_page_has_freed_nothing|"
  "M5_batch_free_lets_go|a_batch_free_with_an_unfreeable_page_frees_none_of_it|a_concurrent_pinner_never_sees_a_half_freed_set"
  "M6_bitmap_written_as_built|a_batch_free_with_an_unfreeable_page_frees_none_of_it|"
)

run_target() { # $1 = label, $2 = d237 | batch | heap
  case "$2" in
    d237)  timeout 1800 cargo test --test d237_pin_leak > "$OUT/$1.d237.txt" 2>&1 ;;
    batch) timeout 1800 cargo test --test d237_batch_free > "$OUT/$1.batch.txt" 2>&1 ;;
    heap)  timeout 1800 cargo test --lib storage::heap_file_manager > "$OUT/$1.heap.txt" 2>&1 ;;
  esac
  echo "rc=$?" >> "$OUT/$1.$2.txt"
}

# Short names of every test that FAILED in a label's outputs, sorted, one per line.
failed_names() {
  cat "$OUT/$1".*.txt 2>/dev/null | sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' | sed -E 's/.*:://' | sort -u
}

# 0 if every output of the label has a result line (the target compiled and ran).
all_ran() {
  for f in "$OUT/$1".*.txt; do
    grep -qE '^test result:' "$f" || return 1
  done
  return 0
}

# "N passed" for one output file, or "none".
passed_in() {
  grep -hE '^test result:' "$OUT/$1.$2.txt" | tail -1 | sed -nE 's/.* ([0-9]+) passed.*/\1/p'
}

# sorted words of a space-separated list, one per line
words() { printf '%s\n' $1 | sed '/^$/d' | sort -u; }

# Verdict on a label against required and optional killer lists.
verdict() { # $1 label, $2 required, $3 optional
  if ! all_ran "$1"; then echo "COMPILE-FAIL"; return; fi
  actual=$(failed_names "$1")
  if [ -z "$actual" ]; then echo "SURVIVED"; return; fi
  missing=$(comm -23 <(words "$2") <(printf '%s\n' "$actual"))
  extra=$(comm -13 <(words "$2 $3") <(printf '%s\n' "$actual"))
  if [ -z "$missing" ] && [ -z "$extra" ]; then
    echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else
    echo "MISMATCH (missing: $(echo $missing); unexpected: $(echo $extra))"
  fi
}

bad=0

echo "== base: src/ at 9aa6968"
git checkout 9aa6968 -- src/
run_target base d237
git checkout "$SUBJECT_SHA" -- src/
git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after base" >&2; exit 3; }
v=$(verdict base "a_read_that_fails_unpins_its_page a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none a_drop_refused_for_a_pinned_page_has_freed_nothing" "")
echo "base: $v" | tee "$OUT/summary.txt"
case "$v" in KILLED-AS-REGISTERED*) ;; *) bad=$((bad + 1)) ;; esac

echo "== base2: src/ at f651096"
git checkout f651096 -- src/
run_target base2 batch
git checkout "$SUBJECT_SHA" -- src/
git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after base2" >&2; exit 3; }
v=$(verdict base2 "a_batch_free_with_an_unfreeable_page_frees_none_of_it" "a_concurrent_pinner_never_sees_a_half_freed_set")
echo "base2: $v" | tee -a "$OUT/summary.txt"
case "$v" in KILLED-AS-REGISTERED*) ;; *) bad=$((bad + 1)) ;; esac

echo "== control: $SUBJECT_SHA"
for t in d237 batch heap; do run_target control "$t"; done
if ! all_ran control || [ -n "$(failed_names control)" ] \
   || [ "$(passed_in control d237)" != 3 ] || [ "$(passed_in control batch)" != 2 ] \
   || [ "$(passed_in control heap)" != 5 ]; then
  echo "control: VOID (failed: $(failed_names control | tr '\n' ' '); passed d237=$(passed_in control d237) batch=$(passed_in control batch) heap=$(passed_in control heap); expected 3/2/5)" | tee -a "$OUT/summary.txt"
  exit 2
fi
echo "control: clean (3/2/5 passed)" | tee -a "$OUT/summary.txt"

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
    echo "$name: NOT APPLIED" | tee -a "$OUT/summary.txt"
    bad=$((bad + 1))
    continue
  fi
  git diff --stat -- "$FILE" > "$OUT/$name.diffstat.txt"
  for t in d237 batch heap; do run_target "$name" "$t"; done
  git checkout "$SUBJECT_SHA" -- "$FILE"
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: restore of $FILE after $name left a difference" >&2; exit 3; }
  v=$(verdict "$name" "$req" "$opt")
  echo "$name: $v" | tee -a "$OUT/summary.txt"
  case "$v" in KILLED-AS-REGISTERED*) ;; *) bad=$((bad + 1)) ;; esac
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
