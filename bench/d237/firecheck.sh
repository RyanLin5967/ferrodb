#!/usr/bin/env bash
# D237 fire-check. Arm "base" is the red run: the tests at the fix commit against 9aa6968's src/.
# Arm "control" is the fix. Each mutant reverts ONE piece of the fix and must turn at least one named
# test in tests/d237_pin_leak.rs red. Pre-registration: artie-research frontier/lane_d237.md. Run from
# the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work: it runs only when the lead
# releases the FAN-QUEUE row for D237.
#
# Blind spots, stated: two selections per arm (the d237 target and the heap's own unit tests), not the
# whole suite, so a mutant killed only elsewhere reads as a survivor here. A compile error is recorded
# as COMPILE-FAIL, which is not a kill.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=f651096           # the fix; the mutants' text is taken from it
BASE_SHA=9aa6968              # the red arm's src/
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

# name|file|old text (python literal)|new text (python literal)
H=src/storage/heap_file_manager.rs
B=src/buffer/buffer_pool.rs
C=src/catalog/catalog.rs
MUTANTS=(
  "M1_read_hand_unpin|$H|"'"        let pin = self.buffer_pool_manager.pin(record_id.page_id)?;\n        let frame = self.buffer_pool_manager.frames[pin.frame()].read().unwrap();\n        let page = Page::deserialize(frame.data)?;\n        let tuple = page.read(record_id.slot_num as usize)?;\n        drop(frame);\n        pin.unpin(false);\n"|"        let frame_i = self.buffer_pool_manager.fetch_page(record_id.page_id)?;\n        let frame = self.buffer_pool_manager.frames[frame_i].read().unwrap();\n        let page = Page::deserialize(frame.data)?;\n        let tuple = page.read(record_id.slot_num as usize)?;\n        drop(frame);\n        self.buffer_pool_manager.unpin_page(record_id.page_id, false);\n"'
  "M2_batch_free_unchecked|$B|"'"                if let Some(&frame_i) = pt.get(page_id) {\n                    if self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {\n                        return Err(FerroError::PagePinned);\n                    }\n                }\n"|""'
  "M3_drop_per_structure|$C|"'"        let mut pages = HeapFileManager::open(heap_dir, self.buffer_pool.clone()).page_ids()?;\n        pages.extend(HeapFileManager::open(tt_root, self.buffer_pool.clone()).page_ids()?);\n        pages.extend(BPlusTreeManager::<Value, RecordId>::open(primary_root, self.buffer_pool.clone()).page_ids()?);\n        for root in sec_roots {\n            pages.extend(BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).page_ids()?);\n        }\n        self.buffer_pool.free_pages(&pages)?;\n"|"        HeapFileManager::open(heap_dir, self.buffer_pool.clone()).free_all()?;\n        HeapFileManager::open(tt_root, self.buffer_pool.clone()).free_all()?;\n        BPlusTreeManager::<Value, RecordId>::open(primary_root, self.buffer_pool.clone()).free_all()?;\n        for root in sec_roots {\n            BPlusTreeManager::<(Value, Value), ()>::open(root, self.buffer_pool.clone()).free_all()?;\n        }\n"'
  "M4_guard_skips_early_return|$B|"'"        let dirty = self.stated.unwrap_or(self.wrote.get());\n        self.pool.unpin_page(self.page_id, dirty);\n"|"        if let Some(dirty) = self.stated {\n            self.pool.unpin_page(self.page_id, dirty);\n        }\n"'
)

run_targets() { # $1 = label
  timeout 1800 cargo test --test d237_pin_leak > "$OUT/$1.d237.txt" 2>&1
  echo "d237_rc=$?" >> "$OUT/$1.d237.txt"
  timeout 1800 cargo test --lib storage::heap_file_manager > "$OUT/$1.heap.txt" 2>&1
  echo "heap_rc=$?" >> "$OUT/$1.heap.txt"
}

summarise() { # $1 = label
  for t in d237 heap; do
    f="$OUT/$1.$t.txt"
    res=$(grep -E "^test result:" "$f" | tail -1)
    [ -z "$res" ] && res="COMPILE-FAIL or no result line"
    failed=$(grep -E "^test .* FAILED$" "$f" | sed -E 's/^test ([^ ]+) .*/\1/' | tr '\n' ' ')
    echo "$1 $t: $res | failed: ${failed:-none}"
  done
}

echo "== base (src/ at $BASE_SHA, tests at $SUBJECT_SHA): the red run"
git checkout "$BASE_SHA" -- src/
run_targets base
git checkout "$SUBJECT_SHA" -- src/
if ! git diff --quiet "$SUBJECT_SHA" -- src/; then
  echo "ABORT: restore of src/ after the base arm left a difference" >&2
  exit 3
fi
summarise base | tee "$OUT/summary.txt"

echo "== control (unmutated $SUBJECT_SHA)"
run_targets control
summarise control | tee -a "$OUT/summary.txt"

survivors=0
for m in "${MUTANTS[@]}"; do
  IFS='|' read -r name FILE old new <<< "$m"
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
    survivors=$((survivors + 1))
    continue
  fi
  git diff --stat -- "$FILE" > "$OUT/$name.diffstat.txt"
  run_targets "$name"
  git checkout "$SUBJECT_SHA" -- "$FILE"
  if ! git diff --quiet "$SUBJECT_SHA" -- src/; then
    echo "ABORT: restore of $FILE after $name left a difference" >&2
    exit 3
  fi
  summarise "$name" | tee -a "$OUT/summary.txt"
  if ! grep -qE "^test .* FAILED$" "$OUT/$name.d237.txt" "$OUT/$name.heap.txt"; then
    echo "$name: SURVIVED (no test failed)" | tee -a "$OUT/summary.txt"
    survivors=$((survivors + 1))
  fi
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "survivors=$survivors" | tee -a "$OUT/summary.txt"
[ "$survivors" -eq 0 ]
