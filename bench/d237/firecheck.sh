#!/usr/bin/env bash
# D237 fire-check, amendment 4: the judge reads only the target files it names; each file gets a state
# (TIMEOUT, COMPILE-FAIL, ...) before any name is read; $OUT is cleared; the control runs first; src/ is
# restored on any exit; --self-test runs the judge on planted outputs. Pre-registration: artie-research
# frontier/lane_d237.md §11 (the arms and killers of §10 are unchanged). Run from the worktree root at
# DEFAULT QoS (never taskpolicy -b). This is fan work: it runs only when the lead releases the FAN-QUEUE row.
#
# Usage: bash bench/d237/firecheck.sh               the run
#        bash bench/d237/firecheck.sh --self-test   the judge on planted outputs only: no cargo, no git writes
#
# Targets: d237 (tests/d237_pin_leak.rs), batch (tests/d237_batch_free.rs), heap (the heap's unit tests),
# pool (buffer_pool's unit tests, which hold the N1 tests), doc (`cargo test --doc PagePin`, the two
# compile_fail doctests). Each runs one test binary.
# Arms, in run order:
# - control: SUBJECT_SHA, all five targets: every file OK, nothing FAILED, and per target
#   passed + ignored == the harness's own `-- --list` count, both > 0. Anything else VOIDS the run (exit 2).
# - base:  src/ at 9aa6968, d237: exactly its three tests FAIL.
# - base2: src/ at f651096, batch: the unfreeable-page test FAILS; the concurrent one may.
# - base3: src/ at 50688fe (the N1 red commit), pool: exactly the two N1 tests FAIL.
# - mutants: KILLED-AS-REGISTERED when every file is OK, every required killer FAILED, and nothing outside
#   required + optional did (a FAILED doctest counts as outside, except for M8). M8 is judged on the doc
#   target by count (2 FAILED), since doctest names carry line numbers. Anything else counts against the run.
#
# Blind spots, stated: five selections, not the whole suite. The judge reads cargo's own lines
# ("test result:", "test <name> ... FAILED") and the rc this script appends; a test that prints a line of
# that exact shape to stdout under --nocapture would be misread (none is run with --nocapture).
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=8987a52
OUT=bench/d237/firecheck

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
  local rc
  # shellcheck disable=SC2046
  timeout 1800 cargo test $(target_args "$2") > "$OUT/$1.$2.out" 2>&1
  rc=$?
  echo "rc=$rc" >> "$OUT/$1.$2.out"
}

listed_in() { # $1 = target: what the harness says it will run, at SUBJECT_SHA (the list is kept)
  # shellcheck disable=SC2046
  timeout 1800 cargo test $(target_args "$1") -- --list > "$OUT/control.$1.list" 2>&1
  grep -cE ': test$' "$OUT/control.$1.list"
}

# ---- The judge. It reads ONLY "$OUT/<label>.<target>.out" for the targets it is given. Any other file in
# $OUT (a diffstat, a list, the self-test log, a stale output) cannot change a verdict (D233 review 3 H1). ----
tfile() { printf '%s/%s.%s.out' "$OUT" "$1" "$2"; }
binaries() { echo 1; } # test binaries per target: one each here

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

has_target() { # $1 target, then a list
  local want=$1 t
  shift
  for t in "$@"; do [ "$t" = "$want" ] && return 0; done
  return 1
}

failed_names() { # $1 label, then targets: short names of FAILED tests; the doc target is counted apart
  local label=$1 t
  shift
  for t in "$@"; do
    [ "$t" = doc ] && continue
    sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' "$(tfile "$label" "$t")"
  done | sed -E 's/.*:://' | sort -u
}

doc_failed() { grep -cE '^test .+ \.\.\. FAILED$' "$(tfile "$1" doc)"; }

count_in() { # $1 label, $2 target, $3 passed|ignored: summed over the file's result lines
  grep -E '^test result:' "$(tfile "$1" "$2")" | sed -nE "s/.* ([0-9]+) $3.*/\\1/p" | awk '{ s += $1 } END { print s + 0 }'
}

words() { printf '%s\n' $1 | sed '/^$/d' | sort -u; }

verdict() { # $1 label, $2 required (or DOC2), $3 optional, then the targets the arm ran
  local label=$1 req=$2 opt=$3 st actual extra missing d
  shift 3
  st=$(arm_state "$label" "$@")
  [ "$st" = OK ] || { echo "$st"; return; }
  actual=$(failed_names "$label" "$@")
  d=0
  if has_target doc "$@"; then d=$(doc_failed "$label"); fi
  if [ "$req" = DOC2 ]; then
    if [ "$d" = 2 ] && [ -z "$actual" ]; then echo "KILLED-AS-REGISTERED (doc: 2 FAILED)"
    else echo "MISMATCH (doc FAILED: $d; unexpected: $(echo $actual))"; fi
    return
  fi
  extra=$(comm -13 <(words "$req $opt") <(printf '%s\n' "$actual" | sed '/^$/d'))
  [ "$d" = 0 ] || extra="$extra doctests-FAILED:$d"
  if [ -z "$actual" ] && [ "$d" = 0 ]; then echo "SURVIVED"; return; fi
  missing=$(comm -23 <(words "$req") <(printf '%s\n' "$actual"))
  if [ -z "$missing" ] && [ -z "$(echo $extra)" ]; then echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else echo "MISMATCH (missing: $(echo $missing); unexpected: $(echo $extra))"; fi
}

control_verdict() { # $1 label, $2 "target=listed ..." (the harness's own -- --list counts)
  local label=$1 lists=$2 st t l p i
  # shellcheck disable=SC2086
  st=$(arm_state "$label" $TARGETS)
  [ "$st" = OK ] || { echo "VOID ($st)"; return; }
  # shellcheck disable=SC2086
  if [ -n "$(failed_names "$label" $TARGETS)" ] || [ "$(doc_failed "$label")" != 0 ]; then
    echo "VOID (a test FAILED)"; return
  fi
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

self_test() {
  local keep=$OUT st_bad=0 tmp lists m name rest t k1 k2 base_req
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/d237-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp
  lists="d237=3 batch=2 heap=5 pool=12 doc=2"

  ok_file control d237 3; ok_file control batch 2; ok_file control heap 5; ok_file control pool 12; ok_file control doc 2
  expect "clean control" "clean" "$(control_verdict control "$lists")"

  ok_file dirty batch 2; ok_file dirty heap 5; ok_file dirty pool 12; ok_file dirty doc 2
  fail_file dirty d237 2 a_read_that_fails_unpins_its_page
  # The list count is planted equal to the passed count, so the count gate alone would pass this control:
  # the case proves the FAILED check fires on its own.
  expect "dirty control" "VOID (a test FAILED)" "$(control_verdict dirty "d237=2 batch=2 heap=5 pool=12 doc=2")"

  ok_file empty d237 3; ok_file empty batch 2; ok_file empty heap 5; ok_file empty doc 2
  plant empty pool 0 "running 0 tests" "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 90 filtered out; finished in 0.00s"
  expect "control that collected nothing" "VOID" "$(control_verdict empty "d237=3 batch=2 heap=5 pool=0 doc=2")"

  # M1 with its registered sets, read from the table the run uses.
  killers_of M1_read_hand_unpin || { echo "self-test FAIL  M1 has no KILLERS row"; st_bad=$((st_bad + 1)); }
  k1=${req%% *}; k2=${req#* }
  fail_file M1 d237 2 "$k1"; fail_file M1 batch 1 "$k2"
  ok_file M1 heap 5; ok_file M1 pool 12; ok_file M1 doc 2
  # shellcheck disable=SC2086
  expect "killed mutant" "KILLED-AS-REGISTERED" "$(verdict M1 "$req" "$opt" $TARGETS)"

  # The same outputs, with stray files that share the label. Each holds no result line and an unregistered
  # FAILED test, so a judge that read them would say COMPILE-FAIL or MISMATCH.
  for name in "M1.diffstat" "M1.diffstat.txt" "M1.stale.out"; do
    printf '%s\n' " src/buffer/buffer_pool.rs | 2 +-" "test stray::not_a_registered_killer ... FAILED" > "$OUT/$name"
  done
  # shellcheck disable=SC2086
  expect "killed mutant, stray files in \$OUT" "KILLED-AS-REGISTERED" "$(verdict M1 "$req" "$opt" $TARGETS)"

  for t in $TARGETS; do plant cf "$t" 101 "error[E0308]: mismatched types" "error: could not compile \`ferrodb\` (lib test) due to 1 previous error"; done
  # shellcheck disable=SC2086
  expect "compile failure" "COMPILE-FAIL" "$(verdict cf "$req" "$opt" $TARGETS)"

  ok_file to d237 3; ok_file to batch 2; ok_file to heap 5; ok_file to doc 2
  plant to pool 124 "running 12 tests" "test buffer::buffer_pool::tests::a_fault_that_claims_a_frame_while_a_set_is_freed_keeps_it has been running for over 60 seconds"
  # shellcheck disable=SC2086
  expect "timeout" "TIMEOUT" "$(verdict to "$req" "$opt" $TARGETS)"

  ok_file gone d237 3; ok_file gone batch 2; ok_file gone heap 5; ok_file gone doc 2
  # shellcheck disable=SC2086
  expect "a target with no output file" "INCOMPLETE (pool: no output file)" "$(verdict gone "$req" "$opt" $TARGETS)"

  ok_file rcm batch 2; ok_file rcm heap 5; ok_file rcm pool 12; ok_file rcm doc 2
  plant rcm d237 0 "test d237_pin_leak::$k1 ... FAILED" "test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  # shellcheck disable=SC2086
  expect "rc=0 with a FAILED line" "RC-MISMATCH" "$(verdict rcm "$req" "$opt" $TARGETS)"

  for t in $TARGETS; do ok_file sv "$t" 2; done
  # shellcheck disable=SC2086
  expect "survivor" "SURVIVED" "$(verdict sv "$req" "$opt" $TARGETS)"

  fail_file mm d237 1 "$k1" "d237_pin_leak::not_a_registered_killer"; fail_file mm batch 1 "$k2"
  ok_file mm heap 5; ok_file mm pool 12; ok_file mm doc 2
  # shellcheck disable=SC2086
  expect "unexpected failure" "MISMATCH" "$(verdict mm "$req" "$opt" $TARGETS)"

  fail_file md d237 2 "$k1"; fail_file md batch 1 "$k2"; ok_file md heap 5; ok_file md pool 12
  fail_file md doc 1 "src/buffer/buffer_pool.rs - buffer::buffer_pool::PagePin (line 212) - compile fail"
  # shellcheck disable=SC2086
  expect "unexpected doctest failure" "MISMATCH" "$(verdict md "$req" "$opt" $TARGETS)"

  killers_of M8_guards_outlive_the_pin || { echo "self-test FAIL  M8 has no KILLERS row"; st_bad=$((st_bad + 1)); }
  ok_file M8 d237 3; ok_file M8 batch 2; ok_file M8 heap 5; ok_file M8 pool 12
  fail_file M8 doc 0 "src/buffer/buffer_pool.rs - buffer::buffer_pool::PagePin (line 212) - compile fail" \
    "src/buffer/buffer_pool.rs - buffer::buffer_pool::PagePin (line 225) - compile fail"
  # shellcheck disable=SC2086
  expect "M8 by count" "KILLED-AS-REGISTERED (doc: 2 FAILED)" "$(verdict M8 "$req" "$opt" $TARGETS)"

  base_req="a_read_that_fails_unpins_its_page a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none a_drop_refused_for_a_pinned_page_has_freed_nothing"
  # shellcheck disable=SC2086
  fail_file base d237 0 $(printf 'd237_pin_leak::%s ' $base_req)
  expect "base arm" "KILLED-AS-REGISTERED" "$(verdict base "$base_req" "" d237)"

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
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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

echo "== base: src/ at 9aa6968"
git checkout 9aa6968 -- src/; run_target base d237; restore base
v=$(verdict base "a_read_that_fails_unpins_its_page a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none a_drop_refused_for_a_pinned_page_has_freed_nothing" "" d237)
echo "base: $v" | tee -a "$OUT/summary.txt"; ok_verdict "$v" || bad=$((bad + 1))

echo "== base2: src/ at f651096"
git checkout f651096 -- src/; run_target base2 batch; restore base2
v=$(verdict base2 "a_batch_free_with_an_unfreeable_page_frees_none_of_it" "a_concurrent_pinner_never_sees_a_half_freed_set" batch)
echo "base2: $v" | tee -a "$OUT/summary.txt"; ok_verdict "$v" || bad=$((bad + 1))

echo "== base3: src/ at 50688fe (the N1 red commit)"
git checkout 50688fe -- src/; run_target base3 pool; restore base3
v=$(verdict base3 "a_fault_that_claims_a_frame_while_a_set_is_freed_keeps_it a_refused_free_does_not_relabel_a_frame_a_fault_claimed" "" pool)
echo "base3: $v" | tee -a "$OUT/summary.txt"; ok_verdict "$v" || bad=$((bad + 1))

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
    echo "$name: NOT APPLIED" | tee -a "$OUT/summary.txt"; bad=$((bad + 1)); continue
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

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
