#!/usr/bin/env bash
# D237 fire-check, amendment 5 (review 3 R3/R4/R6 and the judge review J1-J7): killers are bound to a TARGET
# and a registered panic REASON as well as a name; the control checks the registered counts as well as
# `-- --list`; a crashed binary and an outside kill get their own labels; the listing runs in the main shell;
# HUP and QUIT are trapped; src/ checkouts are --no-overlay; --self-test covers every clause and file state.
# Pre-registration: artie-research frontier/lane_d237.md §12 and its addendum (the arms of §10-§11 otherwise
# unchanged). Run from the worktree root at DEFAULT QoS (never taskpolicy -b). This is fan work: it runs
# only when the lead releases the FAN-QUEUE row.
#
# Usage: bash bench/d237/firecheck.sh               the run
#        bash bench/d237/firecheck.sh --self-test   the judge on planted outputs only: no cargo, no git writes
#
# Targets: d237 (tests/d237_pin_leak.rs), batch (tests/d237_batch_free.rs), heap (the heap's unit tests),
# pool (buffer_pool's unit tests: the N1 and R1 tests), doc (`cargo test --doc PagePin`, the two compile_fail
# doctests). Each runs one test binary.
# Arms, in run order:
# - control: SUBJECT_SHA, all five targets: every file OK, nothing FAILED, and per target passed equals the
#   REGISTERED count and passed + ignored equals the harness's own `-- --list` count. Anything else VOIDS
#   the run (exit 2).
# - base:  src/ at 9aa6968, d237: exactly its three tests FAIL, each with its registered reason.
# - base2: src/ at f651096, batch: the unfreeable-page test FAILS (reason registered); the concurrent one may.
# - base3: src/ at 50688fe (the N1 red commit), pool: exactly the two N1 tests FAIL (reasons registered).
# - mutants: KILLED-AS-REGISTERED when every file is OK, every required target:killer FAILED with its
#   registered reason (where one is registered), and nothing outside required + optional did (a FAILED
#   doctest counts as outside, except for M8). M8 is judged on the doc target by count: exactly 2 FAILED
#   and nothing else. Anything else counts against the run.
#
# Blind spots, stated: five selections, not the whole suite. The judge reads cargo's own lines ("test
# result:", "test <name> ... FAILED", "---- <name> stdout ----" blocks) and the rc this script appends; a test
# that printed a line of those shapes to stdout would be misread (none is run with --nocapture). A reason is a
# fixed substring of the panic message; a killer with no registered reason is judged by target and name only.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

SUBJECT_SHA=c925cbd
OUT=bench/d237/firecheck
REGISTERED="d237=3 batch=2 heap=5 pool=8 doc=2" # lane §12 addendum, read from the source at SUBJECT_SHA

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
  "M11_evict_calls_freeing_gone|$B|"'"            if frame.dirty_flag.load(Ordering::Relaxed) && !frame.freeing {\n"|"            if frame.freeing {\n                return Ok(Evicted::Gone);\n            }\n            if frame.dirty_flag.load(Ordering::Relaxed) {\n"'
)
# name|required target:killers (all must FAIL), or DOC2 for "the doc target has exactly 2 FAILED"|optional
READ_FAILS=d237:a_read_that_fails_unpins_its_page
REINSERT=d237:a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none
DROP_REFUSED=d237:a_drop_refused_for_a_pinned_page_has_freed_nothing
UNFREEABLE=batch:a_batch_free_with_an_unfreeable_page_frees_none_of_it
CONCURRENT=batch:a_concurrent_pinner_never_sees_a_half_freed_set
N1_KEEPS=pool:a_fault_that_claims_a_frame_while_a_set_is_freed_keeps_it
N1_REFUSED=pool:a_refused_free_does_not_relabel_a_frame_a_fault_claimed
R1_REFUSED=pool:a_victim_a_refused_free_was_freeing_is_not_reported_gone
R1_FREED=pool:a_victim_a_completed_free_took_is_reported_gone
KILLERS=(
  "base|$READ_FAILS $REINSERT $DROP_REFUSED|"
  "base2|$UNFREEABLE|$CONCURRENT"
  "base3|$N1_KEEPS $N1_REFUSED|"
  "M1_read_hand_unpin|$READ_FAILS $REINSERT|"
  "M2_batch_free_unchecked|$DROP_REFUSED|$CONCURRENT"
  "M3_drop_per_structure|$DROP_REFUSED|"
  "M4_guard_skips_early_return|$READ_FAILS $REINSERT $DROP_REFUSED|"
  "M5_batch_free_lets_go|$UNFREEABLE|$CONCURRENT $N1_KEEPS $N1_REFUSED $R1_REFUSED $R1_FREED"
  "M6_bitmap_written_as_built|$UNFREEABLE|"
  "M7_no_unmark_on_pass_2_error|$UNFREEABLE|$N1_REFUSED $R1_REFUSED"
  "M8_guards_outlive_the_pin|DOC2|"
  "M9_pass_1_unlabels|$N1_KEEPS $N1_REFUSED $R1_REFUSED|$UNFREEABLE $DROP_REFUSED $CONCURRENT"
  "M10_pin_ignores_freeing|$N1_KEEPS|$CONCURRENT"
  "M11_evict_calls_freeing_gone|$R1_REFUSED|"
)
# name|target:killer|registered reason: fixed substrings of its panic message, alternatives separated by ~~
REASONS=(
  "base|$READ_FAILS|and left page"
  "base|$REINSERT|left these pages of t pinned"
  "base|$DROP_REFUSED|having already freed"
  "base2|$UNFREEABLE|having already freed"
  "base3|$N1_KEEPS|the fault claimed frame"
  "base3|$N1_REFUSED|table entry names frame"
  "M1_read_hand_unpin|$READ_FAILS|and left page"
  "M1_read_hand_unpin|$REINSERT|left these pages of t pinned"
  "M2_batch_free_unchecked|$DROP_REFUSED|succeeded while page"
  "M3_drop_per_structure|$DROP_REFUSED|having already freed"
  "M4_guard_skips_early_return|$READ_FAILS|and left page"
  "M4_guard_skips_early_return|$REINSERT|left these pages of t pinned"
  "M4_guard_skips_early_return|$DROP_REFUSED|refused with nothing pinned~~premise: the test's own pin"
  "M5_batch_free_lets_go|$UNFREEABLE|having already freed"
  "M6_bitmap_written_as_built|$UNFREEABLE|having already freed"
  "M7_no_unmark_on_pass_2_error|$UNFREEABLE|cannot be pinned after the refusal"
  "M9_pass_1_unlabels|$N1_KEEPS|the fault claimed frame"
  "M9_pass_1_unlabels|$N1_REFUSED|table entry names frame"
  "M9_pass_1_unlabels|$R1_REFUSED|premise: the victim's frame keeps its label"
  "M10_pin_ignores_freeing|$N1_KEEPS|pin_if_labelled pinned"
  "M11_evict_calls_freeing_gone|$R1_REFUSED|reported Gone"
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
binaries() { echo 1; } # test binaries per target: one each here

# A command runs in the background and the script waits on it, so a TERM, INT, HUP or QUIT reaches the traps
# at once instead of after the command (bash defers a trap until a foreground child exits, and `timeout`
# puts cargo in its own process group, out of reach of a signal to this script's group). The trap stops the
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
  waited timeout 1800 cargo test $(target_args "$2") > "$OUT/$1.$2.out" 2>&1
  rc=$?
  echo "rc=$rc" >> "$OUT/$1.$2.out"
}

# The listing runs in the main shell, through `waited`, never inside `$(...)`: a command substitution is a
# subshell, so the trap could not see its `child`, and a TERM was deferred until the listing ended or left
# it running (judge review J4). Only the count runs inside `$(...)`.
list_target() { # $1 = target: what the harness says it will run, at SUBJECT_SHA, kept as control.<t>.list
  # shellcheck disable=SC2046
  waited timeout 1800 cargo test $(target_args "$1") -- --list > "$OUT/control.$1.list" 2>&1
}
listed_count() { grep -cE ': test$' "$OUT/control.$1.list"; }

# ---- The judge. It reads ONLY "$OUT/<label>.<target>.out" for the targets it is given. Any other file in
# $OUT (a diffstat, a list, the self-test log, a stale output) cannot change a verdict (D233 review 3 H1). ----
tfile() { printf '%s/%s.%s.out' "$OUT" "$1" "$2"; }

# One target file's state: OK, or the reason its names cannot be judged.
tstate() { # $1 label, $2 target
  local f last rc n nf
  f=$(tfile "$1" "$2")
  [ -f "$f" ] || { echo "INCOMPLETE ($2: no output file)"; return; }
  last=$(tail -n 1 "$f")
  case "$last" in rc=[0-9]*) rc=${last#rc=} ;; *) echo "INCOMPLETE ($2: no rc line)"; return ;; esac
  if [ "$rc" = 124 ]; then echo "TIMEOUT ($2)"; return; fi
  # Before the result-line count (judge review J1): an outside SIGKILL is RC-137, not COMPILE-FAIL.
  case "$rc" in 0|101) ;; *) echo "RC-$rc ($2)"; return ;; esac
  # A test binary killed by a signal (an abort, a stack overflow, a segfault): cargo exits 101 and names
  # the signal. Checked before the result-line count, which a crash also removes (J1).
  if grep -qE "process didn't exit successfully: .*\(signal: [0-9]+" "$f"; then echo "CRASHED ($2)"; return; fi
  n=$(grep -cE '^test result:' "$f")
  if [ "$n" -eq 0 ]; then echo "COMPILE-FAIL ($2)"; return; fi
  if [ "$n" -ne "$(binaries "$2")" ]; then echo "INCOMPLETE ($2: $n result lines)"; return; fi
  nf=$(grep -cE '^test .+ \.\.\. FAILED$' "$f")
  case "$rc/$nf" in
    0/0) ;;
    101/0) echo "RC-MISMATCH ($2: rc=101 and nothing FAILED)"; return ;;
    101/*) ;;
    *) echo "RC-MISMATCH ($2: rc=0 with FAILED lines)"; return ;;
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

# The FAILED tests as target:short-name pairs, so a killer is bound to the binary that holds it (judge
# review J2). The doc target is counted apart: doctest names carry line numbers.
failed_pairs() { # $1 label, then targets
  local label=$1 t
  shift
  for t in "$@"; do
    [ "$t" = doc ] && continue
    sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' "$(tfile "$label" "$t")" | sed -E "s/.*:://; s/^/$t:/"
  done | sort -u
}

doc_failed() { grep -cE '^test .+ \.\.\. FAILED$' "$(tfile "$1" doc)"; }

count_in() { # $1 label, $2 target, $3 passed|ignored: summed over the file's result lines
  grep -E '^test result:' "$(tfile "$1" "$2")" | sed -nE "s/.* ([0-9]+) $3.*/\\1/p" | awk '{ s += $1 } END { print s + 0 }'
}

words() { printf '%s\n' $1 | sed '/^$/d' | sort -u; }

# ---- Reasons (review 3 R4): a required killer counts only if its panic message holds its registered reason.
reason_of() { # $1 KILLERS/REASONS key, $2 target:killer: the registered reason spec, or nothing
  local r rname rkey rreason
  for r in "${REASONS[@]}"; do
    IFS='|' read -r rname rkey rreason <<< "$r"
    if [ "$rname" = "$1" ] && [ "$rkey" = "$2" ]; then printf '%s' "$rreason"; return; fi
  done
}
full_name() { # $1 label, $2 target, $3 short name: the full name its FAILED line gives
  sed -nE 's/^test (.+) \.\.\. FAILED$/\1/p' "$(tfile "$1" "$2")" |
    awk -v s="$3" '{ n = $0; sub(/.*::/, "", n); if (n == s) { print; exit } }'
}
failure_block() { # $1 label, $2 target, $3 full name: the lines of its "---- <name> stdout ----" block
  awk -v h="---- $3 stdout ----" '
    $0 == h { on = 1; next }
    on && (/^---- .* stdout ----$/ || /^failures:$/) { exit }
    on { print }' "$(tfile "$1" "$2")"
}
reason_ok() { # $1 label, $2 target:killer, $3 reason spec: some alternative is in its failure block
  local t=${2%%:*} s=${2#*:} full block rest alt
  full=$(full_name "$1" "$t" "$s")
  [ -n "$full" ] || return 1
  block=$(failure_block "$1" "$t" "$full")
  rest=$3
  while [ -n "$rest" ]; do
    case "$rest" in
      *'~~'*) alt=${rest%%~~*}; rest=${rest#*~~} ;;
      *) alt=$rest; rest="" ;;
    esac
    case "$block" in *"$alt"*) return 0 ;; esac
  done
  return 1
}

verdict() { # $1 label, $2 key (KILLERS/REASONS), $3 required (or DOC2), $4 optional, then the targets
  local label=$1 key=$2 req=$3 opt=$4 st actual extra missing d k spec wrong=""
  shift 4
  st=$(arm_state "$label" "$@")
  [ "$st" = OK ] || { echo "$st"; return; }
  actual=$(failed_pairs "$label" "$@")
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
  for k in $req; do
    printf '%s\n' "$actual" | grep -qxF "$k" || continue
    spec=$(reason_of "$key" "$k")
    [ -z "$spec" ] || reason_ok "$label" "$k" "$spec" || wrong="$wrong $k"
  done
  if [ -z "$missing" ] && [ -z "$(echo $extra)" ] && [ -z "$wrong" ]; then echo "KILLED-AS-REGISTERED ($(echo $actual))"
  else echo "MISMATCH (missing: $(echo $missing); unexpected: $(echo $extra); wrong reason:$wrong)"; fi
}

control_verdict() { # $1 label, $2 "target=listed ..." (the harness's own -- --list counts)
  local label=$1 lists=$2 st t l p i r
  # shellcheck disable=SC2086
  st=$(arm_state "$label" $TARGETS)
  [ "$st" = OK ] || { echo "VOID ($st)"; return; }
  # shellcheck disable=SC2086
  if [ -n "$(failed_pairs "$label" $TARGETS)" ] || [ "$(doc_failed "$label")" != 0 ]; then
    echo "VOID (a test FAILED)"; return
  fi
  for t in $TARGETS; do
    # shellcheck disable=SC2086
    l=$(printf '%s\n' $lists | sed -n "s/^$t=//p")
    # shellcheck disable=SC2086
    r=$(printf '%s\n' $REGISTERED | sed -n "s/^$t=//p")
    p=$(count_in "$label" "$t" passed)
    i=$(count_in "$label" "$t" ignored)
    case "$l" in ''|*[!0-9]*) echo "VOID ($t: no list count)"; return ;; esac
    if [ "$l" -eq 0 ] || [ "$p" -eq 0 ] || [ $((p + i)) -ne "$l" ]; then
      echo "VOID ($t: listed $l, passed $p, ignored $i)"; return
    fi
    # Review 3 R3: a test lost at the subject lowers the list and the passed count alike; only the
    # registered count sees it.
    if [ "$p" != "$r" ]; then echo "VOID ($t: passed $p, registered ${r:-none})"; return; fi
  done
  echo clean
}

ok_verdict() { case "$1" in KILLED-AS-REGISTERED*) return 0 ;; *) return 1 ;; esac; }

killers_of() { # $1 key: sets req and opt from KILLERS; returns 1 if it has no row
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
result_ok() { echo "test result: ok. $1 passed; 0 failed; $2 ignored; 0 measured; 0 filtered out; finished in 0.01s"; }
ok_file() { # $1 label, $2 target, $3 passed
  plant "$1" "$2" 0 "running $3 tests" "$(result_ok "$3" 0)"
}
# The FAILED tests are given as "full::name@@panic message"; the message defaults to "a planted failure".
# Each gets its FAILED line and a "---- <full name> stdout ----" block, as libtest prints them.
fail_file() { # $1 label, $2 target, $3 passed, then the failures
  local label=$1 t=$2 p=$3 e full msg lines="" blocks=""
  shift 3
  for e in "$@"; do
    full=${e%%@@*}
    case "$e" in *@@*) msg=${e#*@@} ;; *) msg="a planted failure" ;; esac
    lines="${lines}test $full ... FAILED
"
    blocks="${blocks}---- $full stdout ----
thread '$full' panicked at src/planted.rs:1:1:
$msg

"
  done
  plant "$label" "$t" 101 "running tests" "${lines%?}" "" "failures:" "" "${blocks%?}" "failures:" \
    "test result: FAILED. $p passed; $# failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
}
reg() { printf '%s\n' $REGISTERED | sed -n "s/^$1=//p"; } # a target's registered count
full_of() { # $1 target:killer: a plausible full name for it in its binary
  case "${1%%:*}" in
    pool) echo "buffer::buffer_pool::tests::${1#*:}" ;;
    heap) echo "storage::heap_file_manager::tests::${1#*:}" ;;
    *) echo "${1#*:}" ;;
  esac
}
# Plant a mutant's outputs: each named killer FAILS in its own target, with the reason registered for it
# under `key` (or "a planted failure"); $4 are more failures as target:killer; every other target is clean.
plant_kill() { # $1 label, $2 key, $3 target:killers, $4 more target:killers
  local label=$1 key=$2 t k spec n entries
  for t in d237 batch heap pool; do
    n=0
    for k in $3 $4; do [ "${k%%:*}" = "$t" ] && n=$((n + 1)); done
    if [ "$n" -eq 0 ]; then ok_file "$label" "$t" "$(reg "$t")"; continue; fi
    entries=()
    for k in $3 $4; do
      [ "${k%%:*}" = "$t" ] || continue
      spec=$(reason_of "$key" "$k")
      entries+=("$(full_of "$k")@@${spec%%~~*}")
    done
    fail_file "$label" "$t" 1 "${entries[@]}"
  done
  ok_file "$label" doc "$(reg doc)"
}
expect() { # $1 case, $2 expected verdict prefix, $3 actual verdict
  case "$3" in
    "$2"*) echo "self-test PASS  $1: $3" ;;
    *) echo "self-test FAIL  $1: expected '$2', got '$3'"; st_bad=$((st_bad + 1)) ;;
  esac
}
clean_all() { local t; for t in $TARGETS; do ok_file "$1" "$t" "$(reg "$t")"; done; }

self_test() {
  local keep=$OUT st_bad=0 tmp lists m name rest t k1
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/d237-selftest.XXXXXX") || { echo "self-test: mktemp failed"; return 1; }
  OUT=$tmp
  lists=$REGISTERED

  # -- the control
  clean_all control
  expect "clean control" "clean" "$(control_verdict control "$lists")"
  clean_all dirty
  fail_file dirty d237 2 a_read_that_fails_unpins_its_page
  # The list count is planted equal to the passed count, so the count gates alone would pass this
  # control (bar the registered count, set here to match): the case proves the FAILED check fires on its own.
  expect "dirty control" "VOID (a test FAILED)" "$(REGISTERED="d237=2 batch=2 heap=5 pool=8 doc=2" control_verdict dirty "d237=2 batch=2 heap=5 pool=8 doc=2")"
  clean_all docdirty
  fail_file docdirty doc 1 "src/buffer/buffer_pool.rs - buffer::buffer_pool::PagePin (line 212) - compile fail"
  expect "control with a FAILED doctest, list = passed" "VOID (a test FAILED)" "$(REGISTERED="d237=3 batch=2 heap=5 pool=8 doc=1" control_verdict docdirty "d237=3 batch=2 heap=5 pool=8 doc=1")"
  clean_all empty
  plant empty pool 0 "running 0 tests" "$(result_ok 0 0)"
  expect "control that collected nothing" "VOID (pool: listed 0" "$(control_verdict empty "d237=3 batch=2 heap=5 pool=0 doc=2")"
  clean_all short
  expect "control listed 9, passed 8" "VOID (pool: listed 9, passed 8, ignored 0)" "$(control_verdict short "d237=3 batch=2 heap=5 pool=9 doc=2")"
  clean_all allign
  plant allign pool 0 "running 8 tests" "$(result_ok 0 8)"
  expect "control whose target passed nothing and ignored all" "VOID (pool: listed 8, passed 0, ignored 8)" "$(control_verdict allign "$lists")"
  clean_all nolist
  expect "control with no list count" "VOID (doc: no list count)" "$(control_verdict nolist "d237=3 batch=2 heap=5 pool=8")"
  clean_all lost
  ok_file lost pool 7
  expect "control that lost a registered test (list agrees)" "VOID (pool: passed 7, registered 8)" "$(control_verdict lost "d237=3 batch=2 heap=5 pool=7 doc=2")"
  clean_all tocount
  plant tocount pool 124 "running 8 tests" "$(result_ok 8 0)"
  expect "control with a TIMEOUT whose counts add up" "VOID (TIMEOUT (pool))" "$(control_verdict tocount "$lists")"

  # -- a killed mutant, with its sets and reasons read from the tables the run uses
  killers_of M1_read_hand_unpin || { echo "self-test FAIL  M1 has no KILLERS row"; st_bad=$((st_bad + 1)); }
  plant_kill M1 M1_read_hand_unpin "$req" ""
  # shellcheck disable=SC2086
  expect "killed mutant" "KILLED-AS-REGISTERED" "$(verdict M1 M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  # Stray files that share the label, each with no result line and an unregistered FAILED test.
  for name in "M1.diffstat" "M1.diffstat.txt" "M1.stale.out" "M1.d237.out.bak"; do
    printf '%s\n' " src/buffer/buffer_pool.rs | 2 +-" "test stray::not_a_registered_killer ... FAILED" > "$OUT/$name"
  done
  # shellcheck disable=SC2086
  expect "killed mutant, stray files in \$OUT" "KILLED-AS-REGISTERED" "$(verdict M1 M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  plant_kill part M1_read_hand_unpin "${req% *}" ""
  # shellcheck disable=SC2086
  expect "a partial kill (one required killer passed)" "MISMATCH (missing: ${req##* }" "$(verdict part M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  plant_kill mm M1_read_hand_unpin "$req" "pool:not_a_registered_killer"
  # shellcheck disable=SC2086
  expect "unexpected failure" "MISMATCH" "$(verdict mm M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  plant_kill md M1_read_hand_unpin "$req" ""
  fail_file md doc 1 "src/buffer/buffer_pool.rs - buffer::buffer_pool::PagePin (line 212) - compile fail"
  # shellcheck disable=SC2086
  expect "unexpected doctest failure" "MISMATCH" "$(verdict md M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  # The killer's name FAILED, but in the wrong binary (J2).
  k1=${req%% *}
  plant_kill wt M1_read_hand_unpin "${req#* }" "pool:${k1#*:}"
  # shellcheck disable=SC2086
  expect "a killer that failed in the wrong target" "MISMATCH (missing: $k1" "$(verdict wt M1_read_hand_unpin "$req" "$opt" $TARGETS)"

  # -- reasons (R4): M10's killer failing on a seam timeout is not M10's kill
  killers_of M10_pin_ignores_freeing
  ok_file r10 d237 3; ok_file r10 batch 2; ok_file r10 heap 5; ok_file r10 doc 2
  fail_file r10 pool 7 "buffer::buffer_pool::tests::${req#pool:}@@free_pages never reached its seam"
  # shellcheck disable=SC2086
  expect "M10's killer failed on a seam timeout" "MISMATCH (missing: ; unexpected: ; wrong reason: $req" "$(verdict r10 M10_pin_ignores_freeing "$req" "$opt" $TARGETS)"
  plant_kill r10ok M10_pin_ignores_freeing "$req" ""
  # shellcheck disable=SC2086
  expect "M10's killer failed with its registered reason" "KILLED-AS-REGISTERED" "$(verdict r10ok M10_pin_ignores_freeing "$req" "$opt" $TARGETS)"
  killers_of M4_guard_skips_early_return
  ok_file r4 heap 5; ok_file r4 pool 8; ok_file r4 batch 2; ok_file r4 doc 2
  fail_file r4 d237 0 "a_read_that_fails_unpins_its_page@@HeapFileManager::read returned x and left page 3 pinned" \
    "a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none@@the re-INSERT failed and left these pages of t pinned" \
    "a_drop_refused_for_a_pinned_page_has_freed_nothing@@premise: the test's own pin on 9 is released"
  # shellcheck disable=SC2086
  expect "a reason matched by its second alternative" "KILLED-AS-REGISTERED" "$(verdict r4 M4_guard_skips_early_return "$req" "$opt" $TARGETS)"

  # -- the optional-only failure
  killers_of M2_batch_free_unchecked
  plant_kill optonly M2_batch_free_unchecked "" "$opt"
  # shellcheck disable=SC2086
  expect "only an optional killer FAILED" "MISMATCH (missing: $req" "$(verdict optonly M2_batch_free_unchecked "$req" "$opt" $TARGETS)"

  # -- M8, by count
  killers_of M8_guards_outlive_the_pin || { echo "self-test FAIL  M8 has no KILLERS row"; st_bad=$((st_bad + 1)); }
  local pp="src/buffer/buffer_pool.rs - buffer::buffer_pool::PagePin"
  ok_file m8 d237 3; ok_file m8 batch 2; ok_file m8 heap 5; ok_file m8 pool 8
  fail_file m8 doc 0 "$pp (line 212) - compile fail" "$pp (line 225) - compile fail"
  # shellcheck disable=SC2086
  expect "M8 by count" "KILLED-AS-REGISTERED (doc: 2 FAILED)" "$(verdict m8 M8_guards_outlive_the_pin "$req" "$opt" $TARGETS)"
  ok_file m8one d237 3; ok_file m8one batch 2; ok_file m8one heap 5; ok_file m8one pool 8
  fail_file m8one doc 1 "$pp (line 212) - compile fail"
  # shellcheck disable=SC2086
  expect "M8 with 1 doctest FAILED" "MISMATCH (doc FAILED: 1" "$(verdict m8one M8_guards_outlive_the_pin "$req" "$opt" $TARGETS)"
  clean_all m8none
  # shellcheck disable=SC2086
  expect "M8 with no doctest FAILED" "MISMATCH (doc FAILED: 0" "$(verdict m8none M8_guards_outlive_the_pin "$req" "$opt" $TARGETS)"
  ok_file m8three d237 3; ok_file m8three batch 2; ok_file m8three heap 5; ok_file m8three pool 8
  fail_file m8three doc 0 "$pp (line 212) - compile fail" "$pp (line 225) - compile fail" "$pp (line 240) - compile fail"
  # shellcheck disable=SC2086
  expect "M8 with 3 doctests FAILED" "MISMATCH (doc FAILED: 3" "$(verdict m8three M8_guards_outlive_the_pin "$req" "$opt" $TARGETS)"
  ok_file m8x d237 3; ok_file m8x batch 2; ok_file m8x heap 5
  fail_file m8x pool 7 "buffer::buffer_pool::tests::x"
  fail_file m8x doc 0 "$pp (line 212) - compile fail" "$pp (line 225) - compile fail"
  # shellcheck disable=SC2086
  expect "M8 with 2 doctests and another test FAILED" "MISMATCH (doc FAILED: 2; unexpected: pool:x" "$(verdict m8x M8_guards_outlive_the_pin "$req" "$opt" $TARGETS)"

  # -- the file states
  killers_of M1_read_hand_unpin
  for t in $TARGETS; do plant cf "$t" 101 "error[E0308]: mismatched types" "error: could not compile \`ferrodb\` (lib test) due to 1 previous error"; done
  # shellcheck disable=SC2086
  expect "compile failure" "COMPILE-FAIL" "$(verdict cf M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all to
  plant to pool 124 "running 8 tests" "test buffer::buffer_pool::tests::x has been running for over 60 seconds"
  # shellcheck disable=SC2086
  expect "timeout" "TIMEOUT (pool)" "$(verdict to M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all gone
  rm -f "$(tfile gone pool)"
  # shellcheck disable=SC2086
  expect "a target with no output file" "INCOMPLETE (pool: no output file)" "$(verdict gone M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all late
  printf '%s\n' "$(result_ok 8 0)" "rc=0" "a line written after the rc line" > "$(tfile late pool)"
  # shellcheck disable=SC2086
  expect "a line after the rc line" "INCOMPLETE (pool: no rc line)" "$(verdict late M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all two
  plant two pool 0 "$(result_ok 4 0)" "$(result_ok 4 0)"
  # shellcheck disable=SC2086
  expect "two result lines in a one-binary target" "INCOMPLETE (pool: 2 result lines)" "$(verdict two M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all k9r
  plant k9r pool 137 "running 8 tests" "$(result_ok 8 0)"
  # shellcheck disable=SC2086
  expect "rc=137 with a result line" "RC-137 (pool)" "$(verdict k9r M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all k9
  plant k9 pool 137 "   Compiling ferrodb v0.1.0"
  # shellcheck disable=SC2086
  expect "rc=137 with no result line (an outside SIGKILL)" "RC-137 (pool)" "$(verdict k9 M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all nofail
  plant nofail pool 101 "running 8 tests" "test result: FAILED. 7 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  # shellcheck disable=SC2086
  expect "rc=101 with nothing FAILED" "RC-MISMATCH (pool: rc=101 and nothing FAILED)" "$(verdict nofail M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all rcm
  plant rcm pool 0 "test buffer::buffer_pool::tests::x ... FAILED" "test result: FAILED. 7 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
  # shellcheck disable=SC2086
  expect "rc=0 with a FAILED line" "RC-MISMATCH (pool: rc=0 with FAILED lines)" "$(verdict rcm M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all crash
  plant crash batch 101 "running 2 tests" "test a_batch_free_with_an_unfreeable_page_frees_none_of_it ... FAILED" \
    "thread 'a_concurrent_pinner_never_sees_a_half_freed_set' panicked while processing panic. aborting." \
    "error: test failed, to rerun pass \`--test d237_batch_free\`" "" "Caused by:" \
    "  process didn't exit successfully: \`/t/target/debug/deps/d237_batch_free-0123\` (signal: 6, SIGABRT: process abort signal)"
  # shellcheck disable=SC2086
  expect "a test binary killed by a signal" "CRASHED (batch)" "$(verdict crash M1_read_hand_unpin "$req" "$opt" $TARGETS)"
  clean_all sv
  # shellcheck disable=SC2086
  expect "survivor" "SURVIVED" "$(verdict sv M1_read_hand_unpin "$req" "$opt" $TARGETS)"

  # -- the base arms, by their own rows
  killers_of base
  plant_kill base base "$req" ""
  expect "base arm" "KILLED-AS-REGISTERED" "$(verdict base base "$req" "$opt" d237)"
  killers_of base3
  plant_kill base3 base3 "$req" ""
  expect "base3 arm" "KILLED-AS-REGISTERED" "$(verdict base3 base3 "$req" "$opt" pool)"

  # -- registration: every mutant and arm has a row with a required killer; every reason names a required killer
  for m in "${MUTANTS[@]}" "base|" "base2|" "base3|"; do
    IFS='|' read -r name rest <<< "$m"
    if ! killers_of "$name" || [ -z "$req" ]; then
      echo "self-test FAIL  registration: $name has no required killer"; st_bad=$((st_bad + 1))
    fi
  done
  for m in "${REASONS[@]}"; do
    IFS='|' read -r name rest <<< "$m"
    k1=${rest%%|*}
    if ! killers_of "$name" || ! printf '%s\n' $req | grep -qxF "$k1"; then
      echo "self-test FAIL  registration: the reason for $name names $k1, which is not a required killer"; st_bad=$((st_bad + 1))
    fi
  done
  [ "$st_bad" -eq 0 ] && echo "self-test PASS  registration: every row has a required killer, and every reason a required killer"

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

# A run the self-test refuses has already cleared $OUT (judge review, minor): nothing is tracked there.
rm -rf "$OUT"
mkdir -p "$OUT"
if ! self_test > "$OUT/selftest.log" 2>&1; then
  cat "$OUT/selftest.log" >&2
  echo "REFUSED: the judge's self-test failed" >&2
  exit 2
fi

# Every checkout of src/ is --no-overlay, so a file absent at the target commit is removed and "src/ at <sha>"
# is exact (judge review J6); an overlay checkout only adds and overwrites.
on_exit() {
  git checkout --no-overlay "$SUBJECT_SHA" -- src/ 2>/dev/null
  git diff --quiet "$SUBJECT_SHA" -- src/ tests/ ||
    echo "ON EXIT: src/ or tests/ still differ from $SUBJECT_SHA; restore with: git checkout --no-overlay $SUBJECT_SHA -- src/" >&2
}
on_signal() { # $1 = the exit status
  if [ -n "$child" ]; then
    kill -TERM "$child" 2>/dev/null
    wait "$child" 2>/dev/null
  fi
  exit "$1"
}
trap on_exit EXIT
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 131' QUIT
trap 'on_signal 143' TERM

bad=0
restore() {
  git checkout --no-overlay "$SUBJECT_SHA" -- src/
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: src/ not restored after $1" >&2; exit 3; }
}

echo "== control: $SUBJECT_SHA"
for t in $TARGETS; do run_target control "$t"; done
for t in $TARGETS; do list_target "$t"; done
lists=""
for t in $TARGETS; do lists="$lists $t=$(listed_count "$t")"; done
v=$(control_verdict control "$lists")
echo "control: $v (lists:$lists; registered: $REGISTERED)" | tee "$OUT/summary.txt"
[ "$v" = clean ] || exit 2

run_base() { # $1 label, $2 commit, $3 target
  echo "== $1: src/ at $2"
  git checkout --no-overlay "$2" -- src/
  run_target "$1" "$3"
  restore "$1"
  killers_of "$1" || { echo "$1: NO KILLERS ROW" | tee -a "$OUT/summary.txt"; bad=$((bad + 1)); return; }
  v=$(verdict "$1" "$1" "$req" "$opt" "$3")
  echo "$1: $v" | tee -a "$OUT/summary.txt"
  ok_verdict "$v" || bad=$((bad + 1))
}
run_base base 9aa6968 d237
run_base base2 f651096 batch
run_base base3 50688fe pool

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
  git checkout --no-overlay "$SUBJECT_SHA" -- "$FILE"
  git diff --quiet "$SUBJECT_SHA" -- src/ || { echo "ABORT: restore of $FILE after $name left a difference" >&2; exit 3; }
  # shellcheck disable=SC2086
  v=$(verdict "$name" "$name" "$req" "$opt" $TARGETS)
  echo "$name: $v" | tee -a "$OUT/summary.txt"
  ok_verdict "$v" || bad=$((bad + 1))
done

git diff --quiet "$SUBJECT_SHA" -- src/ tests/ || { echo "ABORT: tree not restored" >&2; exit 3; }
echo "not-as-registered=$bad" | tee -a "$OUT/summary.txt"
[ "$bad" -eq 0 ]
