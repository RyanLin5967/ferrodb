#!/usr/bin/env python3
"""D35 C1 FIRE CHECK: break the mirror on purpose and prove the suite notices.

The D35 design entry names this as mandatory -- "a detector that has never fired is not a clean
result" -- and states the gate is VOID without it. So each mutation below removes one load-bearing
piece of the lock-free page-table mirror, the buffer-pool suites are run against the break, and the
run is recorded as FIRED (at least one test failed) or SILENT (everything passed).

SILENT is a finding, not a pass. A mutation nothing catches means the suite does not cover that
piece, and the only mutation allowed to be silent is the CONTROL -- which is why there is one: a
mutation that makes the mirror useless without making it wrong must leave the suite green, or the
suite is firing on something other than correctness.

Run from a THROWAWAY worktree. It edits src/ in place and restores from git between mutations.
"""

import subprocess
import sys
import os
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PT = os.path.join(ROOT, "src/buffer/page_table.rs")
BP = os.path.join(ROOT, "src/buffer/buffer_pool.rs")
TQ = os.path.join(ROOT, "src/buffer/touch_queue.rs")

# The committed BASE eviction sequence. ARC is not modified by either half of D44, so this must
# come back byte-identical from every unmutated build -- an EQUALITY gate, not a tolerance.
TRACE_REF = os.path.join(ROOT, "bench/d35_c1_evictiontrace_c1.txt")

# The suites that are supposed to cover the mirror. `--lib` carries PageTable's own unit tests.
SUITES = [
    ["cargo", "test", "--lib", "buffer::"],
    ["cargo", "test", "--test", "integration_buffer_pool_mirror"],
    ["cargo", "test", "--test", "integration_buffer_pool_touch_batching"],
    ["cargo", "test", "--test", "integration_buffer_pool_latch"],
    ["cargo", "test", "--test", "integration_buffer_pool_concurrency"],
]

# (id, file, expectation, old, new). `expectation` is FIRED for a mutation the suite must catch and
# SILENT for the control; both spellings must match the verdict strings computed below exactly.
MUTATIONS = [
    (
        "M1-remove-leaves-the-slot",
        PT,
        "FIRED",
        """        let slot = &self.mirror[self.slot_of(*page_id)];
        if tag_of(slot.load(Ordering::Relaxed)) == Some(*page_id) {
            slot.store(EMPTY, Ordering::Release);
        }
        prev""",
        """        prev""",
    ),
    (
        "M2-lookup-drops-the-tag-check",
        PT,
        "FIRED",
        """        if tag_of(slot) == Some(page_id) { Some(frame_of(slot)) } else { None }""",
        """        if slot != EMPTY { Some(frame_of(slot)) } else { None }""",
    ),
    (
        "M3-clear-leaves-the-mirror",
        PT,
        "FIRED",
        """        self.map.clear();
        for slot in self.mirror {
            slot.store(EMPTY, Ordering::Release);
        }""",
        """        self.map.clear();""",
    ),
    (
        "M4-fetch-drops-the-frame-label-recheck",
        BP,
        "FIRED",
        """        let frame = self.frames[frame_i].read().unwrap();
        if frame.page_id != Some(page_id) {
            return None;
        }
        frame.pin_counter.fetch_add(1, Ordering::Relaxed);
        Some(frame_i)""",
        """        let frame = self.frames[frame_i].read().unwrap();
        frame.pin_counter.fetch_add(1, Ordering::Relaxed);
        Some(frame_i)""",
    ),
    (
        "M5-unpin-drops-the-frame-label-recheck",
        BP,
        "FIRED",
        """        let frame = self.frames[frame_i].read().unwrap();
        if frame.page_id != Some(page_id) {
            return false;
        }
        if is_dirty {""",
        """        let frame = self.frames[frame_i].read().unwrap();
        if is_dirty {""",
    ),
    (
        "M6-fetch-does-not-fall-back-to-the-map",
        BP,
        "FIRED",
        """        let frame_i = self.page_table.read().unwrap().get(&page_id).copied()?;
        self.pin_if_labelled(frame_i, page_id)""",
        """        None""",
    ),
    (
        # Never publishing to the mirror silently turns C1 back into the code it replaced: every
        # lookup misses and both hit paths fall back to the map. Correct, and a total loss of the
        # change. A suite that cannot see that cannot tell C1 from its own absence, so this must
        # FIRE even though nothing it does is unsafe.
        "M7-insert-never-publishes",
        PT,
        "FIRED",
        """        self.mirror[self.slot_of(page_id)].store(pack(page_id, frame_i), Ordering::Release);
        prev""",
        """        let _ = pack(page_id, frame_i);
        prev""",
    ),
    # ── D44: the batched hit-path policy update ───────────────────────────────────────────────
    (
        # A shard that fills hands its batch back to be applied. Dropping it silently discards
        # recency information -- ARC would think the hottest pages were cold, and NO throughput
        # benchmark could see it. This is the failure the eviction-trace gate exists for.
        "T1-record-drops-a-full-batch",
        TQ,
        "FIRED",
        """        if pending.len() >= self.batch {
            // Swap in a fresh buffer rather than draining into the caller's, so the shard lock is
            // held for a pointer swap and not for a copy.
            return Some(std::mem::replace(&mut *pending, Vec::with_capacity(self.batch)));
        }
        None""",
        """        if pending.len() >= self.batch {
            pending.clear();
            return None;
        }
        None""",
    ),
    (
        # The whole correctness argument for batching. Without the drain, ARC decides what to evict
        # from a recency order that is missing every hit since the last miss.
        "T2-arc_locked-does-not-drain",
        BP,
        "FIRED",
        """        let mut pending = Vec::new();
        self.touch_queue.drain_into(&mut pending);
        for page_id in pending {""",
        """        let pending: Vec<u32> = Vec::new();
        for page_id in pending {""",
    ),
    (
        # The hit path throwing away the batch its own shard handed back.
        "T3-hit-path-ignores-the-returned-batch",
        BP,
        "FIRED",
        """                if let Some(batch) = self.touch_queue.record(page_id) {
                    // `arc_locked` has already drained what was pending when it took the lock;
                    // this batch left the shard before that, so it is applied after, which is the
                    // order the hits happened in.
                    let mut cache = self.arc_locked();
                    for id in batch {
                        cache.touch(id);
                    }
                }""",
        """                let _ = self.touch_queue.record(page_id);""",
    ),
    (
        # A drain that copies instead of taking. Every update is then applied again on the next
        # drain: a `touch` ARC never earned, moving a page to the front of T2 for a reference that
        # did not happen.
        "T4-drain-does-not-empty-the-shard",
        TQ,
        "FIRED",
        """            let mut pending = shard.pending.lock().unwrap();
            out.append(&mut pending);""",
        """            let pending = shard.pending.lock().unwrap();
            out.extend_from_slice(&pending);""",
    ),
    (
        # ⭐ THE SECOND CONTROL, and it tests a CLAIM rather than just the detector. The batch size
        # is documented as unable to affect ARC's DECISIONS at all, because every decision is
        # preceded by a full drain, so no decision can ever observe a backlog. If that is true,
        # a 64-FOLD change -- 512 down to 8 -- must leave both the suites and the eviction trace
        # untouched. If this FIRES, the claim is false and the batch size is a policy knob in
        # disguise, which would make 512 unshippable however fast it is.
        "CONTROL-batch-size-must-not-change-behaviour",
        BP,
        "SILENT",
        """const TOUCH_BATCH: usize = 512;""",
        """const TOUCH_BATCH: usize = 8;""",
    ),
    (
        # THE CONTROL, and the gate is void without it: a detector that fires on everything is not
        # a detector. This mutates the SAME LINE as M2 -- `lookup`'s load -- to a STRONGER memory
        # ordering, which cannot change any outcome. If this fires, the suite is reacting to the
        # line having been touched rather than to the pool being wrong, and every FIRE above is
        # worth less than it looks.
        "CONTROL-stronger-ordering-must-stay-silent",
        PT,
        "SILENT",
        """        let slot = self.mirror[self.slot_of(page_id)].load(Ordering::Acquire);
        if tag_of(slot) == Some(page_id) { Some(frame_of(slot)) } else { None }""",
        """        let slot = self.mirror[self.slot_of(page_id)].load(Ordering::SeqCst);
        if tag_of(slot) == Some(page_id) { Some(frame_of(slot)) } else { None }""",
    ),
]


def run(cmd, timeout=1800):
    t0 = time.time()
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout + p.stderr, time.time() - t0


def restore():
    subprocess.run(["git", "checkout", "HEAD", "--", PT, BP, TQ], cwd=ROOT, check=True)


def sha256(path):
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


def check_trace():
    """Replay the fixed eviction trace and compare it to the committed BASE sequence.

    This is the gate the suites cannot be: a batching scheme that quietly reorders ARC's evictions
    breaks nothing a unit test asserts and no throughput benchmark can see it. Returns MATCH,
    DIFFERS, or BUILD-FAILED.
    """
    rc, out, _ = run(["cargo", "build", "--release", "--example", "d35_eviction_trace"])
    if rc != 0:
        return "BUILD-FAILED", None
    binp = os.path.join(ROOT, "target/release/examples/d35_eviction_trace")
    tmp = os.path.join(ROOT, "target", "firecheck_trace.txt")
    with open(tmp, "wb") as f:
        pr = subprocess.run([binp], cwd=ROOT, stdout=f, stderr=subprocess.DEVNULL, timeout=900)
    if pr.returncode != 0:
        return "TRACE-ABORTED", None
    got = sha256(tmp)
    want = sha256(TRACE_REF)
    return ("MATCH" if got == want else "DIFFERS"), got


def apply(path, old, new):
    src = open(path).read()
    if src.count(old) != 1:
        raise SystemExit(
            f"REFUSING: the anchor for this mutation appears {src.count(old)} times in {path}, "
            f"not once. A mutation that does not land makes the whole check a lie."
        )
    open(path, "w").write(src.replace(old, new))


def main():
    restore()
    # Tracked files only: this script itself is untracked in the throwaway worktree it runs in.
    rc, out, _ = run(["git", "status", "--porcelain", "--untracked-files=no"])
    if out.strip():
        raise SystemExit(f"REFUSING: tracked files are modified, so the baseline is not the tip:\n{out}")

    print("# D35 C1 fire check: break the page-table mirror on purpose, confirm the suite notices")
    print(f"# generated {time.strftime('%Y-%m-%dT%H:%M:%S%z')}")
    rc, head, _ = run(["git", "rev-parse", "HEAD"])
    rc, branch, _ = run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    print(f"# base commit: {head.strip()} on {branch.strip()}")
    print(f"# host: {os.cpu_count()} cores, load average at start: {os.getloadavg()}")
    print("#")
    print("# FIRED  = at least one test failed against the break. That is the result wanted.")
    print("# SILENT = everything passed against the break. A hole in the suite, not a pass --")
    print("#          except for the CONTROL, which must be SILENT or the tests are keyed to the")
    print("#          mirror being USED rather than to the pool being CORRECT.")
    print()

    # The baseline first. Without it a mutation that fails to COMPILE would read as FIRED.
    # Twice, because the concurrent case is the only nondeterministic test here and a flaky
    # baseline would make every SILENT below unreadable.
    print("===== BASELINE (unmutated, 2 reps) =====")
    baseline_ok = True
    for rep in (1, 2):
        for cmd in SUITES:
            rc, out, secs = run(cmd)
            line = [l for l in out.splitlines() if l.startswith("test result:")]
            print(f"  rep{rep} {' '.join(cmd[1:])}: rc={rc} {' | '.join(line)} ({secs:.1f}s)")
            if rc != 0:
                baseline_ok = False
    tv, tsha = check_trace()
    print(f"  BASELINE eviction trace: {tv} (sha256 {tsha})")
    if tv != "MATCH":
        baseline_ok = False
    print(f"  BASELINE = {'GREEN' if baseline_ok else 'RED -- every verdict below is worthless'}")
    print()
    if not baseline_ok:
        raise SystemExit(2)

    verdicts = []
    for name, path, expectation, old, new in MUTATIONS:
        restore()
        apply(path, old, new)
        print(f"===== {name}  (expect {expectation}) =====")
        print(f"# in {os.path.relpath(path, ROOT)}")
        fired = False
        compiled = True
        for cmd in SUITES:
            rc, out, secs = run(cmd)
            results = [l for l in out.splitlines() if l.startswith("test result:")]
            failed = [l for l in out.splitlines() if l.strip().startswith("test ") and "FAILED" in l]
            if "error[" in out or "error: could not compile" in out:
                compiled = False
                print(f"  {' '.join(cmd[1:])}: DID NOT COMPILE")
                continue
            print(f"  {' '.join(cmd[1:])}: rc={rc} {' | '.join(results)} ({secs:.1f}s)")
            for f in failed[:6]:
                print(f"      {f.strip()}")
            if rc != 0:
                fired = True
        if compiled:
            tv, tsha = check_trace()
            print(f"  eviction trace: {tv}" + (f" (sha256 {tsha})" if tsha else ""))
            if tv == "BUILD-FAILED":
                compiled = False
            elif tv != "MATCH":
                fired = True
        if not compiled:
            verdict = "DID-NOT-COMPILE"
        else:
            verdict = "FIRED" if fired else "SILENT"
        ok = verdict == expectation
        print(f"  VERDICT: {verdict}   expected {expectation}   -> {'OK' if ok else '*** PROBLEM ***'}")
        print()
        verdicts.append((name, expectation, verdict, ok))

    restore()
    print("===== SUMMARY =====")
    for name, exp, got, ok in verdicts:
        print(f"  {'OK ' if ok else 'BAD'}  {name}: expected {exp}, got {got}")
    bad = [v for v in verdicts if not v[3]]
    print(f"FIRECHECK_RESULT={'PASS' if not bad else 'FAIL'} ({len(verdicts) - len(bad)}/{len(verdicts)})")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
