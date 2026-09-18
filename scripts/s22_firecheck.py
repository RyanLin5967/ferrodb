#!/usr/bin/env python3
"""S22 fire-check: break each buffer-pool guard on purpose and confirm a test catches it.

A guard that has never been observed to fire is not a verified guard, and a test that has
never been observed to fail is not a verified test. This applies one mutation at a time to
`src/buffer/buffer_pool.rs`, runs the two S22 invariant tests, records whether they failed,
and restores the file.

Every mutation is applied to a pristine copy read from git, so a crash between mutations
cannot leave a broken guard in the tree.
"""
import subprocess, sys, pathlib, shutil, tempfile, os

ROOT = pathlib.Path("/Users/idide/wt/ferrodb-S22-bufpool-latch")
SRC = ROOT / "src/buffer/buffer_pool.rs"
ROUNDS = 3

PIN_TEST = "a_pinned_page_is_never_evicted_however_hard_the_pool_churns"
DUP_TEST = "concurrent_fetches_of_one_cold_page_read_it_once_into_one_frame"

# (id, description, which test should catch it, [(find, replace), ...])
MUTATIONS = [
    ("M1", "try_pin_resident: trust the page table, drop the frame-label re-check", PIN_TEST, [
        ("""        let frame = self.frames[frame_i].read().unwrap();
        if frame.page_id != Some(page_id) {
            return None;
        }
        frame.pin_counter.fetch_add(1, Ordering::Relaxed);""",
         """        let frame = self.frames[frame_i].read().unwrap();
        frame.pin_counter.fetch_add(1, Ordering::Relaxed);"""),
    ]),
    ("M2b", "evict_into: drop BOTH frame-latch pin checks, keep the policy's is_pinned", PIN_TEST, [
        ("""            if frame.page_id != Some(victim) || frame.pin_counter.load(Ordering::Relaxed) > 0 {
                return Ok(None);
            }""",
         """            if frame.page_id != Some(victim) {
                return Ok(None);
            }"""),
        ("""        if frame.page_id != Some(victim)
            || frame.pin_counter.load(Ordering::Relaxed) > 0
            || frame.dirty_flag.load(Ordering::Relaxed)
        {
            return Ok(None);
        }""",
         """        if frame.page_id != Some(victim)
            || frame.dirty_flag.load(Ordering::Relaxed)
        {
            return Ok(None);
        }"""),
    ]),
    ("M2c", "M2b PLUS is_pinned always false: the policy no longer skips pinned pages either", PIN_TEST, [
        ("""            if frame.page_id != Some(victim) || frame.pin_counter.load(Ordering::Relaxed) > 0 {
                return Ok(None);
            }""",
         """            if frame.page_id != Some(victim) {
                return Ok(None);
            }"""),
        ("""        if frame.page_id != Some(victim)
            || frame.pin_counter.load(Ordering::Relaxed) > 0
            || frame.dirty_flag.load(Ordering::Relaxed)
        {
            return Ok(None);
        }""",
         """        if frame.page_id != Some(victim)
            || frame.dirty_flag.load(Ordering::Relaxed)
        {
            return Ok(None);
        }"""),
        ("""        self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0
    }""",
         """        let _ = frame_i;
        false
    }"""),
    ]),
    ("M3", "fetch_page: never wait on in_transit, so two threads fault the same page at once", DUP_TEST, [
        ("""            if transit.contains(&page_id) {""",
         """            if false {"""),
    ]),
]


def run(cmd, **kw):
    return subprocess.run(cmd, shell=True, cwd=ROOT, capture_output=True, text=True, **kw)


def pristine():
    r = run("git show HEAD:src/buffer/buffer_pool.rs")
    if r.returncode != 0:
        sys.exit(f"cannot read pristine source: {r.stderr}")
    return r.stdout


def main():
    base = pristine()
    # The working tree must match HEAD before we start, or "restored" means nothing.
    if SRC.read_text() != base:
        sys.exit("REFUSING: src/buffer/buffer_pool.rs differs from HEAD before any mutation.")

    results = []
    try:
        for mid, desc, test, edits in MUTATIONS:
            text = base
            for find, repl in edits:
                if text.count(find) != 1:
                    sys.exit(f"{mid}: anchor matched {text.count(find)} times, expected 1:\n{find[:120]}")
                text = text.replace(find, repl)
            SRC.write_text(text)

            outcomes = []
            for r in range(ROUNDS):
                p = run(f"timeout 600 cargo test --test integration_buffer_pool_concurrency {test} 2>&1")
                out = p.stdout + p.stderr
                if "error[" in out or "error: could not compile" in out:
                    outcomes.append("COMPILE-ERROR")
                elif " 1 passed" in out and "0 failed" in out:
                    outcomes.append("pass")
                elif "FAILED" in out or " 1 failed" in out:
                    outcomes.append("FAIL")
                else:
                    outcomes.append("?unclear")
            fails = sum(1 for o in outcomes if o == "FAIL")
            results.append((mid, desc, test, fails, outcomes))
            print(f"{mid}: {fails}/{ROUNDS} runs FAILED  ({','.join(outcomes)})  <- {desc}", flush=True)
    finally:
        SRC.write_text(base)
        print("restored src/buffer/buffer_pool.rs to HEAD", flush=True)

    lines = ["# S22 fire-check: every guard broken on purpose, and whether a test caught it",
             "#",
             "# A guard that has never fired is not a verified guard. Each row applies ONE mutation to",
             "# src/buffer/buffer_pool.rs, runs the named test 3 times, and reports how many runs failed.",
             "# 3/3 FAILED is the result being sought. 0/3 means the test does NOT cover that guard.",
             ""]
    for mid, desc, test, fails, outcomes in results:
        verdict = "CAUGHT" if fails == ROUNDS else ("FLAKY-CAUGHT" if fails else "NOT CAUGHT")
        lines.append(f"{mid}\t{fails}/{ROUNDS} failed\t{verdict}\t{test}")
        lines.append(f"\tmutation: {desc}")
        lines.append(f"\truns: {','.join(outcomes)}")
        lines.append("")
    (ROOT / "bench/s22_firecheck.txt").write_text("\n".join(lines))
    print("wrote bench/s22_firecheck.txt")


if __name__ == "__main__":
    main()
