#!/usr/bin/env python3
"""S22 fire-check: break each buffer-pool guard on purpose and see which tests catch it.

A guard that has never been observed to fire is not a verified guard, and a test that has
never been observed to fail is not a verified test.

This runs the WHOLE buffer-pool test file against each mutation rather than one test chosen
in advance. The first version of this script asked one named test per mutation and reported
"M1 NOT CAUGHT" -- which was true of that test and false of the suite, because the mutation
is caught by a different test than the one I had guessed. Guessing which test covers which
guard is the thing being measured; it is not an input.

Every mutation is applied to a pristine copy read from git, so a crash between mutations
cannot leave a broken guard in the tree, and the restore is verified byte-for-byte.
"""
import os
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(os.environ.get("S22_ROOT", "/Users/idide/wt/ferrodb-S22-verify"))
SRC = ROOT / "src/buffer/buffer_pool.rs"
TEST_TARGET = "integration_buffer_pool_concurrency"
ROUNDS = 3

# Anchors are exact text from src/buffer/buffer_pool.rs. Each must match exactly once, or the
# script refuses: a mutation that silently applied to nothing would report "no test caught it"
# while having changed no code at all.
MUTATIONS = [
    ("M1", "try_pin_resident: trust the page table, drop the frame-label re-check before pinning", [
        ("""        let frame = self.frames[frame_i].read().unwrap();
        if frame.page_id != Some(page_id) {
            return None;
        }
        frame.pin_counter.fetch_add(1, Ordering::Relaxed);""",
         """        let frame = self.frames[frame_i].read().unwrap();
        frame.pin_counter.fetch_add(1, Ordering::Relaxed);"""),
    ]),
    ("M2b", "evict_into: drop BOTH frame-latch pin checks, keep the policy's is_pinned filter", [
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
    ("M2c", "M2b PLUS is_pinned always false: the policy stops skipping pinned pages too", [
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
    ("M3", "fetch_page: never wait on in_transit, so two threads fault the same page at once", [
        ("""            if transit.contains(&page_id) {""",
         """            if false {"""),
    ]),
    ("M4", "fault_in: publish the page-table mapping BEFORE the bytes land in the frame", [
        ("""        self.frames[frame_i].write().unwrap().data = data;
        self.page_table.write().unwrap().insert(page_id, frame_i);""",
         """        self.page_table.write().unwrap().insert(page_id, frame_i);
        self.frames[frame_i].write().unwrap().data = data;"""),
    ]),
]

TEST_LINE = re.compile(r"^test (\S+) \.\.\. (ok|FAILED)", re.M)


def run(cmd):
    return subprocess.run(cmd, shell=True, cwd=ROOT, capture_output=True, text=True)


def main():
    r = run(f"git show HEAD:src/buffer/buffer_pool.rs")
    if r.returncode != 0:
        sys.exit(f"cannot read pristine source: {r.stderr}")
    base = r.stdout
    if SRC.read_text() != base:
        sys.exit("REFUSING: src/buffer/buffer_pool.rs differs from HEAD before any mutation.")

    # The control. If the suite is not green on unmutated code, every row below is meaningless.
    ctl = run(f"timeout 600 cargo test --test {TEST_TARGET} 2>&1")
    ctl_out = ctl.stdout + ctl.stderr
    ctl_results = dict(TEST_LINE.findall(ctl_out))
    if not ctl_results:
        sys.exit(f"REFUSING: the control run collected no tests.\n{ctl_out[-2000:]}")
    if any(v == "FAILED" for v in ctl_results.values()):
        sys.exit(f"REFUSING: the control run is not green: {ctl_results}")
    print(f"control: {len(ctl_results)} tests, all green", flush=True)

    results = []
    try:
        for mid, desc, edits in MUTATIONS:
            text = base
            for find, repl in edits:
                n = text.count(find)
                if n != 1:
                    sys.exit(f"{mid}: anchor matched {n} times, expected 1:\n{find[:160]}")
                text = text.replace(find, repl)
            SRC.write_text(text)

            catchers = {}      # test name -> how many of ROUNDS it failed in
            compile_error = False
            for _ in range(ROUNDS):
                p = run(f"timeout 600 cargo test --test {TEST_TARGET} 2>&1")
                out = p.stdout + p.stderr
                if "error[" in out or "error: could not compile" in out:
                    compile_error = True
                    break
                found = TEST_LINE.findall(out)
                if not found:
                    catchers.setdefault("<no tests collected>", 0)
                    catchers["<no tests collected>"] += 1
                    continue
                for name, verdict in found:
                    if verdict == "FAILED":
                        catchers[name] = catchers.get(name, 0) + 1

            results.append((mid, desc, catchers, compile_error))
            if compile_error:
                print(f"{mid}: COMPILE ERROR (mutation did not build)", flush=True)
            elif catchers:
                summary = ", ".join(f"{k} {v}/{ROUNDS}" for k, v in sorted(catchers.items()))
                print(f"{mid}: CAUGHT by {summary}", flush=True)
            else:
                print(f"{mid}: *** NOT CAUGHT by any test in {TEST_TARGET} ***", flush=True)
    finally:
        SRC.write_text(base)
        if SRC.read_text() != base:
            sys.exit("RESTORE FAILED: src/buffer/buffer_pool.rs still differs from HEAD.")
        print("restored src/buffer/buffer_pool.rs to HEAD (verified byte-identical)", flush=True)

    lines = [
        "# S22 fire-check: every guard in buffer_pool.rs broken on purpose, and what caught it",
        "#",
        "# A guard that has never fired is not a verified guard. Each mutation is applied to a",
        f"# pristine copy of src/buffer/buffer_pool.rs, and the WHOLE {TEST_TARGET}",
        f"# file is run {ROUNDS} times against it. The row records which tests failed and how often.",
        "#",
        "# 'NOT CAUGHT' is a real result and is reported as one: it means that guard has no test.",
        f"# control: {len(ctl_results)} tests, all green on unmutated code.",
        "",
    ]
    for mid, desc, catchers, compile_error in results:
        if compile_error:
            verdict = "COMPILE-ERROR (no evidence either way)"
        elif not catchers:
            verdict = "NOT CAUGHT"
        elif any(v == ROUNDS for v in catchers.values()):
            verdict = "CAUGHT (deterministic)"
        else:
            verdict = "CAUGHT (flaky — race-dependent)"
        lines.append(f"{mid}\t{verdict}")
        lines.append(f"\tmutation: {desc}")
        for name, count in sorted(catchers.items()):
            lines.append(f"\tfailed {count}/{ROUNDS}: {name}")
        lines.append("")
    (ROOT / "bench/s22_firecheck.txt").write_text("\n".join(lines))
    print("wrote bench/s22_firecheck.txt", flush=True)


if __name__ == "__main__":
    main()
