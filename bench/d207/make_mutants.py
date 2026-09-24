#!/usr/bin/env python3
"""Write D207's and D220's mutants of consensus/transport.rs, config.rs and replicate.rs as patches.

Each mutant re-introduces one defect (or removes one guard) by an exact-string edit that must match
exactly once, or the script refuses. Reads the file from a commit, never from the working tree, and
writes only into bench/d207/mutants/. Run from the worktree root:

    python3 bench/d207/make_mutants.py <sha>

Predictions for every mutant are in bench/d207/PREREG.md; the current table is amendment 5's. M9-M13
mutated the per-frame budget, which amendment 4 removed, and are retired rather than renumbered.
"""
import difflib
import pathlib
import subprocess
import sys

PATH = "src/consensus/transport.rs"
CONFIG = "src/consensus/config.rs"
REPLICATE = "src/consensus/replicate.rs"

SETUP_EXIT = """                    counters.refused_conns.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }
                let _ = stream.set_nodelay(true);
"""

# (name, [(old, new), ...]) — every `old` must occur exactly once in the file.
MUTANTS = [
    # --- the leaked accept slot -------------------------------------------------------------
    ("M1_setup_exit_forgets_slot", [(
        SETUP_EXIT,
        SETUP_EXIT.replace(
            "                    continue;\n",
            "                    std::mem::forget(registration);\n                    continue;\n",
        ),
    )]),
    ("M2_setup_exit_uncounted", [(
        SETUP_EXIT,
        SETUP_EXIT.replace("                    counters.refused_conns.fetch_add(1, Ordering::SeqCst);\n", ""),
    )]),
    ("M3_slot_released_at_thread_start", [(
        "    // including the refused-handshake return.\n\n    let verdict = recv_handshake(",
        "    // including the refused-handshake return.\n    drop(_registration);\n\n    let verdict = recv_handshake(",
    )]),
    # --- the outbound byte bound --------------------------------------------------------------
    ("M4_byte_bound_removed", [(
        "                || st.bytes.saturating_add(frame.len()) > self.max_bytes)\n",
        "                || (false && st.bytes.saturating_add(frame.len()) > self.max_bytes))\n",
    )]),
    ("M5_drop_not_refunded", [(
        "            if let Some(old) = st.queue.pop_front() {\n                st.bytes -= old.len();\n",
        "            if let Some(old) = st.queue.pop_front() {\n                let _ = old.len();\n",
    )]),
    ("M6_pop_not_refunded", [(
        "            let taken = st.queue.pop_front();\n            if let Some(f) = taken.as_ref() {\n                st.bytes -= f.len();\n            }\n            taken\n",
        "            st.queue.pop_front()\n",
    )]),
    ("M7_oversized_frame_refused", [(
        "        // Both bounds, whichever binds first.",
        "        if frame.len() > self.max_bytes {\n            self.dropped.fetch_add(1, Ordering::SeqCst);\n            return;\n        }\n        // Both bounds, whichever binds first.",
    )]),
    ("M8_empty_guard_removed", [(
        "        while !st.queue.is_empty()\n            && (st.queue.len() >= self.depth\n",
        "        while (st.queue.len() >= self.depth\n",
    )]),
    ("M14_spawn_failure_uncounted", [(
        "                        // for the same reason — it is a connection this node closed unserved.\n                        counters.refused_conns.fetch_add(1, Ordering::SeqCst);\n",
        "                        // for the same reason — it is a connection this node closed unserved.\n",
    )]),
    # --- amendment 2: the lead-scoped rest of 207d362 --------------------------------------------
    ("M15_zero_idle_deadline_accepted", [(
        "        if opts.idle_deadline.is_zero() {\n",
        "        if false && opts.idle_deadline.is_zero() {\n",
    )]),
    ("M16_zero_cap_accepted", [(
        "        if opts.max_inbound_conns == 0 {\n",
        "        if false && opts.max_inbound_conns == 0 {\n",
    )]),
    ("M17_cap_full_unpaced", [(
        "                        // the poll the stop flag is already allowed (D207, from `207d362`).\n                        std::thread::sleep(opts.poll_interval);\n",
        "                        // the poll the stop flag is already allowed (D207, from `207d362`).\n",
    )]),
    ("M18_lost_wakeup", [(
        "        if st.stopped || stop.load(Ordering::SeqCst) {\n            return;\n        }\n        let _ = self.woken.wait_timeout(st, delay);\n",
        "        let _ = stop;\n        let _ = self.woken.wait_timeout(st, delay);\n",
    )]),
    # --- amendment 5: the review of dd9d1e1 -----------------------------------------------------
    ("M21_try_clone_uncounted", [(
        "                    // descriptor exhaustion these meters exist to show. Counted with the others.\n                    counters.refused_conns.fetch_add(1, Ordering::SeqCst);\n",
        "                    // descriptor exhaustion these meters exist to show. Counted with the others.\n",
    )]),
    # --- amendment 6: D220, the Append byte budget --------------------------------------------------
    ("M24_mac_not_reserved", [(
        "        .map_or(0, |b| MAX_FRAME_BYTES.saturating_sub(b.len() + signing::MAC_LEN))\n",
        "        .map_or(0, |b| MAX_FRAME_BYTES.saturating_sub(b.len()))\n",
    )]),
    ("M26_envelope_not_reserved", [(
        "        .map_or(0, |b| MAX_FRAME_BYTES.saturating_sub(b.len() + signing::MAC_LEN))\n",
        "        .map_or(0, |_b| MAX_FRAME_BYTES.saturating_sub(signing::MAC_LEN))\n",
    )]),
]

# Mutants of replicate.rs (D220), same shape.
REPLICATE_MUTANTS = [
    ("M23_append_count_only", [(
        "                super::transport::append_entries_budget(),\n",
        "                usize::MAX,\n",
    )]),
    ("M25_cut_one_early", [(
        "            if !taken.is_empty() && bytes.saturating_add(len) > max_bytes {\n",
        "            if !taken.is_empty() && bytes.saturating_add(len) >= max_bytes {\n",
    )]),
    ("M27_empty_when_first_too_big", [(
        "            if !taken.is_empty() && bytes.saturating_add(len) > max_bytes {\n",
        "            if bytes.saturating_add(len) > max_bytes {\n",
    )]),
]

# Mutants of config.rs, same shape.
CONFIG_MUTANTS = [
    ("M19_retain_scans", [(
        "    items.retain(|n| sorted.binary_search(n).is_err());\n",
        "    items.retain(|n| !sorted.contains(n));\n",
    )]),
    ("M20_with_learners_scans_directly", [(
        "        retain_absent(&mut l, &self.members);\n",
        "        l.retain(|n| !self.members.contains(n));\n",
    )]),
    ("M22_retain_inverted", [(
        "    items.retain(|n| sorted.binary_search(n).is_err());\n",
        "    items.retain(|n| sorted.binary_search(n).is_ok());\n",
    )]),
]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: make_mutants.py <sha>", file=sys.stderr)
        return 2
    sha = sys.argv[1]
    out = pathlib.Path("bench/d207/mutants")
    out.mkdir(parents=True, exist_ok=True)
    for path, mutants in ((PATH, MUTANTS), (CONFIG, CONFIG_MUTANTS), (REPLICATE, REPLICATE_MUTANTS)):
        rc = write(sha, path, mutants, out)
        if rc:
            return rc
    return 0


def write(sha: str, path: str, mutants, out: pathlib.Path) -> int:
    src = subprocess.run(
        ["git", "show", f"{sha}:{path}"], check=True, capture_output=True, text=True
    ).stdout
    for name, edits in mutants:
        text = src
        for old, new in edits:
            n = text.count(old)
            if n != 1:
                print(f"{name}: an edit matches {n} times, not once; refusing", file=sys.stderr)
                return 1
            text = text.replace(old, new)
        if text == src:
            print(f"{name}: the mutant is identical to the source; refusing", file=sys.stderr)
            return 1
        diff = difflib.unified_diff(
            src.splitlines(keepends=True),
            text.splitlines(keepends=True),
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
            n=3,
        )
        (out / f"{name}.patch").write_text("".join(diff))
        print(f"{name}: written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
