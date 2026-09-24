#!/usr/bin/env python3
"""Write D207's mutants of src/consensus/transport.rs as `git apply`-able patches.

Each mutant re-introduces one defect (or removes one guard) by an exact-string edit that must match
exactly once, or the script refuses. Reads the file from a commit, never from the working tree, and
writes only into bench/d207/mutants/. Run from the worktree root:

    python3 bench/d207/make_mutants.py <sha>

Predictions for every mutant are in bench/d207/PREREG.md, amendment 1.
"""
import difflib
import pathlib
import subprocess
import sys

PATH = "src/consensus/transport.rs"

SETUP_EXIT = """                    counters.refused_conns.fetch_add(1, Ordering::SeqCst);
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }
                let _ = stream.set_nodelay(true);
"""

CAP_BLOCK = """    if count > MAX_CONFIG_NODES {
        return Err(FerroError::Wal(format!(
            "a configuration claims {count} {what}s, over the {MAX_CONFIG_NODES} limit. A frame is \\
             allowed to be large; the comparisons a configuration costs are quadratic in its two \\
             list lengths, so a large one is a request for this node's CPU rather than a cluster"
        )));
    }
"""

BUDGET_BLOCK = """    if count > *budget {
        return Err(FerroError::Wal(format!(
            "a configuration claims {count} {what}s, but only {} of this frame's \\
             {MAX_FRAME_CONFIG_NODES}-node budget is left. A configuration costs work quadratic in \\
             its own size, so bounding one configuration bounds nothing about a frame that carries \\
             a thousand of them",
            *budget
        )));
    }
"""

ENCODE_BUDGET = """        if list.len() > *budget {
            return Err(FerroError::Wal(format!(
                "a configuration holds {} nodes, but only {} of this frame's \\
                 {MAX_FRAME_CONFIG_NODES}-node budget is left; a peer would refuse this frame, so it \\
                 is refused here where the cause is visible. Send fewer Membership entries per \\
                 Append",
                list.len(),
                *budget
            )));
        }
        *budget -= list.len();
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
    # --- the per-frame config budget ------------------------------------------------------------
    ("M9_decode_budget_not_charged", [(
        "    *budget -= count;\n    // Not pre-allocated from `count`",
        "    // Not pre-allocated from `count`",
    )]),
    ("M10_budget_checked_before_cap", [(
        CAP_BLOCK,
        "",
    ), (
        BUDGET_BLOCK + "    *budget -= count;\n",
        BUDGET_BLOCK + CAP_BLOCK + "    *budget -= count;\n",
    )]),
    ("M11_learners_uncharged", [(
        '    let learners = decode_node_list(bytes, at, "learner", budget)?;\n',
        '    let learners = decode_node_list(bytes, at, "learner", &mut usize::MAX)?;\n',
    )]),
    ("M12_encoder_budget_removed", [(
        ENCODE_BUDGET,
        "        let _ = &budget;\n",
    )]),
    ("M13_fresh_budget_per_entry", [(
        "        entries.push(decode_entry(bytes, at, budget).map_err(|e| {\n",
        "        let _ = &budget;\n        entries.push(decode_entry(bytes, at, &mut { MAX_FRAME_CONFIG_NODES }).map_err(|e| {\n",
    )]),
    ("M14_spawn_failure_uncounted", [(
        "                        // for the same reason — it is a connection this node closed unserved.\n                        counters.refused_conns.fetch_add(1, Ordering::SeqCst);\n",
        "                        // for the same reason — it is a connection this node closed unserved.\n",
    )]),
]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: make_mutants.py <sha>", file=sys.stderr)
        return 2
    sha = sys.argv[1]
    src = subprocess.run(
        ["git", "show", f"{sha}:{PATH}"], check=True, capture_output=True, text=True
    ).stdout
    out = pathlib.Path("bench/d207/mutants")
    out.mkdir(parents=True, exist_ok=True)
    for name, edits in MUTANTS:
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
            fromfile=f"a/{PATH}",
            tofile=f"b/{PATH}",
            n=3,
        )
        (out / f"{name}.patch").write_text("".join(diff))
        print(f"{name}: written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
