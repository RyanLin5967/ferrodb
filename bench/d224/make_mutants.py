#!/usr/bin/env python3
"""Write D224's mutants of src/consensus/transport.rs as `git apply`-able patches.

The same shape as bench/d207/make_mutants.py. Each edit must match exactly once, or the script
refuses. It reads the file from a commit, never from the working tree, and writes only into
bench/d224/mutants/. Predictions are in bench/d224/PREREG.md.

    python3 bench/d224/make_mutants.py <sha>
"""
import difflib
import pathlib
import subprocess
import sys

PATH = "src/consensus/transport.rs"
GATE = "        if last_used.elapsed() >= probe_gap {\n"
REDIAL = "            if peer_has_closed(s) {\n"

MUTANTS = [
    ("N1_probe_removed", [(GATE, "        if false && last_used.elapsed() >= probe_gap {\n")]),
    ("N2_probe_every_frame", [(GATE, "        if true || last_used.elapsed() >= probe_gap {\n")]),
    ("N3_probe_result_ignored", [(REDIAL, "            if false && peer_has_closed(s) {\n")]),
    ("N4_frame_not_carried", [(
        "                carried = Some(frame);\n",
        "                drop(frame);\n",
    )]),
    ("N5_last_use_not_refreshed", [(
        "        } else {\n            last_used = Instant::now();\n        }\n",
        "        }\n",
    )]),
    # PREREG amendment 2 (the D224 review's F4): the code at 7d9567f, which read unread bytes as alive.
    ("N6_refusal_bytes_read_as_alive", [(
        "        Ok(_) => true,\n",
        "        Ok(_) => false,\n",
    )]),
]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: make_mutants.py <sha>", file=sys.stderr)
        return 2
    src = subprocess.run(
        ["git", "show", f"{sys.argv[1]}:{PATH}"], check=True, capture_output=True, text=True
    ).stdout
    out = pathlib.Path("bench/d224/mutants")
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
