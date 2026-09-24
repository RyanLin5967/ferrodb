#!/usr/bin/env python3
"""Write D254's mutants as `git apply`-able patches.

The same shape as bench/d249/make_mutants.py. Each edit must match exactly once, or the script
refuses. It reads each file from a commit, never from the working tree, and writes only into
bench/d254/mutants/. Predictions are in bench/d254/PREREG.md.

    python3 bench/d254/make_mutants.py <sha>
"""
import difflib
import pathlib
import subprocess
import sys

CATALOG = "src/catalog/catalog.rs"

# Each is written as `if false { … }`, so `refuse_unless_encodable` stays referenced and the mutant
# builds under CI's `-D dead_code` as well.
MUTANTS = [
    ("P1_persist_prepass_removed", CATALOG, [(
        "            refuse_unless_encodable(entry)?;\n",
        "            if false { refuse_unless_encodable(entry)?; }\n",
    )]),
    ("P2_create_table_check_removed", CATALOG, [(
        "        refuse_unless_encodable(&entry)?;\n",
        "        if false { refuse_unless_encodable(&entry)?; }\n",
    )]),
    ("P3_create_index_check_removed", CATALOG, [(
        "            with_index.indexes.push(IndexInfo { column_name: column.to_string(), root_page_id: 0 });\n"
        "            refuse_unless_encodable(&with_index)?;\n",
        "            with_index.indexes.push(IndexInfo { column_name: column.to_string(), root_page_id: 0 });\n"
        "            if false { refuse_unless_encodable(&with_index)?; }\n",
    )]),
    ("P4_create_fulltext_check_removed", CATALOG, [(
        "            with_index.fulltext_indexes.push(FullTextIndexInfo { column_name: column.to_string(), root_page_id: 0 });\n"
        "            refuse_unless_encodable(&with_index)?;\n",
        "            with_index.fulltext_indexes.push(FullTextIndexInfo { column_name: column.to_string(), root_page_id: 0 });\n"
        "            if false { refuse_unless_encodable(&with_index)?; }\n",
    )]),
]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: make_mutants.py <sha>", file=sys.stderr)
        return 2
    out = pathlib.Path("bench/d254/mutants")
    out.mkdir(parents=True, exist_ok=True)
    for name, path, edits in MUTANTS:
        src = subprocess.run(
            ["git", "show", f"{sys.argv[1]}:{path}"], check=True, capture_output=True, text=True
        ).stdout
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
