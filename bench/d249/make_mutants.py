#!/usr/bin/env python3
"""Write D249's mutants as `git apply`-able patches.

The same shape as bench/d224/make_mutants.py. Each edit must match exactly once, or the script
refuses. It reads each file from a commit, never from the working tree, and writes only into
bench/d249/mutants/. Predictions are in bench/d249/PREREG.md.

    python3 bench/d249/make_mutants.py <sha>
"""
import difflib
import pathlib
import subprocess
import sys

ALTER = "src/catalog/alter.rs"
PAGE = "src/catalog/catalog_page.rs"
CALL = "        refuse_unless_encodable(&installed)?;\n"

MUTANTS = [
    # Written as `if false { … }`, so the function stays referenced and the mutant builds under CI's
    # `-D dead_code` as well.
    ("E1_precheck_removed", ALTER, [(CALL, "        if false { refuse_unless_encodable(&installed)?; }\n")]),
    # The wrong fix: the same check, asked in `finish`, after the heap rewrite.
    ("E2_precheck_in_finish", ALTER, [
        (CALL, "        if false { refuse_unless_encodable(&installed)?; }\n"),
        (
            "        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;\n"
            "        entry.schema = new_schema;\n",
            "        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;\n"
            "        let mut installed = entry.clone();\n"
            "        installed.schema = new_schema.clone();\n"
            "        refuse_unless_encodable(&installed)?;\n"
            "        entry.schema = new_schema;\n",
        ),
    ]),
    ("E3_oversize_arm_removed", PAGE, [(
        "    if !page.has_space(entry) {\n",
        "    if false && !page.has_space(entry) {\n",
    )]),
    # PREREG amendment 3 (the D249 review). E4: the checked entry omits the index renames (F2).
    # E5: the staleness check in `apply_plan` never refuses (F7).
    ("E4_index_renames_not_checked", ALTER, [(
        "        rename_indexed_columns(&mut installed, actions);\n",
        "",
    )]),
    ("E5_staleness_check_removed", ALTER, [(
        "        if self.require_table(&table)? != &read {\n",
        "        if false && self.require_table(&table)? != &read {\n",
    )]),
]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: make_mutants.py <sha>", file=sys.stderr)
        return 2
    out = pathlib.Path("bench/d249/mutants")
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
