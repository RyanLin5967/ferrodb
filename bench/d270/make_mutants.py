#!/usr/bin/env python3
"""Write D270's mutants as `git apply`-able patches.

The same shape as bench/d254/make_mutants.py. Each edit must match exactly once, or the script
refuses. It reads each file from a commit, never from the working tree, and writes only into
bench/d270/mutants/. Predictions are in bench/d270/PREREG.md.

    python3 bench/d270/make_mutants.py <sha>
"""
import difflib
import pathlib
import subprocess
import sys

CATALOG = "src/catalog/catalog.rs"

EARLY_WRITE_AFTER_WALK = (
    "        {\n"
    "            let mut early = images[0];\n"
    "            CatalogPage::stamp_links(&mut early, chain[0], chain.get(1).copied().unwrap_or(0));\n"
    "            self.write_catalog_page(chain[0], &early)?;\n"
    "        }\n"
)
EARLY_WRITE_BEFORE_WALK = (
    "        {\n"
    "            let mut early = images[0];\n"
    "            CatalogPage::stamp_links(&mut early, self.first_catalog_page_id, 0);\n"
    "            self.write_catalog_page(self.first_catalog_page_id, &early)?;\n"
    "        }\n"
)

MUTANTS = [
    # The old order for the allocation route: page 1 is written before the chain's pages are allocated.
    ("Q1_first_page_written_before_allocating", CATALOG, [(
        "        let mut fresh: Vec<u32> = Vec::new();\n",
        EARLY_WRITE_AFTER_WALK + "        let mut fresh: Vec<u32> = Vec::new();\n",
    )]),
    # The old order for the reading route: page 1 is written before the rest of the chain is read.
    ("Q2_first_page_written_before_the_walk", CATALOG, [(
        "        let mut chain: Vec<u32> = Vec::new();\n",
        EARLY_WRITE_BEFORE_WALK + "        let mut chain: Vec<u32> = Vec::new();\n",
    )]),
    # The layout no longer refuses an entry that fits no empty page.
    ("Q3_layout_size_refusal_removed", CATALOG, [(
        "                if !fresh.has_space(entry) {\n",
        "                if false && !fresh.has_space(entry) {\n",
    )]),
]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: make_mutants.py <sha>", file=sys.stderr)
        return 2
    out = pathlib.Path("bench/d270/mutants")
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
