#!/usr/bin/env python3
"""D260 + D262 mutants. Pre-registration: artie-research frontier/lane_d260_d262_new_page.md
(§D260.2 and §D262.2).

    mutants.py list                 names, one per line
    mutants.py check                every anchor occurs exactly once in the SUBJECT blob, and
                                    applying the mutant changes the text; reads blobs only
    mutants.py apply NAME           refuses unless each file it touches equals the SUBJECT blob,
                                    then applies NAME's replacements (each exactly once)

Run from the worktree root. Restoring is the caller's job: `git checkout --no-overlay SUBJECT -- src/`.
"""
import subprocess
import sys

SUBJECT = "d2a2921"
B = "src/buffer/buffer_pool.rs"
H = "src/storage/heap_file_manager.rs"
T = "src/branch/table_catalog.rs"

# name -> [(file, old, new)]; each old text must occur exactly once in its file.
MUTANTS = {
    # D260: each extra unpin put back, one site at a time.
    "NA_heap_new_extra_unpin": [
        (H, "        let dir_page_id = buffer_pool_manager.new_page()?;\n",
         "        let dir_page_id = buffer_pool_manager.new_page()?;\n"
         "        buffer_pool_manager.unpin_page(dir_page_id, false);\n"),
    ],
    "NB_add_empty_page_extra_unpin": [
        (H, "        let new_page_id = self.buffer_pool_manager.new_page()?;\n",
         "        let new_page_id = self.buffer_pool_manager.new_page()?;\n"
         "        self.buffer_pool_manager.unpin_page(new_page_id, false);\n"),
    ],
    "NC_create_with_header_extra_unpin": [
        (T, "        let header_page = pool.new_page()?;\n",
         "        let header_page = pool.new_page()?;\n"
         "        pool.unpin_page(header_page, false);\n"),
    ],
    # D260: new_page returns the page pinned (its own unpin removed).
    "ND_new_page_returns_pinned": [
        (B, "        self.unpin_page(page_id, false);\n        Ok(page_id)\n",
         "        Ok(page_id)\n"),
    ],
    # D262: add_empty_page no longer gives the page back.
    "OA_add_empty_page_keeps_the_orphan": [
        (H, "        if let Err(e) = self.list_empty_page(new_page_id) {\n"
            "            let _ = self.buffer_pool_manager.free_page(new_page_id);\n",
         "        if let Err(e) = self.list_empty_page(new_page_id) {\n"),
    ],
    # D262: new_page no longer gives the page back.
    "OB_new_page_keeps_the_orphan": [
        (B, "            let _ = self.free_page(page_id);\n            return Err(e);\n",
         "            return Err(e);\n"),
    ],
    # D262: the give-back also fires after a SUCCESSFUL link.
    "OC_add_empty_page_frees_a_listed_page": [
        (H, "        Ok(new_page_id)\n    }\n\n    /// Lay an empty heap page",
         "        let _ = self.buffer_pool_manager.free_page(new_page_id);\n"
         "        Ok(new_page_id)\n    }\n\n    /// Lay an empty heap page"),
    ],
}


def blob(path):
    return subprocess.run(
        ["git", "show", f"{SUBJECT}:{path}"], check=True, capture_output=True, text=True
    ).stdout


def mutate(texts, name):
    out = dict(texts)
    for path, old, new in MUTANTS[name]:
        n = out[path].count(old)
        if n != 1:
            raise SystemExit(f"{name}: anchor occurs {n} times in {path}, not once: {old[:60]!r}")
        out[path] = out[path].replace(old, new)
    return out


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    cmd = sys.argv[1]
    if cmd == "list":
        print("\n".join(MUTANTS))
    elif cmd == "check":
        for name, reps in MUTANTS.items():
            paths = {p for p, _, _ in reps}
            before = {p: blob(p) for p in paths}
            after = mutate(before, name)
            unchanged = sorted(p for p in paths if after[p] == before[p])
            if unchanged:
                raise SystemExit(f"{name}: applying it left {unchanged} unchanged")
            print(f"{name}: ok ({', '.join(sorted(paths))})")
    elif cmd == "apply" and len(sys.argv) == 3:
        name = sys.argv[2]
        if name not in MUTANTS:
            raise SystemExit(f"unknown mutant {name}")
        paths = {p for p, _, _ in MUTANTS[name]}
        texts = {p: open(p).read() for p in paths}
        for p in paths:
            if texts[p] != blob(p):
                raise SystemExit(f"{name}: {p} on disk differs from {SUBJECT}; refusing")
        for p, t in mutate(texts, name).items():
            open(p, "w").write(t)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
