#!/usr/bin/env python3
"""D257 mutants. Pre-registration: artie-research frontier/lane_d257_insert_bound.md §3.

    mutants.py list                 names, one per line
    mutants.py check                every anchor occurs exactly once in the SUBJECT blob, and
                                    applying the mutant changes the text; reads blobs only
    mutants.py apply NAME           refuses unless each file it touches equals the SUBJECT blob,
                                    then applies NAME's replacements (each exactly once)

Run from the worktree root. Restoring is the caller's job: `git checkout SUBJECT -- <file>`.
"""
import subprocess
import sys

SUBJECT = "e11a3b1"
H = "src/storage/heap_file_manager.rs"
U = "src/execution/update.rs"

GUARD = "        if tuple_len > MAX_TUPLE_SIZE {\n"

# name -> [(file, old, new)]; each old text must occur exactly once in its file.
MUTANTS = {
    # 1.1's refusal disabled: the base's behaviour, with the checked conversion kept.
    "MA_no_refusal": [
        (H, GUARD, "        if false && tuple_len > MAX_TUPLE_SIZE {\n"),
    ],
    # Off by one: refuses the largest tuple a page holds.
    "MB_off_by_one": [
        (H, GUARD, "        if tuple_len >= MAX_TUPLE_SIZE {\n"),
    ],
    # The base's wrapping arithmetic, behind the refusal. Pre-registered EQUIVALENT (§1.5).
    "MC_wrapping_cast": [
        (
            H,
            "        u16::try_from(tuple_len + SLOT_ENTRY_SIZE).map_err(|_| {\n",
            "        Ok::<u16, ()>(tuple_len as u16 + SLOT_ENTRY_SIZE as u16).map_err(|_| {\n",
        ),
    ],
    # UPDATE's check before the time-travel write removed.
    "MD_no_update_precheck": [
        (U, "            HeapFileManager::space_needed(tuple.data.len())?;\n", ""),
    ],
    # insert_into back to the 9aa6968 shape: a bare frame index and a hand-written unpin.
    "ME_insert_into_hand_unpin": [
        (
            H,
            "        let pin = self.buffer_pool_manager.pin(page_id)?;\n"
            "        let mut frame = pin.write();\n"
            "        let mut page = Page::deserialize(frame.data)?;\n"
            "        let tuple_bytes = tuple.data.clone();\n",
            "        let frame_i = self.buffer_pool_manager.fetch_page(page_id)?;\n"
            "        let mut frame = self.buffer_pool_manager.frame_write(frame_i);\n"
            "        let mut page = Page::deserialize(frame.data)?;\n"
            "        let tuple_bytes = tuple.data.clone();\n",
        ),
        (
            H,
            "        pin.unpin(true);\n"
            "        self.update_directory_entry(page_id, page.get_free_space_end() - page.get_free_space_start())?;\n",
            "        self.buffer_pool_manager.unpin_page(page_id, true);\n"
            "        self.update_directory_entry(page_id, page.get_free_space_end() - page.get_free_space_start())?;\n",
        ),
    ],
    # The refusal is the old error again: nothing names the limit.
    "MF_old_error": [
        (H, GUARD, GUARD + "            return Err(FerroError::NotEnoughSpace);\n"),
    ],
}


def blob(path):
    return subprocess.run(
        ["git", "show", f"{SUBJECT}:{path}"], check=True, capture_output=True, text=True
    ).stdout


def mutate(texts, name):
    """Apply NAME to {path: text}; returns the new texts or raises with the anchor that failed."""
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
            changed = [p for p in paths if after[p] != before[p]]
            if sorted(changed) != sorted(paths):
                raise SystemExit(f"{name}: applying it left {sorted(paths - set(changed))} unchanged")
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
