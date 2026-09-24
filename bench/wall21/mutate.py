#!/usr/bin/env python3
"""Apply one named mutant to the ferrodb tree in the current directory (lane §8.22). Exact-string
replacement; exits non-zero if the anchor is not found exactly once, so a mutant that no longer
applies cannot pass silently as a no-op."""
import sys

MUTANTS = {
    # M64: E1's page_birth mapping removed.
    "page_birth": ("src/branch/arena.rs",
        '                let birth = read(slot, "a page\'s birth epoch", self.page_birth(page_id))?;\n',
        '                let birth = self.page_birth(page_id)?;\n'),
    # M65: E1's slow-path window-read mapping removed.
    "window_read": ("src/branch/arena.rs",
        '                if !read(\n'
        '                    slot,\n'
        '                    "the live children in a page\'s epoch window",\n'
        '                    self.catalog.live_child_in_epoch_range(slot, birth, free_epoch),\n'
        '                )? {\n',
        '                if !self.catalog.live_child_in_epoch_range(slot, birth, free_epoch)? {\n'),
    # M69: the drain's window-read mapping removed.
    "drain_window": ("src/branch/reaper.rs",
        '                    Ok(_) => one_slot_read(\n'
        '                        owner,\n'
        '                        "the live children in a parked page\'s epoch window",\n'
        '                        self.catalog.live_child_in_epoch_range(owner, pf.birth_epoch, pf.free_epoch),\n'
        '                    ),\n',
        '                    Ok(_) => self.catalog.live_child_in_epoch_range(owner, pf.birth_epoch, pf.free_epoch),\n'),
    # M70: the drain's record-read mapping removed.
    "drain_record": ("src/branch/reaper.rs",
        '                let pinned = match one_slot_read(owner, "the record", self.catalog.get_raw(owner)) {\n',
        '                let pinned = match self.catalog.get_raw(owner) {\n'),
}

name = sys.argv[1]
path, old, new = MUTANTS[name]
text = open(path).read()
n = text.count(old)
if n != 1:
    sys.exit(f"mutant {name}: anchor found {n} times in {path}")
open(path, "w").write(text.replace(old, new))
print(f"mutant {name} applied to {path}")
