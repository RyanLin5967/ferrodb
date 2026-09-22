#!/usr/bin/env python3
"""Arm 7: separate the two explanations for 'the probe recovered 2 of the 3 pages written'.

The D85 row says the missing page is one that never reached disk. The arithmetic suggests a
different cause: the restored `recycled` list still names a page that was REUSED after the
checkpoint, so `allocated_pages` (which subtracts `recycled`) hides it even when the probe
raised `next_free` correctly. These predict different values of `next_free` after the probe,
so printing it settles which is true. Adds only test-only accessors.
"""
import os

WT = "/Users/idide/wt/ferrodb-d85-rederive"
ARENA = os.path.join(WT, "src/branch/arena.rs")

ANCHOR_ACCESSOR = """    /// Test-only: how many arenas are still under fill suspicion."""

NEW_ACCESSORS = """    /// Test-only: the extent's recorded bump pointer, unfiltered by `recycled`.
    ///
    /// `allocated_pages` subtracts the recycled set, so it cannot distinguish "the probe did not
    /// raise `next_free` this far" from "it did, but the page is listed free". D85's report needs
    /// them apart.
    #[cfg(test)]
    pub fn debug_next_free(&self, arena: ArenaId) -> u32 {
        self.state.lock().unwrap().extents.get(&arena).map(|e| e.next_free).unwrap_or(0)
    }

    /// Test-only: how many of this extent's pages the store believes are free for reuse.
    #[cfg(test)]
    pub fn debug_recycled(&self, arena: ArenaId) -> Vec<PageId> {
        self.state.lock().unwrap().recycled.get(&arena).cloned().unwrap_or_default()
    }

"""

# The diagnostic test goes at the end of the arena test module, just before its closing brace.
DIAG_TEST = r'''
    /// **D85 arm 7 (diagnostic, not a guard).** WHY does the probe report fewer pages than were
    /// written after the checkpoint? Two candidate explanations predict different numbers:
    ///
    /// (a) the row's: the missing page never reached disk, so `read_page` fails and the probe
    ///     stops early. Predicts `next_free` after the probe < pages written after the checkpoint.
    /// (b) the arithmetic's: `alloc_in_arena` pops `recycled` BEFORE bumping, so a page drained
    ///     into `recycled` before the image and reused after it is recorded FREE in the image and
    ///     still listed free after restore. `allocated_pages` subtracts `recycled`, hiding it.
    ///     Predicts `next_free` reaches the truth but a live page sits in `recycled`.
    ///
    /// Printing `next_free` and `recycled` separates them. This asserts only what both agree on.
    #[test]
    fn d85_diag_why_the_probe_reports_fewer_pages_than_were_written() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
        let epoch = h.catalog.next_epoch();
        for _ in 0..6 {
            h.store.alloc_for(b.branch_id, PageType::BTreeLeaf, epoch).unwrap();
        }
        // Drive to a fresh extent, exactly as the end-to-end fixture does.
        let mut arena = *h.catalog.get(b.branch_id).unwrap().arenas.last().unwrap();
        for _ in 0..64 {
            h.store.alloc_for(b.branch_id, PageType::BTreeLeaf, epoch).unwrap();
            let now = *h.catalog.get(b.branch_id).unwrap().arenas.last().unwrap();
            if now != arena && h.store.allocated_pages(now).len() <= 1 {
                arena = now;
                break;
            }
            arena = now;
        }
        // Drain it so the image records it as empty -- this is what puts pages in `recycled`.
        let drained: Vec<PageId> = h.store.allocated_pages(arena);
        for p in drained.iter().copied() {
            h.store.release_page(p, arena);
        }
        let nf_at_image = h.store.debug_next_free(arena);
        let rec_at_image = h.store.debug_recycled(arena);
        let image = h.store.state_bytes();

        // Pages written AFTER the image. The first of these REUSES a drained id.
        let mut after: Vec<PageId> = Vec::new();
        for _ in 0..3 {
            after.push(h.store.alloc_for(b.branch_id, PageType::BTreeLeaf, epoch).unwrap());
        }
        h.store.flush().unwrap();
        let reused: Vec<PageId> =
            after.iter().copied().filter(|p| rec_at_image.contains(p)).collect();

        let re = h.fresh_store();
        re.load_state(&image).unwrap();
        let nf_restored = re.debug_next_free(arena);
        re.resolve_fill(arena);
        let nf_probed = re.debug_next_free(arena);
        let rec_after = re.debug_recycled(arena);
        let visible = re.allocated_pages(arena);

        println!(
            "D85 arm7: image next_free={nf_at_image} recycled={rec_at_image:?}\n\
             D85 arm7: wrote after the image {after:?} (of which REUSED a recycled id: {reused:?})\n\
             D85 arm7: restored next_free={nf_restored} -> after probe {nf_probed}\n\
             D85 arm7: recycled after restore={rec_after:?}  allocated_pages={visible:?}"
        );
        // Both explanations agree the probe may only raise, never lower.
        assert!(nf_probed >= nf_restored, "the probe lowered next_free");
        // And on this: a page holding live post-image data must not be BOTH listed free and
        // inside the probed range -- that is a page the store would hand out twice.
        for p in reused.iter().copied() {
            if rec_after.contains(&p) {
                println!(
                    "D85 arm7: ** page {p} holds data written after the image and is listed FREE \
                     after restore (next_free={nf_probed}). `fill_unknown` corrects next_free \
                     only; the restored `recycled` list carries the same staleness."
                );
            }
        }
    }
'''


def main():
    src = open(ARENA).read()
    assert src.count(ANCHOR_ACCESSOR) == 1, "accessor anchor is not unique"
    src = src.replace(ANCHOR_ACCESSOR, NEW_ACCESSORS + ANCHOR_ACCESSOR, 1)
    # The arena test module ends with the d85 test then "}\n\n}" -- append before the final brace.
    marker = "\n}\n"
    assert src.endswith(marker), f"unexpected file tail: {src[-40:]!r}"
    src = src[: -len(marker)] + DIAG_TEST + marker
    open(ARENA, "w").write(src)
    print("arm 7 patch applied")


if __name__ == "__main__":
    main()
