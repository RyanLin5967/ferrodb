//! D35 C1 — the lock-free page-table mirror, and the invariants that make it safe.
//!
//! The resident hit loop is `fetch_page` + `unpin_page`, and between them it used to take
//! `page_table.read()` twice per iteration. A Rust `RwLock`'s reader count is one process-wide
//! cache line that every reader atomically RMWs, so those two acquisitions contended exactly like
//! a mutex. C1 resolves `page_id -> frame` through a lock-free, direct-mapped, tagged mirror
//! instead (`src/buffer/page_table.rs`), keeping the frame latch, the pin, and `touch`.
//!
//! The mirror is a **cache of the map, and the map is the authority**. It is allowed to be wrong
//! in exactly two ways: absent when the map has an entry (a slot collision), and naming a frame
//! the page has since left (rejected by the `frame.page_id` re-check under the frame's latch). It
//! may never name a *different live* frame, and these tests are what keep that true.
//!
//! Each test names the mutation it kills, in the style of `integration_buffer_pool_latch.rs`. The
//! mutations were applied and the tests were watched to fail — `bench/d35_c1_firecheck.txt`.
//!
//! What is NOT tested here, because it is not expressible: "a maintenance site was forgotten". The
//! map inside [`ferrodb::buffer::page_table::PageTable`] is private and the only way to mutate it
//! is a guard whose `insert`/`remove`/`clear` update both halves in one call. `HashMap`'s own
//! mutators need `&mut self`, which the guard's `Deref`-without-`DerefMut` does not hand out. A
//! write that skips the mirror does not compile, so there is no runtime behaviour to assert.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Barrier};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

/// The pool has 1024 frames; more pages than that forces real eviction rather than hoping for it.
const PAGES: u32 = 1600;

fn stamp(data: &mut [u8; PAGE_SIZE], page_id: u32) {
    data[0..4].copy_from_slice(&page_id.to_be_bytes());
}

fn read_stamp(data: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_be_bytes([data[0], data[1], data[2], data[3]])
}

/// A pool over a real file with `pages` stamped pages already on disk.
///
/// The pages are written through the disk manager directly rather than through `new_page`, because
/// one test needs several thousand of them and does not need any of them resident.
fn pool(tag: &str, pages: u32) -> (tempfile::TempDir, Arc<BufferPoolManager>, Vec<u32>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());

    let mut ids = Vec::with_capacity(pages as usize);
    for _ in 0..pages {
        let id = dm.allocate().expect("allocate");
        let mut data = [0u8; PAGE_SIZE];
        stamp(&mut data, id);
        dm.write(id, &data).expect("write");
        ids.push(id);
    }

    let bp = Arc::new(BufferPoolManager::new(dm));
    (dir, bp, ids)
}

/// Every mirror slot that is tagged must name exactly what the map names for that tag.
///
/// This is the invariant the whole design rests on: the mirror may be MISSING an entry the map
/// has, and never the other way round. Swept slot by slot rather than page by page, so a slot
/// tagged with a page that left the table long ago is caught.
fn assert_mirror_agrees_with_map(bp: &BufferPoolManager, context: &str) {
    let slots = bp.page_table.mirror_slots();
    let map = bp.page_table.read().unwrap();
    let mut tagged = 0usize;
    for slot in 0..slots {
        // For `slot < slots`, `slot_of(slot) == slot`, so this sweeps each slot exactly once.
        if let Some((tag, frame_i)) = bp.page_table.mirror_slot_raw(slot as u32) {
            tagged += 1;
            assert_eq!(
                map.get(&tag).copied(),
                Some(frame_i),
                "{context}: mirror slot {slot} is tagged page {tag} -> frame {frame_i}, which the \
                 page table does not agree with (it says {:?}). A stale mirror entry hands a \
                 reader a frame that holds somebody else's page.",
                map.get(&tag).copied()
            );
        }
    }
    assert!(
        tagged > 0,
        "{context}: not one mirror slot is tagged, so this check inspected nothing. Either the \
         mirror is never being written or the sweep is broken."
    );
}

/// **Kills: not clearing the mirror slot in `PageTableWriteGuard::remove`.**
///
/// An eviction storm unmaps and remaps pages thousands of times. A `remove` that left its slot
/// tagged would leave the mirror naming a frame that now holds a different page, and the next
/// `fetch_page` of the evicted id would resolve to it.
#[test]
fn the_mirror_agrees_with_the_page_table_after_an_eviction_storm() {
    let (_dir, bp, ids) = pool("storm", PAGES);

    for &id in &ids {
        let idx = bp.fetch_page(id).expect("fetch");
        assert_eq!(
            read_stamp(&bp.frames[idx].read().unwrap().data),
            id,
            "fetch_page({id}) returned a frame holding another page"
        );
        bp.unpin_page(id, false);
    }

    assert_mirror_agrees_with_map(&bp, "after a single sequential eviction storm");

    // Again in reverse, so pages are re-mapped into frames different from the ones they had.
    for &id in ids.iter().rev() {
        let idx = bp.fetch_page(id).expect("fetch");
        assert_eq!(read_stamp(&bp.frames[idx].read().unwrap().data), id);
        bp.unpin_page(id, false);
    }

    assert_mirror_agrees_with_map(&bp, "after a reversed eviction storm");
}

/// **Kills: not clearing the mirror slot on eviction, deterministically rather than by hammering.**
///
/// Built rather than hoped for: page `p` is made resident, then forced out by touching enough
/// other pages to fill the pool, then fetched again. If the mirror still named `p`'s old frame the
/// fetch would resolve there — and by then that frame holds one of the pages that evicted it.
#[test]
fn a_page_that_is_evicted_and_refetched_resolves_to_its_new_frame() {
    let (_dir, bp, ids) = pool("refetch", PAGES);
    let p = ids[0];

    let first_frame = bp.fetch_page(p).expect("fetch p");
    bp.unpin_page(p, false);
    assert_eq!(bp.page_table.lookup(p), Some(first_frame), "the mirror did not publish p");

    // Fill the pool with everything else, so p is chosen as a victim at some point.
    for &id in &ids[1..] {
        let idx = bp.fetch_page(id).expect("fetch filler");
        bp.unpin_page(id, false);
        let _ = idx;
    }
    assert!(
        !bp.page_table.read().unwrap().contains_key(&p),
        "precondition: p must have been evicted by the filler sweep"
    );
    assert_eq!(
        bp.page_table.lookup(p),
        None,
        "p was evicted but the mirror still names a frame for it"
    );

    let second_frame = bp.fetch_page(p).expect("refetch p");
    let f = bp.frames[second_frame].read().unwrap();
    assert_eq!(f.page_id, Some(p), "refetching p returned a frame labelled {:?}", f.page_id);
    assert_eq!(read_stamp(&f.data), p, "refetching p returned another page's bytes");
    drop(f);
    bp.unpin_page(p, false);
}

/// **Kills: dropping the tag check in `PageTable::lookup`, and dropping either fallback to the map.**
///
/// Two page ids that land in the same mirror slot. Only one can own it; the other must still be
/// served, through the map, by both `fetch_page` and `unpin_page`.
///
/// The `fetch_page` half is decisive rather than circumstantial. If `try_pin_resident` reported a
/// mirror miss as "not resident", `fetch_page` would fall to its fault path, find the page already
/// in the table under the transit lock, `continue`, and do that `FETCH_ATTEMPTS` times before
/// returning an error. An `Ok` here is proof the fallback ran.
#[test]
fn two_pages_sharing_a_mirror_slot_are_both_served_correctly() {
    // A pool must exist before its slot count is known, and the colliding id depends on it.
    let (_probe_dir, probe, _ids) = pool("slotprobe", 1);
    let slots = probe.page_table.mirror_slots() as u32;
    drop(probe);

    let (_dir, bp, ids) = pool("collide", slots + 1);
    let low = ids[0];
    let high = ids[slots as usize];
    assert_eq!(
        low % slots,
        high % slots,
        "precondition: pages {low} and {high} must share a mirror slot"
    );

    let low_frame = bp.fetch_page(low).expect("fetch low");
    bp.unpin_page(low, false);
    let high_frame = bp.fetch_page(high).expect("fetch high");
    bp.unpin_page(high, false);
    assert_ne!(low_frame, high_frame, "precondition: the two pages must be in different frames");

    // `high` was published second, so it owns the slot and `low` has no fast path.
    assert_eq!(bp.page_table.lookup(high), Some(high_frame));
    assert_eq!(
        bp.page_table.lookup(low),
        None,
        "a collision must report absent. Reporting page {high}'s frame for page {low} is the \
         failure the tag exists to prevent."
    );

    // Both must still be served correctly, through the map.
    for (id, expected_frame) in [(low, low_frame), (high, high_frame)] {
        let idx = bp.fetch_page(id).unwrap_or_else(|e| {
            panic!("fetch_page({id}) failed with {e:?}; the map fallback is missing")
        });
        assert_eq!(idx, expected_frame, "fetch_page({id}) moved a resident page");
        let f = bp.frames[idx].read().unwrap();
        assert_eq!(f.page_id, Some(id));
        assert_eq!(read_stamp(&f.data), id, "fetch_page({id}) returned another page's bytes");
        drop(f);
    }

    // Two pins outstanding on each (the warm-up fetch was unpinned; the loop above was not).
    assert_eq!(bp.frames[low_frame].read().unwrap().pin_counter.load(Ordering::Relaxed), 1);
    assert_eq!(bp.frames[high_frame].read().unwrap().pin_counter.load(Ordering::Relaxed), 1);

    // The unpin half of the same fallback: the collided-out page must reach ITS frame.
    bp.unpin_page(low, true);
    bp.unpin_page(high, false);
    assert_eq!(
        bp.frames[low_frame].read().unwrap().pin_counter.load(Ordering::Relaxed),
        0,
        "unpin_page({low}) did not reach its frame through the map fallback"
    );
    assert!(
        bp.frames[low_frame].read().unwrap().dirty_flag.load(Ordering::Relaxed),
        "unpin_page({low}, true) did not mark ITS frame dirty"
    );
    assert!(
        !bp.frames[high_frame].read().unwrap().dirty_flag.load(Ordering::Relaxed),
        "unpin_page({low}, true) marked page {high}'s frame dirty - it resolved through the \
         mirror slot {low} does not own"
    );
}

/// **Kills: dropping the `frame.page_id` re-check in `unpin_page`, on EITHER resolution path.**
///
/// The structural argument says a pinned page cannot be evicted, so `unpin_page`'s candidate can
/// never be stale. That argument rests on five separate call sites staying correct. This builds
/// the state those sites are supposed to prevent — a candidate frame that now holds another page —
/// and checks that `unpin_page` does not act on it.
///
/// Acting on it would decrement another page's pin count and set another page's dirty flag, and a
/// dirty flag on the wrong frame means the page that IS dirty is written under somebody else's id.
///
/// The fixture leaves the mirror and the map naming the same frame, so this kills the check on
/// both paths at once — which is the point of the two paths having one contract. Same genre as
/// `a_stale_page_table_entry_never_yields_another_pages_frame` in
/// `integration_buffer_pool_latch.rs`: the state is built rather than raced for, because the real
/// window is nanoseconds wide and hammering never lands in it.
#[test]
fn unpin_never_touches_a_frame_that_does_not_hold_the_page() {
    let (_dir, bp, ids) = pool("unpinstale", 8);
    let p = ids[0];
    let q = ids[1];

    for &id in &[p, q] {
        bp.fetch_page(id).expect("fetch");
        bp.unpin_page(id, false);
    }
    let p_frame = bp.page_table.lookup(p).expect("p mirrored");
    let q_frame = bp.page_table.lookup(q).expect("q mirrored");
    assert_ne!(p_frame, q_frame, "precondition: different frames");

    // Pin q twice, then hand p's frame to q behind the pool's back — the state that exists for a
    // few nanoseconds inside every eviction, with the mirror still naming p's old frame.
    bp.fetch_page(q).expect("pin q");
    bp.fetch_page(q).expect("pin q again");
    {
        let mut f = bp.frames[p_frame].write().unwrap();
        f.page_id = Some(q);
        stamp(&mut f.data, q);
        f.pin_counter = AtomicU16::new(2);
    }

    // `unpin_page(p)` now resolves to a frame labelled q, through the mirror and then through the
    // map, which name the same frame. Both must refuse it. Doing nothing is the correct answer:
    // there is no frame holding p to act on.
    bp.unpin_page(p, true);

    let f = bp.frames[p_frame].read().unwrap();
    assert_eq!(
        f.page_id,
        Some(q),
        "the fixture was not set up: this frame should be labelled q"
    );
    assert!(
        !f.dirty_flag.load(Ordering::Relaxed),
        "unpin_page({p}, true) marked a frame holding page {q} dirty. It acted on a mirror entry \
         whose frame had changed hands, which is exactly what the frame.page_id re-check is for."
    );
    assert_eq!(
        f.pin_counter.load(Ordering::Relaxed),
        2,
        "unpin_page({p}) decremented page {q}'s pin count"
    );
}

/// **Kills: missing mirror maintenance in `delete_page` and `free_page`.**
///
/// Both drop a page from the table and zero its frame. A mirror entry that outlived either would
/// name a free frame, and the next fetch of that id would find it unlabelled — or, once the frame
/// is reused, labelled with somebody else's page.
#[test]
fn delete_and_free_take_the_mirror_entry_with_them() {
    let (_dir, bp, ids) = pool("deletefree", 8);
    let to_delete = ids[0];
    let to_free = ids[1];

    for &id in &[to_delete, to_free] {
        bp.fetch_page(id).expect("fetch");
        bp.unpin_page(id, false);
        assert!(bp.page_table.lookup(id).is_some(), "precondition: {id} must be mirrored");
    }

    bp.delete_page(to_delete).expect("delete");
    assert_eq!(bp.page_table.lookup(to_delete), None, "delete_page left a mirror entry behind");
    assert!(!bp.page_table.read().unwrap().contains_key(&to_delete));

    bp.free_page(to_free).expect("free");
    assert_eq!(bp.page_table.lookup(to_free), None, "free_page left a mirror entry behind");
    assert!(!bp.page_table.read().unwrap().contains_key(&to_free));
}

/// **Kills: missing mirror maintenance in `PageTableWriteGuard::clear`, used by `invalidate_all`.**
///
/// `invalidate_all` exists because a snapshot install replaces the page file underneath the pool,
/// so every frame describes a database that no longer exists. A surviving mirror entry would hand
/// a reader a frame of the OLD database, and every such page still passes its checksum.
#[test]
fn invalidate_all_empties_the_mirror() {
    let (_dir, bp, ids) = pool("invalidate", 8);
    for &id in &ids {
        bp.fetch_page(id).expect("fetch");
        bp.unpin_page(id, false);
    }
    assert!(
        ids.iter().any(|&id| bp.page_table.lookup(id).is_some()),
        "precondition: something must be mirrored before the sweep"
    );

    bp.invalidate_all().expect("invalidate");

    assert!(bp.page_table.read().unwrap().is_empty(), "the map survived invalidate_all");
    for &id in &ids {
        assert_eq!(
            bp.page_table.lookup(id),
            None,
            "invalidate_all left page {id} in the mirror; a reader would get a frame of the \
             database that was just replaced"
        );
    }
    for slot in 0..bp.page_table.mirror_slots() {
        assert_eq!(
            bp.page_table.mirror_slot_raw(slot as u32),
            None,
            "invalidate_all left mirror slot {slot} tagged"
        );
    }
}

/// **Kills: a stale mirror entry handing out another page's frame, under real concurrency.**
///
/// Every page is stamped with its own id, so "did this fetch return the right page" is decidable
/// from the bytes alone rather than from anything the pool reports about itself. The working set
/// is larger than the pool, so eviction runs throughout and the mirror is being written by every
/// thread while every thread reads it.
#[test]
fn concurrent_fetches_never_resolve_to_the_wrong_frame_through_the_mirror() {
    let (_dir, bp, ids) = pool("concurrent", PAGES);
    let threads = 8;
    let per_thread = 4000;
    let barrier = Arc::new(Barrier::new(threads));

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let mut wrong: Vec<(u32, u32)> = Vec::new();
                // A per-thread stride so the threads do not walk in lockstep.
                let stride = (t * 7 + 1) as usize;
                let mut at = t * 97;
                for _ in 0..per_thread {
                    at = (at + stride) % ids.len();
                    let id = ids[at];
                    let Ok(idx) = bp.fetch_page(id) else { continue };
                    let got = {
                        let f = bp.frames[idx].read().unwrap();
                        (f.page_id.unwrap_or(u32::MAX), read_stamp(&f.data))
                    };
                    if got.0 != id || got.1 != id {
                        wrong.push((id, got.1));
                    }
                    bp.unpin_page(id, false);
                }
                wrong
            })
        })
        .collect();

    let mut wrong: Vec<(u32, u32)> = Vec::new();
    for h in handles {
        wrong.extend(h.join().expect("thread panicked"));
    }
    assert!(
        wrong.is_empty(),
        "{} fetches returned the wrong page. First few (asked, got): {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(8)]
    );

    // Every pin taken above was released, so nothing may still be pinned.
    let pinned: Vec<usize> = (0..bp.frames.len())
        .filter(|&i| bp.frames[i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0)
        .collect();
    assert!(
        pinned.is_empty(),
        "frames {pinned:?} are still pinned after every fetch was unpinned - unpin_page resolved \
         through the mirror to the wrong frame, so some frame was never released"
    );

    assert_mirror_agrees_with_map(&bp, "after 8 threads x 4000 fetches");

    // And the table itself is still one-to-one, which a mirror that handed out a frame twice
    // would have broken through the eviction path.
    let frames: Vec<usize> = bp.page_table.read().unwrap().values().copied().collect();
    let distinct: BTreeSet<usize> = frames.iter().copied().collect();
    assert_eq!(frames.len(), distinct.len(), "two pages map to one frame");
}
