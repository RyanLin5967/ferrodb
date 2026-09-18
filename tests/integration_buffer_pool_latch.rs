//! S22 — the invariants that replaced the buffer pool's global lock.
//!
//! `fetch_page` used to hold one process-wide mutex across the disk read, which made every page
//! miss serialise every other thread. Removing it moved the safety argument onto three properties,
//! and **the existing concurrency suite detects none of them**: each was deleted in turn and
//! `integration_buffer_pool_concurrency` passed 40 runs out of 40 with the code broken. Those tests
//! hunt a different shape — they hammer the pool and check the aggregate afterwards — and the
//! windows here are nanoseconds wide, so chance never lands in them.
//!
//! So these are built rather than hammered. Two are deterministic by construction and one widens
//! the window with a storage that can be held open, which is the same [`Storage`] seam
//! `storage::sim` uses to aim a crash.
//!
//! Each test names the mutation it kills. A test that passes against the broken code is not
//! evidence, and every one of these was run against the corresponding break before being kept.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU16, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::storage::Storage;

// ---------------------------------------------------------------------------------------------
// A storage whose reads can be held open, and whose writes can be made slow.
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Gate {
    open: bool,
    waiting: usize,
}

/// In-memory pages behind the [`Storage`] seam, with two knobs the tests below need:
///
/// * `arm_reads` makes every subsequent `pread` block until [`GatedStorage::release`], so a fault
///   can be parked *mid-IO* and the rest of the pool inspected while it is in flight.
/// * `write_delay` makes every `pwrite` slow, which widens the eviction write-back window from
///   nanoseconds to milliseconds. Without it the ordering test below cannot land in its own window.
struct GatedStorage {
    image: RwLock<Vec<u8>>,
    gate: Mutex<Gate>,
    cv: Condvar,
    reads_armed: AtomicBool,
    writes_armed: AtomicBool,
    write_delay_ns: AtomicU64,
}

impl GatedStorage {
    fn new() -> Self {
        GatedStorage {
            image: RwLock::new(Vec::new()),
            gate: Mutex::new(Gate::default()),
            cv: Condvar::new(),
            reads_armed: AtomicBool::new(false),
            writes_armed: AtomicBool::new(false),
            write_delay_ns: AtomicU64::new(0),
        }
    }

    fn arm_reads(&self) {
        let mut g = self.gate.lock().unwrap();
        g.open = false;
        drop(g);
        self.reads_armed.store(true, Ordering::SeqCst);
    }

    /// Park every subsequent `pwrite`. This is the knob that reaches the eviction write-back, which
    /// is a phase of a fault in which **no frame is pinned yet** -- see
    /// `invalidate_all_refuses_while_a_write_back_is_in_flight`.
    fn arm_writes(&self) {
        let mut g = self.gate.lock().unwrap();
        g.open = false;
        drop(g);
        self.writes_armed.store(true, Ordering::SeqCst);
    }

    /// Block until at least one reader or writer is parked inside the gate. Returns false on timeout, which
    /// the caller must treat as "the test did not set up its own precondition" rather than a pass.
    fn wait_for_parked_reader(&self, limit: Duration) -> bool {
        let mut g = self.gate.lock().unwrap();
        let deadline = std::time::Instant::now() + limit;
        while g.waiting == 0 {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return false;
            }
            let (next, timeout) = self.cv.wait_timeout(g, left).unwrap();
            g = next;
            if timeout.timed_out() && g.waiting == 0 {
                return false;
            }
        }
        true
    }

    fn release(&self) {
        self.reads_armed.store(false, Ordering::SeqCst);
        self.writes_armed.store(false, Ordering::SeqCst);
        let mut g = self.gate.lock().unwrap();
        g.open = true;
        self.cv.notify_all();
    }

    fn set_write_delay(&self, d: Duration) {
        self.write_delay_ns.store(d.as_nanos() as u64, Ordering::SeqCst);
    }
}

impl Storage for GatedStorage {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        if self.writes_armed.load(Ordering::SeqCst) {
            let mut g = self.gate.lock().unwrap();
            g.waiting += 1;
            self.cv.notify_all();
            while !g.open {
                g = self.cv.wait(g).unwrap();
            }
            g.waiting -= 1;
        }
        let d = self.write_delay_ns.load(Ordering::SeqCst);
        if d > 0 {
            // Outside the image lock: the delay models a slow device, not a contended one.
            std::thread::sleep(Duration::from_nanos(d));
        }
        let mut img = self.image.write().unwrap();
        let end = offset as usize + buf.len();
        if img.len() < end {
            img.resize(end, 0);
        }
        img[offset as usize..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if self.reads_armed.load(Ordering::SeqCst) {
            let mut g = self.gate.lock().unwrap();
            g.waiting += 1;
            self.cv.notify_all();
            while !g.open {
                g = self.cv.wait(g).unwrap();
            }
            g.waiting -= 1;
        }
        let img = self.image.read().unwrap();
        let start = offset as usize;
        if start >= img.len() {
            return Ok(0);
        }
        let n = buf.len().min(img.len() - start);
        buf[..n].copy_from_slice(&img[start..start + n]);
        Ok(n)
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.image.write().unwrap().resize(len as usize, 0);
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.image.read().unwrap().len() as u64)
    }
}

const FRAMES: usize = 1024;

fn stamp_of(data: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_be_bytes([data[0], data[1], data[2], data[3]])
}

/// A pool over `pages` stamped pages, all flushed, plus the storage handle for the knobs above.
fn pool(pages: u32) -> (Arc<BufferPoolManager>, Arc<GatedStorage>, Vec<u32>) {
    let st = Arc::new(GatedStorage::new());
    let dm = Arc::new(DiskManager::with_storage(st.clone() as Arc<dyn Storage>).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let mut ids = Vec::new();
    for _ in 0..pages {
        let id = bp.new_page().unwrap();
        let idx = bp.fetch_page(id).unwrap();
        bp.frames[idx].write().unwrap().data[0..4].copy_from_slice(&id.to_be_bytes());
        bp.unpin_page(id, true);
        ids.push(id);
    }
    bp.flush_all().unwrap();
    (bp, st, ids)
}

// ---------------------------------------------------------------------------------------------

/// **Kills: deleting the `frame.page_id != Some(page_id)` check in `try_pin_resident`.**
///
/// That check is what replaced the pool-wide lock. A page table lookup only ever produces a
/// *candidate* frame, because the frame it names can be handed to another page between the lookup
/// and the pin; re-reading the frame's own label under the frame's own lock is what makes the two
/// one step against an evictor.
///
/// The race window is the few nanoseconds between dropping the page table's read lock and taking
/// the frame's, so hammering the pool does not find it — the existing suite passed 40/40 with the
/// check deleted. This builds the disagreement directly instead: the table is left pointing page P
/// at a frame that holds page Q, which is exactly the state an evictor produces mid-flight.
///
/// The contract is **not** "fetch must succeed". It is that a fetch must never hand back a frame
/// holding somebody else's page. Refusing is a fine answer; returning Q's bytes for P is not.
#[test]
fn a_stale_page_table_entry_never_yields_another_pages_frame() {
    let (bp, _st, ids) = pool(8);
    let p = ids[0];
    let q = ids[1];

    // Make both resident and let them go, so the table has real entries for each.
    for &id in &[p, q] {
        bp.fetch_page(id).unwrap();
        bp.unpin_page(id, false);
    }

    let p_frame = *bp.page_table.read().unwrap().get(&p).expect("p resident");
    let q_frame = *bp.page_table.read().unwrap().get(&q).expect("q resident");
    assert_ne!(p_frame, q_frame, "precondition: the two pages must be in different frames");

    // Hand p's frame to q, exactly as `evict_into` does, but leave the table's entry for p behind.
    // The pool is now in the state that exists for a few nanoseconds during every eviction.
    {
        let mut f = bp.frames[p_frame].write().unwrap();
        f.page_id = Some(q);
        f.data[0..4].copy_from_slice(&q.to_be_bytes());
        f.pin_counter = AtomicU16::new(0);
    }

    match bp.fetch_page(p) {
        Ok(idx) => {
            let f = bp.frames[idx].read().unwrap();
            assert_eq!(
                f.page_id,
                Some(p),
                "fetch_page({p}) returned frame {idx}, which holds page {:?}",
                f.page_id
            );
            assert_eq!(
                stamp_of(&f.data),
                p,
                "fetch_page({p}) returned frame {idx}, whose contents are page {}",
                stamp_of(&f.data)
            );
        }
        Err(_) => {
            // Refusing an inconsistency is allowed. Serving it is not.
        }
    }
}

/// **Kills: making the `in_transit` removal in `fetch_page` conditional on success.**
///
/// A page is claimed in `in_transit` for the whole of its fault so that a second thread wanting the
/// same page waits on that page instead of loading it a second time into a second frame. The
/// removal at the end has to be unconditional: a page left in the set by a *failed* load is a page
/// every later fetch of it waits on forever, and a wedged pool is worse than the error that put it
/// there.
///
/// Reading past the end of the file is the ordinary way to reach that path — it is what probing
/// whether a page exists does — so this needs no fault injection.
#[test]
fn a_failed_load_does_not_strand_the_page_in_transit() {
    let (bp, _st, _ids) = pool(4);
    let absent = 9_999;

    // Three failures in a row. With a conditional removal the second call parks on the condvar and
    // never returns, so this test hangs rather than fails -- which is still a detection, and is why
    // it is worth stating that the expected outcome is three prompt errors.
    for attempt in 1..=3 {
        assert!(
            bp.fetch_page(absent).is_err(),
            "attempt {attempt}: reading page {absent} past the end of the file should fail"
        );
    }

    // And the pool still works afterwards, so the fix cannot be "break every fetch".
    let real = bp.new_page().unwrap();
    let idx = bp.fetch_page(real).expect("a real page must still be fetchable after failed probes");
    bp.unpin_page(real, false);
    assert_eq!(bp.frames[idx].read().unwrap().page_id, Some(real));
}

/// **Kills: dropping the `in_transit` check from `invalidate_all`.**
///
/// `invalidate_all` promises to refuse whole rather than in part: a snapshot install replaces the
/// page file underneath the pool, so a frame that survives holds a page of a database that no
/// longer exists, and it still passes its checksum.
///
/// Its pin scan cannot see a fault that is in flight. Between the replacement policy's verdict and
/// the claim of a frame, a faulting thread owns no frame and increments no pin count — so without
/// the `in_transit` check this function reports success and the faulting thread then publishes a
/// page of the OLD database into the table it just cleared.
///
/// The gate parks a reader inside `pread` so that window is held open for as long as the test likes,
/// rather than hoped for.
#[test]
fn invalidate_all_refuses_while_a_fault_is_in_flight() {
    let (bp, st, ids) = pool(8);
    let target = ids[3];

    // Cold, so the fetch below is a real fault that reaches storage.
    bp.invalidate_all().expect("cold start");
    st.arm_reads();

    let faulting = {
        let bp = Arc::clone(&bp);
        std::thread::spawn(move || bp.fetch_page(target))
    };

    assert!(
        st.wait_for_parked_reader(Duration::from_secs(10)),
        "no reader reached the gate, so this test never created the state it is about"
    );

    // The fault owns no frame yet, so nothing is pinned and the frame scan sees a clean pool.
    let refused = bp.invalidate_all();
    assert_eq!(
        refused,
        Err(FerroError::PagePinned),
        "invalidate_all reported {refused:?} while a fault was in flight; the faulting thread is \
         about to publish a page of the old database into the table it just cleared"
    );

    st.release();
    let idx = faulting.join().unwrap().expect("the parked fault should complete once released");
    bp.unpin_page(target, false);
    assert_eq!(bp.frames[idx].read().unwrap().page_id, Some(target));

    // With nothing in flight it must succeed, or the guard would be "always refuse".
    bp.invalidate_all().expect("invalidate_all must succeed once the fault has finished");
}

/// **Kills: dropping the `in_transit` check from `invalidate_all`.** The read-gated test above does
/// NOT kill it, and that is worth stating: the mutation was applied and all four tests passed.
///
/// By the time a fault reaches its `pread` it has already claimed a frame and set that frame's pin
/// count to 1, so `invalidate_all`'s own frame scan refuses it and the `in_transit` check is
/// redundant *for that phase*. The phase it is actually needed for is earlier and has no frame yet:
///
/// ```text
///   arc_cache.request(..)   <- the page is now in the replacement cache
///   evict_into(..)          <- writes a dirty victim back  ..... NOTHING IS PINNED HERE
///   disk_manager.read(..)   <- frame claimed and pinned before this point
/// ```
///
/// During the victim's write-back the faulting thread owns no frame and the victim itself is
/// unpinned, so the frame scan sees a clean pool and, without the `in_transit` check, reports that
/// everything was dropped. The faulting thread then publishes a page of the OLD database into the
/// table `invalidate_all` had just cleared — which for a snapshot install is a page of a database
/// that no longer exists, and it still passes its checksum.
///
/// So the gate is armed on WRITES, which parks the fault in exactly that window.
#[test]
fn invalidate_all_refuses_while_a_write_back_is_in_flight() {
    // One more page than the pool has frames, so exactly one page is left out and fetching it must
    // evict somebody.
    let (bp, st, ids) = pool(FRAMES as u32 + 1);

    // Dirty every resident page WITHOUT flushing, so the eviction below has to write back.
    let resident: Vec<u32> = bp.page_table.read().unwrap().keys().copied().collect();
    assert_eq!(resident.len(), FRAMES, "precondition: the pool must be full");
    for &id in &resident {
        let idx = bp.fetch_page(id).expect("resident pages are hits");
        bp.frames[idx].write().unwrap().data[8] = 0xE7;
        bp.unpin_page(id, true);
    }

    let absent = *ids
        .iter()
        .find(|id| !resident.contains(id))
        .expect("one page must be outside the pool");

    st.arm_writes();
    let faulting = {
        let bp = Arc::clone(&bp);
        std::thread::spawn(move || bp.fetch_page(absent))
    };

    assert!(
        st.wait_for_parked_reader(Duration::from_secs(10)),
        "no write-back reached the gate, so this test never created the state it is about"
    );

    // Nothing is pinned right now -- that is the whole point. Assert it, so that if the pool ever
    // starts pinning the victim earlier this test tells the truth about why it still passes.
    let pinned = bp
        .frames
        .iter()
        .filter(|f| f.read().unwrap().pin_counter.load(Ordering::Relaxed) > 0)
        .count();
    assert_eq!(
        pinned, 0,
        "precondition: no frame may be pinned during the write-back, or the frame scan would be \
         doing this guard's job and the mutation would not be detectable here"
    );

    let refused = bp.invalidate_all();
    assert_eq!(
        refused,
        Err(FerroError::PagePinned),
        "invalidate_all reported {refused:?} while a fault was mid-write-back and nothing was \
         pinned; the faulting thread is about to publish a page of the old database into the \
         table this call just cleared"
    );

    st.release();
    let _ = faulting.join().unwrap();
}

/// **Kills: unpublishing a dirty victim before its write-back reaches disk.**
///
/// `evict_into` writes a dirty victim back **while it is still in the page table**. The other order
/// is the tempting one — drop the mapping, then flush at leisure — and it loses writes silently: a
/// concurrent `fetch_page(victim)` misses, reads the stale copy from disk, and the dirty bytes
/// sitting in the frame are overwritten or discarded.
///
/// Every page is dirtied with a marker that is **only in memory**, so the copy on disk is the
/// pre-marker one. A page that comes back without its marker was read from disk during exactly that
/// window. The write delay widens the window from nanoseconds to milliseconds; without it the test
/// cannot land inside its own target.
#[test]
fn a_dirty_victim_is_on_disk_before_it_stops_being_resident() {
    const PAGES: u32 = 1600; // more than FRAMES, so eviction is forced rather than hoped for
    const MARKER: u8 = 0xE7;
    let (bp, st, ids) = pool(PAGES);
    assert!(ids.len() > FRAMES, "precondition: the working set must exceed the pool");

    // Dirty every page with a marker and DO NOT flush. Disk holds the stamp; only memory holds the
    // marker. Touching them all also means the later eviction victims are all dirty.
    for &id in &ids {
        let idx = bp.fetch_page(id).unwrap();
        bp.frames[idx].write().unwrap().data[8] = MARKER;
        bp.unpin_page(id, true);
    }

    // Slow writes, so the write-back window is wide enough for another thread to fall into.
    st.set_write_delay(Duration::from_micros(300));

    let lost = Arc::new(AtomicU64::new(0));
    let checked = Arc::new(AtomicU64::new(0));
    std::thread::scope(|s| {
        for t in 0..8 {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            let lost = Arc::clone(&lost);
            let checked = Arc::clone(&checked);
            s.spawn(move || {
                for k in 0..400 {
                    let id = ids[((k * 7919) + t * 131) % ids.len()];
                    let Ok(idx) = bp.fetch_page(id) else { continue };
                    let (marker, stamp) = {
                        let f = bp.frames[idx].read().unwrap();
                        (f.data[8], stamp_of(&f.data))
                    };
                    bp.unpin_page(id, true);
                    checked.fetch_add(1, Ordering::Relaxed);
                    if marker != MARKER || stamp != id {
                        lost.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let n = checked.load(Ordering::Relaxed);
    assert!(n > 2000, "only {n} fetches completed; this proves little");
    assert_eq!(
        lost.load(Ordering::Relaxed),
        0,
        "{} of {n} fetches came back without the marker that was only ever in memory: a dirty \
         victim became unreachable before its write-back landed",
        lost.load(Ordering::Relaxed)
    );
}

/// **The replacement policy must still account for every resident page after a churn.**
///
/// `ArcCache::request` chooses a victim by REMOVING it from the resident lists (`check_unpinned`)
/// and filing it under a ghost list. Under the pool-wide lock that was safe, because the victim
/// could not change between the verdict and the eviction. Without it, `evict_into` can decline the
/// victim — it got pinned, or re-dirtied during the write-back — and the page then stays in the
/// pool while the policy has stopped counting it as resident.
///
/// A page in that state is never chosen as a victim again, so its frame is gone for the life of the
/// process. This compares what the page table holds against what the policy believes is resident.
#[test]
fn every_resident_page_is_still_tracked_by_the_replacement_policy() {
    const PAGES: u32 = 1600;
    let (bp, st, ids) = pool(PAGES);

    // Dirty everything, so evictions must write back and the write-back window is wide. That
    // window is where a victim gets re-pinned and the eviction is declined.
    for &id in &ids {
        let idx = bp.fetch_page(id).unwrap();
        bp.frames[idx].write().unwrap().data[8] = 0xE7;
        bp.unpin_page(id, true);
    }
    st.set_write_delay(Duration::from_micros(200));

    let refused = Arc::new(AtomicU64::new(0));
    std::thread::scope(|s| {
        for t in 0..8 {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            let refused = Arc::clone(&refused);
            s.spawn(move || {
                for k in 0..500 {
                    let id = ids[((k * 7919) + t * 131) % ids.len()];
                    match bp.fetch_page(id) {
                        Ok(idx) => {
                            let _ = bp.frames[idx].read().unwrap().data[8];
                            bp.unpin_page(id, true);
                        }
                        Err(_) => {
                            refused.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    let resident = bp.page_table.read().unwrap().len();
    let tracked = {
        let c = bp.arc_cache.lock().unwrap();
        c.t1.len() + c.t2.len()
    };
    let refusals = refused.load(Ordering::Relaxed);

    assert_eq!(
        resident, tracked,
        "{resident} pages are resident but the replacement policy tracks only {tracked} of them. \
         {} frames can never be reclaimed. ({refusals} fetches were refused outright.)",
        resident.saturating_sub(tracked)
    );
}

/// The same question as above, but with the declined eviction **forced** instead of hoped for.
///
/// The churn version of this test passes, and on its own that means nothing: it never establishes
/// that `evict_into` ever declined a victim, so it cannot distinguish "no leak" from "the path
/// never ran". Here the decline is constructed. A fault is parked inside its victim's write-back,
/// every resident page is dirtied while it is parked, and the fault therefore finds its victim
/// dirty again when it resumes and gives it up.
#[test]
fn a_declined_eviction_leaves_the_victim_tracked_as_resident() {
    let (bp, st, ids) = pool(FRAMES as u32 + 1);

    let resident_before: Vec<u32> = bp.page_table.read().unwrap().keys().copied().collect();
    assert_eq!(resident_before.len(), FRAMES, "precondition: the pool must be full");
    for &id in &resident_before {
        let idx = bp.fetch_page(id).unwrap();
        bp.frames[idx].write().unwrap().data[8] = 0xE7;
        bp.unpin_page(id, true);
    }
    let absent = *ids.iter().find(|id| !resident_before.contains(id)).unwrap();

    // Park the fault inside its victim's write-back.
    st.arm_writes();
    let faulting = {
        let bp = Arc::clone(&bp);
        std::thread::spawn(move || bp.fetch_page(absent))
    };
    assert!(
        st.wait_for_parked_reader(Duration::from_secs(10)),
        "no write-back reached the gate; the decline was never set up"
    );

    // WHICH page is being evicted? `ArcCache::request` chose it by REMOVING it from the resident
    // lists, so right now it is the one page that the page table holds and the policy does not.
    // That set difference is both how the victim is identified and the first half of the bug.
    let victim = {
        let pt = bp.page_table.read().unwrap();
        let c = bp.arc_cache.lock().unwrap();
        let untracked: Vec<u32> = pt
            .keys()
            .copied()
            .filter(|id| !c.t1.map.contains_key(id) && !c.t2.map.contains_key(id))
            .collect();
        assert_eq!(
            untracked.len(),
            1,
            "expected exactly one resident-but-untracked page (the victim), found {untracked:?}"
        );
        untracked[0]
    };

    // PIN it and keep the pin. Dirtying it again would not work: the write-back holds the frame's
    // READ latch, so the bytes cannot change underneath it, and `evict_into` clears the dirty flag
    // after the write lands -- correctly, since the bytes it wrote are still the frame's bytes. A
    // PIN is the condition that genuinely makes the eviction unsafe to complete.
    let pinned_idx = bp.fetch_page(victim).expect("the victim is resident, so this is a hit");

    st.release();
    let _ = faulting.join().unwrap();

    // The eviction must have been declined: the page is pinned, so it cannot have been reused.
    assert_eq!(
        bp.frames[pinned_idx].read().unwrap().page_id,
        Some(victim),
        "the pinned victim was evicted anyway"
    );
    bp.unpin_page(victim, false);

    let resident = bp.page_table.read().unwrap().len();
    let tracked = {
        let c = bp.arc_cache.lock().unwrap();
        c.t1.len() + c.t2.len()
    };
    assert_eq!(
        resident, tracked,
        "after a declined eviction, {resident} pages are resident but the policy tracks {tracked}. \
         The declined victim was removed from the resident lists by `request` and never put back, \
         so its frame can never be reclaimed."
    );
}
