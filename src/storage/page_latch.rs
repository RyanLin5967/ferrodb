//! Per-page reader/writer latches, sitting **above** the buffer pool in the lock order.
//!
//! # Why this exists rather than reusing `buffer_pool.frames[i]`
//!
//! The obvious move for latch crabbing is to hold the frame's `RwLock` while descending. It
//! deadlocks, and the inversion is not subtle:
//!
//! - `BufferPoolManager::fetch_page` takes `arc_cache` **first** and holds it across the whole
//!   call — deliberately, see its comment — and takes frame locks underneath it (the pin bump on
//!   the hit path, the victim's lock on the evict path, and `frames[..].read()` inside the
//!   cache's `is_pinned` probe, which can land on *any* resident page).
//! - Crabbing needs to fetch the **child** while still holding the **parent**, which would take
//!   `frame(parent) → arc_cache`.
//!
//! So thread A holding `frame(P).write()` blocks on `arc_cache`, while thread B holding
//! `arc_cache` blocks on `frame(P).read()`. A latch layer above the pool removes the cycle: the
//! order is `page latch → arc_cache → page_table → frame`, and nothing under `arc_cache` ever
//! reaches for a page latch.
//!
//! # Why hand-rolled rather than `RwLock` per page
//!
//! A `HashMap<u32, RwLock<()>>` cannot hand out a guard that outlives the map lookup without
//! either a self-referential struct or `unsafe`, and this crate has no dependencies
//! (`Cargo.toml` lists none), so `parking_lot`'s owned `ArcRwLockReadGuard` is not available.
//! A counter under one `Mutex` + `Condvar` is a few lines, is obviously correct, and drops the
//! entry when the page goes idle so the table does not grow without bound.
//!
//! Striping a fixed array of `RwLock`s by `page_id % N` was considered and is **wrong here**:
//! crabbing latches a parent and then its child, and two pages that collide on a stripe would
//! make a thread block on a latch it already holds. Distinct pages must get distinct latches.
//!
//! # Fairness
//!
//! Writer-preferred: a reader waits while a writer is queued. Without it a steady stream of point
//! lookups starves the writer that is trying to split the leaf they are all reading.
//!
//! # The ordering discipline callers must keep
//!
//! Latches are acquired **down** the tree (parent before child) and **rightward** along the leaf
//! chain (a leaf before its `next`). Never upward, never leftward. The wait-for graph is then
//! ordered by depth and then by key, so it has no cycle. `src/storage/index.rs` is the only
//! caller and states where each latch is taken.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Condvar, Mutex};

#[derive(Default)]
struct Latch {
    readers: u32,
    writer: bool,
    /// Writers blocked on this page. Readers defer to them; see "Fairness" above.
    writers_waiting: u32,
}

impl Latch {
    fn idle(&self) -> bool {
        self.readers == 0 && !self.writer && self.writers_waiting == 0
    }
}

/// One latch per page id, created on demand and dropped when the page goes idle.
#[derive(Default)]
pub struct PageLatches {
    table: Mutex<HashMap<u32, Latch>>,
    wake: Condvar,
}

impl PageLatches {
    pub fn new() -> Self {
        PageLatches::default()
    }

    /// Shared access to `page_id`. Blocks while a writer holds or is waiting for it.
    pub fn read(&self, page_id: u32) -> PageReadGuard<'_> {
        let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            let l = table.entry(page_id).or_default();
            if !l.writer && l.writers_waiting == 0 {
                l.readers += 1;
                return PageReadGuard { latches: self, page_id };
            }
            table = self.wake.wait(table).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Exclusive access to `page_id`. Blocks until no reader and no writer holds it.
    pub fn write(&self, page_id: u32) -> PageWriteGuard<'_> {
        let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        table.entry(page_id).or_default().writers_waiting += 1;
        loop {
            let l = table.entry(page_id).or_default();
            if !l.writer && l.readers == 0 {
                l.writers_waiting -= 1;
                l.writer = true;
                return PageWriteGuard { latches: self, page_id };
            }
            table = self.wake.wait(table).unwrap_or_else(|p| p.into_inner());
        }
    }

    fn release_read(&self, page_id: u32) {
        let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        if let Entry::Occupied(mut e) = table.entry(page_id) {
            e.get_mut().readers -= 1;
            if e.get().idle() {
                e.remove();
            }
        }
        self.wake.notify_all();
    }

    fn release_write(&self, page_id: u32) {
        let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        if let Entry::Occupied(mut e) = table.entry(page_id) {
            e.get_mut().writer = false;
            if e.get().idle() {
                e.remove();
            }
        }
        self.wake.notify_all();
    }

    /// Latches currently held or queued. Test-only introspection: a non-zero value after an
    /// operation returns means a guard leaked.
    pub fn outstanding(&self) -> usize {
        self.table.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
}

#[must_use = "a page latch is released when its guard is dropped"]
pub struct PageReadGuard<'a> {
    latches: &'a PageLatches,
    page_id: u32,
}

impl Drop for PageReadGuard<'_> {
    fn drop(&mut self) {
        self.latches.release_read(self.page_id);
    }
}

#[must_use = "a page latch is released when its guard is dropped"]
pub struct PageWriteGuard<'a> {
    latches: &'a PageLatches,
    page_id: u32,
}

impl PageWriteGuard<'_> {
    pub fn page_id(&self) -> u32 {
        self.page_id
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        self.latches.release_write(self.page_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn readers_share_and_a_writer_excludes() {
        let l = Arc::new(PageLatches::new());
        let a = l.read(7);
        let b = l.read(7); // two readers at once, or this would block forever
        drop(a);
            drop(b);

        let w = l.write(7);
        let blocked = Arc::new(AtomicUsize::new(0));
        let (l2, b2) = (Arc::clone(&l), Arc::clone(&blocked));
        let h = std::thread::spawn(move || {
            let _g = l2.read(7);
            b2.fetch_add(1, Ordering::SeqCst);
        });
        // The reader must not have got in while the write latch is held.
        std::thread::yield_now();
        assert_eq!(blocked.load(Ordering::SeqCst), 0, "a reader entered while a writer held the latch");
        drop(w);
        h.join().unwrap();
        assert_eq!(blocked.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn different_pages_do_not_block_each_other() {
        let l = PageLatches::new();
        let _a = l.write(1);
        let _b = l.write(2); // a stripe-by-hash design would deadlock here
        let _c = l.read(3);
    }

    #[test]
    fn the_table_empties_when_every_guard_is_dropped() {
        let l = PageLatches::new();
        {
            let _a = l.write(1);
            let _b = l.read(2);
            assert_eq!(l.outstanding(), 2);
        }
        assert_eq!(l.outstanding(), 0, "a latch entry outlived its guard");
    }

    /// Mutual exclusion is the whole point, so assert it rather than assuming it: a counter
    /// incremented non-atomically under the write latch must come out exact.
    #[test]
    fn the_write_latch_actually_serialises() {
        let l = Arc::new(PageLatches::new());
        let cell = Arc::new(Mutex::new(0u64)); // used only to hand a &mut across threads
        let mut hs = Vec::new();
        for _ in 0..8 {
            let l = Arc::clone(&l);
            let cell = Arc::clone(&cell);
            hs.push(std::thread::spawn(move || {
                for _ in 0..500 {
                    let _g = l.write(42);
                    // read-modify-write with a gap in the middle, exactly the shape the B+tree had
                    let v = *cell.lock().unwrap();
                    std::thread::yield_now();
                    *cell.lock().unwrap() = v + 1;
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(*cell.lock().unwrap(), 4000, "the write latch did not serialise");
    }
}
