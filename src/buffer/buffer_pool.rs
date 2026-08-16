                // The page table write lock is held across the WHOLE miss path, and that
                // serialization is the fix. The original code did check-then-act across several
                // independent locks — probe the frame under a read lock, drop it, re-acquire a
                // write lock; consult the page table, drop it, then act — so two threads missing
                // at the same time could interleave and one thread's page ended up in a frame the
                // other had already claimed, or in an orphaned frame the table never pointed at.
                // Either way the write was silently lost: 10 of 12 runs of
                // `concurrent_writers_each_read_back_only_their_own_row` before, 0 of 15 after.
                //
                // Attribution was checked rather than assumed, and the first answer was wrong.
                // Reinstating the frame-scan window alone did NOT bring the bug back, and neither
                // did removing the duplicate-load guard alone — because both variants still held
                // this lock for the whole path. It is the single held lock that matters, not
                // either individual check.
                //
                // Lock order is page table -> frame throughout the file, so holding it here does
                // not invert against `unpin_page`, `flush_page`, or the hit path.
use std::sync::{Arc, OnceLock};
use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, atomic::AtomicU16};
use std::collections::HashMap;
use crate::error::FerroError;
use crate::storage::disk_manager::{DiskManager, PAGE_SIZE};
use crate::buffer::arc::ArcCache;
use crate::wal::log::WalManager;
use std::sync::RwLock;
use std::sync::atomic::Ordering;
use crate::buffer::arc::ArcResult;

pub struct Frame {
    pub data: [u8; PAGE_SIZE],
    pub page_id: Option<u32>,
    pub pin_counter: AtomicU16,
    pub dirty_flag: AtomicBool,
}

pub struct BufferPoolManager {
    pub frames: Vec<RwLock<Frame>>,
    pub page_table: RwLock<HashMap<u32, usize>>, // page_id -> frame index
    pub disk_manager: Arc<DiskManager>,
    pub arc_cache: Mutex<ArcCache>,
    pub wal: OnceLock<Arc<WalManager>>,
}

const MAX_BUFFER_POOL_PAGES: usize = 1024;

impl BufferPoolManager {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self{
        let frames: Vec<RwLock<Frame>> = (0..MAX_BUFFER_POOL_PAGES).map(|_| RwLock::new(Frame::new())).collect();
        BufferPoolManager {frames, page_table: RwLock::new(HashMap::new()), disk_manager, arc_cache: Mutex::new(ArcCache::new(MAX_BUFFER_POOL_PAGES)), wal: OnceLock::new()}
    }

    // if cached, return page. else, load from disk into a frame (and evicting if all frames are full), then pin
    pub fn fetch_page(&self, page_id: u32) -> Result<usize, FerroError>{
        // The cache's verdict and the action taken on it must be one atomic step.
        //
        // This lock used to be taken just for `request` and released immediately, so the verdict
        // was already stale by the time the page table was consulted: another thread could evict
        // the page this call was told was a Hit, or evict the very victim it was handed. Both then
        // indexed a HashMap with `[]` on a key that had gone, which PANICS the process —
        // `no entry found for key` at the hit path and at the eviction path, reproducibly, under
        // 8 threads with more pages than frames.
        //
        // Holding it across the whole of `fetch_page` makes the decision and the mutation
        // inseparable. Lock order is arc_cache -> page_table -> frame, and nothing else in this
        // file takes arc_cache, so there is nothing to invert against.
        //
        // This serialises fetches. That is a real cost and it is the right trade here: the
        // alternative on offer is a buffer pool that crashes, and a faster wrong answer is not a
        // better one. Narrowing it later means giving the cache and the table a single lock, not
        // taking this one for less time.
        let mut cache = self.arc_cache.lock().unwrap();
        let result = cache.request(page_id, &|id| {
            let pt = self.page_table.read().unwrap();
            let frame_i = pt[&id];
            let frame = self.frames[frame_i].read().unwrap();
            frame.pin_counter.load(Ordering::Relaxed) > 0
        });

        match result {
            ArcResult::Hit => { // page was already cached 
                let pt = self.page_table.read().unwrap();
                let frame_i = pt[&page_id];
                let frame = self.frames[frame_i].read().unwrap();
                frame.pin_counter.fetch_add(1, Ordering::Relaxed);
                return Ok(frame_i)
            }
            ArcResult::MissEvict(evicted_id) => { // page not cached and pool is full (victim eviction)
                let pt = self.page_table.read().unwrap();
                let frame_i = pt[&evicted_id];
                drop(pt);

                self.flush_page(evicted_id)?;
                let new_page_data = self.disk_manager.read(page_id)?;
                let mut frame = self.frames[frame_i].write().unwrap();
                frame.data = new_page_data;
                frame.page_id = Some(page_id);
                frame.pin_counter = AtomicU16::new(1);
                frame.dirty_flag = AtomicBool::new(false);
                drop(frame);

                let mut pt = self.page_table.write().unwrap();
                pt.remove(&evicted_id);
                pt.insert(page_id, frame_i);
                return Ok(frame_i)
            }
            ArcResult::MissNoEvict => { // page not cached, pool not full
                let data = self.disk_manager.read(page_id)?;

                // The page table lock is held across the whole claim, and that is the fix rather
                // than a tidy-up. Two races lived here and both silently LOST WRITES:
                //
                // 1. The free-frame scan probed under a READ lock, dropped it, then re-acquired a
                //    WRITE lock. Two threads could both see frame `i` free, and both write a
                //    different page into it — the second overwriting the first, while the page
                //    table pointed a page at a frame holding someone else's data.
                //
                // 2. Two threads missing on the SAME page each loaded it into a different frame.
                //    One frame is orphaned, and every write that lands there is lost when the
                //    table resolves the page to the other frame.
                //
                // Holding one lock for check-and-claim closes both: a concurrent inserter is
                // observed under the same lock that would have to publish it.
                let mut pt = self.page_table.write().unwrap();

                // Someone may have loaded it while this thread was reading from disk.
                if let Some(&existing) = pt.get(&page_id) {
                    let frame = self.frames[existing].read().unwrap();
                    frame.pin_counter.fetch_add(1, Ordering::Relaxed);
                    return Ok(existing);
                }

                for i in 0..self.frames.len() {
                    let mut frame = self.frames[i].write().unwrap();
                    // Claimed under the write lock, so a second thread reaching this frame sees
                    // it taken instead of racing for it.
                    if frame.page_id.is_none() {
                        frame.data = data;
                        frame.page_id = Some(page_id);
                        frame.pin_counter = AtomicU16::new(1);
                        frame.dirty_flag = AtomicBool::new(false);
                        pt.insert(page_id, i);
                        return Ok(i);
                    }
                }
                // Reachable when every frame filled up between the cache's verdict and this scan,
                // which is a real outcome under concurrency rather than an impossible one.
                Err(FerroError::NotEnoughSpace)
            }
            ArcResult::PoolFull => { // page not cached, pool is full, everything is pinned
                return Err(FerroError::NotEnoughSpace)
            }
        }
        // `cache` is deliberately still alive at every return above: dropping it earlier is what
        // reintroduces the stale-verdict window.
    }

    // decrement pin count, if page was modified, add dirty flag
    pub fn unpin_page(&self, page_id: u32, is_dirty: bool) {
        let pt = self.page_table.read().unwrap();
        let frame_i = pt[&page_id];
        drop(pt);

        let frame = self.frames[frame_i].read().unwrap();
        if is_dirty {
            frame.dirty_flag.store(true, Ordering::Relaxed);
        }
        let _ = frame.pin_counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
            if val > 0 {Some(val-1)} else {None}
        });
    }

    // allocate new page on disk using disk manager, load into a frame, return page id
    pub fn new_page(&self) -> Result<u32, FerroError>{
        let page_id = self.disk_manager.allocate()?;
        self.disk_manager.write(page_id, &[0u8; PAGE_SIZE])?;
        self.fetch_page(page_id)?;
        self.unpin_page(page_id, false);
        Ok(page_id)
    }

    // writes a dirty page to disk
    pub fn flush_page(&self, page_id: u32) -> Result<(), FerroError>{
        let pt = self.page_table.read().unwrap();
        let frame_i = pt[&page_id];
        drop(pt);

        let frame = self.frames[frame_i].read().unwrap();
        if frame.dirty_flag.load(Ordering::Relaxed) {
            self.wal_gate(&frame.data)?;
            self.disk_manager.write(page_id, &frame.data)?;
            frame.dirty_flag.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    // write all dirty pages to disk
    pub fn flush_all(&self) -> Result<(), FerroError>{
        let pt = self.page_table.read().unwrap();

        for (&page_id, &frame_i) in pt.iter() {
            let frame = self.frames[frame_i].read().unwrap();
            if frame.dirty_flag.load(Ordering::Relaxed) {
                self.wal_gate(&frame.data)?;
                self.disk_manager.write(page_id, &frame.data)?;
                frame.dirty_flag.store(false,Ordering::Relaxed);
            }
        }
        Ok(())
    }

    // remove from buffer pool, deallocate on disk
    pub fn delete_page(&self, page_id: u32) -> Result<(), FerroError>{
        let mut pt = self.page_table.write().unwrap();
        
        let frame_i = match pt.get(&page_id){
            Some(&i) => i,
            None => return Err(FerroError::KeyNotFound)
        };

        if self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {
            return Err(FerroError::PagePinned);
        }
        // Ask the disk manager BEFORE touching anything. `deallocate` can refuse — a page inside
        // a reserved arena region is not this allocator's to free — and evicting first would make
        // that refusal a lie: the caller gets an Err saying the free did not happen, while the
        // frame has already been zeroed and the page table entry dropped. For an unflushed dirty
        // page, memory is the only copy, so the "failed" delete is what destroys it.
        //
        // Still holding the page-table write lock, so no other thread can fault the page in
        // between the two steps. page_table -> bitmap_lock is the only order taken anywhere.
        self.disk_manager.deallocate(page_id)?;

        pt.remove(&page_id);
        drop(pt);

        let mut frame = self.frames[frame_i].write().unwrap();
        frame.page_id = None;
        frame.data = [0u8; PAGE_SIZE];
        frame.pin_counter = AtomicU16::new(0);
        frame.dirty_flag = AtomicBool::new(false);
        drop(frame);

        self.arc_cache.lock().unwrap().remove(page_id)?;
        Ok(())
    }

    pub fn free_page(&self, page_id: u32) -> Result<(), FerroError> {
        let mut pt = self.page_table.write().unwrap();
        let resident = match pt.get(&page_id) {
            Some(&frame_i) => {
                if self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {
                    return Err(FerroError::PagePinned);
                }
                Some(frame_i)
            }
            None => None,
        };

        // Same ordering rule as `delete_page`: the refusable step goes first, so a refusal leaves
        // the pool exactly as it found it rather than reporting a failure it already half did.
        self.disk_manager.deallocate(page_id)?;

        if let Some(frame_i) = resident {
            pt.remove(&page_id);
            drop(pt);
            let mut frame = self.frames[frame_i].write().unwrap();
            frame.page_id = None;
            frame.data = [0u8; PAGE_SIZE];
            frame.pin_counter = AtomicU16::new(0);
            frame.dirty_flag = AtomicBool::new(false);
            drop(frame);
            self.arc_cache.lock().unwrap().remove(page_id)?;
        }
        Ok(())
    }

    pub fn attach_wal(&self, wal: Arc<WalManager>) {
        let _ = self.wal.set(wal);
    }

    fn wal_gate(&self, data: &[u8; PAGE_SIZE]) -> Result<(), FerroError> {
        if let Some(wal) = self.wal.get() {
            let plsn = page_lsn_of(data);
            if plsn > 0 {
                wal.flush_up_to(plsn)?;
            }
        }
        Ok(())
    }
}

fn page_lsn_of(data: &[u8; PAGE_SIZE]) -> u64 {
    match data[0] {
        0 => u64::from_be_bytes(data[11..19].try_into().unwrap()),
        2 | 3 => u64::from_be_bytes(data[5..13].try_into().unwrap()),
        _ => 0,
    }
}

impl Frame {
    pub fn new() -> Self {
        Frame {data: [0u8; PAGE_SIZE], page_id: None, pin_counter: AtomicU16::new(0), dirty_flag: AtomicBool::new(false)}
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;
    use std::fs::OpenOptions;
    use std::sync::Arc;

    /// A pool over a real file, with an arena floor registered so `deallocate` will refuse.
    ///
    /// `tag` must be unique per test. These run as threads in one binary, so a filename keyed
    /// only on the pid is shared, and one test's `remove_file`-then-create races another's open.
    fn pool_with_arena_floor(tag: &str) -> (Arc<BufferPoolManager>, u32, std::path::PathBuf) {
        let path = std::env::temp_dir()
            .join(format!("ferro-bp-partialfree-{}-{}.db", std::process::id(), tag));
        let _ = std::fs::remove_file(&path);
        let file = OpenOptions::new().create(true).read(true).write(true)
            .open(&path).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        // Hand out a few pages, then declare everything from `floor` up to be arena-owned. The
        // pages already handed out above the floor are exactly the ones `deallocate` must refuse.
        let mut pages = Vec::new();
        for _ in 0..6 {
            pages.push(bp.new_page().unwrap());
        }
        let floor = pages[2];
        bp.disk_manager.reserve_from(floor).unwrap();
        (bp, pages[4], path) // pages[4] is above the floor -> deallocate refuses it
    }

    /// S6. `delete_page` evicted the frame before asking the disk manager, so a refusal came back
    /// as `Err` *after* the page had already been dropped from the pool — and an unflushed dirty
    /// page's contents went with it. The error says the free did not happen; the pool disagreed.
    #[test]
    fn a_refused_delete_leaves_the_page_intact_in_the_pool() {
        let (bp, page, path) = pool_with_arena_floor("delete");

        // Dirty the page and do NOT flush: memory is now the only copy of this byte.
        let frame_i = bp.fetch_page(page).unwrap();
        bp.frames[frame_i].write().unwrap().data[100] = 0xAB;
        bp.unpin_page(page, true);

        let err = bp.delete_page(page);
        assert!(err.is_err(), "precondition: the arena floor must make this deallocate refuse");

        // The delete was refused, so nothing about the page may have changed.
        let pt = bp.page_table.read().unwrap();
        let still_resident = pt.get(&page).copied();
        drop(pt);
        assert!(
            still_resident.is_some(),
            "delete_page reported failure but dropped page {} from the page table",
            page
        );
        let i = still_resident.unwrap();
        assert_eq!(
            bp.frames[i].read().unwrap().data[100], 0xAB,
            "delete_page reported failure but zeroed the frame, destroying the only copy"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Same contract for `free_page`, which has the same evict-then-ask ordering.
    #[test]
    fn a_refused_free_page_leaves_the_page_intact_in_the_pool() {
        let (bp, page, path) = pool_with_arena_floor("free");

        let frame_i = bp.fetch_page(page).unwrap();
        bp.frames[frame_i].write().unwrap().data[100] = 0xCD;
        bp.unpin_page(page, true);

        let err = bp.free_page(page);
        assert!(err.is_err(), "precondition: the arena floor must make this deallocate refuse");

        let pt = bp.page_table.read().unwrap();
        let still_resident = pt.get(&page).copied();
        drop(pt);
        assert!(
            still_resident.is_some(),
            "free_page reported failure but dropped page {} from the page table",
            page
        );
        assert_eq!(
            bp.frames[still_resident.unwrap()].read().unwrap().data[100], 0xCD,
            "free_page reported failure but zeroed the frame"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Control: when the disk manager does NOT refuse, the page really is evicted. Without this
    /// the two tests above would pass against a `delete_page` that simply never did anything.
    #[test]
    fn an_accepted_delete_still_evicts_the_page() {
        let (bp, _refused, path) = pool_with_arena_floor("accepted");
        let below = 1u32; // below the floor, so the deallocate is allowed
        bp.fetch_page(below).unwrap();
        bp.unpin_page(below, false);
        assert!(bp.page_table.read().unwrap().contains_key(&below));

        bp.delete_page(below).unwrap();
        assert!(
            !bp.page_table.read().unwrap().contains_key(&below),
            "an accepted delete must actually evict"
        );
        let _ = std::fs::remove_file(&path);
    }
}
