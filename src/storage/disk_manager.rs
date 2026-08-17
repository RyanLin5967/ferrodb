use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use crate::error::FerroError;

pub const PAGE_SIZE: usize = 4096;
const BITS_PER_BITMAP: u32 = (PAGE_SIZE as u32 - 4) *8;
pub struct DiskManager {
    pub next_page_id: AtomicU32,
    pub file: File,
    bitmap_lock: Mutex<()>,
    /// First page of a region this allocator must never touch, or `u32::MAX` when there is none.
    ///
    /// `branch::arena::ArenaPageStore` hands out pages from extents it tracks itself and never
    /// sets their bitmap bits, so without this floor the bitmap scan below sees that whole region
    /// as free and hands the same pages out a second time. The bits are zero from page 0, so this
    /// is not a hazard that needs the file to grow into the region first: on a fresh database the
    /// very first `allocate()` collides with the very first arena page. Two writers then share a
    /// page and each silently overwrites the other.
    ///
    /// Documented exclusivity is not exclusivity. This is the enforcement.
    arena_floor: AtomicU32,
}

impl DiskManager{

    // writes page 0 if it isn't already written with data. bytes 0-3 are header(pointer to next bitmap page), 4 is 1, rest is 0
    pub fn new(file: File) -> Result<Self, FerroError>{
        let metadata = match file.metadata().map_err(|e| FerroError::Io(e.to_string())){
            Ok(me) => me,
            Err(e) => return Err(FerroError::Io(e.to_string()))
        };
        let next_page_id: u32;
        if metadata.len() == 0{
            let mut first_page_bitmap = [0u8; PAGE_SIZE];
            first_page_bitmap[4] = 1;
            let mut total_written = 0;
            while total_written < PAGE_SIZE{
                let written = match pwrite(&file, &first_page_bitmap[total_written..], total_written as u64) {
                    Ok(w) => w,
                    Err(e) => return Err(FerroError::Io(e.to_string()))
                };
                total_written += written;
                if written == 0 {
                    return Err(FerroError::Io(format!("couldn't write all {} bytes", PAGE_SIZE)))
                }
            }
            next_page_id = 1;
        }else {
            next_page_id = (metadata.len()/PAGE_SIZE as u64) as u32;
        }
        Ok(DiskManager {
            next_page_id: AtomicU32::new(next_page_id),
            file,
            bitmap_lock: Mutex::new(()),
            arena_floor: AtomicU32::new(u32::MAX),
        })
    }
    
    pub fn write(&self, page_id: u32, data: &[u8]) -> Result<(), FerroError>{
        if data.len() != PAGE_SIZE{
            return Err(FerroError::Io(format!("Page length must be: {}", PAGE_SIZE)))
        }
        let offset:u64 = page_id as u64* PAGE_SIZE as u64;
        let mut total_wrote = 0;
        while total_wrote < PAGE_SIZE {
            let written = match pwrite(&self.file, &data[total_wrote..] , offset + total_wrote as u64){
                Ok(w) => w,
                Err(e) => return Err(FerroError::Io(e.to_string()))
            };
            if written == 0 {
                return Err(FerroError::Io(format!("couldn't write all {} bytes", PAGE_SIZE)))
            }
            total_wrote += written;
        }
        
        Ok(())
    }

    pub fn read(&self, page_id: u32) -> Result<[u8; PAGE_SIZE], FerroError>{
        let mut buffer = [0u8; PAGE_SIZE];
        let offset = page_id as u64 * PAGE_SIZE as u64;
        let mut total_read = 0;
        while total_read < PAGE_SIZE {
            let size = match pread(&self.file, &mut buffer[total_read..], offset + total_read as u64) {
                Ok(s) => s,
                Err(e) => return Err(FerroError::Io(e.to_string()))
            };
            total_read += size;

            if size == 0 {
                return Err(FerroError::Io(String::from("eof before finished reading")))
            }
        }
        Ok(buffer)
    }

    // sets a page as free/unused
    pub fn deallocate(&self, page_id: u32) -> Result<(), FerroError>{
        let _guard = self.bitmap_lock.lock().unwrap();
        // An arena page has no bit here. Clearing the bit at that index would free an unrelated
        // page belonging to this allocator.
        if page_id >= self.arena_floor.load(Ordering::SeqCst) {
            return Err(FerroError::Io(format!(
                "page {} is inside the reserved arena region and is not this allocator's to free",
                page_id
            )));
        }
        let mut current_bitmap_id = 0;
        let mut jumps_needed = page_id/BITS_PER_BITMAP;
        let mut page_bitmap = self.read(current_bitmap_id)?;

        while jumps_needed > 0 {
            let next_bitmap_id = u32::from_le_bytes(page_bitmap[0..4].try_into().unwrap());
            if next_bitmap_id == 0 {
                return Err(FerroError::Io(String::from("can't deallocate an unmapped page")))
            }
            current_bitmap_id = next_bitmap_id;
            page_bitmap = self.read(current_bitmap_id)?;
            jumps_needed -=1;
        }

        let local_page_id = page_id % BITS_PER_BITMAP;
        let byte_index = (local_page_id/8) as usize + 4;
        let bit_index = local_page_id % 8;
        page_bitmap[byte_index] &= !(1 << bit_index);
        match self.write(current_bitmap_id, &page_bitmap) {
            Ok(_) => (),
            Err(e) => return Err(e)
        };
        Ok(())
    }

    /// Highest page the **bitmap allocator** has handed out, plus one.
    ///
    /// Distinct from [`DiskManager::high_water`], which additionally clamps with `next_page_id`.
    /// That clamp is right for placing a *new* arena, but wrong for reattaching to an existing
    /// one: on reopen `next_page_id` is derived from the file length, and an arena's pages extend
    /// the file without ever setting a bitmap bit, so the clamped mark sits *above* the arena's
    /// own base and would refuse the arena the very region it owns. This answers the narrower
    /// question — what does the bitmap allocator itself claim? — which is what an arena reattach
    /// needs to check against.
    pub fn bitmap_high_water(&self) -> Result<u32, FerroError> {
        let _guard = self.bitmap_lock.lock().unwrap();
        self.scan_bitmap_high_water()
    }

    /// Caller must hold `bitmap_lock`; the mutex is not reentrant.
    fn scan_bitmap_high_water(&self) -> Result<u32, FerroError> {
        let mut current_bitmap_id = 0;
        let mut global_offset = 0u32;
        let mut highest: Option<u32> = None;
        loop {
            let page_bitmap = self.read(current_bitmap_id)?;
            for local in (0..BITS_PER_BITMAP).rev() {
                let byte_index = (local / 8) as usize + 4;
                if page_bitmap[byte_index] & (1 << (local % 8)) != 0 {
                    highest = Some(global_offset + local);
                    break;
                }
            }
            let next_bitmap_id = u32::from_le_bytes(page_bitmap[0..4].try_into().unwrap());
            if next_bitmap_id == 0 {
                break;
            }
            current_bitmap_id = next_bitmap_id;
            global_offset += BITS_PER_BITMAP;
        }
        Ok(highest.map(|h| h + 1).unwrap_or(0))
    }

    /// Where a **new** allocator may start: past everything the bitmap owns, and past the page
    /// counter as well.
    ///
    /// Deliberately NOT bounded by an explicit file-length read, though "every page that exists"
    /// sounds like the safer answer. An arena's pages extend the file without ever setting a bit
    /// here, so folding the file length in makes the mark climb above the arena's own base, and
    /// `ArenaPageStore::new` — which refuses a base below the mark — could then never reopen a
    /// store at the base it already uses. Tried it; it breaks every restart test
    /// (`free_space_map_survives_a_restart`, `checkpoint_round_trips_through_a_file`,
    /// `branches_abandoned_before_a_restart_are_still_reaped_after_it`).
    ///
    /// Note the clamp below is not fully free of that effect: `DiskManager::new` seeds
    /// `next_page_id` from the file length, so on the FIRST call after a reopen this mark does sit
    /// above any arena pages written by the previous process. That is correct for placing a new
    /// arena and wrong for reattaching to an existing one, which is why reattach asks
    /// [`DiskManager::bitmap_high_water`] instead. See `ArenaPageStore::reopen`.
    pub fn high_water(&self) -> Result<u32, FerroError> {
        let _guard = self.bitmap_lock.lock().unwrap();
        let from_bitmap = self.scan_bitmap_high_water()?;
        Ok(from_bitmap.max(self.next_page_id.load(Ordering::SeqCst)))
    }

    /// Reserve `[base, infinity)` for another allocator, so this one stops there.
    ///
    /// Called by `ArenaPageStore::new`, which has already checked that `base` is at or above the
    /// high-water mark. Registering a second, lower floor is refused rather than accepted: the
    /// pages between the two are already inside the first store's extents, and lowering the floor
    /// would put them back in circulation.
    ///
    /// **Blind spot, stated deliberately.** This guard separates *this* allocator from the arena
    /// region. It says nothing about two arena stores sharing that region with each other: a
    /// second store registered at the same or a higher base is accepted, and if it is live at the
    /// same time as the first they will hand out the same pages. That is not hypothetical — the
    /// branch module's own harness constructs a second store at the same base on purpose, to
    /// simulate a restart. It is safe there only because the first is no longer being written
    /// through. Nothing here enforces that, so two *concurrent* arena stores over one file remain
    /// unsafe.
    pub fn reserve_from(&self, base: u32) -> Result<(), FerroError> {
        let _guard = self.bitmap_lock.lock().unwrap();
        let current = self.arena_floor.load(Ordering::SeqCst);
        if current == u32::MAX {
            self.arena_floor.store(base, Ordering::SeqCst);
            return Ok(());
        }
        if base == current {
            // Reattaching to the same region. This is a restart, and the branch module's harness
            // does it deliberately (`fresh_store()` passes `self.store.base_page()`).
            return Ok(());
        }
        if base < current {
            return Err(FerroError::Io(format!(
                "page region [{}, inf) is already reserved; cannot lower the floor to {}",
                current, base
            )));
        }
        // base > current. The old code returned Ok here and recorded NOTHING, which is the worst
        // of the three options: the caller is told its region is reserved while the first store's
        // extent bump pointer has no upper bound and will walk straight into it. Recording it
        // instead would be just as wrong — raising the floor puts the first store's own extents
        // back into the bitmap's circulation. There is no correct single-floor answer for two
        // distinct live regions, so refuse and say why.
        Err(FerroError::Io(format!(
            "page region [{}, inf) is already reserved by another store; a second region at {} \
             cannot be represented by a single floor, and the existing store's extents are \
             unbounded above",
            current, base
        )))
    }

    /// First page this allocator must not touch.
    pub fn arena_floor(&self) -> u32 {
        self.arena_floor.load(Ordering::SeqCst)
    }


/// The message a caller meets when ordinary tables have grown into the arena floor.
///
/// Shared by both exhaustion paths so they cannot drift, and worded around the fact that makes this
/// error different from an ordinary "disk full": **the floor is chosen once, when the database is
/// created, and then persisted in the arena checkpoint.** Raising `FERRODB_ARENA_HEADROOM` afterwards
/// changes nothing for this database, because moving the floor would put pages the arena already
/// owns back into the ordinary allocator's circulation. A message that named the knob without
/// saying that would send the reader to set a variable and watch it not work.
fn arena_floor_exhausted(what: &str, floor: u32) -> FerroError {
    FerroError::Io(format!(
        "{what} the reserved arena region at page {floor}. Ordinary tables occupy [0, {floor}) and \
         the copy-on-write branch arena owns everything from {floor} up, so the table region is \
         full even though the file can still grow.\n\
         This floor was fixed when the database was created and is stored in the arena checkpoint: \
         raising FERRODB_ARENA_HEADROOM now will NOT move it, because pages at or above {floor} are \
         already owned by the arena and re-issuing them would corrupt live branches.\n\
         To get more table space, create a new database with a larger FERRODB_ARENA_HEADROOM and \
         copy the data across."
    ))
}

    //first checks bitmap if there is a free page if not, then give it next_page_id and increment it
    pub fn allocate(&self) -> Result<u32, FerroError>{
        let _guard = self.bitmap_lock.lock().unwrap();
        let floor = self.arena_floor.load(Ordering::SeqCst);
        let mut current_bitmap_id = 0;
        let mut global_offset = 0;
        loop {
            let mut page_bitmap = self.read(current_bitmap_id)?;

            for byte_index in 4..PAGE_SIZE {
                if page_bitmap[byte_index] != 0xFF {
                    for bit_index in 0..8 {
                        if page_bitmap[byte_index] & (1<<bit_index) == 0 {
                            let page_id: usize = (byte_index - 4) * 8 + bit_index;
                            let candidate = global_offset + page_id as u32;
                            // Everything at or above the floor belongs to the arena store, whose
                            // pages are not tracked here. Handing one out would alias it.
                            if candidate >= floor {
                                return Err(Self::arena_floor_exhausted(
                                    "no free page below",
                                    floor,
                                ));
                            }
                            page_bitmap[byte_index] |= 1 << bit_index;
                            self.write(current_bitmap_id, &page_bitmap)?;
                            return Ok(candidate);
                        }
                    }
                }
            }
            let next_bitmap_id = u32::from_le_bytes(page_bitmap[0..4].try_into().unwrap());
            
            if next_bitmap_id != 0 {
                current_bitmap_id = next_bitmap_id;
                global_offset += BITS_PER_BITMAP;
                continue;
            }
            // This path runs only when every bit in every chained bitmap is set — i.e. at least
            // BITS_PER_BITMAP (32736) pages are already allocated. `next_page_id` can still read 1
            // at that moment, because the fast path above never advances it. Growing from that
            // counter therefore hands out pages the bitmap already owns, and the floor check based
            // on it is comparing the wrong number. Grow from the real mark instead.
            let grow_base = self
                .scan_bitmap_high_water()?
                .max(self.next_page_id.load(Ordering::SeqCst));
            // Two pages are about to be taken: the new bitmap page and the page it serves.
            if grow_base.saturating_add(1) >= floor {
                return Err(Self::arena_floor_exhausted("cannot grow the bitmap past", floor));
            }
            let new_bitmap_id = grow_base;
            let page_id = grow_base + 1;
            // Keep the counter monotonic and never behind what has actually been handed out.
            self.next_page_id.fetch_max(page_id + 1, Ordering::SeqCst);
            page_bitmap[0..4].copy_from_slice(&new_bitmap_id.to_le_bytes());
            self.write(current_bitmap_id, &page_bitmap)?;
            let mut new_bitmap = [0u8; PAGE_SIZE];

            let bm_local_id = new_bitmap_id % BITS_PER_BITMAP;
            let byte_index = (bm_local_id/8) as usize + 4;
            let bit_index = (bm_local_id % 8) as usize;
            new_bitmap[byte_index] |= 1 << bit_index;

            let local_id = page_id% BITS_PER_BITMAP;
            let byte_ind = (local_id/8)as usize + 4;
            let bit_ind = local_id % 8;
            new_bitmap[byte_ind] |= 1 << bit_ind;

            self.write(new_bitmap_id, &new_bitmap)?;
            return Ok(page_id)
        }
    }

    pub fn sync(&self) -> Result<(), FerroError>{
        self.file.sync_all().map_err(|e| FerroError::Io(e.to_string()))
    }
}

pub fn pwrite(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(windows)]
    { use std::os::windows::fs::FileExt; file.seek_write(buf, offset)}
    #[cfg(unix)]
    { use std::os::unix::fs::FileExt; file.write_at(buf, offset)}
}

pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(windows)]
    { use std::os::windows::fs::FileExt; file.seek_read(buf, offset)}
    #[cfg(unix)]
    { use std::os::unix::fs::FileExt; file.read_at(buf, offset)}
}
#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::storage::disk_manager::DiskManager;
    use std::sync::atomic::Ordering;
    use super::{BITS_PER_BITMAP, PAGE_SIZE};
    use std::fs::OpenOptions;
    #[test]
    pub fn test_rw() -> Result<(), Box<dyn std::error::Error>>{
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let temp_file = OpenOptions::new().read(true).write(true).create(true)
                                                .open(&temp_path)?;
        let dm = DiskManager::new(temp_file).unwrap();
        let data1 = [8u8; 4096];
        let data2 = [2u8; 4096];
        let _ = dm.write(1, &data1);
        let _ = dm.write(3, &data2);
        let read1 = dm.read(1)?;
        let read2 = dm.read(3)?;
        assert_eq!(read1, data1);
        assert_eq!(read2, data2);
        Ok(())
    }

    /// S1: the trap that made an arena alias the bitmap allocator.
    ///
    /// `next_page_id` looks like a high-water mark and is not one. `allocate()`'s fast path
    /// satisfies a request from a free bit and returns without touching it, so it stays at 1
    /// through thousands of allocations. Anything validating "is this page region unclaimed?"
    /// against it accepts a region the bitmap already owns.
    ///
    /// This asserts the gap directly, so the trap cannot quietly come back.
    #[test]
    pub fn next_page_id_is_not_the_high_water_mark() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new().unwrap();
        let temp_file = OpenOptions::new().read(true).write(true).create(true)
            .open(temp_dir.path().join("hw.db"))?;
        let dm = DiskManager::new(temp_file).unwrap();

        let mut highest = 0u32;
        for _ in 0..500 {
            highest = highest.max(dm.allocate().unwrap());
        }
        assert!(highest >= 500, "expected ~500 pages handed out, got {}", highest);

        // The stale counter: still 1 after 500 allocations.
        let stale = dm.next_page_id.load(Ordering::SeqCst);
        assert!(
            stale <= highest,
            "next_page_id ({}) unexpectedly tracked the allocator (highest {}) - if this ever \
             becomes true the S1 trap is gone, but high_water() must still be the API used",
            stale, highest
        );

        // The real answer covers everything handed out.
        let hw = dm.high_water().unwrap();
        assert!(
            hw > highest,
            "high_water ({}) must exceed the highest allocated page ({})",
            hw, highest
        );
        Ok(())
    }

    /// A page freed and reused must not push the high-water mark backwards.
    #[test]
    fn exhausting_the_table_region_says_the_knob_will_not_help_this_database() {
        use std::fs::OpenOptions;
        let temp_dir = TempDir::new().unwrap();
        let f = OpenOptions::new().read(true).write(true).create(true)
            .open(temp_dir.path().join("full.db")).unwrap();
        let dm = DiskManager::new(f).unwrap();
        dm.reserve_from(12).expect("reserve");

        // Allocate until the region below the floor is gone.
        let mut err = None;
        for _ in 0..64 {
            if let Err(e) = dm.allocate() {
                err = Some(e);
                break;
            }
        }
        let msg = format!("{}", err.expect("the table region never filled, so nothing was tested"));

        // The number alone is what this used to say, and it sent the reader nowhere.
        assert!(msg.contains("FERRODB_ARENA_HEADROOM"), "the message does not name the knob: {msg}");
        // The load-bearing sentence. Naming the knob without this is worse than not naming it: the
        // reader sets the variable, reopens, and meets the identical error with no idea why.
        assert!(
            msg.contains("will NOT move it"),
            "the message does not say the floor is fixed for this database, so it invites the \
             reader to set a variable that cannot help them: {msg}"
        );
        assert!(
            msg.contains("copy the data across"),
            "the message states the problem but not the remedy: {msg}"
        );
    }

    #[test]
    pub fn high_water_does_not_regress_after_a_free() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new().unwrap();
        let temp_file = OpenOptions::new().read(true).write(true).create(true)
            .open(temp_dir.path().join("hw2.db"))?;
        let dm = DiskManager::new(temp_file).unwrap();
        for _ in 0..64 { dm.allocate().unwrap(); }
        let before = dm.high_water().unwrap();
        dm.deallocate(10).unwrap();
        let after = dm.high_water().unwrap();
        assert!(after >= before - 1, "high_water fell from {} to {} after one free", before, after);
        Ok(())
    }

    /// S3: growing the bitmap must not allocate from the stale counter.
    ///
    /// The grow path runs only when every bit is set — at least BITS_PER_BITMAP pages already
    /// allocated — yet `next_page_id` can still read 1 there, because the fast path never
    /// advances it. Growing from it hands out pages the bitmap already owns.
    ///
    /// Filling 32736 pages for real would be 260MB of bitmap I/O, so the full bitmap is written
    /// directly. That is the same state `allocate()` would reach organically, minus the wait.
    #[test]
    pub fn growing_the_bitmap_does_not_reuse_owned_pages() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new().unwrap();
        let temp_file = OpenOptions::new().read(true).write(true).create(true)
            .open(temp_dir.path().join("grow.db"))?;
        let dm = DiskManager::new(temp_file).unwrap();

        // Every page this bitmap covers is allocated; no next-bitmap pointer yet.
        let mut full = [0xFFu8; PAGE_SIZE];
        full[0..4].copy_from_slice(&0u32.to_le_bytes());
        dm.write(0, &full)?;

        // The counter is still at its initial value and knows nothing about those 32736 pages.
        let stale = dm.next_page_id.load(Ordering::SeqCst);
        assert!(stale < BITS_PER_BITMAP, "counter {} was expected to be stale", stale);

        let got = dm.allocate().unwrap();
        assert!(
            got >= BITS_PER_BITMAP,
            "allocate() handed out page {}, which the bitmap already owns (grew from the stale \
             counter {} instead of the real high-water mark {})",
            got, stale, BITS_PER_BITMAP
        );
        // And the page it served must not be the new bitmap page itself.
        assert_ne!(got, BITS_PER_BITMAP, "served the new bitmap page as data");
        Ok(())
    }

    /// S4: a second reservation at a HIGHER base used to return Ok while recording nothing.
    ///
    /// The caller then believes its region is reserved when it is not, and the first store's
    /// extent bump pointer — which has no upper bound — walks into it. Refusing is the only
    /// honest answer: a single floor cannot represent two distinct live regions.
    #[test]
    pub fn a_second_reservation_at_a_higher_base_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new().unwrap();
        let temp_file = OpenOptions::new().read(true).write(true).create(true)
            .open(temp_dir.path().join("res.db"))?;
        let dm = DiskManager::new(temp_file).unwrap();

        dm.reserve_from(1024).expect("first reservation");
        assert_eq!(dm.arena_floor(), 1024);

        let second = dm.reserve_from(2048);
        assert!(second.is_err(), "a second region at 2048 was silently accepted");
        assert_eq!(dm.arena_floor(), 1024, "the floor moved on a refused reservation");
        Ok(())
    }

    /// Control: reattaching at the SAME base must still succeed, because that is a restart and
    /// the branch harness depends on it. Without this, the fix above would break every reopen.
    #[test]
    pub fn reattaching_at_the_same_base_is_allowed() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new().unwrap();
        let temp_file = OpenOptions::new().read(true).write(true).create(true)
            .open(temp_dir.path().join("res2.db"))?;
        let dm = DiskManager::new(temp_file).unwrap();
        dm.reserve_from(1024).expect("first");
        dm.reserve_from(1024).expect("reattach at the same base must be allowed");
        assert_eq!(dm.arena_floor(), 1024);
        // ...and lowering is still refused.
        assert!(dm.reserve_from(512).is_err(), "lowering the floor was accepted");
        Ok(())
    }

    #[test]
    pub fn test_freelist() -> Result<(), Box<dyn std::error::Error>>{
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let temp_file = OpenOptions::new().read(true).write(true).create(true)
                                                .open(&temp_path)?;
        let dm = DiskManager::new(temp_file).unwrap();
        let page1 = dm.allocate().unwrap();
        let _page2 = dm.allocate().unwrap();
        let _page3 = dm.allocate().unwrap();
        let _ = dm.deallocate(page1);
        let page4 = dm.allocate().unwrap();
        assert_eq!(page1, page4);
        Ok(())
    }
}

