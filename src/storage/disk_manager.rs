use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use crate::error::FerroError;
use crate::storage::storage::Storage;

pub const PAGE_SIZE: usize = 4096;
const BITS_PER_BITMAP: u32 = (PAGE_SIZE as u32 - 4) *8;
pub struct DiskManager {
    pub next_page_id: AtomicU32,
    /// Where the pages actually go.
    ///
    /// This was a concrete `File`, and that was the reason no crash could be *aimed* at this
    /// database: there was no seam at which a write could be made to tear, vanish, or fail to flush,
    /// so the recovery path — the entire justification for a write-ahead log — was reachable only by
    /// hand-editing a file after the fact. `impl Storage for File` means the production path performs
    /// the same syscalls in the same order as before; see [`DiskManager::new`].
    pub storage: Arc<dyn Storage>,
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
    ///
    /// **A single floor could represent exactly one region, and this allocator's own
    /// `reserve_from` said so**: it refused a second reservation with "a second region at {base}
    /// cannot be represented by a single floor". That refusal was correct and it was also a wall —
    /// the branch catalog needs a page source of its own that does not depend on the branch
    /// catalog, and there was nowhere to put it. So the floor became a table.
    regions: Mutex<Vec<Region>>,
}

/// A half-open page range `[lo, hi)` owned by some allocator other than this one.
///
/// `hi == u32::MAX` means unbounded above, which is what the branch arena takes today. An
/// unbounded region is the reason the ordinary allocator still cannot grow past it — see
/// [`DiskManager::reserve_region`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Region {
    /// `&'static str` rather than `String`: every region is named at a call site, and the error
    /// paths below run while the bitmap lock is held, where an allocation is the wrong thing to do.
    name: &'static str,
    lo: u32,
    hi: u32,
}

/// Advance `from` past any reserved region that would contain it **or the page after it**, since
/// growth always takes two consecutive pages: the new bitmap page and the page it serves.
///
/// `Err(region)` means an unbounded region blocks the way and there is nothing above it to reach.
///
/// **A free function so it can be tested.** Inline in `allocate`, this logic only runs after
/// BITS_PER_BITMAP (32736) pages have been handed out, so no reasonable test reached it - and a
/// mutant that replaced the advance with `break` survived the whole suite, silently placing a
/// bitmap page inside another store's region. Untestable-in-practice code is untested code.
fn advance_past_regions(regions: &[Region], from: u32) -> Result<u32, Region> {
    let mut at = from;
    // BOUNDED, not `loop`. Each step clears at least one region from a disjoint sorted list, so
    // `regions.len()` steps always suffice -- but that argument silently depends on `contains`
    // being HALF-OPEN. A mutant widening it to `page <= self.hi` made `at = r.hi` land back
    // inside the same region and the unbounded version spun forever, hanging the test rather
    // than failing it. A hang in production is worse than a wrong answer that refuses, so the
    // bound is structural here rather than an argument in a comment.
    for _ in 0..=regions.len() {
        match regions.iter().find(|r| r.contains(at) || r.contains(at.saturating_add(1))) {
            None => return Ok(at),
            Some(r) if r.hi == u32::MAX => return Err(*r),
            Some(r) => at = r.hi,
        }
    }
    // Unreachable while regions are disjoint and half-open. If it is ever reached the invariant
    // is broken, and the safe answer is to refuse to grow rather than hand out a page that may
    // belong to another store.
    Err(regions
        .iter()
        .find(|r| r.contains(at) || r.contains(at.saturating_add(1)))
        .copied()
        .unwrap_or(Region { name: "unknown", lo: at, hi: at.saturating_add(1) }))
}

impl Region {
    fn contains(&self, page: u32) -> bool {
        page >= self.lo && page < self.hi
    }
    /// Half-open overlap. `lo < self.hi && self.lo < hi` is the whole test; writing it out because
    /// getting it wrong by one produces regions that touch and are accepted as disjoint.
    fn overlaps(&self, lo: u32, hi: u32) -> bool {
        lo < self.hi && self.lo < hi
    }
}

impl DiskManager{

    /// Open a database on a real file. The production entry point, and behaviourally identical to
    /// what it replaced: `File` implements [`Storage`] by forwarding to the same free `pwrite`/`pread`
    /// helpers this function used to call directly.
    // writes page 0 if it isn't already written with data. bytes 0-3 are header(pointer to next bitmap page), 4 is 1, rest is 0
    pub fn new(file: File) -> Result<Self, FerroError>{
        Self::with_storage(Arc::new(file))
    }

    /// Open a database on any [`Storage`]. This is the injection point: hand it a
    /// [`crate::storage::sim::SimStorage`] and a write can be made to tear at a chosen byte.
    pub fn with_storage(storage: Arc<dyn Storage>) -> Result<Self, FerroError>{
        let file_len = match storage.len().map_err(|e| FerroError::Io(e.to_string())){
            Ok(l) => l,
            Err(e) => return Err(FerroError::Io(e.to_string()))
        };
        let next_page_id: u32;
        if file_len == 0{
            let mut first_page_bitmap = [0u8; PAGE_SIZE];
            first_page_bitmap[4] = 1;
            let mut total_written = 0;
            while total_written < PAGE_SIZE{
                let written = match storage.pwrite(&first_page_bitmap[total_written..], total_written as u64) {
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
            next_page_id = (file_len/PAGE_SIZE as u64) as u32;
        }
        Ok(DiskManager {
            next_page_id: AtomicU32::new(next_page_id),
            storage,
            bitmap_lock: Mutex::new(()),
            regions: Mutex::new(Vec::new()),
        })
    }
    
    pub fn write(&self, page_id: u32, data: &[u8]) -> Result<(), FerroError>{
        if data.len() != PAGE_SIZE{
            return Err(FerroError::Io(format!("Page length must be: {}", PAGE_SIZE)))
        }
        let offset:u64 = page_id as u64* PAGE_SIZE as u64;
        let mut total_wrote = 0;
        while total_wrote < PAGE_SIZE {
            let written = match self.storage.pwrite(&data[total_wrote..], offset + total_wrote as u64){
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
            let size = match self.storage.pread(&mut buffer[total_read..], offset + total_read as u64) {
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
        if let Some(r) = self.region_containing(page_id) {
            return Err(FerroError::Io(format!(
                "page {} is inside the reserved '{}' region [{}, {}) and is not this allocator's \
                 to free",
                page_id, r.name, r.lo, r.hi
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
        self.reserve_region("branch arena", base, u32::MAX)
    }

    /// Reserve `[lo, hi)` for another allocator. Refuses any overlap with an existing region or
    /// with a page this allocator has already handed out.
    ///
    /// Re-registering a region **identical** to an existing one is accepted: that is a restart, and
    /// the branch module's harness does it deliberately (`fresh_store()` passes
    /// `self.store.base_page()`).
    ///
    /// **Blind spot, stated deliberately and unchanged from the single-floor version.** This
    /// separates regions from *this* allocator and from each other. It says nothing about two
    /// stores sharing one region: a second store registered over the identical range is accepted,
    /// and if both are live they will hand out the same pages. The branch harness relies on that to
    /// simulate a restart, and it is safe there only because the first store is no longer written
    /// through. Nothing here enforces that.
    ///
    /// **What this still does not fix.** An unbounded region (`hi == u32::MAX`) leaves the ordinary
    /// allocator boxed in below it, which is the README's "table space is fixed at creation". The
    /// table makes a bounded arena *representable*; it does not by itself make the arena bounded.
    /// Claiming otherwise would be the overclaim this project keeps making — see `SCALE-DESIGN.md`
    /// D1 addendum 2, which was corrected on exactly this point.
    pub fn reserve_region(&self, name: &'static str, lo: u32, hi: u32) -> Result<(), FerroError> {
        if lo >= hi {
            return Err(FerroError::Io(format!(
                "region '{name}' is empty or inverted: [{lo}, {hi})"
            )));
        }
        let _guard = self.bitmap_lock.lock().unwrap();
        let mut regions = self.regions.lock().unwrap();

        if regions.iter().any(|r| r.lo == lo && r.hi == hi) {
            return Ok(());
        }
        if let Some(clash) = regions.iter().find(|r| r.overlaps(lo, hi)) {
            return Err(FerroError::Io(format!(
                "region '{}' [{}, {}) overlaps the existing '{}' [{}, {}); regions must be \
                 disjoint, and moving an existing one would put pages it already owns back into \
                 circulation",
                name, lo, hi, clash.name, clash.lo, clash.hi
            )));
        }
        // NO high-water check here, deliberately, and it was tried. Refusing a region that starts
        // below the high-water mark looks like "make the dangerous state unrepresentable", and it
        // is the wrong layer for it: it makes `deallocate`'s own region guard UNREACHABLE. If no
        // page can ever be both allocated by this allocator and inside a region, that guard is
        // dead code — and three buffer-pool tests exist precisely to prove it is not
        // (`a_refused_delete_leaves_the_page_intact_in_the_pool` and its siblings build exactly
        // that state on purpose). Adding the check here broke all three, which is the tests doing
        // their job.
        //
        // The layering that already exists is the right one: `ArenaPageStore::new` refuses a base
        // below `high_water()` so production never constructs the overlap, and `deallocate`
        // refuses the page anyway if something ever does. Defence in depth, not a single gate.
        regions.push(Region { name, lo, hi });
        regions.sort_unstable_by_key(|r| r.lo);
        Ok(())
    }

    fn region_containing(&self, page: u32) -> Option<Region> {
        self.regions.lock().unwrap().iter().find(|r| r.contains(page)).copied()
    }

    /// Every reserved region, lowest first. Diagnostics and tests.
    pub fn reserved_regions(&self) -> Vec<(&'static str, u32, u32)> {
        self.regions.lock().unwrap().iter().map(|r| (r.name, r.lo, r.hi)).collect()
    }

    /// First page this allocator must not touch, or `u32::MAX` when nothing is reserved.
    ///
    /// With a region table this is the **lowest** reserved page, not a description of everything
    /// above it: pages between two bounded regions belong to this allocator and `allocate` hands
    /// them out. Kept under its old name because its old meaning — "the first page that is not
    /// mine" — is still exactly true, and every caller uses it for exactly that.
    pub fn arena_floor(&self) -> u32 {
        self.regions.lock().unwrap().first().map(|r| r.lo).unwrap_or(u32::MAX)
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
        // Snapshot once. There are a handful of regions at most, and re-locking inside the bit
        // loop would take the regions lock millions of times per scan.
        let regions: Vec<Region> = self.regions.lock().unwrap().clone();
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
                            // A reserved region's pages are not tracked in this bitmap, so their
                            // bits read as free from page 0 and handing one out would alias a
                            // page another store is already writing.
                            //
                            // SKIP rather than refuse. With one unbounded region the two are
                            // identical, because everything above it is reserved — which is why
                            // the single-floor version could get away with refusing. With a
                            // BOUNDED region the pages above it are this allocator's, and
                            // stopping at the first reserved page would strand every one of them.
                            if let Some(r) = regions.iter().find(|r| r.contains(candidate)) {
                                if r.hi == u32::MAX {
                                    return Err(Self::arena_floor_exhausted(
                                        "no free page below",
                                        r.lo,
                                    ));
                                }
                                continue;
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
            // Two pages are about to be taken: the new bitmap page and the page it serves. BOTH
            // must fall outside every reserved region. `advance_past_regions` is a free function
            // precisely so this can be tested without first handing out 32736 pages.
            let raw_base = self
                .scan_bitmap_high_water()?
                .max(self.next_page_id.load(Ordering::SeqCst));
            let grow_base = match advance_past_regions(&regions, raw_base) {
                Ok(b) => b,
                Err(r) => {
                    return Err(Self::arena_floor_exhausted("cannot grow the bitmap past", r.lo));
                }
            };
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
        self.storage.sync_all().map_err(|e| FerroError::Io(e.to_string()))
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
    use super::{advance_past_regions, Region, BITS_PER_BITMAP, PAGE_SIZE};
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

    fn fresh_dm(tag: &str) -> (DiskManager, TempDir) {
        use std::fs::OpenOptions;
        let temp_dir = TempDir::new().unwrap();
        let f = OpenOptions::new().read(true).write(true).create(true)
            .open(temp_dir.path().join(format!("{tag}.db"))).unwrap();
        (DiskManager::new(f).unwrap(), temp_dir)
    }

    /// The thing a single floor could not represent, and said so in its own refusal message:
    /// "a second region at {base} cannot be represented by a single floor".
    #[test]
    fn two_bounded_regions_coexist_which_a_single_floor_refused() {
        let (dm, _d) = fresh_dm("two-regions");
        dm.reserve_region("catalog", 300, 400).expect("first region");
        dm.reserve_region("arena", 100, 200).expect("second, lower region");
        assert_eq!(
            dm.reserved_regions(),
            vec![("arena", 100, 200), ("catalog", 300, 400)],
            "regions must be kept sorted by lo, whatever order they were registered in"
        );
        assert_eq!(dm.arena_floor(), 100, "the floor is the LOWEST reserved page");
    }

    /// **The island property.** With one floor, the first reserved page ended the allocator's
    /// world; every page above it was stranded. A bounded region is skipped instead.
    #[test]
    fn allocate_skips_a_bounded_region_and_keeps_handing_out_pages_above_it() {
        let (dm, _d) = fresh_dm("island");
        dm.reserve_region("catalog", 10, 20).expect("reserve");

        let mut handed = Vec::new();
        for _ in 0..24 {
            handed.push(dm.allocate().expect("allocate must not stop at the region"));
        }
        for p in &handed {
            assert!(
                !(*p >= 10 && *p < 20),
                "handed out page {p}, which is inside the reserved region [10, 20) - that page \
                 belongs to another store and two writers now share it"
            );
        }
        assert!(
            handed.iter().any(|p| *p >= 20),
            "no page above the region was ever handed out, so the region was treated as a floor \
             and everything above it is stranded: {handed:?}"
        );
    }

    /// An unbounded region has nothing above it to reach, so it must still stop the allocator -
    /// and still explain that the knob will not help.
    #[test]
    fn an_unbounded_region_still_stops_the_allocator_and_names_the_knob() {
        let (dm, _d) = fresh_dm("unbounded");
        dm.reserve_region("branch arena", 12, u32::MAX).expect("reserve");
        let mut err = None;
        for _ in 0..64 {
            if let Err(e) = dm.allocate() {
                err = Some(e);
                break;
            }
        }
        let msg = format!("{}", err.expect("an unbounded region must eventually refuse"));
        assert!(msg.contains("will NOT move it"), "lost the fixed-for-this-database sentence: {msg}");
    }

    #[test]
    fn overlapping_regions_are_refused_and_the_message_names_both() {
        let (dm, _d) = fresh_dm("overlap");
        dm.reserve_region("catalog", 100, 200).expect("first");
        let err = dm.reserve_region("arena", 150, 250).expect_err("an overlap must be refused");
        let msg = format!("{err}");
        assert!(msg.contains("catalog"), "does not name the region already there: {msg}");
        assert!(msg.contains("arena"), "does not name the region being refused: {msg}");
        assert_eq!(dm.reserved_regions().len(), 1, "a refused reservation must not be recorded");

        // Touching but disjoint is NOT an overlap. Getting this wrong by one is the classic error.
        dm.reserve_region("adjacent", 200, 300).expect("[200,300) does not overlap [100,200)");
    }

    /// A restart re-registers the identical region. Refusing that would break every reopen.
    #[test]
    fn re_registering_an_identical_region_is_a_restart_not_an_error() {
        let (dm, _d) = fresh_dm("restart");
        dm.reserve_region("arena", 64, u32::MAX).expect("first");
        dm.reserve_region("arena", 64, u32::MAX).expect("identical re-registration is a restart");
        assert_eq!(dm.reserved_regions().len(), 1, "the restart duplicated the region");
    }

    /// Between two regions is this allocator's own space, so freeing there must work. Under a
    /// single floor there was no "between".
    #[test]
    fn a_page_between_two_regions_belongs_to_this_allocator_and_can_be_freed() {
        let (dm, _d) = fresh_dm("between");
        dm.reserve_region("low", 4, 8).expect("low");
        dm.reserve_region("high", 16, 32).expect("high");

        let mut between = None;
        for _ in 0..24 {
            let p = dm.allocate().expect("allocate");
            if p >= 8 && p < 16 {
                between = Some(p);
            }
            assert!(!(p >= 4 && p < 8) && !(p >= 16 && p < 32), "handed out reserved page {p}");
        }
        let p = between.expect("no page was handed out between the two regions");
        dm.deallocate(p).expect("a page between regions is this allocator's to free");

        // ...while a page inside the HIGHER region is still refused, and the error names it.
        let err = dm.deallocate(20).expect_err("page 20 is inside the 'high' region");
        assert!(format!("{err}").contains("high"), "the refusal does not name which region: {err}");
    }

    /// Growth takes TWO consecutive pages, so a region containing either one blocks it. This is
    /// the path that only runs after 32736 allocations; testing the pure function is how it gets
    /// covered at all. A mutant replacing the advance with `break` survived the entire suite
    /// before this existed.
    #[test]
    fn growth_advances_past_bounded_regions_and_stops_at_an_unbounded_one() {
        let r = |name, lo, hi| Region { name, lo, hi };

        assert_eq!(advance_past_regions(&[], 5), Ok(5), "no regions, no advance");

        let one = [r("a", 10, 20)];
        assert_eq!(advance_past_regions(&one, 5), Ok(5), "below the region, untouched");
        assert_eq!(advance_past_regions(&one, 10), Ok(20), "inside the region, advanced past it");
        assert_eq!(
            advance_past_regions(&one, 9),
            Ok(20),
            "page 9 is free but page 10 is not, and growth needs BOTH - this is the off-by-one \
             that puts a bitmap page inside another store's region"
        );
        assert_eq!(advance_past_regions(&one, 20), Ok(20), "at hi is outside, half-open");

        // Two adjacent regions must be walked in one call, not one hop per call.
        let two = [r("a", 10, 20), r("b", 20, 30)];
        assert_eq!(advance_past_regions(&two, 12), Ok(30), "must clear BOTH regions");

        let unbounded = [r("arena", 10, u32::MAX)];
        assert_eq!(advance_past_regions(&unbounded, 5), Ok(5), "below it is still fine");
        assert_eq!(
            advance_past_regions(&unbounded, 12),
            Err(r("arena", 10, u32::MAX)),
            "nothing is above an unbounded region, so this must refuse rather than loop"
        );
        // A bounded region below an unbounded one: advancing past the first lands in the second.
        let mixed = [r("cat", 10, 20), r("arena", 20, u32::MAX)];
        assert_eq!(advance_past_regions(&mixed, 15), Err(r("arena", 20, u32::MAX)));
    }
}
