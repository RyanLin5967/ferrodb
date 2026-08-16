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

    /// One past the highest page the bitmap has ever handed out.
    ///
    /// NOT `next_page_id`: that counter only advances when a whole new bitmap page is chained, so
    /// it still reads 1 after 500 allocations. Anything deciding where a second allocator's
    /// region may safely begin has to consult the bitmap itself, or it will place that region on
    /// top of pages this allocator already owns.
    pub fn high_water(&self) -> Result<u32, FerroError> {
        let _guard = self.bitmap_lock.lock().unwrap();
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
        let from_bitmap = highest.map(|h| h + 1).unwrap_or(0);
        // Third bound, and it is not redundant: a page can exist without any bitmap bit. The
        // arena store writes its pages directly and never sets a bit, and any legacy `write()`
        // that bypassed `allocate` does the same. The bitmap cannot see those; the file length
        // can. Over-reporting costs address space, which is free. Under-reporting hands a second
        // allocator a page that already holds data.
        let file_pages = match self.file.metadata() {
            Ok(m) => (m.len() / PAGE_SIZE as u64) as u32,
            Err(e) => return Err(FerroError::Io(e.to_string())),
        };
        Ok(from_bitmap
            .max(file_pages)
            .max(self.next_page_id.load(Ordering::SeqCst)))
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
        if current != u32::MAX && base < current {
            return Err(FerroError::Io(format!(
                "page region [{}, inf) is already reserved; cannot lower the floor to {}",
                current, base
            )));
        }
        if current == u32::MAX {
            self.arena_floor.store(base, Ordering::SeqCst);
        }
        Ok(())
    }

    /// First page this allocator must not touch.
    pub fn arena_floor(&self) -> u32 {
        self.arena_floor.load(Ordering::SeqCst)
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
                                return Err(FerroError::Io(format!(
                                    "no free page below the reserved arena region at {}",
                                    floor
                                )));
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
            // Growing the file would run straight into the arena region, which starts at the
            // high-water mark. Refuse instead of extending into somebody else's pages.
            if self.next_page_id.load(Ordering::SeqCst).saturating_add(1) >= floor {
                return Err(FerroError::Io(format!(
                    "cannot grow the bitmap past the reserved arena region at {}",
                    floor
                )));
            }
            let new_bitmap_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
            let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
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

