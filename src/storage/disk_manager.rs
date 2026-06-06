use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use crate::error::FerroError;

#[cfg(windows)]
use std::os::windows::fs::FileExt;
#[cfg(unix)]
use std::os::unix::fs::FileExt;

pub const PAGE_SIZE: usize = 4096;
const BITS_PER_BITMAP: u32 = (PAGE_SIZE as u32 - 4) *8;
pub struct DiskManager {
    pub next_page_id: AtomicU32,
    pub file: File,
    bitmap_lock: Mutex<()>
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
            bitmap_lock: Mutex::new(())
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

    //first checks bitmap if there is a free page if not, then give it next_page_id and increment it
    pub fn allocate(&self) -> Result<u32, FerroError>{
        let _guard = self.bitmap_lock.lock().unwrap();
        let mut current_bitmap_id = 0;
        let mut global_offset = 0;
        loop {
            let mut page_bitmap = self.read(current_bitmap_id)?;

            for byte_index in 4..PAGE_SIZE {
                if page_bitmap[byte_index] != 0xFF {
                    for bit_index in 0..8 {
                        if page_bitmap[byte_index] & (1<<bit_index) == 0 {
                            page_bitmap[byte_index] |= 1 << bit_index;
                            self.write(current_bitmap_id, &page_bitmap)?;
                            let page_id: usize = (byte_index - 4) * 8 + bit_index;
                            return Ok(global_offset + page_id as u32);
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
}

pub fn pwrite(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(windows)]
    { file.seek_write(buf, offset)}
    #[cfg(unix)]
    { file.write_at(buf, offset)}
}

pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(windows)]
    { file.seek_read(buf, offset)}
    #[cfg(unix)]
    { file.read_at(buf, offset)}
}
#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::storage::disk_manager::DiskManager;
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

