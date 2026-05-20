use std::io::{Read, Write, Seek};
use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use crate::error::FerroError;
use std::os::windows::fs::FileExt;

const PAGE_SIZE: u64 = 4096;
pub struct DiskManager {
    pub next_page_id: AtomicU32,
    pub file: File,
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
            let mut first_page_bitmap = [0u8; PAGE_SIZE as usize];
            first_page_bitmap[4] = 1;
            let mut total_written = 0;
            while total_written < PAGE_SIZE as usize {
                let written = match file.seek_write(&first_page_bitmap[total_written..], total_written as u64) {
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
            file
        })
    }
    // need to set it to allocated too
    pub fn write(&self, page_id: u64, data: &[u8]) -> Result<(), FerroError>{
        if data.len() != PAGE_SIZE as usize{
            return Err(FerroError::Io(format!("Page length must be: {}", PAGE_SIZE)))
        }
        let offset = page_id * PAGE_SIZE;
        let mut total_wrote = 0;
        while total_wrote < PAGE_SIZE as usize {
            let written = match self.file.seek_write(&data[total_wrote..], offset + total_wrote as u64) {
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
    pub fn read(&self, page_id: u64) -> Result<[u8; PAGE_SIZE as usize], FerroError>{
        let mut buffer = [0u8; PAGE_SIZE as usize];
        let offset = page_id * PAGE_SIZE;
        let mut total_read = 0;
        while total_read < PAGE_SIZE as usize {
            let size = match self.file.seek_read(&mut buffer[total_read..], offset + total_read as u64) {
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
    pub fn deallocate(&self, page_id: u64) -> Result<(), FerroError>{
        let mut page_bitmap = self.read(0)?;
        let byte_index = (page_id/8) as usize + 4;
        let bit_index = page_id % 8;
        page_bitmap[byte_index] &= !(1 << bit_index);
        match self.write(0, &page_bitmap) {
            Ok(_) => (),
            Err(e) => return Err(e)
        };
        Ok(())
    }

    //first checks bitmap if there is a free page if not, then give it next_page_id and increment it
    pub fn allocate(&self) -> Result<u64, FerroError>{
        let mut page_bitmap = self.read(0)?;
        for byte_index in 4..PAGE_SIZE as usize {
            for bit_index in 0..8 {
                if page_bitmap[byte_index] & (1<<bit_index) == 0 {
                    let page_id = (byte_index - 4) * 8 + bit_index;
                    page_bitmap[byte_index] |= 1 << bit_index;
                    self.write(0, &page_bitmap)?;
                    return Ok(page_id as u64)
                }
            }
        }
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let byte_index = (page_id/8) as usize + 4;
        let bit_index = (page_id % 8) as usize;
        page_bitmap[byte_index] |= 1 << bit_index;
        self.write(0, &page_bitmap)?;
        return Ok(self.next_page_id.fetch_add(1, Ordering::SeqCst) as u64)
    }
}



