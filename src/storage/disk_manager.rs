use std::io::{Read, Write, Seek};
use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::error::FerroError;
use std::os::windows::fs::FileExt;

const PAGE_SIZE: u64 = 4096;

pub struct DiskManager {
    pub next_page_id: AtomicU32,
    pub file: File,
}

impl DiskManager{
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
}



