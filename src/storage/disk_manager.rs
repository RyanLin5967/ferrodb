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
        match self.file.seek_write(data, offset){
            Ok(_) => (),
            Err(e) => return Err(FerroError::Io(e.to_string()))
        };
        Ok(())
    }

    pub fn read(&self, page_id: u64) -> Result<[u8; 4096], FerroError>{
        let mut buffer = [0u8; PAGE_SIZE as usize];
        let offset = page_id * PAGE_SIZE;

        let size = match self.file.seek_read(&mut buffer, offset) {
            Ok(s) => s,
            Err(e) => return Err(FerroError::Io(e.to_string()))
        };
        Ok(buffer)
    }
    
}



