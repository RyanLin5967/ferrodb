use std::sync::Arc;

use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{heap_file_manager::RecordId, heap_page::Page, page_directory::PageDirectory, tuple::Tuple}};

pub struct HeapScanner {
    pub buffer_pool: Arc<BufferPoolManager>,
    pub dir_page_id: u32,
    pub data_page_ids: Vec<u32>,
    pub data_idx: usize,
    pub slot_idx: usize,
    pub current_page: Option<(u32, Page)>,
}

impl HeapScanner {
    pub fn load_page(&self, page_id: u32) -> Result<Page, FerroError> {
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let page = {
            let frame = self.buffer_pool.frames[frame_i].read().unwrap();
            Page::deserialize(frame.data)?
        };
        self.buffer_pool.unpin_page(page_id, false);
        Ok(page)
    }

    pub fn load_directory(&self, dir_page_id: u32) -> Result<(Vec<u32>, u32), FerroError> {
        let frame_i = self.buffer_pool.fetch_page(dir_page_id)?;
        let dir = {
            let frame = self.buffer_pool.frames[frame_i].read().unwrap();
            PageDirectory::deserialize(frame.data)
        };
        self.buffer_pool.unpin_page(dir_page_id, false);
        let ids = dir.entries.iter().map(|e| e.page_id).collect();
        Ok((ids, dir.next_page_directory))
    }
}

impl Iterator for HeapScanner {
    type Item = Result<(RecordId, Tuple), FerroError>;

    fn next(&mut self) -> Option<Self::Item>{
        loop {
            if let Some((page_id, page)) = &self.current_page {
                while self.slot_idx < page.slot_arr.len() {
                    let slot = self.slot_idx;
                    self.slot_idx += 1;
                    match page.read(slot) {
                        Ok(tuple) => return Some(Ok((RecordId::new(*page_id, slot as u16), tuple))),
                        Err(FerroError::SlotDeleted) => continue,
                        Err(e) => return Some(Err(e)),
                    }
                }
                self.current_page = None;
            }

            if self.data_idx < self.data_page_ids.len() {
                let page_id = self.data_page_ids[self.data_idx];
                self.data_idx += 1;
                match self.load_page(page_id) {
                    Ok(page) => {
                        self.current_page = Some((page_id, page));
                        self.slot_idx = 0;
                    }
                    Err(e) => return Some(Err(e)),
                }
                continue
            }

            if self.dir_page_id == 0 {
                return None;
            }

            match self.load_directory(self.dir_page_id) {
                Ok((ids, next)) => {
                    self.data_page_ids = ids;
                    self.data_idx = 0;
                    self.dir_page_id = next;
                }
                Err(e) => return Some(Err(e))
            }
        }
    }
}