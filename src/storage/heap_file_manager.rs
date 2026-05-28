use std::sync::Arc;

use crate::{buffer::buffer_pool::{BufferPoolManager, Frame}, error::FerroError, storage::{disk_manager::DiskManager, heap_page::Page, page_directory::PageDirectory, tuple::Tuple}};
use crate::storage::heap_page::{SLOT_ENTRY_SIZE, HEADER_SIZE};
use crate::storage::disk_manager::PAGE_SIZE;
pub struct RecordId {
    pub page_id: u32,
    pub slot_num: u16,
}
pub struct HeapFileManager {
    pub buffer_pool_manager: Arc<BufferPoolManager>,
    pub first_directory_page_id: u32,
}

impl HeapFileManager {
    
    pub fn new(buffer_pool_manager: Arc<BufferPoolManager>) -> Result<Self, FerroError> {
        let dir_page_id = buffer_pool_manager.new_page()?;
        let frame_i = buffer_pool_manager.fetch_page(dir_page_id)?;
        let mut frame = buffer_pool_manager.frames[frame_i].write().unwrap();
        let empty_dir = PageDirectory::new(dir_page_id);
        frame.data = empty_dir.serialize();
        drop(frame);
        buffer_pool_manager.unpin_page(dir_page_id, true);
        Ok(HeapFileManager { buffer_pool_manager, first_directory_page_id: dir_page_id})
    }

    // fetches page, reads slot, unpins
    pub fn read(&self, record_id: RecordId) -> Result<Tuple, FerroError>{
        let frame_i = self.buffer_pool_manager.fetch_page(record_id.page_id)?;
        let frame = self.buffer_pool_manager.frames[frame_i].read().unwrap();
        let page = Page::deserialize(frame.data)?;
        let tuple = page.read(record_id.slot_num as usize)?;
        drop(frame);
        self.buffer_pool_manager.unpin_page(record_id.page_id, false);
        Ok(tuple)
    }

    // finds page with space (via page dir), fetch through buffer pool, insert tuple, update directory, unpin
    pub fn insert(&self, tuple: Tuple) -> Result<RecordId, FerroError>{
        let page_id = match self.find_page_with_space(tuple.data.len() as u16 + SLOT_ENTRY_SIZE as u16)?{ 
            Some(id) => id,
            None => {
                let new_page_id = self.buffer_pool_manager.new_page()?;
                let frame_i = self.buffer_pool_manager.fetch_page(new_page_id)?;
                let mut frame = self.buffer_pool_manager.frames[frame_i].write().unwrap();
                let empty_page = Page::empty(new_page_id);
                frame.data = empty_page.serialize()?;
                drop(frame);
                self.buffer_pool_manager.unpin_page(new_page_id, true);
                let free_space = (PAGE_SIZE - HEADER_SIZE) as u16;
                self.add_to_directory(new_page_id, free_space)?;
                new_page_id
            }
        };

        let frame_i = self.buffer_pool_manager.fetch_page(page_id)?;
        let mut frame = self.buffer_pool_manager.frames[frame_i].write().unwrap();
        let mut page = Page::deserialize(frame.data)?;
        let slot_num = page.insert(tuple)?;
        frame.data = page.serialize()?;
        drop(frame);
        self.buffer_pool_manager.unpin_page(page_id, true);
        self.update_directory_entry(page_id, page.get_free_space_end() - page.get_free_space_start())?;
        Ok(RecordId::new(page_id,slot_num))
    }

    // fetch page, try in place page update first, if page returns NotEnoughSpace, delete from this page and insert elsewhere
    pub fn update(&self, record_id: RecordId, new_tuple: Tuple) -> Result<RecordId, FerroError> {
        let frame_i = self.buffer_pool_manager.fetch_page(record_id.page_id)?;
        let mut frame = self.buffer_pool_manager.frames[frame_i].write().unwrap();
        let mut page = Page::deserialize(frame.data)?;
        let clone = Tuple::new(new_tuple.data.clone());
        match page.update(record_id.slot_num as usize, new_tuple){
            Ok(_) => {
                frame.data = page.serialize()?;
                drop(frame);
                self.buffer_pool_manager.unpin_page(record_id.page_id, true);
                self.update_directory_entry(record_id.page_id, page.get_free_space_end() - page.get_free_space_start())?;
                return Ok(record_id)
            },
            Err(FerroError::NotEnoughSpace) => {
                page.delete(record_id.slot_num as usize)?;
                frame.data = page.serialize()?;
                drop(frame);
                self.buffer_pool_manager.unpin_page(record_id.page_id, true);
                let new_record_id = self.insert(clone)?;
                self.update_directory_entry(record_id.page_id, page.get_free_space_end() - page.get_free_space_start())?;
                return Ok(new_record_id)
            }
            Err(e) => return Err(e)
        };
    }

    // iterate all dir entries, fetch page, collect tuples
    pub fn scan(&self) -> Result<Vec<Tuple>, FerroError> {
        let mut dir_page_id = self.first_directory_page_id;
        let mut tuples: Vec<Tuple> = Vec::new();

        loop {
            let frame_i = self.buffer_pool_manager.fetch_page(dir_page_id)?;
            let frame = self.buffer_pool_manager.frames[frame_i].read().unwrap();
            let dir = PageDirectory::deserialize(frame.data);
            drop(frame);

            self.buffer_pool_manager.unpin_page(dir_page_id, false);

            for entry in &dir.entries {
                let data_frame_i = self.buffer_pool_manager.fetch_page(entry.page_id)?;
                let data_frame = self.buffer_pool_manager.frames[data_frame_i].read().unwrap();
                let page = Page::deserialize(data_frame.data)?;
                drop(data_frame);

                for slot_num in 0..page.slot_arr.len() {
                    match page.read(slot_num) {
                        Ok(tuple) => tuples.push(tuple),
                        Err(FerroError::SlotDeleted) => continue,
                        Err(e) => {
                            self.buffer_pool_manager.unpin_page(entry.page_id, false);
                            return Err(e)
                        }
                    }
                }
                self.buffer_pool_manager.unpin_page(entry.page_id, false);
            }
            if dir.next_page_directory == 0 {
                break;
            }
            dir_page_id = dir.next_page_directory;
        }
        Ok(tuples)
    }

    // fetches page, mark slot dead, unpin
    pub fn delete(&self, record_id: RecordId) -> Result<(), FerroError> {
        let frame_i = self.buffer_pool_manager.fetch_page(record_id.page_id)?;
        let mut frame = self.buffer_pool_manager.frames[frame_i].write().unwrap();
        let mut page = Page::deserialize(frame.data)?;
        page.delete(record_id.slot_num as usize)?;
        frame.data = page.serialize()?;
        drop(frame);
        self.buffer_pool_manager.unpin_page(record_id.page_id, true);
        self.update_directory_entry(record_id.page_id, page.get_free_space_end() - page.get_free_space_start())?;
        Ok(())
    }

    pub fn find_page_with_space(&self, needed: u16) -> Result<Option<u32>, FerroError>{
        let mut dir_page_id = self.first_directory_page_id;

        loop {
            let frame_i = self.buffer_pool_manager.fetch_page(dir_page_id)?;
            let frame = self.buffer_pool_manager.frames[frame_i].read().unwrap();
            let dir = PageDirectory::deserialize(frame.data);
            drop(frame);
            self.buffer_pool_manager.unpin_page(dir_page_id, false);

            if let Some(id) = dir.find_page_with_space(needed){
                return Ok(Some(id))
            }

            if dir.next_page_directory == 0 {
                return Ok(None)
            }
            dir_page_id = dir.next_page_directory;
        }
    }

    pub fn open(first_directory_page_id: u32, buffer_pool_manager: Arc<BufferPoolManager>) -> Self{
        HeapFileManager { buffer_pool_manager, first_directory_page_id }
    }

    pub fn add_to_directory(&self, new_page_id: u32, free_space: u16) -> Result<(), FerroError> {
        let mut dir_page_id = self.first_directory_page_id;
        loop {
            let frame_i = self.buffer_pool_manager.fetch_page(dir_page_id)?;
            let mut frame = self.buffer_pool_manager.frames[frame_i].write().unwrap();
            let mut dir = PageDirectory::deserialize(frame.data);

            match dir.add_entry(new_page_id, free_space) {
                Ok(_) => {
                    frame.data = dir.serialize();
                    drop(frame);
                    self.buffer_pool_manager.unpin_page(dir_page_id, true);
                    return Ok(());
                }
                Err(FerroError::NotEnoughSpace) => {
                    if dir.next_page_directory == 0 {
                        let new_dir_id = self.buffer_pool_manager.new_page()?;
                        dir.next_page_directory = new_dir_id;
                        frame.data = dir.serialize();
                        drop(frame);
                        self.buffer_pool_manager.unpin_page(dir_page_id, true);
                        // add entry to new dir page
                        let new_frame_i = self.buffer_pool_manager.fetch_page(new_dir_id)?;
                        let mut new_frame = self.buffer_pool_manager.frames[new_frame_i].write().unwrap();
                        let mut new_dir = PageDirectory::new(new_dir_id);
                        new_dir.add_entry(new_page_id, free_space)?;
                        new_frame.data = new_dir.serialize();
                        drop(new_frame);
                        self.buffer_pool_manager.unpin_page(new_dir_id, true);
                        return Ok(());
                    }
                    drop(frame);
                    self.buffer_pool_manager.unpin_page(dir_page_id, false);
                    dir_page_id = dir.next_page_directory;
                }
                Err(e) => return Err(e)
            }
        }
    }

    pub fn update_directory_entry(&self, target_page_id: u32, new_free_space: u16) -> Result<(), FerroError> {
        let mut dir_page_id = self.first_directory_page_id;

        loop {
            let frame_i = self.buffer_pool_manager.fetch_page(dir_page_id)?;
            let mut frame = self.buffer_pool_manager.frames[frame_i].write().unwrap();
            let mut dir = PageDirectory::deserialize(frame.data);

            match dir.update_entry(target_page_id, new_free_space) {
                Ok(_) => {
                    frame.data = dir.serialize();
                    drop(frame);
                    self.buffer_pool_manager.unpin_page(dir_page_id, true);
                    return Ok(());
                }
                Err(_) => {
                    drop(frame);
                    self.buffer_pool_manager.unpin_page(dir_page_id, false);
                    if dir.next_page_directory == 0 {
                        return Err(FerroError::KeyNotFound);
                    }
                    dir_page_id = dir.next_page_directory;
                }
            }
        }
    }
}

impl RecordId {
    pub fn new(page_id: u32, slot_num: u16) -> Self{
        RecordId { page_id, slot_num }
    }
}