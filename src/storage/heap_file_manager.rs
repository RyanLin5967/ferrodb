use std::sync::Arc;

use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{heap_page::Page, heap_scanner::HeapScanner, page_directory::PageDirectory, tuple::Tuple}};
use crate::storage::heap_page::{SLOT_ENTRY_SIZE, HEADER_SIZE};
use crate::storage::disk_manager::PAGE_SIZE;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord)]
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
        buffer_pool_manager.unpin_page(dir_page_id, false);
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
                self.buffer_pool_manager.unpin_page(new_page_id, false);
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
    pub fn scan(&self) -> HeapScanner {
        HeapScanner{
            buffer_pool: self.buffer_pool_manager.clone(),
            dir_page_id: self.first_directory_page_id,
            data_page_ids: Vec::new(),
            data_idx: 0,
            current_page: None,
            slot_idx: 0,
        }
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
                        drop(frame);                                            // release before allocating
                        let new_dir_id = self.buffer_pool_manager.new_page()?;

                        dir.next_page_directory = new_dir_id;                   // dir is a local copy, still valid
                        {
                            let mut frame = self.buffer_pool_manager.frames[frame_i].write().unwrap();
                            frame.data = dir.serialize();
                        }
                        self.buffer_pool_manager.unpin_page(dir_page_id, true);
                        let new_frame_i = self.buffer_pool_manager.fetch_page(new_dir_id)?;
                        let mut new_dir = PageDirectory::new(new_dir_id);
                        new_dir.add_entry(new_page_id, free_space)?;
                        {
                            let mut new_frame = self.buffer_pool_manager.frames[new_frame_i].write().unwrap();
                            new_frame.data = new_dir.serialize();
                        }
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

    pub fn free_all(&self) -> Result<(), FerroError> {
        let mut dir_page_id = self.first_directory_page_id;
        while dir_page_id != 0 {
            let frame_i = self.buffer_pool_manager.fetch_page(dir_page_id)?;
            let dir = {
                let frame = self.buffer_pool_manager.frames[frame_i].read().unwrap();
                PageDirectory::deserialize(frame.data)
            };
            self.buffer_pool_manager.unpin_page(dir_page_id, false);
            for entry in &dir.entries {
                self.buffer_pool_manager.free_page(entry.page_id)?;
            }
            let next = dir.next_page_directory;
            self.buffer_pool_manager.free_page(dir_page_id)?;
            dir_page_id = next;
        }
        Ok(())
    }
}

impl RecordId {
    pub fn new(page_id: u32, slot_num: u16) -> Self{
        RecordId { page_id, slot_num }
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::sync::Arc;
    use std::fs::OpenOptions;
    use crate::storage::disk_manager::DiskManager;
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::storage::tuple::Tuple;
    use crate::catalog::column::{Column, DataType, Value};
    use crate::catalog::schema::Schema;

    fn setup() -> HeapFileManager {
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open("test.db").unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(dm));
        HeapFileManager::new(bpm).unwrap()
    }

    fn test_schema() -> Schema {
        Schema::new(vec![Column::new("id".into(), DataType::Integer, false), Column::new("name".into(), DataType::Varchar(50), false)])
    }

    #[test]
    fn test_insert_and_read() {
        let hfm = setup();
        let schema = test_schema();
        let values = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let tuple = Tuple::serialize(&values, &schema).unwrap();
        let rid = hfm.insert(tuple).unwrap();
        let result = hfm.read(rid).unwrap();
        let decoded = result.deserialize(&schema).unwrap();
        assert_eq!(decoded, values);
    }

    // #[test]
    // fn test_delete() {
    //     let hfm = setup();
    //     let schema = test_schema();
    //     let values = vec![Value::Integer(2), Value::Varchar("world".into())];
    //     let tuple = Tuple::serialize(&values, &schema).unwrap();
    //     let rid = hfm.insert(tuple).unwrap();
    //     hfm.delete(rid).unwrap();
    //     // should fail
    // }

    #[test]
    fn test_update_in_place() {
        let hfm = setup();
        let schema = test_schema();
        let values = vec![Value::Integer(3), Value::Varchar("old".into())];
        let tuple = Tuple::serialize(&values, &schema).unwrap();
        let rid = hfm.insert(tuple).unwrap();

        let new_values = vec![Value::Integer(3), Value::Varchar("new".into())];
        let new_tuple = Tuple::serialize(&new_values, &schema).unwrap();
        let new_rid = hfm.update(rid, new_tuple).unwrap();
        let result = hfm.read(new_rid).unwrap();
        let decoded = result.deserialize(&schema).unwrap();
        assert_eq!(decoded, new_values);
    }

    #[test]
    fn test_scan() {
        let hfm = setup();
        let schema = test_schema();
        for i in 0..10 {
            let values = vec![Value::Integer(i), Value::Varchar(format!("row{}", i)) ];
            let tuple = Tuple::serialize(&values, &schema).unwrap();
            hfm.insert(tuple).unwrap();
        }
        let tuples: Result<Vec<_>, _> = hfm.scan().collect();
        assert_eq!(tuples.unwrap().len(), 10);
    }

    #[test]
    fn test_multiple_pages() {
        let hfm = setup();
        let schema = Schema::new(vec![
            Column::new("data".into(), DataType::Varchar(200), false),
        ]);
        for _ in 0..100 {
            let values = vec![Value::Varchar("x".repeat(200))];
            let tuple = Tuple::serialize(&values, &schema).unwrap();
            hfm.insert(tuple).unwrap();
        }
        
        let tuples: Result<Vec<_>, _> = hfm.scan().collect();
        assert_eq!(tuples.unwrap().len(), 100);
    }

    #[test]
    fn test_insert_delete_scan() {
        let hfm = setup();
        let schema = test_schema();
        let mut rids = Vec::new();
        for i in 0..10 {
            let values = vec![Value::Integer(i), Value::Varchar(format!("row{}", i))];
            let tuple = Tuple::serialize(&values, &schema).unwrap();
            rids.push(hfm.insert(tuple).unwrap());
        }
        // delete every other row
        for i in (0..10).step_by(2) {
            hfm.delete(rids.remove(i / 2)).unwrap(); // adjust index since we're removing
        }
        let tuples: Result<Vec<_>, _> = hfm.scan().collect();
        assert_eq!(tuples.unwrap().len(), 5);
    }

}