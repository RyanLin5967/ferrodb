use crate::{error::FerroError, storage::tuple::Tuple};


pub struct RecordId {
    page_id: u32,
    slot_num: u16,
}
pub struct HeapFileManager {
    pub first_directory_page_id: u32,
}

impl HeapFileManager {

    // look up page, calls page.read
    pub fn read(record_id: RecordId) -> Result<Tuple, FerroError>{
        Ok(Tuple::new(Vec::new()))
    }

    // finds page with free space (using page dir), inserts, updates page directory
    pub fn insert(tuple: Tuple) -> Result<RecordId, FerroError>{
        Ok(RecordId::new(1,1))
    }

    // try in place page update first, if page returns NotEnoughSpace, delete from this page and insert elsewhere
    pub fn update(record_id: RecordId, new_tuple: Tuple) -> Result<(), FerroError> {
        Ok(())
    }

    // pub fn scan() -> Result<impl Iterator<Item = Tuple>, FerroError> {
        
    // }

    // calls page.delete, updates page directory's entry
    pub fn delete(record_id: RecordId) -> Result<(), FerroError> {
        Ok(())
    }

}

impl RecordId {
    pub fn new(page_id: u32, slot_num: u16) -> Self{
        RecordId { page_id, slot_num }
    }
}