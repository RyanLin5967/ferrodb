use crate::{error::FerroError, storage::{disk_manager::PAGE_SIZE, tuple::Tuple}};

#[derive(Debug, PartialEq)]
pub struct SlotEntry {
    pub offset: u16,
    pub length: u16
}

#[derive(Debug, PartialEq)]
pub struct Page {
    pub page_type: u8,
    pub page_id: u32,
    pub lsn: u64,
    pub checksum: u32,
    pub slot_arr: Vec<SlotEntry>,
    pub tuples: Vec<u8>
}

pub const HEADER_SIZE: usize = 23;
pub const SLOT_ENTRY_SIZE: usize = 4;
const HEAP_PAGE_TYPE:u8 = 0;
// HEADER LAYOUT: |page_type (u8, 1)|page_id (u32, 4)|num_slots (u16, 2)|
// free_space_start (u16, 2)|free_space_end (u16, 2)|lsn (u64, 8)|checksum (u32, 4)
impl Page {

    pub fn new(page_type: u8, page_id: u32, lsn: u64, checksum: u32, slot_arr: Vec<SlotEntry>, tuples: Vec<u8>) -> Self{
        Page { page_type, page_id, lsn, checksum, slot_arr, tuples }
    }

    pub fn empty(page_id: u32) -> Self{
        Page {page_type: HEAP_PAGE_TYPE, page_id, lsn: 0, checksum: 0, slot_arr: Vec::new(), tuples: Vec::new()}
    }
    // header has num slots, slot array, free space pointer start and end, page id, lsn, checksum, 
    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError> {
        let mut buffer = [0u8; PAGE_SIZE];
        // header
        buffer[0..1].copy_from_slice(&self.page_type.to_be_bytes()); //page type
        buffer[1..5].copy_from_slice(&self.page_id.to_be_bytes()); // page id
        buffer[5..7].copy_from_slice(&(self.slot_arr.len() as u16).to_be_bytes()); // num slots
        buffer[7..9].copy_from_slice(&((HEADER_SIZE+self.slot_arr.len()*SLOT_ENTRY_SIZE) as u16).to_be_bytes());// free space start
        buffer[9..11].copy_from_slice(&self.get_free_space_end().to_be_bytes()); // free space end
        buffer[11..19].copy_from_slice(&self.lsn.to_be_bytes()); // lsn
        buffer[19..23].copy_from_slice(&self.checksum.to_be_bytes()); // checksum
        
        // slot array
        for (i, slot_entry) in self.slot_arr.iter().enumerate() {
            buffer[HEADER_SIZE+i*SLOT_ENTRY_SIZE..HEADER_SIZE+i*SLOT_ENTRY_SIZE + SLOT_ENTRY_SIZE/2].copy_from_slice(&slot_entry.offset.to_be_bytes());
            buffer[HEADER_SIZE+i*SLOT_ENTRY_SIZE+SLOT_ENTRY_SIZE/2..HEADER_SIZE+i*SLOT_ENTRY_SIZE+SLOT_ENTRY_SIZE].copy_from_slice(&slot_entry.length.to_be_bytes());
        }
        
        //tuples
        buffer[PAGE_SIZE-self.tuples.len()..PAGE_SIZE].copy_from_slice(&self.tuples);
        
        Ok(buffer)
    }

    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        let page_type = u8::from_be_bytes(bytes[0..1].try_into().unwrap());
        let page_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
        let free_space_start = u16::from_be_bytes(bytes[7..9].try_into().unwrap());
        let free_space_end = u16::from_be_bytes(bytes[9..11].try_into().unwrap());
        let lsn = u64::from_be_bytes(bytes[11..19].try_into().unwrap());
        let checksum = u32::from_be_bytes(bytes[19..23].try_into().unwrap());

        let raw_slot_arr = &bytes[HEADER_SIZE..free_space_start as usize];
        let mut slot_arr = Vec::new();
        for slice in raw_slot_arr.chunks(SLOT_ENTRY_SIZE) {
            let offset = u16::from_be_bytes(slice[0..2].try_into().unwrap());
            let length = u16::from_be_bytes(slice[2..4].try_into().unwrap());
            slot_arr.push(SlotEntry::new(offset, length))
        }
        
        let tuples = bytes[free_space_end as usize..PAGE_SIZE].to_vec();
        Ok (Page { page_type, page_id, lsn, checksum, slot_arr, tuples })
    }

    // finds space in page, writes tuple bytes, add slot entry
    pub fn insert(&mut self, tuple: Tuple) -> Result<u16, FerroError>{
        let free_space_start = self.get_free_space_start();      
        let free_space_end = self.get_free_space_end();
        
        if (free_space_end as usize - free_space_start as usize) < tuple.data.len() + SLOT_ENTRY_SIZE{
            return Err(FerroError::NotEnoughSpace);
        } 
        self.tuples.splice(0..0, tuple.data.clone());
        self.slot_arr.push(SlotEntry::new(PAGE_SIZE as u16 -self.tuples.len() as u16, tuple.data.len() as u16));
        Ok((self.slot_arr.len() -1) as u16)
    }

    // deserialze tuple from slot number
    pub fn read(&self, slot_num: usize) -> Result<Tuple, FerroError>{
        if slot_num >= self.slot_arr.len() {
            return Err(FerroError::Io(String::from("slot num out of bounds")));
        }
        let slot = &self.slot_arr[slot_num];
        if slot.length == 0 && slot.offset == 0 { // marked as deleted
            return Err(FerroError::SlotDeleted);
        }
        let local_offset = slot.offset as usize - self.get_free_space_end() as usize;
        let raw_tuple = &self.tuples[local_offset..local_offset + slot.length as usize];
        Ok(Tuple::new(raw_tuple.to_vec()))
    }

    // update in place if fits, else delete and reinsert
    pub fn update(&mut self, slot_num: usize, new_tuple: Tuple) -> Result<(), FerroError>{
        let old_tuple = self.read(slot_num).unwrap();
        let slot = &self.slot_arr[slot_num];
        let free_space_start = self.get_free_space_start();
        let free_space_end = self.get_free_space_end();
        let local_offset: usize = slot.offset as usize - free_space_end as usize;
        if new_tuple.data.len() <= old_tuple.data.len() {
            self.tuples[local_offset..local_offset + new_tuple.data.len()].copy_from_slice(&new_tuple.data);
            self.slot_arr[slot_num].length = new_tuple.data.len() as u16;
        } else if free_space_end as usize - free_space_start as usize>= new_tuple.data.len(){
            self.slot_arr[slot_num].offset = free_space_end - new_tuple.data.len() as u16;
            self.slot_arr[slot_num].length = new_tuple.data.len() as u16;
            self.tuples.splice(0..0, new_tuple.data);
        } else {
            return Err(FerroError::NotEnoughSpace);
        }
        Ok(())
    }   

    // nullify slot entry
    pub fn delete(&mut self, slot_num: usize) -> Result<(), FerroError>{
        if slot_num >= self.slot_arr.len() {
            return Err(FerroError::Io(String::from("slot num out of bounds")));
        }
        self.slot_arr[slot_num].offset = 0;
        self.slot_arr[slot_num].length = 0;
        Ok(())
    }
    
    pub fn compact(&mut self) {
        let mut buffer =[0u8; PAGE_SIZE];
        let mut offset: usize = PAGE_SIZE;
        let current_free_space_end = PAGE_SIZE - self.tuples.len();
        for i in 0..self.slot_arr.len() {
            if self.slot_arr[i].offset != 0 && self.slot_arr[i].length != 0 {
                let local_source = self.slot_arr[i].offset as usize - current_free_space_end;
                let length = self.slot_arr[i].length as usize;
                let raw_tuple = &self.tuples[local_source..local_source + length];
                offset -= raw_tuple.len();
                self.slot_arr[i].offset = offset as u16;
                buffer[offset..offset + raw_tuple.len()].copy_from_slice(&raw_tuple);
            }
        }
        self.tuples = buffer[offset..PAGE_SIZE].to_vec();
    }
    pub fn get_free_space_start(&self) -> u16{
        return (HEADER_SIZE + self.slot_arr.len()*SLOT_ENTRY_SIZE) as u16;
    }
    pub fn get_free_space_end(&self) -> u16{
        let mut min_offset: u16;
        
        if self.slot_arr.len() == 0 {
            min_offset = PAGE_SIZE as u16;
        }else {
            min_offset = self.slot_arr[0].offset;
            for slot_entry in &self.slot_arr {
                if slot_entry.offset < min_offset {
                    min_offset = slot_entry.offset;
                }
            }
        }
        return min_offset;
    }
}

impl SlotEntry {
    pub fn new(offset: u16, length: u16) -> Self{
        SlotEntry {offset, length}
    }
}

#[cfg(test)]
mod tests {

    use crate::storage::heap_page::Page;
    use crate::storage::heap_page::SlotEntry;

    #[test]
    fn test_basic() {
        let page = Page::new(1,2,3,4,Vec::new(), Vec::new());
        let bytes = page.serialize().unwrap();
        let de_page = Page::deserialize(bytes).unwrap();

        assert_eq!(page, de_page);
        assert_eq!(de_page.slot_arr.len(), 0);
        assert_eq!(de_page.tuples.len(), 0);
    }

    #[test]
    fn test_exact_bytes() {
        let mut slots = Vec::new();
        slots.push(SlotEntry::new(4000, 10));

        let page = Page::new(
            7,
            0x12345678,
            0x1122334455667788,
            0xAABBCCDD,
            slots,
            vec![0; 10]
        );

        let bytes = page.serialize().unwrap();
        assert_eq!(bytes[0], 7); //page type
        assert_eq!(&bytes[1..5], &[0x12, 0x34, 0x56, 0x78]); // page_id
        assert_eq!(&bytes[5..7], &[0x00, 0x01]); //num_slots
        assert_eq!(&bytes[7..9], &[0x00, 27]); //free_space_start
        assert_eq!(&bytes[9..11], &[0x0F, 0xA0]); // free_space_end
        assert_eq!(&bytes[11..19], &[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]); // lsn
        assert_eq!(&bytes[19..23], &[0xAA, 0xBB, 0xCC, 0xDD]); // checksum
        assert_eq!(&bytes[23..27], &[0x0F, 0xA0, 0x00, 0x0A]); // slot entry
    }
}