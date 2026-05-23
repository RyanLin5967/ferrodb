use crate::{error::FerroError, storage::disk_manager::PAGE_SIZE};
pub struct SlotEntry {
    offset: u16,
    length: u16
}

pub struct Page {
    page_type: u8,
    page_id: u32,
    lsn: u64,
    checksum: u32,
    slot_arr: Vec<SlotEntry>,
    tuples: Vec<u8>
}

const HEADER_SIZE: usize = 23;
const SLOT_ENTRY_SIZE: usize = 4;
// HEADER LAYOUT: |page_type (u8, 1)|page_id (u32, 4)|num_slots (u16, 2)|
// free_space_start (u16, 2)|free_space_end (u16, 2)|lsn (u64, 8)|checksum (u32, 4)
impl Page {
    // header has num slots, slot array, free space pointer start and end, page id, lsn, checksum, 
    pub fn serialize(&self) -> Result<[u8; PAGE_SIZE], FerroError> {
        let mut buffer = [0u8; PAGE_SIZE];
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
        // header
        buffer[0..1].copy_from_slice(&self.page_type.to_be_bytes()); //page type
        buffer[1..5].copy_from_slice(&self.page_id.to_be_bytes()); // page id
        buffer[5..7].copy_from_slice(&(self.slot_arr.len() as u16).to_be_bytes()); // num slots
        buffer[7..9].copy_from_slice(&((23+self.slot_arr.len()*SLOT_ENTRY_SIZE) as u16).to_be_bytes());// free space start
        buffer[9..11].copy_from_slice(&min_offset.to_be_bytes()); // free space end
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
        let num_slots = u16::from_be_bytes(bytes[5..7].try_into().unwrap());
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
}

impl SlotEntry {
    pub fn new(offset: u16, length: u16) -> Self{
        SlotEntry {offset, length}
    }
}
