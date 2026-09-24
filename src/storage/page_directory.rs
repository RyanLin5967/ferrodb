use crate::storage::disk_manager::PAGE_SIZE;
use crate::error::FerroError;

pub struct PageDirectory {
    pub page_id: u32,
    pub next_page_directory: u32,
    pub num_entries: u16,
    pub entries: Vec<PageDirectoryEntry>,
}
pub struct PageDirectoryEntry {
    pub page_id: u32,
    pub free_space: u16,
}

const HEADER_SIZE: usize = 11;
const ENTRY_SIZE: usize = 6;
const PAGE_TYPE_DIRECTORY: u8 = 1;
/// The most entries one directory page holds: `add_entry` refuses the next.
pub const MAX_ENTRIES: usize = (PAGE_SIZE - HEADER_SIZE) / ENTRY_SIZE;
// HEADER FORMAT: |page_type (u8, 1)|page_id (u32, 4)|next_page_directory (u32, 4)|num_entries (u16, 2)|
impl PageDirectory {

    pub fn new(page_id: u32) -> Self{
        PageDirectory {page_id, next_page_directory:0, num_entries:0, entries: Vec::new()}
    }
    // heap file manager will loop through all pages if first one doesn't have
    pub fn find_page_with_space(&self, needed: u16) -> Option<u32>{
        for entry in &self.entries {
            if entry.free_space >= needed {
                return Some(entry.page_id)
            }
        }
        None
    }

    pub fn update_entry(&mut self, page_id: u32, new_free_space: u16) -> Result<(), FerroError> {
        for i in 0..self.entries.len() {
            if self.entries[i].page_id == page_id {
                self.entries[i].free_space = new_free_space;
                return Ok(())
            }
        }
        Err(FerroError::Io(String::from("didn't find entry with given page id")))
    }

    pub fn add_entry(&mut self, page_id: u32, free_space: u16) -> Result<(), FerroError>{
        if HEADER_SIZE + (self.entries.len() + 1)*ENTRY_SIZE <= PAGE_SIZE {
            self.entries.push(PageDirectoryEntry::new(page_id, free_space));
            self.num_entries += 1;
            return Ok(());
        }
        Err(FerroError::NotEnoughSpace)
    }

    pub fn remove_entry(&mut self, page_id: u32) -> Result<(), FerroError>{
        let mut new_entries = Vec::new();
        let mut found = false;
        for i in 0..self.entries.len() {
            if self.entries[i].page_id == page_id {
                self.num_entries -= 1;
                found = true;
                continue;
            }
            new_entries.push(PageDirectoryEntry::new(self.entries[i].page_id, self.entries[i].free_space));
        }
        if found == false {
            return Err(FerroError::Io(String::from("didn't find entry with given page id")));
        }
        self.entries = new_entries;
        Ok(())
    }

    pub fn serialize(&self) -> [u8; PAGE_SIZE]{
        let mut buffer = [0u8; PAGE_SIZE];
        buffer[0] = PAGE_TYPE_DIRECTORY;
        buffer[1..5].copy_from_slice(&self.page_id.to_be_bytes());
        buffer[5..9].copy_from_slice(&self.next_page_directory.to_be_bytes());
        buffer[9..11].copy_from_slice(&(self.entries.len() as u16).to_be_bytes());

        for (i, entry) in self.entries.iter().enumerate() {
            let offset = HEADER_SIZE + i*ENTRY_SIZE;
            buffer[offset..offset + 4].copy_from_slice(&entry.page_id.to_be_bytes());
            buffer[offset + 4..offset + 6].copy_from_slice(&entry.free_space.to_be_bytes());
        }

        buffer
    }

    pub fn deserialize(bytes: [u8; PAGE_SIZE]) -> Self{
        let page_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
        let next_page_directory = u32::from_be_bytes(bytes[5..9].try_into().unwrap());
        let num_entries = u16::from_be_bytes(bytes[9..11].try_into().unwrap()) as usize;

        let mut entries = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let offset = HEADER_SIZE + i * ENTRY_SIZE;
            let entry_page_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            let free_space = u16::from_be_bytes(bytes[offset + 4..offset + 6].try_into().unwrap());
            entries.push(PageDirectoryEntry { page_id: entry_page_id, free_space });
        }

        PageDirectory {
            page_id,
            next_page_directory,
            num_entries: num_entries as u16,
            entries,
        }
    }

    /// [`PageDirectory::deserialize`] for bytes read after a crash, which must not be trusted to
    /// be a directory page (D229's recovery reset; the design's E6). `deserialize` checks neither
    /// the type byte nor the entry count, and a count past the page panics on a slice bound.
    ///
    /// `Ok(None)` for an all-zero page: the image a crash leaves of a directory page that was
    /// allocated (`new_page` zero-writes at once) and never written. It names nothing.
    pub fn parse_checked(bytes: [u8; PAGE_SIZE]) -> Result<Option<Self>, FerroError> {
        if bytes.iter().all(|b| *b == 0) {
            return Ok(None);
        }
        let stored_id = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
        if bytes[0] != PAGE_TYPE_DIRECTORY {
            return Err(FerroError::Io(format!(
                "page {stored_id} was reached as a heap directory page but its type byte is {}",
                bytes[0]
            )));
        }
        let num_entries = u16::from_be_bytes(bytes[9..11].try_into().unwrap()) as usize;
        if num_entries > MAX_ENTRIES {
            return Err(FerroError::Io(format!(
                "directory page {stored_id} claims {num_entries} entries; a page holds at most {MAX_ENTRIES}"
            )));
        }
        Ok(Some(Self::deserialize(bytes)))
    }

}

impl PageDirectoryEntry {
    pub fn new(page_id: u32, free_space: u16) -> Self{
        PageDirectoryEntry { page_id, free_space }
    }
}