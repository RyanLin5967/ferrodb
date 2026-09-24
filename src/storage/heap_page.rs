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

/// The largest tuple any page can ever hold, and therefore the only size question that can be
/// answered *before* touching a page.
///
/// A fresh page's whole free span is `PAGE_SIZE - HEADER_SIZE`, and `insert` also has to fit the
/// one slot entry it appends, so `HEADER_SIZE + SLOT_ENTRY_SIZE` is what a tuple can never have.
/// `Page::insert` derives the same number at runtime from `get_free_space_start`/
/// `get_free_space_end`; this constant is not a second opinion about the layout but the same
/// arithmetic evaluated for an empty page, and
/// `integration_alter_refusal_safety::max_tuple_size_is_the_boundary_page_insert_actually_enforces`
/// pins the two against each other in both directions.
///
/// It exists because "will this tuple fit *somewhere*" has to be answerable while the heap is
/// still untouched. `HeapFileManager::update` relocates a tuple by deleting it and re-inserting
/// it, so a caller that discovers the answer from the failed insert discovers it one step too
/// late — see the refusal in `update` and the precheck in `catalog::alter::rewrite_heap`.
pub const MAX_TUPLE_SIZE: usize = PAGE_SIZE - HEADER_SIZE - SLOT_ENTRY_SIZE;

/// **D213: the flag marking a RETIRED slot.** It is bit 15 of the slot's `length`, which a real
/// length never sets: a tuple is at most [`MAX_TUPLE_SIZE`] (4069) bytes, so no page written
/// before this flag existed has it set.
///
/// A slot is in one of three states:
/// - **free**, `(0, 0)`: it holds nothing. The bytes it last held become free space once they fall
///   below the lowest occupied offset;
/// - **live**, `(offset, length)`: a tuple. `length` is the slot's CAPACITY, and a shrink keeps it
///   (see [`Page::update`]), so growing the tuple back to any earlier size happens in place;
/// - **retired**, `(offset, length | RETIRED)`: the tuple was deleted by a transaction that may
///   still roll back. It reads as deleted. Its bytes still count as occupied, so no insert can
///   take them, and the rollback puts the tuple back where it was ([`Page::restore_at`]). When the
///   transaction commits, the slot becomes free ([`Page::release`], logged as
///   `RecKind::HeapRelease`).
///
/// Together these make "an undo that can never find room" unrepresentable (ledger D213, the lead's
/// design): space freed by an uncommitted transaction is not reusable until it commits. InnoDB
/// keeps a delete-marked record until purge for the same reason, and PostgreSQL never frees a
/// tuple in place before vacuum. Before this, a relocation freed its old slot at once, and a
/// rollback had to write the tuple back at the front of the page. By then another transaction
/// could have committed rows into that room, and the rollback, or recovery's undo of the loser
/// at the next open, could never finish.
pub const RETIRED: u16 = 0x8000;
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
        // A retired slot reads as deleted, exactly as the slot a relocation freed did before D213:
        // for every reader, the row lives elsewhere or does not exist.
        if slot.is_free() || slot.is_retired() {
            return Err(FerroError::SlotDeleted);
        }
        let local_offset = slot.offset as usize - self.get_free_space_end() as usize;
        let raw_tuple = &self.tuples[local_offset..local_offset + slot.length as usize];
        Ok(Tuple::new(raw_tuple.to_vec()))
    }

    // update in place if fits, else delete and reinsert
    pub fn update(&mut self, slot_num: usize, new_tuple: Tuple) -> Result<(), FerroError>{
        let capacity = self.read(slot_num)?.data.len();
        let slot = &self.slot_arr[slot_num];
        let free_space_start = self.get_free_space_start();
        let free_space_end = self.get_free_space_end();
        let local_offset: usize = slot.offset as usize - free_space_end as usize;
        if new_tuple.data.len() <= capacity {
            // **D213: a shrink keeps the slot's capacity.** This set `length` to the new, shorter
            // size, and the remnant became garbage, since nothing compacts a page. Growing the tuple
            // back, which is what rolling back a shrink does, then needed a NEW copy at the front
            // of the page. Another transaction could commit rows into that room first, and then the
            // rollback could never finish. The tail is zero-filled rather than left holding old
            // bytes, so the page stays a function of its log: redo writes the same bytes. A reader
            // gets the padded bytes, and `Tuple::deserialize` reads only what the schema describes.
            let end = local_offset + new_tuple.data.len();
            self.tuples[local_offset..end].copy_from_slice(&new_tuple.data);
            self.tuples[end..local_offset + capacity].fill(0);
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

    /// **D213: delete a tuple so that a rollback can put it back where it was.** From here on the
    /// slot reads as deleted, and its bytes still count as occupied (see [`RETIRED`]). A LOGGED
    /// delete does this (`HeapFileManager`, and redo of a forward `HeapDelete`). [`Page::delete`]
    /// frees the slot at once, for a delete that nothing will roll back: undoing an insert, or an
    /// unlogged heap.
    pub fn retire(&mut self, slot_num: usize) -> Result<(), FerroError> {
        let slot = self.slot_arr.get(slot_num).ok_or_else(|| FerroError::Io(String::from("slot num out of bounds")))?;
        if slot.is_free() || slot.is_retired() {
            return Err(FerroError::SlotDeleted);
        }
        self.slot_arr[slot_num].length |= RETIRED;
        Ok(())
    }

    /// **D213: the transaction that retired this slot has committed, so its bytes are free now.**
    /// Logged as `RecKind::HeapRelease`, after the `Commit`.
    ///
    /// A LIVE slot is refused, because freeing it would lose a row: the page does not match the
    /// release being applied. A slot that is already free is left as it is, so a release applied
    /// twice changes nothing.
    pub fn release(&mut self, slot_num: usize) -> Result<(), FerroError> {
        let slot = self.slot_arr.get(slot_num).ok_or_else(|| FerroError::Io(String::from("slot num out of bounds")))?;
        if slot.is_free() {
            return Ok(());
        }
        if !slot.is_retired() {
            return Err(FerroError::Wal(format!(
                "slot {slot_num} holds a live tuple, not a retired one; the page does not match the \
                 release being applied"
            )));
        }
        self.slot_arr[slot_num] = SlotEntry::new(0, 0);
        Ok(())
    }

    pub fn compact(&mut self) {
        let mut buffer =[0u8; PAGE_SIZE];
        let mut offset: usize = PAGE_SIZE;
        let current_free_space_end = PAGE_SIZE - self.tuples.len();
        for i in 0..self.slot_arr.len() {
            // A retired slot keeps its bytes: its rollback restores them in place (D213).
            if !self.slot_arr[i].is_free() {
                let local_source = self.slot_arr[i].offset as usize - current_free_space_end;
                let length = self.slot_arr[i].span();
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
    pub fn get_free_space_end(&self) -> u16 {
        let mut min_offset = PAGE_SIZE as u16;
        for slot_entry in &self.slot_arr {
            // Only a FREE slot is skipped. A retired slot's bytes still count as occupied, so they
            // are neither dropped at the next serialise nor offered as free space (D213).
            if slot_entry.is_free() {
                continue;
            }
            if slot_entry.offset < min_offset {
                min_offset = slot_entry.offset;
            }
        }
        min_offset
    }

    pub fn restore_at(&mut self, slot_num: usize, data: &[u8]) -> Result<(), FerroError> {
        // Refuse rather than index out of bounds. Redo reaches here with a slot number that came
        // off the wire or out of a log, and a replica applying a WAL from a position it has no
        // base image for will legitimately ask for a slot this page has never had — that PANICKED
        // (`index out of bounds: the len is 0 but the index is 75`) instead of reporting that the
        // page and the record disagree.
        if slot_num >= self.slot_arr.len() {
            return Err(FerroError::Wal(format!(
                "slot {slot_num} is past this page's {} slot(s); the page does not match the \
                 record being applied, which usually means replay started without a base image \
                 of the page",
                self.slot_arr.len()
            )));
        }
        // **D213: a retired slot is restored where it was.** Its bytes were never free, so no other
        // transaction could have taken them, and this needs no room at all. Every logged delete
        // retires its slot, so this is the path every rollback of a relocation takes, and so is
        // recovery's redo of that rollback's CLR. The restored image cannot be longer than what the
        // slot kept, because the log recorded exactly those bytes; a longer one means the page and
        // the log disagree.
        let slot = &self.slot_arr[slot_num];
        if slot.is_retired() {
            let span = slot.span();
            if data.len() > span {
                return Err(FerroError::Wal(format!(
                    "slot {slot_num} was retired holding {span} bytes and the record restores {}; \
                     the page does not match the record being applied",
                    data.len()
                )));
            }
            let local = slot.offset as usize - (PAGE_SIZE - self.tuples.len());
            self.tuples[local..local + data.len()].copy_from_slice(data);
            self.tuples[local + data.len()..local + span].fill(0);
            self.slot_arr[slot_num].length = span as u16;
            return Ok(());
        }
        // A LIVE slot is refused. The splice below would point the slot at the restored copy and
        // abandon the tuple it holds, losing that row silently; no undo or redo restores over a
        // live tuple unless the page and the log disagree.
        if !slot.is_free() {
            return Err(FerroError::Wal(format!(
                "slot {slot_num} holds a live tuple, and restoring over it would lose that row; the \
                 page does not match the record being applied"
            )));
        }
        // **D210 — refuse rather than splice into room the page does not have.** This spliced
        // unconditionally and set `offset = PAGE_SIZE - tuples.len()`. Rolling back a relocation
        // onto a page that other inserts had filled in the meantime ran the tuple region down over
        // the slot array and the header, and `serialize` writes tuples LAST, so it silently
        // overwrote them: a corrupt page for every row on it, reported as success (the fresh-context
        // re-adversary, `frontier/d205_readversary.md`, `fffdc62`;
        // `tests/abort_that_cannot_finish.rs`). The room is measured where the splice will land, from
        // the start of the tuple region, not from a header field. The slot entry already exists, so
        // no slot space is needed.
        //
        // What REDO does with this: recovery reaches it only through the CLR of an undone relocation
        // (`redo_one`, a `HeapInsert` at an existing slot). Since D211 a CLR is logged only after its
        // undo has applied (`TxnManager::apply_then_log`), and redo replays the page's history in LSN
        // order, so it finds the same room the undo found. A refusal in redo therefore means the page
        // and the log disagree, and it surfaces as a recovery error instead of a page quietly
        // overwritten.
        //
        // Since D213 this splice is reached only for a FREE slot. That happens when a binary from
        // before D213 freed the slot at once and wrote the page back, so redo skips the delete by
        // the page's LSN and replays the rollback's CLR onto a free slot. Such a rollback can still
        // lack room, and it is still refused here.
        let tuples_start = PAGE_SIZE - self.tuples.len();
        let room = tuples_start.saturating_sub(self.get_free_space_start() as usize);
        if room < data.len() {
            return Err(FerroError::NotEnoughSpace);
        }
        self.tuples.splice(0..0, data.iter().copied());
        self.slot_arr[slot_num].offset = PAGE_SIZE as u16 - self.tuples.len() as u16;
        self.slot_arr[slot_num].length = data.len() as u16;
        Ok(())
    }
}

impl SlotEntry {
    pub fn new(offset: u16, length: u16) -> Self{
        SlotEntry {offset, length}
    }

    /// Holds nothing: `(0, 0)`.
    pub fn is_free(&self) -> bool {
        self.offset == 0 && self.length == 0
    }

    /// Deleted by a transaction that has not committed. See [`RETIRED`].
    pub fn is_retired(&self) -> bool {
        self.length & RETIRED != 0
    }

    /// The bytes this slot owns on the page, whether it is live or retired.
    pub fn span(&self) -> usize {
        (self.length & !RETIRED) as usize
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

    // ---- D213: space an uncommitted transaction freed is not reusable until it commits.

    use crate::error::FerroError;
    use crate::storage::heap_page::{RETIRED, SLOT_ENTRY_SIZE};
    use crate::storage::tuple::Tuple;

    /// A page holding one tuple per entry of `lens`, the i-th being `lens[i]` bytes of `i + 1`. The
    /// LAST one is the lowest on the page.
    fn page_with(lens: &[usize]) -> Page {
        let mut page = Page::empty(1);
        for (i, len) in lens.iter().enumerate() {
            page.insert(Tuple::new(vec![i as u8 + 1; *len])).unwrap();
        }
        page
    }

    /// The page as the next writer sees it: every operation starts from a deserialised frame, and
    /// that is where bytes below the lowest occupied offset are dropped.
    fn round_trip(page: &Page) -> Page {
        Page::deserialize(page.serialize().unwrap()).unwrap()
    }

    /// Free bytes between the slot array and the lowest occupied offset: what an insert can use.
    fn room(page: &Page) -> usize {
        page.get_free_space_end() as usize - page.get_free_space_start() as usize
    }

    #[test]
    fn a_shrink_keeps_the_slots_capacity_so_growing_back_is_in_place() {
        let mut page = page_with(&[100, 50]);
        let before = room(&page);
        page.update(0, Tuple::new(vec![9; 30])).unwrap();
        let mut page = round_trip(&page);
        assert_eq!(page.slot_arr[0].length, 100, "the shrink gave up the slot's capacity");
        let read = page.read(0).unwrap().data;
        assert_eq!(read[..30], [9u8; 30], "the shrink did not write the new bytes");
        assert!(read[30..].iter().all(|b| *b == 0), "the tail was not zero-filled, so redo would write other bytes");

        let offset = page.slot_arr[0].offset;
        page.update(0, Tuple::new(vec![1; 100])).unwrap();
        assert_eq!(page.slot_arr[0].offset, offset, "growing back within the capacity moved the tuple");
        assert_eq!(room(&page), before, "growing back within the capacity used free space");
        assert_eq!(page.read(0).unwrap().data, vec![1; 100]);
    }

    #[test]
    fn a_retired_slot_reads_as_deleted_keeps_its_bytes_and_is_restored_in_place() {
        // Slot 1 is the LOWEST tuple: the case where a plain delete freed its bytes at the next
        // serialise, so another transaction could take them before the rollback.
        let mut page = page_with(&[100, 50]);
        let before = room(&page);
        let old = page.read(1).unwrap().data;
        page.retire(1).unwrap();
        let mut page = round_trip(&page);
        assert!(matches!(page.read(1), Err(FerroError::SlotDeleted)), "a retired slot is readable");
        assert_eq!(room(&page), before, "retiring the lowest tuple freed its bytes before its transaction committed");

        // Other transactions fill every free byte, as they may.
        let fill = room(&page) - SLOT_ENTRY_SIZE;
        page.insert(Tuple::new(vec![7; fill])).unwrap();
        let mut page = round_trip(&page);
        assert_eq!(room(&page), 0, "premise: the page is not full");

        page.restore_at(1, &old).unwrap();
        let page = round_trip(&page);
        assert_eq!(page.read(1).unwrap().data, old, "the restore did not put the tuple back");
        assert_eq!(page.read(0).unwrap().data, vec![1; 100], "the restore damaged another tuple");
        assert_eq!(page.read(2).unwrap().data, vec![7; fill], "the restore damaged the tuple that filled the page");
    }

    #[test]
    fn releasing_a_retired_slot_frees_its_bytes() {
        let mut page = page_with(&[100, 50]);
        let before = room(&page);
        page.retire(1).unwrap();
        let mut page = round_trip(&page);
        page.release(1).unwrap();
        let page = round_trip(&page);
        assert!(page.slot_arr[1].is_free(), "the released slot is not free");
        assert_eq!(room(&page), before + 50, "releasing the lowest retired tuple did not free its bytes");
        assert_eq!(page.read(0).unwrap().data, vec![1; 100], "the release damaged the other tuple");
    }

    #[test]
    fn a_restore_into_a_free_slot_without_room_is_refused_and_changes_nothing() {
        // The D210 path `restore_at` keeps for a FREE slot (a page from a binary before D213).
        // Slot 0 is the top tuple, so freeing it frees no room.
        let mut page = page_with(&[100, 50]);
        let old = page.read(0).unwrap().data;
        page.delete(0).unwrap();
        let mut page = round_trip(&page);
        let fill = room(&page) - SLOT_ENTRY_SIZE - 10;
        page.insert(Tuple::new(vec![7; fill])).unwrap();
        let mut page = round_trip(&page);
        assert_eq!(room(&page), 10, "premise: the page does not have exactly 10 B free");

        let bytes = page.serialize().unwrap();
        assert!(
            matches!(page.restore_at(0, &old), Err(FerroError::NotEnoughSpace)),
            "a 100 B restore into 10 B was not refused"
        );
        assert_eq!(page.serialize().unwrap(), bytes, "a refused restore changed the page");
    }

    #[test]
    fn only_a_live_slot_is_retired_and_only_a_retired_one_is_released() {
        let mut page = page_with(&[100]);
        assert!(page.release(0).is_err(), "a LIVE slot was released, which would lose its row");
        page.retire(0).unwrap();
        assert_eq!(page.slot_arr[0].length, 100 | RETIRED);
        assert!(matches!(page.retire(0), Err(FerroError::SlotDeleted)), "a slot was retired twice");
        assert!(page.restore_at(0, &[1; 101]).is_err(), "a restore longer than the retired slot's bytes was accepted");
        let mut live = page_with(&[100, 50]);
        assert!(live.restore_at(0, &[1; 100]).is_err(), "a restore over a LIVE tuple was accepted, which loses that row");
        assert_eq!(live.read(0).unwrap().data, vec![1; 100], "a refused restore over a live tuple changed it");
        page.release(0).unwrap();
        page.release(0).unwrap();
        assert!(page.slot_arr[0].is_free(), "the release did not free the slot");
        assert!(page.retire(0).is_err(), "a free slot was retired");
        assert!(page.retire(5).is_err(), "a slot past the array was retired");
        assert!(page.release(5).is_err(), "a slot past the array was released");
    }
}