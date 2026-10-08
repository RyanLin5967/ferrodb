use std::sync::Arc;

use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{heap_page::Page, heap_scanner::HeapScanner, page_directory::PageDirectory, tuple::Tuple}, wal::txn::TxnManager};
use crate::storage::heap_page::{SLOT_ENTRY_SIZE, HEADER_SIZE, MAX_TUPLE_SIZE};
use crate::storage::disk_manager::PAGE_SIZE;

/// The page space of the largest tuple a page holds must fit the `u16` a directory entry records
/// free space in, or [`HeapFileManager::space_needed`] could not answer in the unit
/// `find_page_with_space` compares. Checked at compile time, so a `PAGE_SIZE` change that broke it
/// fails the build rather than an insert (D257).
const _: () = assert!(
    MAX_TUPLE_SIZE + SLOT_ENTRY_SIZE <= u16::MAX as usize,
    "a tuple's page space no longer fits a u16; PageDirectoryEntry::free_space must widen"
);

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct RecordId {
    pub page_id: u32,
    pub slot_num: u16,
}
pub struct HeapFileManager {
    pub buffer_pool_manager: Arc<BufferPoolManager>,
    pub first_directory_page_id: u32,
    pub txn: Option<Arc<TxnManager>>,
    pub txn_id: u64,
}

impl HeapFileManager {
    
    pub fn new(buffer_pool_manager: Arc<BufferPoolManager>) -> Result<Self, FerroError> {
        let dir_page_id = buffer_pool_manager.new_page()?;
        buffer_pool_manager.unpin_page(dir_page_id, false);
        let frame_i = buffer_pool_manager.fetch_page(dir_page_id)?;
        let mut frame = buffer_pool_manager.frame_write(frame_i);
        let empty_dir = PageDirectory::new(dir_page_id);
        frame.data = empty_dir.serialize();
        drop(frame);
        buffer_pool_manager.unpin_page(dir_page_id, true);
        Ok(HeapFileManager { buffer_pool_manager, first_directory_page_id: dir_page_id, txn: None, txn_id: 0})
    }

    // fetches page, reads slot, unpins
    pub fn read(&self, record_id: RecordId) -> Result<Tuple, FerroError>{
        let pin = self.buffer_pool_manager.pin(record_id.page_id)?;
        let frame = pin.read();
        let page = Page::deserialize(frame.data)?;
        let tuple = page.read(record_id.slot_num as usize)?;
        drop(frame);
        pin.unpin(false);
        Ok(tuple)
    }

    /// A page that can hold a tuple of `tuple_len` bytes, making one if no existing page can.
    ///
    /// Split out of [`Self::insert`] so a caller can obtain the destination **before** it gives up
    /// the space it is replacing. This is the only part of an insert that can fail for a reason
    /// unrelated to the tuple: `new_page` goes to `DiskManager::allocate`, which refuses once the
    /// table region below the copy-on-write arena floor is full — a real limit, fixed when the
    /// database is created (`cli::DEFAULT_ARENA_HEADROOM`), not a test contrivance. See the
    /// relocation branch of [`Self::update`] for what that cost before this split existed.
    ///
    /// **A tuple no page can hold is refused first**, before the directory is read and before
    /// anything is allocated: see [`Self::space_needed`] (D257).
    pub fn find_or_make_page(&self, tuple_len: usize) -> Result<u32, FerroError> {
        let needed = Self::space_needed(tuple_len)?;
        if let Some(id) = self.find_page_with_space(needed)? {
            return Ok(id);
        }
        self.add_empty_page()
    }

    /// The page space a tuple of `tuple_len` bytes occupies (its bytes plus the slot entry that
    /// addresses it), or the refusal if no page can ever hold it. **D257.**
    ///
    /// The one place the heap turns a tuple's length into page space, and the one place it
    /// refuses a tuple for its size. [`Self::find_or_make_page`] asks it before anything else,
    /// which covers [`Self::insert`] and the relocation branch of [`Self::update`];
    /// `execution::update` asks it before it writes the old version to the time-travel heap.
    ///
    /// Before it, nothing on the insert path compared the length with [`MAX_TUPLE_SIZE`]. No
    /// directory entry ever claims more than `PAGE_SIZE - HEADER_SIZE` free, so for a longer
    /// tuple the search always failed and `add_empty_page` allocated and listed a page, unlogged,
    /// before `insert_into` found that the tuple did not fit. Every refused attempt left one more
    /// page in the file and the directory, for good. The length also went through
    /// `tuple_len as u16 + SLOT_ENTRY_SIZE as u16`, which panicked (debug) or wrapped (release) at
    /// 65532..=65535 and truncated from 65536 on, where the small figure it produced was answered
    /// by any page. Pinned by `tests/d257_insert_bound.rs`.
    ///
    /// `Constraint` rather than `NotEnoughSpace`: a row too wide is the statement's fault, not a
    /// server fault, and that is how every other row-width refusal here reports it
    /// (`catalog::alter`). So this build answers a client with SQLSTATE 23000
    /// (`pgwire::sqlstate_of` maps `Constraint` there; `NotEnoughSpace` would be XX000, which a
    /// pool may treat as a reason to drop the connection). **PostgreSQL itself reports an
    /// over-size row as 54000, `program_limit_exceeded`.** Matching it would take a variant or a
    /// mapping shared by every row-width refusal, `catalog::alter`'s included, so that is a change
    /// to all of them together, not to this one.
    pub fn space_needed(tuple_len: usize) -> Result<u16, FerroError> {
        if tuple_len > MAX_TUPLE_SIZE {
            return Err(FerroError::Constraint(format!(
                "a row stored as {tuple_len} bytes cannot be written: a row must fit in one \
                 {PAGE_SIZE}-byte page, which holds at most {MAX_TUPLE_SIZE} bytes of row, and \
                 a row does not span pages. Nothing was allocated or written for it; shorten \
                 the row, usually its longest VARCHAR."
            )));
        }
        // Cannot fail while the assertion at the top of this file holds. Checked anyway, so a
        // wrong answer is an error and never a smaller number.
        u16::try_from(tuple_len + SLOT_ENTRY_SIZE).map_err(|_| {
            FerroError::Internal(format!(
                "the page space of a {tuple_len}-byte tuple does not fit a u16, which the \
                 compile-time bound on MAX_TUPLE_SIZE ({MAX_TUPLE_SIZE}) rules out"
            ))
        })
    }

    /// Allocate one empty data page and record it in the directory. **Always allocates.**
    ///
    /// Separate from [`Self::find_or_make_page`] because "give me somewhere to put this tuple" and
    /// "give me one more page" are different requests, and conflating them does not terminate: an
    /// empty page has exactly `PAGE_SIZE - HEADER_SIZE` free, which satisfies the search for the
    /// largest tuple a page can hold, so a loop calling `find_or_make_page` to grow the heap finds
    /// the page it added last time and adds nothing. That is not hypothetical — it hung
    /// `integration_alter_column::a_lookup_by_key_still_finds_a_row_the_rewrite_moved`, a 200-row
    /// ALTER needing eleven pages, for eighteen minutes with no output.
    fn add_empty_page(&self) -> Result<u32, FerroError> {
        let new_page_id = self.buffer_pool_manager.new_page()?;
        self.buffer_pool_manager.unpin_page(new_page_id, false);
        let pin = self.buffer_pool_manager.pin(new_page_id)?;
        let mut frame = pin.write();
        let empty_page = Page::empty(new_page_id);
        frame.data = empty_page.serialize()?;
        drop(frame);
        pin.unpin(true);
        let free_space = (PAGE_SIZE - HEADER_SIZE) as u16;
        self.add_to_directory(new_page_id, free_space)?;
        Ok(new_page_id)
    }

    /// Free space this heap holds across every data page, as the page directory reports it.
    ///
    /// Used by [`Self::reserve_free_space`]; it is the directory's own numbers rather than a
    /// re-derivation from the pages, because the directory is what `find_page_with_space` consults.
    pub fn free_space(&self) -> Result<usize, FerroError> {
        let mut total = 0usize;
        let mut dir_page_id = self.first_directory_page_id;
        loop {
            let frame_i = self.buffer_pool_manager.fetch_page(dir_page_id)?;
            let frame = self.buffer_pool_manager.frames[frame_i].read().unwrap();
            let dir = PageDirectory::deserialize(frame.data);
            drop(frame);
            self.buffer_pool_manager.unpin_page(dir_page_id, false);
            total += dir.entries.iter().map(|e| e.free_space as usize).sum::<usize>();
            if dir.next_page_directory == 0 {
                return Ok(total);
            }
            dir_page_id = dir.next_page_directory;
        }
    }

    /// Add empty pages until the heap holds `bytes` of free space, returning how many were added.
    ///
    /// For a caller that is about to relocate many tuples and cannot survive discovering half way
    /// through that the page allocator is exhausted — `catalog::alter::rewrite_heap`, which is
    /// unlogged and therefore has no undo. Failing here is harmless: the only thing it can leave
    /// behind is empty pages the heap will use for its next insert, and not one tuple has moved.
    ///
    /// **What it does not promise, one.** Free space in the aggregate is not free space in one page:
    /// the pages counted may each hold less than the next tuple needs, in which case an insert still
    /// allocates. It converts the common exhaustion — no room anywhere — into a refusal before the
    /// first write, and leaves fragmentation to the reserve-before-delete order in [`Self::update`].
    ///
    /// An adversarial pass tried to reach that fragmentation case through `ADD COLUMN` and a retype
    /// and could not, with a reason worth keeping: those alterations grow *every* row by the same
    /// amount, so a page whose own rows need more space than it has free is exactly a page that
    /// contributed that shortfall to the aggregate — per-page free space cannot be short while the
    /// total is sufficient. It did **not** test whether a hole left by `DELETE` (whose bytes are
    /// never reclaimed, since nothing compacts a heap page) breaks that argument, so it is narrowed
    /// rather than closed.
    ///
    /// **What it does not promise, two.** A reservation that adds pages and *then* fails leaves
    /// those pages in the heap. They are empty and in the page directory, so the next insert uses
    /// them; no row, value, shape, index answer or feed record differs, and a reader cannot tell.
    /// The file is one page per added page longer, which is a durable difference produced by a
    /// statement that reported failure, and it is left that way on purpose: giving them back means
    /// removing directory entries and calling `DiskManager::deallocate`, and a page freed while a
    /// directory still lists it is handed to another table — real corruption traded for a leak that
    /// costs nothing and is reused. Pinned by
    /// `integration_alter_refusal_safety::a_refusal_after_a_partial_reservation_leaves_only_empty_pages`.
    pub fn reserve_free_space(&self, bytes: usize) -> Result<usize, FerroError> {
        // `free_space` walks the whole directory chain, so it is read ONCE and then advanced by
        // what each added page is worth. Re-reading it per iteration made growing the heap by n
        // pages cost n directory walks, which is quadratic in the size of the table being altered.
        let mut free = self.free_space()?;
        let per_page = PAGE_SIZE - HEADER_SIZE;
        let mut added = 0usize;
        while free < bytes {
            self.add_empty_page()?;
            free += per_page;
            added += 1;
        }
        Ok(added)
    }

    // finds page with space (via page dir), fetch through buffer pool, insert tuple, update directory, unpin
    pub fn insert(&self, tuple: Tuple) -> Result<RecordId, FerroError>{
        let page_id = self.find_or_make_page(tuple.data.len())?;
        self.insert_into(page_id, tuple)
    }

    /// Write `tuple` into `page_id`, which the caller has already established can hold it.
    ///
    /// The pin is a `PagePin`, so every `?` after it releases it (D237). The one a directory entry
    /// that overstates a page's free space reaches, `page.insert`, is forced by
    /// `tests/d257_insert_bound.rs::an_insert_refused_inside_its_page_releases_the_pin`.
    fn insert_into(&self, page_id: u32, tuple: Tuple) -> Result<RecordId, FerroError> {
        let pin = self.buffer_pool_manager.pin(page_id)?;
        let mut frame = pin.write();
        let mut page = Page::deserialize(frame.data)?;
        let tuple_bytes = tuple.data.clone();
        let slot_num = page.insert(tuple)?;
        if let Some(txn) = &self.txn {
            let lsn = txn.log_insert(self.txn_id, self.first_directory_page_id, page_id, slot_num, &tuple_bytes)?;
            page.lsn = lsn;
        }
        frame.data = page.serialize()?;
        drop(frame);
        pin.unpin(true);
        self.update_directory_entry(page_id, page.get_free_space_end() - page.get_free_space_start())?;
        Ok(RecordId::new(page_id,slot_num))
    }

    // fetch page, try in place page update first, if page returns NotEnoughSpace, delete from this page and insert elsewhere
    pub fn update(&self, record_id: RecordId, new_tuple: Tuple) -> Result<RecordId, FerroError> {
        let pin = self.buffer_pool_manager.pin(record_id.page_id)?;
        let mut frame = pin.write();
        let mut page = Page::deserialize(frame.data)?;
        let old_bytes = page.read(record_id.slot_num as usize)?.data;
        let new_bytes = new_tuple.data.clone();
        let clone = Tuple::new(new_tuple.data.clone());
        match page.update(record_id.slot_num as usize, new_tuple){
            Ok(_) => {
                if let Some(txn) = &self.txn {
                    let lsn = txn.log_update(self.txn_id, self.first_directory_page_id, record_id.page_id, record_id.slot_num, &old_bytes, &new_bytes)?;
                    page.lsn = lsn;
                }
                frame.data = page.serialize()?;
                drop(frame);
                pin.unpin(true);
                self.update_directory_entry(record_id.page_id, page.get_free_space_end() - page.get_free_space_start())?;
                return Ok(record_id)
            },
            Err(FerroError::NotEnoughSpace) => {
                // **Decided before the delete below, while the row is still on its page — as far
                // as the directory can be trusted.**
                //
                // The relocation deletes the slot and unpins the page DIRTY before the insert that
                // is supposed to replace it, so past `page.delete` the row exists nowhere: if the
                // insert then fails, the `?` unwinds with the row already gone and no caller can
                // tell that from an update that simply did not happen. `find_or_make_page` settles
                // two of the ways that insert can fail before the delete:
                //
                // - **Can any page hold this tuple?** `space_needed` refuses one past
                //   `MAX_TUPLE_SIZE` before touching anything. This used to be a guard of its own
                //   here, on the same predicate; D257 needed the refusal on INSERT too and gave it
                //   one definition inside `find_or_make_page`, which this branch already called
                //   before the delete. A second copy here would test nothing the first does not,
                //   and would hide a broken first copy from every test.
                // - **Is there a page at all?** `DiskManager::allocate` refuses once the table
                //   region below the copy-on-write arena floor is full, and that floor is fixed
                //   when the database is created, so it is an ordinary end-state rather than an
                //   exotic one. Measured when the delete came first: a 41-row single-page heap with
                //   the floor reached lost row 1 outright to an `ALTER TABLE ... ADD COLUMN` that
                //   reported failure, and left the primary index pointing at the deleted slot —
                //   durably, across checkpoint, flush and a reopen.
                //
                // ⛔ **It does NOT settle the third, and the insert below CAN still fail after the
                // delete (ledger D261, open).** When `find_or_make_page` hands back a FRESH page,
                // `insert_into` cannot fail for want of space: a fresh page holds any tuple up to
                // `MAX_TUPLE_SIZE`. When it hands back an EXISTING page, it chose it by the
                // directory's free-space figure, and a rollback can leave that figure overstating
                // the page: runtime undo never writes the directory, so an aborted relocation
                // (`undo_delete` splices the old bytes back) or a rolled-back in-place shrink
                // (`undo_update` regrows the slot) leaves the entry claiming space the page no
                // longer has. `insert_into` then refuses with `NotEnoughSpace` after the delete.
                // On a logged update the abort restores the row, and the client sees a refusal for
                // a row that fits. On an unlogged one (`catalog::alter::commit_rewrite`, `txn:
                // None`) the row is lost and the primary index points at the deleted slot. The fix
                // (the insert's space checked on the real page, under its latch, before the
                // delete) is D261's, not this change's; D257's refusal above never covered it and
                // neither did the guard it replaced.
                //
                // This is not redundant with the callers' own checks. A logged update survives the
                // old behaviour by accident: the delete is a WAL record, so the statement's abort
                // undoes it. Every caller that opens a heap through `HeapFileManager::open` gets
                // `txn: None` — `catalog::alter::rewrite_heap` is one — and for those there is no
                // undo record and no recovery: the row is simply gone. Measured before the size
                // refusal existed: an unlogged `update` with a 4124-byte tuple took the heap from
                // one live tuple to zero and left the slot reading `SlotDeleted`.
                //
                // Reordering the delete after the insert was the alternative and it is worse: the
                // insert needs the frame lock this function is holding (deadlock unless the lock is
                // dropped and the page re-fetched), and it changes the order of the WAL records a
                // logged update writes, which is the order recovery's undo path reads them in.
                //
                // `Page::update` returned `NotEnoughSpace` without touching the page, so nothing
                // has changed yet. The lock is released here because `find_or_make_page` fetches
                // directory pages and may allocate, and holding this frame's write lock across
                // that would deadlock the moment the allocator handed back a page whose frame is
                // this one.
                drop(frame);
                pin.unpin(false);
                let dest = self.find_or_make_page(new_bytes.len())?;

                let pin = self.buffer_pool_manager.pin(record_id.page_id)?;
                let mut frame = pin.write();
                let mut page = Page::deserialize(frame.data)?;
                page.delete(record_id.slot_num as usize)?;
                if let Some(txn) = &self.txn {
                    let lsn = txn.log_delete(self.txn_id, self.first_directory_page_id, record_id.page_id, record_id.slot_num, &old_bytes)?;
                    page.lsn = lsn;
                }
                frame.data = page.serialize()?;
                drop(frame);
                pin.unpin(true);
                let new_record_id = self.insert_into(dest, clone)?;
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
    //
    // **Test-only, by construction (D203).** A physical delete logs a `HeapDelete` of whatever the
    // slot holds, and the logical decoder reads a `HeapDelete` of a LIVE row as the first half of a
    // relocating UPDATE (`replication::logical`, module doc point 3): the relocation arm of
    // `update` above is the only production writer of one. A production caller of this function
    // that deleted a live row and then inserted the same key in one transaction would be reported
    // to every change-feed consumer as an UPDATE. So it does not compile outside this crate's unit
    // tests; a production need for a physical delete has to be classified against the decoder first.
    #[cfg(test)]
    pub fn delete(&self, record_id: RecordId) -> Result<(), FerroError> {
        let pin = self.buffer_pool_manager.pin(record_id.page_id)?;
        let mut frame = pin.write();
        let mut page = Page::deserialize(frame.data)?;
        let old_bytes = page.read(record_id.slot_num as usize)?.data;
        page.delete(record_id.slot_num as usize)?;
        if let Some(txn) = &self.txn {
            let lsn = txn.log_delete(self.txn_id, self.first_directory_page_id, record_id.page_id, record_id.slot_num, &old_bytes)?;
            page.lsn = lsn;
        }
        frame.data = page.serialize()?;
        drop(frame);
        pin.unpin(true);
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
        HeapFileManager { buffer_pool_manager, first_directory_page_id, txn: None, txn_id: 0 }
    }

    pub fn add_to_directory(&self, new_page_id: u32, free_space: u16) -> Result<(), FerroError> {
        let mut dir_page_id = self.first_directory_page_id;
        loop {
            let pin = self.buffer_pool_manager.pin(dir_page_id)?;
            let mut frame = pin.write();
            let mut dir = PageDirectory::deserialize(frame.data);

            match dir.add_entry(new_page_id, free_space) {
                Ok(_) => {
                    frame.data = dir.serialize();
                    drop(frame);
                    pin.unpin(true);
                    return Ok(());
                }
                Err(FerroError::NotEnoughSpace) => {
                    if dir.next_page_directory == 0 {
                        drop(frame);                                            // release before allocating
                        let new_dir_id = self.buffer_pool_manager.new_page()?;

                        dir.next_page_directory = new_dir_id;                   // dir is a local copy, still valid
                        {
                            let mut frame = pin.write();
                            frame.data = dir.serialize();
                        }
                        pin.unpin(true);
                        let new_pin = self.buffer_pool_manager.pin(new_dir_id)?;
                        let mut new_dir = PageDirectory::new(new_dir_id);
                        new_dir.add_entry(new_page_id, free_space)?;
                        {
                            let mut new_frame = new_pin.write();
                            new_frame.data = new_dir.serialize();
                        }
                        new_pin.unpin(true);
                        return Ok(());
                    }
                    drop(frame);
                    pin.unpin(false);
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
            let mut frame = self.buffer_pool_manager.frame_write(frame_i);
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

    /// Free every page this heap owns, or none of them. See [`Self::page_ids`].
    pub fn free_all(&self) -> Result<(), FerroError> {
        self.buffer_pool_manager.free_pages(&self.page_ids()?)
    }

    /// Every page this heap owns: each data page a directory lists, then that directory page, down
    /// the chain. Collected in full before anything is freed (D237), so `Catalog::drop_table` can
    /// refuse a table having freed none of it.
    pub fn page_ids(&self) -> Result<Vec<u32>, FerroError> {
        let mut out = Vec::new();
        let mut dir_page_id = self.first_directory_page_id;
        while dir_page_id != 0 {
            let dir = {
                let pin = self.buffer_pool_manager.pin(dir_page_id)?;
                let frame = pin.read();
                PageDirectory::deserialize(frame.data)
            };
            out.extend(dir.entries.iter().map(|e| e.page_id));
            out.push(dir_page_id);
            dir_page_id = dir.next_page_directory;
        }
        Ok(out)
    }

    pub fn set_transaction(&mut self, txn: Arc<TxnManager>, txn_id: u64) {
        self.txn = Some(txn);
        self.txn_id = txn_id;
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

    fn setup() -> (HeapFileManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("heap.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&path).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(dm));
        (HeapFileManager::new(bpm).unwrap(), dir)
    }

    fn test_schema() -> Schema {
        Schema::new(vec![Column::new("id".into(), DataType::Integer, false), Column::new("name".into(), DataType::Varchar(50), false)])
    }

    #[test]
    fn test_insert_and_read() {
        let (hfm, _dir) = setup();
        let schema = test_schema();
        let values = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let tuple = Tuple::serialize(&values, &schema, 0).unwrap();
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
        let (hfm, _dir) = setup();
        let schema = test_schema();
        let values = vec![Value::Integer(3), Value::Varchar("old".into())];
        let tuple = Tuple::serialize(&values, &schema, 0).unwrap();
        let rid = hfm.insert(tuple).unwrap();

        let new_values = vec![Value::Integer(3), Value::Varchar("new".into())];
        let new_tuple = Tuple::serialize(&new_values, &schema, 0).unwrap();
        let new_rid = hfm.update(rid, new_tuple).unwrap();
        let result = hfm.read(new_rid).unwrap();
        let decoded = result.deserialize(&schema).unwrap();
        assert_eq!(decoded, new_values);
    }

    #[test]
    fn test_scan() {
        let (hfm, _dir) = setup();
        let schema = test_schema();
        for i in 0..10 {
            let values = vec![Value::Integer(i), Value::Varchar(format!("row{}", i)) ];
            let tuple = Tuple::serialize(&values, &schema, 0).unwrap();
            hfm.insert(tuple).unwrap();
        }
        let tuples: Result<Vec<_>, _> = hfm.scan().collect();
        assert_eq!(tuples.unwrap().len(), 10);
    }

    #[test]
    fn test_multiple_pages() {
        let (hfm, _dir) = setup();
        let schema = Schema::new(vec![
            Column::new("data".into(), DataType::Varchar(200), false),
        ]);
        for _ in 0..100 {
            let values = vec![Value::Varchar("x".repeat(200))];
            let tuple = Tuple::serialize(&values, &schema, 0).unwrap();
            hfm.insert(tuple).unwrap();
        }
        
        let tuples: Result<Vec<_>, _> = hfm.scan().collect();
        assert_eq!(tuples.unwrap().len(), 100);
    }

    #[test]
    fn test_insert_delete_scan() {
        let (hfm, _dir) = setup();
        let schema = test_schema();
        let mut rids = Vec::new();
        for i in 0..10 {
            let values = vec![Value::Integer(i), Value::Varchar(format!("row{}", i))];
            let tuple = Tuple::serialize(&values, &schema, 0).unwrap();
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