use crate::{error::FerroError, storage::{heap_file_manager::{HeapFileManager, RecordId}, tuple::Tuple}, wal::txn::ReadView};

pub fn resolve_visibility(view: &ReadView, tt_heap: &HeapFileManager, head: Tuple) -> Result<Option<Tuple>, FerroError> {
    let mut current = head;
    loop {
        let h = current.version_header()?;
        if !view.visible(&h) {
            return Ok(Some(current));
        }
        match h.prev() {
            Some((page, slot)) => current = tt_heap.read(RecordId::new(page, slot))?,
            None => return Ok(None),
        }
    }
}