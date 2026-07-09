use std::{collections::HashMap, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}};

use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{heap_page::Page, tuple::Tuple}, wal::log::{RecKind, WalManager}};

pub struct TxnManager {
    pub wal: Arc<WalManager>,
    pub bp: Arc<BufferPoolManager>,
    pub next_txn_id: AtomicU64,
    pub att: Mutex<HashMap<u64, TxnEntry>>
}

pub struct TxnEntry {
    pub status: TxnStatus,
    pub last_lsn: u64
}

pub enum TxnStatus {
    Running, 
    Commiting,
    Aborting
}

impl TxnManager {
    pub fn new(wal: Arc<WalManager>, bp: Arc<BufferPoolManager>) -> Self {
        Self { wal, bp, next_txn_id: AtomicU64::new(1), att: Mutex::new(HashMap::new()) }
    }

    pub fn begin(&self) -> Result<u64, FerroError> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let lsn = self.wal.append(txn_id, 0, &RecKind::Begin)?;
        self.att.lock().unwrap().insert(txn_id, TxnEntry { status: TxnStatus::Running, last_lsn: lsn });
        Ok(txn_id)
    }

    pub fn log_insert(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, tuple: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapInsert { dir_root, page_id, slot, tuple: tuple.to_vec() })
    }

    pub fn log_delete(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, old: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapDelete { dir_root, page_id, slot, old: old.to_vec() })
    }

    pub fn log_update(&self, txn_id: u64, dir_root: u32, page_id: u32, slot: u16, old: &[u8], new: &[u8]) -> Result<u64, FerroError> {
        self.append_chained(txn_id, &RecKind::HeapUpdate { dir_root, page_id, slot, old: old.to_vec(), new: new.to_vec() })
    }

    pub fn append_chained(&self, txn_id: u64, kind: &RecKind) -> Result<u64, FerroError> {
        let mut att = self.att.lock().unwrap();
        let entry = att.get_mut(&txn_id).ok_or_else(|| FerroError::Wal("txn not active".into()))?;
        let lsn = self.wal.append(txn_id,entry.last_lsn, kind)?;
        entry.last_lsn = lsn;
        Ok(lsn)
    }

    pub fn commit(&self, txn_id: u64) -> Result<(), FerroError> {
        let commit_lsn = self.append_chained(txn_id, &RecKind::Commit)?;
        self.wal.flush_up_to(commit_lsn)?;
        let _ = self.append_chained(txn_id, &RecKind::TxnEnd)?;
        self.att.lock().unwrap().remove(&txn_id);
        Ok(())
    }

    pub fn abort(&self, txn_id: u64) -> Result<(), FerroError> {
        let abort_lsn = self.append_chained(txn_id, &RecKind::Abort)?;
        let _ = abort_lsn;
        {
            self.att.lock().unwrap().get_mut(&txn_id).unwrap().status = TxnStatus::Aborting;
        }
        let mut lsn = {
            self.att.lock().unwrap().get(&txn_id).unwrap().last_lsn
        };
        loop {
            let (rec, _) = self.wal.read_record(lsn)?;
            match rec.kind {
                RecKind::Begin => break,
                RecKind::HeapInsert { dir_root, page_id, slot, .. } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn , undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapDelete{ dir_root, page_id, slot, old: Vec::new() })
                    };
                    let clr_lsn = self.append_chained(txn_id, &clr)?;
                    undo_insert(&self.bp, page_id, slot, clr_lsn)?;
                }
                RecKind::HeapDelete { dir_root, page_id, slot, old } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn, undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapInsert { dir_root, page_id, slot, tuple: old.to_vec() })
                    };
                    let clr_lsn = self.append_chained(txn_id, &clr)?;
                    undo_delete(&self.bp, page_id, slot, &old, clr_lsn)?;
                }
                RecKind::HeapUpdate { dir_root, page_id, slot, old, new } => {
                    let clr = RecKind::Clr { undone_lsn: rec.lsn, undo_next: rec.prev_lsn, 
                        redo: Box::new(RecKind::HeapUpdate { dir_root, page_id, slot, old: new.clone(), new: old.clone() })
                    }; 
                    let clr_lsn = self.append_chained(txn_id, &clr)?;
                    undo_update(&self.bp, page_id, slot, &old, clr_lsn)?;
                }
                RecKind::Clr {undo_next, .. } => {
                    if undo_next == 0 {
                        break;
                    }
                    lsn = undo_next;
                    continue;
                }
                _ => {}
            }
            if rec.prev_lsn == 0 {
                break;
            }
            lsn = rec.prev_lsn;
        }
        let _ = self.append_chained(txn_id, &RecKind::TxnEnd)?;
        self.att.lock().unwrap().remove(&txn_id);
        Ok(())
    }
}

pub fn undo_insert(bp: &BufferPoolManager, page_id: u32, slot: u16, clr_lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, clr_lsn, |page| page.delete(slot as usize))
}

pub fn undo_delete(bp: &BufferPoolManager, page_id: u32, slot: u16, old: &[u8], clr_lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, clr_lsn, |page| page.restore_at(slot as usize, old))
}

pub fn undo_update(bp: &BufferPoolManager, page_id: u32, slot: u16, old: &[u8], clr_lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, clr_lsn, |page| page.update(slot as usize, Tuple::new(old.to_vec())))
}

pub fn stamp_page_lsn(bp: &BufferPoolManager, page_id: u32, lsn: u64) -> Result<(), FerroError> {
    with_page(bp, page_id, lsn, |_| Ok(()))
}

pub fn with_page<F>(bp: &BufferPoolManager, page_id: u32, lsn: u64, f: F) -> Result<(), FerroError> 
where F: FnOnce(&mut Page) -> Result<(), FerroError> {
    let frame_i = bp.fetch_page(page_id)?;
    let mut frame = bp.frames[frame_i].write().unwrap();
    let mut page = Page::deserialize(frame.data)?;
    f(&mut page)?;
    page.lsn = lsn;
    frame.data = page.serialize()?;
    drop(frame);
    bp.unpin_page(page_id, true);
    Ok(())
}
