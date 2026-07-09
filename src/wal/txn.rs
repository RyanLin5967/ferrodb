use std::{collections::HashMap, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}};

use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{heap_page::Page, tuple::Tuple}, wal::log::{RecKind, WalManager}};

const CHECKPOINT_INTERVAL: u64 = 256;

pub struct TxnManager {
    pub wal: Arc<WalManager>,
    pub bp: Arc<BufferPoolManager>,
    pub next_txn_id: AtomicU64,
    pub att: Mutex<HashMap<u64, TxnEntry>>,
    pub commits_since_checkpoint: AtomicU64,
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
        Self { wal, bp, next_txn_id: AtomicU64::new(1), att: Mutex::new(HashMap::new()), commits_since_checkpoint: AtomicU64::new(0) }
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
        if self.commits_since_checkpoint.fetch_add(1, Ordering::SeqCst) + 1 >= CHECKPOINT_INTERVAL {
            self.checkpoint()?;
        }
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

    pub fn checkpoint(&self) -> Result<(), FerroError> {
        if !self.att.lock().unwrap().is_empty() {
            return Err(FerroError::Wal("checkpoint with active txns".into()));
        }
        self.wal.flush()?;
        self.bp.flush_all()?;
        self.bp.disk_manager.sync()?;
        self.wal.truncate()?;
        self.commits_since_checkpoint.store(0, Ordering::SeqCst);
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

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions};

use crate::{catalog::catalog::Catalog, execution::executor::{Outcome, run}, parser::{parser::Parser, scanner::Scanner}, storage::{disk_manager::DiskManager, heap_file_manager::HeapFileManager}, wal::log::LogRecord};

use super::*;

    fn setup() -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.path().join("txn.db")).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let wal = Arc::new(WalManager::new(dir.path().join("txn.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        (bp, wal, txn, dir)
    }

    fn walk_log(wal: &WalManager) -> Vec<LogRecord> {
        let mut out = Vec::new();
        let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
        let end = wal.next_lsn.load(Ordering::SeqCst);
        while lsn < end {
            let (rec, next) = wal.read_record(lsn).unwrap();
            out.push(rec);
            lsn = next;
        }
        out
    }

    #[test]
    fn test_commit_writes_chain_and_flushes() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        heap.insert(Tuple::new(vec![1,2,3,4])).unwrap();
        txn.commit(t1).unwrap();
        let recs = walk_log(&wal);
        assert_eq!(recs.len(), 4);
        assert!(matches!(recs[0].kind, RecKind::Begin));
        assert!(matches!(recs[1].kind, RecKind::HeapInsert { .. }));
        assert!(matches!(recs[2].kind, RecKind::Commit));
        assert!(matches!(recs[3].kind, RecKind::TxnEnd));
        assert_eq!(recs[1].prev_lsn, recs[0].lsn);
        assert_eq!(recs[2].prev_lsn, recs[1].lsn);
        assert!(wal.flushed_lsn.load(Ordering::SeqCst) > recs[2].lsn);
        if let RecKind::HeapInsert { dir_root, tuple, .. } = &recs[1].kind {
            assert_eq!(*dir_root, heap.first_directory_page_id);
            assert_eq!(tuple, &vec![1, 2, 3, 4]);
        }

        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_abort_insert_removes_rows() {
        let (bp, wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);

        for i in 0..3u8 {
            heap.insert(Tuple::new(vec![i,i,i])).unwrap();
        }
        txn.abort(t1).unwrap();
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        // begin, hi, hi, hi, abort, clr, clr, clr, txnend
        let recs = walk_log(&wal);
        let undone: Vec<u64> = recs[5..8].iter().map(|r| match &r.kind {
            RecKind::Clr { undone_lsn, .. } => *undone_lsn,
            _ => panic!()
        }).collect();
        assert!(rows.is_empty());
        assert_eq!(recs.len(), 9);
        assert!(matches!(recs[4].kind, RecKind::Abort));
        assert!(matches!(recs[8].kind, RecKind::TxnEnd));
        assert_eq!(undone, vec![recs[3].lsn, recs[2].lsn, recs[1].lsn]);
    }

    #[test]
    fn test_abort_delete_restores_row() {
        let (bp, _wal, txn, _dir) = setup();
        let t1 = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t1);
        let rid = heap.insert(Tuple::new(vec![8,8,8])).unwrap();
        txn.commit(t1).unwrap();

        let t2 = txn.begin().unwrap();
        heap.set_transaction(txn.clone(), t2);
        heap.delete(rid).unwrap();
        assert!(heap.read(rid).is_err());
        txn.abort(t2).unwrap();
        
        assert_eq!(heap.read(rid).unwrap().data, vec![8,8,8]);
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn sql_insert_commits_through_run() {
        let (bp, wal, txn, _dir) = setup();
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let exec = |sql: &str, catalog: &mut Catalog| -> Outcome {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.remove(0), catalog, bp.clone(), txn.clone()).unwrap()
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR(20));", &mut catalog);
        let out = exec("INSERT INTO t VALUES (1, 'a');", &mut catalog);
        assert!(matches!(out, Outcome::Affected(1)));
        let recs = walk_log(&wal);
        assert!(recs.iter().any(|r| matches!(r.kind, RecKind::HeapInsert { .. })));
        assert!(recs.iter().any(|r| matches!(r.kind, RecKind::Commit)));
    }
}