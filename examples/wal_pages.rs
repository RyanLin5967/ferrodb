//! Diagnostic: which page ids does a WAL's records describe?
//!
//! Uses the real deserializer rather than hand-parsing the frame layout, because guessing at a
//! binary layout is how a confident wrong answer gets produced.
use ferrodb::wal::log::{RecKind, WalManager};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

fn main() {
    let path = std::env::args().nth(1).expect("usage: wal_pages <wal-path>");
    let wal = WalManager::new(path.into()).expect("open wal");
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    let mut pages = BTreeSet::new();
    let mut kinds: std::collections::BTreeMap<&str, usize> = Default::default();
    while lsn < end {
        let (rec, next) = match wal.read_record(lsn) { Ok(r) => r, Err(_) => break };
        let name = match &rec.kind {
            RecKind::Begin => "Begin",
            RecKind::Commit => "Commit",
            RecKind::Abort => "Abort",
            RecKind::TxnEnd => "TxnEnd",
            RecKind::HeapInsert { page_id, .. } => { pages.insert(*page_id); "HeapInsert" }
            RecKind::HeapDelete { page_id, .. } => { pages.insert(*page_id); "HeapDelete" }
            RecKind::HeapUpdate { page_id, .. } => { pages.insert(*page_id); "HeapUpdate" }
            _ => "other",
        };
        *kinds.entry(name).or_default() += 1;
        lsn = next;
    }
    println!("kinds: {kinds:?}");
    println!("pages described by heap records: {:?}", pages);
}
