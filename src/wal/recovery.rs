use std::{collections::{BTreeMap, HashMap, HashSet}, sync::{Arc, atomic::Ordering}};

use crate::{buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, column::Value}, error::FerroError, storage::{heap_file_manager::{HeapFileManager, RecordId}, heap_page::Page, index::BPlusTreeManager, index_fulltext::{indexed_text, post_tokens}, tuple::Tuple}, wal::{log::RecKind, txn::{TxnEntry, TxnManager, TxnStatus}}};

pub fn recover(txn: &TxnManager) -> Result<bool, FerroError> {
    let wal = &txn.wal;
    // read whole log
    let mut records = Vec::new();
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    while lsn < end {
        let (rec, next) = wal.read_record(lsn)?;
        records.push(rec);
        lsn = next;
    }

    if records.is_empty() {
        return Ok(false);
    }

    let mut max_txn = 0u64;
    let mut last_lsn = HashMap::new();
    // The earliest record each transaction still has in the retained log. For a loser this is its
    // `Begin` unless a truncation cut above it, in which case it is the oldest record that
    // survives — which is the same thing the field means: the earliest point a reader would have
    // to start from to see everything this transaction did.
    let mut first_lsn: HashMap<u64, u64> = HashMap::new();
    let mut ended: HashSet<u64> = HashSet::new();
    let mut touched = HashSet::new();
    // analysis
    for rec in &records {
        max_txn = max_txn.max(rec.txn_id);
        last_lsn.insert(rec.txn_id, rec.lsn);
        first_lsn.entry(rec.txn_id).or_insert(rec.lsn);
        match &rec.kind {
            RecKind::Commit | RecKind::TxnEnd => {
                ended.insert(rec.txn_id);
            }
            RecKind::HeapDelete { dir_root, page_id, .. } | RecKind::HeapInsert { dir_root, page_id, .. } | RecKind::HeapUpdate { dir_root, page_id, .. } => {
                touched.insert((*dir_root, *page_id));
            }
            RecKind::Clr { redo, .. } => {
                if let RecKind::HeapInsert { dir_root, page_id, ..} | RecKind::HeapDelete { dir_root, page_id, ..} | 
                RecKind::HeapUpdate { dir_root, page_id, .. } = redo.as_ref() {
                    touched.insert((*dir_root, *page_id));
                }
            }
            _ => {}
        }
    }
    // F4: the counter is a leader-granted range now, not an atomic. The call is the same
    // statement it always was -- "at least this much was issued" -- and is still monotone; it
    // deliberately does NOT create a grant, so a recovered cluster member still refuses to begin a
    // transaction until the leader gives it one.
    txn.raise_next_txn_id(max_txn + 1);

    // **Every loop below walks `touched` in this fixed order, not the `HashSet`'s.**
    //
    // `touched` is a `HashSet`, seeded per instance, so recovery used to repair pages — and *write*
    // them — in a different order on every run. That makes a crash during recovery irreproducible:
    // recovery is itself a sequence of durable writes, and it is the sequence most likely to be
    // interrupted, because it only runs after something has already gone wrong. A crash-in-recovery
    // that cannot be replayed cannot be debugged, and "the same seed reproduces the same byte
    // sequence" is unmeetable while the sequence depends on a hash seed. Sorted by (dir_root,
    // page_id): a total order, and ascending page id is the same order `flush_all` now uses.
    let mut touched: Vec<(u32, u32)> = touched.into_iter().collect();
    touched.sort_unstable();

    // restore pages with broken file extensions
    let bp = &txn.bp;
    for (_, page_id) in &touched {
        if bp.disk_manager.read(*page_id).is_err() {
            bp.disk_manager.write(*page_id, &Page::empty(*page_id).serialize()?)?;
        }
    }

    // redo
    for rec in &records {
        match &rec.kind {
            RecKind::HeapDelete { .. } | RecKind::HeapInsert { .. } | RecKind::HeapUpdate { .. } => {
                redo_one(bp, rec.lsn, &rec.kind)?;
            }
            RecKind::Clr { redo, .. } => redo_one(bp, rec.lsn, redo)?,
            _ => {}
        }
    }

    // undo
    //
    // Sorted for the same reason as `touched`: `last_lsn` is a `HashMap`, so the set of losers came
    // back in a per-process order, and undo *writes* — a CLR record per undone action, plus the page
    // it repairs. Two losers therefore produced two different byte sequences from the same crash.
    // Ascending transaction id is also the order the transactions started in, which is the order a
    // reader of the log would expect their compensation records to appear.
    let mut losers: Vec<u64> = last_lsn.keys().copied().filter(|id| !ended.contains(id)).collect();
    losers.sort_unstable();
    for id in losers {
        // Through the guard, not the raw lock: this ADDS to the active set, so it must move
        // `att_version` or a cached snapshot taken before recovery would miss the losers.
        // A fresh-context review found this site bypassing the funnel.
        txn.att_write().insert(id, TxnEntry {
            status: TxnStatus::Aborting,
            last_lsn: std::sync::atomic::AtomicU64::new(last_lsn[&id]),
            begin_lsn: first_lsn[&id],
            snapshot: None
        });
        txn.abort(id)?;
    }

    // repair directory
    for (dir_root, page_id) in &touched {
        let hfm = HeapFileManager::open(*dir_root, bp.clone());
        let frame_i = bp.fetch_page(*page_id)?;
        let frame = bp.frames[frame_i].read().unwrap();
        let page = Page::deserialize(frame.data)?;
        drop(frame);
        bp.unpin_page(*page_id, false);
        let free = page.get_free_space_end() - page.get_free_space_start();
        match hfm.update_directory_entry(*page_id, free) {
            Ok(()) => {}
            Err(FerroError::KeyNotFound) => hfm.add_to_directory(*page_id, free)?,
            Err(e) => return Err(e)
        }
    }
    Ok(true)
}

/// Apply one log record to the pages, for callers outside recovery — a replica applying a
/// primary's stream is doing redo, and should do it through the same code that recovery uses
/// rather than a second implementation that can drift from it.
///
/// Idempotent by page LSN: a record whose LSN is at or below the page's is skipped, which is what
/// makes a re-sent overlap after a reconnect harmless.
pub fn apply_redo(bp: &Arc<BufferPoolManager>, lsn: u64, kind: &RecKind) -> Result<(), FerroError> {
    redo_one(bp, lsn, kind)
}

fn redo_one(bp: &Arc<BufferPoolManager>, lsn: u64, kind: &RecKind) -> Result<(), FerroError> {
    let page_id = match kind {
        RecKind::HeapDelete { page_id, .. } | RecKind::HeapInsert { page_id, ..} | RecKind::HeapUpdate { page_id, ..} => *page_id,
        _ => return Ok(())
    };
    let frame_i = bp.fetch_page(page_id)?;
    let mut frame = bp.frame_write(frame_i);
    let stored_id = u32::from_be_bytes(frame.data[1..5].try_into().unwrap());
    let mut page = if stored_id != page_id {
        Page::empty(page_id)
    } else {    
        Page::deserialize(frame.data)?
    };

    if page.lsn >= lsn {
        drop(frame);
        bp.unpin_page(page_id, false);
        return Ok(());
    }
    match kind {
        RecKind::HeapDelete { slot, ..} => page.delete(*slot as usize)?,
        RecKind::HeapInsert { slot, tuple, ..} => {
            if (*slot as usize) == page.slot_arr.len() {
                let s = page.insert(Tuple::new(tuple.clone()))?;
                debug_assert_eq!(s, *slot);
            } else {
                page.restore_at(*slot as usize, tuple)?;
            }
        }
        RecKind::HeapUpdate { slot, new, .. } => page.update(*slot as usize, Tuple::new(new.to_vec()))?,
        _ => unreachable!()
    }
    page.lsn = lsn;
    frame.data = page.serialize()?;
    drop(frame);
    bp.unpin_page(page_id, true);
    Ok(())
}

pub fn rebuild_indexes(catalog: &mut Catalog, bp: &Arc<BufferPoolManager>) -> Result<(), FerroError> {
    // **By table name, not by `HashMap` order.** This loop frees every index tree and builds a fresh
    // one, so the order decides which page ids the new trees get and therefore every byte written
    // from here on. Iterating `values_mut()` made that a function of a per-process hash seed: the
    // same crash, recovered twice, produced two different databases. Both were correct; neither could
    // be compared with the other, which is what a crash sweep has to do.
    let mut names: Vec<String> = catalog.tables.keys().cloned().collect();
    names.sort_unstable();
    for name in names {
        let entry = catalog.tables.get_mut(&name).expect("name came from this map");
        let hfm = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
        let mut rows = Vec::new();
        for r in hfm.scan() {
            let (rid, tuple) = r?;
            // `end_ts` is read here because the primary rebuild below has to prefer a live version
            // over a tombstone when the heap holds both under one key.
            let deleted = tuple.version_header()?.end_ts != 0;
            rows.push((rid, tuple.deserialize(&entry.schema)?, deleted));
        }
        let old = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone());
        old.free_tree()?;
        let fresh = BPlusTreeManager::<Value, RecordId>::create(bp.clone())?;

        // **One primary entry per key, pointing at the live version.**
        //
        // `insert_entry` appends at the binary-search position rather than overwriting, so one
        // entry per heap *slot* puts two entries under one key whenever the heap holds two slots
        // for it — which `DELETE` followed by re-`INSERT` of the same key does, because `DELETE`
        // stamps `end_ts` in place and leaves the slot where it is. `search` then returns whichever
        // copy binary search lands on, and when that is the tombstone, a point lookup of a live row
        // answers with nothing.
        //
        // Measured before this guard, through SQL:
        // `INSERT (1,'alpha beta'); DELETE id=1; INSERT (1,'alpha gamma');` then a rebuild left
        // primary entries `[(1, {5,1}), (1, {5,0})]`, `search(1)` returned the tombstoned `{5,0}`,
        // and `SELECT * FROM t WHERE id = 1;` returned **zero rows** while `SELECT * FROM t;`
        // returned the row. This is the same de-duplication rule E66 established for the write
        // paths, in the one path that never had it; it was found by B8's full-text search, which
        // resolves every posting through this index.
        let mut primary: BTreeMap<Value, (RecordId, bool)> = BTreeMap::new();
        for (rid, vals, deleted) in &rows {
            match primary.get_mut(&vals[0]) {
                // A live version replaces a tombstone. Two live versions of one key is an anomaly
                // this loop cannot resolve, so it keeps the first and stays deterministic rather
                // than picking by scan order.
                Some(slot) => {
                    if slot.1 && !*deleted {
                        *slot = (*rid, false);
                    }
                }
                None => {
                    primary.insert(vals[0].clone(), (*rid, *deleted));
                }
            }
        }
        for (pk, (rid, _)) in &primary {
            fresh.insert(pk.clone(), *rid)?;
        }
        entry.primary_index_root = fresh.root_page_id.load(Ordering::SeqCst);

        // secondary indexes
        for info in entry.indexes.iter_mut() {
            let col = entry.schema.columns.iter().position(|c| c.name == info.column_name).ok_or(FerroError::KeyNotFound)?;
            let old = BPlusTreeManager::<(Value, Value), ()>::open(info.root_page_id, bp.clone());
            old.free_tree()?;
            let fresh = BPlusTreeManager::<(Value, Value), ()>::create(bp.clone())?;
            for (_, vals, _) in &rows {
                fresh.insert((vals[col].clone(), vals[0].clone()), ())?;
            }
            info.root_page_id = fresh.root_page_id.load(Ordering::SeqCst);
        }

        // B8 — full-text indexes, rebuilt from the same `rows` by the same three steps: free the
        // old tree, create a fresh one, refill it, record the new root. This is all a full-text
        // index needs to survive a crash, and it is why no WAL record was added for one: index
        // structure is not logged at all, the heap is authoritative after redo/undo, and every tree
        // in the database is reconstructed here.
        //
        // `post_tokens` rather than a bare `insert`, and the difference is load-bearing twice over.
        // A value that repeats a word would post that pair once per occurrence, and — the case that
        // is invisible until it happens — a `DELETE` followed by re-`INSERT` of the same primary key
        // leaves TWO slots with that key in this heap, so `rows` holds both and every token they
        // share is posted twice. `insert_entry` appends rather than overwrites, so the search would
        // then return that row once per copy: a crash would turn a correct index into a
        // double-counting one, which is worse than losing it.
        for info in entry.fulltext_indexes.iter_mut() {
            let col = entry.schema.columns.iter().position(|c| c.name == info.column_name).ok_or(FerroError::KeyNotFound)?;
            let old = BPlusTreeManager::<(Value, Value), ()>::open(info.root_page_id, bp.clone());
            old.free_tree()?;
            let fresh = BPlusTreeManager::<(Value, Value), ()>::create(bp.clone())?;
            for (_, vals, _) in &rows {
                if let Some(text) = indexed_text(&vals[col])? {
                    post_tokens(&fresh, text, &vals[0])?;
                }
            }
            info.root_page_id = fresh.root_page_id.load(Ordering::SeqCst);
        }
    }
    // **The records above are the only copy that moved.** Every loop in this function writes the
    // fresh root into the `TableEntry`, and the SHARED cells (`Catalog::roots`, D53) still hold the
    // roots `Catalog::open` seeded from the catalog page before any of this ran — i.e. the trees
    // `free_tree` has just released. `plan::open_table` prefers the cell to the record, so without
    // this the rebuild is correct on disk and *invisible*: every statement after recovery descends
    // a freed tree, and whether that answers wrongly or merely differently depends on which pages
    // the allocator handed back to `create`.
    //
    // `reseed_root_cells` rather than `sync_root_cells`, and the difference is the whole bug:
    // `sync_root_cells` never overwrites an existing cell, so it cannot repair one.
    catalog.reseed_root_cells();
    catalog.persist()
}

/// Open the catalog for a database, recovering it first if the WAL says it needs it.
///
/// **The single bootstrap, because there used to be two and they disagreed.** `src/cli/cli.rs`
/// ran `recover` → `Catalog::open` → `rebuild_indexes` → `checkpoint`; `examples/pgserver.rs` —
/// the shipped server, and the path every pgwire measurement is taken on — ran `recover` and
/// **discarded its return value**, so it never rebuilt an index. Index structure is not WAL-logged
/// at all (see `rebuild_indexes`: the heap is authoritative after redo/undo and every tree is
/// reconstructed here), so on that path a crash left the trees in whatever partially-flushed state
/// the crash produced, against a heap the redo pass had moved on. `rebuild_indexes` was never
/// present in `pgserver.rs` — it is an omission from the wire-protocol commit that introduced it,
/// not a removal — and `tests/sim_durability.rs` models the *cli* sequence by name, so no test
/// covered the server's actual bootstrap.
///
/// Taking the sequence away from both callers is the point: a second copy is how they diverged.
pub fn open_recovered(
    bp: &Arc<BufferPoolManager>,
    txn: &TxnManager,
    existed: bool,
    first_catalog_page_id: u32,
) -> Result<Catalog, FerroError> {
    // Before `Catalog::open`: redo/undo repairs the heap that `rebuild_indexes` then reads.
    let recovered = recover(txn)?;
    let mut catalog = if existed {
        Catalog::open(Arc::clone(bp), first_catalog_page_id)?
    } else {
        Catalog::create(Arc::clone(bp))?
    };
    if recovered {
        rebuild_indexes(&mut catalog, bp)?;
        // The rebuilt trees and the catalog page are on disk now, so the log that produced them
        // has nothing left to say. Without this the next open replays the same records and rebuilds
        // every tree again.
        txn.checkpoint()?;
    }
    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, path::Path};

use crate::{execution::session::Session, storage::disk_manager::DiskManager, wal::log::WalManager};

use super::*; 

    fn setup(dir: &Path) -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>) {
        let file = OpenOptions::new().read(true).write(true).create(true).open(dir.join("recovery.db")).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let wal = Arc::new(WalManager::new(dir.join("recovery.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        (bp, wal, txn)
    }

    #[test]
    fn test_insert_survives_crash() {
        let dir = tempfile::tempdir().unwrap();
        let (dir_root, rid);
        {
            let (bp, _wal, txn) = setup(dir.path());
            let t = txn.begin().unwrap();
            let mut heap = HeapFileManager::new(bp.clone()).unwrap();
            dir_root = heap.first_directory_page_id;
            heap.set_transaction(txn.clone(), t);
            rid = heap.insert(Tuple::new(vec![1,2,3])).unwrap();
            txn.commit(t).unwrap();
        }
        let (bp, _wal, txn) = setup(dir.path());
        assert!(recover(&txn).unwrap());
        let heap = HeapFileManager::open(dir_root, bp.clone());
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(heap.read(rid).unwrap().data, vec![1,2,3]);
    }

    #[test]
    fn test_uncommited_rolled_back() {
        let dir = tempfile::tempdir().unwrap();
        let dir_root;
        {
            let (bp, wal, txn) = setup(dir.path());
            let t = txn.begin().unwrap();
            let mut heap = HeapFileManager::new(bp.clone()).unwrap();
            dir_root = heap.first_directory_page_id;
            heap.set_transaction(txn.clone(), t);
            heap.insert(Tuple::new(vec![7,7])).unwrap();
            heap.insert(Tuple::new(vec![6,7])).unwrap();
            wal.flush().unwrap();
        }
        let (bp, _wal, txn) = setup(dir.path());
        assert!(recover(&txn).unwrap());
        let heap = HeapFileManager::open(dir_root, bp.clone());
        assert!(heap.scan().collect::<Result<Vec<_>, _>>().unwrap().is_empty());
        assert!(recover(&txn).unwrap());
        assert!(heap.scan().collect::<Result<Vec<_>, _>>().unwrap().is_empty());
    }

    #[test]
    fn test_update_delete_redo_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let (dir_root, rid_a, rid_b);
        {
            let (bp, _wal, txn) = setup(dir.path());
            let t1 = txn.begin().unwrap();
            let mut heap = HeapFileManager::new(bp.clone()).unwrap();
            dir_root = heap.first_directory_page_id;
            heap.set_transaction(txn.clone(), t1);
            rid_a = heap.insert(Tuple::new(vec![1,1])).unwrap();
            rid_b = heap.insert(Tuple::new(vec![2,2])).unwrap();
            txn.commit(t1).unwrap();
            txn.checkpoint().unwrap();

            let t2 = txn.begin().unwrap();
            heap.set_transaction(txn.clone(), t2);
            heap.update(rid_a, Tuple::new(vec![9,9])).unwrap();
            txn.commit(t2).unwrap();

            let t3 = txn.begin().unwrap();
            heap.set_transaction(txn.clone(), t3);
            heap.delete(rid_b).unwrap();
            txn.commit(t3).unwrap();
        }
        let (bp, _wal, txn) = setup(dir.path());
        assert!(recover(&txn).unwrap());
        let heap = HeapFileManager::open(dir_root, bp.clone());
        assert!(heap.read(rid_b).is_err());
        assert_eq!(heap.read(rid_a).unwrap().data, vec![9,9]);
        assert_eq!(heap.scan().collect::<Result<Vec<_>, _>>().unwrap().len(), 1);
    }

    #[test]
    fn sql_crash_recover_rebuild_query() {
        use crate::execution::executor::{run, Outcome};
        use crate::parser::{parser::Parser, scanner::Scanner};

        let exec = |sql: &str, catalog: &mut Catalog, bp: &Arc<BufferPoolManager>, txn: &Arc<TxnManager>| -> Outcome {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty(), "parse errors: {:?}", p.errors);
            let mut session = Session::new();
            run(stmts.remove(0), catalog, bp.clone(), txn.clone(), &mut session).unwrap()
        };
        let dir = tempfile::tempdir().unwrap();
        {
            let (bp, _wal, txn) = setup(dir.path());
            let mut catalog = Catalog::create(bp.clone()).unwrap();
            exec("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR(16));", &mut catalog, &bp, &txn); // fence checkpoints
            for i in 0..3 {
                exec(&format!("INSERT INTO t VALUES ({}, 'u{}');", i, i), &mut catalog, &bp, &txn);
            }
        }
        let (bp, _wal, txn) = setup(dir.path());
        assert!(recover(&txn).unwrap());
        let mut catalog = Catalog::open(bp.clone(), 1).unwrap(); // FIRST_CATALOG_PAGE_ID
        rebuild_indexes(&mut catalog, &bp).unwrap();

        match exec("SELECT name FROM t;", &mut catalog, &bp, &txn) {
            Outcome::Rows(rows) => assert_eq!(rows.len(), 3),
            _ => panic!("expected rows"),
        }
        let entry = catalog.get_table("t").unwrap();
        let tree = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone());
        assert!(tree.search(&Value::Integer(1)).unwrap().is_some());
    }

    /// A crash rebuild must leave the SHARED root cells pointing at the trees it just built.
    ///
    /// `rebuild_indexes` frees every index tree and writes a fresh root into the `TableEntry`. The
    /// shared cells (D53, `Catalog::roots`) were seeded by `Catalog::open` from the **pre-crash**
    /// roots, and `sync_root_cells` cannot repair them — it creates missing cells and never
    /// overwrites an existing one. Without `Catalog::reseed_root_cells` the rebuild is therefore
    /// correct on disk and invisible to every statement after it, because `plan::open_table` and
    /// the point-lookup path in `optimizer.rs` both prefer the cell to the record.
    ///
    /// **Why the test above does not catch this**, which is the reason this one exists:
    /// `sql_crash_recover_rebuild_query` asserts through `entry.primary_index_root` — the record,
    /// not the cell — and its `SELECT name FROM t;` is an unqualified scan that reads the heap. Both
    /// halves sit above the cell and cannot see it hold a freed page.
    ///
    /// **TWO tables and 400 rows each, not one small table**, and that is load-bearing: `free_tree`
    /// returns the old pages to the allocator and `create` can hand the very same page straight
    /// back, so a single-page tree can rebuild to an identical root id and hide the defect. A
    /// multi-level tree's final root is a page allocated late in the refill, and the second table
    /// starts from an allocator the first has already churned.
    #[test]
    fn crash_rebuild_reseeds_the_shared_root_cells() {
        use crate::execution::executor::{run, Outcome};
        use crate::parser::{parser::Parser, scanner::Scanner};

        let exec = |sql: &str, catalog: &mut Catalog, bp: &Arc<BufferPoolManager>, txn: &Arc<TxnManager>| -> Outcome {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty(), "parse errors: {:?}", p.errors);
            let mut session = Session::new();
            run(stmts.remove(0), catalog, bp.clone(), txn.clone(), &mut session).unwrap()
        };

        const ROWS: i64 = 400;
        let dir = tempfile::tempdir().unwrap();
        {
            let (bp, _wal, txn) = setup(dir.path());
            let mut catalog = Catalog::create(bp.clone()).unwrap();
            for t in ["t", "u"] {
                exec(&format!("CREATE TABLE {t} (id INTEGER NOT NULL, name VARCHAR(16));"), &mut catalog, &bp, &txn);
                for i in 0..ROWS {
                    exec(&format!("INSERT INTO {t} VALUES ({i}, 'r{i}');"), &mut catalog, &bp, &txn);
                }
            }
            // No checkpoint and no clean close: the WAL still holds records, so the reopen below
            // takes the recovery path rather than trusting the catalog page.
        }

        let (bp, _wal, txn) = setup(dir.path());
        let mut catalog = open_recovered(&bp, &txn, true, 1).unwrap(); // FIRST_CATALOG_PAGE_ID

        // The structural law, and the one that fails first: the two places a root is written down
        // must name the same page. Read from the system on both sides — neither number is written
        // into this test.
        for t in ["t", "u"] {
            let record = catalog.get_table(t).unwrap().primary_index_root;
            let cell = catalog
                .root_cell(t, None)
                .expect("Catalog::open seeds a cell for every table it loads")
                .load(Ordering::SeqCst);
            assert_eq!(
                cell, record,
                "table '{t}': the shared root cell still names page {cell}, the tree \
                 `rebuild_indexes` freed; the rebuilt tree is at page {record}"
            );
        }

        // The user-visible consequence: an indexed point lookup is the path that reads the cell.
        for t in ["t", "u"] {
            let probe = ROWS - 1;
            match exec(&format!("SELECT name FROM {t} WHERE id = {probe};"), &mut catalog, &bp, &txn) {
                Outcome::Rows(rows) => assert_eq!(
                    rows.len(), 1,
                    "table '{t}': point lookup of id={probe} after crash recovery"
                ),
                _ => panic!("table '{t}': expected rows"),
            }
        }
    }
}