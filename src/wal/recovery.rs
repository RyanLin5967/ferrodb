use std::{collections::{BTreeMap, HashMap, HashSet}, fs::OpenOptions, path::{Path, PathBuf}, sync::{Arc, atomic::Ordering}};

use crate::{buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, column::Value}, error::FerroError, storage::{db_lock::DbLock, disk_manager::DiskManager, heap_file_manager::{HeapFileManager, RecordId}, heap_page::Page, index::BPlusTreeManager, index_fulltext::{indexed_text, post_tokens}, tuple::Tuple}, wal::{log::{RecKind, WalManager}, txn::{TxnEntry, TxnManager, TxnStatus}}};

/// Redo and undo the heap records in the log, and say whether the index trees may now disagree
/// with the heap.
///
/// **D216: `true` means the log held a DATA record** (a heap insert, delete or update, or a CLR),
/// and nothing else makes it `true`. Only such a record changes something an index reflects. Index
/// pages are not logged, so after a crash a tree on disk may lack the change or name a row undo
/// has just removed. A data record counts whether or not redo had to apply it: a heap page can
/// reach the disk while its index page does not, and then redo skips the record while the tree
/// still lacks the key (`a_committed_row_whose_heap_page_reached_disk_still_forces_the_rebuild`).
///
/// It used to be `true` for ANY non-empty log, and a clean close leaves a non-empty one:
/// `TxnManager::checkpoint_locked` truncates the log and then re-appends this process's DDL and
/// run declarations as transaction-0 records. So the first restart after any process that ran DDL
/// or bound an agent run rebuilt every tree, O(rows), for records that change no page. Those
/// records (`Ddl`, `RunIdentity`), and `Begin`, `Commit`, `Abort` and `TxnEnd`, are still read,
/// because the analysis below raises the transaction-id watermark past every id the log names, and
/// the WAL header catches up only at a checkpoint.
///
/// A loser whose records are all transaction control undoes nothing, so it does not count either.
/// That is sound on four conditions:
/// - no heap, index or catalog page of a table reaches the disk before the log records it depends
///   on: `BufferPoolManager::wal_gate`, against `Frame::wal_mark` for a page with no LSN of its own;
/// - a record `WalManager::flush_up_to` reports durable IS durable. Until the D216 adversary's F1
///   it skipped a record that started exactly at the flushed point;
/// - every index write outside DDL follows the heap record it indexes. `execution::insert` and
///   `execution::update` write the heap first, and D202's rollback undo (`TxnManager::abort`)
///   takes back entries whose heap records were appended before it;
/// - no transaction writes between a checkpoint's check for active transactions and its
///   truncation, which would discard that transaction's records. `TxnManager::checkpoint` holds
///   the check only briefly, so this rests on statements being serialised per database (the
///   PRECONDITION on `TxnManager::undo_primary_writes`).
///
/// Together they mean that an index page on disk carrying a transaction's change implies the
/// change's heap record is in the log. (`rebuild_indexes` writes trees with no record at all. It
/// runs only when this function or the marker has already asked for it, and until its checkpoint
/// completes, the same trigger is still there for the next open. That makes the next open rebuild
/// again; it does not make a rebuild interrupted part-way safe to walk, which predates D216.)
///
/// Not covered, and never covered by more than coincidence:
/// - A crash inside a DDL statement. DDL is not logged, and no DDL here is crash-atomic (the ALTER
///   arm of `execution::executor::run` says so). The old any-record rule rebuilt after some such
///   crashes only because earlier re-declarations happened to be in the log. That was never
///   reliable, and not reliably a repair either: `rebuild_indexes` first frees the old tree by
///   walking it (`free_tree`), and a half-written tree's child pointers can name pages that were
///   never written.
/// - Pages that reach the file without the buffer pool: a restored base backup
///   (`replication::backup::restore`) or an installed snapshot (`consensus::snapshot`). Their index
///   pages are a copy taken while the source was running, and the redo window that makes their
///   heap consistent replays heap records only.
/// - A streaming replica. `replication::ReplicaApplier` redoes the primary's heap records into this
///   pool and writes no index page and no local log record, so nothing here says its trees are
///   behind its heap.
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
    //
    // **Transaction 0 is not a transaction, so it is never a loser (D216).** DDL and run
    // declarations are logged under it (`TxnManager::append_ddl`, `replay_runs`), and it never
    // commits, so this used to take it for a loser and "abort" it. That appended an `Abort` and a
    // `TxnEnd` under id 0 and undid nothing. While every non-empty log rebuilt, the rebuild's
    // checkpoint discarded them at once. A log of declarations now rebuilds nothing and is left as
    // it was, so they would stay, and a change-feed decoder reports every `Abort` it reads
    // (`Decoded::aborted`).
    let mut losers: Vec<u64> = last_lsn.keys().copied().filter(|id| *id != 0 && !ended.contains(id)).collect();
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
    // Every data record, and nothing else, put a page into `touched`.
    Ok(!touched.is_empty())
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
    // Every tree this rebuilds, with its fresh root: the shared cells are repointed from this at
    // the end (D205), once the `&mut` borrow of each entry has ended.
    let mut rebuilt: Vec<(String, Option<String>, u32)> = Vec::new();
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
        //
        // ⚠ Since the reused-key fix (`execution::insert`, "The reused key must keep its old
        // version reachable"), SQL no longer leaves two slots for one key: the new version is
        // written into the dead version's slot. A heap written BEFORE that fix still holds them,
        // and this rebuild is what reads such a heap after a crash, so the rule stays.
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
        rebuilt.push((name.clone(), None, entry.primary_index_root));

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
            rebuilt.push((name.clone(), Some(info.column_name.clone()), info.root_page_id));
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
            rebuilt.push((name.clone(), Some(info.column_name.clone()), info.root_page_id));
        }
    }
    // **D205: the records above are the only copy that moved.** Every loop in this function
    // writes the fresh root into the `TableEntry`, and the SHARED cells (`Catalog::roots`, D53)
    // still hold the roots `Catalog::open` seeded before any of this ran: the trees `free_tree` has
    // just released. `plan::open_table` and the optimizer prefer the cell to the record, so without
    // this the rebuild is correct on disk and invisible. Every statement after recovery descended a
    // freed tree, and the fresh-context adversary's schedule (a DROP below the table, then a crash)
    // turned that into a committed row missing by key and a duplicate admitted.
    //
    // **Stored INTO the existing cell, not by re-creating the map.** `b9a0a75` (W4(c), never merged)
    // closed the same hole with `reseed_root_cells`: clear every cell and re-create it. That is
    // correct only while nothing holds a clone of a cell. A holder keeps an `Arc` nobody updates
    // again, giving two root pointers for one tree, which is the D53 defect. Storing into the cell
    // has no such precondition: every holder sees the new root, and there stays one cell per tree
    // (`a_rebuild_repoints_the_cells_it_finds_and_does_not_replace_them`). `sync_root_cells` then
    // creates a cell for any tree that has none, and it never overwrites the ones just repointed.
    //
    // ⚠ Pre-existing, and not changed here: a cell is keyed by `(table, column)` with no index kind,
    // so a secondary index and a full-text index on ONE column share a cell, and the full-text root
    // is stored last. See `frontier/lane_rollback_index_orphan.md` §14.
    //
    // Here, and not only in `open_recovered`, because this is the function that makes the cells
    // wrong. A caller that rebuilds and then queries, as the full-text and recovery tests do, gets
    // cells that match what it built.
    for (table, column, root) in &rebuilt {
        if let Some(cell) = catalog.root_cell(table, column.as_deref()) {
            cell.store(*root, Ordering::SeqCst);
        }
    }
    catalog.sync_root_cells();
    catalog.persist()
}

/// The page the table catalog starts on, in every database file. One constant for the one open
/// path. The CLI and three examples each used to declare their own.
pub const FIRST_CATALOG_PAGE_ID: u32 = 1;

/// A database file, opened and recovered, with every index rebuilt from the recovered heap if
/// recovery or a marker said the trees could be stale.
pub struct OpenedDatabase {
    pub bp: Arc<BufferPoolManager>,
    pub wal: Arc<WalManager>,
    pub txn: Arc<TxnManager>,
    pub catalog: Catalog,
    /// Whether the log held a data record ([`recover`]'s answer, D216). The trees were rebuilt
    /// when this was true or the stale-indexes marker was present.
    pub recovered: bool,
}

/// **D204 — THE way to open a database file.** Every binary calls this; none spells the sequence
/// out for itself (`tests/open_path_allowlist.rs` enforces that).
///
/// The order is the whole content:
/// 1. open the file, the buffer pool, the WAL and the transaction manager, and attach the WAL;
/// 2. [`recover`]: redo and undo the HEAP records, and nothing else;
/// 3. open the catalog, or create it for a new file;
/// 4. if recovery replayed a data record, [`rebuild_indexes`] from the recovered heap, then checkpoint.
///    The rebuild ends by repointing the shared root cells at the trees it built (D205). Without
///    that, every statement after recovery descends the trees the rebuild freed. The checkpoint is
///    there because the rebuilt trees and the catalog page are then on disk, so the log that
///    produced them has nothing left to say; without it, the next open would replay the same
///    records and rebuild every tree again (reasoning from `b9a0a75`). Step 4 also runs when a
///    marker says an earlier rollback's index undo failed (`TxnManager::mark_indexes_stale`), even
///    if the log holds no data record.
///
/// **D216: a clean restart skips step 4.** A clean close ends in a checkpoint, and the log it leaves
/// holds only that checkpoint's re-declarations of DDL and agent runs, which change no page. Step 4
/// used to run for any non-empty log, so every restart after a process that ran DDL or bound a run
/// paid a rebuild of every tree, O(rows). Now such an open rebuilds nothing, and leaves the log
/// exactly as it found it.
///
/// Step 4 is why this exists. Index pages are not logged, so after step 2 each tree is whatever
/// the buffer pool last flushed. That tree can miss a committed row, which makes it invisible by
/// key and lets a second row take its key; or it can name a row recovery just undid. The
/// sequence used to be spelled out in each entry point. `cli::run_cli` had step 4 and
/// `examples/pgserver.rs` never did, from D9 until D204. That is the drift this function ends:
/// `tests/pgserver_crash_rebuilds_indexes.rs`.
///
/// **The caller holds the single-writer lock**, and proves it by passing it: a lock on another
/// path is refused. It is taken by the caller rather than here because the callers differ in
/// what they do BEFORE it. `pgserver` reads its environment first, because its refusal path is
/// `process::exit`, which skips `Drop`, and `table_dump` refuses a missing file first.
///
/// Anything built on top, such as the agent runtime and its arena, comes AFTER this returns. The
/// rebuild allocates pages, and the arena floor must sit above everything this has allocated
/// (`cli::run_cli` explains the arena ordering).
pub fn open_recovered(db_path: &Path, lock: &DbLock) -> Result<OpenedDatabase, FerroError> {
    if !lock.guards(db_path) {
        return Err(FerroError::Io(format!(
            "refusing to open {}: the lock passed in is for a different database, so this open is \
             not protected against a second writer",
            db_path.display()
        )));
    }
    let existed = db_path.exists();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(db_path)
        .map_err(|e| FerroError::Io(format!("open {}: {e}", db_path.display())))?;
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file)?)));
    let mut wal_path = db_path.as_os_str().to_os_string();
    wal_path.push(".wal");
    let wal = Arc::new(WalManager::new(PathBuf::from(wal_path))?);
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    let recovered = recover(&txn)?;
    let mut catalog = if existed {
        Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID)?
    } else {
        Catalog::create(bp.clone())?
    };
    // D205 C1 correction: a rollback in an earlier process whose index undo failed left a marker
    // (`TxnManager::mark_indexes_stale`), because its orphaned entries are on disk and a log with no
    // data record (an empty one, or since D216 one of re-declarations) does not trigger the rebuild
    // below. The marker is removed only after the rebuilt trees are checkpointed. If removal fails,
    // the next open simply rebuilds again, which is harmless.
    let marker = crate::wal::txn::stale_indexes_marker(&wal.path);
    let stale = marker.exists();
    if recovered || stale {
        rebuild_indexes(&mut catalog, &bp)?;
        txn.checkpoint()?;
        if stale {
            if let Err(e) = std::fs::remove_file(&marker) {
                use std::io::Write;
                let _ = writeln!(
                    std::io::stderr(),
                    "ferrodb: rebuilt the indexes, but could not remove {} ({e}); the next open rebuilds again",
                    marker.display()
                );
            }
        }
    }
    Ok(OpenedDatabase { bp, wal, txn, catalog, recovered })
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

    /// **D204 — `open_recovered` refuses a lock held for another database, and opens with its own.**
    ///
    /// The lock is the caller's proof that no second writer shares the file. Without the refusal, a
    /// caller holding the lock on one database could open another unprotected, and nothing on the
    /// SQL path could tell. The second half is the anti-vacuity: a guard that refused every lock would
    /// pass the first half and open nothing.
    #[test]
    fn open_recovered_refuses_a_lock_for_another_database_and_opens_with_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a.db"), dir.path().join("b.db"));
        let lock_a = DbLock::acquire(&a).unwrap();

        let refused = open_recovered(&b, &lock_a).err().expect("a lock on a.db opened b.db");
        assert!(format!("{refused}").contains("different database"), "refused for the wrong reason: {refused}");
        assert!(!b.exists(), "the refusal created b.db before refusing");

        let opened = open_recovered(&a, &lock_a).expect("a.db did not open under its own lock");
        assert!(!opened.recovered, "a brand-new database reported a recovery");
        assert!(a.exists(), "a.db was not created");
    }

    /// **D205 — after a crash rebuild, every shared root cell names the tree the rebuild built.**
    ///
    /// `Catalog::open` seeds one shared cell per tree (D53) from the pre-crash roots on the catalog
    /// page. `rebuild_indexes` frees each old tree and writes the fresh root into the `TableEntry`
    /// only, and `sync_root_cells` never overwrites an existing cell. `plan::open_table` and the
    /// optimizer prefer the cell, so unless the rebuild repoints the cells, every statement after recovery descends a
    /// FREED tree (the fresh-context adversary, `frontier/d202_adversary.md` §2).
    ///
    /// The DROP is load-bearing. The fixture needs a free page BELOW `t`'s root, so that the
    /// rebuild's lowest-free allocation cannot hand `t` its old root back and hide the defect. The
    /// shipped pgserver test (one table, no holes) is exactly that coincidence. `t`'s first row is
    /// inserted BEFORE the DROP, so its heap page exists and the post-DROP insert allocates nothing.
    ///
    /// Structural first: record == cell for every table, both read from the system and neither
    /// written into this test. Then the user-visible consequence, a lookup by key and a refused
    /// duplicate.
    #[test]
    fn a_crash_rebuild_points_every_shared_root_cell_at_its_new_tree() {
        use crate::execution::executor::{run, Outcome};
        use crate::parser::{parser::Parser, scanner::Scanner};

        fn exec(sql: &str, o: &mut OpenedDatabase) -> Result<Outcome, FerroError> {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty(), "parse errors in `{sql}`: {:?}", p.errors);
            run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), &mut Session::new())
        }

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("crash.db");
        let pre_crash_root;
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            for sql in [
                "CREATE TABLE a (id INTEGER NOT NULL, v INTEGER);",
                "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);",
                // A secondary tree too, so the structural check covers more than primary cells
                // (a gap the re-adversary named).
                "CREATE INDEX iv ON t (v);",
                "INSERT INTO t VALUES (0, 0);",
                "DROP TABLE a;",
                "INSERT INTO t VALUES (1, 10);",
            ] {
                exec(sql, &mut o).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            pre_crash_root = o.catalog.get_table("t").unwrap().primary_index_root;
            // The crash: every handle is dropped with no checkpoint and no flush. `BufferPoolManager`
            // has no `Drop`, so the last insert's pages die here and only its WAL records survive.
        }

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        assert!(o.recovered, "premise failed: the reopen replayed nothing, so no rebuild ran");
        let tables: Vec<String> = o.catalog.tables.keys().cloned().collect();
        assert!(tables.contains(&"t".to_string()), "premise failed: table t did not survive: {tables:?}");
        // The premise the re-adversary asked for: the rebuilt root really MOVED. If the rebuild handed
        // back the pre-crash page, cell == record would hold whether or not the cells were repointed.
        assert_ne!(
            o.catalog.get_table("t").unwrap().primary_index_root,
            pre_crash_root,
            "premise failed: t's rebuilt root is its pre-crash page, so the structural check cannot fail"
        );
        for name in &tables {
            let entry = o.catalog.get_table(name).unwrap();
            let mut trees: Vec<(Option<String>, u32)> = vec![(None, entry.primary_index_root)];
            trees.extend(entry.indexes.iter().map(|i| (Some(i.column_name.clone()), i.root_page_id)));
            trees.extend(entry.fulltext_indexes.iter().map(|i| (Some(i.column_name.clone()), i.root_page_id)));
            assert!(trees.len() >= 2 || name != "t", "premise failed: t lost its secondary index");
            for (column, record) in trees {
                let cell = o
                    .catalog
                    .root_cell(name, column.as_deref())
                    .expect("Catalog::open seeds a cell for every tree it loads")
                    .load(Ordering::SeqCst);
                assert_eq!(
                    cell, record,
                    "table '{name}', tree {column:?}: the shared root cell names page {cell}, a tree the \
                     rebuild FREED; the rebuilt tree is at page {record}"
                );
            }
        }

        match exec("SELECT id, v FROM t WHERE id = 1;", &mut o).unwrap() {
            Outcome::Rows(rows) => assert_eq!(
                rows,
                vec![vec![Value::Integer(1), Value::Integer(10)]],
                "after the crash, a committed row is missing by key"
            ),
            _ => panic!("a SELECT did not return rows"),
        }
        match exec("INSERT INTO t VALUES (1, 99);", &mut o) {
            Err(FerroError::Constraint(m)) if m.contains("duplicate primary key") => {}
            Err(e) => panic!("a second row 1 was refused, but not as a duplicate: {e}"),
            Ok(_) => panic!("after the crash, a second row 1 was ADMITTED"),
        }
    }

    /// **D205, the choice between the two copies: the rebuild REPOINTS the cells it finds, and
    /// never replaces them.**
    ///
    /// `b9a0a75` closed the hole with `reseed_root_cells`: clear the map and re-create every cell.
    /// That is correct only while no handle holds a clone of a cell, because a handle that does
    /// keeps an `Arc` nobody updates any more: two root pointers for one tree, which is the D53
    /// defect itself. Its doc stated that precondition; nothing enforced it. Storing the fresh root
    /// into the EXISTING cell has no precondition. Every holder of the `Arc` sees the new root, and
    /// D53's invariant (one cell per tree for the life of the process) is kept.
    ///
    /// So this holds a cell across `rebuild_indexes`, as a live handle would, and requires that it
    /// is still THE cell afterwards and names the rebuilt tree. FAILS under the clear-and-resync
    /// form (`d81f080`) at `ptr_eq`. Passes under store-in-place.
    #[test]
    fn a_rebuild_repoints_the_cells_it_finds_and_does_not_replace_them() {
        use crate::execution::executor::run;
        use crate::parser::{parser::Parser, scanner::Scanner};

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("held.db");
        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        for sql in ["CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", "INSERT INTO t VALUES (1, 10);"] {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), &mut Session::new())
                .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
        }
        let held = o.catalog.root_cell("t", None).expect("CREATE TABLE seeds a cell");

        rebuild_indexes(&mut o.catalog, &o.bp).unwrap();

        let now = o.catalog.root_cell("t", None).expect("the rebuild left no cell for t");
        assert!(
            Arc::ptr_eq(&held, &now),
            "the rebuild REPLACED t's cell, so a handle holding the old one now has a private root pointer (D53)"
        );
        assert_eq!(
            held.load(Ordering::SeqCst),
            o.catalog.get_table("t").unwrap().primary_index_root,
            "the cell a handle holds does not name the tree the rebuild built"
        );
    }

    /// **D205, the second route to the same drift: the shape of the rebuilt tree.** Ported from
    /// `b9a0a75`'s `crash_rebuild_reseeds_the_shared_root_cells` (W4(c), never merged) and adapted
    /// to this branch's `open_recovered`.
    ///
    /// The DROP test above puts a free page BELOW a one-leaf tree. This one needs no DROP. Two
    /// tables of 400 rows each give multi-level trees, and a multi-level tree's final root is a page
    /// allocated late in the refill, so it differs from the pre-crash root by construction. The
    /// second table starts from an allocator the first has already churned. It passes at `d81f080`
    /// and fails when the rebuild stops repointing the cells (mutant Q1).
    #[test]
    fn a_crash_rebuild_of_multi_level_trees_leaves_every_cell_on_its_new_root() {
        use crate::execution::executor::{run, Outcome};
        use crate::parser::{parser::Parser, scanner::Scanner};

        fn exec(sql: &str, o: &mut OpenedDatabase) -> Outcome {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty(), "parse errors in `{sql}`: {:?}", p.errors);
            run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), &mut Session::new())
                .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        }

        const ROWS: i64 = 400;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("shape.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            for t in ["t", "u"] {
                exec(&format!("CREATE TABLE {t} (id INTEGER NOT NULL, name VARCHAR(16));"), &mut o);
                for i in 0..ROWS {
                    exec(&format!("INSERT INTO {t} VALUES ({i}, 'r{i}');"), &mut o);
                }
            }
            // No checkpoint and no clean close: the reopen below takes the recovery path.
        }

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        assert!(o.recovered, "premise failed: the reopen replayed nothing, so no rebuild ran");
        for t in ["t", "u"] {
            let record = o.catalog.get_table(t).unwrap().primary_index_root;
            let cell = o.catalog.root_cell(t, None).expect("a cell per loaded table").load(Ordering::SeqCst);
            assert_eq!(cell, record, "table '{t}': the cell names page {cell}, a freed tree; the rebuilt one is page {record}");
        }
        for t in ["t", "u"] {
            match exec(&format!("SELECT name FROM {t} WHERE id = {};", ROWS - 1), &mut o) {
                Outcome::Rows(rows) => assert_eq!(rows.len(), 1, "table '{t}': point lookup after crash recovery"),
                _ => panic!("table '{t}': expected rows"),
            }
        }
    }

    /// **C1, made true: after an index undo fails, the NEXT OPEN rebuilds the indexes, even when
    /// the log it opens is empty.**
    ///
    /// `open_recovered` rebuilds when `recover` replays something. `recover` returns `false` for an
    /// empty log, and a clean restart can leave one: `schema_log` is filled only by `log_ddl` in the
    /// running process, so a checkpoint in a process that ran no DDL re-declares nothing (the
    /// re-adversary's C1 correction). So "the next open rebuilds every tree" was false exactly when
    /// it was needed, and an entry that a failed index undo left on a freed slot would have outlived
    /// the restart. The fix is a durable marker beside the log (`<db>.wal.stale-indexes`), which
    /// `open_recovered` honours and then removes.
    ///
    /// FAILS at `d7891d5` at "left no marker". The failure is forced with a recorded write whose root
    /// is past end-of-file, as in `txn.rs`'s C1 test.
    #[test]
    fn a_failed_index_undo_makes_the_next_open_rebuild_even_when_the_log_is_empty() {
        use crate::execution::executor::run;
        use crate::parser::{parser::Parser, scanner::Scanner};

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("stale.db");
        let marker = PathBuf::from(format!("{}.wal.stale-indexes", db.display()));
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            for sql in ["CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", "INSERT INTO t VALUES (1, 10);"] {
                let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
                let mut p = Parser::new(tokens);
                let mut stmts = p.parse();
                run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), &mut Session::new())
                    .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
        }
        {
            // A second "process": it recovers and rebuilds, and its own checkpoint re-declares
            // nothing, because it ran no DDL.
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).unwrap();
            let t = o.txn.begin().unwrap();
            o.txn.record_primary_write(t, Arc::new(std::sync::atomic::AtomicU32::new(1_000_000)), Value::Integer(5), None);
            o.txn.abort(t).expect("the transaction ended, so its abort reports success");
            assert!(marker.exists(), "a failed index undo left no marker, so a clean restart would not rebuild");
            o.txn.checkpoint().unwrap();
        }
        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock).unwrap();
        assert!(!o.recovered, "premise failed: the log was not empty, so recovery alone would have rebuilt");
        assert!(!marker.exists(), "the marker survived the open, so the rebuild it asks for did not run");
    }

    // ---------------------------------------------------------------------------------------------
    // D216 — a clean restart must not rebuild every index.
    //
    // `checkpoint_locked` truncates the log and then re-appends this process's DDL (`replay_schema`)
    // and run declarations (`replay_runs`) as transaction-0 records, so a CLEAN close leaves a log
    // that is not empty. `recover` treated any non-empty log as work, so `open_recovered` rebuilt
    // every tree from the heap, O(rows), on the first restart after any process that ran DDL or bound
    // a run. The rule these tests pin: the trees are stale only when the log holds a DATA record
    // (a heap insert, delete or update, or a CLR), whether or not redo had to apply it, and only if
    // no index page can reach the disk ahead of the log records it depends on.
    // ---------------------------------------------------------------------------------------------

    /// One statement through the executor, in `session`, so a `BEGIN` stays open across calls.
    fn d216_sql(o: &mut OpenedDatabase, session: &mut Session, sql: &str) -> Result<crate::execution::executor::Outcome, FerroError> {
        use crate::parser::{parser::Parser, scanner::Scanner};
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse errors in `{sql}`: {:?}", p.errors);
        crate::execution::executor::run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), session)
    }

    /// The rows a statement returned, outside any transaction.
    fn d216_rows(o: &mut OpenedDatabase, sql: &str) -> Vec<Vec<Value>> {
        match d216_sql(o, &mut Session::new(), sql) {
            Ok(crate::execution::executor::Outcome::Rows(rows)) => rows,
            Ok(_) => panic!("`{sql}` did not return rows"),
            Err(e) => panic!("`{sql}` failed: {e}"),
        }
    }

    /// Every record in the retained log, buffered ones included, as (lsn, transaction, kind).
    fn d216_log(wal: &WalManager) -> Vec<(u64, u64, RecKind)> {
        let mut out = Vec::new();
        let end = wal.next_lsn.load(Ordering::SeqCst);
        let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
        while lsn < end {
            let (rec, next) = wal.read_record(lsn).unwrap();
            out.push((rec.lsn, rec.txn_id, rec.kind));
            lsn = next;
        }
        out
    }

    /// Whether every record is a transaction-0 re-declaration, which is all a clean close leaves.
    fn d216_only_declarations(log: &[(u64, u64, RecKind)]) -> bool {
        log.iter().all(|(_, txn, kind)| *txn == 0 && matches!(kind, RecKind::Ddl { .. } | RecKind::RunIdentity { .. }))
    }

    /// The log's (base, end).
    fn d216_bounds(wal: &WalManager) -> (u64, u64) {
        (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst))
    }

    /// D216's fixture: table `t` with a secondary index on `v` and two committed rows, closed CLEANLY
    /// the way `cli::run_cli` exits, with a checkpoint. Returns the log's bounds as the close left
    /// them.
    ///
    /// Checks the premise every D216 test stands on: the log is NOT empty (an empty log never
    /// rebuilt, so it could not show the defect), and it holds nothing but re-declarations.
    fn d216_cleanly_closed(db: &Path) -> (u64, u64) {
        let lock = DbLock::acquire(db).unwrap();
        let mut o = open_recovered(db, &lock).unwrap();
        for sql in [
            "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);",
            "CREATE INDEX iv ON t (v);",
            "INSERT INTO t VALUES (1, 10);",
            "INSERT INTO t VALUES (2, 20);",
        ] {
            d216_sql(&mut o, &mut Session::new(), sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
        }
        o.txn.checkpoint().unwrap();
        let log = d216_log(&o.wal);
        assert!(!log.is_empty(), "premise failed: the clean close left an empty log, and an empty log never rebuilt");
        assert!(d216_only_declarations(&log), "premise failed: the clean close left more than re-declarations: {log:?}");
        d216_bounds(&o.wal)
    }

    /// Write `t`'s primary root to disk through `flush_page` (the same write-back, `wal_gate` then
    /// `DiskManager::write`, that an eviction does) and check that the page on disk carries `key`.
    /// `t` is small enough that its primary tree is one leaf, and the check says so.
    fn d216_flush_primary_root_holding(o: &OpenedDatabase, key: i32) {
        use crate::storage::index_page::{BPlusTreeLeafPage, BPLUS_LEAF_TYPE};
        let root = o.catalog.root_cell("t", None).expect("CREATE TABLE seeds a cell").load(Ordering::SeqCst);
        o.bp.flush_page(root).unwrap();
        let on_disk = o.bp.disk_manager.read(root).unwrap();
        assert_eq!(on_disk[0], BPLUS_LEAF_TYPE, "premise failed: t's primary root is not a leaf, so writing it did not write the entry");
        let leaf = BPlusTreeLeafPage::<Value, RecordId>::deserialize(on_disk).unwrap();
        assert!(leaf.key_arr.contains(&Value::Integer(key)), "premise failed: the index page on disk does not carry key {key}");
    }

    /// **D216 — a clean restart after DDL must not rebuild the indexes.**
    ///
    /// A `Ddl` record changes no page (recovery does not replay DDL; the catalog lives outside the
    /// WAL), so there is nothing for a tree to be stale against. Observed through `recovered`, which
    /// is what `open_recovered` rebuilds on (with the marker, absent here). Then the trees the open
    /// did not rebuild must answer.
    ///
    /// ⚖ Amended in this lane before landing: the first version also asserted that the open left
    /// the log's bounds unchanged. The open now checkpoints any non-empty log without rebuilding
    /// (see `open_recovered`, D216 and D227), so that instrument no longer tells a rebuild from no
    /// rebuild. Its other job, that recovery appends nothing under transaction 0, moved to
    /// `recovery_appends_nothing_for_a_log_of_declarations`, which calls `recover` directly.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at `!o.recovered`.
    #[test]
    fn a_clean_restart_after_ddl_does_not_rebuild_the_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("clean.db");
        d216_cleanly_closed(&db);

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        assert!(
            !o.recovered,
            "a clean restart whose log held only DDL re-declarations reported a recovery, so the open rebuilt every index (D216)"
        );
        assert_eq!(d216_rows(&mut o, "SELECT v FROM t WHERE id = 2;"), vec![vec![Value::Integer(20)]], "a lookup by key");
        assert_eq!(d216_rows(&mut o, "SELECT id FROM t WHERE v = 10;"), vec![vec![Value::Integer(1)]], "a lookup by the indexed value");
        match d216_sql(&mut o, &mut Session::new(), "INSERT INTO t VALUES (2, 99);") {
            Err(FerroError::Constraint(m)) if m.contains("duplicate primary key") => {}
            Err(e) => panic!("a duplicate of a committed key was refused, but not as a duplicate: {e}"),
            Ok(_) => panic!("a duplicate of a committed key was ADMITTED"),
        }
    }

    /// **D216, the run half — a clean restart after a process that declared an agent run.**
    ///
    /// `replay_runs` re-declares every run at the head of the new log, as `replay_schema` does for
    /// DDL, so a process that served one attributed MERGE also left a non-empty log at a clean
    /// close. The middle process here runs no DDL, so the log it leaves holds run declarations only.
    ///
    /// ⚖ Amended in this lane before landing: the log-bounds assertion is withdrawn for the reason
    /// given on `a_clean_restart_after_ddl_does_not_rebuild_the_indexes`.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at `!o.recovered`.
    #[test]
    fn a_clean_restart_after_a_run_declaration_does_not_rebuild_the_indexes() {
        use crate::{branch::types::BranchId, provenance::{ProvId, RunEntity}};

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("runs.db");
        d216_cleanly_closed(&db);
        {
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).unwrap();
            let run = RunEntity::new(ProvId(1), "restock-agent", "run-1", "a-model", "v1", [0u8; 32], 1_700_000_000_000, BranchId::new(1, 0));
            o.txn.declare_run(run).unwrap();
            o.txn.checkpoint().unwrap();
            let log = d216_log(&o.wal);
            assert!(
                log.iter().any(|(_, _, kind)| matches!(kind, RecKind::RunIdentity { .. })),
                "premise failed: the clean close re-declared no run: {log:?}"
            );
            assert!(d216_only_declarations(&log), "premise failed: the clean close left more than re-declarations: {log:?}");
        }

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        assert!(
            !o.recovered,
            "a clean restart whose log held only run declarations reported a recovery, so the open rebuilt every index (D216)"
        );
        assert_eq!(d216_rows(&mut o, "SELECT v FROM t WHERE id = 1;"), vec![vec![Value::Integer(10)]], "a lookup by key");
    }

    /// **D216 — a log with transactions but no data records rebuilds nothing, and recovery still
    /// reads it.**
    ///
    /// A transaction that wrote nothing leaves `Begin` and `Commit`, which change no page, so no tree
    /// can be stale against them. But `recover` must still scan them: the WAL header's transaction
    /// id moves only at a checkpoint, so it is recovery's analysis that raises the watermark past
    /// the ids a crashed process issued. A fix that returned early for a log with no data records
    /// would hand the same id out twice. The second assertion pins that.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at `!o.recovered`; the watermark
    /// assertion holds there.
    #[test]
    fn a_log_with_no_data_records_rebuilds_nothing_and_still_raises_the_id_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idle.db");
        d216_cleanly_closed(&db);
        let committed = {
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).unwrap();
            let t = o.txn.begin().unwrap();
            o.txn.commit(t).unwrap();
            // The crash: no checkpoint, so the header still holds the id it held at open.
            t
        };

        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock).unwrap();
        assert!(
            !o.recovered,
            "a log holding a transaction that wrote nothing reported a recovery, so the open rebuilt every index"
        );
        let next = o.txn.begin().unwrap();
        assert!(
            next > committed,
            "recovery did not raise the id watermark past transaction {committed}, which the crashed process committed; \
             the next transaction was given id {next}"
        );
    }

    /// **D216 control — a crashed, uncommitted heap write still forces the rebuild.**
    ///
    /// The loser's records are made durable by hand and its primary-index page is written to disk,
    /// so the tree on disk names a row that recovery then undoes. Only a rebuild removes that entry;
    /// without one, key 3 fails every lookup and every INSERT (D202's crash half,
    /// `tests/pgserver_crash_rebuilds_indexes.rs`).
    ///
    /// Passes at `00f4c39`, where any non-empty log rebuilt, and must keep passing: the loser's
    /// `HeapInsert` and the CLR that undoes it are data records. (⚖ Amended before landing: a
    /// `base == end` check for "the rebuild's checkpoint ran" is withdrawn. The open now checkpoints
    /// without a rebuild too, and D227 re-declares the tables at that checkpoint. The INSERT and
    /// the lookup below are what prove the rebuild.)
    #[test]
    fn a_crashed_uncommitted_heap_write_still_forces_the_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("loser.db");
        d216_cleanly_closed(&db);
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let mut s = Session::new();
            for sql in ["BEGIN;", "INSERT INTO t VALUES (3, 30);"] {
                d216_sql(&mut o, &mut s, sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            o.wal.flush().unwrap();
            d216_flush_primary_root_holding(&o, 3);
            // The crash: no COMMIT, no checkpoint.
        }

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        assert!(o.recovered, "a crash left an uncommitted heap write in the log, and the open reported no recovery");
        assert_eq!(d216_rows(&mut o, "SELECT v FROM t WHERE id = 3;"), Vec::<Vec<Value>>::new(), "the undone row is visible by key");
        d216_sql(&mut o, &mut Session::new(), "INSERT INTO t VALUES (3, 31);")
            .unwrap_or_else(|e| panic!("key 3 was never committed, and inserting it after the crash failed: {e}"));
        assert_eq!(d216_rows(&mut o, "SELECT v FROM t WHERE id = 3;"), vec![vec![Value::Integer(31)]], "the new row by key");
    }

    /// **D216 control — a data record counts even when redo SKIPS it.**
    ///
    /// "Recovery replayed a data record" has to mean the record is in the log, not that redo changed
    /// a page. Here a committed row's heap page reaches the disk and its index page does not, which
    /// is an order eviction can produce. Redo then skips the record, because the page's LSN is
    /// already at it, and the tree on disk still lacks the key. A rule that counted only the records
    /// redo applied would skip the rebuild and lose the row by key.
    ///
    /// Passes at `00f4c39` and must keep passing.
    #[test]
    fn a_committed_row_whose_heap_page_reached_disk_still_forces_the_rebuild() {
        use crate::storage::index_page::BPlusTreeLeafPage;

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("skipped.db");
        d216_cleanly_closed(&db);
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            d216_sql(&mut o, &mut Session::new(), "INSERT INTO t VALUES (3, 30);").unwrap();
            let cell = o.catalog.root_cell("t", None).expect("CREATE TABLE seeds a cell");
            let root = cell.load(Ordering::SeqCst);
            let rid = BPlusTreeManager::<Value, RecordId>::open_shared(cell, o.bp.clone())
                .search(&Value::Integer(3))
                .unwrap()
                .expect("row 3 is in the tree in memory");
            o.bp.flush_page(rid.page_id).unwrap();

            let insert_lsn = d216_log(&o.wal)
                .iter()
                .rev()
                .find(|(_, _, kind)| matches!(kind, RecKind::HeapInsert { .. }))
                .map(|(lsn, ..)| *lsn)
                .expect("the INSERT logged a HeapInsert");
            let heap_on_disk = Page::deserialize(o.bp.disk_manager.read(rid.page_id).unwrap()).unwrap();
            assert!(heap_on_disk.read(rid.slot_num as usize).is_ok(), "premise failed: row 3 did not reach disk in its heap page");
            assert!(
                heap_on_disk.lsn >= insert_lsn,
                "premise failed: the heap page on disk (lsn {}) is behind its record (lsn {insert_lsn}), so redo would apply it, not skip it",
                heap_on_disk.lsn
            );
            let index_on_disk = BPlusTreeLeafPage::<Value, RecordId>::deserialize(o.bp.disk_manager.read(root).unwrap()).unwrap();
            assert!(
                !index_on_disk.key_arr.contains(&Value::Integer(3)),
                "premise failed: the index page reached disk too, so the tree would find row 3 without a rebuild"
            );
            // The crash: the commit made the log durable; nothing else is written.
        }

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        assert!(
            o.recovered,
            "a committed heap write was in the log, and the open reported no recovery because redo found its page already current"
        );
        assert_eq!(d216_rows(&mut o, "SELECT v FROM t WHERE id = 3;"), vec![vec![Value::Integer(30)]], "a committed row is missing by key");
        match d216_sql(&mut o, &mut Session::new(), "INSERT INTO t VALUES (3, 99);") {
            Err(FerroError::Constraint(m)) if m.contains("duplicate primary key") => {}
            Err(e) => panic!("a second row 3 was refused, but not as a duplicate: {e}"),
            Ok(_) => panic!("a second row 3 was ADMITTED"),
        }
    }

    /// **D216 control — the stale-indexes marker still forces the rebuild when the log holds only
    /// re-declarations.**
    ///
    /// This is the state D216 makes new: `recover` reports no work for this log (the first test here
    /// pins that on this same fixture), so the marker is the only thing left that can ask for a
    /// rebuild. `a_failed_index_undo_makes_the_next_open_rebuild_even_when_the_log_is_empty` covers
    /// the empty log; this covers the non-empty one. The marker is planted by hand, since only
    /// where it lives matters to the open.
    ///
    /// Passes at `00f4c39`, where the log alone rebuilt, and must keep passing. (⚖ Amended before
    /// landing: a `base == end` check is withdrawn for the reason given on the loser control. The
    /// marker is removed only inside the rebuild's branch, so its absence is the proof.)
    #[test]
    fn the_stale_marker_still_forces_the_rebuild_over_a_log_of_declarations() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("marked.db");
        d216_cleanly_closed(&db);
        let marker = PathBuf::from(format!("{}.wal.stale-indexes", db.display()));
        std::fs::write(&marker, "planted by the test\n").unwrap();

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        assert!(!marker.exists(), "the marker survived the open, so the rebuild it asks for did not run");
        assert_eq!(d216_rows(&mut o, "SELECT v FROM t WHERE id = 2;"), vec![vec![Value::Integer(20)]], "a lookup by key");
    }

    /// **D216's precondition — an index page reaches the disk only after the log records it depends
    /// on.**
    ///
    /// Index pages carry no LSN (`index_page.rs` writes 0), so `wal_gate` used to skip them: an
    /// eviction could write a leaf holding an uncommitted key while the `Begin` and `HeapInsert`
    /// behind that key were still only in the log buffer. After a crash, no record of the
    /// transaction survives, the log shows no work, and the key names a slot the heap never got.
    /// This was already true at `00f4c39` whenever the surviving log was empty, which is how the
    /// middle process here leaves it there (its open rebuilds and checkpoints, and it declared
    /// nothing). D216's rule, "rebuild only for a data record", is sound only once this holds.
    ///
    /// The structural assertion is the rule itself; the reopen is its consequence.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at the `flushed_lsn` assertion.
    #[test]
    fn an_index_page_reaches_disk_only_after_the_log_records_it_depends_on() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("gate.db");
        d216_cleanly_closed(&db);
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let mut s = Session::new();
            for sql in ["BEGIN;", "INSERT INTO t VALUES (3, 30);"] {
                d216_sql(&mut o, &mut s, sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            d216_flush_primary_root_holding(&o, 3);
            assert_eq!(
                o.wal.flushed_lsn.load(Ordering::SeqCst),
                o.wal.next_lsn.load(Ordering::SeqCst),
                "an index page reached the disk while log records appended before it were still only in memory; \
                 a crash now leaves an index entry that no surviving record accounts for"
            );
            // The crash: no COMMIT, no checkpoint.
        }

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        assert!(o.recovered, "the loser's records were durable, and the open reported no recovery");
        d216_sql(&mut o, &mut Session::new(), "INSERT INTO t VALUES (3, 31);")
            .unwrap_or_else(|e| panic!("key 3 was never committed, and inserting it after the crash failed: {e}"));
        assert_eq!(d216_rows(&mut o, "SELECT v FROM t WHERE id = 3;"), vec![vec![Value::Integer(31)]], "the new row by key");
    }

    /// **The gate's negative control: a page that carries its own LSN keeps the precise gate.**
    ///
    /// A heap page is written only after the log is durable up to ITS LSN, and a record appended
    /// after that is not forced out with it. Here the INSERT's commit made the log durable through
    /// the `Commit`, the `TxnEnd` after it is still buffered, and writing the heap page must leave
    /// it buffered. A gate that flushed the whole log for every page would fail this, and would pay
    /// a log write on every heap eviction to do so.
    ///
    /// Passes at `00f4c39` and must keep passing.
    #[test]
    fn a_page_whose_own_record_is_durable_does_not_flush_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("precise.db");
        d216_cleanly_closed(&db);

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        d216_sql(&mut o, &mut Session::new(), "INSERT INTO t VALUES (3, 30);").unwrap();
        let rid = BPlusTreeManager::<Value, RecordId>::open_shared(o.catalog.root_cell("t", None).expect("a cell for t"), o.bp.clone())
            .search(&Value::Integer(3))
            .unwrap()
            .expect("row 3 is in the tree");
        let flushed = o.wal.flushed_lsn.load(Ordering::SeqCst);
        assert!(
            flushed < o.wal.next_lsn.load(Ordering::SeqCst),
            "premise failed: nothing is waiting in the log buffer, so a flush it did not need could not be seen"
        );
        o.bp.flush_page(rid.page_id).unwrap();
        assert_eq!(
            o.wal.flushed_lsn.load(Ordering::SeqCst),
            flushed,
            "writing a heap page whose own record was already durable flushed the log anyway"
        );
    }

    /// **The same control for a page with no LSN: an index page whose changes' records are durable
    /// does not flush the log.**
    ///
    /// The gate's first version flushed the whole log for every such page (the D216 adversary's
    /// F4). A commit leaves its `TxnEnd` in the buffer, so the buffer is almost never empty, and
    /// every eviction of a dirty index, directory or catalog page paid a log write and an fsync,
    /// in a 1024-frame pool. The page needs only the records appended before it last changed.
    /// Here those are the INSERT's, which its commit made durable, so writing the leaf must leave
    /// the `TxnEnd` buffered.
    ///
    /// Passes at `00f4c39`, where the gate skipped these pages. Must keep passing.
    #[test]
    fn an_index_page_whose_records_are_durable_does_not_flush_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("precise_index.db");
        d216_cleanly_closed(&db);

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        d216_sql(&mut o, &mut Session::new(), "INSERT INTO t VALUES (3, 30);").unwrap();
        let flushed = o.wal.flushed_lsn.load(Ordering::SeqCst);
        assert!(
            flushed < o.wal.next_lsn.load(Ordering::SeqCst),
            "premise failed: nothing is waiting in the log buffer, so a flush it did not need could not be seen"
        );
        d216_flush_primary_root_holding(&o, 3);
        assert_eq!(
            o.wal.flushed_lsn.load(Ordering::SeqCst),
            flushed,
            "writing an index page whose changes were already durable flushed the log anyway"
        );
    }

    /// **D216 adversary F1: a commit whose earlier records were already durable must make its own
    /// `Commit` durable.**
    ///
    /// `WalManager::flush_up_to` returned early when `flushed_lsn >= lsn`. But an LSN is where a
    /// record STARTS, and `flushed_lsn` is one past the last durable byte, so the record first in an
    /// empty buffer starts exactly at `flushed_lsn` and was never written. `commit` then returned
    /// `Ok` with its `Commit` only in memory, and a crash undid a transaction whose caller had been
    /// told it committed. Any write-back that flushes the log between a transaction's last record
    /// and its commit sets this up. The flush here is the one such a write-back does, and D216's
    /// gate adds more of them.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at the row count (0 against 1).
    #[test]
    fn a_commit_is_durable_when_everything_before_it_was_already_flushed() {
        let dir = tempfile::tempdir().unwrap();
        let (dir_root, rid);
        {
            let (bp, wal, txn) = setup(dir.path());
            let t = txn.begin().unwrap();
            let mut heap = HeapFileManager::new(bp.clone()).unwrap();
            dir_root = heap.first_directory_page_id;
            heap.set_transaction(txn.clone(), t);
            rid = heap.insert(Tuple::new(vec![4, 5, 6])).unwrap();
            wal.flush().unwrap();
            txn.commit(t).unwrap();
            // The crash: nothing else is written.
        }
        let (bp, _wal, txn) = setup(dir.path());
        recover(&txn).unwrap();
        let heap = HeapFileManager::open(dir_root, bp.clone());
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            rows.len(),
            1,
            "commit returned Ok, and after a crash the transaction was undone: its Commit never reached the log"
        );
        assert_eq!(heap.read(rid).unwrap().data, vec![4, 5, 6]);
    }

    /// **D216 adversary F1, the page half: a heap page is written only after its own record, even
    /// when that record was the first in an empty buffer.** The gate asks `flush_up_to(page LSN)`,
    /// and the page LSN is where its record starts, so this is the same off-by-one: it let a heap
    /// page reach the disk ahead of the record that describes it, the one rule write-ahead logging
    /// exists to enforce.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at the `flushed_lsn` assertion.
    #[test]
    fn a_heap_page_waits_for_its_own_record_when_it_starts_at_the_flushed_point() {
        let dir = tempfile::tempdir().unwrap();
        let (bp, wal, txn) = setup(dir.path());
        let t = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        heap.set_transaction(txn.clone(), t);
        wal.flush().unwrap();
        let flushed = wal.flushed_lsn.load(Ordering::SeqCst);
        let rid = heap.insert(Tuple::new(vec![1, 2, 3])).unwrap();
        let (first, _) = wal.read_record(flushed).unwrap();
        assert!(
            matches!(first.kind, RecKind::HeapInsert { .. }),
            "premise failed: the record at the flushed point is {:?}, not the insert",
            first.kind
        );
        bp.flush_page(rid.page_id).unwrap();
        assert!(
            wal.flushed_lsn.load(Ordering::SeqCst) > flushed,
            "a heap page reached the disk while its own HeapInsert was still only in memory"
        );
        txn.abort(t).unwrap();
    }

    /// **D216 — recovery appends nothing to a log of declarations: transaction 0 is not a loser.**
    ///
    /// DDL and run declarations are logged under id 0, which never commits, so recovery took it for
    /// a loser and "aborted" it: an `Abort` and a `TxnEnd` under id 0 that undid nothing. `recover`
    /// is called directly, because `open_recovered` checkpoints a non-empty log straight after it
    /// and would discard both records. A change-feed decoder reading them first reported
    /// transaction 0 as aborted (`Decoded::aborted`).
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at the second bounds assertion.
    #[test]
    fn recovery_appends_nothing_for_a_log_of_declarations() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("zero.db");
        let closed = d216_cleanly_closed(&db);
        let file = OpenOptions::new().read(true).write(true).open(&db).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(PathBuf::from(format!("{}.wal", db.display()))).unwrap());
        let txn = TxnManager::new(wal.clone(), bp.clone());
        bp.attach_wal(wal.clone());
        assert_eq!(d216_bounds(&wal), closed, "premise failed: the reopened log is not the one the clean close left");
        let found = recover(&txn).unwrap();
        assert_eq!(
            d216_bounds(&wal),
            closed,
            "recovery appended to a log of declarations: it took transaction 0 for a loser and aborted it"
        );
        assert!(!found, "recovery reported work for a log of declarations");
    }

    /// **D227 — after a restart, a checkpoint still declares every table.**
    ///
    /// `schema_log` was filled only by `log_ddl` in the running process, so a restarted process's
    /// checkpoints re-declared nothing, and a reader starting at such a base met rows of a table the
    /// log never named. The shipped feeds seed their decoders from the catalog
    /// (`LogicalDecoder::new`) and resolve anyway. A blank decoder, which is what reading an
    /// archived log or a log whose database is gone amounts to, reported every such row as
    /// unresolved.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at the `unresolved` assertion.
    #[test]
    fn a_checkpoint_after_a_restart_still_declares_every_table() {
        use crate::replication::logical::LogicalDecoder;

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("declared.db");
        d216_cleanly_closed(&db);

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        // The restarted process's own checkpoint, before it has run any DDL.
        o.txn.checkpoint().unwrap();
        d216_sql(&mut o, &mut Session::new(), "INSERT INTO t VALUES (3, 30);").unwrap();
        let (base, end) = d216_bounds(&o.wal);
        let out = LogicalDecoder::blank().decode(&o.wal, base, end).unwrap();
        assert!(
            out.unresolved.is_empty(),
            "a reader starting at a checkpoint taken after the restart cannot name the table its rows belong to: {:?}",
            out.unresolved
        );
        assert!(out.events.iter().any(|e| e.table == "t"), "premise failed: the INSERT did not decode as a change to t");
    }

    /// **D216 adversary F3: transaction id 0 is never handed out.**
    ///
    /// Recovery no longer takes id 0 for a loser, because DDL and run declarations are logged under
    /// it. So a real transaction 0 that crashed would never be undone, and every log reader already
    /// takes a record under id 0 for a declaration (`Snapshot::already_delivered`,
    /// `replication::logical`). A fresh log's header starts ids at 1, but `WalManager::truncate`
    /// writes whatever it is given, and a snapshot install passes 0 (`consensus::snapshot`).
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `00f4c39` at `assert_ne!`: the first id is 0.
    #[test]
    fn transaction_id_zero_is_never_handed_out() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (_bp, wal, _txn) = setup(dir.path());
            wal.truncate(0).unwrap();
        }
        let (_bp, wal, txn) = setup(dir.path());
        assert_eq!(wal.header_txn_id, 0, "premise failed: the log's header does not say 0");
        recover(&txn).unwrap();
        let t = txn.begin().unwrap();
        assert_ne!(t, 0, "a transaction was given id 0, which recovery never undoes and every log reader takes for a declaration");
    }
}
