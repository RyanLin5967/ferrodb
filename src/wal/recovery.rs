use std::{collections::{BTreeMap, HashMap, HashSet}, fs::OpenOptions, path::Path, sync::{Arc, atomic::Ordering}, time::{Duration, Instant}};

use crate::{buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, column::Value}, error::FerroError, storage::{db_lock::DbLock, disk_manager::DiskManager, heap_file_manager::{HeapFileManager, RecordId}, heap_page::Page, index::BPlusTreeManager, index_fulltext::{indexed_text, post_tokens}, index_page::{entry_too_large, first_entry_over_bound, EntryOf, RECOVERY_REMEDY}, tuple::Tuple}, wal::{log::{RecKind, WalManager}, txn::{TxnEntry, TxnManager, TxnStatus}}};

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

/// **D225 — before any tree is freed, refuse by name a heap row whose index entry the rebuilt
/// trees would refuse.**
///
/// Every write path refuses an entry over `MAX_ENTRY_BYTES`, so this build cannot commit such a
/// row, but a build before D225 could: its count split admitted any entry that happened to fit.
/// "Every write path" includes an ALTER that widens an indexed column, whose rewrite writes no
/// index entry at all and would otherwise leave the heap holding values the rebuild widens: its
/// `prepare_rewrite` asks the bound of every widened secondary entry first (review 6 H1; before
/// that, this premise was false).
/// The rebuild below frees each old tree before refilling a fresh one, table by table, and the
/// catalog is persisted only at the end. A refusal part-way would leave the in-memory catalog
/// naming new trees, the persisted one naming freed pages, and the next open freeing them again.
/// So every row of every table is asked first, for every entry the rebuild will make, and a
/// refusal leaves the database exactly as recovery found it. The cost is a second heap scan, at
/// recovery only.
///
/// The database then does not open in this build until the table is repaired with a build that
/// can still open it, `index_page::RECOVERY_REMEDY`. Deleting the row is not a repair: a deleted
/// tuple stays in the heap until its key is inserted again (then its dead version moves to the
/// table's history; D202, #16), nothing else removes it (there is no VACUUM), and this check reads
/// every tuple the heap holds, as the rebuild does.
/// That is deliberate: the alternative is admitting an entry for which a leaf split is not
/// guaranteed to exist.
fn refuse_rows_no_rebuilt_tree_admits(catalog: &Catalog, bp: &Arc<BufferPoolManager>, names: &[String]) -> Result<(), FerroError> {
    for name in names {
        let entry = catalog.tables.get(name).expect("name came from this map");
        let column = |col_name: &str| {
            entry.schema.columns.iter().position(|c| c.name == col_name).ok_or(FerroError::KeyNotFound)
        };
        let secondary = entry.indexes.iter().map(|i| column(&i.column_name)).collect::<Result<Vec<_>, _>>()?;
        let fulltext = entry.fulltext_indexes.iter().map(|i| column(&i.column_name)).collect::<Result<Vec<_>, _>>()?;
        let hfm = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
        for r in hfm.scan() {
            let (_, tuple) = r?;
            let vals = tuple.deserialize(&entry.schema)?;
            let refuse = |what: &str, len: usize| {
                let pk: String = format!("{:?}", vals[0]).chars().take(60).collect();
                FerroError::Constraint(format!(
                    "cannot rebuild the indexes of '{name}' after recovery: the row with primary key \
                     {pk} makes {what} that no rebuilt tree admits: {}. A build before D225 could \
                     store it. Nothing has been freed or rewritten. {RECOVERY_REMEDY} Then reopen.",
                    entry_too_large(len)
                ))
            };
            // Every entry the rebuild below makes, through the one builder of entry shapes
            // (`index_page::row_entry_sizes`, review 7 K9): the primary entry, each secondary
            // entry, and each posting.
            if let Some((of, len)) = first_entry_over_bound(&vals, true, &secondary, &fulltext)? {
                let what = match of {
                    EntryOf::Primary => "its primary entry".to_string(),
                    EntryOf::Secondary(col) => {
                        format!("its entry in the index on '{}'", entry.schema.columns[col].name)
                    }
                    EntryOf::Posting(col) => {
                        format!("a posting in the full-text index on '{}'", entry.schema.columns[col].name)
                    }
                };
                return Err(refuse(&what, len));
            }
        }
    }
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
    refuse_rows_no_rebuilt_tree_admits(catalog, bp, &names)?;
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

/// Wall time of each step of [`open_recovered`], in the order they run. **Observing only.**
///
/// READ-VS-N's restart arm (`bench/read_vs_n/PREREG.md`) times the whole production open, step by
/// step, and D204 made this function the one place these steps run — so they are timed HERE, where
/// every binary pays them, and handed back rather than re-measured around a copy. Nothing reads
/// these to decide anything.
#[derive(Clone, Copy, Debug, Default)]
pub struct BootTimings {
    /// The file, the buffer pool, the WAL and the transaction manager.
    pub files: Duration,
    /// **D239's floor read, kept out of `files` (READ-VS-N, PREREG A19).** D239 adds
    /// `ArenaPageStore::reserve_persisted_floor` between the disk manager and the buffer pool: it
    /// reads and checksums the whole `{db}.arena` image, O(arena extents), where `files` is flat.
    /// ZERO until that call is merged here; the merge times it and subtracts it from `files`.
    pub floor: Duration,
    /// [`recover`]: redo and undo of the heap records.
    pub recover: Duration,
    /// `Catalog::open`, or `Catalog::create` for a new file.
    pub catalog: Duration,
    /// [`rebuild_indexes`], the checkpoint after it and the stale-index marker's removal. Zero when
    /// recovery replayed nothing and no marker was left.
    ///
    /// ⚠ **That is NOT every open after a clean shutdown (D216, lead-verified 2026-09-24).** A clean
    /// close's `TxnManager::checkpoint` truncates the log and then re-appends every retained DDL and
    /// run declaration, so the next open finds records, recovers, and rebuilds every index — after
    /// any process that ran DDL or merged an attributed agent session. This field is how READ-VS-N
    /// measures that cost before D216's fix and its absence after.
    pub rebuild: Duration,
}

/// A database file, opened, recovered, and with every index rebuilt from the recovered heap.
pub struct OpenedDatabase {
    pub bp: Arc<BufferPoolManager>,
    pub wal: Arc<WalManager>,
    pub txn: Arc<TxnManager>,
    pub catalog: Catalog,
    /// Whether the log held anything to replay, which is also whether the trees were rebuilt.
    pub recovered: bool,
    /// How long each step took. See [`BootTimings`].
    pub timings: BootTimings,
}

/// **D204 — THE way to open a database file.** Every binary calls this; none spells the sequence
/// out for itself (`tests/open_path_allowlist.rs` enforces that).
///
/// The order is the whole content:
/// 1. open the file, the buffer pool, the WAL and the transaction manager, and attach the WAL;
/// 2. [`recover`]: redo and undo the HEAP records, and nothing else;
/// 3. open the catalog, or create it for a new file;
/// 4. if recovery replayed anything, [`rebuild_indexes`] from the recovered heap, then checkpoint.
///    The rebuild ends by repointing the shared root cells at the trees it built (D205). Without
///    that, every statement after recovery descends the trees the rebuild freed. The checkpoint is
///    there because the rebuilt trees and the catalog page are then on disk, so the log that
///    produced them has nothing left to say; without it, the next open would replay the same
///    records and rebuild every tree again (reasoning from `b9a0a75`). Step 4 also runs when a
///    marker says an earlier rollback's index undo failed (`TxnManager::mark_indexes_stale`), even
///    if the log is empty.
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
    let mut timings = BootTimings::default();
    let t = Instant::now();
    let existed = db_path.exists();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(db_path)
        .map_err(|e| FerroError::Io(format!("open {}: {e}", db_path.display())))?;
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file)?)));
    // D280: refuses a data file whose pages carry LSNs this log never issued (a restored backup, a
    // replica's file, a lost `<db>.wal`), before anything else is created beside it. Every binary
    // opens through here, so the CLI and pgserver share the refusal as well as the sequence.
    let wal = Arc::new(WalManager::open_for_database(db_path, &bp.disk_manager)?);
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    timings.files = t.elapsed();
    let t = Instant::now();
    let recovered = recover(&txn)?;
    timings.recover = t.elapsed();
    let t = Instant::now();
    let mut catalog = if existed {
        Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID)?
    } else {
        Catalog::create(bp.clone())?
    };
    timings.catalog = t.elapsed();
    // D205 C1 correction: a rollback in an earlier process whose index undo failed left a marker
    // (`TxnManager::mark_indexes_stale`), because its orphaned entries are on disk and an empty log
    // would not trigger the rebuild below. The marker is removed only after the rebuilt trees are
    // checkpointed. If removal fails, the next open simply rebuilds again, which is harmless.
    let marker = crate::wal::txn::stale_indexes_marker(&wal.path);
    let stale = marker.exists();
    let t = Instant::now();
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
    timings.rebuild = t.elapsed();
    Ok(OpenedDatabase { bp, wal, txn, catalog, recovered, timings })
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, path::{Path, PathBuf}};

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

    /// **D236 (found as the D216 adversary's F1): a commit whose earlier records were already
    /// durable must make its own `Commit` durable.**
    ///
    /// `WalManager::flush_up_to` returned early when `flushed_lsn >= lsn`. An LSN is where a record
    /// STARTS, and `flushed_lsn` is one past the last durable byte, so the record first in an empty
    /// buffer starts exactly at `flushed_lsn` and was never written. `commit` then returned `Ok`
    /// with its `Commit` only in memory, and a crash undid a transaction whose caller had been told
    /// it committed. Any flush between a transaction's last record and its commit sets this up. At
    /// `9aa6968` the reachable one is the buffer pool's eviction gate, draining the whole log to
    /// write back a dirty heap page (`tests/d236_commit_after_a_drain_survives_kill9.rs` reaches it
    /// through the CLI). The `wal.flush()` below is that drain, spelled directly.
    ///
    /// Ported from `d216-clean-restart` (`c528509`) unchanged: `setup`, `HeapFileManager`,
    /// `recover` and `flush` have the same signatures at `9aa6968`.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `9aa6968` at the row count (0 against 1).
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

    /// **D236, the page half: a heap page is written only after its own record, even when that
    /// record was the first in an empty buffer.** The gate asks `flush_up_to(page LSN)`, and the
    /// page LSN is where its record starts (`heap_file_manager.rs` stamps `page.lsn = lsn` with the
    /// value `append` returned, in insert, update and both delete paths), so this is the same
    /// off-by-one. It let a heap page reach the disk ahead of the record that describes it, which
    /// is the one rule write-ahead logging exists to enforce.
    ///
    /// Ported from `d216-clean-restart` (`c528509`) unchanged.
    ///
    /// Pre-registered from source, UNBUILT: FAILS at `9aa6968` at the `flushed_lsn` assertion.
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
}
