use std::{collections::{BTreeMap, HashMap, HashSet}, fs::OpenOptions, path::{Path, PathBuf}, sync::{Arc, atomic::Ordering}};

use crate::{agent_sql::runtime::AgentRuntime, buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, column::Value}, error::FerroError, storage::{db_lock::DbLock, disk_manager::DiskManager, heap_file_manager::{HeapFileManager, RecordId}, heap_page::Page, index::BPlusTreeManager, index_fulltext::{indexed_text, post_tokens}, tuple::Tuple}, wal::{log::{DdlOp, RecKind, WalManager}, txn::{RetiredSlot, TxnEntry, TxnManager, TxnStatus}}};

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

    // F1: a log written before D213 (format 2) is replayed with ITS meaning: a forward `HeapDelete`
    // frees its slot, and nothing is owed a release. See `wal::log::VERSION`.
    let legacy = wal.is_legacy();
    // **D250: a record of a table that a LATER `DropTable` names is skipped, by redo, by the
    // directory repair and by the owed-release set.** A DROP frees its table's pages on disk at once
    // and only a truncation removes their records, and a truncation does not always happen: a WAL
    // pin cancels it, and the DROP's checkpoint can fail after the frees. Replayed, those records
    // would land on pages that are free, or already another table's: a reused page never flushed is
    // a zero page, `Page::empty` with LSN 0 below, and every record applies to it. The `DropTable`
    // record is durable before the first free (`TxnManager::drop_checkpointed`), so it is in the log
    // whenever the frees could have happened. LSN order, not the root number alone: a later owner of
    // the same page numbers writes after the DROP, and is never skipped.
    let dropped = dropped_roots(&records);
    let skipped = |kind: &RecKind, lsn: u64| {
        heap_root(kind).is_some_and(|root| dropped.get(&root).is_some_and(|&at| at > lsn))
    };
    let mut max_txn = 0u64;
    let mut last_lsn = HashMap::new();
    // The earliest record each transaction still has in the retained log. For a loser this is its
    // `Begin` unless a truncation cut above it, in which case it is the oldest record that
    // survives — which is the same thing the field means: the earliest point a reader would have
    // to start from to see everything this transaction did.
    let mut first_lsn: HashMap<u64, u64> = HashMap::new();
    let mut ended: HashSet<u64> = HashSet::new();
    let mut committed: HashSet<u64> = HashSet::new();
    // D213: per transaction, the slots its forward deletes retired and no `HeapRelease` has freed.
    let mut owed: HashMap<u64, Vec<RetiredSlot>> = HashMap::new();
    let mut touched = HashSet::new();
    // analysis
    for rec in &records {
        max_txn = max_txn.max(rec.txn_id);
        last_lsn.insert(rec.txn_id, rec.lsn);
        first_lsn.entry(rec.txn_id).or_insert(rec.lsn);
        if skipped(&rec.kind, rec.lsn) {
            // A dropped table's page: nothing owed, nothing touched (D250).
            continue;
        }
        match &rec.kind {
            RecKind::HeapDelete { dir_root, page_id, slot, .. } if !legacy => {
                owed.entry(rec.txn_id).or_default().push(RetiredSlot { dir_root: *dir_root, page_id: *page_id, slot: *slot });
            }
            RecKind::HeapRelease { page_id, slot, .. } => {
                if let Some(slots) = owed.get_mut(&rec.txn_id) {
                    slots.retain(|r| (r.page_id, r.slot) != (*page_id, *slot));
                }
            }
            _ => {}
        }
        match &rec.kind {
            RecKind::Commit | RecKind::TxnEnd => {
                ended.insert(rec.txn_id);
                if matches!(rec.kind, RecKind::Commit) {
                    committed.insert(rec.txn_id);
                }
            }
            RecKind::HeapDelete { dir_root, page_id, .. } | RecKind::HeapInsert { dir_root, page_id, .. } | RecKind::HeapUpdate { dir_root, page_id, .. }
            | RecKind::HeapRelease { dir_root, page_id, .. } => {
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

    // redo. A `Clr` goes in whole: `redo_one` applies the record it carries, and has to know it
    // came from a CLR (D213).
    for rec in &records {
        if skipped(&rec.kind, rec.lsn) {
            continue;
        }
        match &rec.kind {
            RecKind::HeapDelete { .. } | RecKind::HeapInsert { .. } | RecKind::HeapUpdate { .. }
            | RecKind::HeapRelease { .. } | RecKind::Clr { .. } => {
                redo_one(bp, rec.lsn, &rec.kind, !legacy)?;
            }
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

    // D213: finish the releases a crash cut off. A committed transaction's retired slots are freed
    // by `HeapRelease` records written after its `Commit`, and those wait in the log buffer for the
    // next flush. A crash in between leaves the slots retired, and nothing else would ever free
    // them. AFTER the directory repair (the adversary's F3): the directory is not logged, so a page
    // added since the last checkpoint is listed only once the repair has run, and each release
    // tells the directory its page's new free space. Before it, that update found no entry and was
    // counted and printed as a failed release. Ascending id, for the reason the losers are sorted.
    // A release that fails here waits in the pending list. `open_recovered`'s checkpoint then still
    // flushes every page and syncs, and keeps only the log, which is the record of it (F2, review 2's
    // N1). One that turns out to be a page/log mismatch is written to the quarantine file and dropped
    // (review 2's Q3, review 3's decision 6).
    let mut owed: Vec<(u64, Vec<RetiredSlot>)> = owed
        .into_iter()
        .filter(|(id, slots)| committed.contains(id) && !slots.is_empty())
        .collect();
    owed.sort_unstable_by_key(|(id, _)| *id);
    for (id, slots) in owed {
        txn.finish_releases(id, &slots);
    }
    Ok(true)
}

/// Apply one log record to the pages, for callers outside recovery — a replica applying a
/// primary's stream is doing redo, and should do it through the same code that recovery uses
/// rather than a second implementation that can drift from it.
///
/// Idempotent by page LSN: a record whose LSN is at or below the page's is skipped, which is what
/// makes a re-sent overlap after a reconnect harmless.
///
/// `kind` is the record as logged. A `Clr` is passed WHOLE, not unwrapped: a `HeapDelete` inside a
/// CLR frees its slot, while the same record outside one retires it (D213).
///
/// **A replica applies with version-3 meaning** (`wal::log::VERSION`). A primary on this binary
/// never ships a version-2 record: it replays and upgrades an older log before any transaction
/// runs, and the upgrade truncates the older records away. A replica on this binary fed by a
/// primary BEFORE D213 is a mixed-build pair. That primary's forward deletes free, while this
/// replica retires them. The first later insert into those bytes is refused here and the batch
/// fails, so the pair stops rather than diverging silently. The handshake does not refuse the
/// pair: that needs `REPL_VERSION` bumped, which is a lead decision (lane §19).
pub fn apply_redo(bp: &Arc<BufferPoolManager>, lsn: u64, kind: &RecKind) -> Result<(), FerroError> {
    redo_one(bp, lsn, kind, true)
}

/// `retire_forward_deletes` is false only for a log written before D213 (`WalManager::is_legacy`),
/// where a forward `HeapDelete` freed its slot at once.
fn redo_one(bp: &Arc<BufferPoolManager>, lsn: u64, kind: &RecKind, retire_forward_deletes: bool) -> Result<(), FerroError> {
    // A CLR is redone as the record it carries, with one difference that is the CLR's own. Its
    // `HeapDelete` undoes an insert, so it FREES the slot. A forward `HeapDelete` RETIRES it, as the
    // delete did when it ran: its transaction may still roll back, and if it committed, a
    // `HeapRelease` later in the log frees it (D213).
    let (op, compensation) = match kind {
        RecKind::Clr { redo, .. } => (redo.as_ref(), true),
        other => (other, false),
    };
    let page_id = match op {
        RecKind::HeapDelete { page_id, .. } | RecKind::HeapInsert { page_id, ..} | RecKind::HeapUpdate { page_id, ..}
        | RecKind::HeapRelease { page_id, .. } => *page_id,
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
    match op {
        RecKind::HeapDelete { slot, ..} if compensation => page.delete(*slot as usize)?,
        RecKind::HeapDelete { slot, ..} if !retire_forward_deletes => page.delete(*slot as usize)?,
        RecKind::HeapDelete { slot, ..} => page.retire(*slot as usize)?,
        RecKind::HeapRelease { slot, .. } => page.release(*slot as usize)?,
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

/// A database file, opened, recovered, and with every index rebuilt from the recovered heap.
pub struct OpenedDatabase {
    pub bp: Arc<BufferPoolManager>,
    pub wal: Arc<WalManager>,
    pub txn: Arc<TxnManager>,
    pub catalog: Catalog,
    /// Whether the log held anything to replay, which is also whether the trees were rebuilt.
    pub recovered: bool,
    /// The tables whose DROP the log records and this open completed (D250). PRIVATE, so no entry
    /// point can name it (lane §3.7). Test-only since D250 review 2's R2-4: the door reads
    /// `dropped_tables`, which contains these, and CI builds with `-D dead_code`, so the gate is
    /// what keeps an unread field from failing the build.
    #[cfg(test)]
    completed_drops: Vec<String>,
    /// Every table a `DropTable` record in the retained log names and this open's catalog does not:
    /// the tables whose provenance [`OpenedDatabase::attach_runtime`] forgets (D250 review 2's R2-4).
    /// It contains `completed_drops`. PRIVATE for the same reason.
    dropped_tables: Vec<String>,
}

impl OpenedDatabase {
    /// **The one door an agent runtime comes through onto an opened database** (D250 review 1's F7,
    /// the lead's ruling, the same "one function both call" rule as D204). It forgets the provenance
    /// of every table the retained log records as dropped and the catalog does not name, as the
    /// executor does after a DROP (B9, `AgentRuntime::forget_table`), so a table later created under
    /// the name does not inherit the dropped one's authors. The list is private to this module, so an
    /// entry point cannot run the forget itself and cannot leave it out; `tests/open_path_allowlist.rs`
    /// checks that both production entry points build their runtime through here. It lives in the WAL
    /// module because a door in `agent_sql::runtime` would need the list `pub(crate)`, which the CLI
    /// could read.
    ///
    /// **Idempotent, and repeated at every open until it has run** (D250 review 2's R2-4): not only the
    /// DROPs this open completed, but every one the log still records. A crash after an open made its
    /// completion durable and before a runtime was attached is caught by the next open, because
    /// [`open_recovered`] re-declares such a DROP after a checkpoint that truncated past it. The same
    /// record catches a crash between the executor's DROP and its own forget: `ddl_unit` re-declares
    /// the DROP after its truncation. pgserver's forget is a no-op, because its provenance store is in
    /// memory.
    ///
    /// `&self` and no drain: every runtime attached to one open forgets the same tables.
    pub fn attach_runtime(&self, runtime: AgentRuntime) -> Arc<AgentRuntime> {
        for table in &self.dropped_tables {
            runtime.forget_table(table);
        }
        Arc::new(runtime)
    }
}

/// The heap a record writes, as its directory root: a `Heap*` record's own, or the one a CLR redoes.
fn heap_root(kind: &RecKind) -> Option<u32> {
    match kind {
        RecKind::HeapInsert { dir_root, .. }
        | RecKind::HeapDelete { dir_root, .. }
        | RecKind::HeapUpdate { dir_root, .. }
        | RecKind::HeapRelease { dir_root, .. } => Some(*dir_root),
        RecKind::Clr { redo, .. } => heap_root(redo),
        _ => None,
    }
}

/// D250: every heap root a `DropTable` record names (the table's heap and its time-travel heap),
/// mapped to the LSN of the LAST such record. A record of that root below that LSN belongs to a
/// dropped table.
fn dropped_roots(records: &[crate::wal::log::LogRecord]) -> HashMap<u32, u64> {
    let mut dropped = HashMap::new();
    for rec in records {
        if let RecKind::Ddl { op: DdlOp::DropTable, dir_root, time_travel_root, .. } = &rec.kind {
            dropped.insert(*dir_root, rec.lsn);
            dropped.insert(*time_travel_root, rec.lsn);
        }
    }
    dropped
}

/// A `DropTable` record the retained log holds: the table, its heap root, its time-travel root, and
/// the record's LSN.
type LoggedDrop = (String, u32, u32, u64);

/// D250 (b): the tables the retained log DROPPED that the catalog on disk still names, so their
/// DROP never finished: its record is durable before its first free, but the catalog change reaches
/// disk only with the checkpoint after. Returned second; first, every DROP the log records, which
/// the provenance forget reads (D250 review 2's R2-4).
///
/// Only the LAST `DropTable` per heap root counts (D250 review 2's R2-1). A root can carry two drops
/// of one name: a table dropped, re-created at its root by a CREATE whose sync failed, and dropped
/// again. Each would pass every clause below, and completing the second would find the table gone,
/// so the open failed at every attempt. Every exclusion that holds after the later record also holds
/// after the earlier one, and the root match admits one name per root.
///
/// Left alone, as a NEW table re-created at the same root:
/// - one with any DDL record other than a `DropTable` at the root after the DROP: its `CreateTable`,
///   or an `AlterColumn` that answered `Ok` (D250 review 2's R2-2). CREATE INDEX logs no DDL record,
///   so a CREATE INDEX on a table re-created as below, under a pin that keeps the old DROP, is not
///   seen: stated;
/// - one with any heap record (or CLR) on its heap or time-travel root after the DROP (D250 review
///   1's F2). A CREATE whose checkpoint wrote the catalog and then failed to sync logs no
///   `CreateTable`, but every row committed into it leaves such a record. An EMPTY table re-created
///   that way, and never altered, is still forgotten: its CREATE was reported failed, and no
///   committed row is lost (accepted by the lead, lane §3.7).
fn logged_drops_the_catalog_missed(wal: &WalManager, catalog: &Catalog) -> Result<(Vec<LoggedDrop>, Vec<String>), FerroError> {
    let mut drops: Vec<LoggedDrop> = Vec::new();
    let mut created: HashMap<u32, u64> = HashMap::new();
    let mut written: HashMap<u32, u64> = HashMap::new();
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    while lsn < end {
        let (rec, next) = wal.read_record(lsn)?;
        match &rec.kind {
            RecKind::Ddl { op: DdlOp::DropTable, table, dir_root, time_travel_root, .. } => {
                drops.push((table.clone(), *dir_root, *time_travel_root, rec.lsn));
            }
            RecKind::Ddl { dir_root, .. } => {
                created.insert(*dir_root, rec.lsn);
            }
            other => {
                if let Some(root) = heap_root(other) {
                    written.insert(root, rec.lsn);
                }
            }
        }
        lsn = next;
    }
    let later = |map: &HashMap<u32, u64>, root: &u32, at: &u64| map.get(root).is_some_and(|&l| l > *at);
    // Review 2's R2-1: the last per root. The log is read in LSN order, so a later record replaces.
    let last: HashMap<u32, LoggedDrop> = drops.into_iter().map(|d| (d.1, d)).collect();
    let mut drops: Vec<LoggedDrop> = last.into_values().collect();
    drops.sort_by_key(|(_, _, _, at)| *at);
    let missed = drops
        .iter()
        .filter(|(table, dir_root, tt_root, at)| {
            !later(&created, dir_root, at)
                && !later(&written, dir_root, at)
                && !later(&written, tt_root, at)
                && catalog.get_table(table).is_some_and(|e| e.first_directory_page_id == *dir_root)
        })
        .map(|(table, _, _, _)| table.clone())
        .collect();
    Ok((drops, missed))
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
    // D250 (b): finish every DROP the log records and the catalog on disk does not. `recover` has
    // just skipped those tables' records, so keeping them in the catalog would serve a table whose
    // recent writes were not replayed, over pages the DROP may have freed. Removed from the catalog
    // WITHOUT freeing: the directory repair above may already have allocated a page the DROP freed,
    // and a second free would hit its new owner. Stated cost: pages the DROP had not freed yet leak,
    // one table's worth per incomplete DROP. No other mechanism reclaims them: D229 does not defer
    // DROP's frees (the lead, 2026-09-24), so this cost is D250's, stated.
    let (logged_drops, completed_drops) = logged_drops_the_catalog_missed(&wal, &catalog)?;
    for table in &completed_drops {
        use std::io::Write;
        catalog.forget_dropped_table(table)?;
        let _ = writeln!(
            std::io::stderr(),
            "ferrodb: finished the DROP of `{table}`, which the log records but the catalog on disk did not; \
             any of its pages the DROP had not freed yet are leaked"
        );
    }
    // D205 C1 correction: a rollback in an earlier process whose index undo failed left a marker
    // (`TxnManager::mark_indexes_stale`), because its orphaned entries are on disk and an empty log
    // would not trigger the rebuild below. The marker is removed only after the rebuilt trees are
    // checkpointed. If removal fails, the next open simply rebuilds again, which is harmless.
    let marker = crate::wal::txn::stale_indexes_marker(&wal.path);
    let stale = marker.exists();
    // F1: a log written before D213 has just been replayed with its own meaning. The checkpoint
    // rewrites it as version 3. Until then `TxnManager::begin_locked` refuses a transaction and
    // `WalManager::append` refuses the two records whose meaning changed, so it runs even for such a
    // log with nothing in it.
    let legacy = wal.is_legacy();
    if recovered || stale {
        rebuild_indexes(&mut catalog, &bp)?;
    }
    if recovered || stale || legacy {
        use std::io::Write;
        // Review 2's N1 (the lead's decision): the checkpoint ALWAYS runs, and always flushes every
        // page. The rebuild above freed the old trees and reallocated their pages on disk, so a
        // skipped flush left the next open reading a zeroed root. Only the truncation is refused
        // while a release is still owed (F2): the log is then the only record of it. Without a retry
        // first: `recover` has just tried every owed release, and a retry would stand between the
        // rebuild's on-disk frees and the sync (D229's window; lane §21.2). A kept log is counted,
        // and printed when the log-keeping state begins (`TxnManager::checkpoint_or_keep_held`).
        let kept = txn.checkpoint_after_frees()?;
        if !matches!(kept, crate::wal::txn::CheckpointOutcome::KeptForOwed(_)) && stale {
            if let Err(e) = std::fs::remove_file(&marker) {
                let _ = writeln!(
                    std::io::stderr(),
                    "ferrodb: rebuilt the indexes, but could not remove {} ({e}); the next open rebuilds again",
                    marker.display()
                );
            }
        }
    }
    // D250 review 2's R2-4 (the lead's decision): every table the log records as dropped and the
    // catalog now does not name has its provenance forgotten by `attach_runtime`, at this open and at
    // every later one until a runtime has been attached. A table re-created under the name is named
    // by the catalog, and its provenance is its own. The checkpoint above may have truncated those
    // records away BEFORE any runtime is attached, and a crash then would lose the forget for good;
    // so each is declared again, as `ddl_unit` does after a DROP's truncation, and stays until the
    // running process's next truncating checkpoint, which comes after the door. Cost, stated: a
    // crash-residue log that holds only such a record makes the next open replay and rebuild every
    // index (D216's term); a clean CLI exit truncates it.
    let dropped: Vec<LoggedDrop> = logged_drops.into_iter().filter(|(table, ..)| catalog.get_table(table).is_none()).collect();
    for (table, dir_root, time_travel_root, at) in &dropped {
        if wal.base_lsn.load(Ordering::SeqCst) > *at {
            txn.declare_drop_again(&crate::wal::txn::DdlRecord {
                op: DdlOp::DropTable,
                table: table.clone(),
                dir_root: *dir_root,
                time_travel_root: *time_travel_root,
                columns: Vec::new(),
            })?;
        }
    }
    let mut dropped_tables: Vec<String> = dropped.into_iter().map(|(table, ..)| table).collect();
    dropped_tables.sort_unstable();
    dropped_tables.dedup();
    Ok(OpenedDatabase {
        bp,
        wal,
        txn,
        catalog,
        recovered,
        #[cfg(test)]
        completed_drops,
        dropped_tables,
    })
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
    /// the log it opens holds no heap or CLR record** (here: an empty log).
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
    ///
    /// Named for what its premise needs, a log with no record that recovery replays, rather than an
    /// empty one: at this tip that log is empty, and once D227 re-declares the schema at every
    /// checkpoint it holds declarations too (lane §21.6; renamed from
    /// `..._even_when_the_log_is_empty`, assertions unchanged).
    #[test]
    fn a_failed_index_undo_makes_the_next_open_rebuild_even_when_the_log_holds_no_heap_or_clr_record() {
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
        assert!(!o.recovered, "premise failed: the open recovered, so the log held a record recovery replays and recovery alone would have rebuilt");
        assert!(!marker.exists(), "the marker survived the open, so the rebuild it asks for did not run");
    }

    /// **Review 2's N1, on the path its red test cannot reach: a release that fails RETRYABLY at open.**
    ///
    /// `tests/owed_release_at_open_reopens.rs` reaches the open's owed branch through a mismatch whose
    /// quarantine record cannot be written. Here the release fails as an I/O error would (the
    /// thread-local `wal::txn::FAIL_RELEASES`), once, in recovery's `finish_releases`. The open's
    /// checkpoint does not retry it (lane §21.2), so open #1 owes it and must still FLUSH every page
    /// while keeping the log. At `368d0e1` it skipped the flush, and open #2 then walked a root the
    /// rebuild had already zeroed on disk. Its red is mutant-only: the seam is new.
    ///
    /// Amended by lane §21.1: `FAIL_RELEASES` was 2 while the open's checkpoint retried, and open #1's
    /// owed count and its counted deferral are now asserted.
    #[test]
    fn an_open_whose_release_fails_retryably_flushes_so_the_next_open_rebuilds_cleanly() {
        use crate::execution::executor::{run, Outcome};
        use crate::parser::{parser::Parser, scanner::Scanner};
        use crate::wal::txn::FAIL_RELEASES;

        fn exec(sql: &str, o: &mut OpenedDatabase, s: &mut Session) -> Outcome {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty(), "parse errors in `{sql}`: {:?}", p.errors);
            run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        }
        fn by_key(o: &mut OpenedDatabase, id: i32) -> Vec<Vec<Value>> {
            match exec(&format!("SELECT id, note FROM notes WHERE id = {id};"), o, &mut Session::new()) {
                Outcome::Rows(r) => r,
                _ => panic!("SELECT did not return rows"),
            }
        }
        let note = |id: i32, text: &str| vec![Value::Integer(id), Value::Varchar(text.to_string())];

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("owed_io.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let mut main = Session::new();
            // Row 2 (3934 B), then row 1 (35 B), the lowest tuple; T1 relocates row 1 and commits.
            exec("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", &mut o, &mut main);
            exec(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), &mut o, &mut main);
            exec("INSERT INTO notes VALUES (1, 'a');", &mut o, &mut main);
            let mut t1 = Session::new();
            exec("BEGIN;", &mut o, &mut t1);
            exec(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut o, &mut t1);
            exec("COMMIT;", &mut o, &mut t1);
            // The crash: the commit's HeapRelease is still in the log buffer, and is lost.
        }

        // Open #1 owes the release and fails it once, in finish_releases. Its checkpoint does not retry.
        FAIL_RELEASES.with(|f| f.set(1));
        let deferred = crate::wal::txn::deferred_checkpoints();
        {
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).expect("open #1 failed");
            assert!(o.recovered, "premise: open #1 replayed nothing, so no rebuild ran");
            assert_eq!(FAIL_RELEASES.with(|f| f.get()), 0, "premise: the injected failure was not consumed");
            assert_eq!(o.txn.owed_releases(), 1, "open #1 does not owe the release that failed");
            // At least: the lib tests run in parallel on this process-wide counter.
            assert!(
                crate::wal::txn::deferred_checkpoints() > deferred,
                "open #1's checkpoint kept the log without counting a deferral"
            );
            // The log was kept, not truncated: the checkpoint found the release still owed. A
            // truncating checkpoint in a process that ran no DDL leaves the header alone (24 bytes).
            assert!(
                std::fs::metadata(&o.wal.path).unwrap().len() > 24,
                "premise: open #1's checkpoint truncated the log, so no release was owed"
            );
            // Dropped without another checkpoint: open #1's own is all that reached disk.
        }
        FAIL_RELEASES.with(|f| f.set(0));

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).expect("open #2 failed after an open whose owed release failed");
        assert_eq!(by_key(&mut o, 1), vec![note(1, &"y".repeat(200))], "row 1 is not reachable by key after open #2");
        assert_eq!(by_key(&mut o, 2), vec![note(2, &"x".repeat(3900))], "row 2 is not reachable by key after open #2");
    }

    /// **Redo after a DROP touches no page the DROP freed** (lane §21.2; the lead's decision after
    /// review 3). A DROP frees every heap and directory page of its table, and `allocate` hands those
    /// out first. Redo skips a record only when the page's LSN has reached it, and a reused page starts
    /// as a zero page with LSN 0, so a record of the dropped table left in the log would be replayed
    /// onto the page's new owner. So a DROP must truncate even while a release is owed. Here the only
    /// owed release is the dropped table's own, which the DROP discards. Its red is mutant-only: it was
    /// added after the fix, and at `7cede54` this DROP was refused outright.
    ///
    /// D250: a DROP no longer needs its truncation for this, since recovery skips every record a later
    /// DROP names. This test's DROP still truncates (nothing else keeps the log), so it covers the
    /// truncating path; the kept-log paths are the two `after_a_..._drop_...` tests below.
    #[test]
    fn redo_after_dropping_the_table_that_owed_a_release_touches_no_freed_page() {
        use crate::execution::executor::{run, Outcome};
        use crate::parser::{parser::Parser, scanner::Scanner};
        use crate::wal::txn::FAIL_RELEASES;

        fn exec(sql: &str, o: &mut OpenedDatabase, s: &mut Session) -> Outcome {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            assert!(p.errors.is_empty(), "parse errors in `{sql}`: {:?}", p.errors);
            run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        }
        fn rows(sql: &str, o: &mut OpenedDatabase) -> Vec<Vec<Value>> {
            match exec(sql, o, &mut Session::new()) {
                Outcome::Rows(mut r) => {
                    r.sort_by_key(|row| format!("{row:?}"));
                    r
                }
                _ => panic!("`{sql}` did not return rows"),
            }
        }
        /// The heap `(dir_root, page)` a record writes, looking through a CLR to what it redoes.
        fn heap_page(kind: &RecKind) -> Option<(u32, u32)> {
            match kind {
                RecKind::HeapInsert { dir_root, page_id, .. }
                | RecKind::HeapDelete { dir_root, page_id, .. }
                | RecKind::HeapUpdate { dir_root, page_id, .. }
                | RecKind::HeapRelease { dir_root, page_id, .. } => Some((*dir_root, *page_id)),
                RecKind::Clr { redo, .. } => heap_page(redo),
                _ => None,
            }
        }
        /// Every heap `(dir_root, page)` the log from its base to its end writes.
        fn log_pages(o: &OpenedDatabase) -> Vec<(u32, u32)> {
            let mut out = Vec::new();
            let mut lsn = o.wal.base_lsn.load(Ordering::SeqCst);
            let end = o.wal.next_lsn.load(Ordering::SeqCst);
            while lsn < end {
                let (rec, next) = o.wal.read_record(lsn).unwrap();
                out.extend(heap_page(&rec.kind));
                lsn = next;
            }
            out
        }
        let v = |id: i32, n: i32| vec![Value::Integer(id), Value::Integer(n)];

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("drop_owed.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let mut s = Session::new();
            // Row 2 (3934 B), then row 1 (35 B); row 1's 200 B note does not fit beside them, so the
            // UPDATE relocates it and retires its slot. Its release fails at every attempt.
            exec("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", &mut o, &mut s);
            exec(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), &mut o, &mut s);
            exec("INSERT INTO notes VALUES (1, 'a');", &mut o, &mut s);
            FAIL_RELEASES.with(|f| f.set(u32::MAX));
            exec(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut o, &mut s);
            assert_eq!(o.txn.owed_releases(), 1, "premise: the relocation's release is not owed");
            let (heap, tt, primary) = {
                let e = o.catalog.get_table("notes").expect("notes");
                (e.first_directory_page_id, e.time_travel_root, e.primary_index_root)
            };
            let mut owned: Vec<u32> = vec![heap, tt, primary];
            owned.extend(log_pages(&o).into_iter().filter(|(d, _)| *d == heap || *d == tt).map(|(_, p)| p));
            assert!(owned.len() > 3, "premise: the log names no page of the table about to be dropped");

            exec("DROP TABLE notes;", &mut o, &mut s);
            FAIL_RELEASES.with(|f| f.set(0));
            assert_eq!(o.txn.owed_releases(), 0, "the dropped table's release is still owed, so its log was kept");
            let left: Vec<(u32, u32)> =
                log_pages(&o).into_iter().filter(|(d, p)| *d == heap || *d == tt || owned.contains(p)).collect();
            assert!(left.is_empty(), "after the DROP the log still writes pages the DROP freed, and redo would replay them: {left:?}");

            // The freed pages go to a new table, and the process dies before any checkpoint.
            exec("CREATE TABLE fresh (id INTEGER NOT NULL, v INTEGER);", &mut o, &mut s);
            let fresh = o.catalog.get_table("fresh").expect("fresh").first_directory_page_id;
            assert!(owned.contains(&fresh), "premise: `fresh` reused none of the dropped table's pages, so redo could not meet one");
            for id in 1..=3 {
                exec(&format!("INSERT INTO fresh VALUES ({id}, {});", id * 10), &mut o, &mut s);
            }
        }

        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).expect("the open after the DROP and a crash failed");
        assert!(o.catalog.get_table("notes").is_none(), "the dropped table came back");
        assert_eq!(rows("SELECT id, v FROM fresh;", &mut o), vec![v(1, 10), v(2, 20), v(3, 30)], "`fresh` after the reopen");
        assert_eq!(rows("SELECT id, v FROM fresh WHERE id = 2;", &mut o), vec![v(2, 20)], "`fresh` by key after the reopen");
    }

    /// One SQL statement through the executor, in a fresh session.
    fn run_sql(sql: &str, catalog: &mut Catalog, bp: &Arc<BufferPoolManager>, txn: &Arc<TxnManager>) -> Result<crate::execution::executor::Outcome, FerroError> {
        run_sql_in(sql, catalog, bp, txn, &mut Session::new())
    }

    /// One SQL statement through the executor, in `session`.
    fn run_sql_in(sql: &str, catalog: &mut Catalog, bp: &Arc<BufferPoolManager>, txn: &Arc<TxnManager>, session: &mut Session) -> Result<crate::execution::executor::Outcome, FerroError> {
        use crate::parser::{parser::Parser, scanner::Scanner};
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse errors in `{sql}`: {:?}", p.errors);
        crate::execution::executor::run(stmts.remove(0), catalog, bp.clone(), txn.clone(), session)
    }

    /// Every heap `(dir_root, page)` the retained log writes, looking through a CLR to what it redoes.
    fn heap_writes(wal: &WalManager) -> Vec<(u32, u32)> {
        heap_writes_at(wal).into_iter().map(|(dir, page, _)| (dir, page)).collect()
    }

    /// [`heap_writes`], with the LSN of the record that writes each.
    fn heap_writes_at(wal: &WalManager) -> Vec<(u32, u32, u64)> {
        fn heap_page(kind: &RecKind) -> Option<(u32, u32)> {
            match kind {
                RecKind::HeapInsert { dir_root, page_id, .. }
                | RecKind::HeapDelete { dir_root, page_id, .. }
                | RecKind::HeapUpdate { dir_root, page_id, .. }
                | RecKind::HeapRelease { dir_root, page_id, .. } => Some((*dir_root, *page_id)),
                RecKind::Clr { redo, .. } => heap_page(redo),
                _ => None,
            }
        }
        let mut out = Vec::new();
        let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
        let end = wal.next_lsn.load(Ordering::SeqCst);
        while lsn < end {
            let (rec, next) = wal.read_record(lsn).unwrap();
            out.extend(heap_page(&rec.kind).map(|(dir, page)| (dir, page, lsn)));
            lsn = next;
        }
        out
    }

    /// Whether the retained log holds a CLR.
    fn has_clr(wal: &WalManager) -> bool {
        let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
        let end = wal.next_lsn.load(Ordering::SeqCst);
        while lsn < end {
            let (rec, next) = wal.read_record(lsn).unwrap();
            if matches!(rec.kind, RecKind::Clr { .. }) {
                return true;
            }
            lsn = next;
        }
        false
    }

    /// Every page `table` owns as far as its catalog entry and the log say: its heap's directory root,
    /// its time-travel root, its primary root, and every page a record of either heap writes.
    fn pages_of(catalog: &Catalog, wal: &WalManager, table: &str) -> Vec<u32> {
        let e = catalog.get_table(table).expect("table");
        let (heap, tt) = (e.first_directory_page_id, e.time_travel_root);
        let mut pages = vec![heap, tt, e.primary_index_root];
        pages.extend(heap_writes(wal).into_iter().filter(|(d, _)| *d == heap || *d == tt).map(|(_, p)| p));
        pages.sort_unstable();
        pages.dedup();
        pages
    }

    /// The open after a crash must write none of `freed`, and leave them free: each holds the bytes it
    /// held before the crash, and `allocate`, which hands out the lowest clear bit, returns one of them.
    fn assert_reopen_leaves_freed_pages_alone(db: &Path, freed: &[u32], before: &[Vec<u8>]) {
        let lock = DbLock::acquire(db).unwrap();
        let o = open_recovered(db, &lock).expect("the open after the DROP and a crash failed");
        assert!(o.catalog.get_table("t").is_none(), "the dropped table came back");
        for (p, bytes) in freed.iter().zip(before) {
            let now = o.bp.disk_manager.read(*p).unwrap();
            assert!(now.as_slice() == bytes.as_slice(), "the open wrote page {p}, which the DROP freed: redo, the directory repair or a release replayed the dropped table onto it");
        }
        let next = o.bp.disk_manager.allocate().unwrap();
        assert!(freed.contains(&next), "page {next} was handed out, so the pages the DROP freed are not all free any more");
    }

    /// **D250 (lane `lane_d250_drop_logged.md` §2 test 1): after a DROP that a WAL pin kept in the log
    /// and a crash, the next open writes no page the DROP freed.** The pin cancels the DROP's
    /// truncation (`WalManager::truncate`), so the log still holds the table's records. Redo takes a
    /// freed page as it finds it: one never flushed is a zero page, `Page::empty` with LSN 0, and
    /// every record applies. The fix: the DROP's record is durable before its frees, and recovery
    /// skips every record a later DROP names. Red at `2c10f17`, where the DROP is refused for the pin;
    /// the hazard half is red under the mutant that removes the skip.
    #[test]
    fn after_a_pinned_drop_and_a_crash_redo_writes_no_page_the_drop_freed() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("pinned_drop.db");
        let (freed, before) = {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let ok = |sql: &str, o: &mut OpenedDatabase| {
                run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            };
            ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut o);
            for id in 1..=3 {
                ok(&format!("INSERT INTO t VALUES ({id}, {});", id * 10), &mut o);
            }
            // Lane §3.5 (TTm, CLRm): records on the time-travel heap, and CLRs, before the DROP.
            ok("UPDATE t SET v = 11 WHERE id = 1;", &mut o);
            let mut s = Session::new();
            for sql in ["BEGIN;", "INSERT INTO t VALUES (7, 70);", "ROLLBACK;"] {
                run_sql_in(sql, &mut o.catalog, &o.bp, &o.txn, &mut s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            let tt = o.catalog.get_table("t").unwrap().time_travel_root;
            assert!(heap_writes(&o.wal).iter().any(|(d, _)| *d == tt), "premise: the UPDATE wrote nothing to the time-travel heap");
            assert!(has_clr(&o.wal), "premise: the rolled-back INSERT left no CLR");
            let freed = pages_of(&o.catalog, &o.wal, "t");
            let heap = o.catalog.get_table("t").unwrap().first_directory_page_id;
            let base = o.wal.base_lsn.load(Ordering::SeqCst);
            let pin = o.wal.pin(base).expect("pin the log at its base");
            ok("DROP TABLE t;", &mut o);
            assert_eq!(o.wal.base_lsn.load(Ordering::SeqCst), base, "premise: the pin did not keep the log");
            assert!(
                heap_writes(&o.wal).iter().any(|(d, _)| *d == heap),
                "premise: the kept log no longer holds the dropped table's records, so redo has nothing to misapply"
            );
            let before: Vec<Vec<u8>> = freed.iter().map(|p| o.bp.disk_manager.read(*p).unwrap().to_vec()).collect();
            drop(pin);
            (freed, before)
            // The crash: every handle goes, and no checkpoint runs.
        };
        assert_reopen_leaves_freed_pages_alone(&db, &freed, &before);
    }

    /// A page file whose syncs fail while `armed`, so a checkpoint can be made to fail after its flush.
    struct SyncFailsWhenArmed {
        file: std::fs::File,
        armed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl crate::storage::storage::Storage for SyncFailsWhenArmed {
        fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
            crate::storage::storage::Storage::pwrite(&self.file, buf, offset)
        }
        fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            crate::storage::storage::Storage::pread(&self.file, buf, offset)
        }
        fn sync_all(&self) -> std::io::Result<()> {
            if self.armed.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("injected: the page file's sync failed"));
            }
            crate::storage::storage::Storage::sync_all(&self.file)
        }
        fn sync_data(&self) -> std::io::Result<()> {
            if self.armed.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("injected: the page file's sync failed"));
            }
            crate::storage::storage::Storage::sync_data(&self.file)
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            crate::storage::storage::Storage::set_len(&self.file, len)
        }
        fn len(&self) -> std::io::Result<u64> {
            crate::storage::storage::Storage::len(&self.file)
        }
    }

    /// **D250 (lane §2 test 2): after a DROP whose checkpoint failed after its frees, and a crash, the
    /// next open writes no page the DROP freed.** The same state as a pin, reached by an I/O error: the
    /// pages are free on disk, the catalog change was written, and the log still holds the table's
    /// records. Red at `2c10f17` at the page bytes: there the `DropTable` record was logged only after
    /// a successful checkpoint, so the log held the inserts and no DROP, and redo replayed them onto
    /// the table's zeroed data page.
    #[test]
    fn after_a_drop_whose_checkpoint_failed_and_a_crash_redo_writes_no_page_the_drop_freed() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("failed_drop.db");
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (freed, before) = {
            let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&db).unwrap();
            let dm = DiskManager::with_storage(Arc::new(SyncFailsWhenArmed { file, armed: armed.clone() })).unwrap();
            let bp = Arc::new(BufferPoolManager::new(Arc::new(dm)));
            let wal = Arc::new(WalManager::new(PathBuf::from(format!("{}.wal", db.display()))).unwrap());
            let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
            bp.attach_wal(wal.clone());
            let mut catalog = Catalog::create(bp.clone()).unwrap();
            run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn).unwrap();
            for id in 1..=3 {
                run_sql(&format!("INSERT INTO t VALUES ({id}, {});", id * 10), &mut catalog, &bp, &txn).unwrap();
            }
            let freed = pages_of(&catalog, &wal, "t");
            let heap = catalog.get_table("t").unwrap().first_directory_page_id;
            armed.store(true, Ordering::SeqCst);
            let e = match run_sql("DROP TABLE t;", &mut catalog, &bp, &txn) {
                Err(e) => e,
                Ok(_) => panic!("premise failed: the DROP's checkpoint did not fail"),
            };
            assert!(e.to_string().contains("injected"), "premise failed: the DROP failed, but not at its checkpoint's sync: {e}");
            assert!(catalog.get_table("t").is_none(), "premise failed: the DROP failed before its mutation, so it freed nothing");
            // Lane §3.5 (review 1's F6): the log still holds the dropped table's records.
            assert!(
                heap_writes(&wal).iter().any(|(d, _)| *d == heap),
                "premise: the log no longer holds the dropped table's records, so redo has nothing to misapply"
            );
            armed.store(false, Ordering::SeqCst);
            let before: Vec<Vec<u8>> = freed.iter().map(|p| bp.disk_manager.read(*p).unwrap().to_vec()).collect();
            (freed, before)
            // The crash: every handle goes, and no checkpoint runs.
        };
        assert_reopen_leaves_freed_pages_alone(&db, &freed, &before);
    }

    /// **D250 (lane `lane_d250_drop_logged.md` §2 test 5): a DROP whose mutation fails after its record
    /// is durable poisons the log, and the next open completes the DROP.** The log then calls the
    /// table dropped, and recovery skips its records, while the catalog still holds it. So the
    /// running process stops writing (the poison), and the next open removes the table from the
    /// catalog (`logged_drops_the_catalog_missed`). Red only under a mutant: it needs the new
    /// `drop_checkpointed` signature.
    #[test]
    fn a_drop_whose_mutation_fails_after_its_record_is_durable_poisons_the_log_and_the_next_open_completes_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("half_drop.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut o.catalog, &o.bp, &o.txn).unwrap();
            run_sql("INSERT INTO t VALUES (1, 10);", &mut o.catalog, &o.bp, &o.txn).unwrap();
            let record = {
                let e = o.catalog.get_table("t").expect("t");
                crate::wal::txn::DdlRecord {
                    op: DdlOp::DropTable,
                    table: "t".into(),
                    dir_root: e.first_directory_page_id,
                    time_travel_root: e.time_travel_root,
                    columns: Vec::new(),
                }
            };
            let err = o
                .txn
                .drop_checkpointed(record, || Err::<(), _>(FerroError::Internal("injected: the drop failed before it freed anything".into())))
                .expect_err("premise failed: the DROP succeeded although its mutation failed");
            assert!(err.to_string().contains("injected"), "premise failed: the DROP failed, but not in its mutation: {err}");
            assert!(o.catalog.get_table("t").is_some(), "premise failed: the injected failure came after the mutation");
            assert!(
                o.wal.poisoned().is_some(),
                "a DROP that failed after its record was durable left the log writable, so the session could keep \
                 writing a table the log calls dropped"
            );
        }
        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock).expect("the open after a DROP that failed after its record failed");
        assert!(
            o.catalog.get_table("t").is_none(),
            "the next open did not complete the logged DROP: recovery skipped the table's records and left it in the catalog"
        );
        // Lane §3.5 (review 1's F7): the open names the DROP it completed, so `attach_runtime` forgets
        // the table's provenance as the executor's DROP does (lane §3.7, test 11).
        assert_eq!(o.completed_drops, vec!["t".to_string()], "the open did not report the DROP it completed");
    }

    /// A database built by hand on the given page and log storage, at `db` (its log at `<db>.wal`), so
    /// `open_recovered` can reopen it after the handles go.
    fn manual_db(
        db: &Path,
        page_file: Arc<dyn crate::storage::storage::Storage>,
        wal_file: Arc<dyn crate::storage::storage::Storage>,
    ) -> (Arc<BufferPoolManager>, Arc<WalManager>, Arc<TxnManager>, Catalog) {
        let dm = DiskManager::with_storage(page_file).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(dm)));
        let wal = Arc::new(WalManager::with_storage(wal_file, PathBuf::from(format!("{}.wal", db.display()))).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        let catalog = Catalog::create(bp.clone()).unwrap();
        (bp, wal, txn, catalog)
    }

    /// The `id` column of `t`, sorted, or the error that reading it gave.
    fn ids_of_t(o: &mut OpenedDatabase) -> Result<Vec<i32>, FerroError> {
        match run_sql("SELECT id FROM t;", &mut o.catalog, &o.bp, &o.txn)? {
            crate::execution::executor::Outcome::Rows(rows) => {
                let mut ids: Vec<i32> = rows
                    .into_iter()
                    .map(|r| match r.first() {
                        Some(Value::Integer(id)) => *id,
                        other => panic!("an id that is not an integer: {other:?}"),
                    })
                    .collect();
                ids.sort_unstable();
                Ok(ids)
            }
            _ => panic!("SELECT did not return rows"),
        }
    }

    /// A log file whose next write fails once `armed` is set, writing nothing.
    struct WalWriteFailsOnce {
        file: std::fs::File,
        armed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl crate::storage::storage::Storage for WalWriteFailsOnce {
        fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
            if self.armed.swap(false, Ordering::SeqCst) {
                return Err(std::io::Error::other("injected: the log's write failed"));
            }
            crate::storage::storage::Storage::pwrite(&self.file, buf, offset)
        }
        fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            crate::storage::storage::Storage::pread(&self.file, buf, offset)
        }
        fn sync_all(&self) -> std::io::Result<()> {
            crate::storage::storage::Storage::sync_all(&self.file)
        }
        fn sync_data(&self) -> std::io::Result<()> {
            crate::storage::storage::Storage::sync_data(&self.file)
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            crate::storage::storage::Storage::set_len(&self.file, len)
        }
        fn len(&self) -> std::io::Result<u64> {
            crate::storage::storage::Storage::len(&self.file)
        }
    }

    /// **D250 review 1's F1 (lane §3.5 test 6): a DROP whose record cannot be written leaves no later
    /// write to lose.** The failed flush keeps the record in the log buffer, so the next commit's flush
    /// would make the DROP durable after the client was told it failed, and the next open would then
    /// complete it over rows committed since. The decision: that failure poisons the log, as a failed
    /// Commit flush does. Red at `cb00606`, where the INSERT's commit flushed the DROP and the open
    /// dropped `t` with both rows.
    #[test]
    fn a_drop_whose_record_cannot_be_written_leaves_no_later_write_to_lose() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("unwritten_drop.db");
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let insert_answered = {
            let open = |p: PathBuf| OpenOptions::new().read(true).write(true).create(true).truncate(true).open(p).unwrap();
            let page_file = open(db.clone());
            let wal_file = open(PathBuf::from(format!("{}.wal", db.display())));
            let (bp, _wal, txn, mut catalog) =
                manual_db(&db, Arc::new(page_file), Arc::new(WalWriteFailsOnce { file: wal_file, armed: armed.clone() }));
            run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn).unwrap();
            run_sql("INSERT INTO t VALUES (1, 10);", &mut catalog, &bp, &txn).unwrap();
            armed.store(true, Ordering::SeqCst);
            match run_sql("DROP TABLE t;", &mut catalog, &bp, &txn) {
                Err(e) => assert!(e.to_string().contains("injected"), "premise failed: the DROP failed, but not at its record's write: {e}"),
                Ok(_) => panic!("premise failed: the DROP succeeded although its record could not be written"),
            }
            assert!(catalog.get_table("t").is_some(), "premise failed: the DROP ran its mutation although its record was not written");
            run_sql("INSERT INTO t VALUES (9, 90);", &mut catalog, &bp, &txn).is_ok()
            // The crash: every handle goes.
        };
        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).expect("the open after a DROP whose record could not be written failed");
        let ids = ids_of_t(&mut o).unwrap_or_else(|e| panic!("`t` is gone after the reopen, although its DROP was reported failed: {e}"));
        assert!(ids.contains(&1), "`t` lost the row committed before the failed DROP: {ids:?}");
        if insert_answered {
            assert!(ids.contains(&9), "the INSERT after the failed DROP answered Ok, and its row is gone: {ids:?}");
        }
    }

    /// **D250 review 1's F2 (lane §3.5 test 7): a table re-created at the dropped root, whose CREATE
    /// failed to sync, keeps its committed row.** The CREATE's checkpoint wrote the catalog page and
    /// then failed at the sync, so no `CreateTable` was logged, while the table is in the running
    /// catalog and on disk at the root the DROP freed. A row committed into it follows the DROP's
    /// record. The open's completion must not take the logged DROP for this table's. Red at `cb00606`,
    /// where the completion forgot `t` and its row.
    #[test]
    fn a_table_recreated_at_the_dropped_root_whose_create_failed_to_sync_keeps_its_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("recreated.db");
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let open = |p: PathBuf| OpenOptions::new().read(true).write(true).create(true).truncate(true).open(p).unwrap();
            let page_file = open(db.clone());
            let wal_file = open(PathBuf::from(format!("{}.wal", db.display())));
            let (bp, _wal, txn, mut catalog) =
                manual_db(&db, Arc::new(SyncFailsWhenArmed { file: page_file, armed: armed.clone() }), Arc::new(wal_file));
            run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn).unwrap();
            run_sql("INSERT INTO t VALUES (1, 10);", &mut catalog, &bp, &txn).unwrap();
            let root = catalog.get_table("t").unwrap().first_directory_page_id;
            run_sql("DROP TABLE t;", &mut catalog, &bp, &txn).unwrap();
            armed.store(true, Ordering::SeqCst);
            match run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn) {
                Err(e) => assert!(e.to_string().contains("injected"), "premise failed: the CREATE failed, but not at its sync: {e}"),
                Ok(_) => panic!("premise failed: the CREATE's checkpoint did not fail"),
            }
            armed.store(false, Ordering::SeqCst);
            let again = catalog
                .get_table("t")
                .expect("premise failed: the failed CREATE left no table in the running catalog")
                .first_directory_page_id;
            assert_eq!(again, root, "premise failed: the re-created table did not land on the dropped root, so the completion could not mistake it");
            run_sql("INSERT INTO t VALUES (2, 20);", &mut catalog, &bp, &txn).unwrap();
            // The crash: every handle goes.
        }
        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).expect("the open after the re-create failed");
        let ids = ids_of_t(&mut o).unwrap_or_else(|e| panic!("the re-created `t` was forgotten as if it were the dropped one: {e}"));
        assert_eq!(ids, vec![2], "the re-created `t` does not hold exactly its committed row");
    }

    /// **D250 review 1's F3, CRm (lane §3.5 test 8): a table re-created at the dropped root under a pin
    /// survives a crash.** Its `CreateTable` follows the DROP's record in the kept log; no row is
    /// written, so only that exclusion keeps the completion off it. A guard at `cb00606`.
    #[test]
    fn a_table_recreated_at_the_dropped_root_under_a_pin_survives_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("recreated_pinned.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let ok = |sql: &str, o: &mut OpenedDatabase| {
                run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            };
            ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut o);
            ok("INSERT INTO t VALUES (1, 10);", &mut o);
            let root = o.catalog.get_table("t").unwrap().first_directory_page_id;
            let pin = o.wal.pin(o.wal.base_lsn.load(Ordering::SeqCst)).expect("pin the log at its base");
            ok("DROP TABLE t;", &mut o);
            ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut o);
            assert_eq!(o.catalog.get_table("t").unwrap().first_directory_page_id, root, "premise failed: the re-created table did not land on the dropped root");
            drop(pin);
        }
        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock).expect("the open after the re-create failed");
        assert!(o.catalog.get_table("t").is_some(), "the re-created `t` was forgotten as if it were the dropped one");
    }

    /// **D250 review 1's F3, NAMEm (lane §3.5 test 9): a table re-created elsewhere under the dropped
    /// name survives a crash.** An index takes the dropped root, so no `CreateTable` and no heap record
    /// names it after the DROP; the new `t` lands on another root. Only the catalog's root match keeps
    /// the completion off the new `t`. A guard at `cb00606`.
    #[test]
    fn a_table_recreated_elsewhere_under_the_dropped_name_survives_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("recreated_elsewhere.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let ok = |sql: &str, o: &mut OpenedDatabase| {
                run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            };
            ok("CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER);", &mut o);
            ok("INSERT INTO keep VALUES (1, 10);", &mut o);
            ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut o);
            ok("INSERT INTO t VALUES (1, 10);", &mut o);
            let root = o.catalog.get_table("t").unwrap().first_directory_page_id;
            let pin = o.wal.pin(o.wal.base_lsn.load(Ordering::SeqCst)).expect("pin the log at its base");
            ok("DROP TABLE t;", &mut o);
            ok("CREATE INDEX ix ON keep (v);", &mut o);
            let ix = o.catalog.get_table("keep").unwrap().indexes.first().expect("the index").root_page_id;
            assert_eq!(ix, root, "premise failed: the index did not take the dropped root");
            ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut o);
            assert_ne!(o.catalog.get_table("t").unwrap().first_directory_page_id, root, "premise failed: the new `t` landed on the dropped root");
            drop(pin);
        }
        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock).expect("the open after the re-create failed");
        assert!(o.catalog.get_table("t").is_some(), "the new `t`, on another root, was forgotten as if it were the dropped one");
        assert!(o.catalog.get_table("keep").is_some(), "the table whose index took the dropped root is gone");
    }

    /// **D250, the F2 residual the lead accepted (lane §3.7 test 10): an EMPTY table re-created at the
    /// dropped root by a CREATE whose sync failed is forgotten by the next open, and its pages leak.**
    /// After the DROP's record the log holds no `CreateTable` (the CREATE failed before logging it) and
    /// no heap record, so nothing tells the new table from the dropped one. The CREATE was reported
    /// failed, and no committed row is lost (a committed row is test 7). What this pins is that the
    /// forget frees nothing: a failed CREATE's catalog entry names pages that are not known to be its
    /// own, because the catalog page and the bitmap page are separate writes, so a free could hit
    /// another owner. A guard at `f3f75af`, killed by FREEm. Only the three roots are probed; another
    /// page the CREATE allocated is not, which weakens the probe and cannot falsify it.
    #[test]
    fn an_empty_table_recreated_at_the_dropped_root_by_a_failed_create_is_forgotten_and_its_pages_leak() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("recreated_empty.db");
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owned = {
            let open = |p: PathBuf| OpenOptions::new().read(true).write(true).create(true).truncate(true).open(p).unwrap();
            let page_file = open(db.clone());
            let wal_file = open(PathBuf::from(format!("{}.wal", db.display())));
            let (bp, _wal, txn, mut catalog) =
                manual_db(&db, Arc::new(SyncFailsWhenArmed { file: page_file, armed: armed.clone() }), Arc::new(wal_file));
            run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn).unwrap();
            run_sql("INSERT INTO t VALUES (1, 10);", &mut catalog, &bp, &txn).unwrap();
            let root = catalog.get_table("t").unwrap().first_directory_page_id;
            run_sql("DROP TABLE t;", &mut catalog, &bp, &txn).unwrap();
            armed.store(true, Ordering::SeqCst);
            match run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn) {
                Err(e) => assert!(e.to_string().contains("injected"), "premise failed: the CREATE failed, but not at its sync: {e}"),
                Ok(_) => panic!("premise failed: the CREATE's checkpoint did not fail"),
            }
            armed.store(false, Ordering::SeqCst);
            let e = catalog.get_table("t").expect("premise failed: the failed CREATE left no table in the running catalog");
            let owned = vec![e.first_directory_page_id, e.time_travel_root, e.primary_index_root];
            assert_eq!(owned[0], root, "premise failed: the re-created table did not land on the dropped root");
            assert_eq!(owned.iter().collect::<HashSet<_>>().len(), 3, "premise failed: the re-created table's roots are not three pages: {owned:?}");
            // The crash: every handle goes, and nothing was written into the re-created table.
            owned
        };
        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock).expect("the open after the failed re-create failed");
        assert!(
            o.catalog.get_table("t").is_none(),
            "premise failed: the empty re-created `t` survived the open, so the residual whose pages this probes is gone"
        );
        assert_eq!(o.completed_drops, vec!["t".to_string()], "premise failed: the open did not complete the logged DROP");
        // `allocate` hands out the lowest clear bit, so every free page below the high-water mark comes
        // out before the first page at or above it.
        let high = o.bp.disk_manager.high_water().unwrap();
        for _ in 0..=high {
            let next = o.bp.disk_manager.allocate().unwrap();
            assert!(
                !owned.contains(&next),
                "page {next}, a root of the table the open forgot ({owned:?}), was handed out: the forget freed it"
            );
            if next >= high {
                return;
            }
        }
        panic!("`allocate` handed out more pages below the high-water mark {high} than there are");
    }

    /// **D250 review 1's F7, the lead's door (lane §3.7 test 11): a runtime attached to an open that
    /// completed a DROP forgets that table's provenance, and no other table's.** The executor's DROP
    /// forgets a table's row authors (B9); a DROP the next open completed never reached the executor,
    /// so `OpenedDatabase::attach_runtime` forgets them for every runtime built on the open. Red only
    /// under ATTm: it needs the door.
    #[test]
    fn a_runtime_attached_to_an_open_that_completed_a_drop_forgets_that_tables_provenance_and_no_other() {
        use crate::{agent_sql::runtime::table_id, branch::types::BranchId, provenance::{ProvId, RunEntity}};
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("half_drop_provenance.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            for sql in [
                "CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER);",
                "INSERT INTO keep VALUES (1, 10);",
                "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);",
                "INSERT INTO t VALUES (1, 10);",
            ] {
                run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            let record = {
                let e = o.catalog.get_table("t").expect("t");
                crate::wal::txn::DdlRecord {
                    op: DdlOp::DropTable,
                    table: "t".into(),
                    dir_root: e.first_directory_page_id,
                    time_travel_root: e.time_travel_root,
                    columns: Vec::new(),
                }
            };
            let err = o
                .txn
                .drop_checkpointed(record, || Err::<(), _>(FerroError::Internal("injected: the drop failed before it freed anything".into())))
                .expect_err("premise failed: the DROP succeeded although its mutation failed");
            assert!(err.to_string().contains("injected"), "premise failed: the DROP failed, but not in its mutation: {err}");
            // The crash: every handle goes, with the DROP's record durable and the catalog unchanged.
        }
        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock).expect("the open after a half-done DROP failed");
        assert_eq!(o.completed_drops, vec!["t".to_string()], "premise failed: the open did not complete the logged DROP of `t`");
        assert!(o.catalog.get_table("keep").is_some(), "premise failed: `keep` did not survive the open");

        let runtime = AgentRuntime::new();
        let run = RunEntity::new(ProvId::NONE, "agent", "run-1", "model", "v1", [7u8; 32], 1_700_000_000_000, BranchId::new(1, 0));
        let author = runtime.provenance().intern(&run).unwrap();
        for table in ["t", "keep"] {
            runtime.provenance().stamp_row(table_id(table).0, 1, author).unwrap();
            assert_eq!(
                runtime.provenance().row_author(table_id(table).0, 1).unwrap(),
                author,
                "premise failed: row 1 of `{table}` was not attributed before the runtime was attached"
            );
        }
        let runtime = o.attach_runtime(runtime);
        assert_eq!(
            runtime.provenance().row_author(table_id("t").0, 1).unwrap(),
            ProvId::NONE,
            "a runtime attached to the open still names an author for `t`, whose DROP the open completed: a \
             table created under the name later would inherit the dropped one's authors"
        );
        assert_eq!(
            runtime.provenance().row_author(table_id("keep").0, 1).unwrap(),
            author,
            "attaching the runtime forgot the authors of `keep`, which was never dropped"
        );
    }

    /// **D250, the D229 merge review's finding 3 (lane §3.8 test 13): after a pinned DROP whose freed
    /// pages a new table took, and a crash, the new table holds only its own row.** Test 1's hazard is
    /// a freed page that was never flushed. On the D229 tree every page is flushed before the free, so
    /// redo would skip the dropped table's records by page LSN, skip or no skip. REUSE is the hazard
    /// both trees share: `new_page` writes a zero page (LSN 0) to disk when it hands a page out, so the
    /// dropped table's records apply to it whatever was flushed before the free. Here `u` takes `t`'s
    /// freed heap page and its time-travel page under the pin, and nothing flushes `u`'s writes before
    /// the crash. `u` holds ONE row, so a replayed `t` row cannot hide under it. A sibling of test 1,
    /// which keeps its own assertions. A guard on this branch; killed by SKIPm, TTm, CLRm and LSNm on
    /// d250 alone and on the D229 merge (INFERRED).
    #[test]
    fn after_a_pinned_drop_whose_freed_pages_a_new_table_took_and_a_crash_the_new_table_holds_only_its_own_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("pinned_drop_reused.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let ok = |sql: &str, o: &mut OpenedDatabase| {
                run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            };
            ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut o);
            for id in 1..=3 {
                ok(&format!("INSERT INTO t VALUES ({id}, {});", id * 10), &mut o);
            }
            ok("UPDATE t SET v = 11 WHERE id = 1;", &mut o);
            let mut s = Session::new();
            for sql in ["BEGIN;", "INSERT INTO t VALUES (7, 70);", "ROLLBACK;"] {
                run_sql_in(sql, &mut o.catalog, &o.bp, &o.txn, &mut s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            assert!(has_clr(&o.wal), "premise: the rolled-back INSERT left no CLR");
            let (t_heap, t_tt) = {
                let e = o.catalog.get_table("t").unwrap();
                (e.first_directory_page_id, e.time_travel_root)
            };
            let base = o.wal.base_lsn.load(Ordering::SeqCst);
            let pin = o.wal.pin(base).expect("pin the log at its base");
            // Every record of `t` is below this, and every record of `u` above it.
            let dropped_at = o.wal.next_lsn.load(Ordering::SeqCst);
            ok("DROP TABLE t;", &mut o);
            ok("CREATE TABLE u (id INTEGER NOT NULL, v INTEGER);", &mut o);
            ok("INSERT INTO u VALUES (101, 1010);", &mut o);
            ok("UPDATE u SET v = 1011 WHERE id = 101;", &mut o);
            assert_eq!(o.wal.base_lsn.load(Ordering::SeqCst), base, "premise: the pin did not keep the log");
            let (u_heap, u_tt) = {
                let e = o.catalog.get_table("u").unwrap();
                (e.first_directory_page_id, e.time_travel_root)
            };
            let writes = heap_writes_at(&o.wal);
            // The pages `u` writes that `t`'s records also write, and that are older on disk than the
            // first of those records: redo without the skip would apply `t`'s records to each.
            let exposed = |t_root: u32, u_root: u32| -> Vec<u32> {
                let mut pages: Vec<u32> = writes
                    .iter()
                    .filter(|(d, _, at)| *d == u_root && *at > dropped_at)
                    .map(|(_, p, _)| *p)
                    .filter(|p| {
                        let first_of_t =
                            writes.iter().filter(|(d, q, at)| *d == t_root && q == p && *at < dropped_at).map(|(_, _, at)| *at).min();
                        let on_disk = o.bp.disk_manager.read(*p).unwrap();
                        let disk_lsn = u64::from_be_bytes(on_disk[11..19].try_into().unwrap());
                        first_of_t.is_some_and(|first| disk_lsn < first)
                    })
                    .collect();
                pages.sort_unstable();
                pages.dedup();
                pages
            };
            assert!(
                !exposed(t_heap, u_heap).is_empty(),
                "premise failed: no heap page of `u` is one `t`'s records write and would take them on disk, so \
                 redo without the skip meets nothing here: {writes:?}"
            );
            assert!(
                !exposed(t_tt, u_tt).is_empty(),
                "premise failed: no time-travel page of `u` is one `t`'s time-travel records write and would take \
                 them on disk: {writes:?}"
            );
            drop(pin);
            // The crash: every handle goes, and nothing flushed `u`'s writes.
        }
        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock)
            .unwrap_or_else(|e| panic!("the open after a pinned DROP whose pages were reused failed: redo replayed the dropped table onto `u`'s pages: {e}"));
        assert!(o.catalog.get_table("t").is_none(), "the dropped table came back");
        let mut rows = |sql: &str| match run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}")) {
            crate::execution::executor::Outcome::Rows(rows) => rows,
            _ => panic!("`{sql}` did not return rows"),
        };
        let only = vec![vec![Value::Integer(101), Value::Integer(1011)]];
        assert_eq!(rows("SELECT id, v FROM u;"), only, "`u` after the reopen: a row of the dropped `t` was replayed into it, or its own was lost");
        assert_eq!(rows("SELECT id, v FROM u WHERE id = 101;"), only, "`u` by key after the reopen");
    }

    /// **D250 review 2's R2-1 (lane §3.10 test 14): a table dropped twice at one root is completed once,
    /// and the database opens.** `DROP t`, a re-CREATE at the dropped root whose sync fails (no
    /// `CreateTable` is logged), then a second `DROP t` whose record is durable and whose mutation
    /// fails. Both `DropTable` records pass every clause of the completion. At `b57a5d0` each was
    /// completed, the second `forget_dropped_table` failed `require_table`, and `open_recovered`
    /// answered `Err` at this open and at every later one. Only the LAST `DropTable` per root counts.
    #[test]
    fn a_table_dropped_twice_at_one_root_is_completed_once_and_the_database_opens() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("dropped_twice.db");
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let open = |p: PathBuf| OpenOptions::new().read(true).write(true).create(true).truncate(true).open(p).unwrap();
            let page_file = open(db.clone());
            let wal_file = open(PathBuf::from(format!("{}.wal", db.display())));
            let (bp, _wal, txn, mut catalog) =
                manual_db(&db, Arc::new(SyncFailsWhenArmed { file: page_file, armed: armed.clone() }), Arc::new(wal_file));
            run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn).unwrap();
            run_sql("INSERT INTO t VALUES (1, 10);", &mut catalog, &bp, &txn).unwrap();
            let root = catalog.get_table("t").unwrap().first_directory_page_id;
            run_sql("DROP TABLE t;", &mut catalog, &bp, &txn).unwrap();
            armed.store(true, Ordering::SeqCst);
            match run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn) {
                Err(e) => assert!(e.to_string().contains("injected"), "premise failed: the CREATE failed, but not at its sync: {e}"),
                Ok(_) => panic!("premise failed: the CREATE's checkpoint did not fail"),
            }
            armed.store(false, Ordering::SeqCst);
            assert_eq!(
                catalog.get_table("t").expect("premise failed: the failed CREATE left no table in the running catalog").first_directory_page_id,
                root,
                "premise failed: the re-created table did not land on the dropped root"
            );
            let record = {
                let e = catalog.get_table("t").unwrap();
                crate::wal::txn::DdlRecord {
                    op: DdlOp::DropTable,
                    table: "t".into(),
                    dir_root: e.first_directory_page_id,
                    time_travel_root: e.time_travel_root,
                    columns: Vec::new(),
                }
            };
            let err = txn
                .drop_checkpointed(record, || Err::<(), _>(FerroError::Internal("injected: the second drop failed before it freed anything".into())))
                .expect_err("premise failed: the second DROP succeeded although its mutation failed");
            assert!(err.to_string().contains("injected"), "premise failed: the second DROP failed, but not in its mutation: {err}");
            // The crash: every handle goes, with two `DropTable` records of `t` at one root in the log.
        }
        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock)
            .unwrap_or_else(|e| panic!("the open after a table was dropped twice at one root failed, and so would every later one: {e}"));
        assert!(o.catalog.get_table("t").is_none(), "the logged DROP of the re-created `t` was not completed");
        assert_eq!(o.completed_drops, vec!["t".to_string()], "the open did not complete the DROP of `t` exactly once");
    }

    /// **D250 review 2's R2-2 (lane §3.10 test 15): a table re-created at the dropped root and then
    /// altered survives a crash.** Its CREATE failed at the sync, so it logged no `CreateTable`, and it
    /// holds no row; but the ALTER answered `Ok` and logged an `AlterColumn` at the root after the
    /// DROP's record. ALTER does not checkpoint, so the DROP's record stays in the log without a pin.
    /// At `b57a5d0` only a `CreateTable` counted as re-creation, so the open forgot `t` and the
    /// acknowledged ALTER with it. Any DDL record at the root after the DROP now keeps the table.
    #[test]
    fn a_table_recreated_at_the_dropped_root_and_then_altered_survives_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("recreated_altered.db");
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let open = |p: PathBuf| OpenOptions::new().read(true).write(true).create(true).truncate(true).open(p).unwrap();
            let page_file = open(db.clone());
            let wal_file = open(PathBuf::from(format!("{}.wal", db.display())));
            let (bp, _wal, txn, mut catalog) =
                manual_db(&db, Arc::new(SyncFailsWhenArmed { file: page_file, armed: armed.clone() }), Arc::new(wal_file));
            run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn).unwrap();
            run_sql("INSERT INTO t VALUES (1, 10);", &mut catalog, &bp, &txn).unwrap();
            let root = catalog.get_table("t").unwrap().first_directory_page_id;
            run_sql("DROP TABLE t;", &mut catalog, &bp, &txn).unwrap();
            armed.store(true, Ordering::SeqCst);
            match run_sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn) {
                Err(e) => assert!(e.to_string().contains("injected"), "premise failed: the CREATE failed, but not at its sync: {e}"),
                Ok(_) => panic!("premise failed: the CREATE's checkpoint did not fail"),
            }
            armed.store(false, Ordering::SeqCst);
            assert_eq!(
                catalog.get_table("t").expect("premise failed: the failed CREATE left no table in the running catalog").first_directory_page_id,
                root,
                "premise failed: the re-created table did not land on the dropped root"
            );
            run_sql("ALTER TABLE t ADD COLUMN w INTEGER;", &mut catalog, &bp, &txn)
                .unwrap_or_else(|e| panic!("premise failed: the ALTER was not acknowledged: {e}"));
            assert_eq!(
                catalog.get_table("t").unwrap().first_directory_page_id,
                root,
                "premise failed: the ALTER moved `t` off the dropped root, so the root match alone would keep it"
            );
            // The crash: every handle goes.
        }
        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).expect("the open after the ALTER failed");
        assert!(
            o.catalog.get_table("t").is_some(),
            "the re-created `t`, altered with `Ok` after its failed CREATE, was forgotten at the next open, and the \
             acknowledged ALTER with it"
        );
        run_sql("SELECT id, w FROM t;", &mut o.catalog, &o.bp, &o.txn)
            .unwrap_or_else(|e| panic!("the altered `t` does not answer for the column the ALTER added: {e}"));
    }
}
