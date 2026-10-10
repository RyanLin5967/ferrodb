use std::{collections::{BTreeMap, HashMap, HashSet}, fs::OpenOptions, path::{Path, PathBuf}, sync::{Arc, atomic::Ordering}};

use crate::branch::arena::ArenaPageStore;
use crate::{agent_sql::runtime::{table_id, AgentRuntime}, buffer::buffer_pool::BufferPoolManager, provenance::{DurableProvenanceStore, ProvenanceStore}, catalog::{catalog::Catalog, column::Value}, error::FerroError, storage::{db_lock::DbLock, disk_manager::DiskManager, heap_file_manager::{HeapFileManager, RecordId}, heap_page::Page, index::BPlusTreeManager, index_fulltext::{indexed_text, post_tokens}, index_page::{entry_too_large, first_entry_over_bound, EntryOf, RECOVERY_REMEDY}, tuple::Tuple}, wal::{log::{DdlOp, RecKind, WalManager}, txn::{RetiredSlot, TxnEntry, TxnManager, TxnStatus}}};

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

    // **D212 (a') AMENDED 3, item 4: a log that holds REVERT history is recovered only with the
    // store attached**, refused here before recovery writes anything. Such a log was written by a
    // manager that had one (`bind_history` refuses without), and recovering it without one would let
    // the next checkpoint discard the history's only copy. `open_recovered`, the one open path,
    // attaches `<db>.history` before calling this.
    if txn.history_store().is_none() {
        let history = records.iter().filter(|r| matches!(r.kind, RecKind::RevertHistory { .. })).count();
        if history > 0 {
            return Err(FerroError::Internal(format!(
                "the log holds {history} REVERT history record(s) and no history store is attached, so \
                 the next checkpoint would discard them; open the database through \
                 wal::recovery::open_recovered, which attaches <db>.history before recovering"
            )));
        }
    }
    // **AMENDED 3, item 10a: the store must be this database's.** For every attached store, with or
    // without history (review of `c9d1e6e`, F1), the log declares its incarnation after every
    // truncation and at an open that finds none (`TxnManager::declare_history`); a store that names
    // another is refused here, before recovery writes anything. Not checked, stated: a log with no
    // declaration — written before this build, or cut by a crash between a truncation and its
    // declaration, whose next open then declares the store it finds; a history file copied from a
    // fork of this database, which shares its incarnation by construction; and a plain
    // `replication::backup::restore` over a path whose earlier database had history, whose log and
    // store both still name that database, so the restored rows inherit its history (the history
    // twin of the limit `start_fresh_quarantine` states for plain `restore`).
    if let (Some(store), Some(declared)) =
        (txn.history_store(), crate::wal::history::declared_incarnation(&records))
    {
        store.adopt_or_check(declared)?;
        txn.note_history_declared();
    }
    // A log that holds nothing but incarnation declarations has nothing to recover (review of
    // `a71d3ed`, F1): every truncation and every open writes one, and reading one as "recovered"
    // would rebuild every index at every open.
    if records.iter().all(|r| matches!(r.kind, RecKind::IncarnationDecl { .. })) {
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
        if matches!(rec.kind, RecKind::Commit | RecKind::TxnEnd) {
            ended.insert(rec.txn_id);
            if matches!(rec.kind, RecKind::Commit) {
                committed.insert(rec.txn_id);
            }
        }
        // The pages redo below writes, from the one exhaustive list redo uses too.
        touched.extend(rec.kind.heap_page());
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
    //
    // **D267: and mark each page the log names allocated.** Its bitmap bit was set by an unsynced
    // write, and a power loss can drop it while the log, which a COMMIT syncs, keeps the page's
    // records. Redo then rebuilds the page and the directory repair lists it, and without this the
    // next `allocate` handed it to a second owner. `open_recovered`'s checkpoint makes the bit
    // durable. A page of a table that a later `DropTable` in this log dropped is not claimed: D250's
    // skip keeps its records out of `touched` in the analysis above. (On d268 alone, without D250,
    // such a page was claimed and leaked; it was never handed to two owners.)
    let bp = &txn.bp;
    for (_, page_id) in &touched {
        if bp.disk_manager.read(*page_id).is_err() {
            bp.disk_manager.write(*page_id, &Page::empty(*page_id).serialize()?)?;
        }
        bp.disk_manager.claim(*page_id)?;
    }

    // redo. A `Clr` goes in whole: `redo_one` applies the record it carries, and has to know it
    // came from a CLR (D213).
    for rec in &records {
        if skipped(&rec.kind, rec.lsn) {
            continue;
        }
        // One exhaustive list (`RecKind::heap_page`, D268) decides what redo applies, now that D250
        // skips a dropped table's records first: `HeapInitPage` included.
        if rec.kind.heap_page().is_some() {
            redo_one(bp, rec.lsn, &rec.kind, !legacy)?;
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

    // **D212 (a'): REVERT history the store does not hold.** A transaction's history records are in
    // the log beside its rows; if a crash beat the checkpoint that writes them into the store, this
    // is their only copy. Only a transaction with a `Commit` counts — `ended` would also admit one
    // that aborted, since `abort` writes `TxnEnd` too — and the store queues a record only if it does
    // not already hold its `hseq`, wherever it falls at or above the store's prune floor (AMENDED 2,
    // F9; AMENDED 3, item 3). The open's checkpoint writes them. Each record carries its
    // transaction's `Commit` LSN (AMENDED 3, item 2); a transaction without one is not committed.
    // With no store attached, a log holding any was refused above (AMENDED 3, item 4).
    let (history, dropped) = crate::wal::history::committed_in(&records)?;
    if dropped > 0 {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr(),
            "ferrodb: {dropped} committed REVERT history record(s) in the log begin before it and \
             cannot be read back from it; unless the history store already holds them, REVERT of \
             those merges will be refused"
        );
    }
    if let Some(store) = txn.history_store() {
        store.enqueue(history);
    }

    // repair directory
    for (dir_root, page_id) in &touched {
        let hfm = HeapFileManager::open(*dir_root, bp.clone());
        let frame_i = bp.fetch_page(*page_id)?;
        let frame = bp.frames[frame_i].read().unwrap();
        let page = Page::deserialize_at(*page_id, frame.data)?;
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
    // From `kind`, not `op`: a CLR carrying a CLR writes no page, and `heap_page` says so.
    let Some((_, page_id)) = kind.heap_page() else {
        return Ok(());
    };
    let frame_i = bp.fetch_page(page_id)?;
    let mut frame = bp.frame_write(frame_i);
    let stored_id = u32::from_be_bytes(frame.data[1..5].try_into().unwrap());
    let mut page = match op {
        // **D268: a page's init record resets the page**, unless it already holds its own image at or
        // past this record, which the LSN gate below then skips as usual.
        //
        // A reused page keeps its old owner's image on disk, with its own id, and a listed page whose
        // image never reached disk is zeros. A power loss can drop every page write since the last
        // checkpoint and keep the log. Redo parsed such an image and applied the new owner's first
        // insert onto it: a live slot 0 refused the restore at every open, and a free one took the row
        // while the old owner's other rows stayed. Any image here that is not this page's own at or
        // past this record predates the page: a previous owner's, zeros, or another structure's. So a
        // refused parse is not an error in this arm; everywhere else it still is (D256).
        //
        // Gated, not an unconditional reset (PostgreSQL's `XLogInitBufferForRedo`), because this
        // engine writes heap pages without logging in one place: `catalog::alter::rewrite_heap`
        // through `HeapFileManager::open`. Those writes never assign `lsn`, and the page's image
        // carries this record's LSN from its creation (`HeapFileManager::add_empty_page`), so under a
        // kept log a page holding such rows is its own image at this LSN, and is kept
        // (`tests/d268_power_loss_redo.rs`, tests 3 and 11).
        RecKind::HeapInitPage { .. } => match Page::deserialize_at(page_id, frame.data) {
            Ok(own) if own.page_id == page_id && own.lsn >= lsn => own,
            _ => Page::empty(page_id),
        },
        _ if stored_id != page_id => Page::empty(page_id),
        _ => Page::deserialize_at(page_id, frame.data)?,
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
        // The page selection above already made it empty; only its LSN moves.
        RecKind::HeapInitPage { .. } => {}
        // `heap_page` named a page, so `op` is one of the kinds above. Listed rather than `_`, so a
        // new kind is a compile error here too.
        RecKind::Begin
        | RecKind::Commit
        | RecKind::Abort
        | RecKind::TxnEnd
        | RecKind::Checkpoint
        | RecKind::Ddl { .. }
        | RecKind::RunIdentity { .. }
        | RecKind::RevertHistory { .. }
        | RecKind::IncarnationDecl { .. }
        | RecKind::Clr { .. } => unreachable!(),
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
    /// Every table a `DropTable` record in the retained log names and this open's catalog does not
    /// (D250 review 2's R2-4). It contains `completed_drops`. `open_recovered` forgot them in the
    /// database's provenance file; [`OpenedDatabase::attach_runtime`] forgets them in an in-memory
    /// store. PRIVATE for the same reason.
    dropped_tables: Vec<String>,
    /// **D212 (a') AMENDED 3, item 4 — REVERT's history store**, `<db>.history`, opened by
    /// [`open_recovered`] and registered with `txn` before `recover`. [`OpenedDatabase::attach_runtime`]
    /// hands it to the runtime. PRIVATE, so a runtime reaches it only through the door.
    history: Arc<crate::wal::history::HistoryStore>,
    /// The database's provenance file, when this open opened it to forget `dropped_tables` in it
    /// (D250 review 3's A). Handed to the runtime by [`OpenedDatabase::attach_runtime`]. Held here so
    /// the store stays live from the open to the attach: [`DurableProvenanceStore::shared`] keeps one
    /// store per file per process only while someone holds it (D250 review 4's N4).
    provenance: Option<Arc<DurableProvenanceStore>>,
    /// Where that file lives: [`provenance_path`] of the database.
    provenance_path: PathBuf,
}

/// Where a database's durable provenance store lives: beside it, `<db>.provenance`. One spelling, for
/// `open_recovered`'s forget and [`OpenedDatabase::attach_runtime`] (D250 review 3's A); the CLI
/// spelled it itself before.
pub fn provenance_path(db_path: &Path) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(".provenance");
    PathBuf::from(path)
}

/// DROPs whose table's authors `open_recovered` could not forget in the provenance file, since
/// process start (D250 review 3's A). Such an open is refused before its rebuild and checkpoint, so the
/// log keeps the `DropTable` and the next open retries (D250 review 4's N2); this counts and the open
/// prints it.
pub static PROVENANCE_FORGET_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// See [`PROVENANCE_FORGET_FAILURES`].
pub fn provenance_forget_failures() -> u64 {
    PROVENANCE_FORGET_FAILURES.load(Ordering::Relaxed)
}

/// Where an attached runtime keeps its row authors (D250 review 3's A).
pub enum ProvenanceBacking {
    /// The database's provenance file (the CLI): the store `open_recovered` opened, which it has
    /// already made forget every dropped table, or the file opened here when it had nothing to forget.
    Durable,
    /// The runtime's own in-memory store (pgserver). The dropped tables are forgotten in it, which is
    /// a stated no-op in production: a new process's in-memory store starts empty.
    InMemory,
}

impl OpenedDatabase {
    /// **The one door an agent runtime comes through onto an opened database** (D250 review 1's F7,
    /// the lead's ruling, the same "one function both call" rule as D204). A table the retained log
    /// records as dropped, and the catalog does not name, must not keep its authors, or a table later
    /// created under the name inherits them (B9, `AgentRuntime::forget_table`).
    ///
    /// - [`ProvenanceBacking::Durable`] installs the database's provenance file.
    ///   [`open_recovered`] forgot the dropped tables in it BEFORE its checkpoint truncated their
    ///   records (D250 review 3's A), so nothing is left to do here but hand it over. One store per
    ///   file per process ([`DurableProvenanceStore::shared`], D250 review 4's N4): the open's store,
    ///   or the live one another attach already holds, or opened here.
    /// - [`ProvenanceBacking::InMemory`] keeps the runtime's own store and forgets the dropped tables
    ///   in it, which is a stated no-op in production (pgserver's store starts empty).
    ///
    /// The lists are private to this module, so an entry point cannot run the forget itself and cannot
    /// leave it out; `tests/open_path_allowlist.rs` checks that both production entry points build
    /// their runtime through here, with their backing, and that nothing else names
    /// `with_durable_provenance`. **Not type-forced, stated:** `AgentRuntime`'s constructors are
    /// public and used across the test suite, so a runtime can still be built without this door.
    ///
    /// `&self` and no drain: every runtime attached to one open is attached the same way.
    ///
    /// **D212 (a') AMENDED 3, item 4: it also hands the runtime this database's REVERT history
    /// store**, so the runtime is refused over any other database's log
    /// (`AgentRuntime::attach_history`).
    pub fn attach_runtime(&self, runtime: AgentRuntime, backing: ProvenanceBacking) -> Result<Arc<AgentRuntime>, FerroError> {
        let runtime = match backing {
            ProvenanceBacking::Durable => {
                let store = match &self.provenance {
                    Some(store) => store.clone(),
                    None => DurableProvenanceStore::shared(&self.provenance_path)?,
                };
                runtime.with_provenance_store(store)
            }
            ProvenanceBacking::InMemory => {
                for table in &self.dropped_tables {
                    runtime.forget_table(table)?;
                }
                runtime
            }
        };
        runtime.bind_history_store(Arc::clone(&self.history));
        Ok(Arc::new(runtime))
    }
}

/// The heap a record writes, as its directory root: a `Heap*` record's own, or the one a CLR redoes.
fn heap_root(kind: &RecKind) -> Option<u32> {
    // Through D268's one exhaustive list, not a `match` of its own ending in `_ => None`: that arm
    // skipped `HeapInitPage` without a word, so a dropped table's page initialisation would have been
    // redone onto a page a later table may own. A `Clr` answers for the record it carries.
    kind.heap_page().map(|(dir_root, _)| dir_root)
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
///   so a CREATE INDEX on a table re-created as below is not seen while something keeps the old DROP
///   in the log: a pin, or a release still owed (its checkpoint then answers `KeptForOwed` without
///   truncating; D250 review 3's B). Stated;
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

/// D250 review 4's N2 seam: nothing outside this crate's unit tests. The test half, after the tests
/// module, makes one named file's next open-time forget fail through the store's own
/// `fail_next_append`, so the store takes its real failure path.
#[cfg(not(test))]
fn inject_open_forget_failure(_store: &DurableProvenanceStore, _path: &Path) {}

/// **D204 — THE way to open a database file.** Every binary calls this; none spells the sequence
/// out for itself (`tests/open_path_allowlist.rs` enforces that).
///
/// The order is the whole content:
/// 1. open the file, register the arena region a previous session persisted (D239), and open the
///    buffer pool, the WAL and the transaction manager, and attach the WAL;
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
    let dm = Arc::new(DiskManager::new(file)?);
    // **D239: the arena region is reserved BEFORE recovery.** The arena store attaches only after
    // this returns, and until a region is registered every arena page reads as free to the page
    // allocator, because arena pages never set bitmap bits. Recovery's directory repair and the
    // rebuild below both allocate, and a rebuild that outgrew a full table region was handed arena
    // pages and wrote over live branch data. See `ArenaPageStore::reserve_persisted_floor`.
    let mut arena_path = db_path.as_os_str().to_os_string();
    arena_path.push(".arena");
    ArenaPageStore::reserve_persisted_floor(&dm, Path::new(&arena_path))?;
    let bp = Arc::new(BufferPoolManager::new(dm));
    // D280: refuses a data file whose pages carry LSNs this log never issued (a restored backup, a
    // replica's file, a lost `<db>.wal`), before anything else is created beside it. Every binary
    // opens through here, so the CLI and pgserver share the refusal as well as the sequence. The
    // reservation above only reads `<db>.arena` and registers a region in memory, so a refused open
    // still leaves nothing behind.
    let wal = Arc::new(WalManager::open_for_database(db_path, &bp.disk_manager)?);
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    // **D212 (a') AMENDED 3, item 4: REVERT's history store, opened HERE and registered BEFORE
    // `recover`.** The open's catch-up puts the committed history a crash left only in the log into
    // it, and the checkpoint below drains it before truncating that log. This is the one open path,
    // and between the manager's construction and `recover` is the only moment a store can be
    // registered in time; `recover` refuses a log holding history with none.
    let history = crate::wal::history::HistoryStore::open_for_database(db_path, existed)?;
    txn.attach_history_store(Arc::clone(&history))?;
    let recovered = recover(&txn)?;
    let mut catalog = if existed {
        Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID)?
    } else {
        Catalog::create(bp.clone())?
    };
<<<<<<< ours
    // D250 (b): finish every DROP the log records and the catalog on disk does not. `recover` has
    // just skipped those tables' records, so keeping them in the catalog would serve a table whose
    // recent writes were not replayed, over pages the DROP may have freed. Removed from the catalog
    // WITHOUT freeing: the directory repair above may already have allocated a page the DROP freed,
    // and a second free would hit its new owner. Stated cost: pages the DROP had not freed yet leak,
    // one table's worth per incomplete DROP. Nothing on this branch reclaims them, so the cost is
    // D250's, stated. On the D229 merge the DROP's page intent frees them after the open's
    // checkpoint (as `d208-rootcell` reports), and this sentence is D229's to word.
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
    // **D250 review 3's A (the lead's decision): the provenance forget runs HERE, before the
    // checkpoint below truncates the log.** Every table the retained log records as dropped and the
    // catalog now does not name (a table re-created under the name is named, and its provenance is
    // its own) is forgotten in the database's provenance file, when there is one. The completion
    // above becomes durable only at that checkpoint, so a crash before it repeats this open whole,
    // and the forget is idempotent: nothing is re-declared, and no later open is left to finish it.
    // `dd939ab` re-declared each such `DropTable` after the checkpoint instead, and `recover` counts
    // any non-empty log as recovered, so every open of a process that never truncated after its open
    // (pgserver always, a killed CLI) rebuilt every index. The executor's own forget (B9) runs inside
    // the DROP's unit, before its barrier's truncation (D250 review 4's N1): so either it has run, or
    // the log still holds the `DropTable` and this finds it. The store is handed to the runtime
    // through `attach_runtime`, one store per file per process (`DurableProvenanceStore::shared`).
    //
    // **D250 review 4's N2: a forget that fails REFUSES the open, here, before the rebuild and the
    // checkpoint.** Counted and continued (as at `43864d7`), the open's checkpoint truncated the
    // `DropTable`, the only input from which a later open computes what to forget; and a process that
    // went on could truncate it at its next checkpoint, or re-create the table under the name, which
    // takes it out of that computation. Refused here, nothing has run past `recover`, the same state a
    // failed `DurableProvenanceStore::open` leaves, and every later open retries the forget. Stated
    // cost: while the file cannot be appended, no entry point opens a database whose retained log holds
    // a DROP, pgserver included (review 4's N3).
    let mut dropped_tables: Vec<String> = logged_drops
        .into_iter()
        .filter(|(table, ..)| catalog.get_table(table).is_none())
        .map(|(table, ..)| table)
        .collect();
    dropped_tables.sort_unstable();
    dropped_tables.dedup();
    let provenance_path = provenance_path(db_path);
    let provenance = if dropped_tables.is_empty() || !provenance_path.exists() {
        None
    } else {
        let store = DurableProvenanceStore::shared(&provenance_path)?;
        inject_open_forget_failure(&store, &provenance_path);
        for table in &dropped_tables {
            if let Err(e) = store.forget_table(table_id(table).0) {
                use std::io::Write;
                PROVENANCE_FORGET_FAILURES.fetch_add(1, Ordering::Relaxed);
                let why = format!(
                    "the DROP of `{table}` is in the log, but its authors could not be forgotten in {} ({e}); \
                     the open is refused and the log kept, so the next open retries the forget",
                    provenance_path.display()
                );
                let _ = writeln!(std::io::stderr(), "ferrodb: {why}");
                return Err(FerroError::Provenance(format!("refusing to open {}: {why}", db_path.display())));
            }
        }
        Some(store)
    };
||||||| base
=======
    // D230 review 3, F2 (the lead's decision): from here on a failed catalog persist is owed on the
    // transaction manager, and every checkpoint keeps the log until a persist succeeds. Attached
    // before the rebuild below, whose own persist settles it too. This is the one production open
    // (`tests/open_path_allowlist.rs`), so every production catalog carries the debt.
    catalog.owe_persists_to(txn.catalog_persist_debt());
>>>>>>> theirs
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
<<<<<<< ours
        if !matches!(
            kept,
            crate::wal::txn::CheckpointOutcome::KeptForOwed(_) | crate::wal::txn::CheckpointOutcome::KeptForHistory
        ) && stale
        {
||||||| base
        if !matches!(kept, crate::wal::txn::CheckpointOutcome::KeptForOwed(_)) && stale {
=======
        if !matches!(
            kept,
            crate::wal::txn::CheckpointOutcome::KeptForOwed(_) | crate::wal::txn::CheckpointOutcome::KeptForCatalog
        ) && stale
        {
>>>>>>> theirs
            if let Err(e) = std::fs::remove_file(&marker) {
                let _ = writeln!(
                    std::io::stderr(),
                    "ferrodb: rebuilt the indexes, but could not remove {} ({e}); the next open rebuilds again",
                    marker.display()
                );
            }
        }
    }
    // D212 (a') AMENDED 3, item 10a (review of `a71d3ed`, F5): the log this open leaves declares the
    // history's incarnation when the store holds history and the retained log has no declaration,
    // so a log a pin kept since a store was attached is checked at the next open. A log holding only
    // declarations is not "recovered" (`recover`), so this costs the next open nothing, and an open
    // that finds a declaration appends none.
    txn.declare_history_if_missing()?;
    Ok(OpenedDatabase {
        bp,
        wal,
        txn,
        catalog,
        recovered,
        #[cfg(test)]
        completed_drops,
        dropped_tables,
        provenance,
        provenance_path,
        history,
    })
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

    // ---- D253: a checkpoint's truncation must not discard what was appended in its window ------
    //
    // Every checkpoint checks that no transaction is attached, then flushes, syncs and truncates, and
    // `truncate` discards the WHOLE log. On #16 the non-DDL entries (`checkpoint`, `apply_checkpoint`,
    // `ddl_checkpoint`, `checkpoint_keeping_owed`, `checkpoint_after_frees`) release the attach
    // table before the body runs, so a second thread can begin, write and commit inside the window,
    // and the truncation removes its records. `ddl_unit` (`ddl_checkpointed`, `drop_checkpointed`)
    // holds the attach table throughout, but a txn-0 appender (`log_ddl`) can still append inside its
    // window (`frontier/truncate_race_adversary.md` @ `e288e3b`, `lane_d253.md` AMENDMENT 3 in
    // artie-research). Production cannot reach either today: every appender runs under pgwire's
    // catalog mutex or on the CLI's single thread. These tests have no such lock. They aim the second
    // thread with the checkpoint's test pause points, so the schedule is exact rather than timed.

    /// Every D253 test holds this, so the process-wide checkpoint counters move only by what the
    /// test holding it did (T7 asserts exact deltas).
    static D253_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn d253_serial() -> std::sync::MutexGuard<'static, ()> {
        D253_SERIAL.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Hand `txn`'s next checkpoint a pause at `at` that reports it has arrived and then waits to be
    /// released. Returns the arrival signal and the release.
    fn park_checkpoint_at(
        txn: &TxnManager,
        at: crate::wal::txn::CheckpointPausePoint,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (arrived_tx, arrived) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        txn.set_checkpoint_pause(
            at,
            Box::new(move || {
                arrived_tx.send(()).unwrap();
                // A dropped sender also releases, so a failing test thread cannot wedge this one.
                let _ = released.recv();
            }),
        );
        (arrived, release)
    }

    /// A checkpoint entry point, as a test drives it. Path A of lane_d253 AMENDMENT 3: every one of
    /// these reaches `checkpoint_or_keep_locked`, and each shape below runs through each of them.
    type CheckpointEntry = fn(&TxnManager) -> Result<(), FerroError>;

    fn via_checkpoint(txn: &TxnManager) -> Result<(), FerroError> {
        txn.checkpoint()
    }

    fn via_apply_checkpoint(txn: &TxnManager) -> Result<(), FerroError> {
        txn.apply_checkpoint()
    }

    fn via_ddl_checkpoint(txn: &TxnManager) -> Result<(), FerroError> {
        txn.ddl_checkpoint()
    }

    fn via_checkpoint_keeping_owed(txn: &TxnManager) -> Result<(), FerroError> {
        txn.checkpoint_keeping_owed().map(|_| ())
    }

    fn via_checkpoint_after_frees(txn: &TxnManager) -> Result<(), FerroError> {
        txn.checkpoint_after_frees().map(|_| ())
    }

    #[test]
    fn a_commit_inside_the_window_of_checkpoint_survives_a_crash() {
        commit_inside_the_window(via_checkpoint);
    }

    #[test]
    fn a_commit_inside_the_window_of_apply_checkpoint_survives_a_crash() {
        commit_inside_the_window(via_apply_checkpoint);
    }

    #[test]
    fn a_commit_inside_the_window_of_ddl_checkpoint_survives_a_crash() {
        commit_inside_the_window(via_ddl_checkpoint);
    }

    #[test]
    fn a_commit_inside_the_window_of_checkpoint_keeping_owed_survives_a_crash() {
        commit_inside_the_window(via_checkpoint_keeping_owed);
    }

    #[test]
    fn a_commit_inside_the_window_of_checkpoint_after_frees_survives_a_crash() {
        commit_inside_the_window(via_checkpoint_after_frees);
    }

    #[test]
    fn an_uncommitted_write_as_checkpoint_starts_is_undone_after_a_crash() {
        uncommitted_write_as_it_starts(via_checkpoint);
    }

    #[test]
    fn an_uncommitted_write_as_apply_checkpoint_starts_is_undone_after_a_crash() {
        uncommitted_write_as_it_starts(via_apply_checkpoint);
    }

    #[test]
    fn an_uncommitted_write_as_ddl_checkpoint_starts_is_undone_after_a_crash() {
        uncommitted_write_as_it_starts(via_ddl_checkpoint);
    }

    #[test]
    fn an_uncommitted_write_as_checkpoint_keeping_owed_starts_is_undone_after_a_crash() {
        uncommitted_write_as_it_starts(via_checkpoint_keeping_owed);
    }

    #[test]
    fn an_uncommitted_write_as_checkpoint_after_frees_starts_is_undone_after_a_crash() {
        uncommitted_write_as_it_starts(via_checkpoint_after_frees);
    }

    #[test]
    fn checkpoint_with_nothing_in_its_window_still_truncates() {
        nothing_in_the_window(via_checkpoint);
    }

    #[test]
    fn apply_checkpoint_with_nothing_in_its_window_still_truncates() {
        nothing_in_the_window(via_apply_checkpoint);
    }

    #[test]
    fn ddl_checkpoint_with_nothing_in_its_window_still_truncates() {
        nothing_in_the_window(via_ddl_checkpoint);
    }

    #[test]
    fn checkpoint_keeping_owed_with_nothing_in_its_window_still_truncates() {
        nothing_in_the_window(via_checkpoint_keeping_owed);
    }

    #[test]
    fn checkpoint_after_frees_with_nothing_in_its_window_still_truncates() {
        nothing_in_the_window(via_checkpoint_after_frees);
    }

    /// Shape (a): an acknowledged COMMIT between the checkpoint's page flush and its truncation.
    /// Its page change reached no disk, because it came after `flush_all`, so only its records can
    /// bring it back after a crash, and before the fence the truncation discarded them.
    fn commit_inside_the_window(entry: CheckpointEntry) {
        let _serial = d253_serial();
        let dir = tempfile::tempdir().unwrap();
        let (dir_root, before, during);
        {
            let (bp, wal, txn) = setup(dir.path());
            // CONTROL: committed before the checkpoint, so its page exists and the checkpoint's
            // `flush_all` writes it. It must survive at every commit.
            let t = txn.begin().unwrap();
            let mut heap = HeapFileManager::new(bp.clone()).unwrap();
            dir_root = heap.first_directory_page_id;
            heap.set_transaction(txn.clone(), t);
            before = heap.insert(Tuple::new(vec![1])).unwrap();
            txn.commit(t).unwrap();

            let (arrived, release) = park_checkpoint_at(&txn, crate::wal::txn::CheckpointPausePoint::BeforeTruncate);
            let a = {
                let txn = txn.clone();
                std::thread::spawn(move || entry(&txn))
            };
            arrived
                .recv_timeout(std::time::Duration::from_secs(60))
                .expect("fixture: the checkpoint never reached the point before its truncation");

            // Thread B, with no statement lock: begin and write the same page, then commit.
            //
            // The commit runs on a thread of its own. On #16 `commit` takes `release_retry` after its
            // `Commit` is durable (`release_retired` runs on every commit), and the parked checkpoint
            // holds `release_retry` from its retry to its truncation decision, so B's commit returns
            // only after A has decided. So: wait until B's `Commit` is durable, release A, and take
            // B's `Ok` as the acknowledgement. Before the fix the truncation has by then discarded the
            // very record it acknowledges.
            let t = txn.begin().unwrap();
            let mut heap = HeapFileManager::open(dir_root, bp.clone());
            heap.set_transaction(txn.clone(), t);
            during = heap.insert(Tuple::new(vec![2])).unwrap();
            // B's `Commit` starts here, so the log is durable past this point exactly when it is.
            let commit_starts = wal.next_lsn.load(Ordering::SeqCst);
            let b = {
                let txn = txn.clone();
                std::thread::spawn(move || txn.commit(t))
            };
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            while wal.flushed_lsn.load(Ordering::SeqCst) <= commit_starts {
                assert!(
                    std::time::Instant::now() < deadline,
                    "fixture: B's Commit never became durable while the checkpoint was parked"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }

            release.send(()).unwrap();
            a.join().unwrap().expect("the checkpoint failed");
            b.join().unwrap().expect("B's commit was not acknowledged");
            assert!(
                txn.commits_since_checkpoint.load(Ordering::SeqCst) > 0,
                "premise failed: B's commit ran a checkpoint of its own (is FERRODB_CHECKPOINT_INTERVAL \
                 set?), which flushes its page and erases the red. This run is VOID"
            );
            // The crash: everything dropped here, and nothing flushes the pool on drop.
        }
        let (bp, _wal, txn) = setup(dir.path());
        recover(&txn).unwrap();
        let heap = HeapFileManager::open(dir_root, bp.clone());
        assert_eq!(
            heap.read(before).expect("control: the row committed before the checkpoint is gone").data,
            vec![1],
            "control: the row committed before the checkpoint changed"
        );
        match heap.read(during) {
            Ok(row) => assert_eq!(row.data, vec![2], "the row committed inside the window changed"),
            Err(e) => panic!(
                "the commit acknowledged inside the checkpoint's window is gone after a crash: the \
                 truncation discarded its records, and its page never reached the disk ({e})"
            ),
        }
    }

    /// Shape (b): a transaction that begins and writes as the checkpoint starts, and never commits.
    /// The checkpoint's `flush_all` writes its UNCOMMITTED page change, so after a crash only its
    /// records let recovery undo it, and before the fence the truncation discarded them.
    ///
    /// This one also pins WHERE the fence is taken. A transaction that begins after `checkpoint`
    /// releases the attach table and before `checkpoint_locked` runs appends BELOW any sample
    /// taken inside `checkpoint_locked`, so only a sample taken under the attach-table hold sees
    /// that anything moved.
    fn uncommitted_write_as_it_starts(entry: CheckpointEntry) {
        let _serial = d253_serial();
        let dir = tempfile::tempdir().unwrap();
        let (dir_root, before);
        {
            let (bp, _wal, txn) = setup(dir.path());
            let t = txn.begin().unwrap();
            let mut heap = HeapFileManager::new(bp.clone()).unwrap();
            dir_root = heap.first_directory_page_id;
            heap.set_transaction(txn.clone(), t);
            before = heap.insert(Tuple::new(vec![1])).unwrap();
            txn.commit(t).unwrap();

            let (arrived, release) = park_checkpoint_at(&txn, crate::wal::txn::CheckpointPausePoint::AtEntry);
            let a = {
                let txn = txn.clone();
                std::thread::spawn(move || entry(&txn))
            };
            arrived
                .recv_timeout(std::time::Duration::from_secs(60))
                .expect("fixture: the checkpoint never reached its first step");

            // Thread B: begin and write, and never commit. The checkpoint then flushes the page.
            let t = txn.begin().unwrap();
            let mut heap = HeapFileManager::open(dir_root, bp.clone());
            heap.set_transaction(txn.clone(), t);
            heap.insert(Tuple::new(vec![2])).unwrap();

            release.send(()).unwrap();
            a.join().unwrap().expect("the checkpoint failed");
        }
        let (bp, _wal, txn) = setup(dir.path());
        recover(&txn).unwrap();
        let heap = HeapFileManager::open(dir_root, bp.clone());
        let rows: Vec<_> = heap.scan().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            heap.read(before).expect("control: the row committed before the checkpoint is gone").data,
            vec![1],
            "control: the row committed before the checkpoint changed"
        );
        assert_eq!(
            rows.len(),
            1,
            "an uncommitted row is durable after a crash: the checkpoint wrote its page and the \
             truncation discarded the records recovery needed to undo it"
        );
    }

    /// NEGATIVE CONTROL: a checkpoint with nothing appended in its window truncates, exactly as
    /// before. Without it a fix that kept the log on every checkpoint would pass the two tests
    /// above and grow the log for ever.
    fn nothing_in_the_window(entry: CheckpointEntry) {
        let _serial = d253_serial();
        let dir = tempfile::tempdir().unwrap();
        let (dir_root, rid);
        {
            let (bp, wal, txn) = setup(dir.path());
            let t = txn.begin().unwrap();
            let mut heap = HeapFileManager::new(bp.clone()).unwrap();
            dir_root = heap.first_directory_page_id;
            heap.set_transaction(txn.clone(), t);
            rid = heap.insert(Tuple::new(vec![3])).unwrap();
            txn.commit(t).unwrap();

            let base = wal.base_lsn.load(Ordering::SeqCst);
            assert!(
                base < wal.next_lsn.load(Ordering::SeqCst),
                "premise failed: the log holds nothing to discard, so a checkpoint already ran at \
                 COMMIT (is FERRODB_CHECKPOINT_INTERVAL set?). This run is VOID"
            );
            entry(&txn).unwrap();
            let (new_base, end) =
                (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst));
            assert!(new_base > base, "the checkpoint kept the log ({base} -> {new_base})");
            assert_eq!(new_base, end, "the checkpoint did not restart the log at its end");
        }
        let (bp, _wal, txn) = setup(dir.path());
        recover(&txn).unwrap();
        let heap = HeapFileManager::open(dir_root, bp.clone());
        assert_eq!(heap.read(rid).unwrap().data, vec![3], "the checkpointed row is gone");
    }

    /// CONTROL for the DDL path: `ddl_checkpointed` reads its fence AFTER its body, so a record the
    /// body appends is discarded exactly as before D253 and the checkpoint still truncates. Read
    /// before the body, the fence would keep the log at every DDL checkpoint.
    #[test]
    fn a_ddl_checkpoint_whose_body_appends_still_truncates() {
        let _serial = d253_serial();
        let dir = tempfile::tempdir().unwrap();
        let (_bp, wal, txn) = setup(dir.path());
        let appended = txn.ddl_checkpointed(|| wal.append(0, 0, &RecKind::Begin)).unwrap();
        let (base, end) = (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst));
        assert!(
            base > appended,
            "the DDL checkpoint kept the record its own body appended (base {base}, record at {appended})"
        );
        assert_eq!(base, end, "the DDL checkpoint did not restart the log at its end");
    }

    /// Path B: a DDL checkpoint holds the attach table from its check to its truncation, so no
    /// TRANSACTION can append inside its window, but `log_ddl` takes neither `att` nor
    /// `release_retry`. Its record, appended after `ddl_unit` read the fence, must not be discarded.
    /// `through_drop` selects `drop_checkpointed` (a DROP of a fresh heap's directory) over
    /// `ddl_checkpointed`.
    fn a_ddl_record_appended_inside_the_ddl_window_survives(through_drop: bool) {
        let _serial = d253_serial();
        let dir = tempfile::tempdir().unwrap();
        let (bp, wal, txn) = setup(dir.path());
        let frees = if through_drop {
            vec![HeapFileManager::new(bp.clone()).unwrap().first_directory_page_id]
        } else {
            Vec::new()
        };
        let (arrived, release) = park_checkpoint_at(&txn, crate::wal::txn::CheckpointPausePoint::BeforeTruncate);
        let x = {
            let txn = txn.clone();
            std::thread::spawn(move || {
                if frees.is_empty() {
                    txn.ddl_checkpointed(|| Ok(()))
                } else {
                    // D250's API (merged after this test was written): the DROP's record names the
                    // heaps it frees. This raw heap has no time-travel heap, so it names its one root
                    // twice, as D250's own adapted tests do.
                    txn.drop_checkpointed(
                        crate::wal::txn::DdlRecord {
                            op: crate::wal::log::DdlOp::DropTable,
                            table: "d253_dropped".into(),
                            dir_root: frees[0],
                            time_travel_root: frees[0],
                            columns: Vec::new(),
                        },
                        || Ok(()),
                    )
                }
            })
        };
        arrived
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("fixture: the DDL checkpoint never reached the point before its truncation");

        // Another thread's DDL record, appended while the DDL checkpoint is parked.
        let at = wal.next_lsn.load(Ordering::SeqCst);
        txn.log_ddl(crate::wal::txn::DdlRecord {
            op: crate::wal::log::DdlOp::CreateTable,
            table: "d253_inside_the_window".into(),
            dir_root: 9_999,
            time_travel_root: 0,
            columns: Vec::new(),
        })
        .unwrap();

        release.send(()).unwrap();
        x.join().unwrap().expect("the DDL checkpoint failed");
        let base = wal.base_lsn.load(Ordering::SeqCst);
        assert!(
            base <= at,
            "the DDL record appended inside the DDL checkpoint's window is gone: the truncation moved \
             the log's base to {base}, past the record at {at}"
        );
        let (rec, _) = wal.read_record(at).expect("the record inside the window cannot be read");
        assert!(
            matches!(rec.kind, RecKind::Ddl { .. }),
            "the record at {at} is not the DDL record appended inside the window: {:?}",
            rec.kind
        );
    }

    #[test]
    fn a_ddl_record_appended_inside_ddl_checkpointed_survives_its_truncation() {
        a_ddl_record_appended_inside_the_ddl_window_survives(false);
    }

    #[test]
    fn a_ddl_record_appended_inside_drop_checkpointed_survives_its_truncation() {
        a_ddl_record_appended_inside_the_ddl_window_survives(true);
    }

    /// A checkpoint kept by the fence says so: it is neither a truncation nor a pin's keep (lane_d253
    /// AMENDMENT 3 (b)). Through `checkpoint_keeping_owed`, which answers what it did; a commit lands
    /// inside its window, as in `commit_inside_the_window`. Written against the outcomes that exist
    /// before the fence; T7 (`a_fence_keep_is_its_own_outcome_and_counter`) names the new one.
    #[test]
    fn a_fence_keep_is_neither_a_truncation_nor_a_pin() {
        let _serial = d253_serial();
        let dir = tempfile::tempdir().unwrap();
        let (bp, wal, txn) = setup(dir.path());
        let t = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        let dir_root = heap.first_directory_page_id;
        heap.set_transaction(txn.clone(), t);
        heap.insert(Tuple::new(vec![1])).unwrap();
        txn.commit(t).unwrap();

        let (arrived, release) = park_checkpoint_at(&txn, crate::wal::txn::CheckpointPausePoint::BeforeTruncate);
        let a = {
            let txn = txn.clone();
            std::thread::spawn(move || txn.checkpoint_keeping_owed())
        };
        arrived
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("fixture: the checkpoint never reached the point before its truncation");
        let t = txn.begin().unwrap();
        let mut heap = HeapFileManager::open(dir_root, bp.clone());
        heap.set_transaction(txn.clone(), t);
        heap.insert(Tuple::new(vec![2])).unwrap();
        let commit_starts = wal.next_lsn.load(Ordering::SeqCst);
        let b = {
            let txn = txn.clone();
            std::thread::spawn(move || txn.commit(t))
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while wal.flushed_lsn.load(Ordering::SeqCst) <= commit_starts {
            assert!(std::time::Instant::now() < deadline, "fixture: B's Commit never became durable");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        release.send(()).unwrap();
        let outcome = a.join().unwrap().expect("the checkpoint failed");
        b.join().unwrap().expect("B's commit was not acknowledged");
        assert!(
            !matches!(
                outcome,
                crate::wal::txn::CheckpointOutcome::Truncated | crate::wal::txn::CheckpointOutcome::KeptByPin
            ),
            "a checkpoint that a commit inside its window should have kept answered {outcome:?}: a \
             truncation loses that commit, and a pin's keep blames something that did not happen"
        );
    }

    /// T7 (lane_d253 AMENDMENT 3 (b)): a fence keep is its OWN outcome and its OWN count. #16 read
    /// every kept log whose base did not move as a pin's keep, so a fence keep would have been
    /// counted as a pin, bumped `KEPT_LOG_DROPS` on a DROP, and printed a line blaming a pin.
    /// Written against the new API, so its red is mutant-only. Exact deltas are safe because every
    /// D253 test holds `d253_serial`, and nothing else in this process can make the fence keep.
    #[test]
    fn a_fence_keep_is_its_own_outcome_and_counter() {
        use crate::wal::txn::{fence_kept_checkpoints, kept_log_drops, CheckpointOutcome};
        let _serial = d253_serial();

        // (i) The automatic trigger's entry, with a commit inside its window.
        let dir = tempfile::tempdir().unwrap();
        let (bp, wal, txn) = setup(dir.path());
        let t = txn.begin().unwrap();
        let mut heap = HeapFileManager::new(bp.clone()).unwrap();
        let dir_root = heap.first_directory_page_id;
        heap.set_transaction(txn.clone(), t);
        heap.insert(Tuple::new(vec![1])).unwrap();
        txn.commit(t).unwrap();
        let fenced = fence_kept_checkpoints();
        let (arrived, release) = park_checkpoint_at(&txn, crate::wal::txn::CheckpointPausePoint::BeforeTruncate);
        let a = {
            let txn = txn.clone();
            std::thread::spawn(move || txn.checkpoint_keeping_owed())
        };
        arrived
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("fixture: the checkpoint never reached the point before its truncation");
        let t = txn.begin().unwrap();
        let mut heap = HeapFileManager::open(dir_root, bp.clone());
        heap.set_transaction(txn.clone(), t);
        heap.insert(Tuple::new(vec![2])).unwrap();
        let commit_starts = wal.next_lsn.load(Ordering::SeqCst);
        let b = {
            let txn = txn.clone();
            std::thread::spawn(move || txn.commit(t))
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while wal.flushed_lsn.load(Ordering::SeqCst) <= commit_starts {
            assert!(std::time::Instant::now() < deadline, "fixture: B's Commit never became durable");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        release.send(()).unwrap();
        let outcome = a.join().unwrap().expect("the checkpoint failed");
        b.join().unwrap().expect("B's commit was not acknowledged");
        assert_eq!(outcome, CheckpointOutcome::KeptByFence, "the fence's keep was reported as {outcome:?}");
        assert_eq!(fence_kept_checkpoints() - fenced, 1, "the fence's keep was not counted once");

        // (ii) A DROP whose window gets another thread's DDL record.
        let dir = tempfile::tempdir().unwrap();
        let (bp, _wal, txn) = setup(dir.path());
        let frees = vec![HeapFileManager::new(bp.clone()).unwrap().first_directory_page_id];
        let (fenced, drops) = (fence_kept_checkpoints(), kept_log_drops());
        let (arrived, release) = park_checkpoint_at(&txn, crate::wal::txn::CheckpointPausePoint::BeforeTruncate);
        let x = {
            let txn = txn.clone();
            // D250's API, as in `a_ddl_record_appended_inside_the_ddl_window_survives`.
            std::thread::spawn(move || {
                txn.drop_checkpointed(
                    crate::wal::txn::DdlRecord {
                        op: crate::wal::log::DdlOp::DropTable,
                        table: "d253_t7_dropped".into(),
                        dir_root: frees[0],
                        time_travel_root: frees[0],
                        columns: Vec::new(),
                    },
                    || Ok(()),
                )
            })
        };
        arrived
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("fixture: the DROP's checkpoint never reached the point before its truncation");
        txn.log_ddl(crate::wal::txn::DdlRecord {
            op: crate::wal::log::DdlOp::CreateTable,
            table: "d253_t7".into(),
            dir_root: 9_998,
            time_travel_root: 0,
            columns: Vec::new(),
        })
        .unwrap();
        release.send(()).unwrap();
        x.join().unwrap().expect("the DROP failed");
        assert_eq!(fence_kept_checkpoints() - fenced, 1, "the DROP's fence keep was not counted as one");
        assert_eq!(kept_log_drops() - drops, 0, "the DROP's fence keep was counted as a pin's");
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
    /// freed page as it finds it. On this branch a freed page is not flushed before the free, so one
    /// never flushed is a zero page, `Page::empty` with LSN 0, and every record applies. On the D229
    /// merge every page is flushed before any free, so redo's page-LSN skip makes the replay a no-op
    /// there and this test does not discriminate; test 13 (reuse under the pin) does, on both trees
    /// (lane §3.8, §3.12). The fix: the DROP's record is durable before its frees, and recovery skips
    /// every record a later DROP names. Red at `2c10f17`, where the DROP is refused for the pin;
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

    /// **D250 (lane §2 test 2): after a DROP whose checkpoint failed, and a crash, the next open writes
    /// no page the DROP freed.** The same state as a pin, reached by an I/O error: the catalog change
    /// was written, and the log still holds the table's records. On this branch the frees came before
    /// the failed sync, so the pages are free on disk; on the D229 merge the sync fails before any
    /// free, and the frees happen at the reopen (lane §3.12). Red at `2c10f17` at the page bytes:
    /// there the `DropTable` record was logged only after a successful checkpoint, so the log held the
    /// inserts and no DROP, and redo replayed them onto the table's zeroed data page.
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

    /// **D250, the F2 residual the lead accepted (lane §3.7 test 10, rescoped in §3.12): an EMPTY table
    /// re-created at the dropped root by a CREATE whose sync failed is forgotten by the next open, and
    /// no page of it is handed out while anything live names it.** After the DROP's record the log
    /// holds no `CreateTable` (the CREATE failed before logging it) and no heap record, so nothing
    /// tells the new table from the dropped one. The CREATE was reported failed, and no committed row
    /// is lost (a committed row is test 7).
    ///
    /// **The safety property, on every tree:** every page `allocate` hands out after the open, the
    /// forgotten table's roots included, is named by no live catalog entry or index: a free never
    /// aliases a live page. A live table `keep` exists so that property has something to alias. The
    /// first version asserted that none of the forgotten roots is ever handed out, which pinned the
    /// LEAK: on the D229 merge the open-time index reset frees the empty primary-root leaf, correctly.
    /// **The leak is a separate count, stated per tree** in `LEAKED_ROOTS`. Killed by FREEm (the
    /// count) and ALIASm (the safety check). Only the three roots are counted; another page the
    /// CREATE allocated is not.
    #[test]
    fn an_empty_table_recreated_at_the_dropped_root_by_a_failed_create_is_forgotten_and_its_pages_leak() {
        /// The forgotten table's roots that stay allocated and unnamed after the open. 3 on this
        /// branch, where the forget frees nothing. **2 on the D229 merge**, the heap and time-travel
        /// directory roots, where the open-time index reset frees the primary-root leaf: the merge sets
        /// it, as pre-registered by the lead's decision (lane §3.12).
        const LEAKED_ROOTS: usize = 3;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("recreated_empty.db");
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owned = {
            let open = |p: PathBuf| OpenOptions::new().read(true).write(true).create(true).truncate(true).open(p).unwrap();
            let page_file = open(db.clone());
            let wal_file = open(PathBuf::from(format!("{}.wal", db.display())));
            let (bp, _wal, txn, mut catalog) =
                manual_db(&db, Arc::new(SyncFailsWhenArmed { file: page_file, armed: armed.clone() }), Arc::new(wal_file));
            for sql in [
                "CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER);",
                "INSERT INTO keep VALUES (1, 10);",
                "CREATE INDEX kv ON keep (v);",
            ] {
                run_sql(sql, &mut catalog, &bp, &txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
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
        // Every page a live table OWNS, as the open left them (after the rebuild): each heap's and
        // time-travel heap's directory chain and the pages it lists, and every node of every tree.
        // Lane §3.15 (D250 review 3's D): roots alone let a data-page alias through.
        let heap_pages = |dir_root: u32| -> Vec<u32> {
            let mut out = Vec::new();
            let mut dir_page = dir_root;
            while dir_page != 0 {
                let frame_i = o.bp.fetch_page(dir_page).unwrap();
                let data = o.bp.frames[frame_i].read().unwrap().data;
                o.bp.unpin_page(dir_page, false);
                let dir = crate::storage::page_directory::PageDirectory::deserialize(data);
                out.push(dir_page);
                out.extend(dir.entries.iter().map(|e| e.page_id));
                dir_page = dir.next_page_directory;
            }
            out
        };
        let primary_pages = |root: u32| -> Vec<u32> {
            let tree = BPlusTreeManager::<Value, RecordId>::open(root, o.bp.clone());
            let mut out = Vec::new();
            let mut stack = vec![root];
            while let Some(page) = stack.pop() {
                out.push(page);
                if let crate::storage::index_page::BPlusTreePage::Internal(node) = tree.read_node(page).unwrap() {
                    stack.extend(node.child_ptrs.iter().copied());
                }
            }
            out
        };
        let secondary_pages = |root: u32| -> Vec<u32> {
            let tree = BPlusTreeManager::<(Value, Value), ()>::open(root, o.bp.clone());
            let mut out = Vec::new();
            let mut stack = vec![root];
            while let Some(page) = stack.pop() {
                out.push(page);
                if let crate::storage::index_page::BPlusTreePage::Internal(node) = tree.read_node(page).unwrap() {
                    stack.extend(node.child_ptrs.iter().copied());
                }
            }
            out
        };
        let mut live: HashSet<u32> = HashSet::new();
        for e in o.catalog.tables.values() {
            live.extend(heap_pages(e.first_directory_page_id));
            live.extend(heap_pages(e.time_travel_root));
            live.extend(primary_pages(e.primary_index_root));
            for i in &e.indexes {
                live.extend(secondary_pages(i.root_page_id));
            }
            for i in &e.fulltext_indexes {
                live.extend(secondary_pages(i.root_page_id));
            }
        }
        let keep_dir = o.catalog.get_table("keep").expect("premise failed: `keep` did not survive the open").first_directory_page_id;
        assert!(
            heap_pages(keep_dir).len() >= 2,
            "premise failed: `keep`'s heap lists no data page, so a data-page alias could not be seen"
        );
        // `allocate` hands out the lowest clear bit, so every free page below the high-water mark comes
        // out before the first page at or above it.
        let high = o.bp.disk_manager.high_water().unwrap();
        let mut handed_out: HashSet<u32> = HashSet::new();
        let mut reached_the_top = false;
        for _ in 0..=high {
            let next = o.bp.disk_manager.allocate().unwrap();
            assert!(
                !live.contains(&next),
                "page {next} was handed out while a live catalog entry names it{}: a free aliased a live page",
                if owned.contains(&next) { ", and it is a root of the table the open forgot" } else { "" }
            );
            if next >= high {
                reached_the_top = true;
                break;
            }
            handed_out.insert(next);
        }
        assert!(reached_the_top, "`allocate` handed out more pages below the high-water mark {high} than there are");
        let leaked = owned.iter().filter(|p| !handed_out.contains(*p) && !live.contains(*p)).count();
        assert_eq!(
            leaked, LEAKED_ROOTS,
            "the forgotten table's roots {owned:?}: {leaked} stay allocated and unnamed, where this tree's stated leak is \
             {LEAKED_ROOTS}; {} were handed out and {} are live",
            owned.iter().filter(|p| handed_out.contains(*p)).count(),
            owned.iter().filter(|p| live.contains(*p)).count()
        );
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
        // `InMemory` and `.unwrap()`: the door's signature since lane §3.15. The assertions are unchanged.
        let runtime = o.attach_runtime(runtime, ProvenanceBacking::InMemory).unwrap();
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

    /// **D250 review 3's A (lane §3.15 test 18): a DROP in the log does not make every later open
    /// rebuild.** At `cd0914b` the open re-declared every dropped table's `DropTable` after its
    /// truncation, and `recover` counts any non-empty log as recovered, so every open of a process
    /// that never truncated after its open (pgserver always, a killed CLI) rebuilt every index. The
    /// forget now runs inside the open, before its checkpoint, and nothing is re-declared.
    #[test]
    fn a_drop_in_the_log_does_not_make_every_later_open_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("three_opens.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            for sql in ["CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", "INSERT INTO t VALUES (1, 10);", "DROP TABLE t;"] {
                run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            // The crash: every handle goes, and no checkpoint runs after the DROP's.
        }
        {
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).expect("open 1 failed");
            assert!(o.recovered, "premise failed: the log after the DROP held nothing, so nothing here could recur");
        }
        for open in [2, 3] {
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).unwrap_or_else(|e| panic!("open {open} failed: {e}"));
            assert!(
                !o.recovered,
                "open {open} found a non-empty log and rebuilt every index: a DROP long since completed keeps \
                 every open of a process that never truncates rebuilding"
            );
        }
    }

    /// **D250 review 3's A (lane §3.15 test 17): a DROP whose own forget never ran is forgotten by the
    /// next open.** The executor's DROP forgets the table's authors after its barrier (B9); a crash in
    /// between left them in the database's provenance file. The next open finds the DROP in the log and
    /// the table gone from the catalog, and forgets them itself, before its checkpoint truncates the
    /// record away, with no runtime attached.
    #[test]
    fn a_drop_whose_own_forget_never_ran_is_forgotten_by_the_next_open() {
        use crate::{
            agent_sql::runtime::table_id,
            branch::types::BranchId,
            provenance::{DurableProvenanceStore, ProvId, ProvenanceStore, RunEntity},
        };
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("forget_at_open.db");
        let provenance = PathBuf::from(format!("{}.provenance", db.display()));
        {
            // Authors of `t` and `keep` in the database's provenance file, as a CLI session leaves them.
            let store = DurableProvenanceStore::open(&provenance).unwrap();
            let run = RunEntity::new(ProvId::NONE, "agent", "run-1", "model", "v1", [7u8; 32], 1_700_000_000_000, BranchId::new(1, 0));
            let author = store.intern(&run).unwrap();
            store.stamp_row(table_id("t").0, 1, author).unwrap();
            store.stamp_row(table_id("keep").0, 1, author).unwrap();
        }
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            // `run_sql`'s session has an IN-MEMORY runtime, so the executor's forget never reaches the
            // file: the state a crash between the DROP and its forget leaves.
            for sql in [
                "CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER);",
                "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);",
                "INSERT INTO t VALUES (1, 10);",
                "DROP TABLE t;",
            ] {
                run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            // The crash.
        }
        {
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).expect("the open after the DROP failed");
            assert!(o.catalog.get_table("t").is_none(), "premise failed: `t` came back");
            // No runtime is attached: the forget must not depend on one.
        }
        let store = DurableProvenanceStore::open(&provenance).unwrap();
        assert_eq!(
            store.row_author(table_id("t").0, 1).unwrap(),
            ProvId::NONE,
            "the dropped `t`'s authors survived an open that found its DROP in the log: a table created under the \
             name inherits them"
        );
        assert_ne!(
            store.row_author(table_id("keep").0, 1).unwrap(),
            ProvId::NONE,
            "the open forgot the authors of `keep`, which was never dropped"
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
            // Lane §3.15 (D250 review 3's Q3-1): LSNm's kill rests on `u`'s roots BEING `t`'s, which
            // the allocation's symmetry gives today; asserted, so a change to it cannot pass silently.
            assert_eq!(
                (u_heap, u_tt),
                (t_heap, t_tt),
                "premise failed: `u`'s heap and time-travel roots are not the dropped `t`'s, so LSNm could survive"
            );
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

    /// **D250 review 4's N1 (lane §3.17 test 20): a DROP that answers `Err` after its frees has still
    /// forgotten the table's authors.** The executor forgot only after `drop_checkpointed`'s `?`. So an
    /// error from the barrier's checkpoint (here its sync), with the table already gone and the log not
    /// poisoned, skipped the forget, and a `CREATE TABLE t` in the same process inherited the dropped
    /// `t`'s authors for good. Red at `4ae1ec4`.
    #[test]
    fn a_drop_that_fails_at_its_checkpoint_still_forgets_the_tables_authors() {
        use crate::{
            agent_sql::runtime::table_id,
            branch::types::BranchId,
            provenance::{ProvId, RunEntity},
            tel::ids::RowId,
        };
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("failed_drop_authors.db");
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&db).unwrap();
        let dm = DiskManager::with_storage(Arc::new(SyncFailsWhenArmed { file, armed: armed.clone() })).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(dm)));
        let wal = Arc::new(WalManager::new(PathBuf::from(format!("{}.wal", db.display()))).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let runtime = Arc::new(AgentRuntime::new());
        let mut session = Session::with_runtime(runtime.clone());
        for sql in ["CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", "INSERT INTO t VALUES (1, 10);"] {
            run_sql_in(sql, &mut catalog, &bp, &txn, &mut session).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
        }
        let run = RunEntity::new(ProvId::NONE, "agent", "run-1", "model", "v1", [7u8; 32], 1_700_000_000_000, BranchId::new(1, 0));
        let author = runtime.provenance().intern(&run).unwrap();
        runtime.provenance().stamp_row(table_id("t").0, 1, author).unwrap();
        assert!(runtime.who_wrote_row("t", RowId(1)).is_some(), "premise failed: row 1 of `t` has no author before the DROP");
        armed.store(true, Ordering::SeqCst);
        let e = match run_sql_in("DROP TABLE t;", &mut catalog, &bp, &txn, &mut session) {
            Err(e) => e,
            Ok(_) => panic!("premise failed: the DROP's checkpoint did not fail"),
        };
        assert!(e.to_string().contains("injected"), "premise failed: the DROP failed, but not at its checkpoint's sync: {e}");
        assert!(catalog.get_table("t").is_none(), "premise failed: the DROP failed before its mutation, so it freed nothing");
        assert!(
            wal.poisoned().is_none(),
            "premise failed: the log is poisoned, so this is not N1's state (an Err after the frees, on a writable log)"
        );
        armed.store(false, Ordering::SeqCst);
        run_sql_in("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut catalog, &bp, &txn, &mut session)
            .unwrap_or_else(|e| panic!("premise failed: the CREATE after the failed DROP failed: {e}"));
        assert!(
            runtime.who_wrote_row("t", RowId(1)).is_none(),
            "the re-created `t` inherits the dropped `t`'s author: the DROP answered Err after its frees and skipped its forget"
        );
        assert_eq!(
            runtime.provenance().row_author(table_id("t").0, 1).unwrap(),
            ProvId::NONE,
            "the store still names an author for row 1 of the re-created `t`"
        );
    }

    /// **D250 review 4's N4 (lane §3.17 test 21): two `Durable` attaches of one open share one
    /// provenance store.** When the open had nothing to forget, each `attach_runtime(.., Durable)`
    /// opened the file again: two appenders on one file, each with its own in-memory index. Red at
    /// `4ae1ec4`: the second store's index was replayed before the first one's stamp.
    #[test]
    fn two_durable_attaches_of_one_open_share_one_provenance_store() {
        use crate::{agent_sql::runtime::table_id, branch::types::BranchId, provenance::{ProvId, RunEntity}};
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("two_attaches.db");
        let lock = DbLock::acquire(&db).unwrap();
        let o = open_recovered(&db, &lock).unwrap();
        assert!(o.provenance.is_none(), "premise failed: the open had a table to forget, so it opened the store itself");
        let a = o.attach_runtime(AgentRuntime::new(), ProvenanceBacking::Durable).unwrap();
        let b = o.attach_runtime(AgentRuntime::new(), ProvenanceBacking::Durable).unwrap();
        let run = RunEntity::new(ProvId::NONE, "agent", "run-1", "model", "v1", [7u8; 32], 1_700_000_000_000, BranchId::new(1, 0));
        let author = a.provenance().intern(&run).unwrap();
        a.provenance().stamp_row(table_id("t").0, 1, author).unwrap();
        assert_eq!(
            b.provenance().row_author(table_id("t").0, 1).unwrap(),
            author,
            "a second Durable attach of one open does not see a stamp made through the first: two stores append to one \
             file with in-memory indexes that disagree"
        );
    }

    /// **D250 review 4's N2 (lane §3.17 test 22): an open whose provenance forget fails keeps the DROP
    /// for the next open.** The forget was counted and the open went on, and its checkpoint truncated
    /// the `DropTable`, the only record from which a later open computes what to forget. The store's
    /// poison is in memory, so after a restart the dropped table's authors were served again, and
    /// nothing retried. Red at `4ae1ec4`: the log after the first open holds no `DropTable`.
    #[test]
    fn an_open_whose_provenance_forget_fails_keeps_the_drop_for_the_next_open() {
        use crate::{
            agent_sql::runtime::table_id,
            branch::types::BranchId,
            provenance::{DurableProvenanceStore, ProvId, ProvenanceStore, RunEntity},
        };
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("forget_fails_at_open.db");
        let provenance = PathBuf::from(format!("{}.provenance", db.display()));
        {
            let store = DurableProvenanceStore::open(&provenance).unwrap();
            let run = RunEntity::new(ProvId::NONE, "agent", "run-1", "model", "v1", [7u8; 32], 1_700_000_000_000, BranchId::new(1, 0));
            let author = store.intern(&run).unwrap();
            store.stamp_row(table_id("t").0, 1, author).unwrap();
            store.stamp_row(table_id("keep").0, 1, author).unwrap();
        }
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            // `run_sql`'s runtime is in memory, so the executor's forget never reaches the file.
            for sql in [
                "CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER);",
                "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);",
                "INSERT INTO t VALUES (1, 10);",
                "DROP TABLE t;",
            ] {
                run_sql(sql, &mut o.catalog, &o.bp, &o.txn).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            // The crash.
        }
        fail_the_next_open_forget(&provenance);
        let failures = provenance_forget_failures();
        {
            let lock = DbLock::acquire(&db).unwrap();
            // `Ok` or `Err`: either way this process ends here, by a crash or a refused open.
            let _first = open_recovered(&db, &lock);
            assert_eq!(
                provenance_forget_failures(),
                failures + 1,
                "premise failed: the injected forget failure did not fire exactly once"
            );
        }
        let drops_of_t = {
            let wal = WalManager::new(PathBuf::from(format!("{}.wal", db.display()))).unwrap();
            let end = wal.next_lsn.load(Ordering::SeqCst);
            let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
            let mut n = 0;
            while lsn < end {
                let (rec, next) = wal.read_record(lsn).unwrap();
                if matches!(&rec.kind, RecKind::Ddl { op: DdlOp::DropTable, table, .. } if table == "t") {
                    n += 1;
                }
                lsn = next;
            }
            n
        };
        assert!(
            drops_of_t > 0,
            "the open whose forget failed truncated the only DropTable that could retry it: the dropped `t`'s authors \
             are permanent"
        );
        {
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).expect("the open after the failed forget failed");
            assert!(o.catalog.get_table("t").is_none(), "premise failed: `t` came back");
        }
        let store = DurableProvenanceStore::open(&provenance).unwrap();
        assert_eq!(
            store.row_author(table_id("t").0, 1).unwrap(),
            ProvId::NONE,
            "the dropped `t`'s authors survived the open that retried the forget"
        );
        assert_ne!(
            store.row_author(table_id("keep").0, 1).unwrap(),
            ProvId::NONE,
            "the retried forget took the authors of `keep`, which was never dropped"
        );
    }

    /// **D250 review 4's N1, its durable half (lane §3.17 test 23, a guard added with the fix, because it
    /// needs `DurableProvenanceStore::shared`): a DROP whose durable forget fails poisons the log, and the
    /// next open forgets.** The forget runs inside the DROP's unit, so its failure is the unit's `Err`:
    /// the log is poisoned, and the next open completes the DROP and forgets the table in the file
    /// itself. Its discrimination is FGEm, which discards the forget's answer.
    #[test]
    fn a_drop_whose_durable_forget_fails_poisons_the_log_and_the_next_open_forgets() {
        use crate::{agent_sql::runtime::table_id, branch::types::BranchId, provenance::{ProvId, RunEntity}};
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("drop_forget_fails.db");
        {
            let lock = DbLock::acquire(&db).unwrap();
            let mut o = open_recovered(&db, &lock).unwrap();
            let runtime = o.attach_runtime(AgentRuntime::new(), ProvenanceBacking::Durable).unwrap();
            let mut session = Session::with_runtime(runtime.clone());
            for sql in [
                "CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER);",
                "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);",
                "INSERT INTO t VALUES (1, 10);",
            ] {
                run_sql_in(sql, &mut o.catalog, &o.bp, &o.txn, &mut session).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
            }
            let run = RunEntity::new(ProvId::NONE, "agent", "run-1", "model", "v1", [7u8; 32], 1_700_000_000_000, BranchId::new(1, 0));
            let author = runtime.provenance().intern(&run).unwrap();
            for table in ["t", "keep"] {
                runtime.provenance().stamp_row(table_id(table).0, 1, author).unwrap();
            }
            let store = DurableProvenanceStore::shared(provenance_path(&db)).unwrap();
            assert!(
                std::ptr::eq(Arc::as_ptr(&store) as *const u8, Arc::as_ptr(runtime.provenance()) as *const u8),
                "premise failed: the runtime's store is not the file's one store, so arming it arms nothing the DROP uses"
            );
            store.fail_next_append.store(true, Ordering::SeqCst);
            let e = run_sql_in("DROP TABLE t;", &mut o.catalog, &o.bp, &o.txn, &mut session)
                .expect_err("the DROP answered Ok although its durable forget failed");
            assert!(
                o.wal.poisoned().is_some(),
                "a DROP whose durable forget failed left the log writable ({e}): the process can go on, and truncate the \
                 DropTable the next open needs to retry the forget"
            );
            // The crash.
        }
        {
            let lock = DbLock::acquire(&db).unwrap();
            let o = open_recovered(&db, &lock).expect("the open after the DROP whose forget failed");
            assert!(o.catalog.get_table("t").is_none(), "premise failed: `t` came back");
        }
        let store = DurableProvenanceStore::open(provenance_path(&db)).unwrap();
        assert_eq!(
            store.row_author(table_id("t").0, 1).unwrap(),
            ProvId::NONE,
            "the dropped `t`'s authors survived: the DROP's failed forget was not retried by the next open"
        );
        assert_ne!(
            store.row_author(table_id("keep").0, 1).unwrap(),
            ProvId::NONE,
            "the retried forget took the authors of `keep`, which was never dropped"
        );
    }
}

/// Provenance files whose next open-time forget fails, once (D250 review 4's N2, test 22). Keyed by
/// path, so arming it cannot fail another test's open.
#[cfg(test)]
static OPEN_FORGET_FAILURES_TO_INJECT: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

/// Make the next open-time forget in the provenance file at `path` fail.
#[cfg(test)]
fn fail_the_next_open_forget(path: &Path) {
    OPEN_FORGET_FAILURES_TO_INJECT.lock().unwrap().push(path.to_path_buf());
}

/// The test half of the N2 seam: the store forgets in memory, its append fails, and it poisons itself,
/// which is the production failure path.
#[cfg(test)]
fn inject_open_forget_failure(store: &DurableProvenanceStore, path: &Path) {
    let mut armed = OPEN_FORGET_FAILURES_TO_INJECT.lock().unwrap();
    if let Some(i) = armed.iter().position(|p| p == path) {
        armed.remove(i);
        store.fail_next_append.store(true, Ordering::SeqCst);
    }
}
